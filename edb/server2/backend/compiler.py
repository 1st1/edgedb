#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import collections
import hashlib
import typing

import asyncpg

from edb import errors

from edb.server import defines
from edb.server.pgsql import compiler as pg_compiler
from edb.server.pgsql import intromech

from edb.lang import edgeql

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import compiler as ql_compiler

from edb.lang.schema import schema as s_schema
from edb.lang.schema import types as s_types

from edb.server2 import dbstate

from . import sertypes


class CompilerDatabaseState(typing.NamedTuple):

    dbver: int
    con_args: dict
    schema: s_schema.Schema


class CompileContext(typing.NamedTuple):

    db: CompilerDatabaseState
    output_format: pg_compiler.OutputFormat
    single_query_mode: bool
    sess_modaliases: dict
    sess_vars: dict


class Compiler:

    _connect_args: dict
    _dbname: typing.Optional[str]
    _cached_db: typing.Optional[CompilerDatabaseState]

    def __init__(self, connect_args: dict):
        self._connect_args = connect_args
        self._dbname = None
        self._cached_db = None

    async def _get_database(self, dbver: int) -> CompilerDatabaseState:
        if self._cached_db is not None and self._cached_db.dbver == dbver:
            return self._cached_db

        self._cached_db = None

        con_args = self._connect_args.copy()
        con_args['user'] = defines.EDGEDB_SUPERUSER
        con_args['database'] = self._dbname

        con = await asyncpg.connect(**con_args)
        try:
            im = intromech.IntrospectionMech(con)
            schema = await im.readschema()
            db = CompilerDatabaseState(
                dbver=dbver,
                con_args=con_args,
                schema=schema)

            self._cached_db = db
            return db
        finally:
            await con.close()

    def _compile_ql_query(self, ctx: CompileContext, ql: qlast.Base):
        ir = ql_compiler.compile_ast_to_ir(
            ql,
            schema=ctx.db.schema,
            modaliases={None: 'default'},
            implicit_id_in_shapes=False)

        sql_text, argmap = pg_compiler.compile_ir_to_sql(
            ir,
            schema=ir.schema,
            pretty=False,
            output_format=ctx.output_format)

        extra = {}
        if ctx.single_query_mode:
            if ctx.output_format is pg_compiler.OutputFormat.NATIVE:
                out_type_data, out_type_id = sertypes.TypeSerializer.describe(
                    ir.schema, ir.expr.stype, ir.view_shapes)
            else:
                out_type_data, out_type_id = \
                    sertypes.TypeSerializer.describe_json()

            if ir.params:
                subtypes = [None] * len(ir.params)
                first_param_name = next(iter(ir.params))
                if first_param_name.isdecimal():
                    named = False
                    for param_name, param_type in ir.params.items():
                        subtypes[int(param_name)] = (param_name, param_type)
                else:
                    named = True
                    for param_name, param_type in ir.params.items():
                        subtypes[argmap[param_name] - 1] = (
                            param_name, param_type
                        )
                params_type = s_types.Tuple.create(
                    ir.schema,
                    element_types=collections.OrderedDict(subtypes),
                    named=named)
            else:
                params_type = s_types.Tuple.create(
                    ir.schema, element_types={}, named=False)

            in_type_data, in_type_id = sertypes.TypeSerializer.describe(
                ir.schema, params_type, {})

            extra['in_type_id'] = in_type_id.bytes
            extra['in_type_data'] = in_type_data
            extra['out_type_id'] = out_type_id.bytes
            extra['out_type_data'] = out_type_data

        sql_bytes = sql_text.encode(defines.EDGEDB_ENCODING)

        sql_hash = hashlib.sha1(
            b'sql: %b; mode: %b' % (
                sql_bytes, str(ctx.output_format).encode()
            )
        ).hexdigest().encode()

        return dbstate.CompiledQuery(
            dbver=ctx.db.dbver,
            sql=sql_bytes,
            sql_hash=sql_hash,
            **extra)

    def _compile_ql_ddl(self, ctx: CompileContext, ql: qlast.DDL):
        raise NotImplementedError

    def _compile_ql_migration(self, ctx: CompileContext, ql: qlast.Delta):
        raise NotImplementedError

    def _compile_ql_database_cmd(self, ctx: CompileContext,
                                 ql: qlast.Database):
        raise NotImplementedError

    def _compile_ql_transaction(self, ctx: CompileContext,
                                ql: qlast.Transaction):
        raise NotImplementedError

    def _compile_ql_sess_state(self, ctx: CompileContext,
                               ql: qlast.SessionStateDecl):
        raise NotImplementedError

    def _compile_ql(self, ctx: CompileContext, ql: qlast.Base):

        if isinstance(ql, qlast.Database):
            return self._compile_ql_database_cmd(ctx, ql)

        elif isinstance(ql, qlast.Delta):
            return self._compile_ql_migration(ctx, ql)

        elif isinstance(ql, qlast.DDL):
            return self._compile_ql_ddl(ctx, ql)

        elif isinstance(ql, qlast.Transaction):
            return self._compile_ql_transaction(ctx, ql)

        elif isinstance(ql, qlast.SessionStateDecl):
            return self._compile_ql_sess_state(ctx, ql)

        else:
            return self._compile_ql_query(ctx, ql)

    def _compile_single(self, *,
                        db: CompilerDatabaseState,
                        eql: str,
                        sess_modaliases: dict,
                        sess_vars: dict,
                        json_mode: bool=False):

        if json_mode:
            of = pg_compiler.OutputFormat.JSON
        else:
            of = pg_compiler.OutputFormat.NATIVE

        ctx = CompileContext(
            db=db,
            output_format=of,
            single_query_mode=True,
            sess_modaliases=sess_modaliases,
            sess_vars=sess_vars)

        statements = edgeql.parse_block(eql)
        if len(statements) != 1:
            raise errors.ProtocolError(
                f'expected one statement, got {len(statements)}')
        stmt = statements[0]

        return self._compile_ql(ctx, stmt)

    # API

    async def connect(self, dbname: str, dbver: int) -> CompilerDatabaseState:
        self._dbname = dbname
        self._cached_db = None
        await self._get_database(dbver)

    async def compile_eql_stmt(self, dbver: int,
                               eql: bytes,
                               sess_modaliases: dict,
                               sess_vars: dict,
                               json_mode: bool) -> dbstate.CompiledQuery:
        eql = eql.decode()
        db = await self._get_database(dbver)

        return self._compile_single(
            db=db,
            eql=eql,
            sess_modaliases=sess_modaliases,
            sess_vars=sess_vars,
            json_mode=json_mode)
