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


import asyncio
import collections
import hashlib
import typing

import asyncpg

from edb import errors

from edb.server import defines
from edb.server.pgsql import compiler as pg_compiler
from edb.server.pgsql import intromech

from edb.lang import edgeql
from edb.lang.edgeql import compiler as ql_compiler
from edb.lang.schema import types as s_types

from edb.server2 import dbstate

from . import sertypes
from . import state


class Compiler:

    _databases: typing.Dict[str, state.CompilerDatabaseState]

    def __init__(self, connect_args):
        self._connect_args = connect_args
        self._databases = {}

    async def _get_database(self, dbname: str,
                            dbver: int) -> state.CompilerDatabaseState:
        try:
            db = self._databases[dbname]
        except KeyError:
            pass
        else:
            if db.dbver == dbver:
                return db
            else:
                del self._databases[dbname]
                db = None

        con_args = self._connect_args.copy()
        con_args['user'] = defines.EDGEDB_SUPERUSER
        con_args['database'] = dbname

        con = await asyncpg.connect(**con_args)
        try:
            im = intromech.IntrospectionMech(con)
            schema = await im.getschema()
            db = state.CompilerDatabaseState(
                dbver=dbver,
                name=dbname,
                con_args=con_args,
                type_cache=im.type_cache,
                schema=schema)

            self._databases[dbname] = db
            return db
        finally:
            asyncio.create_task(con.close())

    def _compile(self, db: state.CompilerDatabaseState, eql):
        statements = edgeql.parse_block(eql)
        if len(statements) != 1:
            raise errors.ProtocolError(
                f'expected one statement, got {len(statements)}')
        stmt = statements[0]
        return self._compile_stmt(db, stmt)

    def _compile_stmt(self, db: state.CompilerDatabaseState, stmt):
        ir = ql_compiler.compile_ast_to_ir(
            stmt,
            schema=db.schema,
            modaliases={None: 'default'},
            implicit_id_in_shapes=False)

        out_type_data, out_type_id = sertypes.TypeSerializer.describe(
            db, ir.expr.scls, ir.view_shapes)

        sql_text, argmap = pg_compiler.compile_ir_to_sql(
            ir,
            schema=db.schema,
            pretty=False,
            output_format=pg_compiler.OutputFormat.NATIVE)

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
                    subtypes[argmap[param_name] - 1] = (param_name, param_type)
            params_type = s_types.Tuple(
                element_types=collections.OrderedDict(subtypes), named=named)
        else:
            params_type = s_types.Tuple(element_types={}, named=False)

        in_type_data, in_type_id = sertypes.TypeSerializer.describe(
            db, params_type, {})

        sql_bytes = sql_text.encode(defines.EDGEDB_ENCODING)
        sql_hash = hashlib.sha1(sql_bytes).hexdigest().encode('ascii')

        return dbstate.CompiledQuery(
            dbver=db.dbver,
            out_type_data=out_type_data,
            out_type_id=out_type_id.bytes,
            in_type_data=in_type_data,
            in_type_id=in_type_id.bytes,
            sql=sql_bytes,
            sql_hash=sql_hash)

    async def connect(self, dbname, dbver) -> state.CompilerDatabaseState:
        await self._get_database(dbname, dbver)

    async def compile_edgeql(self, dbname, dbver, eql: bytes):
        eql = eql.decode()
        db = await self._get_database(dbname, dbver)
        return self._compile(db, eql)
