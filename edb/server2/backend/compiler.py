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
import pathlib
import pickle
import typing

import asyncpg
import immutables

from edb import errors

from edb.server import defines
from edb.server.pgsql import compiler as pg_compiler
from edb.server.pgsql import intromech

from edb.lang import edgeql
from edb.lang.common import debug

from edb.lang.edgeql import ast as qlast
from edb.lang.edgeql import compiler as ql_compiler
from edb.lang.edgeql import quote as ql_quote

from edb.lang.ir import staeval as ireval

from edb.lang.schema import database as s_db
from edb.lang.schema import ddl as s_ddl
from edb.lang.schema import delta as s_delta
from edb.lang.schema import deltas as s_deltas
from edb.lang.schema import error as s_err
from edb.lang.schema import schema as s_schema
from edb.lang.schema import std as s_std
from edb.lang.schema import types as s_types

from edb.server.pgsql import delta as pg_delta
from edb.server.pgsql import dbops as pg_dbops

from . import config
from . import dbstate
from . import sertypes


class CompilerDatabaseState(typing.NamedTuple):

    dbver: int
    con_args: dict
    schema: s_schema.Schema


class CompileContext(typing.NamedTuple):

    dbver: int
    state: dbstate.ConnectionState
    output_format: pg_compiler.OutputFormat
    single_query_mode: bool


EMPTY_MAP = immutables.Map()


class Compiler:

    _connect_args: dict
    _dbname: typing.Optional[str]
    _cached_db: typing.Optional[CompilerDatabaseState]

    def __init__(self, connect_args: dict, data_dir: str):
        self._connect_args = connect_args
        self._data_dir = pathlib.Path(data_dir)
        self._dbname = None
        self._cached_db = None
        self._cached_std_schema = None
        self._current_db_state = None

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
            schema = await im.readschema(
                schema=self._get_std_schema(),
                exclude_modules=s_std.STD_MODULES)

            db = CompilerDatabaseState(
                dbver=dbver,
                con_args=con_args,
                schema=schema)

            self._cached_db = db
            return db
        finally:
            await con.close()

    def _get_std_schema(self):
        if self._cached_std_schema is not None:
            return self._cached_std_schema

        with open(self._data_dir / 'stdschema.pickle', 'rb') as f:
            try:
                self._cached_std_schema = pickle.load(f)
            except Exception as e:
                raise RuntimeError(
                    'could not load std schema pickle') from e

        return self._cached_std_schema

    def _hash_sql(self, sql: bytes, **kwargs: bytes):
        h = hashlib.sha1(sql)
        for param, val in kwargs.items():
            h.update(param.encode('latin1'))
            h.update(val)
        return h.hexdigest().encode('latin1')

    def _process_delta(self, delta, schema):
        """Adapt and process the delta command."""

        if debug.flags.delta_plan:
            debug.header('Delta Plan')
            debug.dump(delta, schema=schema)

        delta = pg_delta.CommandMeta.adapt(delta)
        context = pg_delta.CommandContext(self.connection)
        schema, _ = delta.apply(schema, context)

        if debug.flags.delta_pgsql_plan:
            debug.header('PgSQL Delta Plan')
            debug.dump(delta, schema=schema)

        return schema, delta

    def _compile_ql_query(
            self, ctx: CompileContext,
            ql: qlast.Base) -> dbstate.BaseQuery:

        current_tx = ctx.state.current_tx()

        ir = ql_compiler.compile_ast_to_ir(
            ql,
            schema=current_tx.get_schema(),
            modaliases=current_tx.get_modaliases(),
            implicit_id_in_shapes=False)

        sql_text, argmap = pg_compiler.compile_ir_to_sql(
            ir,
            schema=ir.schema,
            pretty=debug.flags.edgeql_compile,
            output_format=ctx.output_format)

        sql_bytes = sql_text.encode(defines.EDGEDB_ENCODING)

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

            sql_hash = self._hash_sql(
                sql_bytes, mode=str(ctx.output_format).encode())

            return dbstate.Query(
                sql=sql_bytes,
                sql_hash=sql_hash,
                in_type_id=in_type_id.bytes,
                in_type_data=in_type_data,
                out_type_id=out_type_id.bytes,
                out_type_data=out_type_data,
            )

        else:
            if ir.params:
                raise RuntimeError(
                    'queries compiled in script mode cannot accept parameters')

            return dbstate.SimpleQuery(sql=sql_bytes)

    def _compile_and_apply_delta_command(
            self, ctx: CompileContext, cmd) -> dbstate.BaseQuery:

        current_tx = ctx.state.current_tx()

        schema = current_tx.get_schema()
        context = s_delta.CommandContext()

        if isinstance(cmd, s_deltas.CreateDelta):
            delta = None
        else:
            delta = schema.get(cmd.classname)

        with context(s_deltas.DeltaCommandContext(schema, cmd, delta)):
            if isinstance(cmd, s_deltas.CommitDelta):
                ddl_plan = s_db.AlterDatabase()
                ddl_plan.update(delta.get_commands(schema))
                return self._compile_and_apply_ddl_command(ctx, ddl_plan)

            elif isinstance(cmd, s_deltas.GetDelta):
                delta_ql = s_ddl.ddl_text_from_delta(schema, delta)
                return self._compile_statement(
                    ctx=ctx,
                    eql=f'SELECT {ql_quote.quote_literal(delta_ql)};')

            elif isinstance(cmd, s_deltas.CreateDelta):
                schema, _ = cmd.apply(schema, context)
                current_tx.update_schema(schema)
                return dbstate.DDLQuery(sql=b'')

            else:
                raise RuntimeError(f'unexpected delta command: {cmd!r}')

    def _compile_and_apply_ddl_command(self, ctx: CompileContext, cmd):
        current_tx = ctx.state.current_tx()
        schema = current_tx.get_schema()

        if debug.flags.delta_plan_input:
            debug.header('Delta Plan Input')
            debug.dump(cmd)

        # Do a dry-run on test_schema to canonicalize
        # the schema delta-commands.
        test_schema = schema
        context = s_delta.CommandContext()
        cmd.apply(test_schema, context=context)

        # Apply and adapt delta, build native delta plan, which
        # will also update the schema.
        schema, plan = self._process_delta(cmd, schema)

        context = pg_delta.CommandContext(self.connection)

        if isinstance(plan, (s_db.CreateDatabase, s_db.DropDatabase)):
            block = pg_dbops.SQLBlock()
        else:
            block = pg_dbops.PLTopBlock()

        plan.generate(block)
        sql = block.to_string().encode('utf-8')

        current_tx.update_schema(schema)

        return dbstate.DDLQuery(sql=sql)

    def _compile_command(
            self, ctx: CompileContext, cmd) -> dbstate.BaseQuery:

        if isinstance(cmd, s_deltas.DeltaCommand):
            return self._compile_and_apply_delta_command(ctx, cmd)

        elif isinstance(cmd, s_delta.Command):
            return self._compile_and_apply_ddl_command(ctx, cmd)

        else:
            raise RuntimeError(f'unexpected plan {cmd!r}')

    def _compile_ql_ddl(self, ctx: CompileContext, ql: qlast.DDL):
        cmd = s_ddl.delta_from_ddl(
            ql,
            schema=ctx.schema,
            modaliases=ctx.sess_modaliases,
            testmode=bool(ctx.sess_modaliases.get('__internal_testmode')))

        return self._compile_command(cmd)

    def _compile_ql_migration(self, ctx: CompileContext,
                              ql: typing.Union[qlast.Database, qlast.Delta]):
        cmd = s_ddl.cmd_from_ddl(
            ql,
            schema=ctx.schema,
            modaliases=ctx.sess_modaliases,
            testmode=bool(ctx.sess_modaliases.get('__internal_testmode')))

        if (isinstance(ql, qlast.CreateDelta) and
                cmd.get_attribute_value('target')):

            cmd = s_ddl.compile_migration(
                cmd, self._get_std_schema(), ctx.schema)

        return self._compile_command(cmd)

    def _compile_ql_transaction(
            self, ctx: CompileContext,
            ql: qlast.Transaction) -> dbstate.Query:

        if isinstance(ql, qlast.StartTransaction):
            ctx.state.start_tx()
            sql = b'START TRANSACTION;'
            action = dbstate.TxAction.START

        elif isinstance(ql, qlast.CommitTransaction):
            ctx.state.commit_tx()
            sql = b'COMMIT;'
            action = dbstate.TxAction.COMMIT

        elif isinstance(ql, qlast.RollbackTransaction):
            ctx.state.rollback_tx()
            sql = b'ROLLBACK;'
            action = dbstate.TxAction.ROLLBACK

        else:
            raise ValueError(f'expecting transaction node, got {ql!r}')

        return dbstate.TxControlQuery(sql=sql, action=action)

    def _compile_ql_sess_state(self, ctx: CompileContext,
                               ql: qlast.SetSessionState):
        aliases = {}
        config_vals = {}

        for item in ql.items:
            if isinstance(item, qlast.SessionSettingModuleDecl):
                try:
                    module = ctx.schema.get(item.module)
                except s_err.ItemNotFoundError:
                    raise errors.EdgeQLError(
                        f'module {item.module!r} does not exist',
                        context=item.context
                    )

                aliases[item.alias] = module

            elif isinstance(item, qlast.SessionSettingConfigDecl):
                name = item.alias

                try:
                    desc = config.configs[name]
                except KeyError:
                    raise RuntimeError(
                        f'invalid SET expression: '
                        f'unknown CONFIG setting {name!r}')

                try:
                    val_ir = ql_compiler.compile_ast_fragment_to_ir(
                        item.expr, schema=ctx.schema)
                    val = ireval.evaluate_to_python_val(
                        val_ir, schema=ctx.schema)
                except ireval.StaticEvaluationError:
                    raise RuntimeError('invalid SET expression')
                else:
                    if not isinstance(val, desc.type):
                        dispname = val_ir.stype.get_displayname(
                            ctx.schema)
                        raise RuntimeError(
                            f'expected a {desc.type.__name__} value, '
                            f'got {dispname!r}')
                    else:
                        config_vals[name] = val

            else:
                raise RuntimeError(
                    f'unsupported SET command type {type(item)!r}')

        aliases = immutables.Map(aliases)
        config_vals = immutables.Map(config_vals)

        if aliases:
            ctx.state.update_modaliases(
                ctx.state.get_modaliases().update(aliases))
        if config_vals:
            ctx.state.update_config(
                ctx.state.get_config().update(config_vals))

        return dbstate.SessionStateQuery(
            session_set_modaliases=aliases,
            session_set_configs=config_vals)

    def _compile_dispatch_ql(self, ctx: CompileContext, ql: qlast.Base):

        if isinstance(ql, (qlast.Database, qlast.Delta)):
            return self._compile_ql_migration(ctx, ql)

        elif isinstance(ql, qlast.DDL):
            return self._compile_ql_ddl(ctx, ql)

        elif isinstance(ql, qlast.Transaction):
            return self._compile_ql_transaction(ctx, ql)

        elif isinstance(ql, qlast.SessionStateDecl):
            return self._compile_ql_sess_state(ctx, ql)

        else:
            return self._compile_ql_query(ctx, ql)

    def _compile(self, *,
                 ctx: CompileContext,
                 eql: str) -> typing.List[dbstate.QueryUnit]:

        statements = edgeql.parse_block(eql)

        if ctx.single_query_mode and len(statements) > 1:
            raise errors.ProtocolError(
                f'expected one statement, got {len(statements)}')

        units = []
        unit = None

        txid = None
        if not ctx.state.current_tx().is_implicit():
            txid = ctx.state.current_tx().id

        for stmt in statements:
            comp: dbstate.BaseQuery = self._compile_dispatch_ql(ctx, stmt)

            if unit is None:
                unit = dbstate.QueryUnit(txid=txid)

            if isinstance(comp, dbstate.Query):
                unit.sql += comp.sql

                if ctx.single_query_mode:
                    unit.sql_hash = comp.sql_hash

                    unit.out_type_data = comp.out_type_data
                    unit.out_type_id = comp.out_type_id
                    unit.in_type_data = comp.in_type_data
                    unit.in_type_id = comp.in_type_id

            elif isinstance(comp, dbstate.SimpleQuery):
                unit.sql += comp.sql

            elif isinstance(comp, dbstate.DDLQuery):
                unit.sql += comp.sql
                unit.has_ddl = True

            elif isinstance(comp, dbstate.TxControlQuery):
                unit.sql += comp.sql

                if comp.action == dbstate.TxAction.START:
                    unit.starts_tx = True
                    unit.txid = txid = ctx.state.current_tx().id
                else:
                    if comp.action == dbstate.TxAction.COMMIT:
                        unit.commits_tx = True
                    else:
                        unit.rollbacks_tx = True

                    units.append(unit)
                    unit = None

            elif isinstance(comp, dbstate.SessionStateQuery):
                unit.config = ctx.state.get_config()
                unit.modaliases = ctx.state.get_modaliases()

            else:
                raise RuntimeError('unknown compile state')

        if unit is not None:
            units.append(unit)

        return units

    def _ctx_new_con_state(self, schema, modaliases, config):
        self._current_db_state = dbstate.DatabaseState(
            schema, modaliases, config)
        return self._current_db_state

    def _ctx_from_con_state(self, txid: int):
        if (self._current_db_state is None or
                self._current_db_state.current_tx().id != txid):
            self._current_db_state = None
            raise RuntimeError(
                f'failed to lookup transaction with id={txid}')

        return self._current_db_state

    # API

    async def connect(self, dbname: str, dbver: int) -> CompilerDatabaseState:
        self._dbname = dbname
        self._cached_db = None
        await self._get_database(dbver)

    async def compile_eql(self, dbver: int,
                          eql: bytes,
                          sess_modaliases: immutables.Map,
                          sess_config: immutables.Map,
                          json_mode: bool) -> dbstate.CompiledQuery:
        pass

    async def compile_eql_in_tx(self, eql: bytes, txid: int):
        raise NotImplementedError

    async def compile_eql_script(self, dbver: int,
                                 eql: bytes,
                                 sess_modaliases: immutables.Map,
                                 sess_config: immutables.Map,
                                 json_mode: bool) -> dbstate.CompiledQuery:

        assert isinstance(sess_modaliases, immutables.Map)
        assert isinstance(sess_config, immutables.Map)

        eql = eql.decode()
        db = await self._get_database(dbver)

        state = self._new_con_state(db.schema, sess_modaliases, sess_config)

        if json_mode:
            of = pg_compiler.OutputFormat.JSON
        else:
            of = pg_compiler.OutputFormat.NATIVE

        ctx = CompileContext(
            dbver=dbver,
            state=state,
            output_format=of,
            single_query_mode=True)

        return self._compile_script(ctx=ctx, eql=eql)

    async def compile_eql_script_in_tx(self, eql: bytes, txid: int):
        raise NotImplementedError
