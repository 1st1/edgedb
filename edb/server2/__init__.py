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
import struct
import typing

from edb.lang import edgeql
from edb.lang.edgeql import compiler as ql_compiler
from edb.server.pgsql import intromech

from . import coreserver as core
from . import compilerpool

import uuid
from edb.server.pgsql import delta as delta_cmds
from edb.server.pgsql import compiler as pg_compiler
from edb.lang.schema import objects as s_obj
from edb.lang.schema import scalars as s_scalars
from edb.lang.schema import types as s_types


class PGConParams(typing.NamedTuple):
    user: str
    password: str
    database: str


class Interface:

    def __init__(self, server, host, port):
        self.server = server
        self.host = host
        self.port = port

    def make_protocol(self):
        raise NotImplementedError


class BinaryInterface(Interface):

    def make_protocol(self):
        return core.EdgeConnection(self.server)


class Database:

    def __init__(self, name, intromech, schema, addr):
        self.name = name
        self.intromech = intromech
        self.schema = schema
        self.addr = addr


class Query:

    def __init__(self, dbname,
                 out_type_data, out_type_id,
                 in_type_data, in_type_id,
                 edgeql, sql):
        self.dbname = dbname
        self.out_type_data = out_type_data
        self.out_type_id = out_type_id
        self.in_type_data = in_type_data
        self.in_type_id = in_type_id
        self.edgeql = edgeql
        self.sql = sql


class QueryResultsTypeSerializer:

    EDGE_POINTER_IS_IMPLICIT = 1 << 0
    EDGE_POINTER_IS_LINKPROP = 1 << 1

    def __init__(self, intromech):
        self.intromech = intromech
        self.buffer = []
        self.uuid_to_pos = {}

    def _get_type_id(self, objtype):
        return self.intromech.get_type_id(objtype)

    def _get_collection_type_id(self, coll_type, subtypes, element_names=None):
        subtypes = (f"{st}" for st in subtypes)
        string_id = f'{coll_type}\x00{":".join(subtypes)}'
        if element_names:
            string_id += f'\x00{":".join(element_names)}'
        return uuid.uuid5(delta_cmds.TYPE_ID_NAMESPACE, string_id)

    def _get_union_type_id(self, union_type):
        base_type_id = ','.join(str(self._get_type_id(c))
                                for c in union_type.children(self.schema))

        return uuid.uuid5(delta_cmds.TYPE_ID_NAMESPACE, base_type_id)

    def _get_set_type_id(self, basetype_id):
        return uuid.uuid5(delta_cmds.TYPE_ID_NAMESPACE,
                          'set-of::' + str(basetype_id))

    def _pack_uint16(self, i):
        return struct.pack('!H', i)

    def _pack_uint8(self, i):
        return struct.pack('!B', i)

    def _register_type_id(self, type_id):
        if type_id not in self.uuid_to_pos:
            self.uuid_to_pos[type_id] = len(self.uuid_to_pos)

    def _describe_set(self, t, view_shapes):
        type_id = self._describe_type(t, view_shapes)
        set_id = self._get_set_type_id(type_id)
        if set_id in self.uuid_to_pos:
            return set_id

        self.buffer.append(b'\x00')
        self.buffer.append(set_id.bytes)
        self.buffer.append(self._pack_uint16(self.uuid_to_pos[type_id]))

        self._register_type_id(set_id)
        return set_id

    def _describe_type(self, t, view_shapes):
        # the encoding format is documented in edb/api/types.txt.

        buf = self.buffer

        if isinstance(t, s_types.Tuple):
            subtypes = [self._describe_type(st, view_shapes)
                        for st in t.get_subtypes()]

            if t.named:
                element_names = list(t.element_types)
                assert len(element_names) == len(subtypes)

                type_id = self._get_collection_type_id(
                    t.schema_name, subtypes, element_names)

                if type_id in self.uuid_to_pos:
                    return type_id

                buf.append(b'\x05')
                buf.append(type_id.bytes)
                buf.append(self._pack_uint16(len(subtypes)))
                for el_name, el_type in zip(element_names, subtypes):
                    el_name_bytes = el_name.encode('utf-8')
                    buf.append(self._pack_uint16(len(el_name_bytes)))
                    buf.append(el_name_bytes)
                    buf.append(self._pack_uint16(self.uuid_to_pos[el_type]))

            else:
                type_id = self._get_collection_type_id(t.schema_name, subtypes)

                if type_id in self.uuid_to_pos:
                    return type_id

                buf.append(b'\x04')
                buf.append(type_id.bytes)
                buf.append(self._pack_uint16(len(subtypes)))
                for el_type in subtypes:
                    buf.append(self._pack_uint16(self.uuid_to_pos[el_type]))

            self._register_type_id(type_id)
            return type_id

        elif isinstance(t, s_types.Array):
            subtypes = [self._describe_type(st, view_shapes)
                        for st in t.get_subtypes()]

            assert len(subtypes) == 1
            type_id = self._get_collection_type_id(t.schema_name, subtypes)

            if type_id in self.uuid_to_pos:
                return type_id

            buf.append(b'\x06')
            buf.append(type_id.bytes)
            buf.append(self._pack_uint16(self.uuid_to_pos[subtypes[0]]))

            self._register_type_id(type_id)
            return type_id

        elif isinstance(t, s_types.Collection):
            raise RuntimeError(f'unsupported collection type {t!r}')

        elif view_shapes.get(t):
            # This is a view
            mt = t.material_type()
            if mt.is_virtual:
                base_type_id = self._get_union_type_id(mt)
            else:
                base_type_id = self._get_type_id(mt)

            subtypes = []
            element_names = []
            link_props = []

            for ptr in view_shapes[t]:
                if ptr.singular():
                    subtype_id = self._describe_type(ptr.target, view_shapes)
                else:
                    subtype_id = self._describe_set(ptr.target, view_shapes)
                subtypes.append(subtype_id)
                element_names.append(ptr.shortname.name)
                link_props.append(False)

            if t.rptr is not None:
                # There are link properties in the mix
                for ptr in view_shapes[t.rptr]:
                    if ptr.singular():
                        subtype_id = self._describe_type(
                            ptr.target, view_shapes)
                    else:
                        subtype_id = self._describe_set(
                            ptr.target, view_shapes)
                    subtypes.append(subtype_id)
                    element_names.append(ptr.shortname.name)
                    link_props.append(True)

            type_id = self._get_collection_type_id(
                base_type_id, subtypes, element_names)

            if type_id in self.uuid_to_pos:
                return type_id

            buf.append(b'\x01')
            buf.append(type_id.bytes)

            assert len(subtypes) == len(element_names)
            buf.append(self._pack_uint16(len(subtypes)))

            for el_name, el_type, el_lp in zip(element_names,
                                               subtypes, link_props):
                flags = 0
                if el_lp:
                    flags |= self.EDGE_POINTER_IS_LINKPROP
                buf.append(self._pack_uint8(flags))

                el_name_bytes = el_name.encode('utf-8')
                buf.append(self._pack_uint16(len(el_name_bytes)))
                buf.append(el_name_bytes)
                buf.append(self._pack_uint16(self.uuid_to_pos[el_type]))

            self._register_type_id(type_id)
            return type_id

        else:
            # This is a scalar type

            mt = t.material_type()
            base_type = mt.get_topmost_concrete_base()

            if mt is base_type:
                type_id = self._get_type_id(mt)
                if type_id in self.uuid_to_pos:
                    # already described
                    return type_id

                buf.append(b'\x02')
                buf.append(type_id.bytes)

                self._register_type_id(type_id)
                return type_id

            else:
                bt_id = self._describe_type(base_type, view_shapes)
                type_id = self._get_type_id(mt)

                if type_id in self.uuid_to_pos:
                    return type_id

                buf.append(b'\x03')
                buf.append(type_id.bytes)
                buf.append(self._pack_uint16(self.uuid_to_pos[bt_id]))

                self._register_type_id(type_id)
                return type_id

    @classmethod
    def describe(cls, intromech, typ, view_shapes):
        builder = cls(intromech)
        type_id = builder._describe_type(typ, view_shapes)
        return b''.join(builder.buffer), type_id


class Server(core.CoreServer):

    def __init__(self, loop, cluster, runstate_dir):
        super().__init__(loop)
        self._serving = False
        self._interfaces = []
        self._servers = []
        self._cluster = cluster
        self._runstate_dir = runstate_dir
        self._cpool = None

        self._edgecon_id = 0

        self._databases = {}

    def add_binary_interface(self, host, port):
        self._add_interface(BinaryInterface(self, host, port))

    def new_edgecon_id(self):
        self._edgecon_id += 1
        return str(self._edgecon_id)

    def _add_interface(self, iface: Interface):
        if self._serving:
            raise RuntimeError(
                'cannot add new interfaces after start_serving() call')
        self._interfaces.append(iface)

    async def _load_database(self, user, dbname):
        # TODO: Serialize loads

        con = await self._cluster.connect(
            database=dbname, user=user, loop=self._loop)
        try:
            im = intromech.IntrospectionMech(con)
            schema = await im.getschema()
            self._databases[dbname] = Database(dbname, im, schema, con._addr)
        finally:
            await con.close()

    async def _authorize(self, user, password, dbname, callback):
        if dbname in self._databases:
            self._loop.call_soon(callback, None)
            return

        try:
            await self._load_database(user, dbname)
        except Exception as ex:
            callback(ex)
        else:
            callback(None)

    async def compile(self, dbname, query):
        db = self._databases[dbname]

        statements = edgeql.parse_block(query)
        if len(statements) != 1:
            raise RuntimeError(
                f'expected one statement, got {len(statements)}')
        stmt = statements[0]

        ir = ql_compiler.compile_ast_to_ir(
            stmt,
            schema=db.schema,
            modaliases={None: 'default'},
            implicit_id_in_shapes=False)

        out_type_data, out_type_id = QueryResultsTypeSerializer.describe(
            db.intromech, ir.expr.scls, ir.view_shapes)

        sql_text, argmap = pg_compiler.compile_ir_to_sql(
            ir,
            schema=db.schema,
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

        in_type_data, in_type_id = QueryResultsTypeSerializer.describe(
            db.intromech, params_type, {})

        return Query(
            dbname,
            out_type_data, out_type_id,
            in_type_data, in_type_id,
            query, sql_text)

    async def _parse(self, dbname, stmt_name, query, callback):
        try:
            q = await self.compile(dbname, query)
        except Exception as ex:
            callback(None, None, ex)
        else:
            callback(stmt_name, q, None)

    async def _execute(self, con, query, bind_args: bytes):
        ca = self._cluster.get_connection_spec()
        host = ca.get('host', '')
        port = ca.get('port', '')
        p = PGConParams(con.get_user(), '', con.get_dbname())

        db = self._databases[con.get_dbname()]

        con_fut = self._loop.create_future()

        tr, pr = await self._loop.create_unix_connection(
            lambda: core.PGProto(f'{host}:{port}', con_fut, p, self._loop),
            db.addr)

        try:
            await con_fut

            data = await pr.execute_anonymous(query.sql, bind_args, None)
            con._on_server_execute_data(data)
        finally:
            tr.abort()

    async def _simple_query(self, con, script):
        ca = self._cluster.get_connection_spec()
        host = ca.get('host', '')
        port = ca.get('port', '')
        p = PGConParams(con.get_user(), '', con.get_dbname())

        db = self._databases[con.get_dbname()]

        con_fut = self._loop.create_future()

        tr, pr = await self._loop.create_unix_connection(
            lambda: core.PGProto(f'{host}:{port}', con_fut, p, self._loop),
            db.addr)

        try:
            await con_fut

            data = await pr.simple_query(script, None)
            con._on_server_simple_query(data)
        finally:
            tr.abort()

    async def start(self):
        if self._serving:
            raise RuntimeError('already serving')
        self._serving = True

        self._cpool = await compilerpool.create_pool(
            capacity=10, runstate_dir=self._runstate_dir)

        for iface in self._interfaces:
            srv = await self._loop.create_server(
                iface.make_protocol, host=iface.host, port=iface.port)
            self._servers.append(srv)

    async def stop(self):
        await self._cpool.stop()
