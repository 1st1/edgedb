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


__all__ = 'create_pool',


import asyncio
import collections
import dataclasses
import struct
import uuid

import asyncpg

from edb.server import defines
from edb.server.pgsql import compiler as pg_compiler
from edb.server.pgsql import delta as delta_cmds
from edb.server.pgsql import intromech

from edb.lang import edgeql
from edb.lang.edgeql import compiler as ql_compiler
from edb.lang.schema import error as s_err
from edb.lang.schema import types as s_types

from . import procpool


@dataclasses.dataclass(frozen=True)
class Database:

    name: str
    con_args: dict
    type_cache: object
    schema: object


@dataclasses.dataclass(frozen=True)
class CompiledQuery:

    out_type_data: bytes
    out_type_id: bytes
    in_type_data: bytes
    in_type_id: bytes

    sql: bytes


_uint16_packer = struct.Struct('!H').pack
_uint8_packer = struct.Struct('!B').pack


class QueryResultsTypeSerializer:

    EDGE_POINTER_IS_IMPLICIT = 1 << 0
    EDGE_POINTER_IS_LINKPROP = 1 << 1

    def __init__(self, db):
        self.db = db
        self.buffer = []
        self.uuid_to_pos = {}

    def _get_type_id(self, objtype):
        try:
            return self.db.type_cache[objtype.name]
        except KeyError:
            pass

        msg = 'could not determine backend id for type in this context'
        details = 'ObjectType: {}'.format(objtype.name)
        raise s_err.SchemaError(msg, details=details)

    def _get_collection_type_id(self, coll_type, subtypes, element_names=None):
        subtypes = (f"{st}" for st in subtypes)
        string_id = f'{coll_type}\x00{":".join(subtypes)}'
        if element_names:
            string_id += f'\x00{":".join(element_names)}'
        return uuid.uuid5(delta_cmds.TYPE_ID_NAMESPACE, string_id)

    def _get_union_type_id(self, union_type):
        base_type_id = ','.join(str(self._get_type_id(c))
                                for c in union_type.children(self.db.schema))

        return uuid.uuid5(delta_cmds.TYPE_ID_NAMESPACE, base_type_id)

    def _get_set_type_id(self, basetype_id):
        return uuid.uuid5(delta_cmds.TYPE_ID_NAMESPACE,
                          'set-of::' + str(basetype_id))

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
        self.buffer.append(_uint16_packer(self.uuid_to_pos[type_id]))

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
                buf.append(_uint16_packer(len(subtypes)))
                for el_name, el_type in zip(element_names, subtypes):
                    el_name_bytes = el_name.encode('utf-8')
                    buf.append(_uint16_packer(len(el_name_bytes)))
                    buf.append(el_name_bytes)
                    buf.append(_uint16_packer(self.uuid_to_pos[el_type]))

            else:
                type_id = self._get_collection_type_id(t.schema_name, subtypes)

                if type_id in self.uuid_to_pos:
                    return type_id

                buf.append(b'\x04')
                buf.append(type_id.bytes)
                buf.append(_uint16_packer(len(subtypes)))
                for el_type in subtypes:
                    buf.append(_uint16_packer(self.uuid_to_pos[el_type]))

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
            buf.append(_uint16_packer(self.uuid_to_pos[subtypes[0]]))

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
            buf.append(_uint16_packer(len(subtypes)))

            for el_name, el_type, el_lp in zip(element_names,
                                               subtypes, link_props):
                flags = 0
                if el_lp:
                    flags |= self.EDGE_POINTER_IS_LINKPROP
                buf.append(_uint8_packer(flags))

                el_name_bytes = el_name.encode('utf-8')
                buf.append(_uint16_packer(len(el_name_bytes)))
                buf.append(el_name_bytes)
                buf.append(_uint16_packer(self.uuid_to_pos[el_type]))

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
                buf.append(_uint16_packer(self.uuid_to_pos[bt_id]))

                self._register_type_id(type_id)
                return type_id

    @classmethod
    def describe(cls, db, typ, view_shapes):
        builder = cls(db)
        type_id = builder._describe_type(typ, view_shapes)
        return b''.join(builder.buffer), type_id


class Compiler:

    def __init__(self, connect_args):
        self._connect_args = connect_args
        self._databases = {}

    async def _get_database(self, dbname: str):
        try:
            return self._databases[dbname]
        except KeyError:
            pass

        con_args = self._connect_args.copy()
        con_args['user'] = defines.EDGEDB_SUPERUSER
        con_args['database'] = dbname

        con = await asyncpg.connect(**con_args)
        try:
            im = intromech.IntrospectionMech(con)
            schema = await im.getschema()
            db = Database(
                name=dbname,
                con_args=con_args,
                type_cache=im.type_cache,
                schema=schema)

            self._databases[dbname] = db
            return db
        finally:
            asyncio.create_task(con.close())

    def _compile(self, db, eql):
        statements = edgeql.parse_block(eql)
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

        in_type_data, in_type_id = QueryResultsTypeSerializer.describe(
            db, params_type, {})

        return CompiledQuery(
            out_type_data, out_type_id.bytes,
            in_type_data, in_type_id.bytes,
            sql_text.encode(defines.EDGEDB_ENCODING))

    async def compile_edgeql(self, dbname, eql):
        db = await self._get_database(dbname)
        return self._compile(db, eql)


async def create_pool(*, capacity: int,
                      runstate_dir: str,
                      connection_spec: dict):

    return await procpool.create_pool(
        capacity=capacity,
        runstate_dir=runstate_dir,
        name='edgedb-compiler',
        worker_cls=Compiler,
        worker_args=(connection_spec,))
