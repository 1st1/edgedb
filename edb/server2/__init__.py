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
import math
import os
import typing

from . import coreserver as core
from . import compilerpool
from . import pgpool
from . import state


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

    async def _authorize(self, user, password, dbname, callback):
        try:
            db = self._dbindex.get(dbname, user)
            if db is None:
                holder = await self._pgpool.acquire(dbname, user, password)
                self._pgpool.release(holder)
                if self._dbindex.get(dbname, user) is None:
                    # There can be a race between acquiring a connection
                    # and registering a db.
                    self._dbindex.register(dbname, user)
        except Exception as ex:
            callback(ex)
        else:
            callback(None)

    async def _parse(self, con, stmt_name, eql, callback):
        try:
            db = self._dbindex.get(con._dbname, con._user)
            query = db.lookup_query(eql)
            if query is None:
                compiler = await self._cpool.acquire()
                try:
                    query = db.lookup_query(eql)
                    if query is None:
                        compiled_query = await compiler.call(
                            'compile_edgeql', con._dbname, eql)
                        query = db.add_query(eql, compiled_query)
                finally:
                    self._cpool.release(compiler)
        except Exception as ex:
            callback(None, None, ex)
        else:
            callback(stmt_name, query.compiled, None)

    async def _execute(self, con, query, bind_args: bytes):
        holder = await self._pgpool.acquire(
            con._dbname, con._user, con._password)
        try:
            data = await holder.connection.execute_anonymous(
                query.sql, bind_args)
            con._on_server_execute_data(data)
        finally:
            self._pgpool.release(holder)

    async def _simple_query(self, con, script):

        ca = self._cluster.get_connection_spec()
        host = ca.get('host', '')
        port = ca.get('port', '')
        p = PGConParams(con.get_user(), '', con.get_dbname())

        addr = os.path.join(host, f'.s.PGSQL.{port}')

        con_fut = self._loop.create_future()

        tr, pr = await self._loop.create_unix_connection(
            lambda: core.PGProto(f'{host}:{port}', con_fut, p, self._loop),
            addr)

        try:
            await con_fut

            data = await pr.simple_query(script.encode(), None)
            con._on_server_simple_query(data)
        finally:
            tr.abort()

    async def start(self):
        if self._serving:
            raise RuntimeError('already serving')
        self._serving = True

        concurrency = 4

        self._dbindex = state.DatabasesIndex()

        self._cpool = await compilerpool.create_pool(
            capacity=concurrency,
            runstate_dir=self._runstate_dir,
            connection_spec=self._cluster.get_connection_spec())

        ca = self._cluster.get_connection_spec()
        self._pgpool = pgpool.PGPool(
            loop=asyncio.get_running_loop(),
            max_capacity=math.ceil(concurrency * 1.5),
            concurrency=concurrency,
            pgaddr=os.path.join(ca.get("host"), f'.s.PGSQL.{ca.get("port")}'))

        for iface in self._interfaces:
            srv = await self._loop.create_server(
                iface.make_protocol, host=iface.host, port=iface.port)
            self._servers.append(srv)

    async def stop(self):
        await self._cpool.stop()
        self._cpool = None
