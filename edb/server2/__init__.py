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
import os
import typing
import urllib.parse

from . import coreserver as core
from . import executor
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

        self._epool = None
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
            await self._epool.authorize(dbname, user, password)
        except Exception as ex:
            callback(ex)
        else:
            callback(None)

    def _parse_no_wait(self, con, eql):
        db = self._dbindex.get(con._dbname)
        query = db.lookup_query(eql)
        if query:
            return query

    async def _parse(self, con, stmt_name, eql, callback):
        try:
            query = await self._epool.parse(con, eql)
        except Exception as ex:
            callback(None, None, ex)
        else:
            callback(stmt_name, query, None)

    async def _execute(self, con, query, bind_args):
        self._epool.execute(con, query, bind_args)

    # async def _simple_query(self, con, script):

    #     ca = self._cluster.get_connection_spec()
    #     host = ca.get('host', '')
    #     port = ca.get('port', '')
    #     p = PGConParams(con.get_user(), '', con.get_dbname())

    #     addr = os.path.join(host, f'.s.PGSQL.{port}')

    #     con_fut = self._loop.create_future()

    #     tr, pr = await self._loop.create_unix_connection(
    #         lambda: core.PGProto(f'{host}:{port}', con_fut, p, self._loop),
    #         addr)

    #     try:
    #         await con_fut

    #         data = await pr.simple_query(script.encode(), None)
    #         con._on_server_simple_query(data)
    #     finally:
    #         tr.abort()

    async def start(self):
        if self._serving:
            raise RuntimeError('already serving')
        self._serving = True

        concurrency = 4

        self._dbindex = state.DatabasesIndex()

        pg_con_spec = self._cluster.get_connection_spec()
        if 'host' not in pg_con_spec and 'dsn' in pg_con_spec:
            # XXX
            parsed = urllib.parse.urlparse(pg_con_spec['dsn'])
            query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
            host = query.get("host")[-1]
            port = query.get("port")[-1]
        else:
            host = pg_con_spec.get("host")
            port = pg_con_spec.get("port")

        loop = asyncio.get_running_loop()

        pgaddr = os.path.join(host, f'.s.PGSQL.{port}')
        self._epool = executor.ExecutorPool(loop=loop,
                                            server=self,
                                            concurrency=concurrency,
                                            runstate_dir=self._runstate_dir,
                                            pgaddr=pgaddr)

        await self._epool.start()

        for iface in self._interfaces:
            srv = await self._loop.create_server(
                iface.make_protocol, host=iface.host, port=iface.port)
            self._servers.append(srv)

    async def stop(self):
        if self._epool is not None:
            await self._epool.stop()
            self._epool = None
