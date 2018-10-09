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

from . import compilerpool
from . import dbstate
from . import edgecon
from . import pgpool


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
        return edgecon.EdgeConnection(
            self.server,
            self.server._loop,
            self.server._cpool,
            self.server._pgpool,
            self.server._dbindex)


class Server:

    def __init__(self, *, loop, cluster, runstate_dir,
                 concurrency, max_backend_connections):

        self._loop = loop

        self._serving = False
        self._interfaces = []
        self._servers = []
        self._cluster = cluster

        self._dbindex = dbstate.DatabaseIndex()

        self._runstate_dir = runstate_dir
        self._concurrency = concurrency
        self._max_backend_connections = max_backend_connections

        self._edgecon_id = 0

        self._cpool = None
        self._pgpool = None

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

    async def start(self):
        if self._serving:
            raise RuntimeError('already serving')
        self._serving = True

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

        pgaddr = os.path.join(host, f'.s.PGSQL.{port}')

        self._cpool = await compilerpool.create_pool(
            capacity=self._concurrency + 1,
            runstate_dir=self._runstate_dir,
            connection_spec={
                'host': pgaddr
            })

        self._pgpool = pgpool.PGPool(
            loop=asyncio.get_running_loop(),
            max_capacity=self._max_backend_connections,
            concurrency=self._concurrency,
            pgaddr=pgaddr)

        for iface in self._interfaces:
            srv = await self._loop.create_server(
                iface.make_protocol, host=iface.host, port=iface.port)
            self._servers.append(srv)

    async def stop(self):
        if self._cpool is not None:
            await self._cpool.stop()
            self._cpool = None

        if self._pgpool is not None:
            await self._pgpool.close()
            self._pgpool = None
