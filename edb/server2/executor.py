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
import time

from . import avg
from . import compilerpool
from . import pgpool
from . import state


class ExecutorPool:

    def __init__(self, *,
                 server,
                 concurrency: int,
                 pgaddr: str,
                 runstate_dir: str):

        self._server = server

        self._concurrency = concurrency
        self._pgaddr = pgaddr
        self._runstate_dir = runstate_dir
        self._exec_avg = avg.RollingAverage(state._QUERIES_ROLLING_AVG_LEN)

        self._executors = [Executor(self) for _ in range(concurrency)]
        self._cpool = None
        self._pgpool = None

    async def start(self):
        self._cpool = await compilerpool.create_pool(
            capacity=self._concurrency,
            runstate_dir=self._runstate_dir,
            connection_spec={
                'host': self._pgaddr
            })

        self._pgpool = pgpool.PGPool(
            loop=asyncio.get_running_loop(),
            max_capacity=math.ceil(self._concurrency * 1.5),
            concurrency=self._concurrency,
            pgaddr=self._pgaddr)

    async def stop(self):
        if self._cpool is not None:
            await self._cpool.stop()
            self._cpool = None

        if self._pgpool is not None:
            await self._pgpool.close()
            self._pgpool = None

    async def authorize(self, dbname: str, user: str, password: str):
        db = self._server._dbindex.get(dbname)
        if db is None:
            # XXX actually validate (user, password)
            holder = await self._pgpool.acquire(dbname)
            self._pgpool.release(holder)
            if self._server._dbindex.get(dbname) is None:
                # There can be a race between acquiring a connection
                # and registering a db.
                self._server._dbindex.register(dbname)

    async def parse(self, con, eql: str) -> state.Query:
        db = self._server._dbindex.get(con._dbname)
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

        return query

    async def execute(self, con, query: state.Query, bind_args: bytes):
        st = time.monotonic()
        holder = await self._pgpool.acquire(con._dbname)
        ah = time.monotonic()
        print(f'acquired after {ah-st:.3f}s')
        try:
            return await holder.connection.execute_anonymous(
                con, query.sql, bind_args)
        finally:
            print(f'query {time.monotonic() - ah:.3f}s')
            self._pgpool.release(holder)


class Executor:

    def __init__(self, epool):
        self._epool = epool
