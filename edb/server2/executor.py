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
import enum
import time

from . import avg
from . import compilerpool
from . import pgpool
from . import state


class AvgWeightedQueue:

    def __init__(self, *, loop):
        self._loop = loop

        self._slow = collections.deque()
        self._new = collections.deque()
        self._fast = collections.deque()

        self._fast_waiters = collections.deque()
        self._new_or_fast_waiters = collections.deque()
        self._slow_waiters = collections.deque()

        self._avg = avg.RollingAverage(state._QUERIES_ROLLING_AVG_LEN)
        self._cnt = 0

    def log_time(self, delta):
        self._avg.add(delta)

    def enqueue(self, avg, payload):
        self._cnt += 1
        i = self._cnt

        assert payload is not None

        if avg < 1e-10:
            # new item
            self._new.append((i, payload))
        elif avg > self._avg.avg * 1.4:
            # We only put payloads into the "slow" queue if their avg
            # is significantly greater than the current avg.
            # The reason for this is to avoid scenario when there is a
            # high number of queries with similar average and all executors
            # are busy processing them.  In this case, the avg will slowly
            # grow, making all queries being classified as "slow".
            self._slow.append((i, payload))
        else:
            self._fast.append((i, payload))

        self._wakeup_next()

    def _wakeup_fast(self):
        if self._fast_waiters and self._fast:
            fut = self._fast_waiters.popleft()
            assert not fut.done()
            fut.set_result(True)

    def _wakeup_next(self):
        if self._fast_waiters and self._fast:
            self._wakeup_fast()

        elif self._new_or_fast_waiters and (self._fast or self._new):
            fut = self._new_or_fast_waiters.popleft()
            assert not fut.done()
            fut.set_result(True)

        elif self._slow_waiters and (self._fast or self._new or self._slow):
            fut = self._slow_waiters.popleft()
            assert not fut.done()
            fut.set_result(True)

    def _pop_fast(self):
        if self._fast:
            return self._fast.popleft()[1]

    def _pop_new_or_fast(self):
        if not self._new:
            return self._pop_fast()

        if not self._fast:
            return self._new.popleft()[1]

        if self._new[0][0] > self._fast[0][0]:
            return self._fast.popleft()[1]
        else:
            return self._new.popleft()[1]

    def _pop_slow(self):
        if not self._slow:
            return self._pop_new_or_fast()

        slow_num = self._slow[0][0]
        if ((self._new and self._new[0][0] < slow_num) or
                (self._fast and self._fast[0][0] < slow_num)):
            return self._pop_new_or_fast()

        return self._slow.popleft()[1]

    async def pop_fast(self):
        item = self._pop_fast()
        while item is None:
            fut = self._loop.create_future()
            self._fast_waiters.append(fut)
            await fut
            item = self._pop_fast()
        return item

    async def pop_new_or_fast(self):
        item = self._pop_new_or_fast()
        while item is None:
            fut = self._loop.create_future()
            self._new_or_fast_waiters.append(fut)
            await fut
            item = self._pop_new_or_fast()
        return item

    async def pop_slow(self):
        item = self._pop_slow()
        while item is None:
            fut = self._loop.create_future()
            self._slow_waiters.append(fut)
            await fut
            item = self._pop_slow()
        return item


class CommandType(enum.Enum):

    PARSE = 1
    EXECUTE = 2


class ExecutorPool:

    def __init__(self, *,
                 loop,
                 server,
                 concurrency: int,
                 max_backend_connections: int,
                 pgaddr: str,
                 runstate_dir: str):

        self._server = server
        self._loop = loop

        self._concurrency = concurrency
        self._pgaddr = pgaddr
        self._runstate_dir = runstate_dir
        self._max_backend_connections = max_backend_connections

        self._queue = AvgWeightedQueue(loop=loop)

        slow_num = max(self._concurrency // 2, 1)
        new_num = max(self._concurrency // 4, 1)
        fast_num = max(self._concurrency - slow_num - new_num, new_num)

        # Initialize executor-workers: they process one-shot queries
        # (i.e. queries that don't need to be run in a transaction.)
        slow_execs = [SlowExecutorWorker(self) for _ in range(slow_num)]
        fast_execs = [FastExecutorWorker(self) for _ in range(fast_num)]
        new_or_fast_execs = [NewOrFastExecutorWorker(self)
                             for _ in range(new_num)]
        self._workers = fast_execs + new_or_fast_execs + slow_execs

        self._cons_in_workers = {}
        self._cons_in_tx = {}

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
            max_capacity=self._max_backend_connections,
            concurrency=self._concurrency,
            pgaddr=self._pgaddr)

        for e in self._workers:
            await e.start()

        async def foo():
            while True:
                await asyncio.sleep(8)
                print('====', self._queue._avg.avg)
                for e in self._workers:
                    print(e, e._num_queries)
        asyncio.create_task(foo())

    async def stop(self):
        if self._cpool is not None:
            await self._cpool.stop()
            self._cpool = None

        if self._pgpool is not None:
            await self._pgpool.close()
            self._pgpool = None

        for w in self._workers:
            await w.stop()

    async def authorize(self, dbname: str, user: str, password: str):
        db = self._server._dbindex.get(dbname)
        if db is None:
            # XXX (beta TODO) actually validate user and password!
            # Right now we only check that the DB actually exists
            # by acquiring a PG connection to it.
            holder = await self._pgpool.acquire(dbname)
            self._pgpool.release(holder)

            # There can be a race between acquiring a connection
            # and registering a DB, so check first.
            if self._server._dbindex.get(dbname) is None:
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

    def execute(self, con, query: state.Query, bind_args):
        self._queue.enqueue(query.avg, (con, query, bind_args))

        # st = time.monotonic()
        # holder = await self._pgpool.acquire(con._dbname)
        # ah = time.monotonic()
        # print(f'acquired after {ah-st:.3f}s')
        # try:
        #     return await holder.connection.execute_anonymous(
        #         con, query.sql, bind_args)
        # finally:
        #     print(f'query {time.monotonic() - ah:.3f}s')
        #     self._pgpool.release(holder)


class BaseExecutorWorker:

    def __init__(self, epool):
        self._epool = epool
        self._num_queries = 0
        self._run_task = None

    async def _get_work_item(self):
        raise NotImplementedError

    async def _run(self):
        while True:
            con, query, bind_args = await self._get_work_item()
            holder = await self._epool._pgpool.acquire(con._dbname)

            started_at = time.monotonic()
            try:
                await holder.connection.execute_anonymous(
                    con, query.compiled.sql, bind_args)
            finally:
                self._epool._pgpool.release(holder)

                con._on_server_execute_data()

                dur = time.monotonic() - started_at
                self._epool._queue.log_time(dur)
                query.log_time(dur)

                self._num_queries += 1

    async def start(self):
        self._run_task = self._epool._loop.create_task(self._run())

    async def stop(self):
        if self._run_task:
            rn = self._run_task
            self._run_task = None

            rn.cancel()
            try:
                await rn
            except asyncio.CancelledError:
                pass


class SlowExecutorWorker(BaseExecutorWorker):

    async def _get_work_item(self):
        return await self._epool._queue.pop_slow()


class FastExecutorWorker(BaseExecutorWorker):

    async def _get_work_item(self):
        return await self._epool._queue.pop_fast()


class NewOrFastExecutorWorker(BaseExecutorWorker):

    async def _get_work_item(self):
        return await self._epool._queue.pop_new_or_fast()
