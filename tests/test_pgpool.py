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
import dataclasses
import random
import time

from edb.server2 import pgpool
from edb.server2 import taskgroup
from edb.server import _testbase as tb


@dataclasses.dataclass(frozen=True)
class Named:
    name: str


class TestExc(Exception):
    pass


class TestLRUIndex(tb.TestCase):

    def test_lru_index_1(self):
        idx = pgpool.LRUIndex()

        idx.append(1, '1')
        idx.append(2, '2')
        idx.append(1, '11')
        idx.append(2, '22')
        idx.append(1, '111')

        self.assertEqual(idx.count_keys(), 2)

        self.assertEqual(idx.pop(1), '111')
        self.assertEqual(idx.pop(1), '11')
        self.assertEqual(idx.pop(2), '22')

        idx.append(1, '11')
        self.assertEqual(idx.pop(1), '11')
        self.assertEqual(idx.pop(2), '2')
        self.assertEqual(idx.count_keys(), 1)
        self.assertEqual(idx.pop(1), '1')
        self.assertEqual(idx.count_keys(), 0)

        self.assertIsNone(idx.pop(2))
        self.assertIsNone(idx.pop(1))

        self.assertEqual(idx.count_keys(), 0)

    def test_lru_index_2(self):
        idx = pgpool.LRUIndex()

        idx.append(1, '1')
        idx.append(2, '2')
        idx.append(1, '11')
        idx.append(2, '22')
        idx.append(1, '111')

        self.assertTrue(idx.discard('11'))
        self.assertFalse(idx.discard('11'))
        self.assertFalse(idx.discard('11'))

        self.assertEqual(idx.pop(1), '111')
        self.assertEqual(idx.pop(2), '22')

        idx.append(1, '11')
        self.assertEqual(idx.pop(1), '11')
        self.assertEqual(idx.pop(2), '2')
        self.assertEqual(idx.pop(1), '1')

        self.assertIsNone(idx.pop(2))
        self.assertIsNone(idx.pop(1))

        self.assertEqual(idx.count_keys(), 0)

    def test_lru_index_3(self):
        idx = pgpool.LRUIndex()

        o1 = Named('o1')
        o11 = Named('o11')

        idx.append(1, o1)
        idx.append(1, o11)

        with self.assertRaisesRegex(ValueError, 'already in the index'):
            idx.append(1, o1)

        self.assertTrue(idx.discard(o1))
        self.assertFalse(idx.discard(o1))

        idx.append(1, o1)
        self.assertIs(idx.pop(1), o1)
        self.assertIs(idx.pop(1), o11)

        self.assertEqual(idx.count_keys(), 0)

        self.assertFalse(idx.discard(o1))

    def test_lru_index_4(self):
        idx = pgpool.LRUIndex()

        o1 = Named('o1')
        o11 = Named('o11')
        o111 = Named('o111')
        o2 = Named('o2')

        idx.append(1, o1)
        idx.append(1, o11)
        idx.append(1, o111)
        idx.append(2, o2)

        self.assertEqual(list(idx.lru()), [o1, o11, o111, o2])
        self.assertEqual(idx.count(), 4)

        self.assertIs(idx.pop(1), o111)
        self.assertIs(idx.pop(1), o11)
        self.assertIs(idx.pop(2), o2)
        self.assertEqual(list(idx.lru()), [o1])
        self.assertEqual(idx.count(), 1)

        idx.append(1, o111)
        idx.append(1, o11)

        self.assertEqual(list(idx.lru()), [o1, o111, o11])
        self.assertEqual(idx.count(), 3)

        idx.append(2, o2)

        self.assertIs(idx.pop(1), o11)
        self.assertEqual(list(idx.lru()), [o1, o111, o2])
        self.assertEqual(idx.count(), 3)

        idx.discard(o111)
        self.assertEqual(list(idx.lru()), [o1, o2])
        self.assertEqual(idx.count(), 2)

        self.assertIs(idx.popleft(), o1)
        self.assertEqual(list(idx.lru()), [o2])
        self.assertEqual(idx.count(), 1)

        self.assertIs(idx.pop(2), o2)
        self.assertEqual(list(idx.lru()), [])
        self.assertEqual(idx.count(), 0)


class TestMappedDeque(tb.TestCase):

    def test_mapped_deque_1(self):
        lst = pgpool.MappedDeque()

        o1 = Named('o1')
        o2 = Named('o2')
        o3 = Named('o3')
        o4 = Named('o4')

        lst.append(o1)
        lst.append(o2)
        lst.append(o3)
        lst.append(o4)

        self.assertEqual(list(lst), [o1, o2, o3, o4])

        lst.discard(o2)
        self.assertEqual(list(lst), [o1, o3, o4])

        self.assertIn(o1, lst)
        self.assertNotIn(o2, lst)

        self.assertEqual(lst.popleftitem(), (o1, None))
        self.assertEqual(list(lst), [o3, o4])

        with self.assertRaisesRegex(ValueError, 'already in the list'):
            lst.append(o3)

        lst.append(o1)
        self.assertEqual(list(lst), [o3, o4, o1])

        with self.assertRaises(LookupError):
            lst.discard(o2)

        self.assertEqual(len(lst), 3)
        self.assertTrue(bool(lst))

        self.assertIs(lst.popleft(), o3)
        self.assertIs(lst.pop(), o1)
        self.assertIs(lst.pop(), o4)

        self.assertEqual(list(lst), [])
        self.assertEqual(len(lst), 0)
        self.assertFalse(bool(lst))

        with self.assertRaises(KeyError):
            lst.pop()
        with self.assertRaises(KeyError):
            lst.popleft()

    def test_mapped_deque_2(self):
        orig = [1, 2, 3]
        lst = pgpool.MappedDeque(orig)
        self.assertEqual(list(lst), [1, 2, 3])
        orig.pop()
        self.assertEqual(list(lst), [1, 2, 3])

    def test_mapped_deque_3(self):
        lst = pgpool.MappedDeque()
        lst.append(1, '1')
        self.assertEqual(lst[1], '1')


class TestBasePool(tb.TestCase):

    async def run_monkey_test(self, *, max_capacity, concurrency, dbs, plan):
        """Simulate load.

        *max_capacity* is the max capacity of the pool.

        *concurrency* is the concurrency set for the pool.

        *dbs* is a list of different databases that we'll be simulating
        access to, e.g.:

            dbs = [
                ('db1', 'user1', 'pw'),
                ('db2', 'user1', 'pw')
            ]

        *plan* is how to form the queue of acquire calls. It's a list
        of `(number_of_calls, mode)`, where mode is a float between 0 and 1
        setting the probability for picking a random db from the *dbs* list.

            plan = [(100, 0.1), (50, 0.8)]

        means that the queue will have first 100 calls with
        0.9 probability of connecting to the first DB in the *dbs* list;
        followed by 50 calls with 0.8 probability of connecting to the
        second DB in the *dbs* list.

        Return a pair of a list with connection counts for each DB in *dbs*
        (e.g. [10, 20] means that there were 10 connections to dbs[0] and
        20 to dbs[1]).
        """

        connect_calls = 0
        close_calls = 0
        connects = [0] * len(dbs)

        connect_total_time = 0
        close_total_time = 0
        use_total_time = 0

        MAX_CON_TIME = 0.1
        MAX_CLOSE_TIME = 0.05
        MAX_TASK_TIME = 0.1

        class TestHolder(pgpool.BaseConnectionHolder):

            async def _connect(self, dbname, user, password):
                nonlocal connect_calls, connect_total_time
                connect_calls += 1

                connects[dbs.index((dbname, user, password))] += 1

                d = random.uniform(0.01, MAX_CON_TIME)
                connect_total_time += d

                await asyncio.sleep(d)
                return True

            async def _close(self, con):
                nonlocal close_calls, close_total_time
                close_calls += 1

                d = random.uniform(0.01, MAX_CLOSE_TIME)
                close_total_time += d

                await asyncio.sleep(d)
                assert con

        class TestPool(pgpool.BasePool):

            def _new_holder(self):
                return TestHolder(self)

        async def task(pool, db, user, pw):
            nonlocal use_total_time
            con = await pool.acquire(db, user, pw)
            self.assertEqual(con.key(), (db, user))
            try:
                d = random.uniform(0.04, MAX_TASK_TIME)
                use_total_time += d
                await asyncio.sleep(d)
            finally:
                pool.release(con)

        def randin(low, high, med):
            med = med * (high - low)
            return round(random.triangular(low, high, med))

        loop = asyncio.get_running_loop()
        pool = TestPool(max_capacity=max_capacity,
                        concurrency=concurrency, loop=loop)

        queue = []
        for num, median in plan:
            queue.extend([dbs[randin(0, len(dbs) - 1, median)]
                          for _ in range(num)])

        started_at = time.monotonic()
        async with taskgroup.TaskGroup() as g:
            for q in queue:
                g.create_task(task(pool, *q))
        dur = time.monotonic() - started_at

        linear_time = connect_total_time + close_total_time + use_total_time
        self.assertLess(dur, (linear_time / concurrency) * 1.25)

        await pool.close()

        self.assertEqual(connect_calls, close_calls)

        return connects

    async def test_base_pgpool_monkey_1(self):
        dbs = [
            ('db1', 'user1', 'pw'),
            ('db2', 'user1', 'pw'),
        ]

        plan = [(100, 0.2), (100, 0.8)]

        connects = await self.run_monkey_test(
            max_capacity=8, concurrency=4, plan=plan, dbs=dbs)

        # pool is big enough to keep all 4 connections alive for both DBs
        self.assertEqual(connects, [4, 4])

    async def test_base_pgpool_monkey_2(self):
        dbs = [
            ('db1', 'user1', 'pw'),
            ('db2', 'user1', 'pw'),
        ]

        plan = [(100, 0.5)]

        connects = await self.run_monkey_test(
            max_capacity=5, concurrency=4, plan=plan, dbs=dbs)

        self.assertGreater(connects[0], 4)
        self.assertGreater(connects[1], 4)

    async def test_base_pgpool_monkey_3(self):
        dbs = [
            ('db1', 'user1', 'pw'),
            ('db2', 'user1', 'pw'),
            ('db3', 'user1', 'pw'),
            ('db4', 'user1', 'pw'),
            ('db5', 'user1', 'pw'),
            ('db6', 'user1', 'pw'),
            ('db7', 'user1', 'pw'),
            ('db8', 'user1', 'pw'),
        ]

        plan = [(30, 0.1), (30, 0.8), (30, 0.2), (30, 0.6), (30, 1.0)]

        await self.run_monkey_test(
            max_capacity=100, concurrency=4, plan=plan, dbs=dbs)

        await self.run_monkey_test(
            max_capacity=11, concurrency=10, plan=plan, dbs=dbs)

        plan = [(50, 0.1)]
        await self.run_monkey_test(
            max_capacity=11, concurrency=10, plan=plan, dbs=dbs)

    async def test_base_pgpool_monkey_4(self):
        dbs = [
            ('db1', 'user1', 'pw'),
            ('db2', 'user1', 'pw'),
        ]

        plan = [(50, 0.5)]

        connects = await self.run_monkey_test(
            max_capacity=2, concurrency=1, plan=plan, dbs=dbs)
        self.assertEqual(connects, [1, 1])

    async def test_base_pgpool_1(self):
        class TestHolder(pgpool.BaseConnectionHolder):

            async def _connect(self, dbname, user, password):
                d = random.uniform(0.01, 0.1)
                await asyncio.sleep(d)
                if dbname == 'error':
                    raise TestExc
                if dbname != 'nodb':
                    return True

            async def _close(self, con):
                d = random.uniform(0.01, 0.1)
                await asyncio.sleep(d)
                assert con

        class TestPool(pgpool.BasePool):

            def _new_holder(self):
                return TestHolder(self)

        loop = asyncio.get_running_loop()
        pool = TestPool(max_capacity=2, concurrency=1, loop=loop)

        c1 = asyncio.create_task(pool.acquire('any', 'any', 'any'))
        c2 = asyncio.create_task(pool.acquire('error', 'error', 'error'))

        con = await c1

        self.assertEqual(pool.used_holders_count, 1)
        self.assertEqual(pool.empty_holders_count, 1)
        self.assertEqual(pool.unused_holders_count, 0)

        await asyncio.sleep(0.1)
        pool.release(con)

        self.assertEqual(pool.used_holders_count, 0)
        self.assertEqual(pool.empty_holders_count, 1)
        self.assertEqual(pool.unused_holders_count, 1)

        with self.assertRaisesRegex(RuntimeError, 'not previously acquired'):
            pool.release(con)

        with self.assertRaises(TestExc):
            await c2

        self.assertEqual(pool.used_holders_count, 0)

        with self.assertRaisesRegex(RuntimeError, 'not connected'):
            await pool.acquire('nodb', 'nodb', 'nodb')

        await pool.close()
        with self.assertRaisesRegex(RuntimeError, 'is closed'):
            await pool.acquire('any', 'any', 'any')

    async def test_base_pgpool_2(self):

        class TestHolder(pgpool.BaseConnectionHolder):

            async def _connect(self, dbname, user, password):
                await asyncio.sleep(360)
                return True

            async def _close(self, con):
                assert con

        class TestPool(pgpool.BasePool):

            def _new_holder(self):
                return TestHolder(self)

        loop = asyncio.get_running_loop()
        pool = TestPool(max_capacity=2, concurrency=1, loop=loop)

        c1 = asyncio.create_task(pool.acquire('never', 'never', 'never'))
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(c1, 0.3)
        self.assertTrue(c1.cancelled())

        self.assertEqual(pool.used_holders_count, 0)
        self.assertEqual(pool.empty_holders_count, 2)
        self.assertEqual(pool.unused_holders_count, 0)

    async def test_base_pgpool_3(self):
        loop = asyncio.get_running_loop()

        with self.assertRaisesRegex(RuntimeError, 'greater than concurrency'):
            pgpool.BasePool(max_capacity=1, concurrency=1, loop=loop)

        with self.assertRaisesRegex(RuntimeError, 'greater than 0'):
            pgpool.BasePool(max_capacity=-1, concurrency=1, loop=loop)
