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
import random
import time

from edb.server2 import prisem
from edb.server import _testbase as tb


class RegularSemaphore:

    def __init__(self, *, loop, concurrency, avg_history_size):
        self._loop = loop
        self._sem = asyncio.BoundedSemaphore(concurrency, loop=loop)

    def log_time(self, time):
        pass

    async def acquire(self, time):
        await self._sem.acquire()
        return self._sem


class TestPrioritySemaphore(tb.TestCase):

    async def test_server_prisem_1(self):
        ps = prisem.PrioritySemaphore(
            loop=asyncio.get_running_loop(),
            concurrency=4,
            avg_history_size=100)

        # Test new over-release
        r = await ps.acquire(0)
        r.release()
        with self.assertRaisesRegex(ValueError, 'too many times'):
            r.release()

        # test slow over-release
        for _ in range(100):
            ps.log_time(0.00001)
        r = await ps.acquire(1)
        r.release()
        with self.assertRaisesRegex(ValueError, 'too many times'):
            r.release()

        # Test fast over-release
        for _ in range(100):
            ps.log_time(0.1)
        r = await ps.acquire(0.001)
        r.release()
        with self.assertRaisesRegex(ValueError, 'too many times'):
            r.release()

    async def test_server_prisem_2(self):
        ps = prisem.PrioritySemaphore(
            loop=asyncio.get_running_loop(),
            concurrency=3,
            avg_history_size=100)

        # normalize the avg
        for _ in range(100):
            ps.log_time(1)

        for _ in range(200):
            r1 = await ps.acquire(0.1)
            r2 = await ps.acquire(0)
            r3 = await ps.acquire(10)

            self.assertEqual(r1._kind, 'fast')
            self.assertEqual(r2._kind, 'new')
            self.assertEqual(r3._kind, 'slow')

            ps.log_time(0.1)
            ps.log_time(0)
            ps.log_time(10)

            r1.release()
            r2.release()
            r3.release()

    async def test_server_prisem_3(self):
        TOTAL_TIME = 0

        ps = prisem.PrioritySemaphore(
            loop=asyncio.get_running_loop(),
            concurrency=3,
            avg_history_size=100)

        async def query(idx, order, high_time):
            nonlocal TOTAL_TIME

            delay = random.triangular(0, high_time)
            TOTAL_TIME += delay

            r = await ps.acquire(delay)
            order.append(idx)
            await asyncio.sleep(delay)
            ps.log_time(delay)
            r.release()

        async def run_batch(*, fast_time=0, fast_tasks=0,
                            slow_time=0, slow_tasks=0, new_tasks=0,
                            check_run_order=False):

            fast_run_order = []
            slow_run_order = []
            new_run_order = []

            seq = (['fast' for i in range(fast_tasks)] +
                   ['slow' for i in range(slow_tasks)] +
                   ['new' for i in range(new_tasks)])
            random.shuffle(seq)

            expected_fast_run_order = []
            expected_slow_run_order = []
            expected_new_run_order = []

            tasks = []
            for i, s in enumerate(seq):
                if s == 'fast':
                    t = asyncio.create_task(
                        query(i, fast_run_order, fast_time))
                    expected_fast_run_order.append(i)
                elif s == 'slow':
                    t = asyncio.create_task(
                        query(i, slow_run_order, slow_time))
                    expected_slow_run_order.append(i)
                else:
                    t = asyncio.create_task(
                        query(i, new_run_order, 0))
                    expected_new_run_order.append(i)
                tasks.append(t)

            await asyncio.gather(*tasks)

            if check_run_order:
                self.assertEqual(fast_run_order, expected_fast_run_order)
                self.assertEqual(slow_run_order, expected_slow_run_order)
                self.assertEqual(new_run_order, expected_new_run_order)

        STARTED = time.monotonic()

        print('==========1')
        # Set average to 0.00001:
        for _ in range(100):
            ps.log_time(0.001)
        await run_batch(
            fast_time=0.001,
            fast_tasks=100,
            slow_time=0.01,
            slow_tasks=100,
            new_tasks=5,
            check_run_order=False)

        print('==========2')
        await run_batch(
            fast_time=0.001,
            fast_tasks=100,
            slow_time=0.001,
            slow_tasks=100,
            new_tasks=10)

        print('==========3')
        # Set average to 0.001:
        for _ in range(100):
            ps.log_time(0.001)
        # Now run it again (so that everything is slow)
        await run_batch(
            fast_time=0.01,
            fast_tasks=100,
            slow_time=0.01,
            slow_tasks=100,
            new_tasks=10)

        print('==========4')
        # Set average to 0.1:
        for _ in range(100):
            ps.log_time(0.1)
        # Now run it again (so that everything is fast)
        await run_batch(
            fast_time=0.001,
            fast_tasks=100,
            new_tasks=100)

        print('==========5')
        await run_batch(
            fast_time=0.01,
            fast_tasks=1000,
            slow_time=0.1,
            slow_tasks=1000,
            new_tasks=100)

        print('print absolute time', TOTAL_TIME)
        print('actual time', time.monotonic() - STARTED)
