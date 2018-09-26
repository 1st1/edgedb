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

from . import avg


class PrioritySemaphore:

    def __init__(self, *, loop,
                 concurrency: int,
                 avg_history_size: int):
        self._loop = loop
        self._id_cnt = 0

        self._concurrency = concurrency

        self._slow_num = max(concurrency // 2, 1)
        self._new_num = max(concurrency // 4, 1)
        self._fast_num = max(concurrency - self._slow_num - self._new_num,
                             self._new_num)

        self._slow_bound_num = self._slow_num
        self._fast_bound_num = self._fast_num
        self._new_bound_num = self._new_num

        self._slow_waiters = collections.deque()
        self._fast_waiters = collections.deque()
        self._new_waiters = collections.deque()

        self._release_tok_slow = _SlowReleaseToken(self)
        self._release_tok_fast = _FastReleaseToken(self)
        self._release_tok_new = _NewReleaseToken(self)

        self._avg = avg.RollingAverage(avg_history_size)

    def log_time(self, delta):
        self._avg.add(delta)

    async def acquire(self, avg):
        if self._new_num < 0 or self._slow_num < 0 or self._fast_num < 0:
            raise RuntimeError('negative acquire counters')  # pragma: no cover

        if avg < 1e-10:
            # New query.

            if self._new_num > 0 and not self._new_waiters:
                self._new_num -= 1
                return self._release_tok_new

            if self._slow_num > 0 and not self._slow_waiters:
                self._slow_num -= 1
                return self._release_tok_slow

            queue = self._new_waiters

        elif avg > self._avg.avg * 1.4:
            # Slow query.

            # We only classify payloads as "slow" if their avg
            # is significantly greater than the current avg.
            # The reason for this is to avoid scenario when there is a
            # high number of queries with similar average and all connections
            # are busy processing them.  In this case, the avg will slowly
            # grow, making all queries being classified as "slow".

            if self._slow_num > 0 and not self._slow_waiters:
                self._slow_num -= 1
                return self._release_tok_slow

            queue = self._slow_waiters

        else:
            # Fast query.

            if self._fast_num > 0 and not self._fast_waiters:
                self._fast_num -= 1
                return self._release_tok_fast

            if self._slow_num > 0 and not self._slow_waiters:
                self._slow_num -= 1
                return self._release_tok_slow

            if self._new_num > 0 and not self._new_waiters:
                self._new_num -= 1
                return self._release_tok_new

            queue = self._fast_waiters

        self._id_cnt += 1
        waiter_id = self._id_cnt
        waiter = self._loop.create_future()
        queue.append((waiter_id, waiter))

        try:
            await waiter
        except asyncio.CancelledError:
            if not waiter.cancelled():
                # race between the Task cancellation and a successful acquire?
                self._try_wakeup_next()
            raise

        try:
            if queue is self._fast_waiters:
                self._fast_num -= 1
                return self._release_tok_fast

            elif queue is self._new_waiters:
                self._new_num -= 1
                return self._release_tok_new

            else:
                self._slow_num -= 1
                return self._release_tok_slow
        finally:
            # See if it's possible to wake up more waiters.
            self._try_wakeup_next()

    def _pick_queue(self, q1, q2, q3=None):
        # Return the queue (one of q1, q2, q3) which has
        # the earliest item at the head.

        if q1 and q2:
            if q1[0][0] > q2[0][0]:
                result = q2
            else:
                result = q1
        else:
            result = q1 or q2 or None

        if q3 and result:
            return self._pick_queue(result, q3)

        return q3 or result or None

    def _try_wakeup_next(self):
        queue = None

        if self._fast_num:
            queue = self._fast_waiters

        if not queue and self._new_num:
            queue = self._pick_queue(
                self._fast_waiters, self._new_waiters)

        if not queue and self._slow_num:
            queue = self._pick_queue(
                self._fast_waiters, self._new_waiters, self._slow_waiters)

        if not queue:
            # All queues are empty or we are out of capacity.
            return

        _, waiter = queue.popleft()
        if not waiter.done():
            waiter.set_result(True)  # async

    def _release_new(self):
        if self._new_num >= self._new_bound_num:
            raise ValueError('PrioritySemaphore was released too many times')
        self._new_num += 1
        self._try_wakeup_next()

    def _release_slow(self):
        if self._slow_num >= self._slow_bound_num:
            raise ValueError('PrioritySemaphore was released too many times')
        self._slow_num += 1
        self._try_wakeup_next()

    def _release_fast(self):
        if self._fast_num >= self._fast_bound_num:
            raise ValueError('PrioritySemaphore was released too many times')
        self._fast_num += 1
        self._try_wakeup_next()


class _SemDeque:

    _counter: int

    def __init__(self, value: int):
        self.sem_value = value
        self.waiters = collections.deque()


class _NewReleaseToken:

    def __init__(self, sem):
        self.__sem = sem

    @property
    def _kind(self):
        # for unittests
        return 'new'

    def release(self):
        self.__sem._release_new()


class _SlowReleaseToken:

    def __init__(self, sem):
        self.__sem = sem

    @property
    def _kind(self):
        # for unittests
        return 'slow'

    def release(self):
        self.__sem._release_slow()


class _FastReleaseToken:

    def __init__(self, sem):
        self.__sem = sem

    @property
    def _kind(self):
        # for unittests
        return 'fast'

    def release(self):
        self.__sem._release_fast()
