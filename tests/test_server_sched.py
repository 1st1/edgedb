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


import unittest

from edb.server2 import sched


class VClock:

    def __init__(self):
        self._now = 0

    def advance(self, dt):
        self._now += dt

    def now(self):
        return self._now


class TestServerSched(unittest.TestCase):

    def test_sched_1(self):

        CASES = [
            (100, 3, 5),
        ]

        for period, queues_n, tasks_per_queue in CASES:
            clock = VClock()
            s = sched.Scheduler(clock=clock.now, tick_period=period)

            queues = list(range(queues_n))

            for q in queues:
                for t in range(tasks_per_queue):
                    s.put(q, t)

            clock.advance(1)

            for q in queues:
                for t in range(tasks_per_queue):
                    t = s.get()
                    clock.advance(1)
                    s.complete(t)

            for q in queues:
                print(s._queues[q]._timings)

    def test_sched_2(self):
        clock = VClock()
        s = sched.Scheduler(clock=clock.now, tick_period=3)
        key1 = 1
        key2 = 2

        s.put(key1, 'aaa')
        s.put(key2, 'bbb')
        s.put(key2, 'ccc')

        clock.advance(0.05)
        t1 = s.get()

        clock.advance(0.1)
        t2 = s.get()

        clock.advance(0.2)
        s.complete(t2)

        clock.advance(0.2)
        s.complete(t1)

        clock.advance(0.4)
        t3 = s.get()

        clock.advance(0.5)
        s.complete(t3)

        print(s._queues[key1]._timings)
        print(s._queues[key2]._timings)
