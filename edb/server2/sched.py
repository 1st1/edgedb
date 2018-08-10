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


import collections


class Token:

    def __init__(self, key, payload):
        self.key = key
        self.payload = payload

    def __repr__(self):
        return f'<Token key={self.key!r} payload={self.payload!r}>'


class Queue:

    def __init__(self, *, tick_period: int):
        self._q = collections.deque()
        self._timings = [0] * tick_period
        self._total = 0
        self._busy_tokens = set()

    def _do_tick(self, delta_t, tick):
        self._timings[tick] = len(self._busy_tokens) * delta_t
        self._total = sum(self._timings)

    def push(self, token):
        self._q.append(token)

    def has(self):
        return bool(self._q)

    def get(self) -> Token:
        tok = self._q.popleft()
        self._busy_tokens.add(tok)
        return tok

    def done(self, token):
        assert token in self._busy_tokens
        self._busy_tokens.discard(token)


class Scheduler:

    def __init__(self, *, clock, tick_period: int):
        self._clock_func = clock
        self._tick_period = tick_period
        self._current_tick = 0
        self._queues = {}
        self._last_tick_time = 0

    def _get_next_tick(self):
        self._current_tick += 1
        self._current_tick %= self._tick_period
        return self._current_tick

    def _do_tick(self):
        tick = self._get_next_tick()

        now = self._clock_func()
        delta_t = now - self._last_tick_time
        self._last_tick_time = now

        for q in self._queues.values():
            q._do_tick(delta_t, tick)

    def put(self, key, payload) -> Token:
        tok = Token(key, payload)
        try:
            q = self._queues[key]
        except KeyError:
            min_sum = min(
                (q._total for q in self._queues.values()),
                default=0)

            q = self._queues[key] = Queue(tick_period=self._tick_period)
            q._timings[self._current_tick] = min_sum

        q.push(tok)

    def get(self) -> Token:
        self._do_tick()

        queue = min(
            [q for q in self._queues.values() if q.has()],
            key=lambda q: q._total,
            default=None)

        if queue:
            return queue.get()

    def complete(self, token: Token):
        self._do_tick()
        queue = self._queues[token.key]
        queue.done(token)
