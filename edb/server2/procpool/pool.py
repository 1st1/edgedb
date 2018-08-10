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
import os.path
import sys

from edb.server2 import taskgroup

from . import amsg
from . import worker


PROCESS_INITIAL_RESPONSE_TIMEOUT = 10.0


class Pool:

    def __init__(self, worker_cls, worker_args, *,
                 loop, capacity, runstate_dir, sockname):
        self._worker_cls = worker_cls
        self._worker_args = worker_args

        self._loop = loop
        self._capacity = capacity
        self._runstate_dir = runstate_dir
        self._poolsock_name = os.path.join(self._runstate_dir, sockname)

        self._queue = asyncio.Queue(loop=loop, maxsize=capacity)

    async def start(self):
        async with taskgroup.TaskGroup(name='pool-spawn') as g:
            for i in range(self._capacity):
                g.create_task(self.spawn_worker())

    async def spawn_worker(self):
        res = await self._tpl_con.send(('spawn',))
        if res[0] != 'spawned':
            raise RuntimeError(
                f'unexpected response for "spawn" command: {res}')
        pid = res[1]
        try:
            con = await asyncio.wait_for(
                self._hub.get_by_pid(pid),
                timeout=PROCESS_INITIAL_RESPONSE_TIMEOUT)
        except asyncio.TimeoutError:
            raise RuntimeError('could not spawn compiler worker')

        worker = Worker(self._queue, con)
        self._workers.add(worker)
        worker_task = self._loop.create_task(worker.run())
        worker_task.add_done_callback(
            lambda task: self._on_worker_done(task, worker))

    def _on_worker_done(self, task, worker):
        if self._closing:
            return
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._workers.discard(worker)
            self._loop.create_task(self.spawn_worker())

    async def send(self, msg):
        fut = self._loop.create_future()
        await self._queue.put((fut, msg))
        return await fut

    async def stop(self):
        self._closing = True

        await self._hub.stop()

        for worker in self._workers:
            worker.stop()
        self._workers = None

        if self._tpl_proc is not None and self._tpl_proc.returncode is None:
            self._tpl_proc.terminate()


async def create_pool(*, capacity: int, runstate_dir: str):
    loop = asyncio.get_running_loop()
    pool = CompilerPool(
        loop=loop,
        capacity=capacity,
        runstate_dir=runstate_dir)
    await pool.start()
    return pool
