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
import base64
import os.path
import pickle
import sys

from edb.server2 import taskgroup

from . import amsg


PROCESS_INITIAL_RESPONSE_TIMEOUT = 10.0
KILL_TIMEOUT = 10.0
WORKER_MOD = __name__.rpartition('.')[0] + '.worker'


class Worker:

    def __init__(self, server, command_args):
        self._server = server
        self._command_args = command_args
        self._proc = None
        self._con = None

    async def _kill_proc(self, proc):
        try:
            proc.kill()
        except ProcessLookupError:
            return

        try:
            await asyncio.wait_for(proc.wait(), KILL_TIMEOUT)
        except Exception:
            proc.terminate()
            raise

    async def spawn(self):
        if self._proc is not None:
            asyncio.create_task(self._kill_proc(self._proc))
            self._proc = None

        self._proc = await asyncio.create_subprocess_exec(*self._command_args)
        try:
            self._con = await asyncio.wait_for(
                self._server.get_by_pid(self._proc.pid),
                PROCESS_INITIAL_RESPONSE_TIMEOUT)
        except Exception:
            self._proc.kill()
            raise

    async def call(self, method_name, args):
        if self._con.is_closed():
            await self.spawn()

        msg = pickle.dumps((method_name, args))
        data = await self._con.request(msg)
        return pickle.loads(data)

    def close(self):
        self._proc.kill()


class Pool:

    def __init__(self, *, worker_cls, worker_args,
                 loop, capacity, runstate_dir, name):
        self._worker_cls = worker_cls
        self._worker_args = worker_args

        self._loop = loop
        self._capacity = capacity
        self._runstate_dir = runstate_dir

        self._name = name
        self._poolsock_name = os.path.join(
            self._runstate_dir, f'{name}.socket')

        self._workers_queue = asyncio.Queue(loop=loop)
        self._watchers = []
        self._workers = []

        self._server = amsg.Server(self._poolsock_name, loop)

        self._worker_command_args = [
            sys.executable, '-m', WORKER_MOD,

            '--cls-name',
            f'{self._worker_cls.__module__}.{self._worker_cls.__name__}',

            '--cls-args', base64.b64encode(pickle.dumps(self._worker_args)),
            '--sockname', self._poolsock_name
        ]

    async def _spawn_worker(self):
        worker = Worker(self._server, self._worker_command_args)
        await worker.spawn()
        self._workers.append(worker)
        self._workers_queue.put_nowait(worker)

    async def call(self, method_name, *args):
        worker = await self._workers_queue.get()

        try:
            status, data = await worker.call(method_name, args)
        finally:
            self._workers_queue.put_nowait(worker)

        if status == 0:
            return data
        else:
            raise data

    async def start(self):
        await self._server.start()

        try:
            async with taskgroup.TaskGroup(
                    name=f'{self._name}-pool-spawn') as g:

                for i in range(self._capacity):
                    g.create_task(self._spawn_worker())

        except taskgroup.TaskGroupError:
            await self.stop()
            raise

    async def stop(self):
        await self._server.stop()

        for worker in self._workers:
            worker.close()

        self._workers = []


async def create_pool(*, capacity: int, runstate_dir: str, name: str,
                      worker_cls: type, worker_args: tuple):
    loop = asyncio.get_running_loop()
    pool = Pool(
        loop=loop,
        capacity=capacity,
        runstate_dir=runstate_dir,
        worker_cls=worker_cls,
        worker_args=worker_args,
        name=name)

    await pool.start()
    return pool
