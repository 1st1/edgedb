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


import weakref

from . import compilerworker
from . import pgcon
from . import procpool
from . import taskgroup


class Backend:

    def __init__(self, pgcon, compiler):
        self._pgcon = pgcon
        self._compiler = compiler

    @property
    def pgcon(self):
        return self._pgcon

    @property
    def compiler(self):
        return self._compiler

    async def close(self):
        self._pgcon.abort()
        self._compiler.close()


class BackendManager:

    def __init__(self, *, pgaddr, runstate_dir):
        self._pgaddr = pgaddr
        self._runstate_dir = runstate_dir

        self._backends = weakref.WeakSet()

        self._compiler_manager = None

    async def start(self):
        self._compiler_manager = await procpool.create_manager(
            runstate_dir=self._runstate_dir,
            name='edgedb-compiler',
            worker_cls=compilerworker.Compiler,
            worker_args=(dict(host=self._pgaddr),))

    async def stop(self):
        # TODO: Make a graceful version of this.
        try:
            async with taskgroup.TaskGroup() as g:
                for backend in self._backends:
                    g.create_task(backend.close())
        finally:
            await self._compiler_manager.stop()

    async def new_backend(self, *, dbname: str, dbver: int):
        try:
            compiler = None
            async with taskgroup.TaskGroup() as g:
                new_pgcon = g.create_task(
                    pgcon.connect(self._pgaddr, dbname))

                compiler = await self._compiler_manager.spawn_worker()
                g.create_task(
                    compiler.call('connect', dbname, dbver))

        except Exception:
            try:
                if compiler is not None:
                    compiler.close()
            finally:
                if (new_pgcon.done() and
                        not new_pgcon.cancelled() and
                        not new_pgcon.exception()):
                    con = new_pgcon.result()
                    con.abort()
            raise

        backend = Backend(new_pgcon.result(), compiler)
        self._backends.add(backend)
        return backend
