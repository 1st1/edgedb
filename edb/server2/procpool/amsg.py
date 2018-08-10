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
import os
import pickle
import struct


_len_unpacker = struct.Struct('!I').unpack
_len_packer = struct.Struct('!I').pack


class FramedProtocol(asyncio.Protocol):

    def __init__(self, loop, con_waiter):
        self._loop = loop
        self._buffer = b''
        self._transport = None
        self._msg_waiter = None
        self._con_waiter = None
        self._curmsg_len = -1

    def send(self, waiter, payload: bytes):
        if self._msg_waiter is not None and not self._msg_waiter.done():
            raise RuntimeError('FramedProtocol: another send() is in progress')
        self._msg_waiter = waiter
        self._transport.writelines((_len_packer(len(payload)), payload))

    def data_received(self, data):
        self._buffer += data
        while self._buffer:
            if self._curmsg_len == -1:
                if len(self._buffer) >= 4:
                    self._curmsg_len = _len_unpacker(self._buffer[:4])[0]
                    self._buffer = self._buffer[4:]
                else:
                    return

            if self._curmsg_len > 0 and len(self._buffer) >= self._curmsg_len:
                msg = self._buffer[:self._curmsg_len]
                self._buffer = self._buffer[self._curmsg_len:]
                self._curmsg_len = -1
                self.process_message(msg)
            else:
                return

    def connection_made(self, tr):
        self._transport = tr
        if self._con_waiter is not None:
            self._con_waiter.set_result(True)
            self._con_waiter = None

    def connection_lost(self, exc):
        if self._con_waiter is not None:
            if exc is None:
                exc = ConnectionError('unexpected loss of connection')
            self._con_waiter.set_exception(exc)
            self._con_waiter = None
            return

        if self._msg_waiter is not None:
            if exc is not None:
                self._msg_waiter.set_exception(exc)
            else:
                self._msg_waiter.set_exception(ConnectionError(
                    'unexpected connection_lost call during send'))
            self._msg_waiter = None


class Connection:

    def __init__(self, loop, pid, tr, pr):
        self._transport = tr
        self._protocol = pr
        self._pid = pid
        self._closed = False

    @property
    def pid(self):
        return self._pid

    async def send(self, obj):
        if self.is_closed():
            raise RuntimeError(
                f'cannot send {obj!r}; '
                f'pool connection for {self._pid} PID is closed')
        payload = pickle.dumps(obj)
        response = await self._protocol.push(payload)
        return pickle.loads(response)

    def is_closed(self):
        return self._closed or self._transport.is_closing()

    def close(self):
        self._transport.abort()


class Hub:

    def __init__(self, loop, sockname):
        self._sockname = sockname
        self._loop = loop
        self._server = None
        self._connections = {}
        self._pid_waiters = {}

    def _check_pid(self, pid):
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True

    async def get_by_pid(self, pid: int):
        def _pid_waiter(pid, fut):
            if fut.cancelled() and self._pid_waiters.get(pid) is fut:
                self._pid_waiters[pid]

        try:
            return self._connections[pid]
        except KeyError:
            pass

        if pid in self._pid_waiters:
            await self._pid_waiters
        else:
            fut = self._loop.create_future()
            fut.add_done_callback(lambda fut: _pid_waiter(pid, fut))
            self._pid_waiters[pid] = fut
            await fut

        return self._connections[pid]

    def _register(self, pid, proto):
        con = Connection(self._loop, pid, proto._transport, proto)
        self._connections[pid] = con
        if pid in self._pid_waiters:
            self._pid_waiters[pid].set_result(con)
            del self._pid_waiters[pid]

    async def start(self):
        assert self._server is None
        self._server = await self._loop.create_unix_server(
            lambda: Protocol(self),
            path=self._sockname)

    async def stop(self):
        if self._server is None:
            return

        self._server.close()
        await self._server.wait_closed()
        self._server = None
