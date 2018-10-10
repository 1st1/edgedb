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


cimport cython
cimport cpython

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t, \
                         UINT32_MAX

from edb.server import defines

from edb.server2.pgproto cimport hton
from edb.server2.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer
)

from edb.server2.edgecon cimport edgecon


import asyncio


DEF DATA_BUFFER_SIZE = 100_000


async def connect(addr, dbname):
    loop = asyncio.get_running_loop()

    _, protocol = await loop.create_unix_connection(
        lambda: PGProto(dbname, loop), addr)

    await protocol.connect()
    return protocol


@cython.final
cdef class PGProto:

    def __init__(self, dbname, loop):

        self.buffer = ReadBuffer()

        self.loop = loop
        self.dbname = dbname

        self.transport = None
        self.msg_waiter = None

        self.connected_fut = loop.create_future()
        self.connected = False

        self.xact_status = PQTRANS_UNKNOWN
        self.backend_pid = -1
        self.backend_secret = -1

    def is_connected(self):
        return bool(self.connected and self.transport is not None)

    def abort(self):
        if not self.transport:
            return
        self.transport.abort()
        self.transport = None
        self.connected = False

        if self.msg_waiter and not self.msg_waiter.done():
            self.msg_waiter.set_exception(ConnectionAbortedError())

    async def execute_anonymous(self,
                                edgecon.EdgeConnection edgecon,
                                bytes query,
                                WriteBuffer bind_data):

        cdef:
            WriteBuffer packet
            WriteBuffer buf

        if not self.connected:
            raise RuntimeError('not connected')

        packet = WriteBuffer.new()

        buf = WriteBuffer.new_message(b'P')
        buf.write_bytestring(b'')  # statement name
        buf.write_bytestring(query)
        buf.write_int16(0)
        packet.write_buffer(buf.end_message())

        buf = WriteBuffer.new_message(b'B')
        buf.write_bytestring(b'')  # portal name
        buf.write_bytestring(b'')  # statement name
        buf.write_buffer(bind_data)
        packet.write_buffer(buf.end_message())

        buf = WriteBuffer.new_message(b'E')
        buf.write_bytestring(b'')  # portal name
        buf.write_int32(0)  # limit: number of rows to return; 0 - all
        packet.write_buffer(buf.end_message())

        packet.write_bytes(PG_SYNC_MESSAGE)
        self.write(packet)

        buf = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == b'1':
                    # ParseComplete
                    self.buffer.discard_message()

                elif mtype == b'E':  ## result
                    # ErrorResponse
                    er = self.parse_error_message()
                    raise RuntimeError(str(er))

                elif mtype == b'Z':
                    # ReadyForQuery
                    self.parse_sync_message()
                    return

                elif mtype == b'n':
                    # NoData
                    self.buffer.discard_message()

                elif mtype == b'D':
                    # DataRow
                    if buf is None:
                        buf = WriteBuffer.new()

                    self.buffer.redirect_messages(buf, b'D')
                    if buf.len() > DATA_BUFFER_SIZE:
                        edgecon.write(buf)
                        buf = None

                elif mtype == b's':  ## result
                    # PortalSuspended
                    self.buffer.discard_message()

                elif mtype == b'C':  ## result
                    # CommandComplete
                    if buf is not None:
                        edgecon.write(buf)
                        buf = None

                elif mtype == b'2':
                    # BindComplete
                    self.buffer.discard_message()

                elif mtype == b'I':  ## result
                    # EmptyQueryResponse
                    self.buffer.discard_message()

                else:
                    raise RuntimeError(
                        f'unsupported reply to execute message {chr(mtype)!r}')

            finally:
                self.buffer.finish_message()

    async def connect(self):
        cdef:
            WriteBuffer outbuf
            WriteBuffer buf
            char mtype
            int32_t status

        if self.connected_fut is not None:
            await self.connected_fut
        if self.connected:
            raise RuntimeError('already connected')
        if self.transport is None:
            raise RuntimeError('no transport object in connect()')

        buf = WriteBuffer()

        # protocol version
        buf.write_int16(3)
        buf.write_int16(0)

        buf.write_bytestring(b'client_encoding')
        buf.write_bytestring(b'utf-8')

        buf.write_bytestring(b'edgedb_use_typeoids')
        buf.write_bytestring(b'false')

        buf.write_utf8('user')
        buf.write_utf8(defines.EDGEDB_SUPERUSER)

        buf.write_utf8('database')
        buf.write_utf8(self.dbname)

        buf.write_bytestring(b'')

        # Send the buffer
        outbuf = WriteBuffer()
        outbuf.write_int32(buf.len() + 4)
        outbuf.write_buffer(buf)
        self.write(outbuf)

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == b'R':
                    # Authentication...
                    status = self.buffer.read_int32()
                    if status != 0:
                        raise RuntimeError('unsupported auth method')

                elif mtype == b'S':
                    # ParameterStatus
                    # XXX can come any time, handle it
                    self.buffer.finish_message()

                elif mtype == b'K':
                    # BackendKeyData
                    self.backend_pid = self.buffer.read_int32()
                    self.backend_secret = self.buffer.read_int32()

                elif mtype == b'E':
                    # ErrorResponse
                    er = self.parse_error_message()
                    raise RuntimeError(str(er))

                elif mtype == b'Z':
                    # ReadyForQuery
                    self.parse_sync_message()
                    self.connected = True
                    return

                else:
                    raise RuntimeError(
                        f'unsupported reply to auth message {chr(mtype)!r}')
            finally:
                self.buffer.finish_message()

    cdef write(self, buf):
        self.transport.write(memoryview(buf))

    cdef parse_error_message(self):
        cdef:
            char code
            bytes message
            dict parsed = {}

        while True:
            code = self.buffer.read_byte()
            if code == 0:
                break

            message = self.buffer.read_cstr()

            parsed[chr(code)] = message.decode()

        return parsed

    cdef parse_sync_message(self):
        cdef char status

        assert self.buffer.get_message_type() == b'Z'

        status = self.buffer.read_byte()

        if status == b'I':
            self.xact_status = PQTRANS_IDLE
        elif status == b'T':
            self.xact_status = PQTRANS_INTRANS
        elif status == b'E':
            self.xact_status = PQTRANS_INERROR
        else:
            self.xact_status = PQTRANS_UNKNOWN

        self.buffer.finish_message()

    async def wait_for_message(self):
        if self.buffer.take_message():
            return
        self.msg_waiter = self.loop.create_future()
        await self.msg_waiter

    def connection_made(self, transport):
        if self.transport is not None:
            raise RuntimeError('connection_made: invalid connection status')
        self.transport = transport
        self.connected_fut.set_result(True)
        self.connected_fut = None

    def connection_lost(self, exc):
        if self.connected_fut is not None and not self.connected_fut.done():
            self.connected_fut.set_exception(ConnectionAbortedError())
            return

        if self.msg_waiter is not None:
            self.msg_waiter.set_exception(ConnectionAbortedError())
            self.msg_waiter = None

        self.transport = None

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def data_received(self, data):
        self.buffer.feed_data(data)

        if self.msg_waiter is not None and self.buffer.take_message():
            self.msg_waiter.set_result(True)
            self.msg_waiter = None

    def eof_received(self):
        pass


cdef bytes PG_SYNC_MESSAGE = bytes(WriteBuffer.new_message(b'S').end_message())
