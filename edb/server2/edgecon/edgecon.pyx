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

from edb.server2.pgproto cimport hton
from edb.server2.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,

    FRBuffer,
    frb_init,
    frb_read,
    frb_read_all,
    frb_get_len,
)


import asyncio


cdef class EdgeConnection:

    def __init__(self, server, loop, cpool, pgpool, dbindex):
        self._con_status = EDGECON_NEW
        self._state = EDGEPROTO_AUTH
        self._id = server.new_edgecon_id()

        self.server = server
        self.loop = loop
        self.cpool = cpool
        self.pgpool = pgpool
        self.dbindex = dbindex
        self.dbview = None

        self._transport = None
        self.buffer = ReadBuffer()

        self.pgcon = None
        self.comp = None

        self._parsing = True
        self._reading_messages = False

        self._main_task = None
        self._startup_msg_waiter = loop.create_future()
        self._msg_take_waiter = None

        self._last_anon_compiled = None

    cdef write(self, buf):
        self._transport.write(memoryview(buf))

    async def wait_for_message(self):
        if self.buffer.take_message():
            return
        self._msg_take_waiter = self.loop.create_future()
        await self._msg_take_waiter

    async def auth(self):
        cdef:
            int16_t hi
            int16_t lo
            char mtype

            WriteBuffer msg_buf
            WriteBuffer buf

        await self._startup_msg_waiter

        hi = self.buffer.read_int16()
        lo = self.buffer.read_int16()
        if hi != 1 or lo != 0:
            self._transport.close()
            raise RuntimeError('wrong proto')

        self._con_status = EDGECON_STARTED

        await self.wait_for_message()
        mtype = self.buffer.get_message_type()
        if mtype == b'0':
            user = self.buffer.read_utf8()
            password = self.buffer.read_utf8()
            database = self.buffer.read_utf8()

            # XXX implement auth
            self.dbview = self.dbindex.new_view(database, user=user)
            self.pgcon = await self.pgpool.acquire(database)
            self.comp = await self.cpool.acquire()

            buf = WriteBuffer()

            msg_buf = WriteBuffer.new_message(b'R')
            msg_buf.write_int32(0)
            msg_buf.end_message()
            buf.write_buffer(msg_buf)

            msg_buf = WriteBuffer.new_message(b'K')
            msg_buf.write_int32(0)  # TODO: should send ID of this connection
            msg_buf.end_message()
            buf.write_buffer(msg_buf)

            msg_buf = WriteBuffer.new_message(b'Z')
            msg_buf.write_byte(b'I')
            msg_buf.end_message()
            buf.write_buffer(msg_buf)

            self.write(buf)

            self.buffer.finish_message()

        else:
            raise TypeError(f'---> {chr(mtype)} <---')

    #############

    def parse_success(self, compiled):
        self._last_anon_compiled = compiled

        buf = WriteBuffer.new_message(b'1')  # ParseComplete
        buf.write_bytestring(compiled.out_type_id)
        buf.write_bytestring(compiled.in_type_id)
        buf.end_message()

        self.write(buf)

        self._state = EDGEPROTO_IDLE

    async def parse(self):
        cdef:
            char mtype

        self._last_anon_compiled = None

        stmt_name = self.buffer.read_utf8()
        if stmt_name:
            raise RuntimeError('named statements are not yet supported')

        eql = self.buffer.read_utf8()
        if not eql:
            raise RuntimeError('empty query')

        compiled = self.dbview.lookup_compiled_query(eql)
        if compiled is not None:
            self.parse_success(compiled)
            return

        compiled = await self.comp.call(
            'compile_edgeql', self.dbview.dbname, self.dbview.dbver, eql)

        self.dbview.cache_compiled_query(eql, compiled)

        self.parse_success(compiled)

    #############

    async def describe(self):
        cdef:
            char rtype
            WriteBuffer msg

        rtype = self.buffer.read_byte()
        if rtype == b'T':
            # describe "type id"
            stmt_name = self.buffer.read_utf8()
            type_num = self.buffer.read_int16()

            msg = WriteBuffer.new_message(b'T')

            for i in range(type_num):
                type_id = self.buffer.read_bytes(16)

                if stmt_name:
                    raise RuntimeError('named statements are not yet supported')
                else:
                    if self._last_anon_compiled is None:
                        raise RuntimeError(
                            'no prepared anonymous statement found')

                    if self._last_anon_compiled.out_type_id == type_id:
                        type_data = self._last_anon_compiled.out_type_data
                    elif self._last_anon_compiled.in_type_id == type_id:
                        type_data = self._last_anon_compiled.in_type_data
                    else:
                        raise RuntimeError(
                            f'no spec available for type id {type_id}')

                    msg.write_int16(len(type_data))
                    msg.write_bytes(type_data)

            msg.end_message()
            self.write(msg)

        else:
            raise RuntimeError(
                f'unsupported "describe" message {chr(rtype)!r}')

    async def execute(self):
        cdef:
            WriteBuffer bound_args_buf

        stmt_name = self.buffer.read_utf8()
        bind_args = self.buffer.consume_message()
        compiled = None

        if stmt_name:
            raise RuntimeError('named statements are not yet supported')
        else:
            if self._last_anon_compiled is None:
                raise RuntimeError('no parsed anonymous query')

            compiled = self._last_anon_compiled

        bound_args_buf = self.recode_bind_args(bind_args)
        await self.pgcon.connection.execute_anonymous(
            self, compiled.sql, bound_args_buf)

        self.write(WriteBuffer.new_message(b'C').end_message())

    async def sync(self):
        cdef:
            WriteBuffer buf

        buf = WriteBuffer.new_message(b'Z')
        buf.write_byte(b'I')
        buf.end_message()
        self.write(buf)

    async def main(self):
        cdef:
            char mtype

        try:
            await self.auth()

            while True:
                if not self.buffer.take_message():
                    await self.wait_for_message()
                mtype = self.buffer.get_message_type()

                try:
                    if mtype == b'P':
                        await self.parse()

                    elif mtype == b'D':
                        await self.describe()

                    elif mtype == b'E':
                        await self.execute()

                    elif mtype == b'S':
                        await self.sync()

                    else:
                        raise RuntimeError(
                            f'unknown message type {chr(mtype)!r}')
                finally:
                    self.buffer.finish_message()

        except ConnectionAbortedError:
            return

        except Exception as ex:
            self.loop.call_exception_handler({
                'message': 'unhandled error in edgedb protocol',
                'exception': ex,
                'protocol': self,
                'transport': self._transport,
                'task': self._main_task,
            })

            # XXX instead of aborting:
            # try to send an error message to the client
            self._transport.abort()

    cdef WriteBuffer recode_bind_args(self, bytes bind_args):
        cdef:
            FRBuffer in_buf
            WriteBuffer out_buf = WriteBuffer.new()
            int32_t argsnum
            ssize_t in_len

        assert cpython.PyBytes_CheckExact(bind_args)
        frb_init(
            &in_buf,
            cpython.PyBytes_AS_STRING(bind_args),
            cpython.Py_SIZE(bind_args))

        # all parameters are in binary
        out_buf.write_int32(0x00010001)

        frb_read(&in_buf, 4)  # ignore buffer length

        # number of elements in the tuple
        argsnum = hton.unpack_int32(frb_read(&in_buf, 4))

        out_buf.write_int16(<int16_t>argsnum)

        in_len = frb_get_len(&in_buf)
        out_buf.write_cstr(frb_read_all(&in_buf), in_len)

        # All columns are in binary format
        out_buf.write_int32(0x00010001)
        return out_buf

    def connection_made(self, transport):
        if self._con_status != EDGECON_NEW:
            raise RuntimeError('connection_made: invalid connection status')
        self._transport = transport
        self._main_task = self.loop.create_task(self.main())
        # self.server.edgecon_register(self)

    def connection_lost(self, exc):
        if self._msg_take_waiter is not None:
            self._msg_take_waiter.set_exception(ConnectionAbortedError())
            self._msg_take_waiter = None

        self._transport = None

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def data_received(self, data):
        self.buffer.feed_data(data)

        if self._con_status == EDGECON_NEW and self.buffer.len() >= 4:
            self._startup_msg_waiter.set_result(True)

        elif self._msg_take_waiter is not None and self.buffer.take_message():
            self._msg_take_waiter.set_result(True)
            self._msg_take_waiter = None

    def eof_received(self):
        pass
