#
# This sowurce file is part of the EdgeDB open source project.
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

from edb.server2.pgcon cimport pgcon


import asyncio

DEF FLUSH_BUFFER_AFTER = 100_000


@cython.final
cdef class EdgeConnection:

    def __init__(self, server):
        self._con_status = EDGECON_NEW
        self._id = server.new_edgecon_id()
        self.server = server

        self.loop = server.get_loop()
        self.dbview = None
        self.backend = None

        self._transport = None
        self.buffer = ReadBuffer()

        self._parsing = True
        self._reading_messages = False

        self._main_task = None
        self._startup_msg_waiter = self.loop.create_future()
        self._msg_take_waiter = None

        self._last_anon_compiled = None

        self._write_buf = None

    cdef write(self, WriteBuffer buf):
        if self._write_buf is not None:
            self._write_buf.write_buffer(buf)
            if self._write_buf.len() >= FLUSH_BUFFER_AFTER:
                self.flush()
        else:
            self._write_buf = buf

    cdef flush(self):
        if self._write_buf is not None and self._write_buf.len():
            buf = memoryview(self._write_buf)
            self._write_buf = None
            self._transport.write(buf)

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
            self.dbview = self.server.new_view(
                dbname=database, user=user)
            self.backend = await self.server.new_backend(
                dbname=database, dbver=self.dbview.dbver)

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
            self.flush()

            self.buffer.finish_message()

        else:
            self.fallthrough()

    #############

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
        if compiled is None:
            compiled = await self.backend.compiler.call(
                'compile_edgeql', self.dbview.dbname, self.dbview.dbver, eql)

            self.dbview.cache_compiled_query(eql, compiled)

        self._last_anon_compiled = compiled

        buf = WriteBuffer.new_message(b'1')  # ParseComplete
        buf.write_bytes(compiled.in_type_id)
        buf.write_bytes(compiled.out_type_id)
        buf.end_message()

        self.write(buf)

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
                f'unsupported "describe" message mode {chr(rtype)!r}')

    async def execute(self):
        cdef:
            WriteBuffer bound_args_buf
            bint send_sync

        stmt_name = self.buffer.read_utf8()
        bind_args = self.buffer.consume_message()
        compiled = None

        send_sync = False
        if self.buffer.take_message_type(b'S'):
            # A "Sync" message follows this "Execute" message;
            # send it right away.
            send_sync = True
            self.buffer.finish_message()

        if stmt_name:
            raise RuntimeError('named statements are not yet supported')
        else:
            if self._last_anon_compiled is None:
                raise RuntimeError('no parsed anonymous query')

            compiled = self._last_anon_compiled

        bound_args_buf = self.recode_bind_args(bind_args)

        await self.backend.pgcon.execute_anonymous(
            self, compiled.sql, bound_args_buf,
            send_sync)

        self.write(WriteBuffer.new_message(b'C').end_message())

        if send_sync:
            self.write(self.pgcon_last_sync_status())
            self.flush()

    async def sync(self):
        cdef:
            WriteBuffer buf

        await self.backend.pgcon.sync()
        self.write(self.pgcon_last_sync_status())

        self.flush()

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
                        self.fallthrough()
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

    cdef pgcon_last_sync_status(self):
        cdef:
            pgcon.PGTransactionStatus xact_status
            WriteBuffer buf

        xact_status = <pgcon.PGTransactionStatus>(
            (<pgcon.PGProto>self.backend.pgcon).xact_status)

        buf = WriteBuffer.new_message(b'Z')
        if xact_status == pgcon.PQTRANS_IDLE:
            buf.write_byte(b'I')
        elif xact_status == pgcon.PQTRANS_INTRANS:
            buf.write_byte(b'T')
        elif xact_status == pgcon.PQTRANS_INERROR:
            buf.write_byte(b'E')
        else:
            raise RuntimeError('unknown postgres connection status')
        return buf.end_message()

    cdef fallthrough(self):
        cdef:
            char mtype = self.buffer.get_message_type()

        if mtype == b'H':
            # Flush
            self.flush()
            self.buffer.discard_message()
            return

        raise RuntimeError(
            f'unexpected message type {chr(mtype)!r}')

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
        if (self._msg_take_waiter is not None and
                not self._msg_take_waiter.done()):
            self._msg_take_waiter.set_exception(ConnectionAbortedError())
            self._msg_take_waiter = None

        self._transport = None

        if self.backend is not None:
            self.loop.create_task(self.backend.close())

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
