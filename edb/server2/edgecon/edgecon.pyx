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

from edgedb.pgproto cimport hton
from edgedb.pgproto.pgproto cimport (
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

    def __init__(self, loop, executor):
        self._con_status = EDGECON_NEW
        self._state = EDGEPROTO_AUTH
        self._id = self._server.new_edgecon_id()

        self.loop = loop
        self.executor = executor
        self.dbview = None

        self._transport = None
        self.buffer = ReadBuffer()

        self._parsing = True
        self._reading_messages = False

        self._awaiting_task = None

    # Awaiting on executor methods

    cdef _await(self, coro, success, failure):
        if self._awaiting_task is not None:
            raise RuntimeError('already awaiting')

        self._awaiting_task = self.loop.create_task(coro)
        self._awaiting_task.add_done_callback(
            lambda task: self._await_done(task, success, failure))

    def _await_done(self, task, success, failure):
        self._awaiting_task = None
        if task.cancelled():
            failure(asyncio.CancelledError())
        else:
            exc = task.exception()
            if exc is None:
                success(task.result())
            else:
                failure(exc)

    cdef _pause_parsing(self):
        self._parsing = False

    cdef _resume_parsing(self):
        self._parsing = True
        if not self._reading_messages:
            self._read_buffer_messages()

    cdef _write(self, buf):
        self._transport.write(memoryview(buf))

    cdef _read_buffer_messages(self):
        cdef:
            EdgeProtoState state
            char mtype

        if self._con_status == EDGECON_NEW:
            if not self._handle__startup():
                return

        while self._parsing and self.buffer.take_message() == 1:
            self._pause_parsing()

            mtype = self.buffer.get_message_type()
            state = self._state

            try:
                self._reading_messages = True

                if state == EDGEPROTO_AUTH:
                    self._handle__auth(mtype)

                elif state == EDGEPROTO_IDLE:
                    if mtype == b'P':
                        self._handle__parse()

                    elif mtype == b'D':
                        self._handle__describe()

                    elif mtype == b'S':
                        self._handle__sync()

                    elif mtype == b'E':
                        self._handle__execute()

                    elif mtype == b'Q':
                        self._handle__simple_query()

            except Exception as ex:
                print("EXCEPTION", type(ex), ex)
                self._state = EDGEPROTO_CLOSED
                self._transport.close()
                raise
                return
            finally:
                self.buffer.discard_message()
                self._reading_messages = False

    def _on_server_execute_data(self):
        self._state = EDGEPROTO_IDLE
        self._resume_parsing()

    def _on_server_simple_query(self, data):
        buf = WriteBuffer.new_message(b'C')  # ParseComplete
        buf.write_bytestring(data)
        buf.end_message()
        self._write(buf)

        msg_buf = WriteBuffer.new_message(b'Z')
        msg_buf.write_byte(b'I')
        msg_buf.end_message()
        self._write(msg_buf)

        self._state = EDGEPROTO_IDLE
        self._resume_parsing()

    def _on__failure(self, exc):
        raise exc

    ##### Authentication

    cdef _handle__auth(self, char mtype):
        if mtype == b'0':
            user = self.buffer.read_utf8()
            password = self.buffer.read_utf8()
            database = self.buffer.read_utf8()

            self._await(
                self.executor.authorize(database, user, password),
                self._on__auth_success,
                self._on__failure)

        self._resume_parsing()  # XXX ?

    def _on__auth_success(self, dbview):
        cdef:
            WriteBuffer msg_buf
            WriteBuffer buf

        self.dbview = dbview

        # connection OK

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

        self._write(buf)

        self._state = EDGEPROTO_IDLE
        self._resume_parsing()

    ##### Parse

    cdef _handle__parse(self):
        stmt_name = self.buffer.read_utf8()
        eql = self.buffer.read_utf8()

        if stmt_name == '':
            # anonymous query; try fast path
            compiled = self.dbview.lookup_anonymous_compiled_query(eql)
            if compiled is not None:
                self._on__parse_success(compiled)
                return
        else:
            q = self.dbview.lookup_prepared_query(stmt_name)
            if q is not None:
                # XXX
                raise RuntimeError(
                    f'a prepared statement named {stmt_name!r} already exists')

        self._await(
            self.executor.parse(self, stmt_name, eql),
            self._on__parse_success,
            self._on__failure)

    def _on__parse_success(self, compiled):
        buf = WriteBuffer.new_message(b'1')  # ParseComplete
        buf.write_bytestring(compiled.out_type_id)
        buf.write_bytestring(compiled.in_type_id)
        buf.end_message()

        self._write(buf)

        self._state = EDGEPROTO_IDLE
        self._resume_parsing()

    #####

    cdef _handle__sync(self):
        cdef WriteBuffer msg_buf
        msg_buf = WriteBuffer.new_message(b'Z')
        msg_buf.write_byte(b'I')
        msg_buf.end_message()
        self._write(msg_buf)
        self._resume_parsing()

    cdef _handle__simple_query(self):
        query = self.buffer.read_utf8()
        self._server.edgecon_simple_query(self, query)

    cdef _handle__describe(self):
        cdef:
            char rtype
            WriteBuffer msg

        rtype = self.buffer.read_byte()
        if rtype == b'S':
            # describe statement
            stmt_name = self.buffer.read_utf8()
            q = self._queries[stmt_name]

            msg = WriteBuffer.new_message(b'T')
            msg.write_int16(len(q.compiled.out_type_data))
            msg.write_bytes(q.compiled.out_type_data)
            msg.write_int16(len(q.compiled.in_type_data))
            msg.write_bytes(q.compiled.in_type_data)
            msg.end_message()
            self._write(msg)
            self._resume_parsing()
        else:
            1 / 0

    cdef _recode_args(self, bytes bind_args):
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

    cdef _handle__execute(self):
        stmt_name = self.buffer.read_utf8()
        bind_args = self.buffer.consume_message()
        query = self._queries[stmt_name]
        bind_args = self._recode_args(bind_args)
        self._server.edgecon_execute(self, query, bind_args)

    cdef _handle__startup(self):
        cdef:
            int16_t hi
            int16_t lo
            WriteBuffer buf

        if self.buffer.length() < 4:
            return False

        hi = self.buffer.read_int16()
        lo = self.buffer.read_int16()

        if hi != 1 or lo != 0:
            self._transport.close()
            return False

        self._con_status = EDGECON_STARTED
        return True

    def _send_data(self, data):
        self._transport.write(data)

    def get_user(self):
        return self._user

    def get_dbname(self):
        return self._dbname

    # protocol methods

    def connection_made(self, transport):
        if self._con_status != EDGECON_NEW:
            raise RuntimeError('connection_made: invalid connection status')
        self._transport = transport
        self._server.edgecon_register(self)

    def connection_lost(self, exc):
        self._server.edgecon_unregister(self)
        pass

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def data_received(self, data):
        self.buffer.feed_data(data)
        self._read_buffer_messages()

    def eof_received(self):
        pass
