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


cdef class EdgeConnection:

    def __init__(self, CoreServer server):
        self._con_status = EDGECON_NEW
        self._state = EDGEPROTO_AUTH
        self._server = server
        self._id = self._server.new_edgecon_id()
        self._dbname = None
        self._user = None
        self._password = None

        self._queries = {}

        self._transport = None
        self.buffer = ReadBuffer()

        self._parsing = True
        self._reading_messages = False

    cdef _pause_parsing(self):
        print('$$$ pause parsing')
        self._parsing = False

    cdef _resume_parsing(self):
        print('$$$ resume parsing')
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

        while self._parsing and self.buffer.has_message() == 1:
            self._pause_parsing()

            mtype = self.buffer.get_message_type()
            print('INCOMING MESSAGE', chr(mtype))
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
                return
            finally:
                self.buffer.discard_message()
                self._reading_messages = False

    def _on_server_auth(self, exc):
        cdef:
            WriteBuffer msg_buf
            WriteBuffer buf

        if exc is None:
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

        else:
            # couldn't connect
            1 / 0

    def _on_server_parse(self, stmt_name, q, exc):
        if exc is None:
            self._queries[stmt_name] = q

            buf = WriteBuffer.new_message(b'1')  # ParseComplete
            buf.write_bytestring(q.out_type_id)
            buf.write_bytestring(q.in_type_id)
            buf.end_message()

            self._write(buf)

            self._state = EDGEPROTO_IDLE
            self._resume_parsing()
        else:
            print('!!!!!!', exc)
            raise exc

    def _on_server_execute_data(self, data):
        self._write(data)
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

        print('SENT>>')
        self._state = EDGEPROTO_IDLE
        self._resume_parsing()

    cdef _handle__auth(self, char mtype):
        if mtype == b'0':
            user = self.buffer.read_utf8()
            password = self.buffer.read_utf8()
            database = self.buffer.read_utf8()
            self._dbname = database
            self._user = user

            # The server will call the "_on_server_auth" callback
            # once we verify the database name and user/password.
            self._server.edgecon_authorize(self, user, password, database)

        self._resume_parsing()

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

    cdef _handle__parse(self):
        stmt_name = self.buffer.read_utf8()
        query = self.buffer.read_utf8()
        self._server.edgecon_parse(self, stmt_name, query)

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
            msg.write_int16(len(q.out_type_data))
            msg.write_bytes(q.out_type_data)
            msg.write_int16(len(q.in_type_data))
            msg.write_bytes(q.in_type_data)
            msg.end_message()
            self._write(msg)
            self._resume_parsing()
        else:
            1 / 0

    cdef _recode_args(self, bytes bind_args):
        cdef:
            FastReadBuffer in_buf = FastReadBuffer.new()
            WriteBuffer out_buf = WriteBuffer.new()
            int32_t argsnum
            ssize_t in_len

        assert cpython.PyBytes_CheckExact(bind_args)
        in_buf.buf = cpython.PyBytes_AS_STRING(bind_args)
        in_buf.len = cpython.Py_SIZE(bind_args)

        # all parameters are in binary
        out_buf.write_int32(0x00010001)

        b = in_buf.read(4)  # ignore buffer length
        print(b)


        # number of elements in the tuple
        argsnum = hton.unpack_int32(in_buf.read(4))

        out_buf.write_int16(<int16_t>argsnum)

        in_len = in_buf.len
        out_buf.write_cstr(in_buf.read_all(), in_len)

        # All columns are in binary format
        out_buf.write_int32(0x00010001)

        return bytes(out_buf)

    cdef _handle__execute(self):
        stmt_name = self.buffer.read_utf8()
        bind_args = self.buffer.consume_message().as_bytes()
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
        print('CON LOST', exc)
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
