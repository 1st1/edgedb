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


import socket


cdef class CorePGProto:

    def __init__(self, con_params):
        # type of `con_params` is `_ConnectionParameters`
        self.buffer = ReadBuffer()
        self.auth_msg = None
        self.con_params = con_params
        self.transport = None
        self.con_status = PGCON_BAD
        self.state = PGPROTO_IDLE
        self.xact_status = PQTRANS_IDLE
        self.encoding = 'utf-8'

        self._skip_discard = False

        # executemany support data
        self._execute_iter = None
        self._execute_portal_name = None
        self._execute_stmt_name = None

        self._reset_result()

    cdef _write(self, buf):
        self.transport.write(memoryview(buf))

    cdef inline _write_sync_message(self):
        self.transport.write(PG_SYNC_MESSAGE)

    cdef _read_server_messages(self):
        cdef:
            char mtype
            PGProtocolState state

        while self.buffer.has_message() == 1:
            mtype = self.buffer.get_message_type()
            state = self.state

            try:
                if mtype == b'S':
                    # ParameterStatus
                    self._parse_msg_parameter_status()
                    continue
                elif mtype == b'A':
                    # NotificationResponse
                    self._parse_msg_notification()
                    continue
                elif mtype == b'N':
                    # 'N' - NoticeResponse
                    self._on_notice(self._parse_msg_error_response(False))
                    continue

                if state == PGPROTO_AUTH:
                    self._process__auth(mtype)

                elif state == PGPROTO_EXECUTE_ANONYMOUS:
                    self._process__execute_anonymous(mtype)

                elif state == PGPROTO_SIMPLE_QUERY:
                    self._process__simple_query(mtype)

                elif state == PGPROTO_CANCELLED:
                    # discard all messages until the sync message
                    if mtype == b'E':
                        self._parse_msg_error_response(True)
                    elif mtype == b'Z':
                        self._parse_msg_ready_for_query()
                        self._push_result()
                    else:
                        self.buffer.consume_message()

                elif state == PGPROTO_ERROR_CONSUME:
                    # Error in protocol (on asyncpg side);
                    # discard all messages until sync message

                    if mtype == b'Z':
                        # Sync point, self to push the result
                        if self.result_type != RESULT_FAILED:
                            self.result_type = RESULT_FAILED
                            self.result = RuntimeError(
                                'unknown error in protocol implementation')

                        self._push_result()

                    else:
                        self.buffer.consume_message()

                else:
                    raise RuntimeError(
                        'protocol is in an unknown state {}'.format(state))

            except Exception as ex:
                self.result_type = RESULT_FAILED
                self.result = ex

                if mtype == b'Z':
                    self._push_result()
                else:
                    self.state = PGPROTO_ERROR_CONSUME

            finally:
                if self._skip_discard:
                    self._skip_discard = False
                else:
                    self.buffer.discard_message()

    cdef _process__auth(self, char mtype):
        if mtype == b'R':
            # Authentication...
            self._parse_msg_authentication()
            if self.result_type != RESULT_OK:
                self.con_status = PGCON_BAD
                self._push_result()
                self.transport.close()

            elif self.auth_msg is not None:
                # Server wants us to send auth data, so do that.
                self._write(self.auth_msg)
                self.auth_msg = None

        elif mtype == b'K':
            # BackendKeyData
            self._parse_msg_backend_key_data()

        elif mtype == b'E':
            # ErrorResponse
            self.con_status = PGCON_BAD
            self._parse_msg_error_response(True)
            self._push_result()

        elif mtype == b'Z':
            # ReadyForQuery
            self._parse_msg_ready_for_query()
            self.con_status = PGCON_OK
            self._push_result()

    cdef _process__simple_query(self, char mtype):
        if mtype in {b'D', b'I', b'T'}:
            # 'D' - DataRow
            # 'I' - EmptyQueryResponse
            # 'T' - RowDescription
            self.buffer.consume_message()

        elif mtype == b'E':
            # ErrorResponse
            self._parse_msg_error_response(True)

        elif mtype == b'Z':
            # ReadyForQuery
            self._parse_msg_ready_for_query()
            self._push_result()

        elif mtype == b'C':
            # CommandComplete
            self._parse_msg_command_complete()

        else:
            # We don't really care about COPY IN etc
            self.buffer.consume_message()

    cdef _process__execute_anonymous(self, char mtype):
        if mtype == b'1':
            # ParseComplete
            self.buffer.consume_message()

        elif mtype == b'E':
            # ErrorResponse
            self._parse_msg_error_response(True)

        elif mtype == b'Z':
            # ReadyForQuery
            self._parse_msg_ready_for_query()
            self._push_result()

        elif mtype == b'n':
            # NoData
            self.buffer.consume_message()

        elif mtype == b'D':
            # DataRow
            # XXX this is very slow; optimize by avoiding reconstructing
            # the buffer.
            d = self.buffer.consume_message()
            if self.result_data is None:
                self.result_data = WriteBuffer.new()
            self.result_data.write_byte(b'D')
            self.result_data.write_int32(d.length + 4)
            self.result_data.write_bytes(d.as_bytes())

        elif mtype == b's':
            # PortalSuspended
            self.buffer.consume_message()

        elif mtype == b'C':
            # CommandComplete
            self.result_execute_completed = True
            self._parse_msg_command_complete()

        elif mtype == b'E':
            # ErrorResponse
            self._parse_msg_error_response(True)

        elif mtype == b'2':
            # BindComplete
            self.buffer.consume_message()

        elif mtype == b'I':
            # EmptyQueryResponse
            self.buffer.consume_message()

    cdef _parse_msg_command_complete(self):
        cdef:
            const char* cbuf
            ssize_t cbuf_len

        cbuf = self.buffer.try_consume_message(&cbuf_len)
        if cbuf != NULL and cbuf_len > 0:
            msg = cpython.PyBytes_FromStringAndSize(cbuf, cbuf_len - 1)
        else:
            msg = self.buffer.read_cstr()
        self.result_status_msg = msg

    cdef _parse_msg_backend_key_data(self):
        self.backend_pid = self.buffer.read_int32()
        self.backend_secret = self.buffer.read_int32()

    cdef _parse_msg_parameter_status(self):
        name = self.buffer.read_cstr()
        name = name.decode(self.encoding)

        val = self.buffer.read_cstr()
        val = val.decode(self.encoding)

        # self._set_server_parameter(name, val)

    cdef _parse_msg_notification(self):
        pid = self.buffer.read_int32()
        channel = self.buffer.read_cstr().decode(self.encoding)
        payload = self.buffer.read_cstr().decode(self.encoding)
        self._on_notification(pid, channel, payload)

    cdef _parse_msg_authentication(self):
        cdef:
            int32_t status

        status = self.buffer.read_int32()

        if status == PGAUTH_SUCCESSFUL:
            # AuthenticationOk
            self.result_type = RESULT_OK

        elif status == PGAUTH_REQUIRED_PASSWORD:
            # AuthenticationCleartextPassword
            self.result_type = RESULT_OK
            self.auth_msg = self._auth_password_message_cleartext()

        elif status in (PGAUTH_REQUIRED_KERBEROS, PGAUTH_REQUIRED_SCMCRED,
                        PGAUTH_REQUIRED_GSS, PGAUTH_REQUIRED_GSS_CONTINUE,
                        PGAUTH_REQUIRED_SSPI, PGAUTH_REQUIRED_PASSWORDMD5):
            self.result_type = RESULT_FAILED
            self.result = RuntimeError(
                'unsupported authentication method requested by the '
                'server: {!r}'.format(PGAUTH_METHOD_NAME[status]))

        else:
            self.result_type = RESULT_FAILED
            self.result = RuntimeError(
                'unsupported authentication method requested by the '
                'server: {}'.format(status))

        self.buffer.consume_message()

    cdef _auth_password_message_cleartext(self):
        cdef:
            WriteBuffer msg

        msg = WriteBuffer.new_message(b'p')
        msg.write_bytestring(self.con_params.password.encode('ascii'))
        msg.end_message()

        return msg

    cdef _parse_msg_ready_for_query(self):
        cdef char status = self.buffer.read_byte()

        if status == b'I':
            self.xact_status = PQTRANS_IDLE
        elif status == b'T':
            self.xact_status = PQTRANS_INTRANS
        elif status == b'E':
            self.xact_status = PQTRANS_INERROR
        else:
            self.xact_status = PQTRANS_UNKNOWN

    cdef _parse_msg_error_response(self, is_error):
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

        if is_error:
            self.result_type = RESULT_FAILED
            self.result = parsed
        else:
            return parsed

    cdef _push_result(self):
        try:
            self._on_result()
        finally:
            self._set_state(PGPROTO_IDLE)
            self._reset_result()

    cdef _reset_result(self):
        self.result_type = RESULT_OK
        self.result = None
        self.result_data = None
        self.result_status_msg = None
        self.result_execute_completed = False
        self._discard_data = False

    cdef _set_state(self, PGProtocolState new_state):
        if new_state == PGPROTO_IDLE:
            if self.state == PGPROTO_FAILED:
                raise RuntimeError(
                    'cannot switch to "idle" state; '
                    'protocol is in the "failed" state')
            elif self.state == PGPROTO_IDLE:
                raise RuntimeError(
                    'protocol is already in the "idle" state')
            else:
                self.state = new_state

        elif new_state == PGPROTO_FAILED:
            self.state = PGPROTO_FAILED

        elif new_state == PGPROTO_CANCELLED:
            self.state = PGPROTO_CANCELLED

        else:
            if self.state == PGPROTO_IDLE:
                self.state = new_state

            elif self.state == PGPROTO_FAILED:
                raise RuntimeError(
                    'cannot switch to state {}; '
                    'protocol is in the "failed" state'.format(new_state))
            else:
                raise RuntimeError(
                    'cannot switch to state {}; '
                    'another operation ({}) is in progress'.format(
                        new_state, self.state))

    cdef _ensure_connected(self):
        if self.con_status != PGCON_OK:
            raise RuntimeError('not connected')

    # API for subclasses

    cdef _connect(self):
        cdef:
            WriteBuffer buf
            WriteBuffer outbuf

        if self.con_status != PGCON_BAD:
            raise RuntimeError('already connected')

        self._set_state(PGPROTO_AUTH)
        self.con_status = PGCON_STARTED

        # Assemble a startup message
        buf = WriteBuffer()

        # protocol version
        buf.write_int16(3)
        buf.write_int16(0)

        buf.write_bytestring(b'client_encoding')
        buf.write_bytestring("'{}'".format(self.encoding).encode('ascii'))

        buf.write_bytestring(b'edgedb_use_typeoids')
        buf.write_bytestring(b'false')

        buf.write_str('user', self.encoding)
        buf.write_str(self.con_params.user, self.encoding)

        buf.write_str('database', self.encoding)
        buf.write_str(self.con_params.database, self.encoding)

        # if self.con_params.server_settings is not None:
        #     for k, v in self.con_params.server_settings.items():
        #         buf.write_str(k, self.encoding)
        #         buf.write_str(v, self.encoding)

        buf.write_bytestring(b'')

        # Send the buffer
        outbuf = WriteBuffer()
        outbuf.write_int32(buf.len() + 4)
        outbuf.write_buffer(buf)
        self._write(outbuf)

    cdef _simple_query(self, bytes query):
        cdef WriteBuffer buf

        self._ensure_connected()
        self._set_state(PGPROTO_SIMPLE_QUERY)

        buf = WriteBuffer.new_message(b'Q')
        buf.write_bytestring(query)
        self._write(buf.end_message())

    cdef _execute_anonymous(self, bytes query, bytes bind_data):
        cdef:
            WriteBuffer packet
            WriteBuffer buf

        self._ensure_connected()
        self._set_state(PGPROTO_EXECUTE_ANONYMOUS)

        packet = WriteBuffer.new()

        buf = WriteBuffer.new_message(b'P')
        buf.write_bytestring(b'')  # statement name
        buf.write_bytestring(query)
        buf.write_int16(0)
        packet.write_buffer(buf.end_message())

        buf = WriteBuffer.new_message(b'B')
        buf.write_bytestring(b'')  # portal name
        buf.write_bytestring(b'')  # statement name
        buf.write_bytes(bind_data)
        packet.write_buffer(buf.end_message())

        buf = WriteBuffer.new_message(b'E')
        buf.write_bytestring(b'')  # portal name
        buf.write_int32(0)  # limit: number of rows to return; 0 - all
        packet.write_buffer(buf.end_message())

        packet.write_bytes(PG_SYNC_MESSAGE)
        self._write(packet)

    cdef _terminate(self):
        cdef WriteBuffer buf
        self._ensure_connected()
        buf = WriteBuffer.new_message(b'X')
        buf.end_message()
        self._write(buf)

    cdef _decode_row(self, const char* buf, ssize_t buf_len):
        pass

    # cdef _set_server_parameter(self, name, val):
    #     pass

    cdef _on_result(self):
        pass

    cdef _on_notice(self, parsed):
        pass

    cdef _on_notification(self, pid, channel, payload):
        pass

    cdef _on_connection_lost(self, exc):
        pass

    # asyncio callbacks:

    def data_received(self, data):
        self.buffer.feed_data(data)
        self._read_server_messages()

    def connection_made(self, transport):
        self.transport = transport

        sock = transport.get_extra_info('socket')
        if (sock is not None and
              (not hasattr(socket, 'AF_UNIX')
               or sock.family != socket.AF_UNIX)):
            sock.setsockopt(socket.IPPROTO_TCP,
                            socket.TCP_NODELAY, 1)

        try:
            self._connect()
        except Exception as ex:
            transport.abort()
            self.con_status = PGCON_BAD
            self._set_state(PGPROTO_FAILED)
            self._on_error(ex)

    def connection_lost(self, exc):
        self.con_status = PGCON_BAD
        self._set_state(PGPROTO_FAILED)
        self._on_connection_lost(exc)


cdef bytes PG_SYNC_MESSAGE = bytes(WriteBuffer.new_message(b'S').end_message())
