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


cdef enum PGConnectionStatus:
    PGCON_OK = 1
    PGCON_BAD = 2
    PGCON_STARTED = 3           # Waiting for connection to be made.


cdef enum PGProtocolState:
    PGPROTO_IDLE = 0

    PGPROTO_FAILED = 1
    PGPROTO_ERROR_CONSUME = 2
    PGPROTO_CANCELLED = 3

    PGPROTO_AUTH = 10
    PGPROTO_EXECUTE_ANONYMOUS = 11


cdef enum PGResultType:
    RESULT_OK = 1
    RESULT_FAILED = 2


cdef enum PGTransactionStatus:
    PQTRANS_IDLE = 0                 # connection idle
    PQTRANS_ACTIVE = 1               # command in progress
    PQTRANS_INTRANS = 2              # idle, within transaction block
    PQTRANS_INERROR = 3              # idle, within failed transaction
    PQTRANS_UNKNOWN = 4              # cannot determine status


cdef enum PGAuthenticationMessage:
    PGAUTH_SUCCESSFUL = 0
    PGAUTH_REQUIRED_KERBEROS = 2
    PGAUTH_REQUIRED_PASSWORD = 3
    PGAUTH_REQUIRED_PASSWORDMD5 = 5
    PGAUTH_REQUIRED_SCMCRED = 6
    PGAUTH_REQUIRED_GSS = 7
    PGAUTH_REQUIRED_GSS_CONTINUE = 8
    PGAUTH_REQUIRED_SSPI = 9


cdef dict PGAUTH_METHOD_NAME = {
    PGAUTH_REQUIRED_KERBEROS: 'kerberosv5',
    PGAUTH_REQUIRED_PASSWORD: 'password',
    PGAUTH_REQUIRED_PASSWORDMD5: 'md5',
    PGAUTH_REQUIRED_GSS: 'gss',
    PGAUTH_REQUIRED_SSPI: 'sspi',
}


cdef class CorePGProto:

    cdef:
        ReadBuffer buffer
        bint _skip_discard
        bint _discard_data

        WriteBuffer auth_msg

        PGConnectionStatus con_status
        PGProtocolState state
        PGTransactionStatus xact_status

        str encoding

        object transport
        object edgecon

        str dbname

        readonly int32_t backend_pid
        readonly int32_t backend_secret

        ## Result
        PGResultType result_type
        object result
        WriteBuffer result_data
        bytes result_status_msg

        # True - completed, False - suspended
        bint result_execute_completed

    cdef _process__auth(self, char mtype)
    cdef _process__execute_anonymous(self, char mtype)

    cdef _parse_msg_authentication(self)
    cdef _parse_msg_parameter_status(self)
    cdef _parse_msg_notification(self)
    cdef _parse_msg_backend_key_data(self)
    cdef _parse_msg_ready_for_query(self)
    cdef _parse_msg_error_response(self, is_error)
    cdef _parse_msg_command_complete(self)

    cdef _write(self, buf)
    cdef inline _write_sync_message(self)

    cdef _read_server_messages(self)

    cdef _push_result(self)
    cdef _reset_result(self)
    cdef _set_state(self, PGProtocolState new_state)

    cdef _ensure_connected(self)


    cdef _connect(self)
    cdef _execute_anonymous(self, edgecon, bytes query, bytes bind_data)

    cdef _terminate(self)

    cdef _decode_row(self, const char* buf, ssize_t buf_len)

    cdef _on_result(self)
    cdef _on_notification(self, pid, channel, payload)
    cdef _on_notice(self, parsed)
    # cdef _set_server_parameter(self, name, val)
    cdef _on_connection_lost(self, exc)
