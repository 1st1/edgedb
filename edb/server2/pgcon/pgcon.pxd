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
                         int32_t, uint32_t, int64_t, uint64_t

from edb.server2.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,
)

from edb.server2.pgproto.debug cimport PG_DEBUG

include "./corepgcon.pxd"


cdef class PGProto(CorePGProto):

    cdef:
        object loop
        object address
        object cancel_sent_waiter
        object cancel_waiter
        object waiter
        bint return_extra
        object create_future
        object completed_callback
        object connection
        bint is_reading

        bytes last_query

        bint closing

        readonly uint64_t queries_count

    cdef _check_state(self)
    cdef _new_waiter(self)
    cdef _coreproto_error(self)

    cdef _on_result__connect(self, object waiter)
    cdef _on_result__execute_anonymous(self, object waiter)

    cdef _handle_waiter_on_connection_lost(self, cause)

    cdef _dispatch_result(self)

    cdef inline resume_reading(self)
    cdef inline pause_reading(self)
