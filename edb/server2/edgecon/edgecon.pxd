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


cdef enum EdgeConnectionStatus:
    EDGECON_NEW = 0
    EDGECON_STARTED = 1
    EDGECON_OK = 2
    EDGECON_BAD = 3


cdef enum EdgeProtoState:
    EDGEPROTO_IDLE = 0

    EDGEPROTO_AUTH = 1


    EDGEPROTO_CLOSED = 100


cdef class EdgeConnection:

    cdef:
        EdgeConnectionStatus _con_status
        EdgeProtoState _state
        bint _parsing
        bint _reading_messages
        str _dbname
        str _user
        str _id
        dict _queries
        object _transport
        CoreServer _server
        ReadBuffer buffer

    cdef _recode_args(self, bytes bind_args)

    cdef _read_buffer_messages(self)
    cdef _write(self, buf)

    cdef _handle__startup(self)

    cdef _handle__auth(self, char mtype)

    cdef _handle__simple_query(self)

    cdef _handle__describe(self)

    cdef _handle__parse(self)

    cdef _handle__sync(self)

    cdef _handle__execute(self)

    cdef _pause_parsing(self)
    cdef _resume_parsing(self)
