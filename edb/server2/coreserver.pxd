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


# cython: language_level=3


from libc.stdint cimport int16_t, int32_t, uint16_t, uint32_t, int64_t, uint64_t

from .debug cimport PGBASE_DEBUG

include "pgbase/inc/consts.pxi"
include "pgbase/inc/buffer.pxd"
include "edgecon.pxd"
include "pgcon.pxd"


cdef class CoreServer:
    cdef:
        readonly object _loop

    cdef edgecon_register(self, EdgeConnection con)
    cdef edgecon_unregister(self, EdgeConnection con)
    cdef edgecon_authorize(self, EdgeConnection con,
                           str user, str password, str dbname)

    cdef edgecon_parse(self, EdgeConnection con, str stmt_name, str query)
