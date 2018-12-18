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


@cython.final
@cython.no_gc_clear
@cython.auto_pickle(True)
cdef class CompiledQuery:

    cdef:
        readonly uint64_t dbver
        readonly bytes out_type_data
        readonly bytes out_type_id
        readonly bytes in_type_data
        readonly bytes in_type_id
        readonly bytes sql
        readonly bytes sql_hash

        readonly bint has_ddl
        readonly bint multi_stmt
        readonly bint opens_tx

        readonly dict session_set_modaliases
        readonly dict session_reset_modaliases
        readonly dict session_set_configs
        readonly dict session_reset_configs
