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
cdef class CompiledQuery:

    def __init__(self, uint64_t dbver,
                 bytes out_type_data,
                 bytes out_type_id,
                 bytes in_type_data,
                 bytes in_type_id,
                 bytes sql,
                 bytes sql_hash):

        self.dbver = dbver
        self.out_type_data = out_type_data
        self.out_type_id = out_type_id
        self.in_type_data = in_type_data
        self.in_type_id = in_type_id
        self.sql = sql
        self.sql_hash = sql_hash

    def __repr__(self):
        return (
            f'<CompiledQuery dbver={self.dbver} '
            f'in_tid={self.in_type_id!r} '
            f'out_tid={self.out_type_id!r} '
            f'sql_hash={self.sql_hash!r}>'
        )

    def __hash__(self):
        # We don't have a use case to make this hashable;
        # if we decide to make it hashable we'll need to
        # know if 'dbver' should be part of the hash etc.
        raise TypeError('unhashable type')
