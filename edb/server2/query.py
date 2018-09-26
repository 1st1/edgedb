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


import dataclasses

from edb.server import defines

from . import avg


@dataclasses.dataclass(frozen=True)
class CompiledQuery:

    dbver: int

    out_type_data: bytes
    out_type_id: bytes
    in_type_data: bytes
    in_type_id: bytes

    sql: bytes


class Query:

    def __init__(self, *, eql: str, prepared: bool=False):
        self._eql = eql
        self._prepared = bool(prepared)

        self._exec_avg = avg.RollingAverage(defines._QUERY_ROLLING_AVG_LEN)

    @property
    def prepared(self):
        return self._prepared

    def __eq__(self, other):
        if not isinstance(other, Query):
            return NotImplemented
        if self._prepared != other._prepared:
            # This shouldn't ever happen; if it does it means we have
            # a logical bug in ConnectionDatabaseView.
            raise TypeError(
                'comparing a prepared query with a non-prepared one')
        return self._eql == other._eql

    def __hash__(self):
        return hash(self._eql)

    def log_time(self, delta):
        self._exec_avg.add(delta)

    @property
    def exec_avg(self):
        return self._exec_avg.avg
