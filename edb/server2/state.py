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
import typing

from edb.server import defines

from . import avg
from . import lru


_QUERY_ROLLING_AVG_LEN = 10
_QUERIES_ROLLING_AVG_LEN = 300


@dataclasses.dataclass(frozen=True)
class CompiledQuery:

    dbver: int

    out_type_data: bytes
    out_type_id: bytes
    in_type_data: bytes
    in_type_id: bytes

    sql: bytes


@dataclasses.dataclass
class Query:

    compiled: typing.Optional[CompiledQuery]

    def __post_init__(self):
        self._exec_avg = avg.RollingAverage(_QUERY_ROLLING_AVG_LEN)

    def log_time(self, delta):
        self._exec_avg.add(delta)

    @property
    def avg(self):
        return self._exec_avg.avg


@dataclasses.dataclass
class Database:

    dbname: bytes

    def __post_init__(self):
        self._queries = lru.LRUMapping(
            maxsize=defines._MAX_QUERIES_CACHE)

        self._successful_auths = set()

    def lookup_query(self, eql: str) -> Query:
        try:
            return self._queries[eql]
        except KeyError:
            return None

    def add_query(self, eql: str, compiled: CompiledQuery):
        self._queries[eql] = q = Query(compiled=compiled)
        return q


class DatabasesIndex:

    def __init__(self):
        self._dbs = {}

    def get(self, dbname) -> typing.Optional[Database]:
        return self._dbs.get(dbname)

    def register(self, dbname) -> Database:
        if self.get(dbname) is not None:
            raise RuntimeError(
                f'db {dbname!r} is already registered')
        db = Database(dbname)
        self._dbs[dbname] = db
        return db
