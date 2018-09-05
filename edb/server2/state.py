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
from . import lru


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


@dataclasses.dataclass
class Database:

    username: bytes
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

    def get(self, dbname, username) -> typing.Optional[Database]:
        return self._dbs.get((dbname, username))

    def register(self, dbname, username) -> Database:
        if self.get(dbname, username) is not None:
            raise RuntimeError(
                f'db ({dbname}, {username}) is already registered')
        db = Database(username, dbname)
        self._dbs[(dbname, username)] = db
        return db
