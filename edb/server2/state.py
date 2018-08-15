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


import asyncio
import dataclasses
import enum
import hashlib
import os
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


class AuthenticationType(enum.IntEnum):

    PASSWORD = 1


@dataclasses.dataclass(frozen=True)
class Authentication:

    auth_type: AuthenticationType
    auth_hash: bytes

    _password_salt = os.urandom(20)
    _password_hashiter = 100_000

    @staticmethod
    async def from_password(cls, password: bytes):
        loop = asyncio.get_running_loop()

        auth_hash = await loop.run_in_executor(
            None, hashlib.pbkdf2_hmac,
            'sha256',
            password,
            Authentication._password_salt,
            Authentication._password_hashiter)

        return Authentication(
            auth_type=AuthenticationType.PASSWORD,
            auth_hash=auth_hash)


@dataclasses.dataclass(frozen=True)
class Database:

    username: str
    dbname: str

    def __post_init__(self):
        self._queries = lru.LRUMapping(
            maxsize=defines._MAX_QUERIES_CACHE)

        self._successful_auths = set()


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
