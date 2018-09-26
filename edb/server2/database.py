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


import time
import typing

from edb.server import defines

from . import query
from . import lru


class Database:

    _anon_eql_to_query: typing.Mapping[str, query.Query]
    _anon_query_to_compiler: typing.Mapping[query.Query, query.CompiledQuery]

    def __init__(self):
        self._dbver = time.monotonic_ns()

        self._anon_eql_to_query = lru.LRUMapping(
            defines._MAX_QUERIES_CACHE)

        # Global cache of compiled anonymous queries
        self._anon_query_to_compiled = lru.LRUMapping(
            defines._MAX_QUERIES_CACHE)

    def _signal_ddl(self):
        self._dbver = time.monotonic_ns()  # Advance the version
        self._invalidate_caches()

    def _invalidate_caches(self):
        self._anon_query_to_compiled.clear()

    def _cache_compiled_query(self, q: query.Query,
                              compiled: query.CompiledQuery):
        if q.prepared:
            raise RuntimeError('query {q!} is not anonymous')

        existing = self._anon_query_to_compiled.get(q)
        if existing is not None and existing.dbver > compiled.dbver:
            # We already have a cached query for a more recent DB version.
            return

        self._anon_query_to_compiled[q] = compiled

    def _new_view(self, *, user):
        return ConnectionDatabaseView(self, user=user)


class ConnectionDatabaseView:

    # prepared query name -> Query
    _prep_queries: typing.Dict[str, query.Query]
    _prep_query_to_compiled: typing.Dict[query.Query, query.CompiledQuery]
    _anon_query_to_compiled: typing.Mapping[query.Query, query.CompiledQuery]

    def __init__(self, db, *, user):
        self._db = db

        self._in_tx = False
        self._in_tx_with_ddl = False

        self._user = user

        self._prep_queries = {}
        self._prep_query_to_compiled = {}

        # Whenever we are in a transaction that had executed a
        # DDL command, we use this cache for compiled queries.
        self._anon_query_to_compiled = lru.LRUMapping(
            defines._MAX_QUERIES_CACHE)

    def _invalidate_local_caches(self):
        self._prep_query_to_compiled.clear()
        self._invalidate_local_anon_queries_cache()

    def _invalidate_local_anon_queries_cache(self):
        self._anon_query_to_compiled.clear()

    @property
    def in_tx(self):
        return self._in_tx

    @property
    def user(self):
        return self._user

    @property
    def dbver(self):
        return self._db._dbver

    def create_prepared_query(self, name: str, eql: str) -> query.Query:
        if not name:
            raise RuntimeError('empty name for a prepared statement')
        if name in self._prep_queries:
            raise RuntimeError(
                f'cannot create a prepared statement; another '
                f'prepared statement with {name!r} name already exists')

        q = query.Query(eql=eql, prepared=True)
        self._prep_queries[name] = q
        return q

    def lookup_prepared_query(self, name: str) -> query.Query:
        if not name:
            raise RuntimeError('empty name for a prepared statement')
        return self._prep_queries.get(name)

    def clear_prepared_query(self, name: str):
        if not name:
            raise RuntimeError('empty name for a prepared statement')
        q = self._prep_queries.pop(name, None)
        if q is None:
            raise RuntimeError(
                f'cannot locate prepared statement with {name!r} name')
        self._prep_query_to_compiled.pop(q, None)

    def create_anonymous_query(self, eql: str) -> query.Query:
        q = query.Query(eql=eql)
        self._db._anon_eql_to_query[eql] = q
        return q

    def lookup_anonymous_query(self, eql: str) -> typing.Optional[query.Query]:
        return self._db._anon_eql_to_query.get(eql)

    def lookup_anonymous_compiled_query(self, eql: str) -> query.CompiledQuery:
        q = self._db._anon_eql_to_query.get(eql)
        if q is None:
            return
        return self.lookup_compiled_query(q)

    def lookup_compiled_query(self, q: query.Query) -> query.CompiledQuery:
        compiled: query.CompiledQuery

        if q.prepared:
            compiled = self._prep_query_to_compiled.get(q)
        else:
            if self._in_tx_with_ddl:
                compiled = self._anon_query_to_compiled.get(q)
            else:
                compiled = self._db._anon_query_to_compiled.get(q)

        if compiled.dbver == self.dbver:
            return compiled

    def cache_compiled_query(self, q: query.Query,
                             compiled: query.CompiledQuery):
        if q.prepared:
            self._prep_query_to_compiled[q] = compiled
        else:
            if self._in_tx_with_ddl:
                self._anon_query_to_compiled[q] = compiled
            else:
                self._db._cache_compiled_query(q, compiled)

    def signal_ddl(self):
        if self._in_tx:
            # In a transaction, record that there was a DDL
            # statement.
            self._in_tx_with_ddl = True
        else:
            # Not in a transaction; executed DDL affects all
            # connections immediately.
            self._db._signal_ddl()

        # Whenever we execute a DDL statement (in transaction or not)
        # we want to invalidate local caches (i.e. caches for this
        # particular connection).
        self._invalidate_local_caches()

    def tx_begin(self):
        if self._in_fx:
            raise RuntimeError('cannot begin; already in transaction')

        self._in_tx = True

    def tx_commit(self):
        if not self._in_tx:
            raise RuntimeError('cannot commit; not in transaction')

        if self._in_tx_with_ddl:
            # This transaction had DDL commands in it:
            # signal that to all connections; invalidate all local
            # caches (any compiled query will have to be recompiled
            # anyways because of global DB version bump.)
            self._db._signal_ddl()
            self._invalidate_local_caches()

        self._in_tx = False
        self._in_tx_with_ddl = False

    def tx_rollback(self):
        if not self._in_tx:
            raise RuntimeError('cannot rollback; not in transaction')

        if self._in_tx_with_ddl:
            # We definitely no longer need our local anonymous queries
            # cache: invalidate it.
            self._invalidate_local_anon_queries_cache()

        self._in_tx = False
        self._in_tx_with_ddl = False


class DatabaseIndex:

    def __init__(self):
        self._dbs = {}

    def new_view(self, dbname, *, user) -> ConnectionDatabaseView:
        try:
            db = self._dbs[dbname]
        except KeyError:
            raise RuntimeError(f'no DB {dbname!r} is registered') from None

        return db._new_view(user=user)

    def is_registered(self, dbname) -> bool:
        return dbname in self._dbs

    def register(self, dbname) -> None:
        if self.get(dbname) is not None:
            raise RuntimeError(
                f'db {dbname!r} is already registered')
        db = Database(dbname)
        self._dbs[dbname] = db
