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
import collections


from . import taskgroup
# from . import coreserver as core


class ConnectionHolder:

    def __init__(self, pool):
        self._con = None
        self._user = None
        self._password = None
        self._pool = pool

    async def connect(self, dbname, user, password):
        self._con = await self._connect(dbname, user, password)
        self._user = user
        self._password = password

    async def clear(self):
        if self._con is not None:
            try:
                await self._con.close()
            finally:
                self._user = None
                self._password = None
                self._con = None

    def connected(self):
        return self._con is None

    async def _connect(self, dbname, user, password):
        raise NotImplementedError


class MappedDeque:
    """A deque-like object with an O(1) discard operation."""

    def __init__(self, source=None):
        self._list = collections.OrderedDict()
        if source is not None:
            for el in source:
                self._list[el] = True

    def __contains__(self, item):
        return item in self._list

    def __getitem__(self, item):
        return self._list[item]

    def discard(self, item):
        try:
            self._list.pop(item)
        except KeyError:
            raise LookupError(f'{item!r} is not in {self!r}') from None

    def append(self, item, val=None):
        if item in self._list:
            raise ValueError(f'{item!r} is already in the list {self!r}')
        self._list[item] = val

    def pop(self):
        item, _ = self._list.popitem(last=True)
        return item

    def popleft(self):
        item, _ = self._list.popitem(last=False)
        return item

    def __len__(self):
        return len(self._list)

    def __bool__(self):
        return bool(self._list)

    def __iter__(self):
        return iter(self._list)

    def __repr__(self):
        return f'<{type(self).__name__} {list(self)!r} {id(self):#x}>'


class LRUIndex:
    """A multidict-like mapping with internal LRU lists.

    Key properties:

    * One key can be mapped to a list of objects.

    * Objects must be unique for the entire mapping.  It's an error
      if two different keys point to one object, or of one object is
      referenced by the same key more than once.

    * Every key maps to a LIFO list of objects internally.  LIFO
      is essential to make the global LRU list of connections work:
      if a DB has too many open connections some of them will become
      unused for long enough period of time to be closed.

    * There's a global LIFO list of objects, accessible via the
      "lru()" method.  The "count()" method returns the total number
      of objects in the index.
    """

    def __init__(self):
        self._index = {}
        self._lru_list = MappedDeque()

    def pop(self, key):
        try:
            items = self._index[key]
        except KeyError:
            return None

        o = items.pop()
        self._lru_list.discard(o)

        if not items:
            del self._index[key]
        return o

    def put(self, key, o):
        if o in self._lru_list:
            raise ValueError(f'{key!r}:{o!r} is already in the index {self!r}')
        try:
            items = self._index[key]
        except KeyError:
            items = self._index[key] = MappedDeque()

        items.append(o)
        self._lru_list.append(o, key)

    def discard(self, o):
        try:
            key = self._lru_list[o]
        except KeyError:
            return False

        items = self._index[key]
        items.discard(o)
        self._lru_list.discard(o)

        if not items:
            del self._index[key]

        return True

    def count(self):
        return len(self._lru_list)

    def lru(self):
        return iter(self._lru_list)


class BasePool:

    def __init__(self, *, capacity, concurrency, loop):
        if concurrency <= capacity:
            raise RuntimeError(
                f'{type(self).__name__} expects capacity {capacity} to be '
                f'less than concurrency {concurrency}')
        self._capacity = capacity
        self._concurrency = concurrency

        self._loop = loop
        self._closed = False

        self._holders = [self._new_holder() for _ in range(capacity)]
        self._empty_holders = collections.deque(self._holders)
        self._unused_holders = LRUIndex()

        self._lock = asyncio.BoundedSemaphore(loop=loop, value=concurrency)

    def _new_holder(self):
        raise NotImplementedError

    async def acquire(self, dbname, user, password):
        await self._lock.acquire()
        try:
            holder = await self._acquire(dbname, user)
            if not holder.connected():
                raise RuntimeError('connection holder is not connected')
        except Exception:
            self._lock.release()
            raise
        else:
            return holder

    async def _acquire(self, dbname, user, password) -> ConnectionHolder:
        du = (dbname, user)
        holder = self._unused_holders_index.pop(du)
        if holder is not None:
            return holder

        if self._empty_holders:
            holder = self._empty_holders.popleft()
            try:
                await holder.connect(dbname, user, password)
            except Exception:
                self._empty_holders.append(holder)
                raise
            return holder

        holder = self._lru_holders.popleft()
        if holder.connected():
            if not self._holders_index_du.pop_specific(holder.key(), holder):
                1 / 0
            await holder.clear()
        try:
            await holder.connect(dbname, user, password)
        except Exception:
            self._empty_holders.append(holder)
            raise
        return holder

    async def release(self, holder: ConnectionHolder):
        self._lock.release()
        self._holders_index_du.put(holder.key(), holder)
        self._lru_holders.append(holder)

    async def close(self):
        async with taskgroup.TaskGroup() as g:
            for holder in self._holders:
                g.create_task(holder.close())
