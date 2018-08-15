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


class MappedDeque:
    """A deque-like object with an O(1) discard operation."""

    def __init__(self, source=None):
        if source is None:
            self._list = collections.OrderedDict()
        else:
            self._list = collections.OrderedDict.fromkeys(source)

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

    def popleftitem(self):
        return self._list.popitem(last=False)

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

    def append(self, key, o):
        if o in self._lru_list:
            raise ValueError(f'{key!r}:{o!r} is already in the index {self!r}')
        try:
            items = self._index[key]
        except KeyError:
            items = self._index[key] = MappedDeque()

        items.append(o)
        self._lru_list.append(o, key)

    def popleft(self):
        o, key = self._lru_list.popleftitem()

        items = self._index[key]
        items.discard(o)

        if not items:
            del self._index[key]

        return o

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

    def count_keys(self):
        return len(self._index)

    def count(self):
        return len(self._lru_list)

    def lru(self):
        return iter(self._lru_list)


class BaseConnectionHolder:

    def __init__(self, pool):
        self._con = None
        self._dbname = None
        self._user = None
        self._password = None
        self._pool = pool

    async def connect(self, dbname, user, password):
        if self.connected():
            raise RuntimeError(
                f'cannot connect connection holder to ({dbname!r}, {user!r}) '
                f'as it is already connected to ({self._dbname!r}, '
                f'{self._user})')

        self._con = await self._connect(dbname, user, password)
        self._dbname = dbname
        self._user = user
        self._password = password

    async def clear(self):
        if self._con is not None:
            try:
                await self._close(self._con)
            finally:
                self._user = None
                self._password = None
                self._con = None

    def connected(self):
        return self._con is not None

    def key(self):
        return (self._dbname, self._user)

    async def _connect(self, dbname, user, password):
        raise NotImplementedError

    async def _close(self, con):
        raise NotImplementedError


class BasePool:

    def __init__(self, *, max_capacity, concurrency, loop):
        if max_capacity <= 0:
            raise RuntimeError(
                f'{type(self).__name__} expects max_capacity {max_capacity} '
                f'to be greater than 0')
        if concurrency >= max_capacity:
            raise RuntimeError(
                f'{type(self).__name__} expects max_capacity {max_capacity} '
                f'to be greater than concurrency {concurrency}')

        self._capacity = max_capacity
        self._concurrency = concurrency

        self._loop = loop
        self._closed = False

        self._holders = [self._new_holder() for _ in range(max_capacity)]
        self._empty_holders = collections.deque(self._holders)
        self._unused_holders = LRUIndex()
        self._used_holders = set()

        self._lock = asyncio.BoundedSemaphore(loop=loop, value=concurrency)

    def _new_holder(self):
        raise NotImplementedError

    @property
    def empty_holders_count(self):
        return len(self._empty_holders)

    @property
    def unused_holders_count(self):
        return self._unused_holders.count()

    @property
    def used_holders_count(self):
        return len(self._used_holders)

    async def acquire(self, dbname, user, password):
        if self._closed:
            raise RuntimeError(f'{type(self).__name__} is closed')
        await self._lock.acquire()
        try:
            holder = await self._acquire(dbname, user, password)
            if not holder.connected():
                raise RuntimeError('connection holder is not connected')
            if holder in self._used_holders:
                raise RuntimeError('a holder was not properly released')
        except Exception:
            self._lock.release()
            raise
        else:
            self._used_holders.add(holder)
            return holder

    async def _acquire(self, dbname, user, password) -> BaseConnectionHolder:
        # Let's see if there's an existing connection to (dbname, user)
        # that currently isn't in use.
        holder = self._unused_holders.pop((dbname, user))
        if holder is not None:
            return holder

        # So there are no currently unused connections to (dbname, user);
        # let's check if we have an empty holder that we can start
        # using.
        if self._empty_holders:
            holder = self._empty_holders.popleft()
            try:
                await holder.connect(dbname, user, password)
            except Exception:
                self._empty_holders.append(holder)
                raise
            return holder

        # There are no empty holders.  Let's pop the least used
        # "unused connection", close its connection, and make a
        # new connection to (dbname, user).

        # Since concurrency must be greater than capacity (enforced
        # in __init__ and protected by a semaphore), we must have
        # some currently unused connection holders.
        assert self._unused_holders.count() > 0

        holder = self._unused_holders.popleft()
        try:
            await holder.clear()
            await holder.connect(dbname, user, password)
        except Exception:
            self._empty_holders.append(holder)
            raise
        else:
            return holder

    async def release(self, holder: BaseConnectionHolder):
        if holder not in self._used_holders:
            raise RuntimeError(
                'unable to release a holder that was not previously acquired')

        self._used_holders.discard(holder)
        self._unused_holders.append(holder.key(), holder)
        self._lock.release()

    async def close(self):
        self._closed = True

        async with taskgroup.TaskGroup() as g:
            for _ in range(self._concurrency):
                g.create_task(self._lock.acquire())

        async with taskgroup.TaskGroup() as g:
            for holder in self._holders:
                g.create_task(holder.clear())


class PGConnectionHolder(BaseConnectionHolder):

    async def _connect(self, dbname, user, password):
        raise NotImplementedError

    async def _close(self, con):
        raise NotImplementedError


class PGPool(BasePool):

    def _new_holder(self):
        return PGConnectionHolder(self)
