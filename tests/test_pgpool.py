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


from edb.server2 import pgpool
from edb.server import _testbase as tb


class TestLRUIndex(tb.TestCase):

    def test_lru_index_1(self):
        idx = pgpool.LRUIndex()

        idx.put(1, '1')
        idx.put(2, '2')
        idx.put(1, '11')
        idx.put(2, '22')
        idx.put(1, '111')

        self.assertEqual(idx.pop(1), '111')
        self.assertEqual(idx.pop(1), '11')
        self.assertEqual(idx.pop(2), '22')

        idx.put(1, '11')
        self.assertEqual(idx.pop(1), '11')
        self.assertEqual(idx.pop(2), '2')
        self.assertEqual(idx.pop(1), '1')

        self.assertIsNone(idx.pop(2))
        self.assertIsNone(idx.pop(1))

        self.assertEqual(idx._index, {})

    def test_lru_index_2(self):
        idx = pgpool.LRUIndex()

        idx.put(1, '1')
        idx.put(2, '2')
        idx.put(1, '11')
        idx.put(2, '22')
        idx.put(1, '111')

        self.assertTrue(idx.unref('11'))
        self.assertFalse(idx.unref('11'))
        self.assertFalse(idx.unref('11'))

        self.assertEqual(idx.pop(1), '111')
        self.assertEqual(idx.pop(2), '22')

        idx.put(1, '11')
        self.assertEqual(idx.pop(1), '11')
        self.assertEqual(idx.pop(2), '2')
        self.assertEqual(idx.pop(1), '1')

        self.assertIsNone(idx.pop(2))
        self.assertIsNone(idx.pop(1))

        self.assertEqual(idx._index, {})

    def test_lru_index_3(self):
        idx = pgpool.LRUIndex()

        o1 = object()
        o11 = object()

        idx.put(1, o1)
        idx.put(1, o11)

        with self.assertRaisesRegex(ValueError, 'already in the index'):
            idx.put(1, o1)

        self.assertTrue(idx.unref(o1))
        self.assertFalse(idx.unref(o1))

        idx.put(1, o1)
        self.assertIs(idx.pop(1), o1)
        self.assertIs(idx.pop(1), o11)

        self.assertEqual(idx._index, {})

        self.assertFalse(idx.unref(o1))


class TestMappedDeque(tb.TestCase):

    def test_mapped_deque_1(self):
        lst = pgpool.MappedDeque()

        o1 = object()
        o2 = object()
        o3 = object()
        o4 = object()

        lst.append(o1)
        lst.append(o2)
        lst.append(o3)
        lst.append(o4)

        self.assertEqual(list(lst), [o1, o2, o3, o4])

        lst.discard(o2)
        self.assertEqual(list(lst), [o1, o3, o4])

        self.assertIs(lst.popleft(), o1)
        self.assertEqual(list(lst), [o3, o4])

        with self.assertRaisesRegex(ValueError, 'already in the list'):
            lst.append(o3)

        lst.append(o1)
        self.assertEqual(list(lst), [o3, o4, o1])

        with self.assertRaises(LookupError):
            lst.discard(o2)

        self.assertEqual(len(lst), 3)
        self.assertTrue(bool(lst))

        self.assertIs(lst.popleft(), o3)
        self.assertIs(lst.popleft(), o4)
        self.assertIs(lst.popleft(), o1)

        self.assertEqual(list(lst), [])
        self.assertEqual(len(lst), 0)
        self.assertFalse(bool(lst))

    def test_mapped_deque_2(self):
        orig = [1, 2, 3]
        lst = pgpool.MappedDeque(orig)
        self.assertEqual(list(lst), [1, 2, 3])
        orig.pop()
        self.assertEqual(list(lst), [1, 2, 3])
