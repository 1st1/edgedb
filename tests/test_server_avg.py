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


import math
import unittest

from edb.server2 import avg


class TestServerAvg(unittest.TestCase):

    def test_server_avg_01(self):
        a = avg.RollingAverage(3)

        a.add(1)
        self.assertTrue(math.isclose(a.avg, 1.0))

        a.add(1)
        self.assertTrue(math.isclose(a.avg, 1.0))

        a.add(1)
        self.assertTrue(math.isclose(a.avg, 1.0))

        for _ in range(100):
            a.add(1.1)
            a.add(1.1)
            a.add(1.1)

        self.assertTrue(math.isclose(a.avg, 1.1))

    def test_server_avg_02(self):
        a = avg.RollingAverage(3)
        self.assertEqual(a.avg, 0)

        for _ in range(100):
            a.add(0)

        self.assertEqual(a.avg, 0)

        a.add(1)
        self.assertTrue(math.isclose(a.avg, 1 / 3))

        a.add(2)
        self.assertTrue(math.isclose(a.avg, 3 / 3))

        a.add(3)
        self.assertTrue(math.isclose(a.avg, 6 / 3))

        a.add(5)
        self.assertTrue(math.isclose(a.avg, 10 / 3))

        a.add(1)
        self.assertTrue(math.isclose(a.avg, 9 / 3))

    def test_server_avg_03(self):
        a = avg.RollingAverage(5)

        self.assertEqual(a.avg, 0)

        a.add(1)
        self.assertTrue(math.isclose(a.avg, 1))

        a.add(2)
        self.assertTrue(math.isclose(a.avg, 3 / 2))

        a.add(3)
        self.assertTrue(math.isclose(a.avg, 6 / 3))

        a.add(5)
        self.assertTrue(math.isclose(a.avg, 11 / 4))

        a.add(5)
        self.assertTrue(math.isclose(a.avg, 16 / 5))

        a.add(5)
        self.assertTrue(math.isclose(a.avg, 20 / 5))

        # ticks < 0 will round to 0
        a.add(-5)
        self.assertTrue(math.isclose(a.avg, 18 / 5))
        a.add(-0.1)
        self.assertTrue(math.isclose(a.avg, 15 / 5))
        a.add(0)
        self.assertTrue(math.isclose(a.avg, 10 / 5))

    def test_server_avg_04(self):
        with self.assertRaisesRegex(ValueError,
                                    'expected to be greater than 2'):
            avg.RollingAverage(1)
