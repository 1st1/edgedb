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


from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport uint32_t


cdef class RollingAverage:

    cdef:
        uint32_t _cur
        uint32_t _maxlen
        uint32_t _len
        double *_vals

        readonly double avg

    def __cinit__(self, uint32_t length):
        self._vals = NULL

    def __dealloc__(self):
        PyMem_Free(self._vals)

    def __init__(self, uint32_t length):
        if length <= 2:
            raise ValueError(
                f'RollingAverage history length is expected '
                f'to be greater than 2, got {length}')

        self._maxlen = length
        self._len = 0
        self._cur = 0
        self.avg = 0

        self._vals = <double *>PyMem_Malloc(length * sizeof(double))
        if self._vals == NULL:
            raise MemoryError
        for i in range(length):
            self._vals[i] = 0

    def add(self, double tick):
        cdef:
            double oldval
            uint32_t oldlen
            uint32_t newlen

        if tick < 0:
            tick = 0

        self._cur += 1
        if self._cur >= self._maxlen:
            self._cur = 0

        oldlen = self._len
        if self._len < self._maxlen:
            self._len += 1
        newlen = self._len

        oldval = self._vals[self._cur]
        self._vals[self._cur] = tick

        self.avg = ((self.avg * oldlen) - oldval + tick) / newlen
