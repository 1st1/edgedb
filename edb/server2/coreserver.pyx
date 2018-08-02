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


# cython: language_level=3


import collections


include "./pgbase/pgbase.pyx"

include "./edgecon/edgecon.pyx"

include "./pgcon/pgcon.pyx"


cdef class CoreServer:
    def __init__(self, loop):
        self._loop = loop

    cdef edgecon_register(self, EdgeConnection con):
        pass

    cdef edgecon_unregister(self, EdgeConnection con):
        pass

    cdef edgecon_authorize(self, EdgeConnection con,
                           str user, str password, str dbname):
        self._loop.create_task(
            self._authorize(user, password, dbname, con._on_server_auth))

    cdef edgecon_parse(self, EdgeConnection con, str stmt_name, str query):
        self._loop.create_task(
            self._parse(con._dbname, stmt_name, query, con._on_server_parse))

    cdef edgecon_execute(self, EdgeConnection con, query, bytes bind_args):
        self._loop.create_task(self._execute(con, query, bind_args))

    cdef edgecon_simple_query(self, EdgeConnection con, str query):
        self._loop.create_task(self._simple_query(con, query))
