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


cdef class PostgresConnection:
    def __init__(self):
        pass

    # protocol methods

    def connection_made(self, transport):
        pass

    def connection_lost(self, exc):
        pass

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def data_received(self, data):
        pass

    def eof_received(self):
        pass
