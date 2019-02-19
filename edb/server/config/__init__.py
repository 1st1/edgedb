#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


from .ops import OpLevel, OpCode, Operation, apply, lookup
from .ops import spec_to_json, to_json, from_json
from .ops import value_to_json_edgeql, value_to_json
from .spec import Spec, Setting
from .types import ConfigType, Port


__all__ = (
    'settings',
    'apply', 'lookup',
    'Spec', 'Setting',
    'spec_to_json', 'to_json', 'from_json',
    'value_to_json_edgeql', 'value_to_json',
    'OpLevel', 'OpCode', 'Operation',
    'ConfigType', 'Port',
)


settings = Spec(
    Setting(
        '__internal_no_const_folding',
        type=bool, default=False,
        internal=True),

    Setting(
        '__internal_testmode',
        type=bool, default=False,
        internal=True),

    Setting(
        'ports',
        type=Port, set_of=True, default=frozenset(),
        system=True),
)
