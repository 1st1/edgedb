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


import immutables

from . import setting as cs
from . import types as ct
from . import spec

from .config import Config


__all__ = ('configs', 'Config')


configs = immutables.Map(
    __internal_no_const_folding=cs.setting(
        type=bool,
        default=False,
        level=cs.ConfigLevel.SESSION),

    __internal_testmode=cs.setting(
        type=bool,
        default=False,
        level=cs.ConfigLevel.SESSION),

    ports=cs.setting(
        type=ct.Port,
        default=frozenset(),
        level=cs.ConfigLevel.SYSTEM,
        is_set=True)
)


settings = spec.Spec(
    spec.Setting(
        '__internal_no_const_folding',
        type=bool, default=False,
        internal=True),

    spec.Setting(
        '__internal_testmode',
        type=bool, default=False,
        internal=True),

    spec.Setting(
        'ports',
        type=ct.Port, set_of=True, default=frozenset(),
        system=True),
)
