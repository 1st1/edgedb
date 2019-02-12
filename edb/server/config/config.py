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


from __future__ import annotations

import immutables

from edb import errors
from edb.server import config


class Config:

    def __init__(self):
        self._vals = immutables.Map()

    def __hash__(self):
        return hash(self._vals)

    def __eq__(self, other):
        return type(self) is type(other) and self._vals == other._vals

    @classmethod
    def from_map(cls, vals: immutables.Map) -> Config:
        conf = cls.__new__(cls)
        conf._vals = vals
        return conf

    def _get_setting(self, name):
        try:
            return config.configs[name]
        except KeyError:
            raise errors.ConfigurationError(
                f'unknown config option {name!r}') from None

    def _set(self, name, val) -> Config:
        new_vals = self._vals.set(name, val)
        return self.from_map(new_vals)

    def as_map(self) -> immutables.Map:
        return self._vals

    def get(self, name):
        try:
            return self._vals[name]
        except KeyError:
            pass

        return self._get_setting(name).default

    def set(self, name, val) -> Config:
        setting = self._get_setting(name)
        assert not setting.is_set
        return self._set(name, val)

    def add(self, name, elval) -> Config:
        setting = self._get_setting(name)
        assert setting.is_set

        current_cval = self.get(name)
        new_cval = current_cval | {elval}
        return self._set(name, new_cval)

    def discard(self, name, elval) -> Config:
        setting = self._get_setting(name)
        assert setting.is_set

        current_cval = self.get(name)
        new_cval = current_cval - {elval}
        return self._set(name, new_cval)
