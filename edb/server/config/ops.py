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


import enum
import json
import typing

import immutables

from edb import errors

from . import spec
from . import types


class Level(enum.Enum):

    SESSION = enum.auto()
    SYSTEM = enum.auto()


class OpCode(enum.Enum):

    CONFIG_ADD = enum.auto()
    CONFIG_REM = enum.auto()
    CONFIG_SET = enum.auto()


class Operation(typing.NamedTuple):

    opcode: OpCode
    level: Level
    setting_name: str
    value: typing.Union[str, int, bool]


def _validate_value(setting: spec.Setting, value: object):
    if issubclass(setting.type, types.ConfigType):
        try:
            return setting.type.from_pyvalue(value)
        except (ValueError, TypeError):
            raise errors.ConfigurationError(
                f'invalid value type for the {setting.name!r} setting')
    else:
        if isinstance(value, setting.type):
            return value
        else:
            raise errors.ConfigurationError(
                f'invalid value type for the {setting.name!r} setting')


def apply(spec: spec.Spec,
          storage: immutables.Map,
          ops: typing.List[Operation]) -> immutables.Map:

    for op in ops:
        setting = spec[op.setting_name]
        value = _validate_value(setting, op.value)

        if op.opcode is OpCode.CONFIG_SET:
            assert not setting.set_of
            storage = storage.set(op.setting_name, value)

        elif op.opcode is OpCode.CONFIG_ADD:
            assert setting.set_of
            exist_value = storage.get(op.setting_name, setting.default)
            new_value = exist_value | {value}
            storage = storage.set(op.setting_name, new_value)

        elif op.opcode is OpCode.CONFIG_REM:
            assert setting.set_of
            exist_value = storage.get(op.setting_name, setting.default)
            new_value = exist_value - {value}
            storage = storage.set(op.setting_name, new_value)

    return storage


def to_json(spec: spec.Spec, storage: immutables.Map):
    dct = {}
    for name, value in storage.items():
        setting = spec[name]
        if setting.set_of:
            value = list(value)
        if issubclass(setting.type, types.ConfigType):
            value = [v.to_json() for v in value]
        dct[name] = value
    return json.dumps(dct)


def to_json_edgeql(spec: spec.Spec, storage: immutables.Map):
    dct = {}
    for name, value in storage.items():
        setting = spec[name]
        if setting.set_of:
            value = list(value)
        if issubclass(setting.type, types.ConfigType):
            value = '{' + ','.join(v.to_edgeql() for v in value) + '}'
        else:
            value = repr(value)
        dct[name] = value
    return json.dumps(dct)
