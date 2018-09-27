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


import argparse
import asyncio
import importlib
import base64
import pickle
import traceback

import uvloop

from . import amsg


def load_class(cls_name):
    mod_name, _, cls_name = cls_name.rpartition('.')
    mod = importlib.import_module(mod_name)
    cls = getattr(mod, cls_name)
    return cls


async def worker(cls, cls_args, sockname):
    con = await amsg.worker_connect(sockname)
    worker = cls(*cls_args)

    while True:
        req = await con.next_request()

        try:
            methname, args = pickle.loads(req)
        except Exception as ex:
            ex = clear_exception_frames(ex)
            data = (1, ex)
        else:
            meth = getattr(worker, methname)

            try:
                res = await meth(*args)
                data = (0, res)
            except Exception as ex:
                ex = clear_exception_frames(ex)
                data = (1, ex)

        try:
            pickled = pickle.dumps(data)
        except Exception as ex:
            try:
                ex_str = str(ex)
            except Exception as ex2:
                ex_str = f'{type(ex2).__name__}: cannot call ' \
                         f'{type(ex).__name__}.__str__'

            data = RuntimeError(f'cannot pickle result: {ex_str}')
            pickled = pickle.dumps((1, data))

        await con.reply(pickled)


def run_worker(cls, cls_args, sockname):
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.run(worker(cls, cls_args, sockname))


def clear_exception_frames(er):

    def _clear_exception_frames(er, visited):
        if er in visited:
            return
        visited.add(er)

        traceback.clear_frames(er.__traceback__)

        if er.__cause__ is not None:
            er.__cause__ = _clear_exception_frames(er.__cause__, visited)
        if er.__context__ is not None:
            er.__context__ = _clear_exception_frames(er.__context__, visited)

        return er

    visited = set()
    _clear_exception_frames(er, visited)
    return er


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cls-name')
    parser.add_argument('--cls-args')
    parser.add_argument('--sockname')
    args = parser.parse_args()

    cls = load_class(args.cls_name)
    cls_args = pickle.loads(base64.b64decode(args.cls_args))

    try:
        run_worker(cls, cls_args, args.sockname)
    except amsg.PoolClosedError:
        exit(0)


if __name__ == '__main__':
    main()
