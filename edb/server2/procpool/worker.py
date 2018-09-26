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


async def run_worker(cls_name, cls_args, sockname):
    args = pickle.loads(base64.b64decode(cls_args))
    mod_name, _, cls_name = cls_name.rpartition('.')
    mod = importlib.import_module(mod_name)
    cls = getattr(mod, cls_name)

    con = await amsg.worker_connect(sockname)
    worker = cls(*args)

    while True:
        req = await con.next_request()

        try:
            methname, args = pickle.loads(req)
        except Exception as ex:
            ex = _clear_exception_frames(ex)
            data = (1, ex)
        else:
            meth = getattr(worker, methname)

            try:
                res = await meth(*args)
                data = (0, res)
            except Exception as ex:
                ex = _clear_exception_frames(ex)
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cls-name')
    parser.add_argument('--cls-args')
    parser.add_argument('--sockname')
    args = parser.parse_args()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    try:
        asyncio.run(run_worker(args.cls_name, args.cls_args, args.sockname))
    except amsg.PoolClosedError:
        exit(0)


def _clear_exception_frames(er):
    traceback.clear_frames(er.__traceback__)

    if er.__cause__ is not None:
        er.__cause__ = _clear_exception_frames(er.__cause__)
    if er.__context__ is not None:
        er.__context__ = _clear_exception_frames(er.__context__)

    return er


if __name__ == '__main__':
    main()
