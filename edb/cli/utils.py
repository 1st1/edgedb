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
from typing import *  # NoQA

import click
import edgedb


def new_connection(
    ctx: click.Context,
    *,
    database: Optional[str]=None
) -> edgedb.BlockingIOConnection:
    try:
        return edgedb.connect(
            host=ctx.obj['host'],
            port=ctx.obj['port'],
            user=ctx.obj['user'],
            database=database or ctx.obj['database'],
            admin=ctx.obj['admin'],
            password=ctx.obj['password'],
        )
    except edgedb.AuthenticationError:
        if (ctx.obj['password'] is None
                and ctx.obj['password_prompt'] is not None):
            password = ctx.obj['password_prompt']

            return edgedb.connect(
                host=ctx.obj['host'],
                port=ctx.obj['port'],
                user=ctx.obj['user'],
                database=database or ctx.obj['database'],
                admin=ctx.obj['admin'],
                password=password,
            )
        else:
            raise


def connect(
    ctx: click.Context,
) -> edgedb.BlockingIOConnection:
    conn = new_connection(ctx)
    ctx.obj['conn'] = conn
    ctx.call_on_close(lambda: conn.close())
