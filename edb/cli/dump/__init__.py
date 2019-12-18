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

from edb.cli import cli
from edb.cli import utils
from edb.edgeql import quote as ql_quote

from . import dump as dumpmod
from . import restore as restoremod


@cli.command()
@click.pass_context
@click.argument('file', type=click.Path(exists=False, dir_okay=False,
                                        resolve_path=True))
def dump(ctx, file: str) -> None:
    utils.connect(ctx)

    conn = ctx.obj['conn']

    dumper = dumpmod.DumpImpl(conn)
    dumper.dump(file)


@cli.command()
@click.pass_context
@click.argument('newdb', type=str)
@click.argument('file', type=click.Path(exists=False, dir_okay=False,
                                        resolve_path=True))
def restore(ctx, newdb: str, file: str) -> None:
    conn = utils.new_connection(ctx)
    conn.execute(f'CREATE DATABASE {ql_quote.quote_ident(newdb)}')
    try:
        restorer = restoremod.RestoreImpl()
        new_conn = utils.new_connection(ctx, database=newdb)
        try:
            restorer.restore(new_conn, file)
        finally:
            new_conn.close()
    except BaseException:
        conn.execute(f'DROP DATABASE {ql_quote.quote_ident(newdb)}')
        raise
    finally:
        conn.close()
