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


import hashlib
import unittest  # NoQA

from edb.server2 import dbstate


class TestEdgeDBState(unittest.TestCase):

    def _new_compiled(self, sql, dbver):
        sql_bytes = sql.encode()

        sql_hash = hashlib.sha1(sql_bytes).hexdigest().encode()
        out_id = hashlib.sha1(b'out-type-' + sql_hash).hexdigest().encode()
        in_id = hashlib.sha1(b'in-type-' + sql_hash).hexdigest().encode()

        return dbstate.CompiledQuery(
            dbver=dbver,
            out_type_data=b'some',
            out_type_id=out_id,
            in_type_data=b'some',
            in_type_id=in_id,
            sql=sql,
            sql_hash=sql_hash,
        )

    def test_server_db_state_01(self):
        dbi = dbstate.DatabaseIndex()

        db1 = dbi.new_view(dbname='1', user='1')
        db2 = dbi.new_view(dbname='1', user='1')

        eql = 'aaaa'
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIsNone(db2.lookup_compiled_query(eql))

        eql_comp = self._new_compiled('select aaaa;', db1.dbver)
        db1.cache_compiled_query(eql, eql_comp)
        self.assertIs(db1.lookup_compiled_query(eql), eql_comp)
        self.assertIs(db2.lookup_compiled_query(eql), eql_comp)

        db1.tx_begin()
        self.assertIs(db1.lookup_compiled_query(eql), eql_comp)
        self.assertIs(db2.lookup_compiled_query(eql), eql_comp)

        db1.signal_ddl()
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIs(db2.lookup_compiled_query(eql), eql_comp)

        db1.tx_rollback()
        self.assertIs(db1.lookup_compiled_query(eql), eql_comp)
        self.assertIs(db2.lookup_compiled_query(eql), eql_comp)

        db2.signal_ddl()
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIsNone(db2.lookup_compiled_query(eql))

    def test_server_db_state_02(self):
        dbi = dbstate.DatabaseIndex()

        db1 = dbi.new_view(dbname='1', user='1')
        db2 = dbi.new_view(dbname='2222', user='1')

        eql = 'aaaa'
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIsNone(db2.lookup_compiled_query(eql))

        eql_comp = self._new_compiled('select aaaa;', db1.dbver)
        db1.cache_compiled_query(eql, eql_comp)
        self.assertIs(db1.lookup_compiled_query(eql), eql_comp)
        self.assertIsNone(db2.lookup_compiled_query(eql))

    def test_server_db_state_03(self):
        dbi = dbstate.DatabaseIndex()

        db1 = dbi.new_view(dbname='1', user='1')
        db2 = dbi.new_view(dbname='1', user='1')

        eql = 'aaaa'
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIsNone(db2.lookup_compiled_query(eql))

        eql_comp = self._new_compiled('select aaaa;', db1.dbver)
        db1.cache_compiled_query(eql, eql_comp)

        db1.tx_begin()
        self.assertIs(db1.lookup_compiled_query(eql), eql_comp)
        self.assertIs(db2.lookup_compiled_query(eql), eql_comp)

        db1.signal_ddl()
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIs(db2.lookup_compiled_query(eql), eql_comp)

        db1.tx_commit()
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIsNone(db2.lookup_compiled_query(eql))

    def test_server_db_state_04(self):
        dbi = dbstate.DatabaseIndex()

        db1 = dbi.new_view(dbname='1', user='1')
        db2 = dbi.new_view(dbname='1', user='1')

        eql = 'aaaa'
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIsNone(db2.lookup_compiled_query(eql))

        eql_comp = self._new_compiled('select aaaa;', db1.dbver)
        db1.cache_compiled_query(eql, eql_comp)

        db1.tx_begin()
        self.assertIs(db1.lookup_compiled_query(eql), eql_comp)
        self.assertIs(db2.lookup_compiled_query(eql), eql_comp)

        db1.tx_commit()
        self.assertIs(db1.lookup_compiled_query(eql), eql_comp)
        self.assertIs(db2.lookup_compiled_query(eql), eql_comp)

    def test_server_db_state_05(self):
        dbi = dbstate.DatabaseIndex()

        db1 = dbi.new_view(dbname='1', user='1')
        db2 = dbi.new_view(dbname='1', user='1')

        eql = 'aaaa'
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIsNone(db2.lookup_compiled_query(eql))

        eql_comp = self._new_compiled('select aaaa;', db1.dbver)
        db1.cache_compiled_query(eql, eql_comp)
        self.assertIs(db1.lookup_compiled_query(eql), eql_comp)
        self.assertIs(db2.lookup_compiled_query(eql), eql_comp)

        db1.tx_begin()
        self.assertIs(db1.lookup_compiled_query(eql), eql_comp)
        self.assertIs(db2.lookup_compiled_query(eql), eql_comp)

        db2.tx_begin()
        db1.signal_ddl()
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIs(db2.lookup_compiled_query(eql), eql_comp)

        db2.signal_ddl()
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIsNone(db2.lookup_compiled_query(eql))

        eql_comp2 = self._new_compiled('select aaaa1;', db2.dbver)
        db2.cache_compiled_query(eql, eql_comp2)
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIs(db2.lookup_compiled_query(eql), eql_comp2)

        db1.tx_commit()
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIs(db2.lookup_compiled_query(eql), eql_comp2)

        db2.signal_ddl()
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIsNone(db2.lookup_compiled_query(eql))

        db2.cache_compiled_query(eql, eql_comp2)
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIs(db2.lookup_compiled_query(eql), eql_comp2)

        db2.tx_commit()
        self.assertIsNone(db1.lookup_compiled_query(eql))
        self.assertIsNone(db2.lookup_compiled_query(eql))
