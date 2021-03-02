# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from cassandra_tests.porting import *
from cassandra.query import UNSET_VALUE

def testMapWithUnsetValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, m map<text, text>)") as table:
        # set up
        m = {"k": "v"}
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (10, ?)", m)
        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 10"), [m])

        # test putting an unset map, should not delete the contents
        execute(cql, table, "INSERT INTO %s (k, m) VALUES (10, ?)", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 10"), [m])
        # test unset variables in a map update operation, should not delete the contents
        execute(cql, table, "UPDATE %s SET m['k'] = ? WHERE k = 10", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT m FROM %s WHERE k = 10"), [m])
        assert_invalid_message(cql, table, "Invalid unset map key", "UPDATE %s SET m[?] = 'foo' WHERE k = 10", UNSET_VALUE)

        # test unset value for map key
        assert_invalid_message(cql, table, "Invalid unset map key", "DELETE m[?] FROM %s WHERE k = 10", UNSET_VALUE)

def testListWithUnsetValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, l list<text>)") as table:
        # set up
        l = ["foo", "foo"]
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (10, ?)", l)
        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 10"), [l])

        # replace list with unset value
        execute(cql, table, "INSERT INTO %s (k, l) VALUES (10, ?)", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 10"), [l])

        # add to position
        execute(cql, table, "UPDATE %s SET l[1] = ? WHERE k = 10", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 10"), [l])

        # set in index
        assert_invalid_message(cql, table, "Invalid unset value for list index", "UPDATE %s SET l[?] = 'foo' WHERE k = 10", UNSET_VALUE)

        # remove element by index
        execute(cql, table, "DELETE l[?] FROM %s WHERE k = 10", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 10"), [l])

        # remove all occurrences of element
        execute(cql, table, "UPDATE %s SET l = l - ? WHERE k = 10", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT l FROM %s WHERE k = 10"), [l])

        # select with in clause
        assert_invalid_message(cql, table, "Invalid unset value for column k", "SELECT * FROM %s WHERE k IN ?", UNSET_VALUE)
        # The following cannot be tested with the Python driver because it
        # captures this case before sending it to the server.
        #assert_invalid_message(cql, table, "Invalid unset value for column k", "SELECT * FROM %s WHERE k IN (?)", UNSET_VALUE)

def testSetWithUnsetValues(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int PRIMARY KEY, s set<text>)") as table:
        s = {"bar", "baz", "foo"}
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (10, ?)", s)
        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 10"), [s])

        # replace set with unset value
        execute(cql, table, "INSERT INTO %s (k, s) VALUES (10, ?)", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 10"), [s])

        # add to set
        execute(cql, table, "UPDATE %s SET s = s + ? WHERE k = 10", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 10"), [s])

        # remove all occurrences of element
        execute(cql, table, "UPDATE %s SET s = s - ? WHERE k = 10", UNSET_VALUE)
        assert_rows(execute(cql, table, "SELECT s FROM %s WHERE k = 10"), [s])
