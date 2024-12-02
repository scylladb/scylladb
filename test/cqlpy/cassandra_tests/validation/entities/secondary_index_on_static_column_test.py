# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *


def testSimpleStaticColumn(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int, name text, age int static, PRIMARY KEY (id, name))") as table:
        cql.execute(f"CREATE INDEX static_age on {table}(age)")
        id1 = 1
        id2 = 2
        age1 = 24
        age2 = 32
        name1A = "Taylor"
        name1B = "Swift"
        name2 = "Jamie"

        execute(cql, table, "INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", id1, name1A, age1)
        execute(cql, table, "INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", id1, name1B, age1)
        execute(cql, table, "INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", id2, name2, age2)

        assert_rows(execute(cql, table, "SELECT id, name, age FROM %s WHERE age=?", age1),
              [id1, name1B, age1], [id1, name1A, age1])
        assert_rows(execute(cql, table, "SELECT id, name, age FROM %s WHERE age=?", age2),
                [id2, name2, age2])

        # Update the rows. Validate that updated values will be reflected in the index.
        newAge1 = 40
        execute(cql, table, "UPDATE %s SET age = ? WHERE id = ?", newAge1, id1)
        assert_empty(execute(cql, table, "SELECT id, name, age FROM %s WHERE age=?", age1))
        assert_rows(execute(cql, table, "SELECT id, name, age FROM %s WHERE age=?", newAge1),
                [id1, name1B, newAge1], [id1, name1A, newAge1])
        execute(cql, table, "DELETE FROM %s WHERE id = ?", id2)
        assert_empty(execute(cql, table, "SELECT id, name, age FROM %s WHERE age=?", age2))


def testIndexOnCompoundRowKey(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(interval text, seq int, id int, severity int static, PRIMARY KEY ((interval, seq), id) ) WITH CLUSTERING ORDER BY (id DESC)") as table:
        execute(cql, table, "CREATE INDEX ON %s (severity)")

        execute(cql, table, "insert into %s (interval, seq, id , severity) values('t',1, 3, 10)")
        execute(cql, table, "insert into %s (interval, seq, id , severity) values('t',1, 4, 10)")
        execute(cql, table, "insert into %s (interval, seq, id , severity) values('t',2, 3, 10)")
        execute(cql, table, "insert into %s (interval, seq, id , severity) values('t',2, 4, 10)")
        execute(cql, table, "insert into %s (interval, seq, id , severity) values('m',1, 3, 11)")
        execute(cql, table, "insert into %s (interval, seq, id , severity) values('m',1, 4, 11)")
        execute(cql, table, "insert into %s (interval, seq, id , severity) values('m',2, 3, 11)")
        execute(cql, table, "insert into %s (interval, seq, id , severity) values('m',2, 4, 11)")

        assert_rows(execute(cql, table, "select * from %s where severity = 10 and interval = 't' and seq = 1"),
                   ["t", 1, 4, 10], ["t", 1, 3, 10])

def testIndexOnCollections(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, v int, l list<int> static, s set<text> static, m map<text, int> static, PRIMARY KEY (k, v))") as table:
        execute(cql, table, "CREATE INDEX ON %s (l)")
        execute(cql, table, "CREATE INDEX ON %s (s)")
        execute(cql, table, "CREATE INDEX ON %s (m)")
        execute(cql, table, "CREATE INDEX ON %s (keys(m))")

        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (0, 0, [1, 2],    {'a'},      {'a' : 1, 'b' : 2})")
        execute(cql, table, "INSERT INTO %s (k, v)          VALUES (0, 1)                                  ")
        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (1, 0, [4, 5],    {'d'},      {'b' : 1, 'c' : 4})")

        # lists
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE l CONTAINS 1"), [0, 0], [0, 1])
        assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE k = 1 AND l CONTAINS 1"))
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE l CONTAINS 4"), [1, 0])
        assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE l CONTAINS 6"))

        # update lists
        execute(cql, table, "UPDATE %s SET l = l + [3] WHERE k = ?", 0)
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE l CONTAINS 3"), [0, 0], [0, 1])

        # sets
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE s CONTAINS 'a'"), [0, 0], [0, 1])
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE k = 0 AND s CONTAINS 'a'"), [0, 0], [0, 1])
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE s CONTAINS 'd'"), [1, 0])
        assert_empty(execute(cql, table, "SELECT k, v FROM %s  WHERE s CONTAINS 'e'"))

        # update sets
        execute(cql, table, "UPDATE %s SET s = s + {'b'} WHERE k = ?", 0)
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE s CONTAINS 'b'"), [0, 0], [0, 1])
        execute(cql, table, "UPDATE %s SET s = s - {'a'} WHERE k = ?", 0)
        assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE s CONTAINS 'a'"))

        # maps
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m CONTAINS 1"), [1, 0], [0, 0], [0, 1])
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS 1"), [0, 0], [0, 1])
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m CONTAINS 4"), [1, 0])
        assert_empty(execute(cql, table, "SELECT k, v FROM %s  WHERE m CONTAINS 5"))

        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m CONTAINS KEY 'b'"), [1, 0], [0, 0], [0, 1])
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE k = 0 AND m CONTAINS KEY 'b'"), [0, 0], [0, 1])
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m CONTAINS KEY 'c'"), [1, 0])
        assert_empty(execute(cql, table, "SELECT k, v FROM %s  WHERE m CONTAINS KEY 'd'"))

        # update maps.
        execute(cql, table, "UPDATE %s SET m['c'] = 5 WHERE k = 0")
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m CONTAINS 5"), [0, 0], [0, 1])
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m CONTAINS KEY 'c'"), [1, 0], [0, 0], [0, 1])
        execute(cql, table, "DELETE m['a'] FROM %s WHERE k = 0")
        assert_empty(execute(cql, table, "SELECT k, v FROM %s  WHERE m CONTAINS KEY 'a'"))

def testIndexOnFrozenCollections(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(k int, v int, l frozen<list<int>> static, s frozen<set<text>> static, m frozen<map<text, int>> static, PRIMARY KEY (k, v))") as table:
        execute(cql, table, "CREATE INDEX ON %s (FULL(l))")
        execute(cql, table, "CREATE INDEX ON %s (FULL(s))")
        execute(cql, table, "CREATE INDEX ON %s (FULL(m))")

        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (0, 0, [1, 2],    {'a'},      {'a' : 1, 'b' : 2})")
        execute(cql, table, "INSERT INTO %s (k, v)          VALUES (0, 1)                                  ")
        execute(cql, table, "INSERT INTO %s (k, v, l, s, m) VALUES (1, 0, [4, 5],    {'d'},      {'b' : 1, 'c' : 4})")
        execute(cql, table, "UPDATE %s SET l=[3], s={'3'}, m={'3': 3} WHERE k=3" )

        # lists
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE l = [1, 2]"), [0, 0], [0, 1])
        assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE k = 1 AND l = [1, 2]"))
        assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE l = [4]"))
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE l = [3]"), [3, None])

        # update lists
        execute(cql, table, "UPDATE %s SET l = [1, 2, 3] WHERE k = ?", 0)
        assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE l = [1, 2]"))
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE l = [1, 2, 3]"), [0, 0], [0, 1])

        # sets
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE s = {'a'}"), [0, 0], [0, 1])
        assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE k = 1 AND s = {'a'}"))
        assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE s = {'b'}"))
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE s = {'3'}"), [3, None])

        # update sets
        execute(cql, table, "UPDATE %s SET s = {'a', 'b'} WHERE k = ?", 0)
        assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE s = {'a'}"))
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE s = {'a', 'b'}"), [0, 0], [0, 1])

        # maps
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m = {'a' : 1, 'b' : 2}"), [0, 0], [0, 1])
        assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE k = 1 AND m = {'a' : 1, 'b' : 2}"))
        assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE m = {'a' : 1, 'b' : 3}"))
        assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE m = {'a' : 1, 'c' : 2}"))
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m = {'3': 3}"), [3, None])

        # update maps.
        execute(cql, table, "UPDATE %s SET m = {'a': 2, 'b': 3} WHERE k = ?", 0)
        assert_empty(execute(cql, table, "SELECT k, v FROM %s WHERE m = {'a': 1, 'b': 2}"))
        assert_rows(execute(cql, table, "SELECT k, v FROM %s WHERE m = {'a': 2, 'b': 3}"), [0, 0], [0, 1])

def testStaticIndexAndNonStaticIndex(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id int, company text, age int static, salary int, PRIMARY KEY(id, company))") as table:
        execute(cql, table, "CREATE INDEX on %s(age)")
        execute(cql, table, "CREATE INDEX on %s(salary)")

        company1 = "company1"
        company2 = "company2"

        execute(cql, table, "INSERT INTO %s(id, company, age, salary) VALUES(?, ?, ?, ?)", 1, company1, 20, 1000)
        execute(cql, table, "INSERT INTO %s(id, company,      salary) VALUES(?, ?,    ?)", 1, company2,     2000)
        execute(cql, table, "INSERT INTO %s(id, company, age, salary) VALUES(?, ?, ?, ?)", 2, company1, 40, 2000)

        assert_rows(execute(cql, table, "SELECT id, company, age, salary FROM %s WHERE age = 20 AND salary = 2000 ALLOW FILTERING"),
                   [1, company2, 20, 2000])

def testIndexOnUDT(cql, test_keyspace):
    with create_type(cql, test_keyspace, "(street text, city text)") as type_name:
        with create_table(cql, test_keyspace, f"(id int, company text, home frozen<{type_name}> static, price int, PRIMARY KEY(id, company))") as table:
            execute(cql, table, "CREATE INDEX on %s(home)")

            addressString = "{street: 'Centre', city: 'C'}"
            companyName = "Random"

            execute(cql, table, "INSERT INTO %s(id, company, home, price) "
                + "VALUES(1, '" + companyName + "', " + addressString + ", 10000)")
            assert_rows(execute(cql, table, "SELECT id, company FROM %s WHERE home = " + addressString), [1, companyName])
            newAddressString = "{street: 'Fifth', city: 'P'}"

            execute(cql, table, "UPDATE %s SET home = " + newAddressString + " WHERE id = 1")
            assert_empty(execute(cql, table, "SELECT id, company FROM %s WHERE home = " + addressString))
            assert_rows(execute(cql, table, "SELECT id, company FROM %s WHERE home = " + newAddressString), [1, companyName])
