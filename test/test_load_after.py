#!/usr/bin/env python3

import unittest
import psycopg2

hostname = "localhost"
port = 5432
dbname = "tpchdb"
username = "tpch"
password = "hello123"


class TestLoadAfter(unittest.TestCase):

    def test_tables_created(self):
        tables = ["customer", "lineitem", "nation",
                  "orders", "part", "partsupp", "region",
                  "supplier"]
        connect_string = "host='%s' port='%s' dbname='%s' user='%s' password='%s'"
        conn = psycopg2.connect(connect_string % (hostname, port, dbname, username, password))
        for table in tables:
            cursor = conn.cursor()
            sql = "select relname from pg_class where relkind='r' and relname = '%s';" % table
            cursor.execute(sql)
            found = cursor.fetchall()
            self.assertTrue(len(found) == 1 and found[0][0] == table, "Table %s does not exist!" % table)
            #
            sql = "select count(1) from %s;" % table
            cursor.execute(sql)
            count = cursor.fetchall()
            row_counts = {"customer": 1500, "lineitem": 60175, "nation": 25, "orders": 15000,
                          "part": 2000, "partsupp": 8000, "region": 5, "supplier": 100}
            expected_rows = row_counts[table]
            actual_rows = count[0][0]
            self.assertTrue(len(count) == 1 and actual_rows != 0, "Table %s is empty!" % table)
            self.assertTrue(len(count) == 1 and actual_rows == expected_rows,
                            "Table %s does not contain expected number of rows! "
                            "(expected=%s vs actual=%s)" % (table, expected_rows, actual_rows))


if __name__ == '__main__':
    unittest.main()
