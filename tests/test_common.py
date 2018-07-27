#!/usr/bin/env python3

import os
import psycopg2


class TestCommon(object):
    CONNECT_STRING = "host='%s' port='%s' dbname='%s' user='%s' password='%s'"
    HOSTNAME = "localhost"
    PORT = 5432
    DBNAME = "tpchdb"
    USERNAME = "tpch"
    PASSWORD = "hello123"
    NUM_STREAMS = 2  # because we use scale factor 0.01
    QUERY_NR_RANGE = range(1, NUM_STREAMS + 1)
    ROOT_DIR = ".."  # parent of tests/

    TABLES = ["customer", "lineitem", "nation", "orders",
              "part", "partsupp", "region", "supplier"]
    ROW_COUNTS = {"customer": 1500, "lineitem": 60175,  # will be changed after query
                  "nation": 25, "orders": 15000,
                  "part": 2000, "partsupp": 8000, "region": 5, "supplier": 100}

    def __init__(self):
        self.assertEqual(self.TABLES, list(self.ROW_COUNTS.keys()).sort())

    def set_table_count(self, table, count):
        self.ROW_COUNTS[table] = count

    def pgconnect(self):
        conn = psycopg2.connect(self.CONNECT_STRING % (self.HOSTNAME, self.PORT, self.DBNAME, self.USERNAME, self.PASSWORD))
        return conn

    def check_dir(self, path):
        self.assertTrue(os.path.exists(path), "Folder %s does not exist!" % path)
        self.assertTrue(os.path.isdir(path), "Path %s is not a directory!" % path)

    def check_dir_not_exist(self, path):
        self.assertFalse(os.path.exists(path), "Folder %s already exists!" % path)

    def check_file(self, filename, check_if_not_empty=False):
        self.assertTrue(os.path.exists(filename), "File %s does not exist!" % filename)
        self.assertTrue(os.path.isfile(filename), "Path %s is not a file!" % filename)
        if check_if_not_empty:
            self.assertTrue(os.stat(filename).st_size > 0, "Path %s is empty!" % filename)

    def check_table(self, conn, table):
        cursor = conn.cursor()
        sql = "select relname from pg_class where relkind='r' and relname = '%s';" % table
        cursor.execute(sql)
        found = cursor.fetchall()
        self.assertTrue(len(found) == 1 and found[0][0] == table, "Table %s does not exist!" % table)

    def check_table_count(self, conn, table):
        cursor = conn.cursor()
        sql = "select count(1) from %s;" % table
        cursor.execute(sql)
        count = cursor.fetchall()
        expected_rows = self.ROW_COUNTS[table]
        actual_rows = count[0][0]
        self.assertTrue(len(count) == 1 and actual_rows != 0, "Table %s is empty!" % table)
        self.assertTrue(len(count) == 1 and actual_rows == expected_rows,
                        "Table %s does not contain expected number of rows! "
                        "(expected=%s vs actual=%s)" % (table, expected_rows, actual_rows))
