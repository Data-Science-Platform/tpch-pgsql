#!/usr/bin/env python3

import unittest
import psycopg2
import os
import glob

hostname = "localhost"
port = 5432
dbname = "tpchdb"
username = "tpch"
password = "hello123"
NUM_STREAMS = 2  # because we use scale factor 0.01


class TestLoadAfter(unittest.TestCase):

    def test_tables(self):
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
            row_counts = {"customer": 1500, "lineitem": 60176, "nation": 25, "orders": 15000,
                          "part": 2000, "partsupp": 8000, "region": 5, "supplier": 100}
            expected_rows = row_counts[table]
            actual_rows = count[0][0]
            self.assertTrue(len(count) == 1 and actual_rows != 0, "Table %s is empty!" % table)
            self.assertTrue(len(count) == 1 and actual_rows == expected_rows,
                            "Table %s does not contain expected number of rows! "
                            "(expected=%s vs actual=%s)" % (table, expected_rows, actual_rows))

    def check_dir(self, path):
        self.assertTrue(os.path.exists(path), "Folder %s does not exist!" % path)
        self.assertTrue(os.path.isdir(path), "Path %s is not a directory!" % path)

    def check_file(self, filename, check_if_not_empty=False):
        self.assertTrue(os.path.exists(filename), "File %s does not exist!" % filename)
        self.assertTrue(os.path.isfile(filename), "Path %s is not a file!" % filename)
        if check_if_not_empty:
            self.assertTrue(os.stat(filename).st_size > 0, "Path %s is empty!" % filename)

    def test_results(self):
        root_dir = ".."  # parent of results/
        results_dir = os.path.join(root_dir, "results")
        self.check_dir(results_dir)
        for run_dir in glob.glob(os.path.join(results_dir, '*')):
            power_dir = os.path.join(run_dir, "power")
            self.check_dir(power_dir)
            power_file = os.path.join(power_dir, "Power.json")
            self.check_file(power_file, check_if_not_empty=True)
            throughput_dir = os.path.join(run_dir, "throughput")
            self.check_dir(throughput_dir)
            files = []
            for i in range(1, NUM_STREAMS + 1):
                throughput_file = os.path.join(throughput_dir, "Throughput%s%i.json" % ('QueryStream', i))
                files.append(throughput_file)
            for f in ['RefreshStream', 'Total']:
                throughput_file = os.path.join(throughput_dir, "Throughput%s.json" % f)
                files.append(throughput_file)
            for f in files:
                self.check_file(f, check_if_not_empty=True)


if __name__ == '__main__':
    unittest.main()
