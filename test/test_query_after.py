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
            self.assertTrue(len(count) == 1 and count[0][0] != 0, "Table %s is empty!" % table)
            self.assertTrue(len(count) == 1 and count[0][0] == row_counts[table],
                            "Table %s does not contain expected number of rows %s!" % (table, row_counts[table]))

    def check_dir(self, path):
        print(path)
        self.assertTrue(os.path.exists(path), "Folder %s does not exist!" % path)
        self.assertTrue(os.path.isdir(path), "Path %s is not a directory!" % path)

    def check_file(self, filename):
        print(filename)
        self.assertTrue(os.path.exists(filename), "File %s does not exist!" % filename)
        self.assertTrue(os.path.isfile(filename), "Path %s is not a file!" % filename)

    def test_results(self):
        root_dir = ".."  # parent of results/
        results_dir = os.path.join(root_dir, "results")
        self.check_dir(results_dir)
        for run_dir in glob.glob(os.path.join(results_dir,'*')):
            power_dir = os.path.join(run_dir, "power")
            self.check_dir(power_dir)
            power_file = os.path.join(power_dir, "Power.json")
            self.check_file(power_file)
            throughput_dir = os.path.join(run_dir, "throughput")
            self.check_dir(throughput_dir)
            for f in ['QueryStream', 'RefreshStream', 'Total']:
                if f == 'QueryStream':
                    for i in [1, 2]:
                        throughput_file = os.path.join(throughput_dir, "Throughput%s%i.json" % (f, i))
                else:
                    throughput_file = os.path.join(throughput_dir, "Throughput%s.json" % f)
                self.check_file(throughput_file)


if __name__ == '__main__':
    unittest.main()
