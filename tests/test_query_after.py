#!/usr/bin/env python3

import unittest
from test_common import TestCommon
import os
import glob


class TestLoadAfter(unittest.TestCase, TestCommon):

    def test_tables(self):
        conn = self.pgconnect()
        for table in self.TABLES:
            self.check_table(conn, table)
            #
            self.set_table_count("lineitem", 60176)
            self.check_table_count(conn, table)

    def test_results(self):
        root_dir = ".."  # parent of results/
        results_dir = os.path.join(root_dir, "results")
        self.check_dir(results_dir)
        for run_dir in glob.glob(os.path.join(results_dir, '*')):
            self.assertRegex(run_dir, 'run_\d{8}_\d{6}', "Run folders should be names run_YYYYMMDD_HHMMSS")
            load_dir = os.path.join(run_dir, "load")
            if os.path.exists(load_dir):
                load_file = os.path.join(load_dir, "Load.json")
                self.check_file(load_file)
            else:
                power_dir = os.path.join(run_dir, "power")
                self.check_dir(power_dir)
                power_file = os.path.join(power_dir, "Power.json")
                self.check_file(power_file, check_if_not_empty=True)
                throughput_dir = os.path.join(run_dir, "throughput")
                self.check_dir(throughput_dir)
                files = []
                for i in range(1, self.NUM_STREAMS + 1):
                    throughput_file = os.path.join(throughput_dir, "Throughput%s%i.json" % ('QueryStream', i))
                    files.append(throughput_file)
                for f in ['RefreshStream', 'Total']:
                    throughput_file = os.path.join(throughput_dir, "Throughput%s.json" % f)
                    files.append(throughput_file)
                for f in files:
                    self.check_file(f, check_if_not_empty=True)


if __name__ == '__main__':
    unittest.main()
