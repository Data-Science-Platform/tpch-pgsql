#!/usr/bin/env python3

import unittest
from test_common import TestCommon
import os


class TestPrepareAfter(unittest.TestCase, TestCommon):

    def test_folders_do_exist_now(self):
        folders = ["data", os.path.join("query_root", "perf_query_gen")]
        for folder in folders:
            folder_path = os.path.join(self.ROOT_DIR, folder)
            self.check_dir(folder_path)
            #
            if folder == "data":
                subfolders = ["delete", "load", "update"]
                for subfolder in subfolders:
                    subfolder_path = os.path.join(folder_path, subfolder)
                    self.check_dir(subfolder_path)
                    #
                    if subfolder == "delete":
                        for i in self.QUERY_NR_RANGE:
                            filename = os.path.join(subfolder_path, "delete.%s.csv" % i)
                            self.check_file(filename)
                    elif subfolder == "load":
                        tables = ["customer", "lineitem", "nation",
                                  "orders", "part", "partsupp", "region",
                                  "supplier"]
                        for table in tables:
                            filename = os.path.join(subfolder_path, "%s.tbl.csv" % table)
                            self.check_file(filename)
                    elif subfolder == "update":
                        tables = ["lineitem", "orders"]
                        for table in tables:
                            for i in self.QUERY_NR_RANGE:
                                filename = os.path.join(subfolder_path, "%s.tbl.u%s.csv" % (table,i))
                                self.check_file(filename)
            elif folder == "query_root":
                subfolder = "perf_query_gen"
                subfolder_path = os.path.join(folder_path, subfolder)
                self.check_dir(subfolder_path)
                for i in self.QUERY_NR_RANGE:
                    filename = os.path.join(subfolder_path, "%s.sql" % i)
                    self.check_file(filename)


if __name__ == '__main__':
    unittest.main()
