#!/usr/bin/env python3

import unittest
import os

SCALE_FACTOR = 2  # because we use 0.01 as scale in travis ci build
query_nr_range = range(1, SCALE_FACTOR+1)


class TestPrepareAfter(unittest.TestCase):

    def check_dir(self, path):
        #print(path)
        self.assertTrue(os.path.exists(path), "Folder %s does not exist!" % path)
        self.assertTrue(os.path.isdir(path), "Path %s is not a directory!" % path)

    def check_file(self, filename):
        #print(filename)
        self.assertTrue(os.path.exists(filename), "File %s does not exist!" % filename)
        self.assertTrue(os.path.isfile(filename), "Path %s is not a file!" % filename)

    def test_folders_do_exist_now(self):
        root_dir = ".." # parent of test/
        folders = ["data", os.path.join("query_root", "perf_query_gen")]
        for folder in folders:
            folder_path = os.path.join(root_dir, folder)
            self.check_dir(folder_path)
            #
            if folder == "data":
                subfolders = ["delete", "load", "update"]
                for subfolder in subfolders:
                    subfolder_path = os.path.join(folder_path, subfolder)
                    self.check_dir(subfolder_path)
                    #
                    if subfolder == "delete":
                        for i in query_nr_range:
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
                            for i in query_nr_range:
                                filename = os.path.join(subfolder_path, "%s.tbl.u%s.csv" % (table,i))
                                self.check_file(filename)
            elif folder == "query_root":
                subfolder = "perf_query_gen"
                subfolder_path = os.path.join(folder_path, subfolder)
                self.check_dir(subfolder_path)
                for i in query_nr_range:
                    filename = os.path.join(subfolder_path, "%s.sql" % i)
                    self.check_file(filename)


if __name__ == '__main__':
    unittest.main()
