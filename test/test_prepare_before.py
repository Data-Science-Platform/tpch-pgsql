#!/usr/bin/env python3

import unittest
from test_common import TestCommon
import os


class TestPrepareBefore(unittest.TestCase, TestCommon):

    def test_folders_do_not_exist(self):
        folders = ["data", "results", os.path.join("query_root", "perf_query_gen")]
        for folder in folders:
            path = os.path.join(self.ROOT_DIR, folder)
            self.check_dir_not_exist(path)


if __name__ == '__main__':
    unittest.main()
