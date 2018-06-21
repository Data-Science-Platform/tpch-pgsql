#!/usr/bin/env python3

import unittest
import os


root_dir = ".."


class TestPrepareBefore(unittest.TestCase):

    def test_folders_do_not_exist(self):
        folders = ["data", "results", os.path.join("query_root", "perf_query_gen")]
        for folder in folders:
            path = os.path.join(root_dir, folder)
            self.assertFalse(os.path.exists(path), "Folder %s already exists!" % path)


if __name__ == '__main__':
    unittest.main()
