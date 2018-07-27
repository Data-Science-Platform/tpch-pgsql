#!/usr/bin/env python3

import unittest
from test_common import TestCommon


class TestLoadAfter(unittest.TestCase, TestCommon):

    def test_tables_created(self):
        conn = self.pgconnect()
        #
        for table in self.TABLES:
            self.check_table(conn, table)
            #
            self.set_table_count("lineitem", 60175)
            self.check_table_count(conn, table)


if __name__ == '__main__':
    unittest.main()
