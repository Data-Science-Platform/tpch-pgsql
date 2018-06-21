#!/usr/bin/env python3

import unittest
import benchmark as bm

class TestBenchmark(unittest.TestCase):

    def test_get_timedelta_in_seconds(self):
        testdata = [
            {"00:00:00.123450": .12345},
            {"00:00:00.012345": .012345},
            {"00:00:01.345678": 1.345678},
            {"00:02:31.678912": 151.678912},
            {"10:25:59.741852": 37559.741852},
            {"10:25:59.741853": (10*60*60 + 25*60 + 59) + 0.741853},
            ]
        for td in testdata:
            for input, expected in td.items():
                self.assertEqual(bm.get_timedelta_in_seconds(input), expected)


    def test_get_qphh_size(self):
        testdata = [
            {"input": (1, 1),  "expected": 1},
            {"input": (2, 2),  "expected": 2},
            {"input": (2, 3),  "expected": 2.449489742783178}
        ]
        for td in testdata:
            self.assertEqual(bm.get_qphh_size(td["input"][0], td["input"][1]), td["expected"])


    def test_scale_to_num_streams(self):
        testdata = [
            {"input": 0, "expected": 2},
            {"input": 0.42, "expected": 2},
            {"input": 1, "expected": 2},
            {"input": 3.14, "expected": 3},
            {"input": 10, "expected": 3},
            {"input": 30, "expected": 4},
            {"input": 100, "expected": 5},
            {"input": 300, "expected": 6},
            {"input": 1000, "expected": 7},
            {"input": 3000, "expected": 8},
            {"input": 10000, "expected": 9},
            {"input": 30000, "expected": 10},
            {"input": 30000.01, "expected": 11},
            {"input": 100000, "expected": 11}
        ]
        for td in testdata:
            self.assertEqual(bm.scale_to_num_streams(td["input"]), td["expected"])


if __name__ == '__main__':
    unittest.main()
