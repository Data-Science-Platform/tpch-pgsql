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
        for pair in testdata:
            for input, expected in pair.items():
                self.assertEqual(bm.get_timedelta_in_seconds(input), expected)

    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)

if __name__ == '__main__':
    unittest.main()