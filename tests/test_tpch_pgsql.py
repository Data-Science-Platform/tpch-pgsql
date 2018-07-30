#!/usr/bin/env python3

import unittest

import os
import mock

import tpch_pgsql as bm
from tpch4pgsql import query


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
                self.assertEqual(query.get_timedelta_in_seconds(input), expected)


    def test_get_qphh_size(self):
        testdata = [
            {"input": (1, 1),  "expected": 1},
            {"input": (2, 2),  "expected": 2},
            {"input": (2, 3),  "expected": 2.449489742783178}
        ]
        for td in testdata:
            self.assertEqual(query.get_qphh_size(td["input"][0], td["input"][1]), td["expected"])


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

    def get_json_files_from(path):
        json_files = [pos_json for pos_json in os.listdir(path) if pos_json.endswith('.json')]
        json_files = [os.path.join(path, s) for s in json_files]
        return json_files

    @mock.patch('tpch_pgsql.os.listdir')
    def test_get_json_files_from(self, mock_listdir):
        mock_listdir.return_value = ['a.json', 'b.txt', 'C.json']
        root_dir = 'dummy'
        expected = [os.path.join(root_dir, x) for x in ['a.json', 'C.json']]
        files = query.get_json_files_from(root_dir)
        self.assertEqual(expected, files,
                         "Some json files were not found, others were included, but are not json files!")

    @staticmethod
    def mock_path_isdir_side_effect(arg):
        basename = os.path.basename(arg)
        if basename in ('power', 'throughput'):
            return True
        else:
            return False

    @staticmethod
    def mock_path_exists_side_effect(arg):
        return True

    @mock.patch('tpch_pgsql.os.listdir')
    def test_get_json_files(self, mock_listdir):
        mock_listdir.side_effect = [['run1', 'run2', 'run3', 'run4'],
                                    ['power1.json'], ['throughput1a.json', 'throughput1b.json'],
                                    ['power2.json', 'power2.txt'], ['throughput2.json'],
                                    ['power3a.txt'], ['throughput.txt'],
                                    [], []]
        mock_isdir = mock.patch('os.path.isdir').start()
        mock_isdir.side_effect = self.mock_path_isdir_side_effect
        mock_exists = mock.patch('os.path.exists').start()
        mock_exists.side_effect = self.mock_path_exists_side_effect
        root_dir = 'dummy'
        expected = [os.path.join('dummy', 'run1', 'power', 'power1.json'),
                    os.path.join('dummy', 'run1', 'throughput', 'throughput1a.json'),
                    os.path.join('dummy', 'run1', 'throughput', 'throughput1b.json'),
                    os.path.join('dummy', 'run2', 'power', 'power2.json'),
                    os.path.join('dummy', 'run2', 'throughput', 'throughput2.json')]
        files = query.get_json_files(root_dir)
        self.assertEqual(expected, files,
                         "Some json files were not found, others were included, but are not json files!")


if __name__ == '__main__':
    unittest.main()
