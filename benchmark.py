#!/usr/bin/env python3

import json
import psycopg2
import argparse
import os
import time
import math
from datetime import datetime
import getpass
from itertools import zip_longest
from multiprocessing import Process, Queue

import prepare as prep

# Constants
LOAD_DIR = "load"
UPDATE_DIR = "update"
DELETE_DIR = "delete"
TEMPLATE_QUERY_DIR = "perf_query_template"
GENERATED_QUERY_DIR = "perf_query_gen"
PREP_QUERY_DIR = "prep_query"
RESULTS_DIR = "results"
TABLES = ['LINEITEM', 'PARTSUPP', 'ORDERS', 'CUSTOMER', 'SUPPLIER', 'NATION', 'REGION', 'PART']
POWER = "power"
THROUGHPUT = "throughput"
QUERY_METRIC = "query_stream_%s_query_%s"
REFRESH_METRIC = "refresh_stream_%s_func_%s"
THROUGHPUT_TOTAL_METRIC = "throughput_test_total"
QUERY_ORDER = [  # As given in appendix A of the TPCH-specification
        [14, 2, 9, 20, 6, 17, 18, 8, 21, 13, 3, 22, 16, 4, 11, 15, 1, 10, 19, 5, 7, 12],
        [21, 3, 18, 5, 11, 7, 6, 20, 17, 12, 16, 15, 13, 10, 2, 8, 14, 19, 9, 22, 1, 4],
        [6, 17, 14, 16, 19, 10, 9, 2, 15, 8, 5, 22, 12, 7, 13, 18, 1, 4, 20, 3, 11, 21],
        [8, 5, 4, 6, 17, 7, 1, 18, 22, 14, 9, 10, 15, 11, 20, 2, 21, 19, 13, 16, 12, 3],
        [5, 21, 14, 19, 15, 17, 12, 6, 4, 9, 8, 16, 11, 2, 10, 18, 1, 13, 7, 22, 3, 20],
        [21, 15, 4, 6, 7, 16, 19, 18, 14, 22, 11, 13, 3, 1, 2, 5, 8, 20, 12, 17, 10, 9],
        [10, 3, 15, 13, 6, 8, 9, 7, 4, 11, 22, 18, 12, 1, 5, 16, 2, 14, 19, 20, 17, 21],
        [18, 8, 20, 21, 2, 4, 22, 17, 1, 11, 9, 19, 3, 13, 5, 7, 10, 16, 6, 14, 15, 12],
        [19, 1, 15, 17, 5, 8, 9, 12, 14, 7, 4, 3, 20, 16, 6, 22, 10, 13, 2, 21, 18, 11],
        [8, 13, 2, 20, 17, 3, 6, 21, 18, 11, 19, 10, 15, 4, 22, 1, 7, 12, 9, 14, 5, 16],
        [6, 15, 18, 17, 12, 1, 7, 2, 22, 13, 21, 10, 14, 9, 3, 16, 20, 19, 11, 4, 8, 5],
        [15, 14, 18, 17, 10, 20, 16, 11, 1, 8, 4, 22, 5, 12, 3, 9, 21, 2, 13, 6, 19, 7],
        [1, 7, 16, 17, 18, 22, 12, 6, 8, 9, 11, 4, 2, 5, 20, 21, 13, 10, 19, 3, 14, 15],
        [21, 17, 7, 3, 1, 10, 12, 22, 9, 16, 6, 11, 2, 4, 5, 14, 8, 20, 13, 18, 15, 19],
        [2, 9, 5, 4, 18, 1, 20, 15, 16, 17, 7, 21, 13, 14, 19, 8, 22, 11, 10, 3, 12, 6],
        [16, 9, 17, 8, 14, 11, 10, 12, 6, 21, 7, 3, 15, 5, 22, 20, 1, 13, 19, 2, 4, 18],
        [1, 3, 6, 5, 2, 16, 14, 22, 17, 20, 4, 9, 10, 11, 15, 8, 12, 19, 18, 13, 7, 21],
        [3, 16, 5, 11, 21, 9, 2, 15, 10, 18, 17, 7, 8, 19, 14, 13, 1, 4, 22, 20, 6, 12],
        [14, 4, 13, 5, 21, 11, 8, 6, 3, 17, 2, 20, 1, 19, 10, 9, 12, 18, 15, 7, 22, 16],
        [4, 12, 22, 14, 5, 15, 16, 2, 8, 10, 17, 9, 21, 7, 3, 6, 13, 18, 11, 20, 19, 1],
        [16, 15, 14, 13, 4, 22, 18, 19, 7, 1, 12, 17, 5, 10, 20, 3, 9, 21, 11, 2, 6, 8],
        [20, 14, 21, 12, 15, 17, 4, 19, 13, 10, 11, 1, 16, 5, 18, 7, 8, 22, 9, 6, 3, 2],
        [16, 14, 13, 2, 21, 10, 11, 4, 1, 22, 18, 12, 19, 5, 7, 8, 6, 3, 15, 20, 9, 17],
        [18, 15, 9, 14, 12, 2, 8, 11, 22, 21, 16, 1, 6, 17, 5, 10, 19, 4, 20, 13, 3, 7],
        [7, 3, 10, 14, 13, 21, 18, 6, 20, 4, 9, 8, 22, 15, 2, 1, 5, 12, 19, 17, 11, 16],
        [18, 1, 13, 7, 16, 10, 14, 2, 19, 5, 21, 11, 22, 15, 8, 17, 20, 3, 4, 12, 6, 9],
        [13, 2, 22, 5, 11, 21, 20, 14, 7, 10, 4, 9, 19, 18, 6, 3, 1, 8, 15, 12, 17, 16],
        [14, 17, 21, 8, 2, 9, 6, 4, 5, 13, 22, 7, 15, 3, 1, 18, 16, 11, 10, 12, 20, 19],
        [10, 22, 1, 12, 13, 18, 21, 20, 2, 14, 16, 7, 15, 3, 4, 17, 5, 19, 6, 8, 9, 11],
        [10, 8, 9, 18, 12, 6, 1, 5, 20, 11, 17, 22, 16, 3, 13, 2, 15, 21, 14, 19, 7, 4],
        [7, 17, 22, 5, 3, 10, 13, 18, 9, 1, 14, 15, 21, 19, 16, 12, 8, 6, 11, 20, 4, 2],
        [2, 9, 21, 3, 4, 7, 1, 11, 16, 5, 20, 19, 18, 8, 17, 13, 10, 12, 15, 6, 14, 22],
        [15, 12, 8, 4, 22, 13, 16, 17, 18, 3, 7, 5, 6, 1, 9, 11, 21, 10, 14, 20, 19, 2],
        [15, 16, 2, 11, 17, 7, 5, 14, 20, 4, 21, 3, 10, 9, 12, 8, 13, 6, 18, 19, 22, 1],
        [1, 13, 11, 3, 4, 21, 6, 14, 15, 22, 18, 9, 7, 5, 10, 20, 12, 16, 17, 8, 19, 2],
        [14, 17, 22, 20, 8, 16, 5, 10, 1, 13, 2, 21, 12, 9, 4, 18, 3, 7, 6, 19, 15, 11],
        [9, 17, 7, 4, 5, 13, 21, 18, 11, 3, 22, 1, 6, 16, 20, 14, 15, 10, 8, 2, 12, 19],
        [13, 14, 5, 22, 19, 11, 9, 6, 18, 15, 8, 10, 7, 4, 17, 16, 3, 1, 12, 2, 21, 20],
        [20, 5, 4, 14, 11, 1, 6, 16, 8, 22, 7, 3, 2, 12, 21, 19, 17, 13, 10, 15, 18, 9],
        [3, 7, 14, 15, 6, 5, 21, 20, 18, 10, 4, 16, 19, 1, 13, 9, 8, 17, 11, 12, 22, 2],
        [13, 15, 17, 1, 22, 11, 3, 4, 7, 20, 14, 21, 9, 8, 2, 18, 16, 6, 10, 12, 5, 19]
        ]
NUM_QUERIES = len(QUERY_ORDER[0]) # 22
# End Constants


# Class Definitions
class Result:
    # TODO: add comment
    def __init__(self, title = None):
        self.__title__ = "Result"
        if title:
            self.__title__ = title
        # Stuff for time tracking
        self.__start__ = None
        # Metrics stored in dict
        self.__metrics__ = dict()

    def startTimer(self):
        self.__start__ = datetime.now()

    def stopTimer(self):
        if self.__start__ is not None:
            delta = datetime.now() - self.__start__
            self.__start__ = None
            return delta
        else:
            print("timer not started")
            return None

    def setMetric(self, name, value):
        self.__metrics__[name] = value

    def printPadded(self, txt, width, fill='='):
        space = ' '
        w = int((width - len(txt) - 2 * len(space)) / 2)
        x = len(txt) % 2  # extra fill char if needed
        print(fill * w + space + txt + space + fill * x + fill * w)

    def printResultHeader(self, title):
        title = self.__title__ if not title else title
        width = 60
        print("="*width)
        self.printPadded(title, width)
        print("="*width)

    def printResultFooter(self):
        self.printResultHeader("End Results")

    def printMetrics(self, title=None):
        self.printResultHeader(title)
        for key, value in self.__metrics__.items():
            print("%s: %s" % (key, value))
        self.printResultFooter()

    def saveMetrics(self, run_timestamp, folder):
        path = os.path.join(RESULTS_DIR, run_timestamp, folder)
        os.makedirs(path, exist_ok=True)
        metrics = dict()
        for key, value in self.__metrics__.items():
            metrics[key] = str(value)
        with open(os.path.join(path, self.__title__ + '.json'), 'w') as fp:
            json.dump(metrics, fp, indent=4, sort_keys=True)


class Password(argparse.Action):
    # TODO: add comment
    def __call__(self, parser, namespace, values, option_string):
        if values is None:
            values = getpass.getpass()
        setattr(namespace, self.dest, values)


class PGDB:
    # TODO: add comment
    __connection__ = None
    __cursor__ = None
    
    def __init__(self, host, port, db_name, user, password):
        # Exception handling is done by the method using this.
        self.__connection__ = psycopg2.connect("host='%s' port='%s' dbname='%s' user='%s' password='%s'" %
                                               (host, port, db_name, user, password))
        self.__cursor__ = self.__connection__.cursor()

    def close(self):
        if self.__cursor__ is not None:
            self.__cursor__.close()
            self.__cursor__ = None
        if self.__connection__ is not None:
            self.__connection__.close()
            self.__connection__ = None

    def executeQueryFromFile(self, filepath, function = None):
        if function is None:
            function = lambda x: x
        with open(filepath) as query_file:
            query = query_file.read()
            query = function(query)
            return self.executeQuery(query)

    def executeQuery(self, query):
        if self.__cursor__ is not None:
            self.__cursor__.execute(query)
            return 0
        else:
            print("database has been closed")
            return 1

    def copyFrom(self, filepath, separator, table):
        if self.__cursor__ is not None:
            with open(filepath, 'r') as in_file:
                self.__cursor__.copy_from(in_file, table=table, sep=separator)
            return 0
        else:
            print("database has been closed")
            return 1

    def commit(self):
        if self.__connection__ is not None:
            self.__connection__.commit()
            return 0
        else:
            print("cursor not initialized")
            return 1

# End Class Definitions


def clean_database(query_root, host, port, db_name, user, password):
    """Drops the tables if they exist

    Args:
        query_root (str): Directory in which generated queries directory exists
        host (str): IP/hostname of the PG instance
        port (int): port for the PG instance
        db_name (str): name of the tpch database
        user (str): user for the PG instance
        password (str): password for the PG instance

    Return:
        0 if successful non zero otherwise
    """
    try:
        conn = PGDB(host, port, db_name, user, password)
        try:
            for table in TABLES:
                conn.executeQuery("DROP TABLE IF EXISTS %s " % table)
        except Exception as e:
            print("unable to remove existing tables. %s" % e)
            return 1
        print("dropped existing tables")
        conn.commit()
        conn.close()
        return 0
    except Exception as e:
        print("unable to connect to the database. %s" % e)
        return 1


def create_schema(query_root, host, port, db_name, user, password):
    """Creates the schema for the test. Drops the tables if they exist

    Args:
        query_root (str): Directory in which generated queries directory exists
        host (str): IP/hostname of the PG instance
        port (int): port for the PG instance
        db_name (str): name of the tpch database
        user (str): user for the PG instance
        password (str): password for the PG instance

    Return:
        0 if successful non zero otherwise
    """
    try:
        conn = PGDB(host, port, db_name, user, password)
        try:
            conn.executeQueryFromFile(os.path.join(query_root, PREP_QUERY_DIR, "create_tbl.sql"))
        except Exception as e:
            print("unable to run create tables. %s" % e)
            return 1
        conn.commit()
        conn.close()
    except Exception as e:
        print("unable to connect to the database. %s" % e)
        return 1


def load_tables(query_root, data_dir, host, port, db_name, user, password):
    """Loads data into tables. Expects that tables are already empty.

    Args:
        query_root (str): Directory in which preparation queries directory exists
        data_dir (str): Directory in which load data exists
        host (str): IP/hostname of the PG instance
        port (int): port for the PG instance
        db_name (str): name of the tpch database
        user (str): user for the PG instance
        password (str): password for the PG instance

    Return:
        0 if successful non zero otherwise
    """
    try:
        conn = PGDB(host, port, db_name, user, password)
        try:
            for table in TABLES:
                filepath = os.path.join(data_dir, LOAD_DIR, table.lower() + ".tbl.csv")
                conn.copyFrom(filepath, separator="|", table=table)
            conn.commit()
        except Exception as e:
            print("unable to run load tables. %s" %e)
            return 1
        conn.close()
        return 0
    except Exception as e:
        print("unable to connect to the database. %s" % e)
        return 1


def index_tables(query_root, data_dir, host, port, db_name, user, password):
    """Creates indexes and foreign keys for loaded tables.

    Args:
        query_root (str): Directory in which preparation queries directory exists
        data_dir (str): Directory in which load data exists
        host (str): IP/hostname of the PG instance
        port (int): port for the PG instance
        db_name (str): name of the tpch database
        user (str): user for the PG instance
        password (str): password for the PG instance

    Return:
        0 if successful non zero otherwise
    """
    try:
        conn = PGDB(host, port, db_name, user, password)
        try:
            conn.executeQueryFromFile(os.path.join(query_root, PREP_QUERY_DIR, "create_idx.sql"))
            conn.commit()
        except Exception as e:
            print("unable to run index tables. %s" % e)
            return 1
        conn.close()
        return 0
    except Exception as e:
        print("unable to connect to the database. %s" % e)
        return 1


def grouper(iterable, n, fillvalue=None):
    # TODO: add comment
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def insert_lineitem(cols, conn):
    # TODO: add comment
    li_insert_stmt = """INSERT INTO LINEITEM VALUES (%s, %s, %s, %s, %s, %s, %s, %s, '%s',
                     '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % cols
    conn.executeQuery(li_insert_stmt)


def refresh_func1(conn, data_dir, stream, num_streams, verbose):
    # TODO: add comment
    try:
        if verbose:
            print("Running refresh function #1 in stream #%s" % stream)
        file_nr = stream + 1 # generated files are named 1,2,3,... while streams are indexed 0,1,2,...
        filepath_o = os.path.join(data_dir, UPDATE_DIR, "orders.tbl.u" + str(file_nr) + ".csv")
        filepath_l = os.path.join(data_dir, UPDATE_DIR, "lineitem.tbl.u" + str(file_nr) + ".csv")
        with open(filepath_o) as orders_file, open(filepath_l) as lineitem_file:
            todo_licols = None
            for orders_lines in grouper(orders_file, 100, ''):
                orders_gen = [x.strip() for x in orders_lines if x.strip()]
                for order_line in orders_gen:
                    o_cols = tuple(order_line.split('|'))
                    o_insert_stmt = "INSERT INTO ORDERS VALUES (%s, %s, '%s', %s, '%s', '%s', '%s',  %s, '%s')" % o_cols
                    conn.executeQuery(o_insert_stmt)
                    # As per specification for every ORDERS row we add one to seven LINEITEM rows.
                    if todo_licols:
                        if todo_licols[0] != o_cols[0]:
                            print("bad data file for lineitem. Does not match orders key")
                            return 1
                        else:
                            insert_lineitem(todo_licols, conn)
                            todo_licols = None
                    lineitem_line = lineitem_file.readline()
                    if lineitem_line:
                        li_cols = tuple(lineitem_line.strip().split("|"))
                        while li_cols and o_cols[0] == li_cols[0]:
                            insert_lineitem(li_cols, conn)
                            lineitem_line = lineitem_file.readline()
                            if lineitem_line:
                                li_cols = tuple(lineitem_line.strip().split("|"))
                            else:
                                li_cols = None
                        if li_cols is not None:
                            todo_licols = li_cols

        conn.commit()
        return 0
    except Exception as e:
        print("refresh function #1 failed. %s" % e)
        return 1


def refresh_func2(conn, data_dir, stream, num_streams, verbose):
    # TODO: add comment
    try:
        if verbose:
            print("Running refresh function #2 in stream #%s" % stream)
        file_nr = stream + 1
        filepath = os.path.join(data_dir, DELETE_DIR, "delete." + str(file_nr) + ".csv")
        with open(filepath, 'r') as in_file:
            for ids in grouper(in_file, 100, ''):
                query = "DELETE FROM orders WHERE O_ORDERKEY IN (%s)" % ", ".join([x.strip() for x in ids if x.strip()])
                conn.executeQuery(query)
        conn.commit()
        return 0
    except Exception as e:
        print("refresh function #2 failed. %s" % e)
        return 1


def run_query_stream(conn, query_root, stream, num_streams, result, verbose):
    # TODO: add comment
    index = stream % len(QUERY_ORDER)
    order = QUERY_ORDER[index]
    for i in range(0, 22):
        try:
            if verbose:
                print("Running query #%s in stream #%s ..." % (order[i], stream))
            filepath = os.path.join(query_root, GENERATED_QUERY_DIR, str(order[i]) + ".sql")
            result.startTimer()
            conn.executeQueryFromFile(filepath)
            result.setMetric(QUERY_METRIC % (stream, order[i]), result.stopTimer())
        except Exception as e:
            print("unable to execute query %s in stream %s: %s" % (order[i], stream, e))
            return 1
    return 0


def run_power_test(query_root, data_dir, host, port, db_name, user, password,
                   run_timestamp, num_streams, verbose, read_only):
    # TODO: add comment
    try:
        print("Power test started ...")
        conn = PGDB(host, port, db_name, user, password)
        result = Result("Power")
        result.startTimer()
        stream = 0 # constant for power test
        #
        if not read_only:
            if refresh_func1(conn, data_dir, stream, num_streams, verbose):
                return 1
        result.setMetric(REFRESH_METRIC % (stream, 1), result.stopTimer())
        #
        if run_query_stream(conn, query_root, stream, num_streams, result, verbose):
            return 1
        #
        result.startTimer()
        if not read_only:
            if refresh_func2(conn, data_dir, stream, num_streams, verbose):
                return 1
        result.setMetric(REFRESH_METRIC % (stream, 2), result.stopTimer())
        #
        print("Power test finished.")
        if verbose:
            result.printMetrics()
        result.saveMetrics(run_timestamp, "power")
    except Exception as e:
        print("unable to run power test. DB connection failed: %s" % e)
        return 1


def run_throughput_inner(query_root, data_dir, host, port, db_name, user, password, 
                         stream, num_streams, q, verbose):
    # TODO: add comment
    try:
        conn = PGDB(host, port, db_name, user, password)
        result = Result("ThroughputQueryStream%s" % stream)
        if run_query_stream(conn, query_root, stream, num_streams, result, verbose):
            print("unable to finish query in stream #%s" % stream)
            exit(1)
        q.put(result)
    except Exception as e:
        print("unable to connect to DB for query in stream #%s: %s" % (stream, e))
        exit(1)


def run_throughput_test(query_root, data_dir, host, port, db_name, user, password,
                        run_timestamp, num_streams, verbose, read_only):
    # TODO: add comment
    try:
        print("Throughput test started ...")
        conn = PGDB(host, port, db_name, user, password)
        total = Result("ThroughputTotal")
        total.startTimer()
        processes = []
        q = Queue()
        for i in range(num_streams):
            stream = i + 1
            # queries
            print("Throughput test in stream #%s started ..." % stream)
            p = Process(target=run_throughput_inner,
                        args=(query_root, data_dir, host, port, db_name, user, password,
                              stream, num_streams, q, verbose))
            processes.append(p)
            p.start()
        result = Result("ThroughputRefreshStream")
        for i in range(num_streams):
            stream = i + 1
            # refresh functions
            result.startTimer()
            if not read_only:
                if refresh_func1(conn, data_dir, stream, num_streams, verbose):
                    return 1
            result.setMetric(REFRESH_METRIC % (stream, 1), result.stopTimer())
            #
            result.startTimer()
            if not read_only:
                if refresh_func2(conn, data_dir, stream, num_streams, verbose):
                    return 1
            result.setMetric(REFRESH_METRIC % (stream, 2), result.stopTimer())
            #
        q.put(result)
        for p in processes:
            p.join()
        print("Throughput test finished.")
        for i in range(q.qsize()):
            res = q.get(False)
            if verbose:
                res.printMetrics()
            res.saveMetrics(run_timestamp, THROUGHPUT)
        #
        total.setMetric(THROUGHPUT_TOTAL_METRIC, total.stopTimer())
        if verbose:
            total.printMetrics()
        total.saveMetrics(run_timestamp, THROUGHPUT)
        #
    except Exception as e:
        print("unable to execute throughput test: %s" % e)
        return 1


def scale_to_num_streams(scale):
    # TODO: add comment
    num_streams = 2
    if scale <= 1:
        num_streams = 2
    elif scale <= 10:
        num_streams = 3
    elif scale <= 30:
        num_streams = 4
    elif scale <= 100:
        num_streams = 5
    elif scale <= 300:
        num_streams = 6
    elif scale <= 1000:
        num_streams = 7
    elif scale <= 3000:
        num_streams = 8
    elif scale <= 10000:
        num_streams = 9
    elif scale <= 30000:
        num_streams = 10
    else:
        num_streams = 11
    return num_streams


def get_json_files_from(path):
    # TODO: add comment
    json_files = [pos_json for pos_json in os.listdir(path) if pos_json.endswith('.json')]
    json_files = [os.path.join(path, s) for s in json_files]
    return json_files


def get_json_files(path):
    # TODO: add comment
    json_files = []
    for run_timestamp in os.listdir(os.path.join(path)):
        for mode in [POWER, THROUGHPUT]:
            sub_dir = os.path.join(path, run_timestamp, mode)
            if os.path.exists(sub_dir) and os.path.isdir(sub_dir):
                json_files += get_json_files_from(sub_dir)
    return json_files


def load_results():
    # TODO: add comment
    results = []
    for json_filename in get_json_files(RESULTS_DIR):
        with open(json_filename, 'r') as json_file:
            raw = json_file.read()
            js = json.loads(raw)
            for key, value in js.items():
                results.append({"key": key, "value": value})
    return results


def get_timedelta_in_seconds(time_interval):
    # TODO: add comment
    (hours, minutes, sf) = time_interval.split(":")
    (seconds, fraction) = sf.split(".")
    secs = int(hours) * 60 * 60 + \
           int(minutes) * 60 + \
           int(seconds) + \
           int(fraction) / 1000000
    return secs


def get_average(results, metric_name):
    # TODO: add comment
    values = [js["value"] for js in results if js["key"] == metric_name]
    seconds = [get_timedelta_in_seconds(value) for value in values]
    avg = sum(seconds) / len(values)
    return avg


def qi(results, i, s):
    # TODO: add comment
    # execution time for query Qi within the query stream s
    # i is the ordering number of the query ranging from 1 to 22
    # s is 0 for the power function and the position of the query stream for the throughput test
    assert(1 <= i <= 22)
    assert(0 <= s)
    metric_name = QUERY_METRIC % (s, i)
    ret = get_average(results, metric_name)
    return ret


def ri(results, j, s):
    # TODO: add comment
    # execution time for the refresh function RFi within a refresh stream s
    # j is the ordering function of the refresh function ranging from 1 to 2
    # s is 0 for the power function and the position of the pair of refresh functions
    # in the stream for the throughput test
    assert(j == 1 or j == 2)
    assert(0 <= s)
    metric_name = REFRESH_METRIC % (s, j)
    ret = get_average(results, metric_name)
    return ret


def ts(results):
    # TODO: add comment
    # total time needed to execute the throughput test
    metric_name = THROUGHPUT_TOTAL_METRIC
    ret = get_average(results, metric_name)
    return ret


def get_power_size(results, scale_factor):
    # TODO: add comment
    qi_product = 1
    for i in range(1, NUM_QUERIES + 1):
        qi_product *= qi(results, i, 0)
    ri_product = 1
    for j in [1, 2]:  # two refresh functions
        ri_product *= ri(results, j, 0)
    denominator = math.pow(qi_product * ri_product, 1/24)
    power_size = (3600 / denominator) * scale_factor
    return power_size


def get_throughput_size(results, scale_factor, num_streams):
    # TODO: add comment
    throughput_size = ((num_streams * NUM_QUERIES) / ts(results)) * 3600 * scale_factor
    return throughput_size


def get_qphh_size(power_size, throughput_size):
    # TODO: add comment
    qphh_size = math.sqrt(power_size * throughput_size)
    return qphh_size


def calc_metrics(run_timestamp, scale_factor, num_streams):
    # TODO: add comment
    results = load_results()
    r = Result("Metric")
    #
    power_size = get_power_size(results, scale_factor)
    r.setMetric("power_size", power_size)
    print("Power@Size = %s" % power_size)
    #
    throughput_size = get_throughput_size(results, scale_factor, num_streams)
    r.setMetric("throughput_size", throughput_size)
    print("Throughput@Size = %s" % throughput_size)
    #
    qphh_size = get_qphh_size(power_size, throughput_size)
    r.setMetric("qphh_size", qphh_size)
    print("QphH@Size = %s" % qphh_size)
    #
    r.printMetrics("Metrics")
    r.saveMetrics(run_timestamp, "metrics")


def main(phase, host, port, user, password, database, data_dir, query_root, dbgen_dir,
         scale, num_streams, verbose, read_only):
    # TODO: add comment
    run_timestamp = "run_%s" % time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    if phase == "prepare":
        # try to build dbgen from source and quit if failed
        if prep.build_dbgen(dbgen_dir):
            print("could not build the dbgen/querygen. Check logs.")
            exit(1)
        print("built dbgen from source")
        # try to generate data files
        if prep.generate_data(dbgen_dir, data_dir,
                              LOAD_DIR, UPDATE_DIR, DELETE_DIR,
                              scale, num_streams):
            print("could not generate data files.")
            exit(1)
        print("created data files in %s" % data_dir)
        if prep.generate_queries(dbgen_dir, query_root, TEMPLATE_QUERY_DIR, GENERATED_QUERY_DIR):
            print("could not generate query files")
            exit(1)
        print("created query files in %s" % query_root)
    elif phase == "load":
        result = Result("Load")
        if clean_database(query_root, host, port, database, user, password):
            print("could not clean the database.")
            exit(1)
        print("cleaned database %s" % database)
        result.startTimer()
        if create_schema(query_root, host, port, database, user, password):
            print("could not create schema.")
            exit(1)
        result.setMetric("create_schema: ", result.stopTimer())
        print("done creating schemas")
        result.startTimer()
        if load_tables(query_root, data_dir, host, port, database, user, password):
            print("could not load data to tables")
            exit(1)
        result.setMetric("load_data", result.stopTimer())
        print("done loading data to tables")
        result.startTimer()
        if index_tables(query_root, data_dir, host, port, database, user, password):
            print("could not create indexes for tables")
            exit(1)
        result.setMetric("index_tables", result.stopTimer())
        print("done creating indexes and foreign keys")
        result.printMetrics()
        result.saveMetrics(run_timestamp, "load")
    elif phase == "query":
        if run_power_test(query_root, data_dir, host, port, database, user, password,
                          run_timestamp, num_streams, verbose, read_only):
            print("running power test failed")
            exit(1)
        # Throughput test
        if run_throughput_test(query_root, data_dir, host, port, database, user, password,
                               run_timestamp, num_streams, verbose, read_only):
            print("running throughput test failed")
            exit(1)
        print("done performance test")
        calc_metrics(run_timestamp, scale, num_streams)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PGTPCH")

    default_host = "localhost"
    default_port = 5432
    default_username = "postgres"
    default_password = "test123"
    default_dbname = "tpch"
    default_data_dir = os.path.join(".", "data")
    default_query_root = os.path.join(".", "query_root")
    default_dbgen_dir = os.path.join(".", "tpch-dbgen")
    default_scale = 1.0
    default_num_streams = 0

    parser.add_argument("phase", choices=["prepare", "load", "query"],
                        help="Phase of PGTPCH to run.")
    parser.add_argument("-H", "--host", default=default_host,
                        help="Address of host on which PostgreSQL instance runs; default is %s" % default_host)
    parser.add_argument("-p", "--port", type=int, default=default_port,
                        help="Port on which PostgreSQL instance runs; default is %s" % str(default_port))
    parser.add_argument("-U", "--username", default=default_username,
                        help="User for the PostgreSQL instance; default is %s" % default_username)
    parser.add_argument("-W", "--password", nargs='?', default=default_password, action=Password,
                        help="Password for the PostgreSQL instance; default is %s" % default_password)
    parser.add_argument("-d", "--dbname", default=default_dbname,
                        help="Name of the database; default is %s" % default_dbname)
    parser.add_argument("-i", "--data-dir", default=default_data_dir,
                        help="Directory for generated data; default is %s" % default_data_dir)
    parser.add_argument("-q", "--query-root", default=default_query_root,
                        help="Directory for query files; default is %s" % default_query_root)
    parser.add_argument("-g", "--dbgen-dir", default=default_dbgen_dir,
                        help="Directory containing tpch dbgen source; default is %s" % default_dbgen_dir)
    parser.add_argument("-s", "--scale", type=float, default=default_scale,
                        help="Size of the data generated, scale factor; default is %s = 1GB" % default_scale)
    parser.add_argument("-n", "--num-streams", type=int, default=default_num_streams,
                        help="Number of streams to run the throughput test with; default is %s" % default_num_streams +
                             ", i.e. based on scale factor SF")
    parser.add_argument("-b", "--verbose", action="store_true",
                        help="Print more information to standard output")
    parser.add_argument("-r", "--read-only", action="store_true",
                        help="Do not execute refresh functions during the query phase, " +
                             "which allows for running it repeatedly")
    args = parser.parse_args()

    # Extract all arguments into variables
    phase = args.phase
    host = args.host
    port = args.port
    database = args.dbname
    data_dir = args.data_dir
    query_root = args.query_root
    dbgen_dir = args.dbgen_dir
    scale = args.scale
    num_streams = args.num_streams
    user = args.username
    password = args.password
    verbose = args.verbose
    read_only = args.read_only

    # if no num_streams was provided, then calculate default based on scale factor
    if num_streams == 0:
        num_streams = scale_to_num_streams(scale)

    # main
    main(phase, host, port, user, password, database, data_dir, query_root, dbgen_dir, scale, num_streams, verbose, read_only)
