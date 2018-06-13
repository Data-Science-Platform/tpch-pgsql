#!/usr/bin/env python3

import psycopg2
import argparse
import os
import subprocess
import re
import shutil
from tempfile import mkstemp
from datetime import datetime
import glob
import getpass
from itertools import zip_longest

## Constants
LOAD_DIR = "load"
UPDATE_DIR = "update"
DELETE_DIR = "delete"
TEMPLATE_QUERY_DIR = "perf_query_template"
GENERATED_QUERY_DIR = "perf_query_gen"
PREP_QUERY_DIR = "prep_query"
TABLES = ['LINEITEM', 'PARTSUPP', 'ORDERS', 'CUSTOMER', 'SUPPLIER', 'NATION', 'REGION', 'PART']
## End Constants


## Class Definitions
class Result:

    ## Stuff for time tracking
    __start__ = None

    ## Metrics for Load Phase
    __create_time__ = 0.0
    __load_time__ = 0.0
    __idx_time__ = 0.0

    ## Metrics for Sequential Performance Test
    __refresh_1_time__ = 0.0
    __query_time__ = 0.0
    __refresh_2_time__ = 0.0

    ## Setters
    def setCreateTime(self, time):
        self.__create_time__ = time

    def setLoadTime(self, time):
        self.__load_time__ = time

    def setIndexingTime(self, time):
        self.__idx_time__ = time

    def setSeqTime(self, refresh1, query, refresh2):
        self.__refresh_1_time__ = refresh1
        self.__query_time__ = query
        self.__refresh_2_time__ = refresh2

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

    def printResultHeader(self):
         print("========================================")
         print("================RESULTS=================")
         print("========================================")

    def printResultFooter(self):
         print("========================================")
         print("===============END RESULTS==============")
         print("========================================")

    def printLoadTime(self):
         self.printResultHeader()
         print("Time taken to create schema: %s" % self.__create_time__)
         print("Time taken to load data: %s" % self.__load_time__)
         print("Time taken to index data: %s" % self.__idx_time__)
         self.printResultFooter()

    def printSeqTime(self):
         self.printResultHeader()
         print("Time taken for refresh function 1: %s" % self.__refresh_1_time__)
         print("Time taken for querying data: %s" % self.__query_time__)
         print("Time taken for refresh function 2: %s" % self.__refresh_2_time__)
         self.printResultFooter()



class Password(argparse.Action):
     def __call__(self, parser, namespace, values, option_string):
         if values is None:
             values = getpass.getpass()
         setattr(namespace, self.dest, values)


class PGDB:
    __connection__ = None
    __cursor__ = None

    def __init__(self, host, port, db_name, user, password):
        # Exception handling is done by the method using this.
        self.__connection__ = psycopg2.connect("host='%s' port='%s' dbname='%s' user='%s' password='%s'" % (host, port, db_name, user, password))
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
                self.__cursor__.copy_from(in_file, table = table, sep = separator)
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

##End Class Definitions


def build_dbgen(dbgen_dir):
    """Compiles the dbgen from source.

    The Makefile must be present in the same directory as this script.

    Args:
        dbgen_dir (str): Directory in which the source code is placed.

    Return:
        0 if successful non zero otherwise
    """
    cur_dir = os.getcwd()
    p = subprocess.Popen(["make", "-f", os.path.join(cur_dir, "Makefile")], cwd = dbgen_dir)
    p.communicate()
    return p.returncode

def inner_generate_data(data_dir, dbgen_dir, file_pattern, out_ext):
    try:
        os.makedirs(data_dir, exist_ok = True)
        for in_fname in glob.glob(os.path.join(dbgen_dir, file_pattern)):
            fname = os.path.basename(in_fname)
            out_fname = os.path.join(data_dir, fname + out_ext)
            try:
                with open(in_fname) as in_file, open(out_fname, "w") as out_file:
                    for inline in in_file:
                        outline = re.sub("\|$", "", inline)
                        out_file.write(outline)
                os.remove(in_fname)
            except IOError as e:
                print("something bad happened while transforming data files. (%s)" % e)
                return 1
    except IOError as e:
        print("unable to create data directory %s. (%s)" % (data_dir, e))
        return 1
    ## All files written successfully. Return success code.
    return 0

def generate_data(dbgen_dir, data_dir, scale, num_streams):
    """Generates data for the loading into tables.

    Args:
        dbgen_dir (str): Directory in which the source code is placed.
        data_dir (str): the directory where generated data would be placed.
        scale (float): Amount of data to be generated. 1 = 1GB.
        num_streams (int): Number of streams on which the throuput test is going to be performed.

    Return:
        0 if successful non zero otherwise
    """
    p = subprocess.Popen(["./dbgen", "-vf", "-s", str(scale)], cwd = dbgen_dir)
    p.communicate()
    if (not p.returncode):
        load_dir = os.path.join(data_dir, LOAD_DIR)
        if inner_generate_data(load_dir, dbgen_dir, "*.tbl", ".csv"):
            print("unable to generate data for load phase")
            return 1
        print("generated data for the load phase")
    else:
        return p.returncode

    ## Update/Delete phase data
    p = subprocess.Popen(["./dbgen", "-vf", "-s", str(scale), "-U", str(num_streams)],
                            cwd = dbgen_dir)
    p.communicate()
    if (not p.returncode):
        update_dir = os.path.join(data_dir, UPDATE_DIR)
        delete_dir = os.path.join(data_dir, DELETE_DIR)
        if inner_generate_data(update_dir, dbgen_dir, "*.tbl.u*", ".csv"):
            print("unable to generate data for the update phase")
            return 1
        print("generated data for the update phase")
        if inner_generate_data(delete_dir, dbgen_dir, "delete.*", ".csv"):
            print("unable to generate data for the delete phase")
            return 1
        print("generated data for the delete phase")
        ## All files written successfully. Return success code.
        return 0
    else:
        return p.returncode


def generate_queries(dbgen_dir, query_root):
    """Generates queries for performance tests.

    Args:
        dbgen_dir (str): Directory in which the source code is placed.
        query_root (str): Directory in which query templates directory exists.
                          Also the place where the generated queries are going to be placed.

    Return:
        0 if successful non zero otherwise
    """
    query_root = os.path.abspath(query_root)
    dss_query = os.path.join(query_root, TEMPLATE_QUERY_DIR)
    query_env = os.environ.copy()
    query_env['DSS_QUERY'] = dss_query
    query_gen_dir = os.path.join(query_root, GENERATED_QUERY_DIR)
    os.makedirs(query_gen_dir, exist_ok = True)
    for i in range(1, 23):
        try:
            with open(os.path.join(query_gen_dir, str(i) + ".sql"), "w") as out_file:
                p = subprocess.Popen(["./qgen", str(i)], cwd = dbgen_dir,
                                        env = query_env, stdout = out_file)
                p.communicate()
                if p.returncode:
                    print("Process returned non zero when generating query number %s" % i)
                    return p.returncode
        except IOError as e:
            print("IO Error during query generation %s" % e)
            return 1
    return p.returncode

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
            print("unable to run create tables. %s" %e)
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
                conn.copyFrom(filepath, separator = "|", table = table)
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
            print("unable to run index tables. %s" %e)
            return 1
        conn.close()
        return 0
    except Exception as e:
        print("unable to connect to the database. %s" % e)
        return 1


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def insert_lineitem(cols, conn):
    li_insert_stmt = "INSERT INTO LINEITEM VALUES (%s, %s, %s, %s, %s, %s, %s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % cols
    conn.executeQuery(li_insert_stmt)


def refresh_func1(conn, data_dir, stream_num):
    try:
        filepath_o = os.path.join(data_dir, UPDATE_DIR, "orders.tbl.u" + str(stream_num) + ".csv")
        filepath_l = os.path.join(data_dir, UPDATE_DIR, "lineitem.tbl.u" + str(stream_num) + ".csv")
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
        print("refresh function 1 failed. %s" %e)
        return 1


def refresh_func2(conn, data_dir, stream_num):
    try:
        filepath = os.path.join(data_dir, DELETE_DIR, "delete." + str(stream_num) + ".csv")
        with open(filepath, 'r') as in_file:
            for ids in grouper(in_file, 100, ''):
                query = "DELETE FROM orders WHERE O_ORDERKEY IN (%s)" % ", ".join([x.strip() for x in ids if x.strip()])
                conn.executeQuery(query)
        conn.commit()
        return 0
    except Exception as e:
        print("refresh function 1 failed. %s" %e)
        return 1


def run_query_stream(conn, query_root, start_idx = 1):
    for i in range(0, 22):
        try:
            curr_idx = str((start_idx + i) % 23)
            filepath = os.path.join(query_root, GENERATED_QUERY_DIR, curr_idx + ".sql")
            conn.executeQueryFromFile(filepath)
        except Exception as e:
            print("unable to execute query %s. %s" % (curr_idx, e))
            return 1
    return 0


def run_power_test(query_root, data_dir, result, host, port, db_name, user, password):
    try:
        conn = PGDB(host, port, db_name, user, password)
        result.startTimer()
        if refresh_func1(conn, data_dir, 1):
            return 1
        rf1_t = result.stopTimer()
        result.startTimer()
        if run_query_stream(conn, query_root, 1):
            return 1
        qs_t = result.stopTimer()
        result.startTimer()
        if refresh_func2(conn, data_dir, 1):
            return 1
        rf2_t = result.stopTimer()
        result.setSeqTime(rf1_t, qs_t, rf2_t)
    except Exception as e:
        print("unable to run power test. DB connection failed. %s" % e)
        return 1


def main(phase, host, port, user, password, database, data_dir, query_root, dbgen_dir,
            scale, num_streams):
    if phase == "prepare":
        ## try to build dbgen from source and quit if failed
        if build_dbgen(dbgen_dir):
            print("could not build the dbgen/querygen. Check logs.")
            exit(1)
        print("built dbgen from source")
        ## try to generate data files
        if generate_data(dbgen_dir, data_dir, scale, num_streams):
            print("could not generate data files.")
            exit(1)
        print("created data files in %s" % data_dir)
        if generate_queries(dbgen_dir, query_root):
            print("could not generate query files")
            exit(1)
        print("created query files in %s" % query_root)
    elif phase == "load":
        result = Result()
        if clean_database(query_root, host, port, database, user, password):
            print("could not clean the database.")
            exit(1)
        print("cleaned database %s" % database)
        result.startTimer()
        if create_schema(query_root, host, port, database, user, password):
            print("could not create schema.")
            exit(1)
        result.setCreateTime(result.stopTimer())
        print("done creating schemas")
        result.startTimer()
        if load_tables(query_root, data_dir, host, port, database, user, password):
            print("could not load data to tables")
            exit(1)
        result.setLoadTime(result.stopTimer())
        print("done loading data to tables")
        result.startTimer()
        if index_tables(query_root, data_dir, host, port, database, user, password):
            print("could not create indexes for tables")
            exit(1)
        result.setIndexingTime(result.stopTimer())
        print("done creating indexes and foreign keys")
        result.printLoadTime()
    elif phase == "query":
        # TODO: this thing should run twice and average the results
        result = Result()
        if run_power_test(query_root, data_dir, result, host, port, database, user, password):
            print("Running power test failed")
            exit(1)
        print("Power test finished.")
        result.printSeqTime()
        # TODO: add throughput test here


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "PGTPCH")
    parser.add_argument("phase",  help = "Phase of PGTPCH to run.", choices = ["prepare", "load", "query"])
    parser.add_argument("-a", "--host", default = "localhost", help = "Address of host on which PostgreSQL instance runs; defualt is localhost")
    parser.add_argument("-p", "--port", type = int, default = 5432, help = "Port on which PostgreSQL instance runs; default is 5432")
    parser.add_argument("-u", "--user", default = "postgres", help = "User for the PostgreSQL instance; default is postgres")
    parser.add_argument("-v", "--password", nargs = '?', default = "test123", action = Password, help = "Password for the PostgreSQL instance; default is test123")
    parser.add_argument("-d", "--database", default = "tpch", help = "Name of the database; default is tpch")
    parser.add_argument("-i", "--data-dir", default = "./data", help = "Directory for generated data; default is ./data")
    parser.add_argument("-q", "--query-root", default = "./query_root", help = "Directory for query files; defualt is ./query_root")
    parser.add_argument("-g", "--dbgen-dir", default = "./tpch-dbgen", help = "Directory containing tpch dbgen source; default is ./tpch-dbgen")
    parser.add_argument("-s", "--scale", type = float, default = 1.0, help = "Size of the data generated; default is 1.0 = 1GB")
    parser.add_argument("-n", "--num-streams", type = float, default = 1, help = "Number of streams to run the throughput test with; default is 1")
    args = parser.parse_args()

    ## Extract all arguments into variables
    phase = args.phase
    host = args.host
    port = args.port
    database = args.database
    data_dir = args.data_dir
    query_root = args.query_root
    dbgen_dir = args.dbgen_dir
    scale = args.scale
    num_streams = args.num_streams
    user = args.user
    password = args.password

    ## main
    main(phase, host, port, user, password, database, data_dir, query_root, dbgen_dir, scale, num_streams)

