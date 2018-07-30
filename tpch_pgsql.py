#!/usr/bin/env python3

import argparse
import os
import time

from modules import postgresqldb as pgdb, load, query, prepare as prep, result as r

# Constants

# default values for command line arguments:
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432
DEFAULT_USERNAME = "postgres"
DEFAULT_PASSWORD = "test123"
DEFAULT_DBNAME = "tpch"
DEFAULT_DATA_DIR = os.path.join(".", "data")
DEFAULT_QUERY_ROOT = os.path.join(".", "query_root")
DEFAULT_DBGEN_DIR = os.path.join(".", "tpch-dbgen")
DEFAULT_SCALE = 1.0
DEFAULT_NUM_STREAMS = 0

# other constants
LOAD_DIR = "load"
UPDATE_DIR = "update"
DELETE_DIR = "delete"
TEMPLATE_QUERY_DIR = "perf_query_template"
GENERATED_QUERY_DIR = "perf_query_gen"
PREP_QUERY_DIR = "prep_query"
RESULTS_DIR = "results"
TABLES = ['LINEITEM', 'PARTSUPP', 'ORDERS', 'CUSTOMER', 'SUPPLIER', 'NATION', 'REGION', 'PART']
# End Constants


def scale_to_num_streams(scale):
    """
    Converts scale factor to number of streams as defined in
    https://github.com/slavong/tpch-pgsql/blob/master/iceis2012.pdf
    on page 6 in section 3.3.4 Throughput Tests in table 2

    :param scale: scale factor, 1.0 = 1GB
    :return: number of streams
    """
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


def main(phase, host, port, user, password, database,
         dbgen_dir, data_dir, query_root,
         scale, num_streams, verbose, read_only):
    """Runs main code for three different phases.
    It expects parsed command line arguments, with default already applied.

    :param phase: prepare, load or query
    :param host: hostname where the Postgres database is running
    :param port: port number where the Postgres database is listening
    :param user: username of the Postgres user with full access to the benchmark DB
    :param password: password for the Postgres user
    :param database: database name, where the benchmark will be run
    :param dbgen_dir: directory where dbgen is to be run
    :param data_dir: subdirectory with data to be loaded
    :param query_root: subdirectory with SQL statements
    :param scale: scale factor, 1.0 = 1GB
    :param num_streams: number of streams
    :param verbose: True is more verbose output is required
    :param read_only: True if no update/delete statements are to be executed during throughput test (query phase)
    :return: no return value, uses exit(1) if something goes wrong
    """
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
        result = r.Result("Load")
        if load.clean_database(query_root, host, port, database, user, password, TABLES):
            print("could not clean the database.")
            exit(1)
        print("cleaned database %s" % database)
        result.startTimer()
        if load.create_schema(query_root, host, port, database, user, password, PREP_QUERY_DIR):
            print("could not create schema.")
            exit(1)
        result.setMetric("create_schema: ", result.stopTimer())
        print("done creating schemas")
        result.startTimer()
        if load.load_tables(data_dir, host, port, database, user, password, TABLES, LOAD_DIR):
            print("could not load data to tables")
            exit(1)
        result.setMetric("load_data", result.stopTimer())
        print("done loading data to tables")
        result.startTimer()
        if load.index_tables(query_root, host, port, database, user, password, PREP_QUERY_DIR):
            print("could not create indexes for tables")
            exit(1)
        result.setMetric("index_tables", result.stopTimer())
        print("done creating indexes and foreign keys")
        result.printMetrics()
        result.saveMetrics(RESULTS_DIR, run_timestamp, "load")
    elif phase == "query":
        if query.run_power_test(query_root, data_dir, UPDATE_DIR, DELETE_DIR, GENERATED_QUERY_DIR, RESULTS_DIR,
                                host, port, database, user, password,
                                run_timestamp, num_streams, verbose, read_only):
            print("running power tests failed")
            exit(1)
        # Throughput tests
        if query.run_throughput_test(query_root, data_dir, UPDATE_DIR, DELETE_DIR, GENERATED_QUERY_DIR, RESULTS_DIR,
                                     host, port, database, user, password,
                                     run_timestamp, num_streams, verbose, read_only):
            print("running throughput tests failed")
            exit(1)
        print("done performance tests")
        query.calc_metrics(RESULTS_DIR, run_timestamp, scale, num_streams)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="tpch_pgsql")

    parser.add_argument("phase", choices=["prepare", "load", "query"],
                        help="Phase of TPC-H benchmark to run.")
    parser.add_argument("-H", "--host", default=DEFAULT_HOST,
                        help="Address of host on which PostgreSQL instance runs; default is %s" % DEFAULT_HOST)
    parser.add_argument("-p", "--port", type=int, default=DEFAULT_PORT,
                        help="Port on which PostgreSQL instance runs; default is %s" % str(DEFAULT_PORT))
    parser.add_argument("-U", "--username", default=DEFAULT_USERNAME,
                        help="User for the PostgreSQL instance; default is %s" % DEFAULT_USERNAME)
    parser.add_argument("-W", "--password", nargs='?', default=DEFAULT_PASSWORD, action=pgdb.Password,
                        help="Password for the PostgreSQL instance; default is %s" % DEFAULT_PASSWORD)
    parser.add_argument("-d", "--dbname", default=DEFAULT_DBNAME,
                        help="Name of the database; default is %s" % DEFAULT_DBNAME)
    parser.add_argument("-i", "--data-dir", default=DEFAULT_DATA_DIR,
                        help="Directory for generated data; default is %s" % DEFAULT_DATA_DIR)
    parser.add_argument("-q", "--query-root", default=DEFAULT_QUERY_ROOT,
                        help="Directory for query files; default is %s" % DEFAULT_QUERY_ROOT)
    parser.add_argument("-g", "--dbgen-dir", default=DEFAULT_DBGEN_DIR,
                        help="Directory containing tpch dbgen source; default is %s" % DEFAULT_DBGEN_DIR)
    parser.add_argument("-s", "--scale", type=float, default=DEFAULT_SCALE,
                        help="Size of the data generated, scale factor; default is %s = 1GB" % DEFAULT_SCALE)
    parser.add_argument("-n", "--num-streams", type=int, default=DEFAULT_NUM_STREAMS,
                        help="Number of streams to run the throughput tests with; default is %s" % DEFAULT_NUM_STREAMS +
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
    main(phase, host, port, user, password, database, dbgen_dir, data_dir, query_root, scale, num_streams, verbose, read_only)
