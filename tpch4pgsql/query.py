import os
import math
import json
from itertools import zip_longest
from multiprocessing import Process, Queue

from tpch4pgsql import postgresqldb as pgdb, result as r

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


def grouper(iterable, n, fillvalue=None):
    """Fill iterable up to N values by using fillvalue

    :param iterable: iterable
    :param n: number of values needed
    :param fillvalue: value to be used to fill missing values
    :return: list of values filled up to n elements by using fillvalue
    """
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def insert_lineitem(cols, conn):
    """Insert a row into table LINEITEM

    :param cols: tuple with values to be inserted,
    order of the values must be the same as order of the columns in the target table
    :param conn: open connection to the database
    :return: 0 if successful, 1 otherwise
    """
    li_insert_stmt = """INSERT INTO lineitem VALUES (%s, %s, %s, %s, %s, %s, %s, %s, '%s',
                     '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % cols
    conn.executeQuery(li_insert_stmt)


def refresh_func1(conn, data_dir, update_dir, stream, num_streams, verbose):
    """Run refresh function #1 (update)

    :param conn: open connection to the database
    :param data_dir: subdirectory with data to be loaded
    :param update_dir: subdirectory with data to be updated
    :param stream: stream number
    :param num_streams: total number of streams
    :param verbose: True if more verbose output is required
    :return: 0 if successful, 1 otherwise
    """
    try:
        if verbose:
            print("Running refresh function #1 in stream #%s" % stream)
        file_nr = stream + 1  # generated files are named 1,2,3,... while streams are indexed 0,1,2,...
        filepath_o = os.path.join(data_dir, update_dir, "orders.tbl.u" + str(file_nr) + ".csv")
        filepath_l = os.path.join(data_dir, update_dir, "lineitem.tbl.u" + str(file_nr) + ".csv")
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


def refresh_func2(conn, data_dir, delete_dir, stream, num_streams, verbose):
    """Run refresh function #2 (delete)

    :param conn: open connection to the database
    :param data_dir: subdirectory with data to be loaded
    :param delete_dir: subdirectory with data to be deleted
    :param stream: stream number
    :param num_streams: total number of streams
    :param verbose: True if more verbose output is required
    :return: 0 if successful, 1 otherwise
    """
    try:
        if verbose:
            print("Running refresh function #2 in stream #%s" % stream)
        file_nr = stream + 1
        filepath = os.path.join(data_dir, delete_dir, "delete." + str(file_nr) + ".csv")
        with open(filepath, 'r') as in_file:
            for ids in grouper(in_file, 100, ''):
                query = "DELETE FROM orders WHERE O_ORDERKEY IN (%s)" % ", ".join([x.strip() for x in ids if x.strip()])
                conn.executeQuery(query)
        conn.commit()
        return 0
    except Exception as e:
        print("refresh function #2 failed. %s" % e)
        return 1


def run_query_stream(conn, query_root, generated_query_dir, stream, num_streams, result, verbose):
    """

    :param conn: open connection to the database
    :param query_root: directory where generated SQL statements are stored
    :param generated_query_dir: subdirectory with generated queries
    :param stream: stream number
    :param num_streams: total number of streams
    :param result: result object for string start and stop times
    :param verbose: True if more verbose output is required
    :return: 0 if successful, 1 otherwise
    """
    index = stream % len(QUERY_ORDER)
    order = QUERY_ORDER[index]
    for i in range(0, 22):
        try:
            if verbose:
                print("Running query #%s in stream #%s ..." % (order[i], stream))
            filepath = os.path.join(query_root, generated_query_dir, str(order[i]) + ".sql")
            result.startTimer()
            conn.executeQueryFromFile(filepath)
            result.setMetric(QUERY_METRIC % (stream, order[i]), result.stopTimer())
        except Exception as e:
            print("unable to execute query %s in stream %s: %s" % (order[i], stream, e))
            return 1
    return 0


def run_power_test(query_root, data_dir, update_dir, delete_dir, generated_query_dir, results_dir,
                   host, port, database, user, password,
                   run_timestamp, num_streams, verbose, read_only):
    """

    :param query_root: directory where generated SQL statements are stored
    :param data_dir: subdirectory with data to be loaded
    :param update_dir: subdirectory with data to be updated
    :param delete_dir: subdirectory with data to be deleted
    :param generated_query_dir: subdirectory with generated queries
    :param results_dir: path to the results folder
    :param host: hostname where the Postgres database is running
    :param port: port number where the Postgres database is listening
    :param database: database name, where the benchmark will be run
    :param user: username of the Postgres user with full access to the benchmark DB
    :param password: password for the Postgres user
    :param run_timestamp: name of the run folder, format run_YYYYMMDD_HHMMSS
    :param num_streams: number of streams
    :param verbose: True if more verbose output is required
    :param read_only: True if no inserts/updates/deletes are to be run; can be used to run the same test multiple times
    without (re)loading the data, e.g. while developing
    :return: 0 if successful, 1 otherwise
    """
    try:
        print("Power tests started ...")
        conn = pgdb.PGDB(host, port, database, user, password)
        result = r.Result("Power")
        result.startTimer()
        stream = 0 # constant for power tests
        #
        if not read_only:
            if refresh_func1(conn, data_dir, update_dir, stream, num_streams, verbose):
                return 1
        result.setMetric(REFRESH_METRIC % (stream, 1), result.stopTimer())
        #
        if run_query_stream(conn, query_root, generated_query_dir, stream, num_streams, result, verbose):
            return 1
        #
        result.startTimer()
        if not read_only:
            if refresh_func2(conn, data_dir, delete_dir, stream, num_streams, verbose):
                return 1
        result.setMetric(REFRESH_METRIC % (stream, 2), result.stopTimer())
        #
        print("Power tests finished.")
        if verbose:
            result.printMetrics()
        result.saveMetrics(results_dir, run_timestamp, "power")
    except Exception as e:
        print("unable to run power tests. DB connection failed: %s" % e)
        return 1
    return 0


def run_throughput_inner(query_root, data_dir, generated_query_dir,
                         host, port, database, user, password,
                         stream, num_streams, queue, verbose):
    """

    :param query_root:
    :param data_dir: subdirectory with data to be loaded
    :param generated_query_dir: subdirectory with generated queries
    :param host: hostname where the Postgres database is running
    :param port: port number where the Postgres database is listening
    :param database: database name, where the benchmark will be run
    :param user: username of the Postgres user with full access to the benchmark DB
    :param password: password for the Postgres user
    :param stream: stream number
    :param num_streams: number of streams
    :param queue: process queue
    :param verbose: True if more verbose output is required
    :return: none, uses exit(1) to abort on errors
    """
    try:
        conn = pgdb.PGDB(host, port, database, user, password)
        result = r.Result("ThroughputQueryStream%s" % stream)
        if run_query_stream(conn, query_root, generated_query_dir, stream, num_streams, result, verbose):
            print("unable to finish query in stream #%s" % stream)
            exit(1)
        queue.put(result)
    except Exception as e:
        print("unable to connect to DB for query in stream #%s: %s" % (stream, e))
        exit(1)


def run_throughput_test(query_root, data_dir, update_dir, delete_dir, generated_query_dir, results_dir,
                        host, port, database, user, password,
                        run_timestamp, num_streams, verbose, read_only):
    """

    :param query_root:
    :param data_dir: subdirectory with data to be loaded
    :param update_dir: subdirectory with data to be updated
    :param delete_dir: subdirectory with data to be deleted
    :param generated_query_dir: subdirectory with generated queries
    :param results_dir: path to the results folder
    :param host: hostname where the Postgres database is running
    :param port: port number where the Postgres database is listening
    :param database: database name, where the benchmark will be run
    :param user: username of the Postgres user with full access to the benchmark DB
    :param password: password for the Postgres user
    :param run_timestamp: name of the run folder, format run_YYYYMMDD_HHMMSS
    :param num_streams: number of streams
    :param verbose: True if more verbose output is required
    :param read_only: True if no inserts/updates/deletes are to be run; can be used to run the same test multiple times
    without (re)loading the data, e.g. while developing
    :return: 0 if successful, 1 otherwise
    """
    try:
        print("Throughput tests started ...")
        conn = pgdb.PGDB(host, port, database, user, password)
        total = r.Result("ThroughputTotal")
        total.startTimer()
        processes = []
        queue = Queue()
        for i in range(num_streams):
            stream = i + 1
            # queries
            print("Throughput tests in stream #%s started ..." % stream)
            p = Process(target=run_throughput_inner,
                        args=(query_root, data_dir, generated_query_dir,
                              host, port, database, user, password,
                              stream, num_streams, queue, verbose))
            processes.append(p)
            p.start()
        result = r.Result("ThroughputRefreshStream")
        for i in range(num_streams):
            stream = i + 1
            # refresh functions
            result.startTimer()
            if not read_only:
                if refresh_func1(conn, data_dir, update_dir, stream, num_streams, verbose):
                    return 1
            result.setMetric(REFRESH_METRIC % (stream, 1), result.stopTimer())
            #
            result.startTimer()
            if not read_only:
                if refresh_func2(conn, data_dir, delete_dir, stream, num_streams, verbose):
                    return 1
            result.setMetric(REFRESH_METRIC % (stream, 2), result.stopTimer())
            #
        queue.put(result)
        for p in processes:
            p.join()
        print("Throughput tests finished.")
        for i in range(queue.qsize()):
            res = queue.get(False)
            if verbose:
                res.printMetrics()
            res.saveMetrics(results_dir, run_timestamp, THROUGHPUT)
        #
        total.setMetric(THROUGHPUT_TOTAL_METRIC, total.stopTimer())
        if verbose:
            total.printMetrics()
        total.saveMetrics(results_dir, run_timestamp, THROUGHPUT)
        #
    except Exception as e:
        print("unable to execute throughput tests: %s" % e)
        return 1
    return 0


def get_json_files_from(path):
    """Get list of all JSON file names in path

    :param path: path to a folder
    :return: list of all JSON files, identified by file extension .json, not by content
    """
    json_files = [pos_json for pos_json in os.listdir(path) if pos_json.endswith('.json')]
    json_files = [os.path.join(path, s) for s in json_files]
    return json_files


def get_json_files(path):
    """Gather list of all JSON files in path, incl. subfolders
    It is expected, that the folder structure is as follows
    - path
      - run_YYYYMMDD_HHMMSS
        - power
          - ... JSON files ...
        - throughput
          - ... JSON files ...

    :param path: path to be scanned (only "power" and "throughput" subfolders will be considered on level 2)
    :return: list of JSON file names from all subfolders with expected folder structure
    """
    json_files = []
    for run_timestamp in os.listdir(os.path.join(path)):
        for mode in [POWER, THROUGHPUT]:
            sub_dir = os.path.join(path, run_timestamp, mode)
            if os.path.exists(sub_dir) and os.path.isdir(sub_dir):
                json_files += get_json_files_from(sub_dir)
    return json_files


def load_results(results_dir):
    """Load all results into a list

    :param results_dir: path to results directory
    :return: list of dictionary pairs with metric name as key and value as value
    """
    results = []
    for json_filename in get_json_files(results_dir):
        with open(json_filename, 'r') as json_file:
            raw = json_file.read()
            js = json.loads(raw)
            for key, value in js.items():
                results.append({"key": key, "value": value})
    return results


def get_timedelta_in_seconds(time_interval):
    """Convert time delta as string into numeric value in seconds

    :param time_interval: time interval as string in format HH:MM:SS.FFFFFF
    :return: time interval in seconds
    """
    if ":" not in time_interval:
        return 0
    (hours, minutes, sf) = time_interval.split(":")
    (seconds, fraction) = sf.split(".") if "." in sf else (0, 0)
    secs = int(hours) * 60 * 60 + \
           int(minutes) * 60 + \
           int(seconds) + \
           int(fraction) / 1000000
    return secs


def get_average(results, metric_name):
    """Calculate average value for the metric

    :param results: list of results
    :param metric_name: metric name
    :return: average value for value from results with specified metric name
    """
    values = [js["value"] for js in results if js["key"] == metric_name]
    seconds = [get_timedelta_in_seconds(value) for value in values]
    avg = sum(seconds) / len(values)
    return avg


def qi(results, i, s):
    """Calculate execution time for query Qi within the query stream s

    :param results: list of results
    :param i: the ordering number of the query ranging from 1 to 22
    :param s: either 0 for the power function or the position of the query stream for the throughput tests
    :return: execution time for query Qi within the query stream s
    """
    assert(1 <= i <= 22)
    assert(0 <= s)
    metric_name = QUERY_METRIC % (s, i)
    ret = get_average(results, metric_name)
    return ret


def ri(results, j, s):
    """Calculate execution time for the refresh function RFi within a refresh stream s

    :param results: list of results
    :param j: ordering function of the refresh function ranging from 1 to 2
    :param s: either 0 for the power function
    or the position of the pair of refresh functions in the stream for the throughput tests
    :return: execution time for the refresh function RFi within a refresh stream s
    """
    assert(j == 1 or j == 2)
    assert(0 <= s)
    metric_name = REFRESH_METRIC % (s, j)
    ret = get_average(results, metric_name)
    return ret


def ts(results):
    """Calculate average total time needed to execute the throughput tests

    :param results: list of results
    :return: total time needed to execute the throughput tests
    """
    metric_name = THROUGHPUT_TOTAL_METRIC
    ret = get_average(results, metric_name)
    return ret


def get_power_size(results, scale_factor):
    """Calculate the Power@Size

    :param results: list of results
    :param scale_factor: scale factor
    :return: Power@Size
    """
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
    """Calculate the Troughput@Size

    :param results: list of results
    :param scale_factor: scale factor
    :param num_streams: number of streams
    :return: Troughput@Size
    """
    throughput_size = ((num_streams * NUM_QUERIES) / ts(results)) * 3600 * scale_factor
    return throughput_size


def get_qphh_size(power_size, throughput_size):
    """Calculate QphH@Size

    :param power_size: Power@Size
    :param throughput_size: Throughput@Size
    :return: QphH@Size
    """
    qphh_size = math.sqrt(power_size * throughput_size)
    return qphh_size


def calc_metrics(results_dir, run_timestamp, scale_factor, num_streams):
    """Calculate metrics and save them in an output JSON file

    :param results_dir: path to the results folder
    :param run_timestamp: name of the run folder, format run_YYYYMMDD_HHMMSS
    :param scale_factor: scale factor
    :param num_streams: number of streams
    :return: none
    """
    results = load_results(results_dir)
    res = r.Result("Metric")
    #
    power_size = get_power_size(results, scale_factor)
    res.setMetric("power_size", power_size)
    print("Power@Size = %s" % power_size)
    #
    throughput_size = get_throughput_size(results, scale_factor, num_streams)
    res.setMetric("throughput_size", throughput_size)
    print("Throughput@Size = %s" % throughput_size)
    #
    qphh_size = get_qphh_size(power_size, throughput_size)
    res.setMetric("qphh_size", qphh_size)
    print("QphH@Size = %s" % qphh_size)
    #
    res.printMetrics("Metrics")
    res.saveMetrics(results_dir, run_timestamp, "metrics")
