import os
import glob
import re
import subprocess


def build_dbgen(dbgen_dir):
    """Compiles the dbgen from source.

    The Makefile must be present in the same directory as this script.

    Args:
        dbgen_dir (str): Directory in which the source code is placed.

    Return:
        0 if successful
        non zero otherwise
    """
    cur_dir = os.getcwd()
    p = subprocess.Popen(["make", "-f", os.path.join(cur_dir, "Makefile")], cwd=dbgen_dir)
    p.communicate()
    return p.returncode


def inner_generate_data(data_dir, dbgen_dir, file_pattern, out_ext):
    """Generate data for load/update/delete operations on the tables.

    This function is used by different stages of function generate_data(): load / update / delete

    Args:
        data_dir (str): Root directory for storing generated data and scripts.
        dbgen_dir (str): Directory in which the source code is placed.
        file_pattern (str): file pattern
        out_ext (str): output file extension

    Return:
        0 if successful
        non zero otherwise
    """
    try:
        os.makedirs(data_dir, exist_ok=True)
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
    # All files written successfully. Return success code.
    return 0


def generate_data(dbgen_dir, data_dir, load_dir, update_dir, delete_dir, scale, num_streams):
    """Generates data for the loading into tables.

    Args:
        dbgen_dir (str): Directory in which the source code is to be placed.
        data_dir (str): Directory where generated data is to be placed.
        load_dir (str): Subdirectory where data to be loaded is to be placed.
        update_dir (str): Subdirectory where scripts with data update operations is to be placed.
        delete_dir (str): Subdirectory where scripts with data delete operations is to be placed.
        scale (float): Amount of data to be generated. 1 = 1GB.
        num_streams (int): Number of streams on which the throuput tests is going to be performed.

    Return:
        0 if successful
        non zero otherwise
    """
    p = subprocess.Popen([os.path.join(".", "dbgen"), "-vf", "-s", str(scale)], cwd=dbgen_dir)
    p.communicate()
    if not p.returncode:
        load_path = os.path.join(data_dir, load_dir)
        if inner_generate_data(load_path, dbgen_dir, "*.tbl", ".csv"):
            print("unable to generate data for load phase")
            return 1
        print("generated data for the load phase")
    else:
        return p.returncode

    # Update/Delete phase data
    # we generate num_streams + 1 number of updates because 1 is used by the power tests
    p = subprocess.Popen([os.path.join(".", "dbgen"), "-vf", "-s", str(scale), "-U", str(num_streams + 1)],
                         cwd=dbgen_dir)
    p.communicate()
    if not p.returncode:
        update_path = os.path.join(data_dir, update_dir)
        delete_path = os.path.join(data_dir, delete_dir)
        if inner_generate_data(update_path, dbgen_dir, "*.tbl.u*", ".csv"):
            print("unable to generate data for the update phase")
            return 1
        print("generated data for the update phase")
        if inner_generate_data(delete_path, dbgen_dir, "delete.*", ".csv"):
            print("unable to generate data for the delete phase")
            return 1
        print("generated data for the delete phase")
        # All files written successfully. Return success code.
        return 0
    else:
        return p.returncode


def generate_queries(dbgen_dir, query_root, template_query_dir, generated_query_dir):
    """Generates queries for performance tests.

    Args:
        dbgen_dir (str): Directory in which the source code is placed.
        query_root (str): Directory in which query templates directory exists.
                          Also the place where the generated queries are going to be placed.
        template_query_dir (str): Subdirectory where template SQL queries are to be placed.
        generated_query_dir (str): Subdirectory where generated SQL queries are to be placed.

    Return:
        0 if successful
        non zero otherwise
    """
    query_root = os.path.abspath(query_root)
    dss_query_path = os.path.join(query_root, template_query_dir)
    query_env = os.environ.copy()
    query_env['DSS_QUERY'] = dss_query_path
    query_gen_path = os.path.join(query_root, generated_query_dir)
    os.makedirs(query_gen_path, exist_ok=True)
    for i in range(1, 23):
        try:
            with open(os.path.join(query_gen_path, str(i) + ".sql"), "w") as out_file:
                p = subprocess.Popen([os.path.join(".", "qgen"), str(i)],
                                     cwd=dbgen_dir, env=query_env, stdout=out_file)
                p.communicate()
                if p.returncode:
                    print("Process returned non zero when generating query number %s" % i)
                    return p.returncode
        except IOError as e:
            print("IO Error during query generation %s" % e)
            return 1
    return p.returncode

