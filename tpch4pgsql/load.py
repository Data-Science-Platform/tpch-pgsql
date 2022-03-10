import os
from tpch4pgsql import postgresqldb as pgdb


def clean_database(query_root, host, port, db_name, user, password, tables):
    """Drops the tables if they exist

    Args:
        query_root (str): Directory in which generated queries directory exists
        host (str): IP/hostname of the PG instance
        port (int): port for the PG instance
        db_name (str): name of the tpch database
        user (str): user for the PG instance
        password (str): password for the PG instance
        tables (str): list of tables

    Return:
        0 if successful
        non zero otherwise
    """
    try:
        conn = pgdb.PGDB(host, port, db_name, user, password)
        try:
            for table in tables:
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


def create_schema(query_root, host, port, db_name, user, password, prep_query_dir):
    """Creates the schema for the tests. Drops the tables if they exist

    Args:
        query_root (str): Directory in which generated queries directory exists
        host (str): IP/hostname of the PG instance
        port (int): port for the PG instance
        db_name (str): name of the tpch database
        user (str): user for the PG instance
        password (str): password for the PG instance
        prep_query_dir (str): directory with queries for schema creation

    Return:
        0 if successful
        non zero otherwise
    """
    try:
        conn = pgdb.PGDB(host, port, db_name, user, password)
        try:
            conn.executeQueryFromFile(os.path.join(query_root, prep_query_dir, "create_tbl.sql"))
        except Exception as e:
            print("unable to run create tables. %s" % e)
            return 1
        conn.commit()
        conn.close()
    except Exception as e:
        print("unable to connect to the database. %s" % e)
        return 1


def load_tables(data_dir, host, port, db_name, user, password, tables, load_dir):
    """Loads data into tables. Expects that tables are already empty.

    Args:
        data_dir (str): Directory in which load data exists
        host (str): IP/hostname of the PG instance
        port (int): port for the PG instance
        db_name (str): name of the tpch database
        user (str): user for the PG instance
        password (str): password for the PG instance
        tables (str): list of tables
        load_dir (str): directory with data files to be loaded

    Return:
        0 if successful
        non zero otherwise
    """
    try:
        conn = pgdb.PGDB(host, port, db_name, user, password)
        try:
            for table in tables:
                filepath = os.path.join(data_dir, load_dir, table.lower() + ".tbl.csv")
                conn.copyFrom(filepath, separator="|", table=table.lower())
            conn.commit()
        except Exception as e:
            print("unable to run load tables. %s" %e)
            return 1
        conn.close()
        return 0
    except Exception as e:
        print("unable to connect to the database. %s" % e)
        return 1


def index_tables(query_root, host, port, db_name, user, password, prep_query_dir):
    """Creates indexes and foreign keys for loaded tables.

    Args:
        query_root (str): Directory in which preparation queries directory exists
        host (str): IP/hostname of the PG instance
        port (int): port for the PG instance
        db_name (str): name of the tpch database
        user (str): user for the PG instance
        password (str): password for the PG instance
        prep_query_dir (str): directory with create index script

    Return:
        0 if successful
        non zero otherwise
    """
    try:
        conn = pgdb.PGDB(host, port, db_name, user, password)
        try:
            conn.executeQueryFromFile(os.path.join(query_root, prep_query_dir, "create_idx.sql"))
            conn.commit()
        except Exception as e:
            print("unable to run index tables. %s" % e)
            return 1
        conn.close()
        return 0
    except Exception as e:
        print("unable to connect to the database. %s" % e)
        return 1
