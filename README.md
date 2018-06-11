# tpch-pgsql
Implements the [TPCH benchmark](http://www.tpc.org/tpch/) for Postgres

### Requirements
* The benchmark requires TPC-H dbgen:

```
wget https://github.com/electrum/tpch-dbgen/archive/32f1c1b92d1664dba542e927d23d86ffa57aa253.zip
unzip 32f1c1b92d1664dba542e927d23d86ffa57aa253.zip
mv tpch-dbgen-32f1c1b92d1664dba542e927d23d86ffa57aa253 tpch-dbgen
rm 32f1c1b92d1664dba542e927d23d86ffa57aa253.zip
```

* gcc
* python3
* some running instance of Postgres


### Usage
There is a single python file that implements all phases of the benchmark.

```
usage: benchmark.py [-h] [-a HOST] [-p PORT] [-u USER] [-v [PASSWORD]]
                    [-d DATABASE] [-i DATA_DIR] [-q QUERY_ROOT] [-g DBGEN_DIR]
                    [-s SCALE] [-n NUM_STREAMS]
                    {prepare,load,query}

PGTPCH

positional arguments:
  {prepare,load,query}  Phase of PGTPCH to run.

optional arguments:
  -h, --help            show this help message and exit
  -a HOST, --host HOST  Address of host on which PG instance runs
  -p PORT, --port PORT  Port on which PG instance runs
  -u USER, --user USER  User for the PG instance
  -v [PASSWORD], --password [PASSWORD]
                        Password for the PG instance
  -d DATABASE, --database DATABASE
                        Name of the database
  -i DATA_DIR, --data-dir DATA_DIR
                        Directory for generated data
  -q QUERY_ROOT, --query-root QUERY_ROOT
                        Directory for query files
  -g DBGEN_DIR, --dbgen-dir DBGEN_DIR
                        Directory containing tpch dbgen source
  -s SCALE, --scale SCALE
                        Size of the data generated. 1.0 = 1GB
  -n NUM_STREAMS, --num-streams NUM_STREAMS
                        Number of streams to run the throughput test with.
```

### Phases
* The prepare phase builds TPC-H dbgen and querygen and creates the load and update files. 
* The load phase cleans the database (if required), loads the tables into the database and 
creates indexes for querying. The results for this phase consist of the following metrics:
    * Schema creation time
    * Data loading time
    * Foreign key constraint and index creation time
* The query phase is the actual performance test. It consists of two parts:
    * Power test: This consists of sequential execution of the refresh functions and the query streams. It reports back with the execution times for:
        * refresh function 1
        * query execution time for the 22 TPCH queries
        * refresh function 2
    * Throughput test: This consists of parallel execution of the query streams and the pairs of refresh functions (*not implemented yet*)

### TPCH Process
The complete process for executing TPCH tests is illustrated in the following figure:
![tpch-process](images/tpch_process.png "TPCH Benchmark Process")

### Database Schema
![db-schema](images/TPC-H_Datamodel.png "TPCH Database Schema")

### Known Issues
* Sometimes the data generation phase fails due to file permission issues. In such a scenario delete the data directory and all generated `.tbl` files inside your `tpch-dbgen` directory.

### References

* For notes on how to the TPCH-Benchmark works see the paper [iceis2012](https://github.com/Data-Science-Platform/tpch-pgsql/blob/master/iceis2012.pdf).
* For the TPC-H benchmark specification see [this document](http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.3.pdf).
