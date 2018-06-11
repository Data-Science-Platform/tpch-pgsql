# tpch-pgsql
Implements the (TPCH benchmark)[http://www.tpc.org/tpch/] for Postgres

### Requirements
* The benchmark requires TPC-H dbgen:
`git clone https://github.com/electrum/tpch-dbgen`

* gcc
* python3
* some running instance of PostGres


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
                        Enter the password for the PG instance
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
* The prepare phase builds TPC-H dbgen, creates load and update files and creates the schema in the database specified. 
* The load phase loads the tables into the database and creates indexes for querying.
* The query phase executes queries and measures their times (not implemented right now)

### References

For notes on how to the TPCH-Benchmark works see the paper "iceis2012"
