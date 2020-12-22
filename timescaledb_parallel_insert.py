import psycopg2
import subprocess
import os
import glob
import multiprocessing
import time
import sys
import re

CONNECTION = sys.argv[1]
DIR = sys.argv[2]
os.chdir(DIR)
folder = DIR.replace("\\",str('/'))
arr = re.split('[://:@/?]+',CONNECTION)
dbuser = arr[1]
dbpwd = arr[2]
dbhost = arr[3]
dbport = arr[4]
db = arr[5]
dbmode = arr[6]

def go_timescaledb_parallel(file):
    print(file)
    timescaledb_parallel_copy_query = 'timescaledb-parallel-copy --connection ' + sys.argv[1] + ' --db-name stocks --table stockdata --file ' + folder + file + ' --copy-options \"CSV\" --skip-header'
    subprocess.run(timescaledb_parallel_copy_query, capture_output=True)

def create_database():
    conn = psycopg2.connect(dbname=db, user=dbuser, password=dbpwd, host=dbhost, port=dbport)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'stocks'")
    exists = cur.fetchone()
    if exists: return
    cur.execute("CREATE DATABASE stocks")
    conn.commit()
    
def add_timescaledb_extention():
    conn = psycopg2.connect(dbname='stocks', user=dbuser, password=dbpwd, host=dbhost, port=dbport)
    cur = conn.cursor()
    query_add_timescaledb_ext = "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
    cur.execute(query_add_timescaledb_ext)
    conn.commit()
    
def create_table():
    conn = psycopg2.connect(dbname='stocks', user=dbuser, password=dbpwd, host=dbhost, port=dbport)
    cur = conn.cursor()
    query_create_stockdata_table = "CREATE TABLE IF NOT EXISTS stockdata ( \
                                    Date timestamp, \
                                    Ticker VARCHAR(10), \
                                    Open numeric(14,2) NULL, \
                                    High numeric(14,2) NULL, \
                                    Low numeric(14,2) NULL, \
                                    Close numeric(14,2) NULL, \
                                    Volume numeric(14,1) NULL \
                                    );" 
    cur.execute(query_create_stockdata_table)
    conn.commit()
    query_create_hypertable = "SELECT create_hypertable ('stockdata', 'date', create_default_indexes => FALSE, if_not_exists => TRUE);"
    cur.execute(query_create_hypertable)
    conn.commit()

if __name__ == "__main__":
    create_database()
    add_timescaledb_extention()
    create_table()
    pool = multiprocessing.Pool()
    start_time = time.time()    
    with pool as p:
        p.map(go_timescaledb_parallel, glob.glob("*.csv"))
    print(time.time()-start_time)    
