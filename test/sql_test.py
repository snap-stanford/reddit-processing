#!/usr/bin/env python
"""
File: sql_test
Date: 3/25/18 
Author: Jon Deaton (jdeaton@stanford.edu)
"""

import sqlite3 as sql
from this import s as nonsense
import subprocess
import shlex
import time

max_parallel_processes = 10

def getdb(tableid = "test"):
    dbid = ":memory:"

    stmt_create = "CREATE TABLE %s (id int, comment text)" % tableid
    stmt_insert = "INSERT INTO %s VALUES (?, ?)" % tableid

    values = enumerate(nonsense.split())

    db = sql.connect(dbid)
    db.execute(stmt_create)
    db.executemany(stmt_insert, values)
    return db


def get_ids(db, tableid = "test"):
    stmt_select_id = "SELECT id from %s " % tableid
    crs = db.execute(stmt_select_id)
    result = crs.fetchall()
    for i in result:
        yield i


def main():
    from random import randint

    db = getdb()
    process_lst = {}
    sleep_between_polls_in_seconds = 0.1

    for rowid in get_ids(db):
        if len(process_lst) < max_parallel_processes:
            cmd_str = "sleep %s"  % randint(1, 3)
            cmd = shlex.split(cmd_str)

            print "adding : %s (%s)" % (rowid, cmd_str)

            proc = subprocess.Popen(cmd)
            process_lst[proc] = rowid
            proc.poll()
        else:
            print "max processes (%s) reached" % max_parallel_processes

            for proc in process_lst.keys():
                finished = proc.poll() is not None
                if finished:
                    print "%s finished" % process_lst[proc]
                    del process_lst[proc]

                time.sleep(sleep_between_polls_in_seconds)

    print "All processes processed: %s "  %(len (process_lst) == 0)

if __name__ == "__main__":
    main()