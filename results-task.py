#!/usr/bin/python
# -*- coding:utf-8 -*-
import os, sys
import time
import json
import redis
import urllib
import hashlib
import MySQLdb, MySQLdb.cursors 
from redisqueue import RedisQueue
from redishash import RedisHash

def sqlconn(config):
    conn= MySQLdb.connect( read_default_file=os.path.expanduser('~/replica.my.cnf'), host='tools.labsdb', use_unicode=True, cursorclass=MySQLdb.cursors.DictCursor )
    cursor= conn.cursor()
    return conn,cursor

def writeresumefile(config, job):
    dir= config["download-dir"]
    tmpfile= os.path.join(dir, "resume.tmp")
    resumefile= os.path.join(dir, "resume.json")
    with open(tmpfile, "w") as f:
        f.write(json.dumps(job))
    os.rename(tmpfile, resumefile)

if __name__=='__main__':
    config= json.load(open("config.json"))
    joborderq= RedisQueue(host=config["redis-host"], namespace=config["redis-namespace"], name=config["redis-job-order-queue"])
    resulthash= RedisHash(host=config["redis-host"], namespace=config["redis-namespace"], name=config["redis-result-hash"])
    conn,cursor= sqlconn(config)
    
    while True:
        nextjob= json.loads(joborderq.get())
        while not nextjob["jobid"] in resulthash:
            time.sleep(1.0)
        result= resulthash[nextjob["jobid"]]
        print("got result: %s" % json.dumps(result))
        # xxx write result to db
        writeresumefile(config, nextjob)
        