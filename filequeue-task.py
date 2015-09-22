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

def getCommonsUrl(filename):
    """
    returns the url for a given file on commons.
    """
    base= 'http://upload.wikimedia.org/wikipedia/commons/'
    hash= hashlib.md5(filename).hexdigest()
    path= '%s/%s/' % (hash[0], hash[0:2])
    return base + path + urllib.quote(filename)

def commonsfiles(start=''):
    """
    generator which yields a dict for each file to download:
    { img_name,img_timestamp,img_size,url,resume }
    files are ordered by the sha1 hash of their content
    todo: later, this should support ordering by modification timestamp
    """
    def mkconn():
        conn= MySQLdb.connect( read_default_file=os.path.expanduser('~/replica.my.cnf'), host='commonswiki.labsdb', use_unicode=True, cursorclass=MySQLdb.cursors.DictCursor )
        cursor= conn.cursor()
        return conn,cursor
    conn,cursor= mkconn()
    cursor.execute('USE commonswiki_p')
    chunksize= 50
    sha1= start    #hashlib.sha1(str(time.time())).hexdigest()
    query= 'SELECT img_name,img_timestamp,img_sha1,img_size FROM image WHERE img_sha1 > %s ORDER BY img_sha1 LIMIT ' + str(chunksize)
    while True:
        try:
            conn.ping()
        except MySQLdb.OperationalError as ex:
            if ex[0]==2006: # mysql server has gone away
                conn,cursor= mkconn()
            else:
                raise
        cursor.execute(query, sha1)
        result= cursor.fetchall()
        for col in result:
            sha1= col['img_sha1']
            col['url']= getCommonsUrl(col['img_name'])
            col['resume']= sha1
            yield col



def makeorderqentry(jobid, resume):
    return { "jobid": jobid, "resume": resume }
    
def makedownloadqentry(config, jobid, name,url,timestamp,size):
    return { "jobid": jobid, "name": name, "url": url, "changed": timestamp, "filesize": size }

if __name__=='__main__':
    config= json.load(open("config.json"))
    joborderq= RedisQueue(host=config["redis-host"], namespace=config["redis-namespace"], name=config["redis-job-order-queue-name"])
    downloadq= RedisQueue(host=config["redis-host"], namespace=config["redis-namespace"], name=config["redis-download-queue-name"])
    
    joborderq.clear()
    downloadq.clear()
    
    # xxxx remove stale files?

    jobid= 1
    for row in commonsfiles():
        # wait for queue to shrink
        # xxx todo: it would be nicer to have a blocking version of this, instead of polling every second
        while joborderq.qsize()>=config["redis-max-queued-jobs"]:
            time.sleep(1)
        joborderq.put(json.dumps(makeorderqentry(jobid, row['resume'])))
        downloadq.put(json.dumps(makedownloadqentry(config, jobid, row["img_name"], row["url"], row["img_timestamp"], row["img_size"])))
        print("pushed job %d..." % jobid)
        jobid+= 1
    
    