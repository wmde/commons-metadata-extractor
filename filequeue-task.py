#!/usr/bin/python
# -*- coding:utf-8 -*-
# Commons Metadata Extractor
# Copyright (C) 2015 Wikimedia Deutschland e.V.
# Authors: Johannes Kroll
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

def commonsfiles(start='', sortkey='img_sha1', limit=None):
    """
    generator which yields a dict for each file to download:
    { img_name,img_timestamp,img_size,url,resume }
    files are ordered by the sha1 hash of their content by default
    """
    def mkconn():
        conn= MySQLdb.connect( read_default_file=os.path.expanduser('~/replica.my.cnf'), host='commonswiki.labsdb', use_unicode=True, cursorclass=MySQLdb.cursors.DictCursor )
        cursor= conn.cursor()
        return conn,cursor
    conn,cursor= mkconn()
    cursor.execute('USE commonswiki_p')
    chunksize= 50
    resume= start    #hashlib.sha1(str(time.time())).hexdigest()
    query= 'SELECT img_name,img_timestamp,img_sha1,img_size FROM image WHERE ' + sortkey + ' > %s ORDER BY ' + sortkey + ' LIMIT ' + str(chunksize)
    count= 0
    while True:
        try:
            conn.ping()
        except MySQLdb.OperationalError as ex:
            if ex[0]==2006: # mysql server has gone away
                conn,cursor= mkconn()
            else:
                raise
        cursor.execute(query, resume)
        result= cursor.fetchall()
        for col in result:
            resume= col[sortkey]
            col['resume']= resume
            col['url']= getCommonsUrl(col['img_name'])
            yield col
            count+= 1
            if limit and count>=limit: 
                return

def makeorderqentry(jobid, resume):
    return { "jobid": jobid, "resume": resume }
    
def makedownloadqentry(config, jobid, name,url,timestamp,size):
    return { "jobid": jobid, "name": name, "url": url, "changed": timestamp, "filesize": size }

if __name__=='__main__':
    config= json.load(open("config.json"))
    joborderq= RedisQueue(host=config["redis-host"], namespace=config["redis-namespace"], name=config["redis-job-order-queue"])
    downloadq= RedisQueue(host=config["redis-host"], namespace=config["redis-namespace"], name=config["redis-download-queue"])
    
    joborderq.clear()
    downloadq.clear()
    
    # xxxx remove stale files?

    jobid= 1
    for row in commonsfiles(sortkey='img_sha1', limit=50):
        # wait for queue to shrink
        # xxx todo: it would be nicer to have a blocking version of this, instead of polling every second
        while joborderq.qsize()>=config["redis-max-queued-jobs"]:
            time.sleep(1)
        joborderq.put(json.dumps(makeorderqentry(jobid, row['resume'])))
        downloadq.put(json.dumps(makedownloadqentry(config, jobid, row["img_name"], row["url"], row["img_timestamp"], row["img_size"])))
        print("pushed job %d (%s)..." % (jobid, row['img_name']))
        jobid+= 1
    
    print("filequeue task done, exiting.")
