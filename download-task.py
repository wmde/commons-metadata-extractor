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
import random
import requests
import urllib
import hashlib
import MySQLdb, MySQLdb.cursors 
from redisqueue import RedisQueue

def makedatadir(config):
    try:
        os.makedirs(config["download-dir"])
    except OSError as ex:
        if ex[0]==17:   # directory already exists
            pass
        else:
            raise

def makeprocessqentry(config, jobid, path, imgname, timestamp, size):
    return { "action": "process_file", "jobid": jobid, "path": path, "name": imgname, "changed": timestamp, "filesize": size }

if __name__=='__main__':
    config= json.load(open("config.json"))
    makedatadir(config)
    downloadq= RedisQueue(host=config["redis-host"], namespace=config["redis-namespace"], name=config["redis-download-queue"])
    processq= RedisQueue(host=config["redis-host"], namespace=config["redis-namespace"], name=config["redis-process-queue"])
    session= requests.Session()
    
    processq.clear()

    while True:
        row= json.loads(downloadq.get())
        print "%s => " % row['name'].encode('utf-8'),
        r= session.get(row['url'])
        print(r.status_code)
        if r.status_code!=200:
            raise RuntimeError("requests.get(%s) returned %s" % (row['url'], r.status_code))
        outputpath= os.path.join(os.path.expanduser(config["download-dir"]), row["name"])
        with open(outputpath, "w") as f:
            f.write(r.content)
        processq.put(json.dumps(makeprocessqentry(config, row["jobid"], outputpath, row["name"], row["changed"], row["filesize"])))
    
    
    