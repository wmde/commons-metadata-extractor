#!/usr/bin/python
# -*- coding:utf-8 -*-
import os, sys
import time
import json
import redis
from redisqueue import RedisQueue
from redishash import RedisHash

def process(job):
    if job["action"]=="process_file":
        # { "jobid", "path", "name", "changed", "filesize" }
        # ... do stuff ...
        os.remove(job["path"])
        # return fake entry
        return { "status": "OK", "metadata": { "foo": 47 } }
    else:
        raise runtime_error("unknown job action '%s'" % job["action"])

if __name__=='__main__':
    config= json.load(open("config.json"))
    processq= RedisQueue(host=config["redis-host"], namespace=config["redis-namespace"], name=config["redis-process-queue"])
    resulthash= RedisHash(host=config["redis-host"], namespace=config["redis-namespace"], name=config["redis-result-hash"])
    resulthash.clear()

    while True:
        job= json.loads(processq.get())
        print("processing job %s..." % job["jobid"])
        result= process(job)
        resulthash[job["jobid"]]= result
    