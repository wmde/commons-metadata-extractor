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
    