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
import redis

class RedisHash(dict):
    def __init__(self, name, namespace='hash', **redis_kwargs):
        """The default connection parameters are: host='localhost', port=6379, db=0"""
        self.__db= redis.StrictRedis(**redis_kwargs)
        self.key = '%s:%s' %(namespace, name)

    def __getitem__(self, key):
        return self.__db.hget(self.key, key)
    
    #~ def __missing__(self, key):
    
    def __setitem__(self, key, value):
        self.__db.hset(self.key, key, value)
    
    def __contains__(self, item):
        return self.__db.hexists(self.key, item)
    
    def clear(self):
        return self.__db.delete(self.key)

if __name__=='__main__':
    pass
