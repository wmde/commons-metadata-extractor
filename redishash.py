#!/usr/bin/python
# -*- coding:utf-8 -*-
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
