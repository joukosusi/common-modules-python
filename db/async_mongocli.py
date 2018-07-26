# encoding=utf-8
"""
``motor_mongocli``  提供tornado一起使用的异步的客户端
    AsynMongoHandler： 异步mongo客户端，生成可以操作数据库的句柄
"""
from __future__ import print_function

__author__ = 'qxue'

import tornado.gen
import motor


class AsynMongoHandler(object):
    def __init__(self, host, port, db_name, user, pwd, replicaset=None):
        """
        :param host: ip地址
        :param port:  端口
        :param db_name: 数据库名
        :param user:   用户名
        :param pwd:    密码
        :param replicaset: 副本集
        :return:
        """
        # db
        self._db = None
        self._mc = None

        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.pwd = pwd
        self.replicaset = replicaset

    @tornado.gen.coroutine
    def reset_db(self):
        """
            重连mongo
        """
        try:
            if self._db:
                yield self._db.logout()
            if self.replicaset is None:
                self._mc = motor.MotorClient(self.host, self.port)
            else:
                self._mc = motor.MotorClient(self.host, self.port, replicaset=self.replicaset)
            self._db = self._mc[self.db_name]
            ret = yield self._db.authenticate(self.user, self.pwd)
            if not ret:
                raise Exception('authenticate error')
        except (Exception,) as e:
            print(e)
            self._db = None
            self._mc = None
            raise tornado.gen.Return(False)
        else:
            raise tornado.gen.Return(True)

    @tornado.gen.coroutine
    def alive_db(self):
        if self._mc:
            raise tornado.gen.Return((yield self._mc.alive()))
        else:
            raise tornado.gen.Return(False)

    @tornado.gen.coroutine
    def check_db_alive(self, title=None):
        title_name = title + ' ' if title else ''
        if not (yield self.alive_db()):
            raise Exception(title_name+'cannot connect to db')

    @tornado.gen.coroutine
    def insert(self, collection, data):
        yield self._db[collection].insert(data)

    @tornado.gen.coroutine
    def remove(self, collection, condition):
        yield self._db[collection].remove(condition)

    @tornado.gen.coroutine
    def update(self, collection, condition, operation, upsert=True):
        yield self._db[collection].update(condition,operation, upsert=upsert)

    @tornado.gen.coroutine
    def query_one(self, collection, condition):
        res = yield self._db[collection].find_one(condition)
        raise tornado.gen.Return(res)

    @tornado.gen.coroutine
    def find(self, collection, condition):
        res = yield self._db[collection].find(condition)
        raise tornado.gen.Return(res)