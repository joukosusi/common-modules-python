#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/10 13:45
# @Author  : joukosusi
# @Mail    : joukosusi.sucl@outlook.com
# @File    : kafkacli.py
# @Software: PyCharm

"""
base on confluent-kafka-python
"""

# TODO


import os
import json
import contextlib
import threading
import time
import _md5

from collections import defaultdict

import confluent_kafka
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition


def version_compare(a, b):
    left = a.split('.')
    right = b.split('.')

    for i in range(max(len(left), len(right))):
        try:
            _a = left[i]
        except:
            return False

        try:
            _b = right[i]
        except:
            return True

        if int(_a) == int(_b):
            continue

        return int(_a) > int(_b)


class KafkaMessage(object):
    def __init__(self, topic='', partition=0, offset=0, timestamp=None, value=None):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp
        self.value = value


# conflunt consumer
class KafkaConsumer(object):
    """
    消费者， 可以消费多个topic, 但是不能同时重置多个topic的offset
    """
    def __init__(self, topic, cfg, cid=None, logger=None, normal=True, debug=False, **kwargs):
        """
        :param topic: [(topic, partition)]
        :param cfg: 共有配置
        :param cid: consumer的id
        :param logger: 外部id实例
        :param normal: 消费者模式
        :param kwargs: 主要用于内部参数传递
                  auto_commit: 是否自动提交 True/False
                  block: 是否阻塞获取，默认为False
        """
        self._cfg = dict(cfg, **{
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
            'fetch.min.bytes': 1024 * 1024,  # 一次获取多大消息
            'fetch.wait.max.ms': 1000,  # 耗费多少时间填充信息
            'fetch.message.max.bytes': 1048576,  # 批量信息最大长度
            'on_commit': self._on_commit,
            # 'offset.store.method': 'broker',
            # 'enable.auto.offset.store': True,
            'default.topic.config': {
                'auto.offset.reset': 'earliest',
            },
            'debug': ','.join([cfg.get('debug', ''), 'cgrp,topic,fetch']),
        })

        if debug is False:
            del self._cfg['debug']

        self._cfg['enable.auto.commit'] = kwargs.get('auto_commit', True)

        self._id = cid  # 当前consumer_id
        self._topic = topic  # 当前监听的topic
        self._create_time = time.time()  # 创建时间

        self._block = kwargs.get('block', False)  # 是否阻塞获取

        self._logger = logger
        self._start = True
        self._normal = normal

        self._start_offset = defaultdict(dict)  # 获取的起始offset
        self._end_offset = defaultdict(dict)    # 最后获取的offset
        self._total_offset = None  # 开始运行时的offset范围
        self._ori_offset = None  # 开始运行时

        self._consumer = Consumer(**self._cfg)

        if self._normal:
            self._total_offset = self.total_offset(self._topic)
            self._logger.info("total offset >>> \n{}".format(KafkaConsumer._convert_to_show(self._total_offset)))
            self._ori_offset = self.current_offset()
            self._logger.info("current offset >>> \n{}".format(KafkaConsumer._convert_to_show(self._ori_offset)))
            self._consumer.subscribe(self._topic, on_assign=self._on_assign, on_revoke=self._on_revoke)

    # FIXME 没有清楚何时调用
    def _on_assign(self, c, ps):
        # print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!assign!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        # print(c, ps)
        pass

    # FIXME 没有清楚何时调用
    def _on_revoke(self, c, ps):
        # print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!revoke!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        # print(c, ps)
        pass

    def _on_commit(self, err, partitions):
        pass

    def reset_offset(self, offsets):
        """
        目前重置offset的基本单位是topic, 所有partition都一起重置
        :param offsets:  dict
        :return:
        """
        _range = dict()

        def get_value(topic, partition, offset, p_offset=_range):
            """
            获取offset值并解析
            :param topic:
            :param partition:
            :param offset:
            :param p_offset:
            :return:
            """
            # FIXME 官方不支持commit 提交OFFSET_BEGINNING, OFFSET_END, 使用total_offset替代
            if offset in ('min', 'max'):
                if topic not in p_offset or partition not in p_offset[topic]:
                    _total = self.total_offset([topic])
                    p_offset.update(dict(p_offset, **_total))
                    if _total is None:
                        return None
                return p_offset[topic][partition][0] if offset == 'min' else p_offset[topic][partition][1]
                # return confluent_kafka.OFFSET_BEGINNING if para == 'min' \
                #                                         else confluent_kafka.OFFSET_END
            if isinstance(offset, int) and offset >= 0:
                return offset

            self._logger.warning('unknown reset value: {}, will not reset offset'.format(offset))
            return None

        if isinstance(offsets, dict):
            assigns = [TopicPartition(_topic, 0, get_value(_topic, 0, _offset)) for _topic, _offset in offsets.items()
                       if get_value(_topic, 0, _offset) is not None]
        elif isinstance(offsets, list):
            assigns = [TopicPartition(_item[0], _item[1], get_value(*_item)) for _item in offsets
                       if get_value(*_item) is not None]
        else:
            self._logger.warning('unknown type: {} for param[offsets], will not reset offset'.format(
                                    type(offsets).__name__))
            return False

        if assigns:
            [self._logger.warning('RESET offset to {0} for topic {1}({2})'.format(_t.offset, _t.topic, _t.partition))
             for _t in assigns]
            return self.commit(assigns, if_format=True)

        return not offsets

    def get(self):
        """

        :return: 如果有结果返回dict {
           'topic': xx,
           'partition': xx,
           'offset': xx,
           'tm':
           'data': xx
        },
        没有结果返回None，出错返回-1
        """
        if self._block:
            result = self._consumer.poll()
        else:
            result = self._consumer.poll(timeout=0.00001)

        if not result:
            return None

        if result.error():
            if result.error().code() == KafkaError._PARTITION_EOF:
                self._logger.warning('%s [%d] reached end at offset %d\n'.format(
                    result.topic(), result.partition(), result.offset()
                ))
            else:
                self._logger.error('encourage error:\n{}'.format(json.dumps(
                    {
                        'name': result.error().name(),
                        'code': result.error().code(),
                        'description': result.error().str()
                    }, indent=1)))

            return None

        # 记录处理的数据数量
        self._restore_offset_info(result)

        return KafkaMessage(
            topic=result.topic(),
            partition=result.partition(),
            offset=result.offset(),
            timestamp=result.timestamp()[1]
            if result.timestamp()[0] != confluent_kafka.TIMESTAMP_NOT_AVAILABLE
            else None,
            value=result.value()
        )

    @classmethod
    def _convert_to_show(cls, data, show=True):
        """
        将格式转换成展示的格式
        :param data:
        :return:
        """
        result = {i+'('+str(k)+')': str(l) for i, j in data.items() for k, l in j.items()}

        return json.dumps(result, indent=1) if show else result

    def _restore_offset_info(self, msg):
        """
        记录消费的信息
        :param msg:
        :return:
        """
        if msg.topic() not in self._start_offset or \
                msg.partition() not in self._start_offset[msg.topic()]:
            self._start_offset[msg.topic()][msg.partition()] = msg.offset()
        self._end_offset[msg.topic()][msg.partition()] = msg.offset()

    def commit(self, offsets=None, if_format=False):
        """
        手动提交， 提交最后一个消费的消息, 或者提交指定的offset
        :return:
        """
        if if_format:
            topics = offsets
        else:
            src = self._end_offset if offsets is None else offsets
            topics = [TopicPartition(i, k, m) for i, j in src.items() for k, m in j.items()]

        for _ in range(3):
            try:
                self._consumer.commit(offsets=topics, async=False)
                return True
            except (confluent_kafka.KafkaException,) as e:
                _exp_name, exp_code = e.args[0].name(), e.args[0].code()
                if int(exp_code) == 27:  # REBALANCE_IN_PROGRESS
                    self._logger.warning('COMMIT: kafka server is now in rebalancing, will retry...')
                    time.sleep(1)
                    continue
                self._logger.error('COMMIT: commit offset failed with message: {0}({1}) >> {2}'.format(
                    e.args[0].name(), e.args[0].code(), e.args[0].str()
                ))
                return False

        self._logger.error('COMMIT: commit failed after 3 times retry')
        return False

    def current_offset(self):
        """
        获取topic目前消费的位置
        :return:
        """
        result = defaultdict(dict)

        if not self._total_offset:
            self._total_offset = self.total_offset(self._topic)
        _p_topics = [TopicPartition(_key, _sub_key, -1) for _key, _value in self._total_offset.items()
                     for _sub_key in _value.keys()]

        try:
            r = self._consumer.committed(_p_topics)
            for _d in r:
                result[_d.topic][_d.partition] = _d.offset
            return result
        except (confluent_kafka.KafkaException, ) as e:
            self._logger.error('get total_offset failed with message: {0}({1}) >> {2}'.format(
                e.args[0].name(), e.args[0].code(), e.args[0].str()
            ))
            return None

    def total_offset(self, topics=None):
        """
        get smallest and biggest offset for specified topics
        :param topics: should be a list, exp: [topic,topic,topic]
        :return:
        """
        result = defaultdict(dict)

        for _topic in topics:
            try:
                for _n in range(100):
                    r = self._consumer.get_watermark_offsets(TopicPartition(_topic, _n, -1),
                                                             timeout=30,
                                                             cached=False)
                    result[_topic][_n] = tuple(int(i) for i in r)
            except (confluent_kafka.KafkaException, ) as e:
                name, code, e_str = e.args[0].name(), e.args[0].code(), e.args[0].str()
                if name == '_UNKNOWN_PARTITION':
                    continue
                else:
                    raise

        return result

    # FIXME not work
    def _commit_cb(self, err, reqs):
        self._logger.info('commit callback')
        self._logger.info(err)
        self._logger.info(reqs)

    def stop(self):
        if self._start:
            self._logger.debug('consumer stopped')
            self._consumer.close()
            self._start = False

            if self._normal:
                left, right = KafkaConsumer._convert_to_show(self._start_offset, show=False),\
                              KafkaConsumer._convert_to_show(self._end_offset, show=False)

                _out = {i: ' - '.join((left[i], right[i])) for i in right.keys()}

                self._logger.info('CONSUMER SUM UP:\n'
                                  '-create time: {0}[{1}]\n'
                                  '-consume offsets: \n{2}\n'.format(  # self._id,
                                                                     self._create_time,
                                                                     time.strftime('%Y-%m-%d %H:%M:%S',
                                                                                   time.localtime(self._create_time)
                                                                                   ),
                                                                     json.dumps(_out, indent=1),
                                                              )
                                  )

    def __del__(self):
        self.stop()


class KafkaProducer(object):

    def __init__(self, cfg, add_cfg, pid=None, logger=None, **kwargs):
        """

        :param cfg:
        :param logger:
        :param kwargs:
        """
        self._cfg = dict(cfg, **dict({
            'message.send.max.retries': 3,  # 重试次数
            'retry.backoff.ms': 1000,  # 等待多久重试
            'queue.buffering.max.messages': 200000,  # 本地发送队列消息最大数量
            'queue.buffering.max.kbytes': 1048576,  # 消息最大长度, 要比服务器配置小
            'queue.buffering.max.ms': 500,  # 等待消息堆积的时间 默认为0
            'batch.num.messages': 5,   # 消息集内消息数量
            'compression.codec': 'snappy',  # 压缩方式
            'delivery.report.only.error': False,  # 发送失败的时候才调用回调函数
            'on_delivery': self._delivery_cb,
        }, **add_cfg))

        self._id = pid
        self._logger = logger
        self._start = True
        self._sync = kwargs.get('sync', False)  # dep
        self.s_call_back = kwargs.get('success_callback', None)
        self.f_call_back = kwargs.get('failed_callback', None)
        self._send_success_count = 0  # 该生产者发送成功的条数
        self._send_failed_count = 0   # 该生产者发送失败的条数
        self._create_time = time.time()  # 创建时间

        self._cfg['message.send.max.retries'] = kwargs.get('retry', 3)
        self._producer = Producer(**self._cfg)

        # self._logger.info('new producer[{0}] with config:\n{1}'.format(pid, json.dumps(self._cfg, indent=1)))

    def send(self, topic, message, timeout=60):
        """
        发送数据， 同步发送超时时间为60s
        FIXME 同步的时候回调函数无效,
        :param topic:
        :param message:
        :param timeout:
        :return:
        """

        try:
            self._producer.produce(topic, message)

            if self._sync:
                send_result = self._producer.flush(timeout=timeout)
                self._cal_process_count(not send_result)
                return True if send_result == 0 else False
            else:
                self._producer.poll(timeout=0)
                return True
        except (BufferError,):
            self._logger.error('Local producer queue is full (%d messages awaiting delivery): try again\n'
                               % len(self._producer))
            return False

    def _delivery_cb(self, err, msg):
        """
        发送回调函数, FIXME 异步发送有事在下一个发送成功时，才会返回上一个回调，需要追踪
        :param err:
        :param msg:
        :return:
        """
        self._cal_process_count(not err)
        _msg = KafkaMessage(
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                            timestamp=msg.timestamp()[1]
                            if msg.timestamp()[0] != confluent_kafka.TIMESTAMP_NOT_AVAILABLE
                            else None,
                            value=msg.value()
        )
        if err:
            self._logger.error('send message failed:\n{}'.format(json.dumps(
                {
                    'name': err.name(),
                    'code': err.code(),
                    'description': err.str()
                }, indent=1)))
            if self.f_call_back:
                self.f_call_back(_msg)
        else:
            if self.s_call_back:
                self.s_call_back(_msg)

    def _cal_process_count(self, result):
        if result:
            self._send_success_count += 1
        else:
            self._send_failed_count += 1

    def stop(self):
        """
        停止发送并将本地队列的数据全部发送完(阻塞),
        :return:
        """
        if self._start:
            self._logger.debug('waiting for all data delivered...')
            res = self._producer.flush()
            self._start = False
            self._logger.warning('remain {} message unsent'.format(res))
            self._logger.debug('producer stopped')

            self._logger.info('PRODUCER SUM UP[{0}]:\n'
                              '-create time: {1}[{2}]\n'
                              '-send success: {3}\n'
                              '-send failed: {4}\n'.format(self._id,
                                                           self._create_time,
                                                           time.strftime('%Y-%m-%d %H:%M:%S',
                                                                         time.localtime(self._create_time)
                                                                         ),
                                                           self._send_success_count,
                                                           self._send_failed_count,
                                                           )
                              )

    def __del__(self):
        self.stop()


# conflunt python客户端
class PIOKafkaClient(object):

    def __init__(self, hosts, ssl_path=None, passwd=None,
                 logger=None, debug=False, version=None):

        if logger:
            self._logger = logger
        else:
            import logging
            logging.getLogger('kafka_client')

        self._debug = debug
        self._producer = dict()
        self._consumer = dict()
        self._p_lock = threading.Lock()
        self._c_lock = threading.Lock()
        self._s_callback = None
        self._f_callback = None

        # 通用配置
        self._common_cfg = {
            'group.id': 'default',
            'bootstrap.servers': hosts,
            'security.protocol': 'ssl' if ssl_path else 'plaintext',
            'ssl.key.location': os.path.join(ssl_path, 'client.key.unsecure') if ssl_path else None,
            'ssl.ca.location': os.path.join(ssl_path, 'ca-cert') if ssl_path else None,
            'ssl.certificate.location': os.path.join(ssl_path, 'client.crt') if ssl_path else None,
            'ssl.key.password': passwd,
            'api.version.request': True if version_compare(version,'0.9.0.1') else False,
            'broker.version.fallback': version,
            'log.connection.close': False,
            'heartbeat.interval.ms': 10000,
            'log_level': 6,
            'message.max.bytes': 314572800,
            'stats_cb': self._status_cb,  # 获取此时的连接信息，相当与心跳包
            'error_cb': self._error_cb,
            'statistics.interval.ms': 0,  # stats_cb 执行的间歇时长, 为0时取消
            'session.timeout.ms': 30000,  # 连接窗口超时时间
            # 'topic.metadata.refresh.interval.ms': 40000,   # 客户端定时刷新元数据(30000)
            'debug': 'generic,broker,topic,metadata',
        }

        if debug is False:
            del self._common_cfg['debug']

    def __del__(self):
        self.stop()

    def get(self, topic, group, **kwargs):
        """
        :param topic:
        :param group:
        :param kwargs:
         :parameter auto: 是否自动获取consumer, True/False
        :return:
        """
        kwargs['auto_destory'] = True
        with self.get_consumer(topic, group, debug=self._debug, **kwargs) as c:
            return c.get()

    def send(self, topic, message, **kwargs):
        """

        :param topic:
        :param message:
        :param kwargs:
                    callback: 发送失败的时候调用的回调函数, 参数为msg，发送失败的数据
                    sync: 同步发送标识，默认为False
                    retry: 重发次数
        :return:
        """
        kwargs['auto_destory'] = False
        if self._s_callback:
            kwargs['success_callback'] = self._s_callback
        if self._f_callback:
            kwargs['failed_callback'] = self._f_callback
        with self.get_producer(**kwargs) as p:
            return p.send(topic, message)

    def set_produce_callback(self, sucess_callback=None, failed_callback=None):
        self._s_callback = sucess_callback
        self._f_callback = failed_callback

    @contextlib.contextmanager
    def get_producer(self, **kwargs):
        """
        获取producer
        :param kwargs:
                    callback: 发送失败的时候调用的回调函数, 参数为msg，发送失败的数据
                    sync: 同步发送标识，默认为False
                    retry: 重发次数
        :return:
        """
        _id = ''.join((threading.currentThread().name,
                       'sync' if kwargs.get('sync') else 'async',))

        if _id not in self._producer:
            add_cfg = {}
            self._producer[_id] = KafkaProducer(self._common_cfg, add_cfg, pid=_id, logger=self._logger, **kwargs)
        try:
            yield self._producer[_id]
        finally:
            # 外部调用自动销毁
            if kwargs.get('auto_destory', True) is True:
                self._producer[_id].stop()
                del self._producer[_id]

    @contextlib.contextmanager
    def get_consumer(self, topics, group, **kwargs):
        """
        获取消费器
        :param topics:
        :param group:
        :param kwargs: 内部附加可选参数
                        reset_offset smallest/largest/具体offset 重置offset
                        block True/False 是否阻塞获取
                        auto_commit True/False 是否自动提交
        :return:
        """
        if isinstance(topics, str):
            _topics = [topics]
        elif isinstance(topics, list) or isinstance(topics, tuple):
            _topics = topics
        else:
            raise ValueError("topics must be str or list/tuple, but {}".format(type(topics).__name__))

        _id = ''.join((threading.currentThread().name,
                       group,
                       ':'.join(sorted(topics)),)
                      )
        self._logger.debug('_id >> {}'.format(_id))
        if _id not in self._consumer:
            # 驱动配置
            self._common_cfg['group.id'] = group
            self._consumer[_id] = KafkaConsumer(_topics, self._common_cfg, cid=_id, logger=self._logger, **kwargs)
        try:
            yield self._consumer[_id]
        finally:
            # 外部调用自动销毁
            if kwargs.get('auto_destory', False) is False:
                self._consumer[_id].stop()
                del self._consumer[_id]

    def _status_cb(self, para):
        self._logger.info('status..........')
        self._logger.info(para)

    def _error_cb(self, msg):
        """
        全局错误处理
        :param msg:
        :return:
        """
        extra_msg = '{0}({1}) >> {2}'.format(msg.name(), int(msg.code()), msg.str())
        self._logger.error(extra_msg)

    def stop(self, key='all'):

        self._logger.info('stop client with key[{}]...'.format(key))

        if key in ('all', 'consumer'):
            self._c_lock.acquire()
            for _id in tuple(self._consumer.keys()):
                self._consumer[_id].stop()
                del self._consumer[_id]
            self._c_lock.release()
        if key in ('all', 'producer'):
            self._p_lock.acquire()
            for _id in tuple(self._producer.keys()):
                self._producer[_id].stop()
                del self._producer[_id]
            self._p_lock.release()
        self._logger.info('kafka client[{}] stopped'.format(key))

    def reset_offset(self, group, offsets):
        """
        :param offsets: topic:offset
        :param group
        :return: None
        """
        assert isinstance(offsets, dict) or isinstance(offsets, list)

        with self.get_consumer('CONFLUENT_KAFKA_CLIENT.TOOLS',
                               group,
                               auto_commit=False,
                               normal=False) as c:
            return c.reset_offset(offsets)

    def total_offset(self, topics):
        """

        :param topics:
        :return:
        """
        assert isinstance(topics, list)

        with self.get_consumer('CONFLUENT_KAFKA_CLIENT.TOOLS',
                               'CONFLUENT_KAFKA_CLIENT.TOOLS.GROUP',
                               auto_commit=False,
                               normal=False) as c:
            return c.total_offset(topics)

    def current_offset(self, group, topic):
        """

        :param group:
        :param topic:
        :return:
        """
        with self.get_consumer(topic, group, auto_commit=False, normal=False) as c:
            return c.current_offset()
