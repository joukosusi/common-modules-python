# encoding=utf-8
"""
``log_handler``  提供一个模板日志类，根据参入参数进行定制
    LogHandler： 日志处理类，如果需要sentry，需要安装raven库
"""
from __future__ import print_function
__author__ = 'qxue'

import logging
import logging.config
import logging.handlers
import os
import sys
import traceback

import raven
from raven.utils.encoding import to_string
from raven.handlers.logging import SentryHandler

open_manual_track = {'manual_track': True}
"""手动添加track信息的开关, 使用方法: logger.error(msg, open_manual_track),
   可以在使用logger的同时让sentry显示堆栈信息(没有打开开关没有堆栈信息)
"""


class LocalSentryHandler(SentryHandler):
    """
    重载SentryHandler, 添加异常信息
    """

    def __init__(self, *args, **kwargs):
        raven.handlers.logging.SentryHandler.__init__(self, *args, **kwargs)

    def emit(self, record):
        try:
            # Beware to python3 bug (see #10805) if exc_info is (None, None, None)
            self.format(record)

            if not self.can_record(record):
                print(to_string(record.message), file=sys.stderr)
                return

            # 打开手动添加异常信息的开关, 在logger.error时需要添加 logger.error('11', open_manual_track)
            if isinstance(record.args, dict) and \
                    record.args.get('manual_track', False):
                record.exc_info = sys.exc_info()

            return self._emit(record)
        except Exception:
            if self.client.raise_send_errors:
                raise
            print("Top level Sentry exception caught - failed "
                  "creating log record", file=sys.stderr)
            print(to_string(record.msg), file=sys.stderr)
            print(to_string(traceback.format_exc()), file=sys.stderr)


class LogHandler(logging.LoggerAdapter):
    """
        重载logging的相关处理
    """

    def __init__(self, log_name, *args, **kw):
        """
            :param: log_name 日志文件名字
            :param:  arg 可变参数
            :param: kw 命名参数字典
                    log_level  日志级别
                    path  日志路径
                    backup_count 日志备份多少个文件
                    max_bytes 每个日志文件最大容量
                    sentry_dns sentry配置字符串
                    disable_exist 是否屏蔽已有logger
        """
        path = kw.get("path", "")
        if path == "":
            path = os.path.dirname(os.path.realpath(__file__)) + os.sep + "../log/"

        filename = path + os.sep + log_name + ".log"

        config_dict = {
            'version': 1,
            'disable_existing_loggers': kw.get('disable_exist', False),
        }
        # 全局的配置，所有logger生效
        logging.config.dictConfig(config_dict)

        self.logger = logging.getLogger(log_name)

        # 私有设置
        self.logger.setLevel(kw.get("log_level", "INFO"))
        # 3种基本处理器
        formatter = logging.Formatter("[%(asctime)s][%(process)d][%(thread)d-%(threadName)s][%(levelname)s]"
                                      "<%(filename)s:%(lineno)d> %(message)s")

        # file
        # filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=0
        file_hanlder = logging.handlers.RotatingFileHandler(filename, maxBytes=kw.get("max_bytes", 1024 * 1024 * 10),
                                                            backupCount=kw.get("backup_count", 10))
        file_hanlder.setLevel(kw.get("log_level", "INFO"))
        file_hanlder.setFormatter(formatter)
        self.logger.addHandler(file_hanlder)

        # console
        console = logging.StreamHandler()
        console.setLevel(kw.get("log_level", "INFO"))
        console.setFormatter(formatter)
        self.logger.addHandler(console)

        # sentry
        if "sentry_dns" in kw and kw["sentry_dns"] != "":
            sentry = LocalSentryHandler(kw.get("sentry_dns", ""), level=logging.ERROR)
            self.logger.addHandler(sentry)

        self.extra = {}
        # config_dict = {
        #     'version': 1,
        #     'disable_existing_loggers': kw.get('disable_exist', False),
        #     'formatters': {
        #         'verbose': {
        #             'format': "[%(asctime)s][%(process)d][%(thread)d-%(threadName)s][%(levelname)s]"
        #                       "<%(filename)s:%(lineno)d> %(message)s",
        #             'datefmt': "%Y-%m-%d %H:%M:%S"
        #         },
        #         'simple': {
        #             'format': '%(levelname)s %(message)s'
        #         },
        #     },
        #     'handlers': {
        #         'null': {
        #             'level': kw.get("log_level", "INFO"),
        #             'class': 'logging.NullHandler',
        #         },
        #         'console': {
        #             'level': kw.get("log_level", "INFO"),
        #             'class': 'logging.StreamHandler',
        #             'formatter': 'verbose'
        #         },
        #         'sentry': {
        #             'level': 'ERROR',
        #             'class': 'piowind_utils.log.LocalSentryHandler',
        #             # 'class': 'raven.handlers.logging.SentryHandler',
        #             'dsn': kw.get("sentry_dns", ""),
        #         },
        #         'file': {
        #             'level': kw.get("log_level", "INFO"),
        #             'class': 'logging.handlers.RotatingFileHandler',
        #             # 当达到10MB时分割日志
        #             'maxBytes': kw.get("max_bytes", 1024 * 1024 * 10),
        #             # 最多保留50份文件
        #             'backupCount': kw.get("backup_count", 10),
        #             # If delay is true,
        #             # then file opening is deferred until the first call to emit().
        #             # 'delay': True,
        #             'filename': filename,
        #             'formatter': 'verbose'
        #         }
        #     },
        #     'loggers': {
        #         '': {
        #             'handlers': ['file', 'sentry','console'],
        #             'level': kw.get("log_level", "INFO"),
        #         },
        #     }
        # }
        # if "sentry_dns" not in kw or kw["sentry_dns"] == "":
        #     config_dict["loggers"] = {
        #         '': {
        #             'handlers': ['file', 'console'],
        #             'level': kw.get("log_level", "INFO"),
        #         },
        #     }
        #     del config_dict["handlers"]["sentry"]
        # logging.config.dictConfig(config_dict)
        # self.logger = logging.getLogger(log_name)
        # self.extra = {}
