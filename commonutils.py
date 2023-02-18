# coding:utf-8
import datetime
import logging

import dateutil.parser

log = logging.getLogger()
log.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)
import chinese_calendar
from pymongo import monitoring


def daysMarketOpen(start, end):
    '''
    获取起始区间开市天数
    :param start: 起始日期
    :param end: 截止日期
    :return: 开市天数
    '''
    if not isinstance(start, datetime.datetime):
        start = dateutil.parser.parse(start)
    if not isinstance(end, datetime.datetime):
        end = dateutil.parser.parse(end)

    workdays = chinese_calendar.get_workdays(start,end)
    workdays = [x for x in workdays if x.isoweekday() not in [6, 7]]
    return len(workdays), workdays


def fenci(sentence: str) -> str:
    '''
    为实现mongoengine中文文字搜索，需要对存储的字符串进行分词
    :param sentence: 输入的字符串
    :return:
    '''
    # words = jieba.lcut_for_search(sentence) 用jieba进行分词
    # return ' '.join(words)
    return ' '.join(sentence)  # 用空格进行分词


def joinword(sentence: str) -> str:
    '''
    去掉分词生成的空格
    :param sentence:
    :return:
    '''
    words = sentence.replace(' ', '')
    return words


def dateToTimeStamp(dt):
    '''
    日期转时间戳
    :param date: 输入日期
    :return: 返回时间戳
    '''
    if isinstance(dt, str):
        dt = dateutil.parser.parse(dt)
    return datetime.datetime.timestamp(dt)


# mongodb monitoring class
class CommandLogger(monitoring.CommandListener):

    def started(self, event):
        log.debug("Command {0.command_name} with request id "
                  "{0.request_id} started on server "
                  "{0.connection_id}".format(event))

    def succeeded(self, event):
        log.debug("Command {0.command_name} with request id "
                  "{0.request_id} on server {0.connection_id} "
                  "succeeded in {0.duration_micros} "
                  "microseconds".format(event))

    def failed(self, event):
        log.debug("Command {0.command_name} with request id "
                  "{0.request_id} on server {0.connection_id} "
                  "failed in {0.duration_micros} "
                  "microseconds".format(event))