# coding:utf-8

import re
import json
import datetime
from enum import Enum

from mootdx.affair import Affair
from mootdx.utils import get_frequency
from pymongo import monitoring

import logging

from stock.SecuritiesDateProcess.SecuritiesModels import BarData, Securities, SecuritiesCodePrefix, KType
from stock.SecuritiesDateProcess.commonutils import CommandLogger, daysMarketOpen

log = logging.getLogger()
log.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)

# pandas setting
import pandas as pd
import numpy as np
from mootdx import consts

pd.set_option('expand_frame_repr', False)  # True就是可以换行显示。设置成False的时候不允许换行
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('precision', 2)  # 显示小数点后的位数
pd.set_option('display.width', 200)
pd.set_option('display.float_format', lambda x: '%.2f' % x)  # 为了直观的显示数字，不采用科学计数法

# matplotlib plt setting
import matplotlib.pyplot as plt
import matplotlib as mpl

mpl.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
mpl.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号-显示为方块的问题
from mootdx.quotes import Quotes
from mongoengine import *

# resgister pymongo monitoring
monitoring.register(CommandLogger())

# 连接mongodb
connect('stock', alias='default')

# 通达信数据读取接口
class MootdxHelper():

    def __init__(self):
        self.client = Quotes.factory(market='std', multithread=False, heartbeat=False)

    def downKbars(self, symbol, frequency, startDate=None, endDate=None):
        '''
        获取k线数据
        :param symbol: 代码，字符串或数组
        :param frequency:
        :param offset:
        :return:
        '''
        now = datetime.datetime.now()
        if startDate == None:
            startDate = datetime.datetime(now.year - 1, now.month, now.day)
        if endDate == None:
            endDate = now

        downloadCounts = daysMarketOpen(startDate, endDate)

        # client.bars sysmbol 证券代码 frequency 数据频次, K线周期 start 起始位置 offset 获取数量
        cycles, remainder = divmod(downloadCounts[0], 800)
        data = None
        if cycles:
            data = pd.concat(
                [
                    self.client.bars(symbol=symbol, frequency=frequency, start=i * 800, offset=800)
                    for i in range(cycles)
                ],
                axis=0,
            )
        data = pd.concat(
            [
                data,
                self.client.bars(symbol=symbol, frequency=frequency, start=cycles * 800, offset=remainder)
            ],
            axis=0
        )

        for index, record in data.iterrows():
            if BarData.getkbar(symbol=symbol, ktype=frequency, dt=record['datetime']) == None:
                record['dt'] = record.pop('datetime')
                barData = BarData(symbol=symbol, ktype=frequency, **record)
                barData.save()
                log.debug('download and save Kbar Data success!')

    def realTimeQuotes(self, symbol):
        '''
        查询实时行情
        :param symbol:
        :return:
        '''
        df = self.client.quotes(symbol=symbol)
        print(df)
        print(df.columns)

    def numberOfStock(self, market):
        '''
        指数K线行情
        :param market:
        :return:
        '''
        num = self.client.stock_count(market)
        print(num)
        return num

    def indexQuotation(self, symbol, frequency, market, start=1, offset=2):
        '''
        指数K线行情
        :param market:
        :return:
        '''
        index = self.client.index(frequency=frequency, market=market, symbol=symbol, start=start, offset=offset)
        print(index)
        return index

    def getHistory(self, symbol, date):
        '''
        获取分钟数据
        :param symbol:
        :param date:
        :return:
        '''
        df = self.client.minute(symbol=symbol, date=date)
        print(df)

    def getAffair(self):
        # 获取远程文件列表
        files = Affair.files()
        return files

    def getAffairOne(self, filename, downdir='data/affair'):
        # 下载单个文件
        Affair.fetch(downdir, filename)

    def getAffairAll(self):
        # 下载全部文件
        Affair.parse(downdir='./data/affair')

    def getSecuritiesList(self, market):
        '''
        获取证券列表
        :param market: 证券市场代码 0 深市  1 沪市 2 京市
        :return:
        '''
        Securities = self.client.stocks(market=market)
        Securities['market'] = market
        for securities in Securities.to_dict(orient='records'):
            print(securities)
            _securities = Securities.getSecurities(code=securities['code'], market=securities['market'])
            print(_securities)
            if _securities:
                securities['id'] = _securities['id']
                print(_securities)
            securities = Securities(**securities)
            securities.save()


if __name__ == '__main__':
    fun = 2
    tdx = MootdxHelper()

    if fun == 1:  # 下载kbar data
        tdx.downKbars('600887', KType.day.value, startDate='20190101')
    if fun == 2:  # 查询kbar
        data = BarData.querykbar(symbol='600887', ktype=KType.day.value).exclude('symbol', 'id', 'ktype').as_pymongo()
        df = pd.DataFrame(data)
        print(df)
    if fun == 3:  # 获取实时数据
        tdx.realTimeQuotes(['113615', '128145'])
    if fun == 4:  # 获取深圳市场股票数
        tdx.numberOfStock(market=consts.MARKET_SZ)
    if fun == 5:  # 获取上海市场股票数
        tdx.numberOfStock(market=consts.MARKET_SH)
    if fun == 6:  # 获取北京市场股票数
        tdx.numberOfStock(market=consts.MARKET_BJ)
    if fun == 7:  # 获取指数数据
        tdx.indexQuotation(symbol='000001', market=consts.MARKET_SH, frequency=get_frequency(KType.day.value))
    if fun == 8:  # 获取历史数据
        tdx.getHistory()
    if fun == 9:  # 获取通达信财务数据文件
        print(tdx.getAffair())
    if fun == 10:  # 获取通达信财务数据文件
        print(tdx.getAffairOne('gpcw20221231.zip'))

    if fun == 11:  # 获取上海市场股票列表
        tdx.getSecuritiesList(consts.MARKET_SZ)
    if fun == 12:  # 获取上海市场股票列表
        tdx.getSecuritiesList(consts.MARKET_SH)
    if fun == 13:  # 筛选股票列表
        stocks: QuerySet = Securities.querySecuritiesByCode('601111')
        print(stocks.as_pymongo())
    if fun == 14:  # 筛选股票列表
        stocks = Securities.querySecurities(market=consts.MARKET_SZ, prefix=SecuritiesCodePrefix.深市A股股票)
        print(stocks.count())
        print(stocks.as_pymongo())