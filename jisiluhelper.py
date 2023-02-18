# coding:utf-8

import gzip
import json
import re
import urllib
from http import cookiejar
from pprint import pprint
from urllib.parse import urlencode
from collections import OrderedDict
import logging
import datetime
import dateutil
import os
import joblib

import pandas as pd
import numpy as np
from typing import List

from bs4 import BeautifulSoup
from mongoengine import *

# pandas setting
from pymongo import monitoring

from stock.SecuritiesDateProcess.SecuritiesModels import ConvBond, ConvBondCalendar, ConvBondHistory
from stock.SecuritiesDateProcess.commonutils import dateToTimeStamp, fenci, joinword, CommandLogger

pd.set_option('expand_frame_repr', False)  # True就是可以换行显示。设置成False的时候不允许换行
pd.set_option('display.max_rows', None)  # 设置最多显示行数
pd.set_option('display.max_columns', None)  # 设置显示最多字段数
pd.set_option('precision', 2)  # 显示小数点后的位数
pd.set_option('display.width', 200)  # 设置横向最多显示多少个字符
pd.set_option('display.float_format', lambda x: '%.2f' % x)  # 为了直观的显示数字，不采用科学计数法

# matplot plt setting
import matplotlib.pyplot as plt
import matplotlib as mpl

mpl.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
mpl.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号-显示为方块的问题

# log setting
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# resgister pymongo monitoring
monitoring.register(CommandLogger())
# 连接mongodb
connect('stock', alias='default')


# 集思录日历数据类型
class DataType():
    '''
    集思录日历数据类型
    '''
    新股申购 = 'newstock_apply'  # 新股申购日历
    新股上市 = 'newstock_onlist'  # 新股上市日历
    可转债 = 'CNV'  # 可转债日历信息
    REITS认购 = 'cnreits'  # RETIS认购信息
    债券申购 = 'newbond_apply'
    债券上市 = 'newbond_onlist'
    A股分红 = 'diva'
    基金 = 'FUND'
    债券 = 'BOND'
    股票 = 'STOCK'


# 可转债显示数据类型
class DisplyType():
    '''
    可转债显示数据类型
    '''
    转债溢价率 = 'premium_rt'
    税前到期收益率 = 'ytm_rt'
    转股价值强赎 = 'convert_value'


# 集思录类
class 集思录():
    '''
    集思录可转债
    '''

    def __init__(self, debug=False):
        self.cookie = cookiejar.CookieJar()
        handler = urllib.request.HTTPCookieProcessor(self.cookie)
        self.opener = urllib.request.build_opener(handler)
        self.debug = debug
        self.logined = False

    def login(self):
        '''
        登录集思录网站
        :return:
        '''

        url = 'https://www.jisilu.cn/webapi/account/login_process/'

        data = urlencode(
            {
                'return_url': 'https://www.jisilu.cn/',
                'user_name': 'a336943fd03cfd1cec3f21219f5ec650',
                'password': '67ed99315162798e8a761f42c48f53b9',
                'auto_login': '1',
                'aes': '1',
            })
        data = bytes(data, encoding='utf-8')
        headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',  # 'content-length' : '145' ,
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://www.jisilu.cn',
            'referer': 'https://www.jisilu.cn/account/login/',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36', 'x-requested-with': 'XMLHttpRequest',
        }

        req = urllib.request.Request(url=url, data=data, headers=headers)
        result = self.opener.open(req)
        result = result.read()
        result = gzip.decompress(result)
        result = result.decode()
        result = json.loads(result)
        if result['code'] == 200:
            logger.info('->登录成功！')
            self.logined = True
        else:
            logger.info('->登录失败！')
            self.logined = False  # if self.debug:  #     pprint(result)

    def downConvBond(self, update=False):
        '''
        获取全部上市可转债
        :return: 上市可转债数据存入mongodb
        '''
        if update:
            ConvBond.objects.delete()
        url = 'https://www.jisilu.cn/webapi/cb/list_new/'
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'columns': '1,70,2,3,5,6,11,12,14,15,16,29,30,32,34,44,46,47,50,52,53,54,56,57,58,59,60,62,63,67',
            'if-modified-since': 'Sun, 25 Dec 2022 14:05:39 GMT',
            'init': '1',
            'referer': 'https://www.jisilu.cn/web/data/cb/list',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36',
        }

        req = urllib.request.Request(url=url, headers=headers)
        result = self.opener.open(req)
        result = result.read()
        result = gzip.decompress(result)
        result = result.decode()  # .encode('gbk', 'ignore').decode('gbk')
        result = json.loads(result)

        if len(result['data']) > 0:
            logger.info('->下载数据成功！')
            df = pd.DataFrame(result['data'])
            df.drop('convert_dt', axis=1, inplace=True)
            df['short_maturity_dt'] = df['short_maturity_dt'].apply(lambda x: '20' + x)
            for index, record in df.iterrows():
                print(record)
                bond = ConvBond(**record.to_dict())
                bond.save()
            logger.info('->数据存储数据库成功！')

        else:
            logger.info('->下载数据失败或数据为空！')

    def downBondDetail(self, bondid):
        '''
        获取可转债的详细信息
        :param bondid:
        :return:
        '''
        url = fr'https://www.jisilu.cn/data/convert_bond_detail/{bondid}'
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',  # 'content-length' : '145' ,
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://www.jisilu.cn',
            'referer': 'https://www.jisilu.cn/account/login/',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36',
        }

        req = urllib.request.Request(url=url, headers=headers)
        result = self.opener.open(req)
        result = result.read()
        result = gzip.decompress(result)
        result = result.decode()  # .encode('gbk', 'ignore').decode('gbk')
        # soup = BeautifulSoup(result, 'html5lib')
        # tb = soup.find_all('table')
        try:
            df = pd.read_html(result)
            # print(len(df))
            # print(df[0])
            # 处理表格1
            print(df[0])
            # 1 处理第0行
            line0 = df[0].iloc[0, :][0]  # .to_dict(orient='split')['data']
            line0 = re.findall('(\d{6}).*?正股([\S]+) (\d+).*?行业(.*)', line0)
            assert line0 != [], line0
            data = {}
            data['bond_id'] = line0[0][0]
            data['stock_nm'] = line0[0][1]
            data['stock_id'] = line0[0][2]
            data['industry'] = line0[0][3]
            # 2 处理第1-2行
            line1_2: pd.DataFrame = df[0].iloc[1:3, :]
            data_line1_2 = line1_2.to_dict(orient='split')['data']
            _data = []
            for x in data_line1_2:
                _data.extend(x)
            data_line1_2 = set(_data)
            item: str
            for item in data_line1_2:
                val = float(item.split()[1].strip('%')) if '-' not in item else 0
                if '到期税前收益' in item:
                    data['ytm_rt'] = val
                elif '到期税后收益' in item:
                    data['ytm_rt_tax'] = val
                elif '换手率' in item:
                    data['turnover_rt'] = val
                elif '价格' in item:
                    data['price'] = val
                elif '涨幅' in item:
                    data['increase_rt'] = val
                elif '转股价值' in item:
                    data['convert_value'] = val
                elif '溢价率' in item:
                    data['premium_rt'] = val

            # 3 处理line 3-8
            line3_8: pd.DataFrame = df[0].iloc[3:9, :]
            data_line3_8 = line3_8.to_dict(orient='split')['data']
            _data = []
            for x in data_line3_8:
                _data.extend(x)
            _data = [(_data[i], _data[i + 1]) for i in range(0, len(_data), 2)]
            _data = [x for x in _data if x[0] != x[1]]
            item: str
            for key, val in _data:
                if key == '转股起始日':
                    data['convert_dt'] = val

                if key == '回售起始日':
                    data['put_dt'] = val

                if key == '到期日':
                    data['short_maturity_dt'] = val

                if key == '发行规模(亿)':
                    data['orig_iss_amt'] = float(val) if '-' not in val else 0

                if key == '转股价':
                    data['convert_price'] = float(val) if '-' not in val else 0

                if key == '回售价':
                    data['put_price'] = float(val.replace('+利息', '')) if '-' not in val else 0

                if key == '剩余年限':
                    data['year_left'] = float(val) if '-' not in val else 0

                if key == '剩余规模(亿)':
                    data['curr_iss_amt'] = float(val) if '-' not in val else 0

                if key == '转股代码':
                    data['convert_cd_tip'] = val

                if key == '回售触发价':
                    data['put_convert_price'] = float(val) if '-' not in val else 0

                if key == '到期赎回价':
                    if '+' in val:
                        rt = val.split('+')
                        rt = float(val[0]) + float(val[1])
                    else:
                        rt = 0
                    data['redeem_price'] = rt

                if key == '已转股比例':
                    data['cvt_rt'] = float(val.strip('%')) if '-' not in val else 0

                if key == '转债市值占比':
                    data['convert_amt_ratio2'] = float(val.strip('%')) if '-' not in val else 0

                if key == '正股总市值(亿)':
                    data['total_market_value_stock'] = float(val) if '-' not in val else 0

                if key == '转债流通市值占比':
                    data['convert_amt_ratio'] = float(val.strip('%')) if '-' not in val else 0

                if key == '正股流通市值(亿)':
                    data['total_outstanding_market_value_stock'] = float(val) if '-' not in val else 0

                if key == '下修触发价':
                    data['threshold_value'] = float(val) if '-' not in val else 0

                if key == '强赎触发价':
                    data['force_redeem_price'] = float(val) if '-' not in val else 0

                if key == '正股PB':
                    data['pb'] = float(val) if '-' not in val else 0

                if key == '强赎天计数':
                    data['redeem_status'] = val

                if key == '下修天计数':
                    data['adjust_condition'] = val

                if key == '下修触发价':
                    data['threshold_value'] = float(val) if '-' not in val else 0

            # 处理table 2
            print(df[1])
            line0 = df[1].iloc[0, :]  # .to_dict(orient='split')['data']
            if '-' not in line0[1]:
                data['list_dt'] = line0[1]
            data['stockholders_placing_ratio'] = float(line0[3].strip('%')) if '-' not in line0[3] else 0
            data['winning_rate'] = float(line0[5].strip('%')) if '-' not in line0[5] else 0

            bond: Document = ConvBond.getBond(data['bond_id'])

            if bond:
                bond.update(**data)
            else:
                bond = ConvBond(**data)
                bond.save()
            logging.debug('可转债存储成功！')
        except Exception as e:
            logging.debug(f'可转债获取详情错误！ID:{bondid}')
            logging.debug(e)

    def downCalendaData(self, datatype, startDate=None, endDate=None):
        '''
        获取日历数据
        :return: 存储日历数据
        '''
        now = datetime.datetime.now()
        if startDate == None:
            startDate = now
        elif not isinstance(startDate, datetime.datetime):
            startDate = dateutil.parser.parse(startDate)
        if endDate == None:
            endDate = now + datetime.timedelta(days=30)
        elif not isinstance(endDate, datetime.datetime):
            endDate = dateutil.parser.parse(endDate)

        startDate = datetime.datetime.timestamp(startDate)
        endDate = datetime.datetime.timestamp(endDate)
        urldata = {'qtype': datatype, 'start': startDate, 'end': endDate, '_': '1671951577369', }

        urldata = urlencode(urldata)

        url = 'https://www.jisilu.cn/data/calendar/get_calendar_data/?' + urldata
        headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'referer': 'https://www.jisilu.cn/data/calendar/',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36', 'x-requested-with': 'XMLHttpRequest',
        }

        req = urllib.request.Request(url=url, headers=headers)
        result = self.opener.open(req)
        result = result.read()
        result = gzip.decompress(result)
        result = result.decode()  # .encode('gbk', 'ignore').decode('gbk')
        result = json.loads(result)

        if len(result) > 0:
            logger.info('->下载数据成功！')
            df = pd.DataFrame(result)
            df.drop('color', inplace=True, axis=1)
            ConvBondCalendar.objects.delete()
            for index, record in df.iterrows():
                if ConvBondCalendar.getBondCalendar(code=record['id'], start=record['start']) == None:
                    record['title'] = fenci(record.title)
                    record['start'] = record['start']
                    record['symbol'] = record['code']
                    record['code'] = record['id']
                    record.pop('id')
                    bond = ConvBondCalendar(**record.to_dict())
                    bond.save()
                    logger.info('->数据存储数据库成功！')
                else:
                    logger.info('->数据已存在！')

        else:
            logger.info('->下载数据失败或数据为空！')

    def downHistoryData(self, bond_id):
        '''
        获取可转债历史数据
        :return: 可转债历史数据
        '''

        urldata = {'rp': 100, 'page': 1, }

        urldata = urlencode(urldata)
        urldata = bytes(urldata, encoding='utf-8')

        url = f'https://www.jisilu.cn/data/cbnew/detail_hist/{bond_id}'
        headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            # 'content-length': '12',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36', 'x-requested-with': 'XMLHttpRequest',
        }

        req = urllib.request.Request(url=url, data=urldata, headers=headers, )
        result = self.opener.open(req)
        result = result.read()
        result = gzip.decompress(result)
        result = result.decode()  # .encode('gbk', 'ignore').decode('gbk')
        result = json.loads(result)

        if len(result) > 0:

            logger.info('->下载数据成功！')
            result = [x['cell'] for x in result['rows']]
            df = pd.DataFrame(result)
            print(df)
            df.drop(['_start_dt', '_type', '_cnt', '_skip_dt', '_end_dt'], axis=1, inplace=True)
            # df['amt_change'] = df['amt_change'].apply(lambda x: 0 if x == '-' else x)
            # df['amt_change'] = df['amt_change'].apply(lambda x: 0 if x == '-' else x)
            df.replace('-', 0, inplace=True)
            for index, record in df.iterrows():
                print(record)
                bond = ConvBondHistory(**record.to_dict())
                bond.save()
            return df
        else:
            logger.info('->下载数据失败或数据为空！')
            return None

    def downConvBondDisplyData(self, bond_id, displayType):
        '''
        获取可转债详情
        :return: 获取可转债详情
        '''

        urldata = {'bond_id': bond_id, 'display': displayType, }

        urldata = urlencode(urldata)

        url = 'https://www.jisilu.cn/data/cbnew/detail_pic/?' + urldata
        headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36', 'x-requested-with': 'XMLHttpRequest',
            'x-requested-with': 'XMLHttpRequest',
        }

        req = urllib.request.Request(url=url, headers=headers)
        result = self.opener.open(req)
        result = result.read()
        result = gzip.decompress(result)
        result = result.decode()  # .encode('gbk', 'ignore').decode('gbk')
        result = json.loads(result)

        if len(result) > 0:
            logger.info('->下载数据成功！')
            df = pd.DataFrame(result)
            return df

        else:
            logger.info('->下载数据失败或数据为空！')
            return None


if __name__ == '__main__':

    func = 2
    jisilu = 集思录(debug=True)

    if not jisilu.logined:
        jisilu.login()

    # 下载可转债日历
    if func == 1:
        jisilu.downCalendaData(DataType.可转债)

    # 从数据库查询可转债日历数据
    if func == 2:
        bonds: List[ConvBondCalendar] = ConvBondCalendar.objects.all()
        df = pd.DataFrame([x.to_mongo() for x in bonds])
        df.sort_values('start',inplace=True)
        df.reset_index(drop=True,inplace=True)
        df.drop('_id',axis=1,inplace=True)
        print(df)

    # 从mongodb数据库按条件查询可转债日历数据
    if func == 3:
        bonds: List[ConvBondCalendar] = ConvBondCalendar.queryBondCalendar(bytext=None, start='20230203', end='20230303')
        df = pd.DataFrame(bonds.as_pymongo())
        if len(df) > 0:
            df['title'] = df['title'].apply(lambda x: joinword(x))
        pprint(df)

    # 从数据库查询可转债日历
    if func == 4:
        bonds = ConvBondCalendar.getBondCalendar(code='CNV26358', start='2023-02-20')
        print(bonds.to_mongo())

    # 下载可转债历史数据
    if func == 5:
        data = jisilu.downHistoryData('123125')
        pprint(data)

    # 下载全部可转债
    if func == 6:
        bonds = jisilu.downConvBond(update=True)
        print(bonds)

    # 下载可转债详情数据
    if func == 7:
        for bond in ConvBond.objects.all():
            jisilu.downBondDetail(bond.bond_id)

    # 从mongodb数据库查询全部可转债
    if func == 8:
        bonds: List[ConvBond] = ConvBond.objects.all()
        df = pd.DataFrame(bonds.as_pymongo())
        df.sort_values('price', inplace=True, ascending=False)
        df.reset_index(drop=True, inplace=True)
        print(df)
    # 获得可转债显示数据
    if func == 9:
        bonds = jisilu.downConvBondDisplyData('123125', DisplyType.转股价值强赎)
        pprint(bonds)



