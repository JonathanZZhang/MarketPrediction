# coding:utf-8
import re

from stock.SecuritiesDateProcess.mootdxhelper import MootdxHelper

from stock.SecuritiesDateProcess.SecuritiesModels import en2cn

from stock.SecuritiesDateProcess.jisiluhelper import 集思录

import pandas as pd
import numpy as np

pd.set_option('expand_frame_repr', False)  # True就是可以换行显示。设置成False的时候不允许换行
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('precision', 2)  # 显示小数点后的位数
pd.set_option('display.width', 200)
pd.set_option('display.float_format', lambda x: '%.2f' % x)  # 为了直观的显示数字，不采用科学计数法
import matplotlib.pyplot as plt
import matplotlib as mpl

mpl.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
mpl.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号-显示为方块的问题


def fun1():
    d = {
        'sw_cd': 'sw_cd',  #
        'market_cd': 'market_cd',  #
        'btype': 'btype',  #
        'qflag2': 'qflag2',  #
        'owned': 'owned',  #
        'hold': 'hold',  #
        'bond_value': 'bond_value',  #
        'option_value': 'option_value',  #
        'fund_rt': 'fund_rt',  #
        'year_left': 'year_left',  #
        'ytm_rt': 'ytm_rt',  #
        'put_ytm_rt': 'put_ytm_rt',  #
        'noted': 'noted',  #
        'redeem_icon': 'redeem_icon',  #
        'qstatus': 'qstatus',  #
        'margin_flg': 'margin_flg',  #
        'sqflag': 'sqflag',  #
        'pb_flag': 'pb_flag',  #
        'adj_cnt': 'adj_cnt',  #
        'adj_scnt': 'adj_scnt',  #
        'convert_price_valid': 'convert_price_valid',  #
        'adjusted': 'adjusted',  #
        'notes': 'notes',  #
    }


def fun2():
    txt = '''
        :authority: www.jisilu.cn
    :method: GET
    :path: /data/calendar/get_calendar_data/?qtype=newstock_onlist&start=1669564800&end=1673193600&_=1671951577369
    :scheme: https
    accept: application/json, text/javascript, */*; q=0.01
    accept-encoding: gzip, deflate, br
    accept-language: zh-CN,zh;q=0.9
    cookie: event_checked=true; event_newstock_onlist=checked; event_newstock_apply=checked; event_CNV=checked; event_cnreits=checked; event_FUND=checked; event_BOND=checked; event_STOCK=checked; event_OTHER=checked; event_newbond_apply=checked; event_newbond_onlist=checked; event_diva=checked; event_divhk=checked; kbzw__Session=flfu2cq1fufvlvd3ee4kpupd05; Hm_lvt_164fe01b1433a19b507595a43bf58262=1671950568; kbz_newcookie=1; Hm_lpvt_164fe01b1433a19b507595a43bf58262=1671951578
    referer: https://www.jisilu.cn/data/calendar/
    sec-fetch-dest: empty
    sec-fetch-mode: cors
    sec-fetch-site: same-origin
    user-agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36
    x-requested-with: XMLHttpRequest

    accept: application/json, text/javascript, */*; q=0.01
    accept-encoding: gzip, deflate, br
    accept-language: zh-CN,zh;q=0.9
    content-length: 12
    content-type: application/x-www-form-urlencoded; charset=UTF-8
    origin: https://www.jisilu.cn
        '''

    txt = txt.replace('"', '')
    txt = re.findall('.*', txt)
    txt = [x.strip() for x in txt if x != '']
    print(txt)
    print(len(txt))
    for p in txt:
        p = p.split(': ')
        print(repr(p[0]), ':', repr(p[1].strip()), ',')


def fun3():
    jisilu = ['bond_id', 'last_chg_dt', 'ytm_rt', 'premium_rt', 'convert_value', 'price', 'volume', 'stock_volume', 'curr_iss_amt', 'cflg',  'amt_change',    'turnover_rt']

    fieldType = ['str', 'str', 'str', 'str', 'str', 'str', 'str', 'int', 'str', 'str', 'float', 'float']

    jisilu = zip(jisilu, fieldType)
    for key, typ in list(jisilu):
        if typ == 'str':
            print(f'{key} = StringField(null=True,default="") # {en2cn[key]}')
        elif typ == 'float':
            print(f'{key} = FloatField(null=True,default=0) # {en2cn[key]}')
        elif typ == 'int':
            print(f'{key} = IntField(null=True,default=0) # {en2cn[key]}')
        else:
            print(f'{key}error!')

class KType:
    Min1 = '1m'
    Min5 = '5m'
    Min15 = '15m'
    Min30 = '30m'
    Hour = '1h'
    Day = 'day'
    Days = 'days'
    Week = 'week'
    mon = 'mon'
    Quarter = '3mon'
    Year = 'year'


def fun4():
    jisilu = 集思录(debug=True)
    # df: pd.DataFrame = joblib.load('data/allbond.pkl')
    # df= jisilu.downHistoryData('123125')
    df =MootdxHelper().getKbars('113615', KType.Hour, offset=700)

    df.fillna('', inplace=True)

    line1: pd.Series = df.iloc[0]
    print(line1.to_list())
    tt1 = [f'{x}:{type(x)}' for x in line1]
    typelist = []
    for it in line1:
        if isinstance(it, str):
            typelist.append('str')
        elif isinstance(it, np.float64):
            typelist.append('float')
        elif isinstance(it, np.int64):
            typelist.append('int')
        elif isinstance(it, int):
            typelist.append('int')
        elif isinstance(it, float):
            typelist.append('float')
        else:
            print('*****', it)
    print(typelist)
    exit()
def fun5():
    txt ='''
    K线种类 0 => Min5 => 5m 1 => Min15 => 15m 2 => Min30 => 30m 3 => Hour1 => 1h 4 => Days => days 5 => Week => week 6 => mon => mon 7 => Min1 => 1m  9 => day => day 10 => Quarter => 3mon 11 => Year => year
    '''
    txt =re.findall('(\d+) => (\w+?) => (\w+)',txt)
    print(txt)
    l=[]
    for t in txt:
        a,b,c = t
        print(b)
        l.append((b,(a,c)))
    print(l)

    for it in l:
        print(f'{it[0]} = {it[1]}')

def fun6():
    col= ['open', 'close', 'high', 'low', 'vol', 'amount', 'year', 'month', 'day', 'hour', 'minute', 'datetime', 'volume']
    typ =['float','float','float','float','float','float','int','int','int','int','int','datetime','float']
    jisilu = zip(col, typ)
    for key, typ in list(jisilu):
        if typ == 'str':
            print(f'{key} = StringField(null=True,default="") # ')
        elif typ == 'float':
            print(f'{key} = FloatField(null=True,default=0) # ')
        elif typ == 'int':
            print(f'{key} = IntField(null=True,default=0) # ')
        elif typ == 'datetime':
            print(f'{key} = DateTimeField(unique=True,null=True) # ')

        else:
            print(f'{key}error!')

def fun7():
    txt ='''
    class Min1KBar(KBar):
    pass


class Min5KBar(KBar):
    pass


class Min15KBar(KBar):
    pass


class Min30KBar(KBar):
    pass


class HourKBar(KBar):
    pass


class DayKBar(KBar):
    pass


class WeekKBar(KBar):
    pass


class MonthKBar(KBar):
    pass


class QuarterKBar(KBar):
    pass


class YearKBar(KBar):
    pass
    '''

    txt  =re.findall('class (\w+)\(KBar',txt)
    print(txt)
    for it in txt:
        print(f'{it[0].lower()+it[1:]} = EmbeddedDocumentListField(Bar)')

def fun8():
    txt ='''
        elif ktype == KType.min1:
            if embbar:
                if isinstance(embbar,list):
                    for bar in embbar:
                        kbar.min1KBar.create(bar)
                else:
                     kbar.min1KBar.create(embbar)
            elif data:
                kbar.min1KBar.create(**data)
            else:
                return 0
                
    '''
    txt2 = '''
    min1 = "1m'
    min5 = '5m'
    min15 = '15m'
    min30 = '30m'
    hour = '1h'
    day = 'day'
    week = 'week'
    month = 'mon'
    quarter = '3mon'
    year = 'year'
    '''
    txt2 = re.findall('(\w+) =',txt2)
    print(txt2)
    for it in txt2:
        t = txt.replace('min1', it)
        print(t)

def fun9():
    a = [1,2,3,4,5,6,7,8]
    b = filter(lambda x:x%2==1,a)
    print([x for x in b])
    print([item for item in filter(lambda x:x%2==1, a)])


def tChesecalendar():
    import chinese_calendar
    import dateutil
    workdays = chinese_calendar.get_workdays(dateutil.parser.parse('20230116'),dateutil.parser.parse('20230131'))
    workdays = [ x for x in workdays if x.isoweekday() not in [6,7] ]
    print(workdays)
    print(len(workdays))
    print(type(workdays[0]))

print(tChesecalendar())
