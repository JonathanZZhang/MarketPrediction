# coding:utf-8
import math
import re
from enum import Enum

from mongoengine import *
import datetime
from stock.SecuritiesDateProcess.commonutils import fenci


# 可转债文档定义
class ConvBond(DynamicDocument):
    bond_id = StringField(max_length=6, primary_key=True)  # 可转债编号
    bond_nm = StringField(null=True, default="")  # 可转债名称
    bond_py = StringField(null=True, default="")  # 可转债拼音
    price = FloatField(null=True, default=0)  # 可转债现价
    increase_rt = FloatField(null=True, default=0)  # 可转债涨跌幅
    stock_id = StringField(null=True, default="")  # 正股代码
    stock_nm = StringField(null=True, default="")  # 正股名称
    stock_py = StringField(null=True, default="")  # 正股拼音
    sprice = FloatField(null=True, default=0)  # 正股价格
    sincrease_rt = FloatField(null=True, default=0)  # 正股涨跌幅
    pb = FloatField(null=True, default=0)  # 正股PB
    convert_price = FloatField(null=True, default=0)  # 转股价
    convert_value = FloatField(null=True, default=0)  # 转股价值
    convert_dt = DateField(null=True)  # 转股日期
    premium_rt = FloatField(null=True, default=0)  # 溢价率
    dblow = FloatField(null=True, default=0)  # 双低
    sw_cd = StringField(null=True, default="")  # sw_cd
    market_cd = StringField(null=True, default="")  # market_cd
    btype = StringField(null=True, default="")  # btype
    list_dt = DateField(null=True)  # 上市日
    qflag2 = StringField(null=True, default="")  # qflag2
    owned = IntField(null=True, default=0)  # owned
    hold = IntField(null=True, default=0)  # hold
    bond_value = StringField(null=True, default="")  # bond_value
    rating_cd = StringField(null=True, default="")  # 债券评级
    option_value = StringField(null=True, default="")  # option_value
    put_convert_price = FloatField(null=True, default=0)  # 回售触发价
    force_redeem_price = FloatField(null=True, default=0)  # 强赎触发价
    convert_amt_ratio = FloatField(null=True, default=0)  # 转债占比 转债余额/总市值，转债流通市值占比
    fund_rt = StringField(null=True, default="")  # fund_rt
    short_maturity_dt = DateField(null=True)  # 到期日
    year_left = FloatField(null=True, default=0)  # 剩余年限
    curr_iss_amt = FloatField(null=True, default=0)  # 剩余规模
    volume = FloatField(null=True, default=0)  # 成交量
    svolume = FloatField(null=True, default=0)  # 成交金额
    turnover_rt = FloatField(null=True, default=0)  # 换手率
    ytm_rt = FloatField(null=True, default=0)  # 到期税前收益率
    put_ytm_rt = StringField(null=True, default="")  # 回售收益率
    noted = IntField(null=True, default=0)  # noted
    bond_nm_tip = StringField(null=True, default="")  # 转债提示
    redeem_icon = StringField(null=True, default="")  # redeem_icon
    last_time = StringField(null=True, default="")  # 最后更新时间
    qstatus = StringField(null=True, default="")  # qstatus
    margin_flg = StringField(null=True, default="")  # margin_flg
    sqflag = StringField(null=True, default="")  # sqflag
    pb_flag = StringField(null=True, default="")  # pb_flag
    adj_cnt = IntField(null=True, default=0)  # 下修次数
    adj_scnt = IntField(null=True, default=0)  # adj_scnt
    convert_price_valid = StringField(null=True, default="")  # convert_price_valid
    convert_price_tips = StringField(null=True, default="")  # 转债价格提示
    convert_cd_tip = StringField(null=True, default="")  # 转股代码
    ref_yield_info = StringField(null=True, default="")  # 参考产量信息
    adjusted = StringField(null=True, default="")  # adjusted
    orig_iss_amt = FloatField(null=True, default=0)  # 发行规模
    price_tips = StringField(null=True, default="")  # 价格提示
    redeem_dt = DateField(null=True, default="")  # 最后交易日
    real_force_redeem_price = FloatField(null=True)  # 赎回价
    option_tip = StringField(null=True, default="")  # 可选提示
    notes = StringField(null=True, default="")  # notes
    redeem_price = FloatField(default=0)  # 到期赎回价
    cvt_rt = FloatField(default=0)  # 已转股比例
    convert_amt_ratio2 = FloatField(default=0)  # 转债市值占比
    total_market_value_stock = FloatField(default=0)  # 正股总市值（亿元）
    total_outstanding_market_value_stock = FloatField(default=0)  # 正股总市值（亿元）
    threshold_value = FloatField(default=0)  # 下修触发价
    adjust_condition = StringField(default='')  # 下修天计数
    redeem_status = StringField(default='')  # 强赎天计数
    stockholders_placing_ratio = FloatField(default=0)  # 股东配售率
    winning_rate = FloatField(default=0)  # 中签率
    objects: QuerySet
    meta = {
        'collection': 'convbond',
    }

    @queryset_manager
    def getBond(doc_cls, queryset, bond_id):
        '''
        根据证券编号和日期获得K线数据
        :param code：可转债代码
        :param start 开始日期
        :return: 返回可转债日历
        '''
        return queryset.filter(Q(bond_id=bond_id)).first()

    def calculate_convert_value(self):
        '''
        根据转股价、正股股价和可转债现价计算转股价值
        convert_price 转股价
        sprice 正股股价
        :return:
        '''
        assert self.convert_price > 0, '计算转股价值"转股价"必须>0!'
        assert self.sprice > 0, '计算转股价值"正股股价"必须>0!'

        return 100 / self.convert_price * self.sprice

    def calculate_premium_rt(self):
        '''
        根据转股价和正股股价计算转股溢价率
        convert_price 转股价
        sprice 正股股价
        price 可转债现价

        :return:
        '''
        assert self.convert_price > 0, '计算转股价值"转股价"必须>0!'
        assert self.sprice > 0, '计算转股价值"正股股价"必须>0!'
        assert self.price > 0, '计算溢价率，必须提供“转债价格”！'
        convert_value = self.calculate_convert_value()
        return self.price / convert_value - 1

    def calculateActualEarningsFromStockTransfer(self, numberHeldBond):
        '''
        计算转股实际收益
        price 可转债现价
        numberHeldBond 可转债张数
        sprice 正股股价
        convert_price 转股价
        :return:
        '''
        assert self.price > 0, '计算转股收益_实际"转债价格"必须>0!'
        assert numberHeldBond > 0, '计算转股收益_实际"转债张数"必须>0!'
        assert self.convert_price > 0, '计算转股价值"转股价"必须>0!'

        convStockNum, remainder = divmod(100 * numberHeldBond, self.convert_price)
        cash = remainder * self.sprice
        return self.sprice * convStockNum + cash - self.price * self.numberHeldBond

    def estimateEarningsFromStockTransfer(self, numberHeldBond):
        '''
        估算转股收益
        price 可转债现价
        numberHeldBond 可转债张数
        sprice 正股股价
        convert_price 转股价
        :return:
        '''
        assert self.price > 0, '计算转股收益_估算“转债价格”必须大于0！'
        assert numberHeldBond > 0, '计算转股收益_估算“转债张数”必须大于0！'

        if self.price and self.numberHeldBond:
            convert_value = self.calculate_convert_value()
            return (convert_value - self.price) * numberHeldBond

    #
    def CalculateMinNumOfAllottedInShanghai(self, allotmentPerShare):
        '''
        计算沪市最上配售股票数
        allotmentPerShare 每股获配额
        sprice 正股股价
        :return:
        '''
        assert allotmentPerShare > 0, '计算最小配售股票数“每股获配额”必须大于0！'
        assert self.sprice > 0, '计算最小配售股票数“正股股价”必须大于0！'

        n = 100
        while True:
            k = n * allotmentPerShare / 1000
            if k > 0.5:
                return n, n * self.sprice
            else:
                n += 100

    def CalculateMinNumOfStockAndMarketValue(self, allotmentPerShare):
        '''
        计算股东配债千元最低配债低仓股数及市值
        allotmentPerShare 每股获配额
        sprice 正股股价
        NumberOfBondsIssued 配债股数
        MarketValueOfBondsIssued 配债市值
        :return:
        '''
        assert allotmentPerShare > 0, '计算股东配债“每股获配额”必须大于0！'
        assert self.sprice > 0, '计算股东配债“正股股价”必须大于0！'

        NumberOfBondsIssued = math.ceil(1000 / allotmentPerShare)
        MarketValueOfBondsIssued = NumberOfBondsIssued * self.sprice
        return NumberOfBondsIssued, MarketValueOfBondsIssued

    def calculateBondCushion(self, expectedOpeningPrice, allotmentPerShare, exchange='sh'):
        '''
        计算配债安全垫
        预期开盘价 expectedOpeningPrice
        :param exchange:
        :return:
        '''
        assert expectedOpeningPrice > 0, '计算配债安全垫“预期开盘价”必须大于0！'
        if exchange == 'sh':
            allotment1 = self.CalculateMinNumOfAllottedInShanghai(allotmentPerShare)
            allotment2 = self.CalculateMinNumOfStockAndMarketValue(allotmentPerShare)
            return round((self.expectedOpeningPrice - 100) * 10 / allotment1[1] * 100, 2), round((self.expectedOpeningPrice - 100) * 10 / allotment2[1] * 100, 2)
        else:
            allotment1 = self.CalculateMinNumOfStockAndMarketValue(allotmentPerShare)
            return round((self.expectedOpeningPrice - 100) * 10 / allotment1[1] * 100, 2)

    def 配售收益及综合收益(self, allotmentPerShare, expectedOpeningPrice):
        '''
        计算每股配售收益及综合收益
        预期开盘价 expectedOpeningPrice
        sprice 正股股价
        :return:
        '''
        assert expectedOpeningPrice > 0, '计算配售收益及综合收益“预期开盘价”必须大于0！'
        assert self.sprice > 0, '计算配售收益及综合收益“正股股价”必须大于0！'
        allotment1 = self.CalculateMinNumOfStockAndMarketValue(allotmentPerShare)
        PlacementEarningsPerShare = round((expectedOpeningPrice - 100) * 10 / allotment1[1], 2)
        consolidatedPlacementYield = f'{round(PlacementEarningsPerShare/self.sprice*100,2)}%'
        return (PlacementEarningsPerShare, consolidatedPlacementYield)

    def calculateTheWeight(self, allotmentPerShare):
        '''
        计算含权量
        allotmentPerShare 每股获配额
        sprice 正股股价
        :return:
        '''
        assert allotmentPerShare > 0, '计算含权量“每股获配额”必须大于0！'
        assert self.sprice > 0, '计算含权量“正股股价”必须大于0！'

        return round(allotmentPerShare / self.sprice * 100, 2)

    def calculateTheWinningRate(self, sharePlacementRatio, subscriptionAmount):
        '''
        计算中签率
        orig_iss_amt 发行规模
        sharePlacementRatio 股东配售率
        subscriptionAmount 申购金额
        :return:
        '''
        assert self.orig_iss_amt > 0, '计算中签率“发行金额”必须大于0！'
        assert sharePlacementRatio > 0, '计算中签率“股东配售率”必须大于0！'
        assert subscriptionAmount > 0, '计算中签率“申购金额”必须大于0！'

        return round(self.orig_iss_amt * (100 - sharePlacementRatio) / 100 / subscriptionAmount * 100, 6)

    def calculateConvBondWinning(drawlots, startingNumber, numberOfSubscriptions=1000):
        '''
        计算中签配号结果
        :param drawlots：抽签结果
        晶科配号中签结果 =
            末尾位数
            中签号码
            末"4"位数 4720, 9720
            末"5"位数 0558, 60558, 40558, 20558, 00558
            末"6"位数 04798, 954798, 454798, 204798
            末"8"位数 08308223, 33308223, 58308223, 83308223
            末"9"位数 871041746, 621041746, 371041746, 121041746
            末"10"位数 5429606982, 1970440181, 6194318456

        :param startingNumber: 配号
        [103614678241, 103574175631, 103799298112, 103543312131, 103524500890]
        :param numberOfSubscriptions: 申购数量，默认1000
        :return:
        '''
        drawlotsList = []
        bznum = re.findall(r'(\d+)[,，\s]', drawlots, re.M)
        if isinstance(startingNumber, int):
            for i in range(numberOfSubscriptions):
                myNum = str(startingNumber + i)
                for k in bznum:
                    if myNum.endswith(k):
                        drawlotsList.append((myNum, startingNumber))

        if isinstance(startingNumber, list):
            if isinstance(numberOfSubscriptions, int):
                for n in startingNumber:
                    for i in range(numberOfSubscriptions):
                        myNum = str(n + i)
                        for k in bznum:
                            if myNum.endswith(k):
                                drawlotsList.append((myNum, n))

            elif isinstance(numberOfSubscriptions, list):
                assert len(startingNumber) == len(numberOfSubscriptions), '起始配号数组长度与配号数数组不相等！'
                for index, number in enumerate(startingNumber):
                    for i in range(numberOfSubscriptions[index]):
                        myNum = str(number + i)
                        for k in bznum:
                            if myNum.endswith(k):
                                drawlotsList.append((myNum, number))
            if len(drawlotsList) == 0:
                return '未中签'
            else:
                return f'中签{len(drawlotsList)}签，中签号码{drawlotsList}'


# 可转债日历文档定义
class ConvBondCalendar(Document):
    code = StringField(required=True)  # 可转债转换代码
    symbol = StringField(required=True)  # 转债代码
    title = StringField(null=True, default="")  # 标题
    start = DateField(required=True, unique_with=['code'])  # 开始日期
    description = StringField(null=True, default="")  # 说明
    url = StringField(null=True, default="")  # 详情网址
    objects: QuerySet

    meta = {
        'collection': 'ConvBondCalendar',  #
        'indexes': [{
            'fields': ['$title'],
            'default_language': 'english',
            'weights': {'title': 10}
        }]
    }

    @queryset_manager
    def getBondCalendar(doc_cls, queryset, code, start):
        '''
        根据证券编号和日期获得K线数据
        :param code：可转债代码
        :param start 开始日期
        :return: 返回可转债日历
        '''
        return queryset.filter(Q(code=code) & Q(start=start)).first()

    @queryset_manager
    def queryBondCalendar(doc_cls, queryset, symbol=None, startDate=None, endDate=None, bytext=None):
        '''
        根据证券编号获得K线数据
        :param symbol:可转债代码
        :param startDate:起始日期
        :param endDate:结束日期
        :param bytext:查询文本
        :return:
        '''

        now = datetime.datetime.now()
        if startDate == None:
            startDate = now
        if endDate == None:
            endDate = now + datetime.timedelta(days=30)
        result = queryset.filter(start__gte=startDate, start__lte=endDate)
        if symbol:
            result = result.filter(symbol=symbol)
        if bytext:
            bytext = fenci(bytext)
            result = result.search_text(bytext).order_by('$text_score')
        return result


# 可转债历史数据文档
class ConvBondHistory(Document):
    bond_id = StringField()  # 转债代码
    last_chg_dt = StringField(null=True, default="")  # 最后转换日期
    ytm_rt = StringField(null=True, default="")  # 税前到期收益率
    premium_rt = StringField(null=True, default="")  # 溢价率
    convert_value = StringField(null=True, default="")  # 转股价值
    price = StringField(null=True, default="")  # 现价
    volume = StringField(null=True, default="")  # 成交量
    stock_volume = IntField(null=True, default=0)  # 股票数量
    curr_iss_amt = StringField(null=True, default="")  # 剩余规模
    cflg = StringField(null=True, default="")  # cflg
    amt_change = FloatField(null=True, default=0)  # amt_change
    turnover_rt = FloatField(null=True, default=0)  # 换手率

    objects: QuerySet

    meta = {
        'collection': 'ConvBondHistory',  #
    }


# 转债英文属性转中文属性
en2cn = {
    'bond_id': '转债代码',  #
    'bond_nm': '转债名称',  #
    'bond_nm_tip': '转债提示',  #
    'bond_py': '转债拼音',  #
    'convert_amt_ratio': '流通市值占比',  # 1.7,
    'convert_cd_tip': '转股提示',  #
    'convert_price': '转股价',  # 14.54,
    'convert_price_tips': '转债价格提示',  #
    'convert_value': '转股价值',  # 189.55,
    'curr_iss_amt': '剩余规模',  # 1.716,
    'dblow': '双低',  # 186.88,双低= 转债价格+ 溢价率*100
    'force_redeem_price': '强赎触发价',  # 18.9,stock_volume
    'increase_rt': '涨幅',  # -4.23,
    'last_time': '更新时间',  # '15:34:42',
    'list_dt': '上市日',  # '2020-08-17',
    'option_tip': '可选提示',  # ''如不考虑强赎期权价值为113.18元',
    'orig_iss_amt': '发行规模',  # 3.1,
    'pb': '正股PB',  # 5.43,
    'premium_rt': '溢价率',  # -0.92,
    'price': '现价',  # 187.797,
    'price_tips': '价格提示',  # '全价：187.797 最后更新：15:34:42',
    'put_convert_price': '回售触发价',  # 10.18,
    'rating_cd': '债券评级',  # 'AA-',
    'real_force_redeem_price': '实际强赎触发价',  # '100.480',
    'redeem_dt': '最后交易日',  # '2023-01-09',
    'ref_yield_info': '参考产量信息',  # '强赎登记日2023-01-09，赎回价格100.480元，如不考虑强赎纯债价值为93.59元，计算使用3.6年期 评级为AA- 债参考YTM：6.4252',
    'short_maturity_dt': '到期日',  # '26-07-20',
    'sincrease_rt': '正股涨幅',  # -3.94,
    'sprice': '正股价格',  # 27.56,
    'stock_id': '正股代码',  # '300416',
    'stock_nm': '正股名称',  # '苏试试验',
    'stock_py': '正股拼音',  #
    'svolume': '成交金额',  # 14662.38,
    'turnover_rt': '换手率',  # 56.22,
    'volume': '成交量',  # 18271.83,
    'convert_dt': '转股日期',  #
    'sw_cd': 'sw_cd',  #
    'market_cd': 'market_cd',  #
    'btype': 'btype',  #
    'qflag2': 'qflag2',  #
    'owned': 'owned',  #
    'hold': 'hold',  #
    'bond_value': 'bond_value',  #
    'option_value': 'option_value',  #
    'fund_rt': 'fund_rt',  #
    'year_left': '剩余年限',  #
    'ytm_rt': '税前到期收益率',  #
    'put_ytm_rt': '回售收益率',  #
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
    'last_chg_dt': 'last_chg_dt',  #
    'stock_volume': 'stock_volume',  #
    'cflg': 'cflg',  #
    'amt_change': 'amt_change',  #
}

# 转账中文属性转英文属性
cn2en = {
    '转债代码': 'bond_id',  #
    '转债名称': 'bond_nm',  #
    '转债提示': 'bond_nm_tip',  #
    '转债拼音': 'bond_py',  #
    '流通市值占比': 'convert_amt_ratio',  #
    '转股提示': 'convert_cd_tip',  #
    '转股价': 'convert_price',  #
    '转债价格提示': 'convert_price_tips',  #
    '转股价值': 'convert_value',  #
    '剩余规模': 'curr_iss_amt',  #
    '双低': 'dblow',  #
    '强赎触发价': 'force_redeem_price',  #
    '涨幅': 'increase_rt',  #
    '更新时间': 'last_time',  #
    '上市日': 'list_dt',  #
    '可选提示': 'option_tip',  #
    '发行规模': 'orig_iss_amt',  #
    '正股PB': 'pb',  #
    '溢价率': 'premium_rt',  #
    '价格': 'price',  #
    '价格提示': 'price_tips',  #
    '回售触发价': 'put_convert_price',  #
    '债券评级': 'rating_cd',  #
    '赎回价': 'real_force_redeem_price',  #
    '最后交易日': 'redeem_dt',  #
    '参考产量信息': 'ref_yield_info',  #
    '到期日': 'short_maturity_dt',  #
    '正股涨幅': 'sincrease_rt',  #
    '正股价格': 'sprice',  #
    '正股代码': 'stock_id',  #
    '正股名称': 'stock_nm',  #
    '正股拼音': 'stock_py',  #
    '成交金额': 'svolume',  #
    '换手率': 'turnover_rt',  #
    '成交量': 'volume',  #
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
    '到期收益率': 'ytm_rt',  #
    '回售收益率': 'put_ytm_rt',  #
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


# k line type enum
class KType(Enum):
    min1 = '8'
    min5 = '5m'
    min15 = '15m'
    min30 = '30m'
    hour = '1h'
    day = 'day'
    week = 'week'
    month = 'mon'
    quarter = '3mon'
    year = 'year'


# k line type class tuple
class KTypeN:
    Min1 = ('7', '1m')
    Min5 = ('0', '5m')
    Min15 = ('1', '15m')
    Min30 = ('2', '30m')
    Hour1 = ('3', '1h')
    Day = ('9', 'day')
    Days = ('4', 'days')
    Week = ('5', 'week')
    mon = ('6', 'mon')
    Quarter = ('10', '3mon')
    Year = ('11', 'year')


class SecuritiesCodePrefix():
    沪市A股股票 = '60'
    沪市B股股票 = '900'
    沪市新股申购 = '707|730|732|780|787'
    沪市配股 = '700'
    沪市科创板 = '688'

    深市A股股票 = '00'
    深市B股股票 = '200'
    深市配股 = '080'
    深市创业版股票 = '300'
    深市中小板股票 = '002|003'
    创业板注册制股 = '3008|3009'

    连续亏损 = 'ST'
    退市风险 = '*ST'
    分红 = 'XD'


# k bar data document
class BarData(DynamicDocument):
    symbol = StringField(required=True)  # 证券编号
    ktype = EnumField(KType, required=True)  # k线类型
    dt = DateTimeField(required=True, unique_with=['symbol', 'ktype'])  # 日期时间
    open = FloatField(null=True, default=0)  # 开盘价
    close = FloatField(null=True, default=0)  # 收盘价
    high = FloatField(null=True, default=0)  # 最高价
    low = FloatField(null=True, default=0)  # 最低价
    vol = FloatField(null=True, default=0)  # 交易量
    amount = FloatField(null=True, default=0)  # 交易额
    year = IntField(null=True, default=0)  # 年
    month = IntField(null=True, default=0)  # 月
    day = IntField(null=True, default=0)  # 日
    hour = IntField(null=True, default=0)  # 小时
    minute = IntField(null=True, default=0)  # 分钟
    volume = FloatField(null=True, default=0)  #
    objects: QuerySet
    meta = {
        'collection': 'bardata',  # 集合名称
    }

    @queryset_manager
    def getkbar(doc_cls, queryset, symbol, ktype, dt):
        '''
        从mongodb根据证券编号和日期获得K线数据
        :param symbol: 证券编号
        :param ktype: k线类型
        :param datetime: 日期时间
        :return:
        '''
        # datetime = transDate(datetime)
        # assert isinstance(datetime, dt)
        return queryset.filter(Q(symbol=symbol) & Q(ktype=ktype) & Q(dt=dt)).first()

    @queryset_manager
    def querykbar(doc_cls, queryset, symbol, ktype, startDate=datetime.datetime(1900, 1, 1), endDate=datetime.datetime.utcnow()):
        '''
        从mongodb根据证券编号、k线类型、起始日期、结束日期获得K线数据
        :param symbol:证券编号
        :param ktype:k线类型
        :param startDate:起始日期
        :param endDate:结束日期
        :return:返回queryset
        '''
        # startDate = transDate(startDate)
        # assert isinstance(startDate, dt)
        # endDate = transDate(endDate)
        # assert isinstance(endDate, dt)
        result: QuerySet = queryset.filter(Q(symbol=symbol) & Q(ktype=ktype) & Q(dt__gte=startDate) & Q(dt__lte=endDate))
        return result


barsItems = ['open', 'close', 'high', 'low', 'vol', 'amount', 'year', 'month', 'day', 'hour', 'minute', 'dt', 'volume']
realTimeQuotesItems = ['market', 'code', 'active1', 'price', 'last_close', 'open', 'high', 'low', 'servertime', 'reversed_bytes0', 'reversed_bytes1', 'vol', 'cur_vol', 'amount', 's_vol',
                       'b_vol', 'reversed_bytes2', 'reversed_bytes3', 'bid1', 'ask1', 'bid_vol1', 'ask_vol1', 'bid2', 'ask2', 'bid_vol2', 'ask_vol2', 'bid3', 'ask3', 'bid_vol3', 'ask_vol3',
                       'bid4', 'ask4', 'bid_vol4', 'ask_vol4', 'bid5', 'ask5', 'bid_vol5', 'ask_vol5', 'reversed_bytes4', 'reversed_bytes5', 'reversed_bytes6', 'reversed_bytes7',
                       'reversed_bytes8', 'reversed_bytes9', 'active2', 'volume']


class Securities(DynamicDocument):
    '''
    code  volunit  decimal_point      name  pre_close market

    '''
    code = StringField()  # 证券代码
    volunit = IntField()  # 每首股数
    decimal_point = IntField()  # 十进制小数点数
    name = StringField()  # 证券名称
    pre_close = FloatField()  # 前收盘价
    market = IntField(unique_with=['code'])  # 证券市场代码

    objects: QuerySet
    meta = {
        'collection': 'securities',  # 集合名称
    }

    @queryset_manager
    def getSecurities(doc_cls, queryset, code, market):
        '''
        从mongodb根据证券编号和日期获得K线数据
        :param symbol: 证券编号
        :param ktype: k线类型
        :param datetime: 日期时间
        :return:
        '''
        return queryset.filter(Q(code=code) & Q(market=market)).first()

    @queryset_manager
    def querySecuritiesByCode(doc_cls, queryset, code):
        '''
        从mongodb根据证券编号和日期获得K线数据
        :param symbol: 证券编号
        :param ktype: k线类型
        :param datetime: 日期时间
        :return:
        '''
        return queryset.filter(Q(code=code))

    @queryset_manager
    def querySecurities(doc_cls, queryset, market=None, prefix=None):
        '''
        从mongodb根据证券编号和日期获得K线数据
        :param symbol: 证券编号
        :param ktype: k线类型
        :param datetime: 日期时间
        :return:
        '''
        if market and prefix:
            qs = Q(market=market)
            for p in prefix:
                qs = qs | Q(code__startswith=p)
        elif market:
            qs = Q(market=market)
        elif prefix:
            prefix = prefix.split('|')
            qs = Q(code__startswith=prefix[0])
            for p in prefix[1:]:
                qs = qs | Q(code__startswith=p)
        print(qs)
        return queryset.filter(qs)