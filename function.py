from py_ctp.eventEngine import  *
from py_ctp.eventType import  *
from py_ctp.ctp_struct import *
from datetime import *
import pandas as pd
import numpy as np

def putLogEvent(ee, log):
    event = Event(type_=EVENT_LOG)
    event.dict_['log'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "......" + log
    ee.put(event)

def putLogTickEvent(ee,log):
    event = Event(type_=EVENT_LOGTICK)
    event.dict_['log'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "......" + log
    ee.put(event)

def putLogBarDealEvent(ee, log, freq):
    event = Event(type_=EVENT_LOGBARDEAL)
    event.dict_['freq'] = freq
    event.dict_['log'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "......" + log
    ee.put(event)

def insertDbChg(dict):  # 将一些 np.int64 与 np.float64 与 tslib.Timestamp 的类型转化成常规类型
    for each in dict.keys():
        if isinstance(dict[each], np.int64):
            dict[each] = int(dict[each])
        elif isinstance(dict[each], np.float64):
            dict[each] = float(dict[each])
        elif isinstance(dict[each], pd._libs.tslib.Timestamp):
            dict[each] = dict[each].to_datetime()
    return dict

def changePriceLine(price, MinChangUnit, DuoOrKong, OpenOrClose):  # 将价格进行最小刻度的四舍五入操作
    if DuoOrKong == '多':
        if OpenOrClose in ['止盈', '开仓']:
            return round(price * (1 / MinChangUnit)) * MinChangUnit if round(
                price * (1 / MinChangUnit)) * MinChangUnit > price else round(
                price * (1 / MinChangUnit)) * MinChangUnit + MinChangUnit
        else:
            return round(price * (1 / MinChangUnit)) * MinChangUnit if round(
                price * (1 / MinChangUnit)) * MinChangUnit < price else round(
                price * (1 / MinChangUnit)) * MinChangUnit - MinChangUnit
    else:
        if OpenOrClose in ['止盈', '开仓']:
            return round(price * (1 / MinChangUnit)) * MinChangUnit if round(
                price * (1 / MinChangUnit)) * MinChangUnit < price else round(
                price * (1 / MinChangUnit)) * MinChangUnit - MinChangUnit
        else:
            return round(price * (1 / MinChangUnit)) * MinChangUnit if round(
                price * (1 / MinChangUnit)) * MinChangUnit > price else round(
                price * (1 / MinChangUnit)) * MinChangUnit + MinChangUnit

def getBar(freq, tableName, con, startTime = '2018-01-01', endTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")):
    sql = "select * from cta{}_trade.{} where trade_time > '{}' and trade_time <= '{}' order by trade_time".format(freq, tableName, startTime, endTime)
    df = pd.read_sql(sql, con)
    df = df.drop(['id'], axis=1).set_index('trade_time')
    return df
