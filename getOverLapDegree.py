from datetime import *
import pandas as pd
from sqlalchemy import create_engine
from function import *
from parameter import *
import numpy as np
from chgAdjust import *
"""
精确地去计算重叠度吧
"""
def getOverLapDegree(freq, ee):
    con = dictCon[freq]
    putLogEvent(ee, "检查CTA{}重叠度数据".format(freq))
    for eachGoods in dictGoodsName.keys():
        putLogEvent(ee, "检查CTA{}中{}重叠度数据".format(freq, eachGoods))
        df = pd.read_sql('select trade_time from {} order by trade_time desc limit 1'.format(dictGoodsName[eachGoods] + '_重叠度表'), con).set_index('trade_time')
        if df.shape[0] > 0:
            startTime = df.index[0]
        else:
            startTime = theStartTime
        # endTime 为均值表的最后一条数据
        endTime = pd.read_sql('select trade_time from {} order by trade_time desc limit 1'.format(dictGoodsName[eachGoods] + '_均值表'), con).set_index('trade_time').index[0]
        dfMaAll = getBar(freq , dictGoodsName[eachGoods] + '_均值表',con ,theStartTime, endTime)
        dfMaAll = dfMaAll.drop(['open'], axis = 1)
        # 创建重叠度表格
        dfOverLap = dfMaAll[['goods_code', 'goods_name', 'high', 'low', 'close']]
        # 获取调整时刻表
        dfAdjustAll = dictGoodsAdj[eachGoods].copy()
        for mvl in mvlenvector:
            dfMa = dfMaAll[max(dfMaAll[dfMaAll.index <= startTime].shape[0] - mvl + 1, 0):]
            dfMa['StdMux高均值_{}'.format(mvl)] = np.nan
            dfMa['重叠度高_{}'.format(mvl)] = np.nan
            dfMa['StdMux低均值_{}'.format(mvl)] = np.nan
            dfMa['重叠度低_{}'.format(mvl)] = np.nan
            dfMa['StdMux收均值_{}'.format(mvl)] = np.nan
            dfMa['重叠度收_{}'.format(mvl)] = np.nan
            dfMa['重叠度高收益_{}'.format(mvl)] = np.nan
            dfMa['重叠度低收益_{}'.format(mvl)] = np.nan
            dfMa['重叠度收收益_{}'.format(mvl)] = np.nan
            dfOverLap['StdMux高均值_{}'.format(mvl)] = np.nan
            dfOverLap['重叠度高_{}'.format(mvl)] = np.nan
            dfOverLap['StdMux低均值_{}'.format(mvl)] = np.nan
            dfOverLap['重叠度低_{}'.format(mvl)] = np.nan
            dfOverLap['StdMux收均值_{}'.format(mvl)] = np.nan
            dfOverLap['重叠度收_{}'.format(mvl)] = np.nan
            dfOverLap['重叠度高收益_{}'.format(mvl)] = np.nan
            dfOverLap['重叠度低收益_{}'.format(mvl)] = np.nan
            dfOverLap['重叠度收收益_{}'.format(mvl)] = np.nan
            dfAdj = dfAdjustAll[(dfAdjustAll['adjdate'] > dfMa.index[0]) & (dfAdjustAll['adjdate'] < endTime)]
            if dfAdj.shape[0] > 0:
                dfOverLap.update(getOverLapGeneral(dfMa[dfMa.index < dfAdj['adjdate'][0]].copy(), mvl))
                for each_num in range(dfAdj.shape[0]):
                    loc = dfMa[dfMa.index < dfAdj['adjdate'][each_num]].shape[0]
                    locLeft = max(loc - mvl + 1, 0)
                    dfMa['close'][locLeft:loc] = dfMa['close'][locLeft:loc] + dfAdj['adjinterval'][each_num]
                    dfMa['high'][locLeft:loc] = dfMa['high'][locLeft:loc] + dfAdj['adjinterval'][each_num]
                    dfMa['low'][locLeft:loc] = dfMa['low'][locLeft:loc] + dfAdj['adjinterval'][each_num]
                    if each_num != dfAdj.shape[0] - 1:
                        loc_before = dfMa[dfMa.index < dfAdj['adjdate'][each_num + 1]].shape[0]
                        dfOverLap.update(getOverLapGeneral(dfMa[locLeft:loc_before].copy(), mvl))
                    else:
                        dfOverLap.update(getOverLapGeneral(dfMa[locLeft:].copy(), mvl))
            else:
                dfOverLap.update(getOverLapGeneral(dfMa.copy(), mvl))
        dfOverLap = dfOverLap[dfOverLap.index > startTime]
        dfOverLap.to_sql(dictGoodsName[eachGoods] + '_重叠度表', con, if_exists='append', index=True,
                              schema='cta{}_trade'.format(freq))

def overLapHigh(t, s0, s1, mvl):
    num = mvl // 10
    time_index = s0.index.get_loc(t)
    highstd = s0[time_index - mvl + 1:time_index + 1].sort_values(ascending=False, kind = 'mergesort')[:num]
    high = s1[time_index - mvl + 1:time_index + 1].sort_values(ascending=False, kind = 'mergesort')[:num]
    len_high = len(highstd.index.intersection(high.index))
    if (highstd < 0).all():
        return -100
    else:
        return round(len_high / num, 2)

def overLapLow(t, s0, s1, mvl):
    num = mvl // 10
    time_index = s0.index.get_loc(t)  # 获取位置
    lowstd = (s0[time_index - mvl + 1:time_index + 1] * (-1)).sort_values(ascending=False, kind='mergesort')[:num]
    low = (s1[time_index - mvl + 1:time_index + 1] * (-1)).sort_values(ascending=False, kind='mergesort')[:num]
    len_low = len(lowstd.index.intersection(low.index))
    if (lowstd * (-1) > 0).all():
        return -100
    else:
        return round(len_low / num, 2)

def overLapClose(t, s0, s1, mvl):
    num = mvl // 10
    time_index = s0.index.get_loc(t)
    closestd = s0[time_index - mvl + 1:time_index + 1].sort_values(ascending=False, kind='mergesort')[:num]
    close = s1[time_index - mvl + 1:time_index + 1].sort_values(ascending=False, kind='mergesort')[:num]
    len_close = len(closestd.index.intersection(close.index))
    if (s0[time_index - mvl + 1:time_index + 1].sort_index(ascending=False)[:num] < 0).all():
        return -100
    else:
        return round(len_close / num, 2)

def getOverLapGeneral(dfMa, mvl):
    if dfMa[mvl - 1:].shape[0] > 0:
        dfMa['重叠度高_{}'.format(mvl)][mvl - 1:] = pd.Series(dfMa.index)[mvl - 1:].apply(overLapHigh, args = (dfMa['highstdmux_{}'.format(mvl)], dfMa['high'], mvl))
        dfMa['重叠度低_{}'.format(mvl)][mvl - 1:] = pd.Series(dfMa.index)[mvl - 1:].apply(overLapLow, args = (dfMa['lowstdmux_{}'.format(mvl)], dfMa['low'], mvl))
        dfMa['重叠度收_{}'.format(mvl)][mvl - 1:] = pd.Series(dfMa.index)[mvl - 1:].apply(overLapClose, args = (dfMa['stdmux_{}'.format(mvl)], dfMa['close'], mvl))
    return dfMa[['重叠度高_{}'.format(mvl), '重叠度低_{}'.format(mvl), '重叠度收_{}'.format(mvl)]][mvl - 1:]

if __name__ == '__main__':
    freq = 6
    ee = EventEngine()
    ee.start(timer=False)
    dictGoodsName = {'a.DCE':'豆一'}
    con = create_engine('mysql+pymysql://root:rd008@localhost:3306/?charset=utf8').connect()
    getDictGoodsChg()
    getOverLapDegree(freq, ee)
