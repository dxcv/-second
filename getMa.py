from chgAdjust import *
from parameter import *
"""分钟数据过滤处理"""
def getMa(freq, ee):
    con = dictCon[freq]
    putLogEvent(ee, "检查CTA{}均值数据".format(freq))
    for eachGoods in dictGoodsName.keys():
        putLogEvent(ee, "检查CTA{} {}均值数据".format(freq, eachGoods))
        df = pd.read_sql('select trade_time from {} order by trade_time desc limit 1'.format(dictGoodsName[eachGoods] + '_均值表'), con).set_index('trade_time')
        if df.shape[0] > 0:
            startTime = df.index[0]
        else:
            startTime = theStartTime
        end_time = pd.read_sql('select trade_time from {} order by trade_time desc limit 1'.format(dictGoodsName[eachGoods] + '_调整表'), con).set_index('trade_time').index[0]
        dfFreqAll = getBar(freq, dictGoodsName[eachGoods] + '_调整表', con, theStartTime, end_time)
        dfFreqAll['amt'] = dfFreqAll['close'] * dfFreqAll['volume']
        dfFreqResult = dfFreqAll.copy()
        dfAdjustAll = dictGoodsAdj[eachGoods].copy()
        for mvl in mvlenvector:
            dfFreq = dfFreqAll[
                      max(dfFreqAll[dfFreqAll.index <= startTime].shape[0] - mvl + 1, 0):]
            dfFreq['maprice_{}'.format(mvl)] = 0.0
            dfFreq['stdprice_{}'.format(mvl)] = 0.0
            dfFreq['stdmux_{}'.format(mvl)] = 0.0
            dfFreq['highstdmux_{}'.format(mvl)] = 0.0
            dfFreq['lowstdmux_{}'.format(mvl)] = 0.0
            dfFreqResult['maprice_{}'.format(mvl)] = 0.0
            dfFreqResult['stdprice_{}'.format(mvl)] = 0.0
            dfFreqResult['stdmux_{}'.format(mvl)] = 0.0
            dfFreqResult['highstdmux_{}'.format(mvl)] = 0.0
            dfFreqResult['lowstdmux_{}'.format(mvl)] = 0.0
            dfAdj = dfAdjustAll[
                (dfAdjustAll['adjdate'] > dfFreq.index[0]) & (dfAdjustAll['adjdate'] < end_time)]
            if dfAdj.shape[0] > 0:
                dfFreqResult.update(getMaStdGeneral(dfFreq[dfFreq.index < dfAdj['adjdate'][0]].copy(), mvl))
                for eachNum in range(dfAdj.shape[0]):
                    loc = dfFreq[dfFreq.index < dfAdj['adjdate'][eachNum]].shape[0]  # 调整时刻位置
                    locLeft = max(loc - mvl + 1, 0)
                    dfFreq['close'][locLeft:loc] = dfFreq['close'][locLeft:loc] + \
                                                          dfAdj['adjinterval'][eachNum]
                    dfFreq['amt'][locLeft:loc] = dfFreq['close'][locLeft:loc] * dfFreq['volume'][
                                                                                              locLeft:loc]
                    if eachNum != dfAdj.shape[0] - 1:
                        loc_before = dfFreq[dfFreq.index < dfAdj['adjdate'][eachNum + 1]].shape[0]  # 调整时刻位置
                        dfFreqResult.update(getMaStdGeneral(dfFreq[locLeft:loc_before].copy(), mvl))
                    else:
                        dfFreqResult.update(getMaStdGeneral(dfFreq[locLeft:].copy(), mvl))
            else:
                dfFreqResult.update(getMaStdGeneral(dfFreq.copy(), mvl))
        dfFreqResult = dfFreqResult[dfFreqResult.index > startTime]
        dfFreqResult = dfFreqResult.drop(['volume', 'amt', 'oi'], axis=1)
        dfFreqResult.to_sql(dictGoodsName[eachGoods] + '_均值表', con, if_exists='append', index=True,
                              schema='cta{}_trade'.format(freq))

def getMaStdGeneral(dfFreq, mvl):
    dfFreq['maprice_{}'.format(mvl)] = dfFreq['amt'].rolling(mvl).sum() / dfFreq['volume'].rolling(mvl).sum()
    dfFreq['stdprice_{}'.format(mvl)] = dfFreq['close'].rolling(mvl).std()
    dfFreq['stdmux_{}'.format(mvl)] = (dfFreq['close'] - dfFreq['maprice_{}'.format(mvl)]) / dfFreq['stdprice_{}'.format(mvl)]
    dfFreq['highstdmux_{}'.format(mvl)] = (dfFreq['high'] - dfFreq['maprice_{}'.format(mvl)]) / dfFreq['stdprice_{}'.format(mvl)]
    dfFreq['lowstdmux_{}'.format(mvl)] = (dfFreq['low'] - dfFreq['maprice_{}'.format(mvl)]) / dfFreq['stdprice_{}'.format(mvl)]
    return dfFreq[['maprice_{}'.format(mvl),
                    'stdprice_{}'.format(mvl),
                    'stdmux_{}'.format(mvl),
                    'highstdmux_{}'.format(mvl),
                    'lowstdmux_{}'.format(mvl)]][mvl - 1:]

if __name__ == '__main__':
    freq = 5
    ee = EventEngine()
    ee.start(timer=False)
    con = create_engine('mysql+pymysql://root:rd008@localhost:3306/?charset=utf8').connect()
    getDictGoodsChg()
    print(dict)
