from getMa import *
from getOverLapDegree import *
from chgAdjust import *
from getWeekTradeTab import *
"""
主要是补充完整的分钟数据，均值，重叠度
"""
def completeDb(ee):
    putLogEvent(ee, "检查chg_data与position是否为最新的数据")
    checkChg()  # 检查chgData是否为最新的数据
    putLogEvent(ee, "更新chg_data与position完成")
    checkCon(ee)

def checkCon(ee):
    checkOneMinBar(ee)  # 检查1分钟数据完整性
    getDictGoodsChg()  # 先检查完1分钟数据，再去获取字典
    for eachFreq in listFreq:
        checkOtherMinBar(eachFreq, ee)  # 检查其它频段数据与调整表
        getMa(eachFreq, ee)
        getOverLapDegree(eachFreq, ee)
        getWeekTradeTab(eachFreq, ee)

def checkOneMinBar(ee):  # 检查1分钟数据完整性
    freq = 1
    putLogEvent(ee, "检查CTA{}数据完整性".format(freq))
    con = dictCon[1]
    for eachGoods in dictGoodsName.keys():
        putLogEvent(ee, "检查CTA{} {}完整性".format(freq, eachGoods))
        # 如果频段数据与调整表数据不相同的话，将调整表数据整个删除，直接使用频段数据即可：
        df1Last = pd.read_sql('select trade_time from {} order by trade_time desc limit 1'.format(dictGoodsName[eachGoods]), con).set_index('trade_time')
        df2Last = pd.read_sql('select trade_time from {} order by trade_time desc limit 1'.format(dictGoodsName[eachGoods] + '_调整表'), con).set_index('trade_time')
        if ((df1Last.shape[0] == df2Last.shape[0] > 0) and (df1Last.index[0] != df2Last.index[0])) or (df1Last.shape[0] > 0 and df2Last.shape[0] == 0):
            df1 = pd.read_sql('select * from {}'.format(dictGoodsName[eachGoods]), con).set_index('trade_time')
            con.execute('truncate table ' + dictGoodsName[eachGoods] + '_调整表')
            df1.to_sql(dictGoodsName[eachGoods] + '_调整表', con, if_exists='append', index=True,
                       schema='cta{}_trade'.format(freq))
        # 读取分钟数据
        dfMinute = pd.read_sql('select trade_time from {} order by trade_time desc limit 1'.format(dictGoodsName[eachGoods] + '_调整表'), con).set_index('trade_time')
        if dfMinute.shape[0] != 0:
            startTime = dfMinute.index[0] + timedelta(minutes=1)
        else:
            startTime = theStartTime
        dfAdj = pd.read_sql('select adjdate from {}_调整时刻表'.format(dictGoodsName[
            eachGoods]) + ' order by adjdate desc limit 1', con)
        df = pd.read_csv('chg_data\\' + eachGoods.upper() + ' chg_data.csv',
                         parse_dates=['adjdate'], encoding='gbk', index_col='id')
        df = df.reset_index(drop=True)
        if eachGoods.split('.')[1] in ['CZC', 'CFE']:
            df['goods_code'] = df['stock']
        else:
            df['goods_code'] = df['stock'].apply(lambda x: x.split('.')[0].lower() + '.' + x.split('.')[1])
        dfChgData = df.copy()
        df = df[['goods_code', 'goods_name', 'adjdate', 'adjinterval']]
        df = df[1:-1]
        if dfAdj.shape[0] == 0:
            df = df.copy()
        else:
            df = df[df['adjdate'] > dfAdj['adjdate'][0]]
        if df.shape[0] > 0:
            df.to_sql(dictGoodsName[eachGoods] + '_调整时刻表', con, if_exists='append', index=False,
                      schema='cta{}_trade'.format(freq))
        dfFreq = pd.DataFrame(
            columns=['trade_time', 'goods_code', 'goods_name', 'open', 'high', 'low', 'close', 'volume', 'amt',
                     'oi']).set_index('trade_time')
        for eachNum in range(dfChgData.shape[0] - 1):
            if eachNum == dfChgData.shape[0] - 2:
                lastEndTime = tradeDay[tradeDay.index(dfChgData['adjdate'][eachNum + 1]) + 1] + timedelta(
                    hours=16)
            else:
                lastEndTime = dfChgData['adjdate'][eachNum + 1] + timedelta(hours=16)
            if dfChgData['adjdate'][eachNum] + timedelta(hours=16) < startTime \
                    and lastEndTime > startTime:
                goods_code = dfChgData['stock'][eachNum]
                if goods_code.split('.')[1] in ['DCE', 'SHF']:
                    goods_code = goods_code.split('.')[0].lower() + '.' + goods_code.split('.')[1]
                windData = w.wsi(goods_code, "open,high,low,close,volume,amt,oi",
                                  startTime, lastEndTime, "")
                if windData.ErrorCode == 0:
                    df_wind = pd.DataFrame(windData.Data, index=['open','high','low','close','volume','amt','oi'], columns=windData.Times)
                    df_wind = df_wind.T
                    df_wind.index.name = 'trade_time'
                    startTime = dfChgData['adjdate'][eachNum + 1] + timedelta(hours=17)
                    df_wind['goods_code'] = goods_code
                    dfFreq = dfFreq.append(df_wind)
                elif windData.ErrorCode == -40520007:
                    break
                else:
                    putLogEvent(ee, "检查CTA{}中{}时Wind出错".format(freq, dictGoodsName[eachGoods]))
                    break
        dfFreq['goods_name'] = dictGoodsName[eachGoods]
        if dfFreq.shape[0] > 0:
            # 删除索引的重复性
            dfFreq = dfFreq.loc[pd.Index(dfFreq.index).duplicated(keep = 'first') == False]
            if eachGoods.split('.')[1] == 'CFE':
                dfFreq = filterMinData(dfFreq, dictGoodsLast[eachGoods], isCFE=True)  # 数据过滤
            else:
                dfFreq = filterMinData(dfFreq, dictGoodsLast[eachGoods])
            dfFreq.to_sql(dictGoodsName[eachGoods], con, if_exists='append', index=True,
                           schema='cta{}_trade'.format(freq))
            dfFreq.to_sql(dictGoodsName[eachGoods] + '_调整表', con, if_exists='append', index=True,
                           schema='cta{}_trade'.format(freq))

def checkOtherMinBar(freq, ee):
    con = dictCon[freq]
    putLogEvent(ee, "检查CTA{}数据完整性".format(freq))
    for eachGoods in dictGoodsName.keys():
        putLogEvent(ee, "检查CTA{} {}完整性".format(freq, eachGoods))
        # 如果频段数据与调整表数据不相同的话，将调整表数据整个删除，直接使用频段数据即可：
        df1Last = pd.read_sql('select trade_time from {} order by trade_time desc limit 1'.format(dictGoodsName[eachGoods]), con).set_index('trade_time')
        df2Last = pd.read_sql('select trade_time from {} order by trade_time desc limit 1'.format(dictGoodsName[eachGoods] + '_调整表'), con).set_index('trade_time')
        if ((df1Last.shape[0] == df2Last.shape[0] > 0) and (df1Last.index[0] != df2Last.index[0])) or (df1Last.shape[0] > 0 and df2Last.shape[0] == 0):
            df1 = pd.read_sql('select * from {}'.format(dictGoodsName[eachGoods]), con).set_index('trade_time')
            con.execute('truncate table ' + dictGoodsName[eachGoods] + '_调整表')
            df1.to_sql(dictGoodsName[eachGoods] + '_调整表', con, if_exists='append', index=True,
                       schema='cta{}_trade'.format(freq))
        # 读取分钟数据
        dfMinute = pd.read_sql('select trade_time from {} order by trade_time desc limit 1'.format(dictGoodsName[eachGoods] + '_调整表'), con).set_index('trade_time')
        if dfMinute.shape[0] != 0:
            startTime = dfMinute.index[0] + timedelta(minutes=1)
        else:
            startTime = theStartTime
        dfAdj = pd.read_sql('select adjdate from {}_调整时刻表'.format(dictGoodsName[
            eachGoods]) + ' order by adjdate desc limit 1', con)
        df = pd.read_csv('chg_data\\' + eachGoods.upper() + ' chg_data.csv',
                         parse_dates=['adjdate'], encoding='gbk', index_col='id')
        df = df.reset_index(drop=True)
        if eachGoods.split('.')[1] in ['CZC', 'CFE']:
            df['goods_code'] = df['stock']
        else:
            df['goods_code'] = df['stock'].apply(lambda x: x.split('.')[0].lower() + '.' + x.split('.')[1])
        df = df[['goods_code', 'goods_name', 'adjdate', 'adjinterval']]
        df = df[1:-1]
        if dfAdj.shape[0] == 0:
            df = df.copy()
        else:
            df = df[df['adjdate'] > dfAdj['adjdate'][0]]
        if df.shape[0] > 0:
            df.to_sql(dictGoodsName[eachGoods] + '_调整时刻表', con, if_exists='append', index=False,
                      schema='cta{}_trade'.format(freq))
        endTime = pd.read_sql('select trade_time from cta{}_trade.{} order by trade_time desc limit 1'.format(1, dictGoodsName[eachGoods] + '_调整表'), con).set_index('trade_time').index[0]
        dfFreq = getBar(1, dictGoodsName[eachGoods] + '_调整表', con, startTime, endTime)
        if dfFreq.shape[0] > 0:
            dfFreq['trade_time'] = dfFreq.index
            """开始合成"""
            for each_num in range(len(dictGoodsClose[freq][eachGoods])):
                if each_num != len(dictGoodsClose[freq][eachGoods]) - 1:
                    df_temp = dfFreq.between_time(dictGoodsClose[freq][eachGoods][each_num],
                                                   dictGoodsClose[freq][eachGoods][each_num + 1],
                                                   include_start=False)
                    df_temp['trade_time'] = pd.to_datetime(df_temp['trade_time'].dt.strftime('%Y-%m-%d ')
                                                           + str(dictGoodsClose[freq][eachGoods][each_num + 1]))
                    dfFreq.update(df_temp)
                else:
                    df_temp = dfFreq.between_time(dictGoodsClose[freq][eachGoods][each_num],
                                                   dictGoodsClose[freq][eachGoods][0], include_start=False)
                    df_temp['trade_time'] = pd.to_datetime(
                        df_temp['trade_time'].dt.strftime('%Y-%m-%d ') + str(dictGoodsClose[freq][eachGoods][
                            0]))
                    dfFreq.update(df_temp)
            dfFreq = dfFreq.groupby(by='trade_time').agg({'goods_code': 'last',
                                                            'goods_name': 'last',
                                                            'close': 'last',
                                                            'open': 'first',
                                                            'high': max,
                                                            'low': min,
                                                            'volume': sum,
                                                            'amt': sum,
                                                            'oi': 'last'})
            dfFreq.to_sql(dictGoodsName[eachGoods], con, if_exists='append', index=True,
                           schema='cta{}_trade'.format(freq))
            dfFreq.to_sql(dictGoodsName[eachGoods] + '_调整表', con, if_exists='append', index=True,
                           schema='cta{}_trade'.format(freq))

"""分钟数据过滤处理"""
def filterMinData(dfMin, lastTime, isCFE=False):
    if isCFE == False:
        for each_dt in dfMin.index[(pd.Series(dfMin.index).dt.time == time(15)) | (
            pd.Series(dfMin.index).dt.time == lastTime)]:  # 将 15：00 与 最后平仓时间合并操作
            if each_dt - timedelta(minutes=1) in dfMin.index:
                dfMin.loc[each_dt - timedelta(minutes=1)] = concatSeries(dfMin.loc[each_dt - timedelta(minutes=1)],
                                                                          dfMin.loc[each_dt])
                dfMin = dfMin.drop([each_dt])
        for each_dt in dfMin.index[(pd.Series(dfMin.index).dt.time == time(8, 59)) | (
            pd.Series(dfMin.index).dt.time == time(20, 59))]:  # 将 15：00 与 21:00的时间操作
            if each_dt + timedelta(minutes=1) in dfMin.index:
                dfMin.loc[each_dt + timedelta(minutes=1)] = concatSeries(dfMin.loc[each_dt],
                                                                          dfMin.loc[each_dt + timedelta(minutes=1)])
                dfMin = dfMin.drop([each_dt])
    else:
        for each_dt in dfMin.index[pd.Series(dfMin.index).dt.time == time(15, 15)]:  # 将 15：15 的时间进行合并
            if each_dt - timedelta(minutes=1) in dfMin.index:
                dfMin.loc[each_dt - timedelta(minutes=1)] = concatSeries(dfMin.loc[each_dt - timedelta(minutes=1)],
                                                                          dfMin.loc[each_dt])
                dfMin = dfMin.drop([each_dt])
        for each_dt in dfMin.index[pd.Series(dfMin.index).dt.time == time(9, 14)]:  # 将 15：00 的时间进行合并
            if each_dt + timedelta(minutes=1) in dfMin.index:
                dfMin.loc[each_dt + timedelta(minutes=1)] = concatSeries(dfMin.loc[each_dt],
                                                                          dfMin.loc[each_dt + timedelta(minutes=1)])
                dfMin = dfMin.drop([each_dt])
    dfMin = dfMin.resample('T', closed='left', label='right').first().dropna(thresh=5)
    return dfMin

def concatSeries(s1, s2):
    """goods_code goods_name open high low close volume amt oi"""
    if s1.isnull().value_counts()[False] <= 4:
        return s2
    if s2.isnull().value_counts()[False] <= 4:
        return s1
    return pd.Series({'open':s1['open'],
            'high':max(s1['high'], s2['high']),
            'low':min(s1['low'], s2['low']),
            'close':s2['close'],
            'volume':sum([s1['volume'], s2['volume']]),
            'amt':sum([s1['amt'], s2['amt']]),
            'goods_code':s1['goods_code'],
            'goods_name':s1['goods_name'],
            'oi':s2['oi']})
