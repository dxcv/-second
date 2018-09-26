from function import *
from parameter import *
import os
from WindPy import w

# 品种对应调整表
dictGoodsAdj = {}
def checkChg():
    s = lambda x: x in '0123456789'
    for file in os.listdir(r'.\position_max'):
        goodsCode = file[:-17]
        if goodsCode.split('.')[1] in ['DCE', 'SHF']:
            goodsCode = goodsCode.split('.')[0].lower() + '.' + goodsCode.split('.')[1]
        goodsName = dictGoodsName[goodsCode]
        dfPosition = pd.read_csv('position_max' + '\\' + file, encoding='gbk',
                                  parse_dates=['trade_time']).set_index('trade_time')
        nextDate = tradeDay[tradeDay.index(dfPosition.index[-1]) + 1]
        nowDate = datetime.now()
        while nextDate.date() < nowDate.date() or (
                nextDate.date() == nowDate.date() and nowDate.time() > time(15, 30)):
            heyueData = w.wset('futurecc', "startdate={};enddate={};wind_code={}".format(nowDate.strftime('%Y-%m-%d'), nowDate.strftime('%Y-%m-%d'), goodsCode))
            listHeyue = heyueData.Data[2]
            maxHeyue = []
            maxLogcation = []
            for tempHeyue in listHeyue:
                # 只取这一天作出比较：
                getLocation = w.wsd(tempHeyue, 'oi', nextDate, nextDate)
                if getLocation.ErrorCode == 0 and getLocation.Data[0][0] != None:
                    maxLogcation.append(getLocation.Data[0][0])
                    maxHeyue.append(tempHeyue)
            # 还需要判断是否存在相同持仓量的情况
            if maxLogcation.count(max(maxLogcation)) > 1:
                dfPosition.loc[nextDate] = {'stock': dfPosition['stock'][-1], 'position': max(maxLogcation)}
            else:
                dfPosition.loc[nextDate] = {'stock': maxHeyue[maxLogcation.index(max(maxLogcation))],
                                             'position': max(maxLogcation)}
            nextDate = tradeDay[tradeDay.index(nextDate) + 1]
        dfPosition.to_csv('position_max' + '\\' + file)
        dfChgData = pd.read_csv(r'.\chg_data' + '\\' + goodsCode.upper() + ' chg_data.csv', encoding='gbk',
                                  parse_dates=['adjdate'])
        dfChgData = dfChgData.drop(['id'], axis=1)
        if dfPosition.index[-1] > dfChgData['adjdate'][dfChgData.shape[0] - 1]:
            chgExcData = dfChgData['adjdate'][dfChgData.shape[0] - 2]
            dfPosition = dfPosition[dfPosition.index >= chgExcData]
            mainHeyue = dfPosition.stock[0]
            dfPosition = dfPosition.drop(['position'], axis=1)
            listDay = [None]
            listStock = [dfChgData.stock[dfChgData.shape[0] - 2]]
            for i in range(1, dfPosition.shape[0]):
                if mainHeyue != dfPosition.stock[i] and int(''.join(list(filter(s, dfPosition.stock[i])))) > \
                        int(''.join(list(filter(s, mainHeyue)))):
                    listDay.append(dfPosition.index[i])
                    listStock.append(dfPosition['stock'][i])
                    mainHeyue = dfPosition['stock'][i]
            adjinterval = []
            if listDay[1:] != []:
                for the_num in range(1, len(listDay)):
                    real_index = listDay[the_num]
                    # 使用日数据的结束时间：那肯定是没有错的话：
                    firstExc = w.wsd(listStock[the_num], 'close', real_index, real_index)
                    firstClose = pd.Series(firstExc.Data[0]).tolist()[0]
                    secondExc = w.wsd(listStock[the_num - 1], 'close', real_index, real_index)
                    secondClose = pd.Series(secondExc.Data[0]).tolist()[0]
                    if firstClose == None:
                        firstClose = 0
                    if secondClose == None:
                        secondClose = 0
                    adjinterval.append(firstClose - secondClose)
            if adjinterval != []:
                theChgData = pd.DataFrame(
                    {'goods_name': goodsName, 'goods_code': goodsCode, 'adjinterval': adjinterval, 'stock': listStock[1:],
                     'adjdate': listDay[1:]})
                theChgData = theChgData[['goods_code', 'goods_name', 'adjdate', 'adjinterval', 'stock']]
                dfChgData = pd.concat([dfChgData[:dfChgData.shape[0] - 1], theChgData])
                dfChgData.index = range(1, dfChgData.shape[0] + 1)
                dfChgData.index.name = 'id'
                dfChgData.loc[dfChgData.shape[0] + 1] = {'goods_code': goodsCode.upper(), 'goods_name': goodsName,
                                                             'adjdate': dfPosition.index[-1], 'adjinterval': 0,
                                                             'stock': listStock[-1]}
                dfChgData = dfChgData[['goods_code', 'goods_name', 'adjdate', 'adjinterval', 'stock']]
                dfChgData.to_csv(r'.\chg_data' + '\\' + goodsCode.upper() + ' chg_data.csv')
            else:
                dfChgData['adjdate'].iat[-1] = dfPosition.index[-1]
                dfChgData.index = range(1, dfChgData.shape[0] + 1)
                dfChgData.index.name = 'id'
                dfChgData = dfChgData[['goods_code', 'goods_name', 'adjdate', 'adjinterval', 'stock']]
                dfChgData.to_csv(r'.\chg_data' + '\\' + goodsCode.upper() + ' chg_data.csv')

def getDictGoodsChg():
    con = dictCon[1]
    for eachGoods in dictGoodsName.keys():
        sql = 'select * from ' + dictGoodsName[eachGoods] + '_调整时刻表'
        dictGoodsAdj[eachGoods] = pd.read_sql(sql, con).set_index('goods_code')
        dictGoodsAdj[eachGoods]['adjdate'] = pd.to_datetime(dictGoodsAdj[eachGoods]['adjdate']) + timedelta(hours=16)