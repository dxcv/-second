from parameter import *
from function import *
"""
删除数据库里的数据操作
同时包含startTime与endTime的数据
"""
def dltData(startTime, endTime = datetime.now()):
    for eachFreq in listFreqPlus:
        if eachFreq == 1:
            con = dictCon[1]
            for eachGoods in dictGoodsName.keys():
                listTables = [dictGoodsName[eachGoods], dictGoodsName[eachGoods] + '_调整表']
                for eachTable in listTables:
                    sql = "delete from {} where trade_time >= '{}' and trade_time <= '{}'". \
                        format(eachTable, startTime.strftime('%Y-%m-%d %H:%M:%S'),
                               endTime.strftime('%Y-%m-%d %H:%M:%S'))
                    con.execute(sql)
        else:
            con = dictCon[eachFreq]
            for eachGoods in dictGoodsName.keys():
                listTables = [dictGoodsName[eachGoods], dictGoodsName[eachGoods] + '_调整表',
                              dictGoodsName[eachGoods] + '_均值表',
                              dictGoodsName[eachGoods] + '_重叠度表']
                for eachTable in listTables:
                    sql = "delete from {} where trade_time >= '{}' and trade_time <= '{}'".format(eachTable,
                        startTime.strftime('%Y-%m-%d %H:%M:%S'), endTime.strftime('%Y-%m-%d %H:%M:%S'))
                    con.execute(sql)
                sql = "delete from {} where 交易时间 >= '{}' and 交易时间 <= '{}'". \
                    format(dictGoodsName[eachGoods] + '_周交易明细表',
                           startTime.strftime('%Y-%m-%d %H:%M:%S'),
                           endTime.strftime('%Y-%m-%d %H:%M:%S'))
                con.execute(sql)

if __name__ == '__main__':
    dltData(datetime(2018, 9, 5, 16))