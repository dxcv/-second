from function import *
from parameter import *
"""
调用这个方法时：
自动补充周交易明细表的所有数据：
"""

def getLastData(freq, tableName, con, endTime = datetime.now(), limit = 1):
    sql = "select * from cta{}_trade.{} where trade_time <= '{}' order by trade_time desc limit {}".format(freq, tableName, endTime, limit)
    df = pd.read_sql(sql, con)
    df = df.drop(['id'], axis=1).set_index('trade_time')
    df = df.sort_index()
    return df

def getWeekTradeTab(freq, ee):
    con = dictCon[freq]
    putLogEvent(ee, "生成CTA{}周交易明细表数据".format(freq))
    for goodsCode in dictGoodsName.keys():
        putLogEvent(ee, "生成CTA{} {}周交易明细表数据".format(freq, goodsCode))
        goodsName = dictGoodsName[goodsCode]
        tb = dictTable[goodsName + '_周交易明细表']
        sql = 'select * from cta{}_trade.'.format(freq) + \
              goodsName + "_周交易明细表 where 交易时间 > '{}' and 交易时间 < '{}' order by 交易时间 desc limit 1".\
            format(weekStartTime, weekEndTime)
        SelectWeekTradeTab = pd.read_sql(sql, con)
        ODMvLen = ParTab[freq]['重叠度滑动长度'][goodsName]
        if SelectWeekTradeTab.shape[0] == 0:
            con.execute('truncate cta{}_trade.'.format(freq) + goodsName + '_周交易明细表')  # 清空周交易明细表
            OverlapDegree = getLastData(freq, goodsName + '_重叠度表', con, endTime=weekStartTime)
            OverlapDegree = OverlapDegree.rename(columns={'重叠度高_{}'.format(ODMvLen): '重叠度高',
                                                          '重叠度低_{}'.format(ODMvLen): '重叠度低',
                                                          '重叠度收_{}'.format(ODMvLen): '重叠度收'})
            tempHighOD = OverlapDegree['重叠度高'][0]
            CalOpenCloseLineTime = OverlapDegree.index[0].strftime('%H:%M')
            EndODtime = OverlapDegree.index[0]
            dr = getOneWeekTradeTab(freq, goodsCode, EndODtime)
            for eachColumn in SelectWeekTradeTab.columns:
                if eachColumn not in dr.keys():
                    dr[eachColumn] = np.NAN
            SelectWeekTradeTab.loc[SelectWeekTradeTab.shape[0]] = dr
        # 已经存在本周数据
        if SelectWeekTradeTab.shape[0] > 0:
            LastODData = getLastData(freq, goodsName + '_重叠度表', con)
            LastODT = LastODData.index[0]
            sql = 'select * from cta{}_trade.'.format(freq) + \
                  goodsName + '_周交易明细表 order by 交易时间 desc limit 1'
            LastWeekTradeTab = pd.read_sql(sql, con)
            LastWeekTradeTime = LastWeekTradeTab['交易时间'][0]
            if LastWeekTradeTime < LastODT:
                sql = 'select goods_name, goods_code, trade_time as 交易时间, high, low, close, ' \
                      '重叠度高_{} as 重叠度高, 重叠度低_{} as 重叠度低, 重叠度收_{} as 重叠度收 from cta{}_trade.'.format(
                    ODMvLen, ODMvLen, ODMvLen, freq) \
                      + goodsName \
                      + "_重叠度表 where trade_time > '{}' order by trade_time".format(LastWeekTradeTime)
                NewODData = pd.read_sql(sql, con)
                for tempI in range(NewODData.shape[0]):
                    CurrentTradeTime = NewODData['交易时间'][tempI]
                    getOneWeekTradeTab(freq, goodsCode, CurrentTradeTime)
        putLogEvent(ee, "生成CTA{} {}周交易明细表数据完成".format(freq, goodsCode))

def getOneWeekTradeTab(freq, goodsCode, CurrentTradeTime):
    # 基本设置
    con = dictCon[freq]
    dbName = 'cta{}_trade'.format(freq)
    goodsName = dictGoodsName[goodsCode]
    TradeCode = goodsCode
    ExchangeCode = goodsCode.split('.')[1]
    CapitalMaxLossRate = GoodsTab['资产回撤率'][goodsName]
    ChengShu = GoodsTab['合约乘数'][goodsName]
    MinChangUnit = GoodsTab['最小变动单位'][goodsName]
    OpenCloseLineMux = GoodsTab['开平仓阈值系数'][goodsName]
    StdMvLen = ParTab[freq]['均值滑动长度'][goodsName]
    ODMvLen = ParTab[freq]['重叠度滑动长度'][goodsName]
    ODth = ParTab[freq]['重叠度阈值'][goodsName]
    AbtainLossRate = ParTab[freq]['盈亏比'][goodsName]
    # 先判断是否存在上一条数据
    sql = "select * from {}.{}_周交易明细表 where 交易时间 < '{}' order by 交易时间 desc limit 1".format(dbName, goodsName, CurrentTradeTime)
    LastWeekTradeTab = pd.read_sql(sql, con)
    if LastWeekTradeTab.shape[0] == 0:
        OverlapDegree = getLastData(freq, goodsName + '_重叠度表', con, endTime=CurrentTradeTime)
        OverlapDegree = OverlapDegree.rename(columns={'重叠度高_{}'.format(ODMvLen): '重叠度高',
                                                      '重叠度低_{}'.format(ODMvLen): '重叠度低',
                                                      '重叠度收_{}'.format(ODMvLen): '重叠度收'})
        #region 做多
        tempHighOD = OverlapDegree['重叠度高'][0]
        CalOpenCloseLineTime = OverlapDegree.index[0].strftime('%H:%M')
        EndODtime = OverlapDegree.index[0]
        sql = 'select goods_name, goods_code, trade_time, high as 最高价 , low as 最低价, close as 收盘价, maprice_{} as 均值, ' \
              'stdprice_{} as 标准差, highstdmux_{} as 标准差倍数高 from cta{}_trade.'.format(ODMvLen, ODMvLen, ODMvLen, freq) \
              + goodsName + "_均值表 where trade_time <= '{}' order by trade_time desc limit {}".format(CurrentTradeTime, StdMvLen)
        StdData = pd.read_sql(sql, con)  # 获取这个时间点，上的ODMvLen 的长度
        HighStdList = StdData['标准差倍数高'].tolist()
        HighQ1 = np.percentile(HighStdList, 90)
        HighQ1MeanList = StdData['标准差倍数高'][StdData['标准差倍数高'] >= HighQ1]
        OpenMux = max(HighQ1MeanList.mean(), StdMuxMinValue)  # 做多开仓倍数
        StopAbtainMux = OpenMux + OpenCloseLineMux * max(1.2 * (HighQ1MeanList.max() - OpenMux),
                                                       StdMuxMinValue)
        StopLossMux = OpenMux - AbtainLossRate * (StopAbtainMux - OpenMux)
        MaPrice = StdData['均值'][0]
        StdPrice = StdData['标准差'][0]
        HighPrice = StdData['最高价'][0]
        LowPrice = StdData['最低价'][0]
        ClosePrice = StdData['收盘价'][0]
        OpenPrice = MaPrice + OpenMux * StdPrice  # 开仓线
        StopAbtainPrice = MaPrice + StopAbtainMux * StdPrice  # 止盈线
        StopLossPrice = MaPrice + StopLossMux * StdPrice  # 上损线
        dr = {}
        dr['周次'] = week
        dr["品种名称"] = goodsName
        dr["交易合约号"] = goodsCode
        dr["交易时间"] = EndODtime
        dr["开仓线多"] = OpenPrice
        dr["止盈线多"] = StopAbtainPrice
        dr["止损线多"] = StopLossPrice
        TradeOkFlag = False
        if ODth == -100:
            if tempHighOD == -100:
                TradeOkFlag = True
        else:
            if tempHighOD > ODth:
                TradeOkFlag = True
        if TradeOkFlag:
            dr["重叠度标识多"] = 1
        else:
            dr["重叠度标识多"] = 0
        dr["均值"] = MaPrice
        dr["标准差"] = StdPrice
        dr["最高价"] = HighPrice
        dr["最低价"] = LowPrice
        dr["标准差倍数高"] = StdData['标准差倍数高'][0]
        dr['做多参数'] = "{},{},{}".format(round(OpenMux, 4), round(StopAbtainMux, 4), round(StopLossMux, 4))
        dr['参数编号'] = 1
        dr['参数'] = "{}-{}-{}-{}".format(StdMvLen, AbtainLossRate, ODMvLen, ODth)
        #endregion
        #region 做空
        tempLowOD = OverlapDegree['重叠度低'][0]
        sql = 'select goods_name, goods_code, trade_time, high, low, close, maprice_{} as 均值, stdprice_{} as 标准差, lowstdmux_{} as 标准差倍数低 from cta{}_trade.'.\
                  format(ODMvLen, ODMvLen, ODMvLen, freq)  + goodsName + "_均值表 where trade_time <= '{}' order by trade_time desc limit {}".\
            format(weekStartTime, StdMvLen)
        StdData = pd.read_sql(sql, con)
        LowStdList = StdData['标准差倍数低']
        LowQ1 = np.percentile(LowStdList, 10)
        LowQ1MeanList = LowStdList[LowStdList <= LowQ1]
        OpenMux = min(LowQ1MeanList.mean(), StdMuxMinValue * (-1))
        StopAbtainMux = OpenMux + OpenCloseLineMux * min(1.2 * (LowQ1MeanList.min() - OpenMux),
                                                         StdMuxMinValue * (-1))
        StopLossMux = OpenMux - AbtainLossRate * (StopAbtainMux - OpenMux)
        OpenPrice = MaPrice + OpenMux * StdPrice
        StopAbtainPrice = MaPrice + StopAbtainMux * StdPrice
        StopLossPrice = MaPrice + StopLossMux * StdPrice
        TradeOkFlag = False
        if ODth == -100:
            if tempLowOD == -100:
                TradeOkFlag = True
        else:
            if tempLowOD > ODth:
                TradeOkFlag = True
        if TradeOkFlag:
            dr["重叠度标识空"] = 1
        else:
            dr["重叠度标识空"] = 0
        dr["开仓线空"] = OpenPrice
        dr["止盈线空"] = StopAbtainPrice
        dr["止损线空"] = StopLossPrice
        dr["标准差倍数低"] = StdData['标准差倍数低'][0]
        dr["做空参数"] = "{},{},{}".format(round(OpenMux, 4), round(StopLossMux, 4), round(StopAbtainMux, 4))
        dr["开平仓标识多"] = 0
        dr["开平仓标识空"] = 0
        #endregion
        dr = insertDbChg(dr)
        con.execute(dictTable[goodsName + '_周交易明细表'].insert(), dr)
        return dr
    else:
        PreTradeDuoFlag = 0
        PreTradeKongFlag = 0
        CangweiDuo = 0
        CangweiKong = 0
        PreKongODFlag = 0
        PreOpenTime = ""
        dr = {}
        sql = 'select goods_name, goods_code, trade_time as 交易时间, high, low, close, 重叠度高_{} as 重叠度高, 重叠度低_{} as 重叠度低, 重叠度收_{} as 重叠度收 from cta{}_trade.'.format(
            ODMvLen, ODMvLen, ODMvLen, freq) + goodsName + "_重叠度表 where trade_time <= '{}' order by trade_time desc limit 1".format(CurrentTradeTime)
        NewODData = pd.read_sql(sql, con)
        CurrentHighOD = NewODData['重叠度高'][0]
        CurrentLowOD = NewODData['重叠度低'][0]
        # 读取前一笔周交易状态
        if str(LastWeekTradeTab['开平仓标识多'][0]) not in ['nan', 'NaT', 'None','NaN']:
            PreTradeDuoFlag = LastWeekTradeTab['开平仓标识多'][0]
        if str(LastWeekTradeTab['开平仓标识空'][0]) not in ['nan', 'NaT', 'None','NaN']:
            PreTradeKongFlag = LastWeekTradeTab['开平仓标识空'][0]
        if str(LastWeekTradeTab['仓位多'][0]) not in ['nan', 'NaT', 'None','NaN']:
            CangweiDuo = LastWeekTradeTab['仓位多'][0]
        if str(LastWeekTradeTab['仓位空'][0]) not in ['nan', 'NaT', 'None','NaN']:
            CangweiKong = LastWeekTradeTab['仓位空'][0]
        PreDuoODFlag = LastWeekTradeTab['重叠度标识多'][0]
        PreKongODFlag = LastWeekTradeTab['重叠度标识空'][0]
        if str(LastWeekTradeTab['开仓时间'][0]) not in ['nan', 'NaT', 'None','NaN']:
            PreOpenTime = LastWeekTradeTab['开仓时间'][0].strftime('%Y-%m-%d %H:%M:%S')
        #region 做多
        sql = 'select goods_name, goods_code, trade_time, high as 最高价, low as 最低价, close as 收盘价, maprice_{} as 均值, stdprice_{} as 标准差, highstdmux_{} as 标准差倍数高 from cta{}_trade.'.\
                  format(ODMvLen, ODMvLen, ODMvLen, freq) + goodsName + "_均值表 where trade_time <= '{}' order by trade_time desc limit {}".\
            format(CurrentTradeTime, StdMvLen)
        StdData = pd.read_sql(sql, con)
        HighStdList = StdData['标准差倍数高'].tolist()
        HighQ1 = np.percentile(HighStdList, 90)
        HighQ1MeanList = StdData['标准差倍数高'][StdData['标准差倍数高'] >= HighQ1]
        OpenMux = max(HighQ1MeanList.mean(), StdMuxMinValue)  # 做多开仓倍数
        StopAbtainMux = OpenMux + OpenCloseLineMux * max(1.2 * (HighQ1MeanList.max() - OpenMux),
                                                         StdMuxMinValue)
        StopLossMux = OpenMux - AbtainLossRate * (StopAbtainMux - OpenMux)
        MaPrice = StdData['均值'][0]
        StdPrice = StdData['标准差'][0]
        HighPrice = StdData['最高价'][0]
        LowPrice = StdData['最低价'][0]
        ClosePrice = StdData['收盘价'][0]
        PreClosePrice = StdData['收盘价'][1]
        OpenPrice = MaPrice + OpenMux * StdPrice  # 开仓线
        StopAbtainPrice = MaPrice + StopAbtainMux * StdPrice  # 止盈线
        StopLossPrice = MaPrice + StopLossMux * StdPrice  # 上损线
        dr['周次'] = week
        dr["品种名称"] = goodsName
        dr["交易合约号"] = goodsCode
        dr["交易时间"] = CurrentTradeTime
        dr["开仓线多"] = OpenPrice
        dr["止盈线多"] = StopAbtainPrice
        dr["止损线多"] = StopLossPrice
        TradeOkFlag = False
        if ODth == -100:
            if CurrentHighOD == -100:
                TradeOkFlag = True
        else:
            if CurrentHighOD > ODth:
                TradeOkFlag = True
        if TradeOkFlag:
            dr["重叠度标识多"] = 1
        else:
            dr["重叠度标识多"] = 0
        dr["均值"] = MaPrice
        dr["标准差"] = StdPrice
        dr["最高价"] = HighPrice
        dr["最低价"] = LowPrice
        dr["标准差倍数高"] = StdData['标准差倍数高'][0]
        dr['做多参数'] = "{},{},{}".format(round(OpenMux, 4), round(StopLossMux, 4), round(StopAbtainMux, 4))
        dr['参数编号'] = 1
        dr['参数'] = "{}-{}-{}-{}".format(StdMvLen, AbtainLossRate, ODMvLen, ODth)
        # 根据上一条的开仓线推出下一条数据是否进行开仓的操作吧：
        PreOpenLine = changePriceLine(LastWeekTradeTab['开仓线多'][0], MinChangUnit, "多", "开仓")
        PreStopAbtainLine = changePriceLine(LastWeekTradeTab['止盈线多'][0], MinChangUnit, "多", "止盈")
        PreStopLossLine = changePriceLine(LastWeekTradeTab['止损线多'][0], MinChangUnit, "多", "止损")
        if PreTradeDuoFlag != 1:
            # 是否满足重叠度标识符号
            if PreDuoODFlag == 1:
                if HighPrice >= PreOpenLine and LowPrice <= PreOpenLine:
                    dr["开平仓标识多"] = 1
                    dr["开仓时间"] = CurrentTradeTime
                    CangweiDuo = (-1) * MaxLossPerCTA / ((PreStopLossLine - PreOpenLine) / PreOpenLine)
                    dr["仓位多"] = CangweiDuo
                    dr["单笔浮赢亏多"] = CangweiDuo * (ClosePrice - OpenPrice) / OpenPrice
                else:
                    dr["开平仓标识多"] = PreTradeDuoFlag
            else:
                dr["开平仓标识多"] = PreTradeDuoFlag
        else:
            # 判断是否满足平仓条件
            CloseFlag = False
            if HighPrice >= PreStopAbtainLine and LowPrice <= PreStopAbtainLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = True
                dr["开平仓标识多"] = -1
                dr["平仓时间"] = CurrentTradeTime
                dr["仓位多"] = CangweiDuo
                dr["单笔浮赢亏多"] = CangweiDuo * (PreStopAbtainLine - PreClosePrice) / PreClosePrice
            elif HighPrice > PreStopAbtainLine and LowPrice > PreStopAbtainLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = true
                dr["开平仓标识多"] = -1
                dr["平仓时间"] = CurrentTradeTime
                dr["仓位多"] = CangweiDuo
                dr["单笔浮赢亏多"] = CangweiDuo * (LowPrice - PreClosePrice) / PreClosePrice
            # 止损判断
            if HighPrice >= PreStopLossLine and LowPrice <= PreStopLossLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = true
                dr["开平仓标识多"] = -2
                dr["平仓时间"] = CurrentTradeTime
                dr["仓位多"] = CangweiDuo
                dr["单笔浮赢亏多"] = CangweiDuo * (PreStopLossLine - PreClosePrice) / PreClosePrice
            elif HighPrice < PreStopLossLine and LowPrice < PreStopLossLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = true
                dr["开平仓标识多"] = -2
                dr["平仓时间"] = CurrentTradeTime
                dr["仓位多"] = CangweiDuo
                dr["单笔浮赢亏多"] = CangweiDuo * (HighPrice - PreClosePrice) / PreClosePrice
            if not CloseFlag:
                dr["开仓时间"] = PreOpenTime
                dr["开平仓标识多"] = PreTradeDuoFlag
                dr["仓位多"] = CangweiDuo
                dr["单笔浮赢亏多"] = CangweiDuo * (ClosePrice - PreClosePrice) / PreClosePrice
        #endregion
        #region 做空
        sql = 'select goods_name, goods_code, trade_time, high as 最高价, low as 最低价, close as 收盘价, maprice_{} as 均值, stdprice_{} as 标准差, lowstdmux_{} as 标准差倍数低 from cta{}_trade.'.format(
            ODMvLen, ODMvLen, ODMvLen, freq) \
              + goodsName \
              + "_均值表 where trade_time <= '{}' order by trade_time desc limit {}".format(CurrentTradeTime,
                                                                                        StdMvLen)
        StdData = pd.read_sql(sql, con)
        dr['标准差倍数低'] = StdData['标准差倍数低'][0]
        LowStdList = StdData['标准差倍数低']
        LowQ1 = np.percentile(LowStdList, 10)
        LowQ1MeanList = LowStdList[LowStdList <= LowQ1]
        OpenMux = min(LowQ1MeanList.mean(), StdMuxMinValue * (-1))
        StopAbtainMux = OpenMux + OpenCloseLineMux * min(1.2 * (LowQ1MeanList.min() - OpenMux),
                                                         StdMuxMinValue * (-1))
        StopLossMux = OpenMux - AbtainLossRate * (StopAbtainMux - OpenMux)
        OpenPrice = MaPrice + OpenMux * StdPrice
        StopAbtainPrice = MaPrice + StopAbtainMux * StdPrice
        StopLossPrice = MaPrice + StopLossMux * StdPrice
        dr["开仓线空"] = OpenPrice
        dr["止盈线空"] = StopAbtainPrice
        dr["止损线空"] = StopLossPrice
        TradeOkFlag = False
        if ODth == -100:
            if CurrentLowOD == -100:
                TradeOkFlag = True
        else:
            if CurrentLowOD > ODth:
                TradeOkFlag = True
        if TradeOkFlag:
            dr["重叠度标识空"] = 1
        else:
            dr["重叠度标识空"] = 0
        dr["做空参数"] = "{},{},{}".format(round(OpenMux, 4), round(StopLossMux, 4), round(StopAbtainMux, 4))
        PreOpenLine = changePriceLine(LastWeekTradeTab['开仓线空'][0], MinChangUnit, "空", "开仓")
        PreStopAbtainLine = changePriceLine(LastWeekTradeTab['止盈线空'][0], MinChangUnit, "空", "止盈")
        PreStopLossLine = changePriceLine(LastWeekTradeTab['止损线空'][0], MinChangUnit, "空", "止损")
        if PreTradeKongFlag != 1:
            if PreKongODFlag == 1:
                if HighPrice >= PreOpenLine and LowPrice <= PreOpenLine:
                    dr["开平仓标识空"] = 1
                    dr["开仓时间"] = CurrentTradeTime
                    CangweiKong = MaxLossPerCTA / ((PreStopLossLine - PreOpenLine) / PreOpenLine)
                    dr["仓位空"] = CangweiKong
                    dr["单笔浮赢亏空"] = (-1) * CangweiKong * (ClosePrice - PreOpenLine) / PreOpenLine
                else:
                    dr["开平仓标识空"] = PreTradeKongFlag
            else:
                dr["开平仓标识空"] = PreTradeKongFlag
        else:
            CloseFlag = False
            if HighPrice >= PreStopAbtainLine and LowPrice <= PreStopAbtainLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = True
                dr["开平仓标识空"] = -1
                dr["平仓时间"] = CurrentTradeTime
                dr["仓位空"] = CangweiKong
                dr["单笔浮赢亏空"] = (-1) * CangweiKong * (PreStopAbtainLine - PreClosePrice) / PreClosePrice
            elif HighPrice < PreStopAbtainLine and LowPrice < PreStopAbtainLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = True
                dr["开平仓标识空"] = -1
                dr["仓位空"] = CangweiKong
                dr["平仓时间"] = CurrentTradeTime
                dr["单笔浮赢亏空"] = (-1) * CangweiKong * (LowPrice - PreClosePrice) / PreClosePrice
            if HighPrice >= PreStopLossLine and LowPrice <= PreStopLossLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = True
                dr["开平仓标识空"] = -2
                dr["平仓时间"] = CurrentTradeTime
                dr["仓位空"] = CangweiKong
                dr["单笔浮赢亏空"] = -CangweiKong * (PreStopLossLine - PreClosePrice) / PreClosePrice
            elif HighPrice > PreStopLossLine and LowPrice > PreStopLossLine:
                dr["开仓时间"] = PreOpenTime
                CloseFlag = True
                dr["开平仓标识空"] = -2
                dr["仓位空"] = CangweiKong
                dr["平仓时间"] = CurrentTradeTime
                dr["单笔浮赢亏空"] = -CangweiKong * (HighPrice - PreClosePrice) / PreClosePrice
            if not CloseFlag:
                dr["开仓时间"] = PreOpenTime
                dr["开平仓标识空"] = PreTradeKongFlag
                dr["仓位空"] = CangweiKong
                dr["单笔浮赢亏空"] = -CangweiKong * (ClosePrice - PreClosePrice) / PreClosePrice
        #endregion
        dr = insertDbChg(dr)
        con.execute(dictTable[goodsName + '_周交易明细表'].insert(), dr)
        return dr

if __name__ == '__main__':
    freq = 5
    ee = EventEngine()
    ee.start(timer=False)
    con = create_engine('mysql+pymysql://root:rd008@localhost:3306/?charset=utf8').connect()
    getWeekTradeTab(freq, ee)