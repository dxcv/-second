from parameter import *
from function import *
from chgAdjust import *
from getWeekTradeTab import getOneWeekTradeTab

def dealBar(strBar, ee):
    dict = eval(strBar.replace("datetime.", ""))
    putLogEvent(ee, strBar)
    tradeTime = dict['trade_time']
    if dict['goods_code'].split('.')[0][-4:].isdigit():
        goodsCode = dict['goods_code'].split('.')[0][:-4] + '.' + dict['goods_code'].split('.')[1]
    else:
        goodsCode = dict['goods_code'].split('.')[0][:-3] + '.' + dict['goods_code'].split('.')[1]
    goodsName = dictGoodsName[goodsCode]
    goodsInstrument = dict['goods_code'].split('.')[0]
    for eachFreq in listFreq:
        if tradeTime.time() in dictGoodsClose[eachFreq][goodsCode]:
            # 先对之前的bar数据进行撤单操作 编号为 时间 频段 品种 第几个bar 开仓（1）还是平仓（0）操作
            putLogBarDealEvent(ee, "品种 {} 频段 {} 时间为 {} 数据处理".
                               format(goodsInstrument, eachFreq, tradeTime.strftime("%Y-%m-%d %H:%M:%S")), eachFreq)
            #region 撤回上个bar数据下的单
            indexGoods = listGoods.index(goodsCode)
            indexBar = dictGoodsClose[eachFreq][goodsCode].index(tradeTime.time())
            indexLastBar = len(dictGoodsClose[eachFreq][goodsCode]) if indexBar == 0 else indexBar - 1
            preOrderRef = (tradeTime + timedelta(hours = 5)).strftime('%Y%m%d')[3:] + \
                       str(eachFreq).zfill(2) + str(indexGoods).zfill(2) + str(indexLastBar).zfill(2)
            orderRef = (tradeTime + timedelta(hours = 5)).strftime('%Y%m%d')[3:] + \
                       str(eachFreq).zfill(2) + str(indexGoods).zfill(2) + str(indexBar).zfill(2)
            print(preOrderRef)
            print(orderRef)
            if tradeTime.time() in dictGoodsPark[goodsCode]:  # 如果撤单的时间刚好为收盘时间，改为预下单操作
                theOrder = EVENT_ORDERPARK
                theCancel = EVENT_ORDERPARKCANCEL
            else:
                theOrder = EVENT_ORDER
                theCancel = EVENT_ORDERCANCEL
            putLogBarDealEvent(ee, "对本地编码为 {} 进行撤单操作".
                               format(preOrderRef + '1'), eachFreq)
            putLogBarDealEvent(ee, "对本地编码为 {} 进行撤单操作".
                               format(preOrderRef + '0'), eachFreq)
            # 撤开仓单操作
            cancelEvent = Event(type_=theCancel)
            cancelEvent.dict_['orderref'] = preOrderRef + '1'
            ee.put(cancelEvent)
            # 撤止盈止损单操作
            cancelEvent = Event(type_=theCancel)
            cancelEvent.dict_['orderref'] = preOrderRef + '0'
            ee.put(cancelEvent)

            # 对bar数据进行处理
            getOrder(eachFreq, goodsCode,goodsInstrument, tradeTime, ee, orderRef)

def getOrder(freq, goodsCode,goodsInstrument, CurrentTradeTime, ee, orderRef):
    if CurrentTradeTime.time() in dictGoodsPark[goodsCode]:  # 是否需要预下单
        theOrder = EVENT_ORDERPARKCOMMAND
        theCancel = EVENT_ORDERPARKCANCEL
    else:
        theOrder = EVENT_ORDERCOMMAND
        theCancel = EVENT_ORDERCANCEL
    con = dictCon[freq]
    goodsName = dictGoodsName[goodsCode]
    theCode = goodsCode
    goodsCode = goodsInstrument
    CapitalMaxLossRate = GoodsTab["资产回撤率"][goodsName]
    ChengShu = GoodsTab["合约乘数"][goodsName]
    MinChangUnit = GoodsTab["最小变动单位"][goodsName]
    CloseAllTimeStr = GoodsTab["交易时间类型"][goodsName]
    MarketOrderMaxNum = GoodsTab["市价强平手数"][goodsName]
    PreTradeCode = GoodsTab["前交易合约号"][goodsName]
    DayTradeEnable = ParTab[freq]["日盘交易标识"][goodsName]
    NightTradeEnable = ParTab[freq]["夜盘交易标识"][goodsName]
    ODMvLen = ParTab[freq]["重叠度滑动长度"][goodsName]
    seriesTime = pd.Series(dictGoodsClose[freq][theCode])
    # 下午最后交易时间
    LastTimeNon = seriesTime.iat[-1]
    # 下午倒数第二交易时间
    PreLastTimeNon = seriesTime.iat[-2]
    # 当日开盘的第一个bar时间
    FirstTradeTimeInDay = seriesTime[(seriesTime > time(hour=8)) & (seriesTime < time(hour=17))].iat[0]
    # 最后交易时间
    tempSeriesTime = seriesTime[(seriesTime < time(hour=8)) | (seriesTime > time(hour=17))]
    LastTimeNight = LastTimeNon if tempSeriesTime.shape[0] == 0 else tempSeriesTime.iat[-1]
    # 倒数第二交易时间
    PreLastTimeNight = PreLastTimeNon if tempSeriesTime.shape[0] == 0 else tempSeriesTime.iat[-2]
    # 根据最新周交易状态，进行下单
    sql = "select * from {}_周交易明细表 where 交易时间 <= '{}' order by 交易时间 desc limit 1".format(goodsName, CurrentTradeTime)
    LastTradeDataTab = pd.read_sql(sql, con)
    # 是否引入重叠度长度对应的均值标识
    if MaWithODLenFlag:
        sql = "select maprice_{} as maprice from {}_均值表 where trade_time <= '{}' order by trade_time desc limit 1".format(ODMvLen, goodsName, CurrentTradeTime)
        MaWithODLen = pd.read_sql(sql, con)["maprice"][0]
    LongParStr = LastTradeDataTab['做多参数'][0]
    ShortParStr = LastTradeDataTab['做空参数'][0]
    LongParList = LongParStr.split(',')
    ShortParList = ShortParStr.split(',')
    TradeTime = LastTradeDataTab['交易时间'][0]

    #region 做多线
    LastDuoFlag = LastTradeDataTab['开平仓标识多'][0]
    LastKongFlag = LastTradeDataTab['开平仓标识空'][0]
    ODduoFlag = LastTradeDataTab['重叠度标识多'][0]
    ODkongFlag = LastTradeDataTab['重叠度标识空'][0]
    # 获取当前策略的持仓信息
    dfHold = dictFreqPosition[freq]
    pos1 = 0
    pos2 = 0
    if goodsCode in dfHold['代码']:
        seriesGoods = dfHold['代码'][dfHold['代码'] == goodsCode]
        if seriesGoods["数量"] > 0:
            pos1 = seriesGoods["数量"]
        else:
            pos2 = seriesGoods["数量"]
    HighPrice = LastTradeDataTab["最高价"][0]
    LowPrice = LastTradeDataTab["最低价"][0]
    OpenLongPrice = LastTradeDataTab['开仓线多'][0]
    LongStopProfit = LastTradeDataTab['止盈线多'][0]
    LongStopLoss = LastTradeDataTab['止损线多'][0]
    accountCapital = dfCapital[dfCapital['账户名'] == accountName]['资金'].iat[0]
    RiskRate = dfCapital[dfCapital['账户名'] == accountName]['风险系数'].iat[0]
    CangWei = CapitalMaxLossRate / ((OpenLongPrice - LongStopLoss) / OpenLongPrice)
    DuoCountMux = 1
    if LowPrice > (OpenLongPrice + LongStopLoss) / 2:  # 最低价在做多的开仓线和止损线之上，则仅开一半仓位
        DuoCountMux = 0.5
    AccoutRate = GoodsTab[accountName + "系数"][goodsName]
    DuoBuyCount = (AccoutRate * accountCapital * CangWei * RiskRate) / (OpenLongPrice * ChengShu)
    Duovolume = round(DuoBuyCount * DuoCountMux)
    print('AccoutRate')
    print(AccoutRate)
    print("DuoBuyCount")
    print(DuoBuyCount)
    print('DuoCountMux')
    print(DuoCountMux)
    # 进行四舍五入的价格调整
    OpenLongPrice = changePriceLine(OpenLongPrice, MinChangUnit, "多", "开仓")
    LongStopProfit = changePriceLine(LongStopProfit, MinChangUnit, "多", "止盈")
    LongStopLoss = changePriceLine(LongStopLoss, MinChangUnit, "多", "止损")
    #endregion

    #region 做空线
    OpenShortPrice = LastTradeDataTab["开仓线空"][0]
    ShortStopProfit = LastTradeDataTab["止盈线空"][0]
    ShortStopLoss = LastTradeDataTab['止损线空'][0]
    KongCountMux = 1
    if HighPrice < (ShortStopLoss + OpenShortPrice) / 2:  # 最高价在做空的开仓线和止损线中线之下，则仅开一半空仓位
        KongCountMux = 0.5
    CangWei = CapitalMaxLossRate / ((ShortStopLoss - OpenShortPrice) / OpenShortPrice)
    KongBuyCount = (AccoutRate * accountCapital * CangWei * RiskRate) / (OpenShortPrice * ChengShu)
    Kongvolume = round(KongBuyCount * KongCountMux)
    OpenShortPrice = changePriceLine(OpenShortPrice, MinChangUnit, "空", "开仓")
    ShortStopProfit = changePriceLine(ShortStopProfit, MinChangUnit, "空", "止盈")
    ShortStopLoss = changePriceLine(ShortStopLoss, MinChangUnit, "空", "止损")
    #endregion

    #region 高低价是否满足重叠度长度对应的均值条件
    if MaWithODLenFlag:
        if HighPrice < MaWithODLen:  # MaWithODLen 不指定的均值
            Duovolume = 0
            putLogBarDealEvent(ee, 'HighPrice < MaWithODLen', freq)
        if LowPrice > MaWithODLen:
            Kongvolume = 0
            putLogBarDealEvent(ee, 'LowPrice > MaWithODLen', freq)
    else:
        putLogBarDealEvent(ee, '无需判断 重叠度对应的均值与高低价关系', freq)
    position = pos1 + pos2
    LongSign = LastDuoFlag
    ShortSign = LastKongFlag
    n_open = 0  # 开仓加价
    n_stop_loss = 0  # 平仓加价
    sendTradeDr = {}
    sendTradeDr["发单时间"] = TradeTime
    sendTradeDr["合约号"] = goodsCode
    sendTradeDr["多手数"] = Duovolume
    sendTradeDr["空手数"] = Kongvolume
    sendTradeDr["下单加价"] = n_open
    sendTradeDr["止损加价"] = n_stop_loss

    #region 做多持仓判断
    if LastDuoFlag != 1:  # 多侍开仓
        if pos1 == 0:  # 正常多开单
            if ODduoFlag == 1:  # 满足重叠度条件,则正常开多单
                TradeDuoOkStatus = 1  # 标识多正常下单
            else:
                TradeDuoOkStatus = 3  # 标识多不下单
        else:  # 本应该开多，但还有剩余仓位，则不开多，分批处理函数
            if ODduoFlag == 1:
                TradeDuoOkStatus = 4
            else:
                TradeDuoOkStatus = 2  # 不满足重叠度条件
    else:  # 平仓判断
        if pos1 == 0:  # 本应该 平多仓，但实际 未持多仓
            TradeDuoOkStatus = 5
        else:
            TradeDuoOkStatus = 1
    if TradeDuoOkStatus == 1 or TradeDuoOkStatus == 4:  # 状态为1，4，则保留正常开平仓线，手数
        sendTradeDr["多开仓线"] = OpenLongPrice
        sendTradeDr["多止损线"] = LongStopLoss
        sendTradeDr["多止盈线"] = LongStopProfit
        sendTradeDr["多标识"] = LongSign
    elif TradeDuoOkStatus == 2 or TradeDuoOkStatus == 3 or TradeDuoOkStatus == 5:  # 强平多仓
        sendTradeDr["多开仓线"] = 1
        sendTradeDr["多止损线"] = 0
        sendTradeDr["多止盈线"] = 2
        sendTradeDr["多标识"] = LongSign
    #endregion

    #region 做空持仓判断
    if LastKongFlag != 1: # 空待开仓
        if pos2 == 0: # 正常开空单
            if ODkongFlag == 1: # 满足重叠度条件, 则正常开空单
                TradeKongOkStatus = 1 # 正常下单
            else:
                TradeKongOkStatus = 3 # 不下单
        else: # 本应开空，但已经持有空，则不开空
            if ODkongFlag == 1: # 满足重叠度条件, 则正常开空单
                TradeKongOkStatus = 4 # 正常下单
            else:
                TradeKongOkStatus = 2 # 不下单
    else:
        if pos2 == 0: # 本应该持空仓，但实际为无持仓，则不发单
            TradeKongOkStatus = 5
        else: # 有持仓，则正常的空线
            TradeKongOkStatus = 1
    if TradeKongOkStatus == 1 or TradeKongOkStatus == 4:
        sendTradeDr["空开仓线"] = OpenShortPrice
        sendTradeDr["空止损线"] = ShortStopLoss
        sendTradeDr["空止盈线"] = ShortStopProfit
        sendTradeDr["空标识"] = ShortSign
    elif TradeKongOkStatus == 2 or TradeKongOkStatus == 3 or TradeKongOkStatus == 5:
        sendTradeDr["空开仓线"] = 1
        sendTradeDr["空止损线"] = 2
        sendTradeDr["空止盈线"] = 0
        sendTradeDr["空标识"] = ShortSign
    #endregion

    putLogBarDealEvent(ee, "{} TradeDuoOkStatus = {}, TradeKongOkStatus = {}"
                       .format(goodsCode, TradeDuoOkStatus, TradeKongOkStatus), freq)

    #region 根据开仓价与止损价相差多少个最小时间单位，进行价格的判断
    if ShortSign != 1 and LongSign != 1:
        if OpenLongPrice != 1:
            if OpenLongPrice - LongStopLoss <= 5 * MinChangUnit:  # 开仓价 - 止损价 <= 5 * Min
                LongOpenCloseMux = round((OpenLongPrice - LongStopLoss) / MinChangUnit)
                putLogBarDealEvent(ee, "{} 做多线 OpenLongPrice = {}, LongStopLoss = {}, 相差价位数 = {}".format(goodsCode, OpenLongPrice, LongStopLoss, LongOpenCloseMux), freq)
                sendTradeDr["多开仓线"] = 1
                sendTradeDr["多止损线"] = 0
                sendTradeDr["多止盈线"] = 2
        if OpenShortPrice != 1:
            if ShortStopLoss - OpenShortPrice <= 5 * MinChangUnit:
                ShortOpenCloseMux = round((ShortStopLoss - OpenShortPrice) / MinChangUnit)
                putLogBarDealEvent(ee, "{} 做空线 OpenShortPrice = {}, ShortStopLoss = {}, 相差价位数 = {}".format(goodsCode, OpenShortPrice, ShortStopLoss, ShortOpenCloseMux), freq)
                sendTradeDr["空开仓线"] = 1
                sendTradeDr["空止损线"] = 2
                sendTradeDr["空止盈线"] = 0

    if TradeDuoOkStatus != 5 and TradeKongOkStatus != 5:
        if TradeDuoOkStatus == 3 and TradeKongOkStatus == 3:
            putLogBarDealEvent(ee, "{} OD均不满足重叠度条件，不下单".format(goodsCode), freq)
        else:
            # 强平操作
            if TradeDuoOkStatus == 2 or TradeDuoOkStatus == 4:
                if pos1 > 0:
                    putLogBarDealEvent(ee, "{} 最后交易时间 TradeTime={}".format(goodsCode, TradeTime), freq)
                    putLogBarDealEvent(ee, "{} 不应持仓，但有持仓，强平多仓".format(goodsCode), freq)
                    orderEvent = Event(type_=theOrder)
                    orderEvent.dict_['InstrumentID'] = goodsCode
                    orderEvent.dict_['Direction'] = DirectionType.Buy
                    orderEvent.dict_['CombOffsetFlag'] = OffsetFlagType.Close.__char__()
                    orderEvent.dict_['OrderPriceType'] = OrderPriceTypeType.AnyPrice
                    orderEvent.dict_['LimitPrice'] = 0
                    orderEvent.dict_['orderref'] = orderRef + '0'
                    orderEvent.dict_['VolumeTotalOriginal'] = abs(pos1)
                    ee.put(orderEvent)
                    position = 0
                else:
                    putLogBarDealEvent(ee, "{} 最后交易时间TradeTime={}".format(goodsCode, TradeTime), freq)
                    putLogBarDealEvent(ee, "{} 不应持仓，但有持仓，强平空仓".format(goodsCode), freq)
                    orderEvent = Event(type_=theOrder)
                    orderEvent.dict_['InstrumentID'] = goodsCode
                    orderEvent.dict_['Direction'] = DirectionType.Sell
                    orderEvent.dict_['CombOffsetFlag'] = OffsetFlagType.Close.__char__()
                    orderEvent.dict_['OrderPriceType'] = OrderPriceTypeType.AnyPrice
                    orderEvent.dict_['LimitPrice'] = 0
                    orderEvent.dict_['orderref'] = orderRef + '0'
                    orderEvent.dict_['VolumeTotalOriginal'] = abs(pos1)
                    ee.put(orderEvent)
                    position = 0
            if TradeKongOkStatus == 2 or TradeKongOkStatus == 4:
                if pos2 < 0:
                    putLogBarDealEvent(ee, "{} 最后交易时间TradeTime={}".format(goodsCode, TradeTime), freq)
                    putLogBarDealEvent(ee, "{} 不应持仓，但有持仓，强平空仓".format(goodsCode), freq)
                    orderEvent = Event(type_=theOrder)
                    orderEvent.dict_['InstrumentID'] = goodsCode
                    orderEvent.dict_['Direction'] = DirectionType.Sell
                    orderEvent.dict_['CombOffsetFlag'] = OffsetFlagType.Close.__char__()
                    orderEvent.dict_['OrderPriceType'] = OrderPriceTypeType.AnyPrice
                    orderEvent.dict_['LimitPrice'] = 0
                    orderEvent.dict_['orderref'] = orderRef + '0'
                    orderEvent.dict_['VolumeTotalOriginal'] = abs(pos2)
                    ee.put(orderEvent)
                    position = 0
                else:
                    putLogBarDealEvent(ee, "{} 最后交易时间 TradeTime={}".format(goodsCode, TradeTime), freq)
                    putLogBarDealEvent(ee, "{} 不应持仓，但有持仓，强平多仓".format(goodsCode), freq)
                    orderEvent = Event(type_=theOrder)
                    orderEvent.dict_['InstrumentID'] = goodsCode
                    orderEvent.dict_['Direction'] = DirectionType.Buy
                    orderEvent.dict_['CombOffsetFlag'] = OffsetFlagType.Close.__char__()
                    orderEvent.dict_['OrderPriceType'] = OrderPriceTypeType.AnyPrice
                    orderEvent.dict_['LimitPrice'] = 0
                    orderEvent.dict_['orderref'] = orderRef + '0'
                    orderEvent.dict_['VolumeTotalOriginal'] = abs(pos2)
                    ee.put(orderEvent)
                    position = 0

            #region 如果未持仓，判断到参数为 1 或者 -1 的话
            if OpenParFiltedOpenLineFlag == True:
                if position == 0:
                    if LongParList[0] == "1":
                            putLogBarDealEvent(ee, "{} 做多，开仓线参数 = {}".format(goodsCode, LongParList[0]), freq)
                            sendTradeDr["多开仓线"] = 1
                            sendTradeDr["多止损线"] = 0
                            sendTradeDr["多止盈线"] = 2
                    if ShortParList[0] == "-1":
                            putLogBarDealEvent(ee, "{} 做空，开仓线参数 = {}".format(goodsCode, ShortParList[0]), freq)
                            sendTradeDr["空开仓线"] = 1
                            sendTradeDr["空止损线"] = 2
                            sendTradeDr["空止盈线"] = 0

            #region 未持仓，在开仓的bar内进行止盈与止损的调整
            if position == 0:
                if OpenLongPrice != 1:
                    # bar内止盈，则按止盈倍数进行调整
                    if InBarCloseAtNMuxFlag == "1":
                        IntervalPrice = OpenLongPrice + (LongStopProfit - OpenLongPrice) * StopAbtainInBarMux
                        LongStopProfit = changePriceLine(IntervalPrice, MinChangUnit, "多", "止盈")
                        sendTradeDr["多止盈线"] = LongStopProfit
                        putLogBarDealEvent(ee, "{} position = {} , 调整多止盈线 LongStopProfit = {}".format(goodsCode, position, LongStopProfit), freq)
                    else:
                        sendTradeDr["多止盈线"] = PricUnreachableHighPrice
                    # bar内止损，则按止损倍数进行调整
                    if InBarStopLossFlag == "1":
                        IntervalPrice = OpenLongPrice - (OpenLongPrice - LongStopLoss) * StopLossInBarMux
                        LongStopLoss = changePriceLine(IntervalPrice, MinChangUnit, "多", "止损")
                        sendTradeDr["多止损线"] = LongStopLoss
                        putLogBarDealEvent(ee, "{} position = {} , 调整多止损线 LongStopLoss = {}".format(goodsCode, position, LongStopLoss), freq)
                    else:
                        sendTradeDr["多止损线"] = PricUnreachableLowPrice
                if OpenShortPrice != 1:
                    if InBarCloseAtNMuxFlag == "1":
                        IntervalPrice = OpenShortPrice - (OpenShortPrice - ShortStopProfit) * StopAbtainInBarMux
                        ShortStopProfit = changePriceLine(IntervalPrice, MinChangUnit, "空", "止盈")
                        sendTradeDr["空止盈线"] = ShortStopProfit
                        putLogBarDealEvent(ee, "{} position = {} , 调整空止盈线 ShortStopProfit = {}".format(goodsCode, position, ShortStopProfit), freq)
                    else:
                        sendTradeDr["空止盈线"] = PricUnreachableLowPrice
                    if InBarStopLossFlag == "1":
                        IntervalPrice = OpenShortPrice + (ShortStopLoss - OpenShortPrice) * StopLossInBarMux
                        ShortStopLoss = changePriceLine(IntervalPrice, MinChangUnit, "空", "止损")
                        sendTradeDr["空止损线"] = ShortStopLoss
                        putLogBarDealEvent(ee, "{} position = {} , 调整空止损线 ShortStopLoss = {}".format(goodsCode, position, ShortStopLoss),freq)
                    else:
                        sendTradeDr["空止损线"] = PricUnreachableHighPrice

            weekLastDay = theTradeDay[(theTradeDay >= weekStartTime.date()) & (theTradeDay <= weekEndTime.date())].iat[0]
            # 本周最后一个bar，则不下单操作
            if TradeTime.date() in futureDate:
                if TradeTime.time() == PreLastTimeNon and LastTimeNon == dictGoodsClose[1][theCode].iat[-1] and TradeTime > datetime(weekLastDay.year, weekLastDay.month, weekLastDay.day, 8):
                    putLogBarDealEvent(ee, "{} 本周最后一个bar时间: {} ,且均待开仓，则不再开仓！".format(goodsCode, TradeTime), freq)
                    return
                elif TradeTime.time() == LastTimeNon and TradeTime > datetime(weekLastDay.year, weekLastDay.month, weekLastDay.day, 8):
                    putLogBarDealEvent(ee, "{} 本周最后一个bar时间: {} ,且均待开仓，则不再开仓！".format(goodsCode, TradeTime), freq)
                    return
            else:
                if TradeTime.time() == PreLastTimeNight and LastTimeNight == dictGoodsLast[theCode] and TradeTime > datetime(weekLastDay.year, weekLastDay.month, weekLastDay.day, 8):
                    putLogBarDealEvent(ee, "{} 本周最后一个bar时间: {} ,且均待开仓，则不再开仓！".format(goodsCode, TradeTime), freq)
                    return
                elif TradeTime.time() == LastTimeNight and TradeTime > datetime(weekLastDay.year, weekLastDay.month, weekLastDay.day, 8):
                    putLogBarDealEvent(ee, "{} 本周最后一个bar时间: {} ,且均待开仓，则不再开仓！".format(goodsCode, TradeTime), freq)
                    return

            # 多待开仓，下单手数为0，则调整做多开平仓线
            if LongSign != 1 and Duovolume == 0:
                sendTradeDr["多开仓线"] = 1
                sendTradeDr["多止盈线"] = 2
                sendTradeDr["多止损线"] = 0
            if ShortSign != 1 and Kongvolume == 0:
                sendTradeDr["空开仓线"] = 1
                sendTradeDr["空止盈线"] = 0
                sendTradeDr["空止损线"] = 2
            if sendTradeDr["多开仓线"] == 1 and sendTradeDr["空开仓线"] == 1:
                putLogBarDealEvent(ee, "{} 多开仓线 = 1 空开仓线 = 1".format(goodsCode), freq)
                return
            else:
                sendTradeDr["发送时间"] = datetime.now()

                if DayTradeEnable == 1 and NightTradeEnable != 1:
                    if TradeTime.time() < time(16) and TradeTime - timedelta(minutes=freq) >= time(9):
                        putLogBarDealEvent(ee, "{} 夜盘不开仓，仅平仓；日盘可开平，现在是日盘，可进行开仓".format(goodsCode), freq)
                    else:
                        putLogBarDealEvent(ee, "{} 夜盘不开仓，仅平仓；日盘可开平，现在有夜盘，不进行开仓".format(goodsCode), freq)
                elif DayTradeEnable != 1 and NightTradeEnable == 1:
                    if TradeTime.time() > time(16) and TradeTime.time() < time(8):
                        putLogBarDealEvent(ee, "{} 日盘不开仓，仅平仓；夜盘可开平，现在是夜盘，可进行开仓".format(goodsCode), freq)
                    else:
                        putLogBarDealEvent(ee, "{} 日盘不开仓，仅平仓；夜盘可开平，现在有日盘，不进行开仓".format(goodsCode), freq)
                elif DayTradeEnable == 1 and NightTradeEnable == 1:
                    putLogBarDealEvent(ee, "{} 日盘夜盘均可开平".format(goodsCode), freq)
                    print(sendTradeDr)
                    pass
                elif DayTradeEnable != 1 and NightTradeEnable != 1:
                    putLogBarDealEvent(ee, "{} 日夜盘均不下单".format(goodsCode), freq)
    else:
        putLogBarDealEvent(ee, "{} 周交易明细有多仓位，实际没持仓，不开仓".format(goodsCode), freq)

def thePrint(event):
    print(event.dict_['log'])

if __name__ == '__main__':
    ee = EventEngine()
    ee.register(EVENT_LOG, thePrint)
    ee.register(EVENT_LOGBARDEAL, thePrint)
    ee.start(timer=False)
    theBar = "{'goods_code': 'bu1901.SHF', 'goods_name': '沥青', 'trade_time': datetime.datetime(2018, 9, 25, 9, 6), 'close': 3714.0, 'volume': 54972, 'amt': 2043954000.00, 'high': 3730, 'low': 3708, 'open': 3720, 'oi': 452054.0000}"
    getDictGoodsChg()
    dealBar(theBar, ee)


