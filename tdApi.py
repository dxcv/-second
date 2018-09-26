from py_ctp.trade import Trade
from parameter import *
from function import *

class TdApi:
    def __init__(self, ee):
        self.ee = ee
        # 初始化账号
        self.t = Trade()
        self.userid = '096114'
        self.password = 'cheng1234567'
        self.brokerid = '9999'
        api = self.t.CreateApi()
        spi = self.t.CreateSpi()
        self.t.RegisterSpi(spi)
        self.t.OnFrontConnected = self.onFrontConnected  # 交易服务器登陆相应
        self.t.OnFrontDisconnected = self.onFrontDisconnected
        self.t.OnRspUserLogin = self.onRspUserLogin  # 用户登陆
        self.t.OnRspUserLogout = self.onRspUserLogout  # 用户登出
        self.t.OnRtnInstrumentStatus = self.onRtnInstrumentStatus
        self.t.OnRspQryInstrument = self.onRspQryInstrument  # 查询全部交易合约
        self.t.OnRspSettlementInfoConfirm = self.onRspSettlementInfoConfirm  # 结算单确认，显示登陆日期
        self.t.OnRspQryInvestorPosition = self.onRspQryInvestorPosition  # 查询持仓
        self.t.OnRspQryTradingAccount = self.onRspQryTradingAccount  # 查询账户
        self.t.OnRtnOrder = self.onRtnOrder  # 报单
        self.t.OnRtnTrade = self.onRtnTrade  # 成交
        self.t.OnRspParkedOrderInsert = self.onRspParkedOrderInsert
        # self.t.OnErrRtnOrderInsert = self.onErrRtnOrderInsert
        # self.t.OnRspQryOrder = self.onRspQryOrder  # 当日委托
        self.t.RegCB()
        self.t.RegisterFront('tcp://180.168.146.187:10000')
        self.t.Init()
        self.islogin = False

    def onFrontConnected(self):
        """服务器连接"""
        putLogEvent(self.ee, '交易服务器连接成功')
        self.t.ReqUserLogin(BrokerID=self.brokerid,
                            UserID=self.userid, Password=self.password)

    def onFrontDisconnected(self, n):
        putLogEvent(self.ee, '交易服务器连接断开')

    def onRspUserLogin(self, data, error, n, last):
        """登陆回报"""
        if error.__dict__['ErrorID'] == 0:
            self.Investor = data.__dict__['UserID']
            self.BrokerID = data.__dict__['BrokerID']
            log = data.__dict__['UserID'] + '交易服务器登陆成功'
            self.islogin = True
            self.t.ReqSettlementInfoConfirm(self.BrokerID, self.Investor)  # 对账单确认
        else:
            log = '交易服务器登陆回报，错误代码：' + str(error.__dict__['ErrorID']) + \
                  ',   错误信息：' + str(error.__dict__['ErrorMsg'])
        putLogEvent(self.ee, log)

    def onRspUserLogout(self, data, error, n, last):
        if error.__dict__['ErrorID'] == 0:
            log = '交易服务器登出成功'
            self.islogin = False
        else:
            log = '交易服务器登出回报，错误代码：' + str(error.__dict__['ErrorID']) + \
                  ',   错误信息：' + str(error.__dict__['ErrorMsg'])
        putLogEvent(self.ee, log)

    def onRtnInstrumentStatus(self, data):
        pass

    def onRspQryInstrument(self, data, error, n, last):
        pass

    def onRspSettlementInfoConfirm(self, data, error, n, last):
        """确认结算信息回报"""
        log = '结算信息确认完成'
        putLogEvent(self.ee, log)
        putLogEvent(self.ee, str(data))

    def onRspQryInvestorPosition(self, data, error, n, last):
        """持仓查询回报"""
        if error.__dict__['ErrorID'] == 0:
            event = Event(type_=EVENT_POSITION)
            event.dict_['data'] = data.__dict__
            event.dict_['last'] = last
            self.ee.put(event)
        else:
            log = ('持仓查询回报，错误代码：'  +str(error.__dict__['ErrorID']) +
                   ',   错误信息：' +str(error.__dict__['ErrorMsg']))
            putLogEvent(self.ee, log)

    def getPosition(self):
        putLogEvent(self.ee, "读取账号持仓情况")
        self.t.ReqQryInvestorPosition(self.brokerid, self.userid)

    def onRspQryTradingAccount(self, data, error, n, last):
        """资金账户查询回报"""
        if error.__dict__['ErrorID'] == 0:
            event = Event(type_=EVENT_ACCOUNT)
            event.dict_['data'] = data.__dict__
            self.ee.put(event)
        else:
            log = ('账户查询回报，错误代码：' +str(error.__dict__['ErrorID']) + ',   错误信息：' +str(error.__dict__['ErrorMsg']))
            putLogEvent(self.ee, log)

    def getAccount(self):
        putLogEvent(self.ee, "读取账户的资金信息")
        self.t.ReqQryTradingAccount(self.brokerid, self.userid)

    def onRtnOrder(self, data):
        # 常规报单事件
        print('Order')
        print(data.__dict__)
        event = Event(type_=EVENT_ORDER)
        event.dict_['data'] = data.__dict__
        self.ee.put(event)

    def onRtnTrade(self, data):
        """成交回报"""
        print('Trade')
        print(data.__dict__)
        event = Event(type_=EVENT_TRADE)
        event.dict_['data'] = data.__dict__
        self.ee.put(event)

    def onRspParkedOrderInsert(self, data=CThostFtdcParkedOrderField, pRspInfo=CThostFtdcRspInfoField,
                               nRequestID=int, bIsLast=bool):
        print('TradePark')
        print(data.__dict__)
        event = Event(type_=EVENT_ORDERPARK)
        event.dict_['data'] = data.__dict__
        self.ee.put(event)

    # 下单操作：
    def sendorder(self, instrumentid, orderref, price, vol, direction, offset, OrderPriceType = OrderPriceTypeType.LimitPrice):
        self.t.ReqOrderInsert(BrokerID=self.brokerid,
                              InvestorID=self.userid,
                              InstrumentID=instrumentid,
                              OrderRef=orderref,
                              UserID=self.userid,
                              OrderPriceType=OrderPriceType,
                              Direction=direction,
                              CombOffsetFlag=offset,
                              CombHedgeFlag=HedgeFlagType.Speculation.__char__(),
                              LimitPrice=price,
                              VolumeTotalOriginal=vol,
                              TimeCondition=TimeConditionType.GFD,
                              VolumeCondition=VolumeConditionType.AV,
                              MinVolume=1,
                              ForceCloseReason=ForceCloseReasonType.NotForceClose,
                              ContingentCondition=ContingentConditionType.Immediately)
        return orderref

    def buy(self, symbol,orderref, price, vol): #多开
        direction = DirectionType.Buy
        offset = OffsetFlagType.Open.__char__()
        self.sendorder(symbol,orderref, price, vol, direction, offset)
    
    def sell(self, symbol,orderref, price, vol): #多平
        direction = DirectionType.Sell
        offset = OffsetFlagType.Close.__char__()
        self.sendorder(symbol,orderref, price, vol, direction, offset)

    def sellMarket(self, symbol,orderref, vol): #多平
        direction = DirectionType.Sell
        offset = OffsetFlagType.Close.__char__()
        self.sendorder(symbol,orderref, 0, vol, direction, offset, OrderPriceTypeType.AnyPrice)

    def selltoday(self, symbol,orderref, price, vol):  # 平今多
        direction = DirectionType.Sell
        offset = OffsetFlagType.CloseToday.__char__()
        self.sendorder(symbol,orderref, price, vol, direction, offset)

    def short(self, symbol,orderref, price, vol):  # 卖开空开
        direction = DirectionType.Sell
        offset = OffsetFlagType.Open.__char__()
        self.sendorder(symbol,orderref, price, vol, direction, offset)

    def cover(self, symbol,orderref, price, vol):  # 空平
        direction = DirectionType.Buy
        offset = OffsetFlagType.Close.__char__()
        self.sendorder(symbol,orderref, price, vol, direction, offset)

    def coverMarket(self, symbol,orderref, vol):  # 空平
        direction = DirectionType.Buy
        offset = OffsetFlagType.Close.__char__()
        self.sendorder(symbol,orderref, 0, vol, direction, offset, OrderPriceTypeType.AnyPrice)

    def covertoday(self, symbol,orderref, price, vol):  # 平今空
        direction = DirectionType.Buy
        offset = OffsetFlagType.CloseToday.__char__()
        self.sendorder(symbol,orderref, price, vol, direction, offset)

    def cancelOrder(self, order):
        """撤单"""
        self.t.ReqOrderAction(BrokerID=self.brokerid,
                              InvestorID=self.userid,
                              OrderRef=order['OrderRef'],
                              FrontID=int(order['FrontID']),
                              SessionID=int(order['SessionID']),
                              OrderSysID=order['OrderSysID'],
                              ActionFlag=ActionFlagType.Delete,
                              ExchangeID=order["ExchangeID"],
                              InstrumentID=order['InstrumentID'])

    # 预埋单
    def sendorderPark(self, instrumentid, orderref, price, vol, direction, offset, OrderPriceType=OrderPriceTypeType.LimitPrice):
        self.t.ReqParkedOrderInsert(BrokerID=self.brokerid,
                              InvestorID=self.userid,
                              InstrumentID=instrumentid,
                              OrderRef=orderref,
                              UserID=self.userid,
                              # OrderPriceType=OrderPriceTypeType.LimitPrice,
                              OrderPriceType=OrderPriceType,
                              Direction=direction,
                              CombOffsetFlag=offset,
                              CombHedgeFlag=HedgeFlagType.Speculation.__char__(),
                              LimitPrice=price,
                              VolumeTotalOriginal=vol,
                              TimeCondition=TimeConditionType.GFD,
                              VolumeCondition=VolumeConditionType.AV,
                              MinVolume=1,
                              ForceCloseReason=ForceCloseReasonType.NotForceClose,
                              ContingentCondition=ContingentConditionType.Immediately)
        return orderref

    def buyPark(self, symbol, orderref, price, vol):  # 多开
        direction = DirectionType.Buy
        offset = OffsetFlagType.Open.__char__()
        self.sendorderPark(symbol, orderref, price, vol, direction, offset)

    def sellPark(self, symbol, orderref, price, vol):  # 多平
        direction = DirectionType.Sell
        offset = OffsetFlagType.Close.__char__()
        self.sendorderPark(symbol, orderref, price, vol, direction, offset)

    def sellMarketPark(self, symbol, orderref, vol):  # 多平
        direction = DirectionType.Sell
        offset = OffsetFlagType.Close.__char__()
        self.sendorderPark(symbol, orderref, 0, vol, direction, offset, OrderPriceTypeType.AnyPrice)

    def selltodayPark(self, symbol, orderref, price, vol):  # 平今多
        direction = DirectionType.Sell
        offset = OffsetFlagType.CloseToday.__char__()
        self.sendorderPark(symbol, orderref, price, vol, direction, offset)

    def shortPark(self, symbol, orderref, price, vol):  # 卖开空开
        direction = DirectionType.Sell
        offset = OffsetFlagType.Open.__char__()
        self.sendorderPark(symbol, orderref, price, vol, direction, offset)

    def coverPark(self, symbol, orderref, price, vol):  # 空平
        direction = DirectionType.Buy
        offset = OffsetFlagType.Close.__char__()
        self.sendorderPark(symbol, orderref, price, vol, direction, offset)

    def coverMarketPark(self, symbol, orderref, vol):  # 空平
        direction = DirectionType.Buy
        offset = OffsetFlagType.Close.__char__()
        self.sendorderPark(symbol, orderref, 0, vol, direction, offset, OrderPriceTypeType.AnyPrice)

    def covertodayPark(self, symbol, orderref, price, vol):  # 平今空
        direction = DirectionType.Buy
        offset = OffsetFlagType.CloseToday.__char__()
        self.sendorderPark(symbol, orderref, price, vol, direction, offset)

    def cancelOrderPark(self, order):
        """预撤单"""
        self.t.ReqParkedOrderAction(BrokerID=self.brokerid,
                              InvestorID=self.userid,
                              OrderRef=order['OrderRef'],
                              FrontID=int(order['FrontID']),
                              SessionID=int(order['SessionID']),
                              OrderSysID=order['OrderSysID'],
                              ActionFlag=ActionFlagType.Delete,
                              ExchangeID=order["ExchangeID"],
                              InstrumentID=order['InstrumentID'])