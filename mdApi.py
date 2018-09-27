from py_ctp.eventEngine import *
from py_ctp.eventType import *
from py_ctp.quote import Quote
import pandas as pd
from datetime import *
import logging
from PyQt5.QtCore import QCoreApplication
import sys
from function import *

class MdApi:
    def __init__(self, ee):
        self.ee = ee
        self.list_account = ['申万实盘', '中证实盘', '国泰君安实盘', '广发实盘', '中国国际实盘']
        self.list_server_brokerid = ["88888", "66666", "7090", "9000", "8090"]
        self.list_server_investorid = ["8701000683", "830300035", "28900528", "886810370", "33305188"]
        self.list_server_password = ["600467", "600467", "600467", "600467", "600467"]
        self.list_server_address = ["tcp://180.168.212.51:41213",
                                    "tcp://ctp1-md7.citicsf.com:41213",
                                    "tcp://180.169.75.21:41213",
                                    "tcp://116.228.246.81:41213",
                                    "tcp://180.168.102.193:41213"]
        self.choice = 0
        self.userid = self.list_server_investorid[self.choice]
        self.password = self.list_server_password[self.choice]
        self.brokerid = self.list_server_brokerid[self.choice]
        self.address = self.list_server_address[self.choice]
        # 创建Quote对象
        self.q = Quote()
        api = self.q.CreateApi()
        spi = self.q.CreateSpi()
        self.q.RegisterSpi(spi)
        self.q.OnFrontConnected = self.onFrontConnected  # 交易服务器登陆相应
        self.q.OnFrontDisconnected = self.onFrontDisconnected
        self.q.OnRspUserLogin = self.onRspUserLogin  # 用户登陆
        self.q.OnRspUserLogout = self.onRspUserLogout  # 用户登出
        self.q.OnRspError = self.onRspError
        self.q.OnRspSubMarketData = self.onRspSubMarketData
        self.q.OnRtnDepthMarketData = self.onRtnDepthMarketData
        self.q.RegCB()
        self.q.RegisterFront(self.address)
        self.q.Init()
        self.islogin = False  # 判断是否登陆成功

    def onFrontConnected(self):
        """服务器连接"""
        putLogEvent(self.ee, '行情服务器连接成功')
        self.q.ReqUserLogin(BrokerID=self.brokerid, UserID=self.userid, Password=self.password)

    def onFrontDisconnected(self, n):
        """服务器断开"""
        putLogEvent(self.ee, '行情服务器连接断开')

    def onRspUserLogin(self, data, error, n, last):
        """登陆回报"""
        if error.__dict__['ErrorID'] == 0:
            log = '行情服务器登陆成功'
            self.islogin = True
        else:
            log = '行情服务器登陆回报，错误代码：' + str(error.__dict__['ErrorID']) + \
                  ',   错误信息：' + str(error.__dict__['ErrorMsg'])
        putLogEvent(self.ee, log)

    def onRspUserLogout(self, data, error, n, last):
        if error.__dict__['ErrorID'] == 0:
            log = '行情服务器登出成功'
            self.islogin = False
        else:
            log = '行情服务器登出回报，错误代码：' + str(error.__dict__['ErrorID']) + \
                  ',   错误信息：' + str(error.__dict__['ErrorMsg'])
        putLogEvent(self.ee, log)

    def onRspError(self, error, n, last):
        """错误回报"""
        log = '行情错误回报，错误代码：' + str(error.__dict__['ErrorID']) \
              + '错误信息：' + + str(error.__dict__['ErrorMsg'])
        putLogEvent(self.ee, log)

    def onRspSubMarketData(self, data, info, n, last):
        pass

    def onRtnDepthMarketData(self, data):
        """行情推送"""
        event = Event(type_=EVENT_MARKETDATA_CONTRACT)
        event.dict_['data'] = data.__dict__
        self.ee.put(event)
