from PyQt5.QtWidgets import QApplication, QMainWindow, QAction, \
    QStatusBar,QGroupBox,QTextEdit,QLineEdit, \
    QWidget,QHBoxLayout, QVBoxLayout, QMessageBox, \
    QTabWidget, QTableWidget,QPushButton, QTableWidgetItem, QLabel
from PyQt5.QtGui import QIcon, QFont, QTextCursor
from PyQt5.QtCore import Qt
import sys
import logging
from WindPy import w
import threading
from parameter import *
from function import *
from orderUi import *
from completeDb import *
from tdApi import *
import socket
import time as ttt

class RdMdUi(QMainWindow):
    def __init__(self):
        super().__init__()
        self.getLog()  # 事务日志初始化
        self.getUi()  # 生成框架
        self.getEngine()  # 创建主力事件

    def getLog(self):
        self.dictFreqLog = {}
        # 主事件
        self.logRuida = logging.getLogger("ruida")
        fileHandle = logging.FileHandler('logging\\ruida.txt')
        fileHandle.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
        self.logRuida.addHandler(fileHandle)
        self.logRuida.setLevel(logging.INFO)
        # tick数据
        self.logTick = logging.getLogger('tick')
        fileHandle = logging.FileHandler('logging\\tick\\{}.txt'.format(datetime.now().strftime('%Y-%m-%d')))
        fileHandle.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
        self.logTick.addHandler(fileHandle)
        self.logTick.setLevel(logging.INFO)
        # 下单日志
        for eachFreq in listFreq:
            self.log = logging.getLogger('CTA{}'.format(eachFreq))
            fileHandle = logging.FileHandler('logging\\CTA{}.txt'.format(eachFreq))
            fileHandle.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
            self.log.addHandler(fileHandle)
            self.log.setLevel(logging.INFO)
            self.dictFreqLog[eachFreq] = self.log  # 对应的分钟处理方法
    
    def getUi(self):
        self.font = QFont('微软雅黑', 16)
        self.setFont(self.font)
        self.setWindowTitle("CTA交易系统")
        self.setWindowIcon(QIcon('material\\icon.png'))
        self.setGeometry(200, 50, 1500, 1000)
        # 内部框架
        vbox0 = QVBoxLayout()
        gbox1 = QGroupBox('频段')
        gbox2 = QGroupBox('基本日志记录')
        vbox0.addWidget(gbox1, stretch=2)
        vbox0.addWidget(gbox2, stretch=1)
        # 频段
        tab = QTabWidget()
        self.TableFreqDuo = {}
        self.TableFreqKong = {}
        self.TableFreqPosition = {}
        self.TableFreqOrder = {}
        self.TableFreqTrade = {}
        for eachFreq in listFreq:
            tabSub = QTabWidget()
            # 做多与做空
            tabSub1 = QWidget()
            tableDuo = QTableWidget(0, len(listDuoKong), self)
            self.TableFreqDuo[eachFreq] = tableDuo
            tableDuo.setHorizontalHeaderLabels(listDuoKong)
            tableDuo.verticalHeader().setVisible(False)
            tableDuo.setFont(self.font)
            tableDuo.resizeColumnsToContents()
            tableKong = QTableWidget(0, len(listDuoKong), self)
            self.TableFreqKong[eachFreq] = tableKong
            tableKong.setHorizontalHeaderLabels(listDuoKong)
            tableKong.verticalHeader().setVisible(False)
            tableKong.setFont(self.font)
            tableKong.resizeColumnsToContents()
            vbox = QVBoxLayout()
            vbox.addWidget(QLabel('做多', self))
            vbox.addWidget(tableDuo)
            vbox.addWidget(QLabel('做空', self))
            vbox.addWidget(tableKong)
            tabSub1.setLayout(vbox)
            # 频段的持仓
            tabSub2 = QWidget()
            tablePosition = QTableWidget(0, len(listFreqPosition), self)
            tablePosition.setHorizontalHeaderLabels(listFreqPosition)
            for num in range(len(listFreqPosition)):
                tablePosition.horizontalHeaderItem(num).setFont(self.font)
            tablePosition.setFont(self.font)
            tablePosition.resizeColumnsToContents()
            tablePosition.verticalHeader().setVisible(False)
            self.TableFreqPosition[eachFreq] = tablePosition
            hbox = QHBoxLayout()
            hbox.addWidget(tablePosition)
            tabSub2.setLayout(hbox)
            # 频段的成交
            tabSub3 = QWidget()
            tableTrade = QTableWidget(0, len(listTrade), self)
            tableTrade.setHorizontalHeaderLabels(listTrade)
            for num in range(len(listTrade)):
                tableTrade.horizontalHeaderItem(num).setFont(self.font)
            tableTrade.setFont(self.font)
            tableTrade.resizeColumnsToContents()
            tableTrade.verticalHeader().setVisible(False)
            self.TableFreqTrade[eachFreq] = tableTrade
            hbox = QHBoxLayout()
            hbox.addWidget(tableTrade)
            tabSub3.setLayout(hbox)
            # 频段的委托
            tabSub4 = QWidget()
            tableOrder = QTableWidget(0, len(listOrder), self)
            tableOrder.setHorizontalHeaderLabels(listOrder)
            for num in range(len(listOrder)):
                tableOrder.horizontalHeaderItem(num).setFont(self.font)
            tableOrder.setFont(self.font)
            tableOrder.resizeColumnsToContents()
            tableOrder.verticalHeader().setVisible(False)
            self.TableFreqOrder[eachFreq] = tableOrder
            hbox = QHBoxLayout()
            hbox.addWidget(tableOrder)
            tabSub4.setLayout(hbox)
            tabSub.addTab(tabSub1, '做多与做空')
            tabSub.addTab(tabSub2, '频段持仓')
            tabSub.addTab(tabSub3, '频段成交')
            tabSub.addTab(tabSub4, '频段委托')
            tab.addTab(tabSub, 'CTA' + str(eachFreq))
        hbox = QHBoxLayout()
        hbox.addWidget(tab)
        gbox1.setLayout(hbox)
        # 日志输出与总持仓显示
        self.txtLog = QTextEdit(self)
        self.txtLog.setEnabled(True)
        hbox = QHBoxLayout()
        tab2 = QTabWidget()
        self.txtLog = QTextEdit(self)
        self.txtLog.setEnabled(True)
        tab2.addTab(self.txtLog, '程序日志')
        self.tabPosition = QWidget()
        self.tabPosition.setLayout(self.tabPositionUi())
        tab2.addTab(self.tabPosition, '账户持仓')
        self.tabAccount = QWidget()
        self.tabAccount.setLayout(self.tabAccountUi())
        tab2.addTab(self.tabAccount, '账户资金')
        tab2.currentChanged.connect(self.switchTab2)
        hbox.addWidget(tab2)
        gbox2.setLayout(hbox)
        # 菜单栏
        menu_root = self.menuBar()
        menu_root.setFont(self.font)
        orderhandle = menu_root.addMenu('手动下单操作')
        ordering = QAction('下单', self)
        ordering.setFont(self.font)
        ordering.triggered.connect(self.orderShow)
        orderhandle.addAction(ordering)

        orderingCancel = QAction('撤单', self)
        orderingCancel.setFont(self.font)
        orderingCancel.triggered.connect(self.orderCancelShow)
        orderhandle.addAction(orderingCancel)

        orderingPark = QAction('预下单', self)
        orderingPark.setFont(self.font)
        orderingPark.triggered.connect(self.orderParkShow)
        orderhandle.addAction(orderingPark)

        init = menu_root.addMenu('初始化操作')
        self.initItem = QAction('初始化开始', self)
        self.initItem.setFont(self.font)
        self.initItem.triggered.connect(self.getInit)
        init.addAction(self.initItem)

        start = menu_root.addMenu('交易中操作')
        self.startItem = QAction('交易开始', self)
        self.startItem.setFont(self.font)
        self.startItem.triggered.connect(self.getTrade)
        start.addAction(self.startItem)
        deleteDb = menu_root.addMenu('删除数据')
        self.dltItem = QAction('删除本交易日数据（含夜盘）', self)
        self.dltItem.setFont(self.font)
        self.dltItem.triggered.connect(self.theDlt)
        self.dltItem1 = QAction('删除本交易日数据（不含夜盘）', self)
        self.dltItem1.setFont(self.font)
        self.dltItem1.triggered.connect(self.theDlt1)
        self.dltItem2 = QAction('删除指定时间数据', self)
        self.dltItem2.setFont(self.font)
        self.dltItem2.triggered.connect(self.theDlt2)
        deleteDb.addAction(self.dltItem)
        deleteDb.addAction(self.dltItem1)
        deleteDb.addAction(self.dltItem2)
        # 总布局
        widget = QWidget()
        widget.setLayout(vbox0)
        self.setCentralWidget(widget)

    def tabPositionUi(self):  # 持仓量界面
        self.tablePositionColumns = listPosition
        self.tablePosition = QTableWidget(0, len(self.tablePositionColumns), self)
        self.tablePosition.setHorizontalHeaderLabels(self.tablePositionColumns)
        for num in range(len(self.tablePositionColumns)):
            self.tablePosition.horizontalHeaderItem(num).setFont(self.font)
        self.tablePosition.setFont(self.font)
        self.tablePosition.resizeColumnsToContents()
        self.tablePosition.verticalHeader().setVisible(False)
        self.tablePosition.setEnabled(True)
        hbox = QHBoxLayout()
        hbox.addWidget(self.tablePosition)
        return hbox

    def tabAccountUi(self):  # 读取资金的界面
        self.tableAccountColumns = listAccount
        self.tableAccount = QTableWidget(0, len(self.tableAccountColumns), self)
        self.tableAccount.setHorizontalHeaderLabels(self.tableAccountColumns)
        for num in range(len(self.tableAccountColumns)):
            self.tableAccount.horizontalHeaderItem(num).setFont(self.font)
        self.tableAccount.setFont(self.font)
        self.tableAccount.resizeColumnsToContents()
        self.tableAccount.verticalHeader().setVisible(False)
        self.tableAccount.setEnabled(True)
        hbox = QHBoxLayout()
        hbox.addWidget(self.tableAccount)
        return hbox

    def switchTab2(self):
        tab2 = self.sender()
        index = tab2.currentIndex()
        if index == 1:  # 说明当时在查询持仓了：
            try:
                # 先将旧持仓清空
                self.tablePosition.clearContents()
                self.tablePosition.setRowCount(0)
                self.td.getPosition()
            except:
                putLogEvent(self.ee, "查询持仓量失败")
        elif index == 2:
            try:
                # 先将旧持仓清空
                self.tableAccount.clearContents()
                self.tableAccount.setRowCount(0)
                self.td.getAccount()
            except:
                putLogEvent(self.ee, "查询账号资金失败")

    # 每天进行初始化数据操作
    def getInit(self):
        self.initItem.setEnabled(False)
        thd = threading.Thread(target=self.getWind, daemon=True)
        thd.start()

    def getWind(self):
        putLogEvent(self.ee, 'Wind连接中')
        w.start()
        if w.start().ErrorCode == 0:
            putLogEvent(self.ee, 'Wind连接成功')
            completeDb(self.ee)  # 检查数据库
        else:
            putLogEvent(self.ee, 'Wind连接失败，主线程关闭')

    # 进行接收Bar的数据操作
    def getTrade(self):
        self.startItem.setEnabled(False)
        thd = threading.Thread(target=self.getTd, daemon=True)
        thd.start()

    def getTd(self):
        # 生成tradeApi
        self.td = TdApi(self.ee)
        try:
            obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            obj.connect((host, port))
            while True:
                reply = obj.recv(4096)
                print(reply)
                print(reply.decode())
        except ConnectionRefusedError as err:
            putLogEvent(self.ee, str(err))

    # 删除数据
    def theDlt(self):
        threading.Thread(target=self.defDlt).start()

    def theDlt1(self):
        threading.Thread(target=self.defDlt1).start()

    def theDlt2(self):
        self.theDltShow = DelUi(self.ee)
        self.theDltShow.setWindowFlags(Qt.Dialog)
        self.theDltShow.setWindowModality(Qt.ApplicationModal)
        self.theDltShow.show()

    def defDlt(self):  # 删除本交易日以来的数据，包含夜盘
        now = datetime.now()
        now = datetime(now.year, now.month, now.day)
        if now in tradeDay:
            startTime = tradeDay[tradeDay.index(now) - 1] + timedelta(hours=16)
        else:
            temp = pd.Series(tradeDay)
            startTime = temp[temp < now].iat[-1] + timedelta(hours=16)
        putLogEvent(self.ee, "删除自{}到最新的数据".format(startTime))
        dltData(startTime)
        putLogEvent(self.ee, "删除自{}到最新的数据完成".format(startTime))

    def defDlt1(self):  # 删除本交易日以来的数据，不包含夜盘
        now = datetime.now()
        now = datetime(now.year, now.month, now.day)
        startTime = now + timedelta(hours=8)
        putLogEvent(self.ee, "删除自{}到最新的数据".format(startTime))
        dltData(startTime)
        putLogEvent(self.ee, "删除自{}到最新的数据完成".format(startTime))

    # 弹出框界面
    def orderShow(self): #显示下单命令
        self.theOrderShow = OrderUi(self.ee)
        self.theOrderShow.setWindowFlags(Qt.Dialog)
        self.theOrderShow.setWindowModality(Qt.ApplicationModal)
        self.theOrderShow.show()

    def orderCancelShow(self): # 手动撤单操作
        self.theOrderCancelShow = OrderCancelUi(self.ee)
        self.theOrderCancelShow.setWindowFlags(Qt.Dialog)
        self.theOrderCancelShow.setWindowModality(Qt.ApplicationModal)
        self.theOrderCancelShow.show()

    def orderParkShow(self): #显示下单命令
        self.theOrderParkShow = OrderParkUi(self.ee)
        self.theOrderParkShow.setWindowFlags(Qt.Dialog)
        self.theOrderParkShow.setWindowModality(Qt.ApplicationModal)
        self.theOrderParkShow.show()

    # 事件对应处理方法
    def getEngine(self):
        self.ee = EventEngine()
        self.ee.register(EVENT_LOG, self.pLog)
        self.ee.register(EVENT_LOGBARDEAL, self.pLogBarDeal)
        self.ee.register(EVENT_LOGTICK, self.pLogTick)
        self.ee.register(EVENT_POSITION, self.position)
        self.ee.register(EVENT_ACCOUNT, self.account)
        self.ee.register(EVENT_ORDER, self.order)
        self.ee.register(EVENT_TRADE, self.trade)
        self.ee.register(EVENT_ORDERCOMMAND, self.orderCommand)
        self.ee.register(EVENT_ORDERPARKCOMMAND, self.orderParkCommand)
        self.ee.register(EVENT_ORDERCANCEL, self.orderCancel)
        self.ee.register(EVENT_ORDERPARKCANCEL, self.orderParkCancel)
        self.ee.start(timer=False)
        putLogEvent(self.ee, '程序启动~~~~~~~~~~~~~~~~')
        putLogTickEvent(self.ee, '程序启动~~~~~~~~~~~~~~~~')
        for eachFreq in listFreq:
            putLogBarDealEvent(self.ee, '程序启动~~~~~~~~~~~~~~~~', eachFreq)

    def pLog(self, event):
        self.txtLog.append(event.dict_['log'])
        self.txtLog.moveCursor(QTextCursor.End)
        self.logRuida.info(event.dict_['log'])

    def pLogBarDeal(self, event):
        self.dictFreqLog[event.dict_['freq']].info(event.dict_['log'])

    def pLogTick(self, event):
        self.logTick.info(event.dict_['log'])

    # 查询委托单
    def order(self, event):
        var = event.dict_['data']
        tmp = {}
        tmp["本地下单码"] = var["OrderRef"].strip()
        tmp["时间"] = pd.Timestamp(var["InsertDate"] + ' ' + var["InsertTime"]).to_datetime()
        tmp["代码"] = var["InstrumentID"]
        if tmp["代码"][-4:].isdigit():
            tmp["代码"] = tmp["代码"] + '.' + dictGoodsChg[tmp["代码"][:-4]]
        else:
            tmp["代码"] = tmp["代码"] + '.' + dictGoodsChg[tmp["代码"][:-3]]
        if var["Direction"] == 'Buy':
            tmp["方向"] = "买/多"
            tmp["价格"] = var["LimitPrice"]
            tmp["数量"] = var["VolumeTotalOriginal"]
        else:
            tmp["方向"] = "卖/空"
            tmp["价格"] = var["LimitPrice"]
            tmp["数量"] = var["VolumeTotalOriginal"] * (-1)
        tmp["状态"] = var["StatusMsg"]
        tmp["已成交"] = var["VolumeTraded"]
        tmp["成交均价"] = var["LimitPrice"]
        tmp["拒绝原因"] = var["VolumeTraded"]
        # 本地下单码 日期 5 位  频段 2 位  品种 2 位  第几个 bar 数据 2 位  开仓 还是 平仓 1 位
        if len(tmp["本地下单码"]) == 12:
            freq = int(tmp["本地下单码"][5:7])

            dfOrder = dictFreqOrder[freq]
            dfOrderSource = dictFreqOrderSource[freq]
            tableOrder = self.TableFreqOrder[freq]
            if tmp["本地下单码"] not in dfOrder['本地下单码'].tolist():
                dfOrder.loc[dfOrder.shape[0]] = tmp
                dfOrderSource.loc[dfOrderSource.shape[0]] = var
                tableOrder.setRowCount(tableOrder.rowCount() + 1)
                for num in range(len(listOrder)):
                    tableOrder.setItem(tableOrder.rowCount() - 1, num,
                                       QTableWidgetItem(str(tmp[listOrder[num]])))
                tableOrder.resizeColumnsToContents()
            else:
                index = dfOrder[dfOrder['本地下单码'] == tmp["本地下单码"]].index[0]
                dfOrder.loc[index] = [tmp[x] for x in listOrder]
                dfOrderSource.loc[index] = [var[x] for x in listOrderColumns]
                for num in range(len(listOrder)):
                    tableOrder.setItem(index, num, QTableWidgetItem(str(tmp[listOrder[num]])))
                tableOrder.resizeColumnsToContents()

    # 查询成交单
    def trade(self, event):
        listTrade = ['本地下单码', '时间', '代码', '名称', '方向', '价格', '数量', '成本金额']
        var = event.dict_['data']
        tmp = {}
        tmp["本地下单码"] = var["OrderRef"].strip()
        tmp["时间"] = pd.Timestamp(var["TradeDate"] + ' ' + var["TradeTime"])
        tmp["代码"] = var["InstrumentID"]
        if tmp["代码"][-4:].isdigit():
            tmp["代码"] = tmp["代码"] + '.' + dictGoodsChg[tmp["代码"][:-4]]
            tmp["名称"] = dictGoodsName[var["InstrumentID"][:-4] + '.' + dictGoodsChg[var["InstrumentID"][:-4]]]
        else:
            tmp["代码"] = tmp["代码"] + '.' + dictGoodsChg[tmp["代码"][:-3]]
            tmp["名称"] = dictGoodsName[var["InstrumentID"][:-3] + '.' + dictGoodsChg[var["InstrumentID"][:-3]]]
        if var["Direction"] == 'Buy':
            tmp["方向"] = "买/多"
            tmp["数量"] = var["Volume"]
        else:
            tmp["方向"] = "卖/空"
            tmp["数量"] = var["Volume"] * (-1)
        tmp["价格"] = var["Price"]
        tmp["成本金额"] = var["PriceSource"]
        # 本地下单码 日期 5 位  频段 2 位  品种 2 位  第几个 bar 数据 2 位  开仓 还是 平仓 1 位
        if len(tmp["本地下单码"]) == 12:
            freq = int(tmp["本地下单码"][5:7])
            tableTrade = self.TableFreqTrade[freq]
            dictFreqTrade[freq].loc[dictFreqTrade[freq].shape[0]] = tmp
            tableTrade.setRowCount(tableTrade.rowCount() + 1)
            for num in range(len(listTrade)):
                tableTrade.setItem(tableTrade.rowCount() - 1, num,
                                   QTableWidgetItem(str(tmp[listTrade[num]])))
            tableTrade.resizeColumnsToContents()
            # 加入到持仓上：
            tablePosition = self.TableFreqPosition[freq]
            if tmp["代码"] not in dictFreqPosition[freq]['代码']:
                dictFreqPosition[freq].loc[dictFreqPosition[freq].shape[0]] = [tmp[x] for x in listFreqPosition]
                tablePosition.setRowCount(tablePosition.rowCount() + 1)
                for num in range(len(listFreqPosition)):
                    tablePosition.setItem(tablePosition.rowCount() - 1, num,
                                          QTableWidgetItem(str(tmp[listOrder[num]])))
                tablePosition.resizeColumnsToContents()
            else:
                index = dictFreqPosition[freq][dictFreqPosition[freq]['代码'] == tmp["代码"]].index[0]
                tmp['数量'] = tmp['数量'] + dictFreqPosition[freq]['数量'][index]
                dictFreqPosition[eachFreq].loc[index] = [tmp[x] for x in listFreqPosition]
                for num in range(len(listFreqPosition)):
                    tablePosition.setItem(index, num,
                                          QTableWidgetItem(str(tmp[listFreqPosition[num]])))
                tablePosition.resizeColumnsToContents()

    def position(self, event):
        var = event.dict_['data']
        tmp = {}
        tmp["代码"] = var["InstrumentID"]
        if tmp["代码"][-4:].isdigit():
            goodsCode = tmp["代码"][:-4] + '.' + dictGoodsChg[tmp["代码"][:-4]]
        else:
            goodsCode = tmp["代码"][:-3] + '.' + dictGoodsChg[tmp["代码"][:-3]]
        tmp["名称"] = dictGoodsName[goodsCode]
        if var["PosiDirection"] == 'Long':
            tmp["方向"] = "买/多"
            tmp["数量"] = var["Position"]
            if tmp["数量"] == 0:
                return None
        else:
            tmp["方向"] = "卖/空"
            tmp["数量"] = var["Position"] * (-1)
            if tmp["数量"] == 0:
                return None
        tmp["多头冻结"] = var["LongFrozen"]
        tmp["空头冻结"] = var["ShortFrozen"]
        tmp["持仓成本"] = round(var["PositionCost"], 2)
        tmp["持仓盈亏"] = var["PositionProfit"]
        tmp["开仓成本"] = round(var["OpenCost"], 2)
        tmp["今日持仓"] = var["TodayPosition"]
        self.tablePosition.setRowCount(self.tablePosition.rowCount() + 1)
        for num in range(len(self.tablePositionColumns)):
            self.tablePosition.setItem(self.tablePosition.rowCount() - 1, num,
                                        QTableWidgetItem(str(tmp[self.tablePositionColumns[num]])))
        self.tablePosition.resizeColumnsToContents()

    def account(self, event):
        var = event.dict_['data']
        tmp = {}
        tmp["经纪公司代码"] = var["BrokerID"]
        tmp["投资者帐号"] = var["AccountID"]
        tmp["上次存款额"] = round(var["PreDeposit"], 2)
        tmp["上次结算准备金"] = round(var["PreBalance"], 2)
        tmp["上次占用的保证金"] = var["PreMargin"]
        tmp["当前保证金总额"] = round(var["CurrMargin"], 2)
        tmp["可用资金"] = round(var["Available"], 2)
        tmp["可取资金"] = round(var["WithdrawQuota"], 2)
        self.tableAccount.setRowCount(self.tableAccount.rowCount() + 1)
        for num in range(len(self.tableAccountColumns)):
            self.tableAccount.setItem(self.tableAccount.rowCount() - 1, num,
                                       QTableWidgetItem(str(tmp[self.tableAccountColumns[num]])))
        self.tableAccount.resizeColumnsToContents()

    # 手动下单操作
    def orderCommand(self, event):
        try:
            # if event.dict_['OrderPriceType'] == OrderPriceTypeType.LimitPrice:
            if event.dict_['Direction'] == DirectionType.Buy: #多空
                if event.dict_['CombOffsetFlag'] == OffsetFlagType.Open.__char__(): #多开
                    self.td.buy(event.dict_['InstrumentID'], event.dict_['orderref'], event.dict_['LimitPrice'], event.dict_['VolumeTotalOriginal'])
                else: #多平
                    if event.dict_['OrderPriceType'] == OrderPriceTypeType.LimitPrice:
                        self.td.sell(event.dict_['InstrumentID'], event.dict_['orderref'],  event.dict_['LimitPrice'],
                                     event.dict_['VolumeTotalOriginal'])
                    else:
                        self.td.sellMarket(event.dict_['InstrumentID'], event.dict_['orderref'],event.dict_['VolumeTotalOriginal'])
            else:
                if event.dict_['CombOffsetFlag'] == OffsetFlagType.Open.__char__(): #空开
                    self.td.short(event.dict_['InstrumentID'], event.dict_['orderref'], event.dict_['LimitPrice'],
                                  event.dict_['VolumeTotalOriginal'])
                else: #空平
                    if event.dict_['OrderPriceType'] == OrderPriceTypeType.LimitPrice:
                        self.td.cover(event.dict_['InstrumentID'], event.dict_['orderref'], event.dict_['LimitPrice'],
                                      event.dict_['VolumeTotalOriginal'])
                    else:
                        self.td.coverMarket(event.dict_['InstrumentID'], event.dict_['orderref'], event.dict_['VolumeTotalOriginal'])
        except:
            putLogEvent(self.ee, '手动下单失败（有可能还没有登陆帐户）')

    def orderParkCommand(self, event):
        try:
            if event.dict_['Direction'] == DirectionType.Buy: #多空
                if event.dict_['CombOffsetFlag'] == OffsetFlagType.Open.__char__(): #多开
                    self.td.buyPark(event.dict_['InstrumentID'], event.dict_['orderref'], event.dict_['LimitPrice'], event.dict_['VolumeTotalOriginal'])
                else: #多平
                    if event.dict_['OrderPriceType'] == OrderPriceTypeType.LimitPrice:
                        self.td.sellPark(event.dict_['InstrumentID'], event.dict_['orderref'],  event.dict_['LimitPrice'],
                                     event.dict_['VolumeTotalOriginal'])
                    else:
                        self.td.sellMarketPark(event.dict_['InstrumentID'], event.dict_['orderref'],event.dict_['VolumeTotalOriginal'])
            else:
                if event.dict_['CombOffsetFlag'] == OffsetFlagType.Open.__char__(): #空开
                    self.td.shortPark(event.dict_['InstrumentID'], event.dict_['orderref'], event.dict_['LimitPrice'],
                                  event.dict_['VolumeTotalOriginal'])
                else: #空平
                    if event.dict_['OrderPriceType'] == OrderPriceTypeType.LimitPrice:
                        self.td.coverPark(event.dict_['InstrumentID'], event.dict_['orderref'], event.dict_['LimitPrice'],
                                      event.dict_['VolumeTotalOriginal'])
                    else:
                        self.td.coverMarketPark(event.dict_['InstrumentID'], event.dict_['orderref'],event.dict_['VolumeTotalOriginal'])
        except:
            putLogEvent(self.ee, '手动下单失败（有可能还没有登陆帐户）')

    def orderCancel(self, event):
        orderref = event.dict_['orderref']
        if len(orderref) == 12:
            freq = int(orderref[5:7])
            if orderref in dictFreqOrderSource[freq]['OrderRef'].tolist():
                index = dictFreqOrderSource[freq]['OrderRef'].tolist().index(orderref)
                if dictFreqOrderSource[freq]['StatusMsg'][index] not in ["全部成交报单已提交", "已撤单"]:
                    dict = dictFreqOrderSource[freq].loc[index].to_dict()
                    self.td.cancelOrder(dict)

    def orderParkCancel(self, event):
        orderref = event.dict_['orderref']
        if len(orderref) == 12:
            freq = int(orderref[5:7])
            if orderref in dictFreqOrderSource[freq]['OrderRef'].tolist():
                index = dictFreqOrderSource[freq]['OrderRef'].tolist().index(orderref)
                if dictFreqOrderSource[freq]['StatusMsg'][index] not in ["全部成交报单已提交", "已撤单"]:
                    dict = dictFreqOrderSource[freq].loc[index].to_dict()
                    self.td.cancelOrderPark(dict)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ui = RdMdUi()
    ui.show()
    sys.exit(app.exec_())