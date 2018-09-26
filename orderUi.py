from PyQt5.QtWidgets import QApplication, QMainWindow, QAction, \
    QStatusBar,QGroupBox,QTextEdit,QLineEdit, \
    QWidget,QHBoxLayout, QVBoxLayout, QMessageBox, QTabWidget, \
    QTableWidget,QLabel,QRadioButton,QGridLayout, QButtonGroup, QPushButton,QDateTimeEdit
from PyQt5.QtGui import QIcon, QFont, QTextCursor
import sys
from py_ctp.eventEngine import  *
from py_ctp.eventType import  *
from py_ctp.ctp_struct import *
from function import *
import threading
from dltData import *

class OrderUi(QMainWindow):
    def __init__(self, ee):
        super().__init__()
        self.ee = ee
        self.getUi()

    def getUi(self):
        self.setWindowTitle('下单操作')
        lbl0 = QLabel('证券代码：', self)
        lbl00 = QLabel('下单编号：', self)
        lbl1 = QLabel('多空方向：', self)
        lbl2 = QLabel('开仓平仓：', self)
        lbl3 = QLabel('类型：', self)
        lbl4 = QLabel('价格：', self)
        lbl5 = QLabel('数量：', self)
        self.lineedit_code = QLineEdit(self)
        self.lineedit_code00 = QLineEdit(self)
        self.lineedit_price = QLineEdit(self)
        self.lineedit_num = QLineEdit(self)
        self.rb_direction0 = QRadioButton('做多', self)
        self.rb_direction0.setChecked(True)
        self.rb_direction1 = QRadioButton('做空', self)
        btng0 = QButtonGroup(self)
        btng0.addButton(self.rb_direction0)
        btng0.addButton(self.rb_direction1)
        self.rb_hold0 = QRadioButton('开仓', self)
        self.rb_hold0.setChecked(True)
        self.rb_hold1 = QRadioButton('平仓', self)
        btng1 = QButtonGroup(self)
        btng1.addButton(self.rb_hold0)
        btng1.addButton(self.rb_hold1)
        self.rb_type0 = QRadioButton('市价', self)
        self.rb_type1 = QRadioButton('限价', self)
        self.rb_type1.setChecked(True)
        btng2 = QButtonGroup(self)
        btng2.addButton(self.rb_type0)
        btng2.addButton(self.rb_type1)
        btnComfirm = QPushButton('下单', self)
        btnComfirm.clicked.connect(self.commandOrder)
        grid = QGridLayout()
        grid.addWidget(lbl0, 0, 0)
        grid.addWidget(self.lineedit_code, 0, 1, 1, 2)
        grid.addWidget(lbl00, 1, 0)
        grid.addWidget(self.lineedit_code00, 1, 1, 1, 2)
        grid.addWidget(lbl1, 2, 0)
        grid.addWidget(self.rb_direction0, 2, 1)
        grid.addWidget(self.rb_direction1, 2, 2)
        grid.addWidget(lbl2, 3, 0)
        grid.addWidget(self.rb_hold0, 3, 1)
        grid.addWidget(self.rb_hold1, 3, 2)
        grid.addWidget(lbl3, 4, 0)
        grid.addWidget(self.rb_type0, 4, 1)
        grid.addWidget(self.rb_type1, 4, 2)
        grid.addWidget(lbl4, 5, 0)
        grid.addWidget(self.lineedit_price, 5, 1, 1, 2)
        grid.addWidget(lbl5,6, 0)
        grid.addWidget(self.lineedit_num, 6, 1, 1, 2)
        grid.addWidget(btnComfirm, 7, 2)
        w = QWidget()
        w.setLayout(grid)
        self.setCentralWidget(w)

    def commandOrder(self):
        try:
            orderEvent = Event(type_=EVENT_ORDERCOMMAND)
            orderEvent.dict_['InstrumentID'] = self.lineedit_code.text()
            if self.rb_direction0.isChecked():
                orderEvent.dict_['Direction'] = DirectionType.Buy
            else:
                orderEvent.dict_['Direction'] = DirectionType.Sell
            orderEvent.dict_['orderref'] = self.lineedit_code00.text()
            if self.rb_hold0.isChecked():
                orderEvent.dict_['CombOffsetFlag'] = OffsetFlagType.Open.__char__()
            else:
                orderEvent.dict_['CombOffsetFlag'] = OffsetFlagType.Close.__char__()
            if self.rb_type0.isChecked():
                orderEvent.dict_['OrderPriceType'] = OrderPriceTypeType.AnyPrice
            else:
                orderEvent.dict_['OrderPriceType'] = OrderPriceTypeType.LimitPrice
            orderEvent.dict_['LimitPrice'] = float(self.lineedit_price.text())
            orderEvent.dict_['VolumeTotalOriginal'] = int(self.lineedit_num.text())
            self.ee.put(orderEvent)
            self.close()
        except:
            QMessageBox.warning(self, '警告', '数据格式有误。')

class OrderParkUi(QMainWindow):
    def __init__(self, ee):
        super().__init__()
        self.ee = ee
        self.getUi()

    def getUi(self):
        self.setWindowTitle('下单操作')
        lbl0 = QLabel('证券代码：', self)
        lbl00 = QLabel('下单编号：', self)
        lbl1 = QLabel('多空方向：', self)
        lbl2 = QLabel('开仓平仓：', self)
        lbl3 = QLabel('类型：', self)
        lbl4 = QLabel('价格：', self)
        lbl5 = QLabel('数量：', self)
        self.lineedit_code = QLineEdit(self)
        self.lineedit_code00 = QLineEdit(self)
        self.lineedit_price = QLineEdit(self)
        self.lineedit_num = QLineEdit(self)
        self.rb_direction0 = QRadioButton('做多', self)
        self.rb_direction0.setChecked(True)
        self.rb_direction1 = QRadioButton('做空', self)
        btng0 = QButtonGroup(self)
        btng0.addButton(self.rb_direction0)
        btng0.addButton(self.rb_direction1)
        self.rb_hold0 = QRadioButton('开仓', self)
        self.rb_hold0.setChecked(True)
        self.rb_hold1 = QRadioButton('平仓', self)
        btng1 = QButtonGroup(self)
        btng1.addButton(self.rb_hold0)
        btng1.addButton(self.rb_hold1)
        self.rb_type0 = QRadioButton('市价', self)
        self.rb_type1 = QRadioButton('限价', self)
        self.rb_type1.setChecked(True)
        btng2 = QButtonGroup(self)
        btng2.addButton(self.rb_type0)
        btng2.addButton(self.rb_type1)
        btnComfirm = QPushButton('下单', self)
        btnComfirm.clicked.connect(self.commandOrder)
        grid = QGridLayout()
        grid.addWidget(lbl0, 0, 0)
        grid.addWidget(self.lineedit_code, 0, 1, 1, 2)
        grid.addWidget(lbl00, 1, 0)
        grid.addWidget(self.lineedit_code00, 1, 1, 1, 2)
        grid.addWidget(lbl1, 2, 0)
        grid.addWidget(self.rb_direction0, 2, 1)
        grid.addWidget(self.rb_direction1, 2, 2)
        grid.addWidget(lbl2, 3, 0)
        grid.addWidget(self.rb_hold0, 3, 1)
        grid.addWidget(self.rb_hold1, 3, 2)
        grid.addWidget(lbl3, 4, 0)
        grid.addWidget(self.rb_type0, 4, 1)
        grid.addWidget(self.rb_type1, 4, 2)
        grid.addWidget(lbl4, 5, 0)
        grid.addWidget(self.lineedit_price, 5, 1, 1, 2)
        grid.addWidget(lbl5,6, 0)
        grid.addWidget(self.lineedit_num, 6, 1, 1, 2)
        grid.addWidget(btnComfirm, 7, 2)
        w = QWidget()
        w.setLayout(grid)
        self.setCentralWidget(w)

    def commandOrder(self):
        try:
            orderEvent = Event(type_=EVENT_ORDERPARKCOMMAND)
            orderEvent.dict_['InstrumentID'] = self.lineedit_code.text()
            if self.rb_direction0.isChecked():
                orderEvent.dict_['Direction'] = DirectionType.Buy
            else:
                orderEvent.dict_['Direction'] = DirectionType.Sell
            orderEvent.dict_['orderref'] = self.lineedit_code00.text()
            if self.rb_hold0.isChecked():
                orderEvent.dict_['CombOffsetFlag'] = OffsetFlagType.Open.__char__()
            else:
                orderEvent.dict_['CombOffsetFlag'] = OffsetFlagType.Close.__char__()
            if self.rb_type0.isChecked():
                orderEvent.dict_['OrderPriceType'] = OrderPriceTypeType.AnyPrice
            else:
                orderEvent.dict_['OrderPriceType'] = OrderPriceTypeType.LimitPrice
            orderEvent.dict_['LimitPrice'] = float(self.lineedit_price.text())
            orderEvent.dict_['VolumeTotalOriginal'] = int(self.lineedit_num.text())
            self.ee.put(orderEvent)
            self.close()
        except:
            QMessageBox.warning(self, '警告', '数据格式有误。')

class OrderCancelUi(QMainWindow):
    def __init__(self, ee):
        super().__init__()
        self.ee = ee
        self.getUi()

    def getUi(self):
        self.setWindowTitle('撤单操作')
        lbl00 = QLabel('撤单编号：', self)
        self.lineedit_code00 = QLineEdit(self)
        btnComfirm = QPushButton('撤单', self)
        btnComfirm.clicked.connect(self.commandOrder)
        grid = QGridLayout()
        grid.addWidget(lbl00, 1, 0)
        grid.addWidget(self.lineedit_code00, 1, 1, 1, 2)
        grid.addWidget(btnComfirm, 2, 2)
        w = QWidget()
        w.setLayout(grid)
        self.setCentralWidget(w)

    def commandOrder(self):
        try:
            cancelEvent = Event(type_=EVENT_ORDERCANCEL)
            cancelEvent.dict_['orderref'] = self.lineedit_code00.text()
            self.ee.put(cancelEvent)
            self.close()
        except:
            QMessageBox.warning(self, '警告', '数据格式有误。')

class DelUi(QMainWindow):
    def __init__(self, ee):
        super().__init__()
        self.ee = ee
        self.getUi()

    def getUi(self):
        self.setWindowTitle('删除指定范围数据')
        lbl00 = QLabel('开始时间：', self)
        self.timeEdit = QLineEdit(self)
        lbl11 = QLabel('结束时间：', self)
        self.timeEdit1 = QLineEdit(self)
        btnComfirm = QPushButton('确定删除', self)
        btnComfirm.clicked.connect(self.btnConnect)
        grid = QGridLayout()
        grid.addWidget(lbl00, 1, 0)
        grid.addWidget(self.timeEdit, 1, 1, 1, 2)
        grid.addWidget(lbl11, 2, 0)
        grid.addWidget(self.timeEdit1, 2, 1, 1, 2)
        grid.addWidget(btnComfirm, 3, 2)
        w = QWidget()
        w.setLayout(grid)
        self.setCentralWidget(w)

    def btnConnect(self):
        try:
            t1 = pd.Timestamp(self.timeEdit.text())
            t2 = pd.Timestamp(self.timeEdit1.text())
            putLogEvent(self.ee, '删除自{}到{}的数据'.format(t1, t2))
            threading.Thread(target=self.thdDlt, args=[t1, t2]).start()
            self.close()
        except:
            QMessageBox.warning(self, '警告', '数据格式有误。')

    def thdDlt(self, t1, t2):
        dltData(t1, t2)
        putLogEvent(self.ee, '删除自{}到{}的数据完成'.format(t1, t2))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    ee = EventEngine()
    ee.start(timer=False)
    ui = DelUi(ee)
    ui.show()
    sys.exit(app.exec_())
