import sys
from PyQt5.QtWidgets import QApplication, QWidget,QPushButton
from PyQt5.QtGui import QIcon
import time

class Example(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setGeometry(300, 300, 300, 220)
        self.setWindowTitle('Icon')
        self.setWindowIcon(QIcon('web.png'))

        btn = QPushButton('Button', self)
        btn.clicked.connect(self.thePrint)

        self.show()

    def thePrint(self):
        while 1:
            print('aaaa')
            time.sleep(1)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = Example()
    # 使用app.exec_() 时会不断地循环，sys.exit()保证不留垃圾地退出：
    # 当不循环时，系统即会关闭吧：
    sys.exit(app.exec_())
