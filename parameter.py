from sqlalchemy import *
from function import *
"""
RD参数文件\\公共参数.xlsx 需要每周进行更改
pickle\\dictGoodsClose.pkl 需要更改
"""
# 执行频段数据
listFreq = [5]
# 公共参数表
dfCapital = pd.read_excel('RD参数文件\\公共参数.xlsx', sheetname='账户资金表')
GoodsTab = pd.read_excel('RD参数文件\\公共参数.xlsx', sheetname='品种信息', index_col='品种名称')
DfWeek = pd.read_excel('RD参数文件\\公共参数.xlsx', sheetname='周时间序列表')
DfWeek['起始时间'] = DfWeek['起始时间'] + timedelta(hours=8)
DfWeek['结束时间'] = DfWeek['结束时间'] + timedelta(hours=8)
now = datetime.now()
for each_num in range(DfWeek.shape[0]):
    if DfWeek['起始时间'][each_num] <= now <= DfWeek['结束时间'][each_num]:
        week = DfWeek['周次'][each_num]
        weekStartTime = DfWeek['起始时间'][each_num]
        weekEndTime = DfWeek['结束时间'][each_num]
        print(weekStartTime)
        print(weekEndTime)
dictGoodsName = {}
listGoods = []
dictGoodsChg = {}
dictGoodsLast = {}  # 品种的收盘时间
dictGoodsClose = pd.read_pickle('pickle\\dictGoodsClose.pkl')  # 不同频段的时间表
for eachNum in range(GoodsTab.shape[0]):
    if GoodsTab['交易所简称'][eachNum] == 'CZCE':
        chg = 'CZC'
    elif GoodsTab['交易所简称'][eachNum] == 'SHFE':
        chg = 'SHF'
    elif GoodsTab['交易所简称'][eachNum] == 'CFFEX':
        chg = 'CFE'
    else:
        chg = GoodsTab['交易所简称'][eachNum]
    goodsCode = GoodsTab['合约前缀'][eachNum] + '.' + chg
    dictGoodsName[goodsCode] = GoodsTab.index[eachNum]
    listGoods.append(goodsCode)
    dictGoodsChg[GoodsTab['合约前缀'][eachNum]] = chg
    if GoodsTab['交易时间类型'][eachNum] == '23.30收盘':
        dictGoodsLast[goodsCode] = time(23, 30)
    elif GoodsTab['交易时间类型'][eachNum] == '1.00收盘':
        dictGoodsLast[goodsCode] = time(1)
    elif GoodsTab['交易时间类型'][eachNum] == '23.00收盘':
        dictGoodsLast[goodsCode] = time(23)
    elif GoodsTab['交易时间类型'][eachNum] == '2.30收盘':
        dictGoodsLast[goodsCode] = time(2, 30)
    elif GoodsTab['交易时间类型'][eachNum] == '15.00收盘':
        dictGoodsLast[goodsCode] = time(15)
    elif GoodsTab['交易时间类型'][eachNum] == '15.15收盘':
        dictGoodsLast[goodsCode] = time(15, 15)
# CTA交易参数表-综合表（每周更新，作为是否开仓的依据）
ParTab = {}
for freq in listFreq:
    ParTab[freq] = pd.read_excel('RD参数文件\\CTA交易参数表-综合版.xlsx',
                                 sheetname='CTA{}'.format(freq), index_col='品种名称')
# 记录各个频段下单持仓数据
dictFreqPosition = {}  # 持仓
dictFreqOrder = {}  # 委托
dictFreqOrderSource = {}  # 原始委托单
dictFreqTrade = {}  # 成交
listPosition = ['代码', '名称', '方向', '数量', '多头冻结', '空头冻结', '持仓成本', '持仓盈亏', '开仓成本', '今日持仓']
listFreqPosition = ['代码', '名称', '方向', '数量']
listOrder = ['本地下单码','时间', '代码', '方向', '价格','数量','状态','已成交','成交均价', '拒绝原因']
listTrade = ['本地下单码','时间', '代码', '名称', '方向', '价格', '数量', '成本金额']
listAccount = ['经纪公司代码', '投资者帐号', '上次存款额', '上次结算准备金', '上次占用的保证金', '当前保证金总额', '可用资金', '可取资金']
listDuoKong = ['序号', '名称', '合约号']
listDuoKong.extend(dfCapital['账户名'].tolist())
listDuoKong.extend(['状态', '刷时', '实价', '开线', '开比例', '盈线',
                           '盈比例', '损线', '损比例', '开时', '初时'])
for eachFreq in listFreq:
    dictFreqPosition[eachFreq] = pd.DataFrame(columns=listFreqPosition)
    listOrderColumns = []
    for each in CThostFtdcOrderField._fields_:
        listOrderColumns.append(each[0])
    dictFreqOrder[eachFreq] = pd.DataFrame(columns=listOrder)
    dictFreqOrderSource[eachFreq] = pd.DataFrame(columns=listOrderColumns)
    dictFreqTrade[eachFreq] = pd.DataFrame(columns=listTrade)
# 基础定义
mvlenvector = [80, 100, 120, 140, 160, 180, 200, 220, 240, 260]
now = datetime.now()
theStartTime = datetime(now.year, now.month, now.day, now.hour, now.minute, now.second) - timedelta(days=120)  # 减去120数据
MaxLossPerCTA = 0.001  # 最大回撤阈值
StdMuxMinValue = 1  # 开平仓线时，开仓倍数的比较值
tradeDay = pd.read_pickle('pickle\\tradeDay.pkl')  # 交易日
theTradeDay = pd.Series(tradeDay).dt.date
dictGoodsPark = pd.read_pickle('pickle\\dictGoodsSend.pkl')  # 需要预撤单与预下单的时间
# 创建所有对象的调整表，均值表，重叠度表格
dictTable = {}  # 每个品种对应
dictCon = {}  # 每个频段都会有一个con
listFreqPlus = listFreq.copy()
listFreqPlus.insert(0, 1)
for eachFreq in listFreqPlus:
    con = create_engine('mysql+pymysql://root:rd008@localhost:3306/?charset=utf8').connect()
    con.execute('create database if not exists cta{}_trade'.format(eachFreq))
    con.close()
    con = create_engine('mysql+pymysql://root:rd008@localhost:3306/cta{}_trade?charset=utf8'.format(eachFreq)).connect()
    dictCon[eachFreq] = con
    metadata = MetaData(con)
    for eachGoodsName in dictGoodsName.values():
        dictTable[eachGoodsName] = Table(eachGoodsName, metadata,
                                                                  Column('id', Integer, autoincrement=True, primary_key=True),
                                                                  Column('goods_code', VARCHAR(16)),
                                                                  Column('goods_name', VARCHAR(16)),
                                                                  Column('trade_time', DATETIME),
                                                                  Column('open', FLOAT, ),
                                                                  Column('high', FLOAT),
                                                                  Column('low', FLOAT),
                                                                  Column('close', FLOAT),
                                                                  Column('volume', FLOAT),
                                                                  Column('amt', FLOAT),
                                                                  Column('oi', FLOAT),
                                                                  extend_existing=True
                                                                  )
        dictTable[eachGoodsName + '_调整表'] = Table(eachGoodsName + '_调整表', metadata,
                                                                  Column('id', Integer, autoincrement=True,
                                                                         primary_key=True),
                                                                  Column('goods_code', VARCHAR(16)),
                                                                  Column('goods_name', VARCHAR(16)),
                                                                  Column('trade_time', DATETIME),
                                                                  Column('open', FLOAT),
                                                                  Column('high', FLOAT),
                                                                  Column('low', FLOAT),
                                                                  Column('close', FLOAT),
                                                                  Column('volume', FLOAT),
                                                                  Column('amt', FLOAT),
                                                                  Column('oi', FLOAT),
                                                                  extend_existing=True
                                                                  )
        dictTable[eachGoodsName + '_调整时刻表'] = Table(eachGoodsName + '_调整时刻表', metadata,
                                                                           Column('id', Integer, autoincrement=True,
                                                                                  primary_key=True),
                                                                           Column('goods_code', VARCHAR(16)),
                                                                           Column('goods_name', VARCHAR(16)),
                                                                           Column('adjdate', DATE),
                                                                           Column('adjinterval', FLOAT),
                                                                           extend_existing=True
                                                                           )
        dictTable[eachGoodsName + '_均值表'] = Table(eachGoodsName + '_均值表', metadata,
                                                                             Column('id', Integer, autoincrement=True,
                                                                                    primary_key=True),
                                                                             Column('goods_code', VARCHAR(16)),
                                                                             Column('goods_name', VARCHAR(16)),
                                                                             Column('trade_time', DATETIME),
                                                                             Column('open', FLOAT),
                                                                             Column('high', FLOAT),
                                                                             Column('low', FLOAT),
                                                                             Column('close', FLOAT),
                                                                             extend_existing=True
                                                                           )
        for eachMvl in mvlenvector:
            dictTable[eachGoodsName + '_均值表'].append_column(Column('maprice_{}'.format(eachMvl), DECIMAL(15, 4)))
            dictTable[eachGoodsName + '_均值表'].append_column(Column('stdprice_{}'.format(eachMvl), DECIMAL(15, 4)))
            dictTable[eachGoodsName + '_均值表'].append_column(Column('stdmux_{}'.format(eachMvl), DECIMAL(15, 4)))
            dictTable[eachGoodsName + '_均值表'].append_column(Column('highstdmux_{}'.format(eachMvl), DECIMAL(15, 4)))
            dictTable[eachGoodsName + '_均值表'].append_column(Column('lowstdmux_{}'.format(eachMvl), DECIMAL(15, 4)))
        dictTable[eachGoodsName + '_重叠度表'] = Table(eachGoodsName + '_重叠度表', metadata,
                                                                           Column('id', Integer, autoincrement=True,
                                                                                  primary_key=True),
                                                                           Column('goods_code', VARCHAR(16)),
                                                                           Column('goods_name', VARCHAR(16)),
                                                                           Column('trade_time', DATETIME),
                                                                           Column('high', FLOAT),
                                                                           Column('low', FLOAT),
                                                                           Column('close', FLOAT),
                                                                           extend_existing=True
                                                                           )
        for eachMvl in mvlenvector:
            dictTable[eachGoodsName + '_重叠度表'].append_column(Column('StdMux高均值_{}'.format(eachMvl),
                                                                                     DECIMAL(10, 2)))
            dictTable[eachGoodsName + '_重叠度表'].append_column(Column('重叠度高_{}'.format(eachMvl),
                                                                                     DECIMAL(10, 2)))
            dictTable[eachGoodsName + '_重叠度表'].append_column(Column('StdMux低均值_{}'.format(eachMvl),
                                                                                     DECIMAL(10, 2)))
            dictTable[eachGoodsName + '_重叠度表'].append_column(Column('重叠度低_{}'.format(eachMvl),
                                                                                      DECIMAL(10, 2)))
            dictTable[eachGoodsName + '_重叠度表'].append_column(Column('StdMux收均值_{}'.format(eachMvl),
                                                                                     DECIMAL(10, 2)))
            dictTable[eachGoodsName + '_重叠度表'].append_column(Column('重叠度收_{}'.format(eachMvl),
                                                                                      DECIMAL(10, 2)))
            dictTable[eachGoodsName + '_重叠度表'].append_column(Column('重叠度高收益_{}'.format(eachMvl),
                                                                                      DECIMAL(10, 2)))
            dictTable[eachGoodsName + '_重叠度表'].append_column(Column('重叠度低收益_{}'.format(eachMvl),
                                                                                      DECIMAL(10, 2)))
            dictTable[eachGoodsName + '_重叠度表'].append_column(Column('重叠度收收益_{}'.format(eachMvl),
                                                                                      DECIMAL(10, 2)))
        dictTable[eachGoodsName + '_周交易明细表'] = Table(eachGoodsName + '_周交易明细表', metadata,
                                                                            Column('id', Integer,
                                                                                   autoincrement=True,
                                                                                   primary_key=True),
                                                                            Column('周次', VARCHAR(16)),
                                                                            Column('品种名称', VARCHAR(16)),
                                                                            Column('交易合约号', VARCHAR(16)),
                                                                            Column('交易时间', DATETIME),
                                                                            Column('开仓时间', DATETIME),
                                                                            Column('平仓时间', DATETIME),
                                                                            Column('开平仓标识多', INTEGER),
                                                                            Column('单笔浮赢亏多', DECIMAL(17, 6)),
                                                                            Column('开平仓标识空', INTEGER),
                                                                            Column('单笔浮赢亏空', DECIMAL(17, 6)),
                                                                            Column('总净值浮赢亏', DECIMAL(17, 6)),
                                                                            Column('总净值最大回撤', DECIMAL(17, 6)),
                                                                            Column('开仓线多', DECIMAL(15, 4)),
                                                                            Column('止盈线多', DECIMAL(15, 4)),
                                                                            Column('止损线多', DECIMAL(15, 4)),
                                                                            Column('开仓线空', DECIMAL(15, 4)),
                                                                            Column('止盈线空', DECIMAL(15, 4)),
                                                                            Column('止损线空', DECIMAL(15, 4)),
                                                                            Column('重叠度标识多', INTEGER),
                                                                            Column('重叠度标识空', INTEGER),
                                                                            Column('均值', DECIMAL(15, 4)),
                                                                            Column('标准差', DECIMAL(15, 4)),
                                                                            Column('最高价', FLOAT),
                                                                            Column('最低价', FLOAT),
                                                                            Column('仓位多', DECIMAL(15, 4)),
                                                                            Column('仓位空', DECIMAL(15, 4)),
                                                                            Column('标准差倍数', DECIMAL(15, 4)),
                                                                            Column('标准差倍数高', DECIMAL(15, 4)),
                                                                            Column('标准差倍数低', DECIMAL(15, 4)),
                                                                            Column('做多参数', VARCHAR(32)),
                                                                            Column('做空参数', VARCHAR(32)),
                                                                            Column('参数编号', INTEGER),
                                                                            Column('参数', VARCHAR(32)),
                                                                            Column('时间段序号', INTEGER),
                                                                            extend_existing=True
                                                                            )
    metadata.create_all(con)
# socket
host = "localhost"
port = 8080
# 是否引入重叠度长度对应的均值标识
MaWithODLenFlag = False
# 资金账号名称
accountName = "测二"
# 在开仓bar内进行止盈与止损操作
StopAbtainInBarMux = 2
StopLossInBarMux = 2
# 开仓参数筛选开仓线标识
OpenParFiltedOpenLineFlag = False
# Bar内止盈止损标识
InBarCloseAtNMuxFlag = "1"
StopAbtainInBarMux = 2
InBarStopLossFlag = "1"
StopLossInBarMux = 2
PricUnreachableHighPrice = 999999  # 下单时，保证价格无效的最大价格
PricUnreachableLowPrice = -1  # 下单时，保证价格无效的最大价格
PricUnreachableOpenPrice = 666666  # 下单时，开仓线无效的标识