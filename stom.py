import sys
import psutil
import logging
import pyqtgraph as pg
from PyQt5.QtTest import QTest
from multiprocessing import Process, Queue, freeze_support
from coin.receiver_upbit import WebsTicker, WebsOrderbook
from coin.collector_coin import CollectorCoin
from coin.strategy_coin import StrategyCoin
from coin.trader_upbit import TraderUpbit
from stock.receiver_kiwoom import ReceiverKiwoom
from stock.receiver_xing import ReceiverXing
from stock.collector_stock import CollectorStock
from stock.strategy_stock import StrategyStock
from stock.trader_kiwoom import TraderKiwoom
from stock.trader_xing import TraderXing
from backtester.backtester_coin_vc import BacktesterCoinVcMain
from backtester.backtester_coin_stg import BackTesterCoinStgMain
from backtester.backtester_stock_vc import BacktesterStockVcMain
from backtester.backtester_stock_stg import BackTesterStockStgMain
from utility.hoga import Hoga
from utility.setui import *
from utility.sound import Sound
from utility.query import Query
from utility.chart import Chart
from utility.query_tick import QueryTick
from utility.telegram_msg import TelegramMsg
from utility.static import *
from utility.setting import *


class Window(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()

        self.log1 = logging.getLogger('Stock')
        self.log1.setLevel(logging.INFO)
        filehandler = logging.FileHandler(filename=f"{SYSTEM_PATH}/log/S{strf_time('%Y%m%d')}.txt", encoding='utf-8')
        self.log1.addHandler(filehandler)

        self.log2 = logging.getLogger('Coin')
        self.log2.setLevel(logging.INFO)
        filehandler = logging.FileHandler(filename=f"{SYSTEM_PATH}/log/C{strf_time('%Y%m%d')}.txt", encoding='utf-8')
        self.log2.addHandler(filehandler)

        SetUI(self)

        if int(strf_time('%H%M%S')) < 83000 or 160000 < int(strf_time('%H%M%S')):
            self.main_tabWidget.setCurrentWidget(self.ct_tab)

        self.dict_set = DICT_SET
        self.counter = 0
        self.cpu_per = 0
        self.int_time = int(strf_time('%H%M%S'))

        self.dict_name = {}
        self.dict_code = {}
        con = sqlite3.connect(DB_SETTING)
        df = pd.read_sql('SELECT * FROM codename', con).set_index('index')
        con.close()
        for code in df.index:
            name = df['종목명'][code]
            self.dict_name[code] = name
            self.dict_code[name] = code

        con = sqlite3.connect(DB_COIN_TICK)
        df = pd.read_sql("SELECT name FROM sqlite_master WHERE TYPE = 'table'", con)
        con.close()
        codenamelist = list(self.dict_code.keys()) + list(self.dict_name.keys()) + list(df['name'].values)
        self.ct_lineEdit_02.setCompleter(QtWidgets.QCompleter(codenamelist))

        self.showQsize = False
        self.chart_name = None
        self.backtester_proc = None
        self.chart1 = None
        self.chart2 = None
        self.chart3 = None
        self.chart4 = None
        self.chart5 = None
        self.chart6 = None
        self.chart1_data = None
        self.chart2_data = None
        self.chart3_data = None
        self.chart4_data = None
        self.chart5_data = None
        self.chart6_data = None
        self.close_line = None

        self.qtimer1 = QtCore.QTimer()
        self.qtimer1.setInterval(1000)
        self.qtimer1.timeout.connect(self.ProcessStarter)
        self.qtimer1.start()

        self.qtimer2 = QtCore.QTimer()
        self.qtimer2.setInterval(500)
        self.qtimer2.timeout.connect(self.UpdateProgressBar)
        self.qtimer2.start()

        self.qtimer3 = QtCore.QTimer()
        self.qtimer3.setInterval(500)
        self.qtimer3.timeout.connect(self.UpdateCpuper)
        self.qtimer3.start()

        self.writer = Writer()
        self.writer.data1.connect(self.UpdateTexedit)
        self.writer.data2.connect(self.UpdateTablewidget)
        self.writer.data3.connect(self.UpdateGaonsimJongmok)
        self.writer.data4.connect(self.DrawChart)
        self.writer.data5.connect(self.DrawRealChart)
        self.writer.start()

        self.receiver_coin_proc1 = Process(target=WebsTicker, args=(qlist,))
        self.receiver_coin_proc2 = Process(target=WebsOrderbook, args=(qlist,))
        self.collector_coin_proc = Process(target=CollectorCoin, args=(qlist,))
        self.strategy_coin_proc = Process(target=StrategyCoin, args=(qlist,))
        self.trader_coin_proc = Process(target=TraderUpbit, args=(qlist,))

        self.receiver_kiwoom_proc = Process(target=ReceiverKiwoom, args=(qlist,))
        self.receiver_xing_proc = Process(target=ReceiverXing, args=(qlist,))
        self.collector_stock_proc1 = Process(target=CollectorStock, args=(1, qlist))
        self.collector_stock_proc2 = Process(target=CollectorStock, args=(2, qlist))
        self.collector_stock_proc3 = Process(target=CollectorStock, args=(3, qlist))
        self.collector_stock_proc4 = Process(target=CollectorStock, args=(4, qlist))
        self.strategy_stock_proc = Process(target=StrategyStock, args=(qlist,))
        self.trader_kiwoom_proc = Process(target=TraderKiwoom, args=(qlist,))
        self.trader_xing_proc = Process(target=TraderXing, args=(qlist,))

    def ProcessStarter(self):
        if now().weekday() not in [6, 7]:
            if self.int_time < 85000 <= int(strf_time('%H%M%S')) and self.dict_set['주식리시버']:
                self.StockReceiverStart()
            if self.int_time < 85200 <= int(strf_time('%H%M%S')) and self.dict_set['주식트레이더']:
                self.StockTraderStart()
        if self.dict_set['코인리시버']:
            self.CoinReceiverStart()
        if self.dict_set['코인콜렉터']:
            self.CoinCollectorStart()
        if self.dict_set['코인트레이더']:
            self.CoinTraderStart()
        if self.int_time < 100 <= int(strf_time('%H%M%S')):
            self.ClearTextEdit()
        self.UpdateWindowTitle()
        self.int_time = int(strf_time('%H%M%S'))

    def StockReceiverStart(self):
        self.backtester_proc = None
        if self.dict_set['아이디2'] is not None:
            start = False
            if self.dict_set['증권사'] == '키움증권' and not self.collector_stock_proc1.is_alive():
                os.system(f'python {LOGIN_PATH}/versionupdater.py')
                os.system(f'python {LOGIN_PATH}/autologin2.py')
            if self.dict_set['주식콜렉터']:
                if not self.collector_stock_proc1.is_alive():
                    self.collector_stock_proc1.start()
                if not self.collector_stock_proc2.is_alive():
                    self.collector_stock_proc2.start()
                if not self.collector_stock_proc3.is_alive():
                    self.collector_stock_proc3.start()
                if not self.collector_stock_proc4.is_alive():
                    self.collector_stock_proc4.start()
            if self.dict_set['증권사'] == '키움증권' and not self.receiver_kiwoom_proc.is_alive():
                self.receiver_kiwoom_proc.start()
                start = True
            elif self.dict_set['증권사'] == '이베스트투자증권' and not self.receiver_xing_proc.is_alive():
                self.receiver_xing_proc.start()
                start = True
            if start:
                if self.dict_set['주식콜렉터']:
                    text = '주식 리시버 및 콜렉터를 시작하였습니다.'
                else:
                    text = '주식 리시버를 시작하였습니다.'
                soundQ.put(text)
                teleQ.put(text)
        else:
            QtWidgets.QMessageBox.critical(
                self, '오류 알림', '두번째 계정이 설정되지 않아\n콜렉터를 시작할 수 없습니다.\n계정 설정 후 다시 시작하십시오.\n')

    def StockTraderStart(self):
        if self.dict_set['아이디1'] is not None:
            start = False
            if self.dict_set['증권사'] == '키움증권' and not self.strategy_stock_proc.is_alive():
                os.system(f'python {LOGIN_PATH}/autologin1.py')
            if not self.strategy_stock_proc.is_alive():
                self.strategy_stock_proc.start()
                start = True
            if self.dict_set['증권사'] == '키움증권' and not self.trader_kiwoom_proc.is_alive():
                self.trader_kiwoom_proc.start()
            elif self.dict_set['증권사'] == '이베스트투자증권' and not self.trader_xing_proc.is_alive():
                self.trader_xing_proc.start()
            if start:
                text = '주식 트레이더를 시작하였습니다.'
                soundQ.put(text)
                teleQ.put(text)
        else:
            QtWidgets.QMessageBox.critical(
                self, '오류 알림', '첫번째 계정이 설정되지 않아\n트레이더를 시작할 수 없습니다.\n계정 설정 후 다시 시작하십시오.\n')

    def CoinReceiverStart(self):
        if not self.receiver_coin_proc1.is_alive() and not self.receiver_coin_proc2.is_alive():
            self.receiver_coin_proc1.start()
            self.receiver_coin_proc2.start()
            text = '코인 리시버를 시작하였습니다.'
            soundQ.put(text)
            teleQ.put(text)

    def CoinCollectorStart(self):
        if not self.collector_coin_proc.is_alive():
            self.collector_coin_proc.start()
            text = '코인 콜렉터를 시작하였습니다.'
            soundQ.put(text)
            teleQ.put(text)

    def CoinTraderStart(self):
        if self.dict_set['Access_key'] is not None:
            if not self.strategy_coin_proc.is_alive():
                self.strategy_coin_proc.start()
            if not self.trader_coin_proc.is_alive():
                self.trader_coin_proc.start()
                text = '코인 트레이더를 시작하였습니다.'
                soundQ.put(text)
                teleQ.put(text)

    def ClearTextEdit(self):
        self.st_textEdit.clear()
        self.ct_textEdit.clear()
        self.sc_textEdit.clear()
        self.cc_textEdit.clear()

    def UpdateWindowTitle(self):
        if self.showQsize:
            queryQ_size = query1Q.qsize() + query2Q.qsize()
            stickQ_size = tick1Q.qsize() + tick2Q.qsize() + tick3Q.qsize() + tick4Q.qsize()
            text = f'STOM - Qsize : ' \
                   f'queryQ[{queryQ_size}] | stockQ[{stockQ.qsize()}] | coinQ[{coinQ.qsize()}] | ' \
                   f'sstgQ[{sstgQ.qsize()}] | cstgQ[{cstgQ.qsize()}] | stickQ[{stickQ_size}] | ctickQ[{tick5Q.qsize()}]'
            self.setWindowTitle(text)
        elif self.windowTitle() != 'STOM':
            self.setWindowTitle('STOM')

    def UpdateProgressBar(self):
        if self.counter > 9:
            self.counter = 0
        self.counter += 1
        self.progressBar.setValue(int(self.cpu_per))
        if self.backtester_proc is not None and self.backtester_proc.is_alive():
            if self.counter % 2 == 0:
                self.sb_pushButton_01.setStyleSheet(style_bc_st)
                self.sb_pushButton_02.setStyleSheet(style_bc_bt)
                self.cb_pushButton_01.setStyleSheet(style_bc_st)
                self.cb_pushButton_02.setStyleSheet(style_bc_bt)
            else:
                self.sb_pushButton_01.setStyleSheet(style_bc_bt)
                self.sb_pushButton_02.setStyleSheet(style_bc_st)
                self.cb_pushButton_01.setStyleSheet(style_bc_bt)
                self.cb_pushButton_02.setStyleSheet(style_bc_st)
        else:
            self.sb_pushButton_01.setStyleSheet(style_bc_st)
            self.sb_pushButton_02.setStyleSheet(style_bc_st)
            self.cb_pushButton_01.setStyleSheet(style_bc_st)
            self.cb_pushButton_02.setStyleSheet(style_bc_st)

    @thread_decorator
    def UpdateCpuper(self):
        self.cpu_per = psutil.cpu_percent(interval=1)

    def ShowQsize(self):
        self.showQsize = True if not self.showQsize else False

    def UpdateTexedit(self, data):
        text = f'[{now()}] {data[1]}'
        if data[0] == ui_num['설정텍스트']:
            self.sj_textEdit.append(text)
        elif data[0] == ui_num['S로그텍스트']:
            self.st_textEdit.append(text)
            self.log1.info(text)
        elif data[0] == ui_num['S단순텍스트']:
            self.sc_textEdit.append(text)
        elif data[0] == ui_num['C로그텍스트']:
            self.ct_textEdit.append(text)
            self.log2.info(text)
        elif data[0] == ui_num['C단순텍스트']:
            self.cc_textEdit.append(text)
        elif data[0] == ui_num['S백테스트']:
            self.ss_textEdit_03.append(text)
            if '백테스팅 소요시간' in data[1]:
                self.ButtonClicked_90()
        elif data[0] == ui_num['C백테스트']:
            self.cs_textEdit_03.append(text)
            if '백테스팅 소요시간' in data[1]:
                self.ButtonClicked_92()

    def UpdateTablewidget(self, data):
        gubun = data[0]
        df = data[1]

        tableWidget = None
        if gubun == ui_num['S실현손익']:
            tableWidget = self.stt_tableWidget
        elif gubun == ui_num['S거래목록']:
            tableWidget = self.std_tableWidget
        elif gubun == ui_num['S잔고평가']:
            tableWidget = self.stj_tableWidget
        elif gubun == ui_num['S잔고목록']:
            tableWidget = self.sjg_tableWidget
        elif gubun == ui_num['S체결목록']:
            tableWidget = self.scj_tableWidget
        elif gubun == ui_num['S당일합계']:
            tableWidget = self.sdt_tableWidget
        elif gubun == ui_num['S당일상세']:
            tableWidget = self.sds_tableWidget
        elif gubun == ui_num['S누적합계']:
            tableWidget = self.snt_tableWidget
        elif gubun == ui_num['S누적상세']:
            tableWidget = self.sns_tableWidget
        elif gubun == ui_num['C실현손익']:
            tableWidget = self.ctt_tableWidget
        elif gubun == ui_num['C거래목록']:
            tableWidget = self.ctd_tableWidget
        elif gubun == ui_num['C잔고평가']:
            tableWidget = self.ctj_tableWidget
        elif gubun == ui_num['C잔고목록']:
            tableWidget = self.cjg_tableWidget
        elif gubun == ui_num['C체결목록']:
            tableWidget = self.ccj_tableWidget
        elif gubun == ui_num['C당일합계']:
            tableWidget = self.cdt_tableWidget
        elif gubun == ui_num['C당일상세']:
            tableWidget = self.cds_tableWidget
        elif gubun == ui_num['C누적합계']:
            tableWidget = self.cnt_tableWidget
        elif gubun == ui_num['C누적상세']:
            tableWidget = self.cns_tableWidget
        elif gubun in [ui_num['C호가종목'], ui_num['S호가종목']]:
            tableWidget = self.hj_tableWidget
        elif gubun in [ui_num['C호가체결'], ui_num['S호가체결']]:
            if not self.dialog_hoga.isVisible():
                sreceivQ.put('000000')
                creceiv1Q.put('000000')
                creceiv2Q.put('000000')
                return
            tableWidget = self.hc_tableWidget
        elif gubun in [ui_num['C호가잔량'], ui_num['S호가잔량']]:
            tableWidget = self.hg_tableWidget
        if tableWidget is None:
            return

        if len(df) == 0:
            tableWidget.clearContents()
            return

        tableWidget.setRowCount(len(df))
        for j, index in enumerate(df.index):
            for i, column in enumerate(df.columns):
                if column == '체결시간':
                    cgtime = str(df[column][index])
                    cgtime = f'{cgtime[8:10]}:{cgtime[10:12]}:{cgtime[12:14]}'
                    item = QtWidgets.QTableWidgetItem(cgtime)
                elif column in ['거래일자', '일자']:
                    day = df[column][index]
                    if '.' not in day:
                        day = day[:4] + '.' + day[4:6] + '.' + day[6:]
                    item = QtWidgets.QTableWidgetItem(day)
                elif column == '종목명':
                    try:
                        item = QtWidgets.QTableWidgetItem(self.dict_name[df[column][index]])
                    except KeyError:
                        item = QtWidgets.QTableWidgetItem(df[column][index])
                elif column in ['주문구분', '기간']:
                    item = QtWidgets.QTableWidgetItem(str(df[column][index]))
                elif (gubun == ui_num['C잔고목록'] and column == '보유수량') or \
                        (gubun == ui_num['C체결목록'] and column == '주문수량') or \
                        (gubun == ui_num['C거래목록'] and column == '주문수량') or \
                        (gubun == ui_num['C호가체결'] and column == '체결수량') or \
                        (gubun == ui_num['C호가잔량'] and column == '잔량'):
                    item = QtWidgets.QTableWidgetItem(changeFormat(df[column][index], dotdown8=True))
                elif (gubun == ui_num['C잔고목록'] and column in ['매입가', '현재가']) or \
                        (gubun == ui_num['C체결목록'] and column in ['체결가', '주문가격']):
                    item = QtWidgets.QTableWidgetItem(changeFormat(df[column][index]))
                elif column not in ['수익률', '등락율', '고저평균대비등락율', '체결강도', '최고체결강도']:
                    item = QtWidgets.QTableWidgetItem(changeFormat(df[column][index], dotdowndel=True))
                else:
                    item = QtWidgets.QTableWidgetItem(changeFormat(df[column][index]))

                if column == '종목명':
                    item.setTextAlignment(int(Qt.AlignVCenter | Qt.AlignLeft))
                elif column in ['거래횟수', '추정예탁자산', '추정예수금', '보유종목수',
                                '주문구분', '체결시간', '거래일자', '기간', '일자']:
                    item.setTextAlignment(int(Qt.AlignVCenter | Qt.AlignCenter))
                else:
                    item.setTextAlignment(int(Qt.AlignVCenter | Qt.AlignRight))

                if gubun in [ui_num['C호가체결'], ui_num['S호가체결']]:
                    if column == '체결수량':
                        if j == 0:
                            item.setIcon(self.icon_totalb)
                        elif j == 11:
                            item.setIcon(self.icon_totals)
                    elif column == '체결강도':
                        if j == 0:
                            item.setIcon(self.icon_up)
                        elif j == 11:
                            item.setIcon(self.icon_down)
                elif gubun in [ui_num['C호가잔량'], ui_num['S호가잔량']]:
                    if column == '잔량':
                        if j == 0:
                            item.setIcon(self.icon_totalb)
                        elif j == 11:
                            item.setIcon(self.icon_totals)
                    elif column == '호가':
                        if j == 0:
                            item.setIcon(self.icon_up)
                        elif j == 11:
                            item.setIcon(self.icon_down)
                        else:
                            if self.hj_tableWidget.item(0, 0) is not None:
                                o = comma2int(self.hj_tableWidget.item(0, columns_hj.index('시가')).text())
                                h = comma2int(self.hj_tableWidget.item(0, columns_hj.index('고가')).text())
                                low = comma2int(self.hj_tableWidget.item(0, columns_hj.index('저가')).text())
                                uvi = comma2int(self.hj_tableWidget.item(0, columns_hj.index('UVI')).text())
                                if o != 0:
                                    if df[column][index] == o:
                                        item.setIcon(self.icon_open)
                                    elif df[column][index] == h:
                                        item.setIcon(self.icon_high)
                                    elif df[column][index] == low:
                                        item.setIcon(self.icon_low)
                                    elif df[column][index] == uvi:
                                        item.setIcon(self.icon_vi)

                if '수익률' in df.columns:
                    if df['수익률'][index] >= 0:
                        item.setForeground(color_fg_bt)
                    else:
                        item.setForeground(color_fg_dk)
                elif gubun in [ui_num['S체결목록'], ui_num['C체결목록']]:
                    if df['주문구분'][index] == '매수':
                        item.setForeground(color_fg_bt)
                    elif df['주문구분'][index] == '매도':
                        item.setForeground(color_fg_dk)
                    elif df['주문구분'][index] in ['매도취소', '매수취소']:
                        item.setForeground(color_fg_bc)
                elif gubun in [ui_num['C호가체결'], ui_num['S호가체결']]:
                    if column == '체결수량':
                        if j == 0:
                            if df[column][index] > df[column][11]:
                                item.setForeground(color_fg_bt)
                            else:
                                item.setForeground(color_fg_dk)
                        elif j == 11:
                            if df[column][index] > df[column][0]:
                                item.setForeground(color_fg_bt)
                            else:
                                item.setForeground(color_fg_dk)
                        else:
                            if self.hg_tableWidget.item(0, 0) is not None and \
                                    self.hg_tableWidget.item(5, columns_hg.index('호가')).text() != '':
                                c = comma2int(self.hg_tableWidget.item(5, columns_hg.index('호가')).text())
                                if df[column][index] > 0:
                                    item.setForeground(color_fg_bt)
                                    if df[column][index] * c > 90000000:
                                        item.setBackground(color_bf_bt)
                                elif df[column][index] < 0:
                                    item.setForeground(color_fg_dk)
                                    if df[column][index] * c < -90000000:
                                        item.setBackground(color_bf_dk)
                    elif column == '체결강도':
                        if df[column][index] >= 100:
                            item.setForeground(color_fg_bt)
                        else:
                            item.setForeground(color_fg_dk)
                elif gubun in [ui_num['C호가잔량'], ui_num['S호가잔량']]:
                    if column == '잔량':
                        if j == 0:
                            if df[column][index] > df[column][11]:
                                item.setForeground(color_fg_bt)
                            else:
                                item.setForeground(color_fg_dk)
                        elif j == 11:
                            if df[column][index] > df[column][0]:
                                item.setForeground(color_fg_bt)
                            else:
                                item.setForeground(color_fg_dk)
                        elif j < 11:
                            item.setForeground(color_fg_bt)
                        else:
                            item.setForeground(color_fg_dk)
                    elif column == '호가':
                        if column == '호가' and df[column][index] != 0:
                            if self.hj_tableWidget.item(0, 0) is not None:
                                c = comma2int(self.hj_tableWidget.item(0, columns_hj.index('현재가')).text())
                                if j not in [0, 11] and df[column][index] == c:
                                    item.setBackground(color_bf_bt)
                tableWidget.setItem(j, i, item)

        if len(df) < 13 and gubun in [ui_num['S거래목록'], ui_num['S잔고목록'], ui_num['C거래목록'], ui_num['C잔고목록']]:
            tableWidget.setRowCount(13)
        elif len(df) < 15 and gubun in [ui_num['S체결목록'], ui_num['C체결목록']]:
            tableWidget.setRowCount(15)
        elif len(df) < 19 and gubun in [ui_num['S당일상세'], ui_num['C당일상세']]:
            tableWidget.setRowCount(19)
        elif len(df) < 28 and gubun in [ui_num['S누적상세'], ui_num['C누적상세']]:
            tableWidget.setRowCount(28)

    def UpdateGaonsimJongmok(self, data):
        gubun = data[0]
        dict_df = data[1]

        if gubun == ui_num['S관심종목']:
            gj_tableWidget = self.sgj_tableWidget
        else:
            gj_tableWidget = self.cgj_tableWidget

        if len(dict_df) == 0:
            gj_tableWidget.clearContents()
            return

        try:
            gj_tableWidget.setRowCount(len(dict_df))
            for j, code in enumerate(list(dict_df.keys())):
                try:
                    item = QtWidgets.QTableWidgetItem(self.dict_name[code])
                except KeyError:
                    item = QtWidgets.QTableWidgetItem(code)
                item.setTextAlignment(int(Qt.AlignVCenter | Qt.AlignLeft))
                gj_tableWidget.setItem(j, 0, item)

                for i, column in enumerate(columns_gj[:-1]):
                    if column in ['초당거래대금', '초당거래대금평균', '당일거래대금']:
                        item = QtWidgets.QTableWidgetItem(changeFormat(dict_df[code][column][0], dotdowndel=True))
                    else:
                        item = QtWidgets.QTableWidgetItem(changeFormat(dict_df[code][column][0]))
                    item.setTextAlignment(int(Qt.AlignVCenter | Qt.AlignRight))
                    gj_tableWidget.setItem(j, i + 1, item)

            if len(dict_df) < 15:
                gj_tableWidget.setRowCount(15)
        except KeyError:
            pass

    def DrawChart(self, data):
        df = data[1]
        name = data[2]
        xticks = data[3]

        if type(df) == str:
            QtWidgets.QMessageBox.critical(self.dialog_chart, '오류 알림', '해당 날짜의 데이터가 존재하지 않습니다.\n')
            return

        def crosshair(main_pg, sub_pg1, sub_pg2):
            vLine1 = pyqtgraph.InfiniteLine()
            vLine1.setPen(pyqtgraph.mkPen(color_cs_hr, width=1))
            vLine2 = pyqtgraph.InfiniteLine()
            vLine2.setPen(pyqtgraph.mkPen(color_cs_hr, width=1))
            vLine3 = pyqtgraph.InfiniteLine()
            vLine3.setPen(pyqtgraph.mkPen(color_cs_hr, width=1))

            hLine1 = pyqtgraph.InfiniteLine(angle=0)
            hLine1.setPen(pyqtgraph.mkPen(color_cs_hr, width=1))
            hLine2 = pyqtgraph.InfiniteLine(angle=0)
            hLine2.setPen(pyqtgraph.mkPen(color_cs_hr, width=1))
            hLine3 = pyqtgraph.InfiniteLine(angle=0)
            hLine3.setPen(pyqtgraph.mkPen(color_cs_hr, width=1))

            main_pg.addItem(vLine1, ignoreBounds=True)
            main_pg.addItem(hLine1, ignoreBounds=True)
            sub_pg1.addItem(vLine2, ignoreBounds=True)
            sub_pg1.addItem(hLine2, ignoreBounds=True)
            sub_pg2.addItem(vLine3, ignoreBounds=True)
            sub_pg2.addItem(hLine3, ignoreBounds=True)

            main_vb = main_pg.getViewBox()
            sub_vb1 = sub_pg1.getViewBox()
            sub_vb2 = sub_pg2.getViewBox()

            def mouseMoved(evt):
                pos = evt[0]
                if main_pg.sceneBoundingRect().contains(pos):
                    mousePoint = main_vb.mapSceneToView(pos)
                    self.ct_labellll_03.setText(f"현재가 {format(round(mousePoint.y(), 2), ',')}")
                    hLine1.setPos(mousePoint.y())
                    vLine1.setPos(mousePoint.x())
                    vLine2.setPos(mousePoint.x())
                    vLine3.setPos(mousePoint.x())
                elif sub_pg1.sceneBoundingRect().contains(pos):
                    mousePoint = sub_vb1.mapSceneToView(pos)
                    self.ct_labellll_04.setText(f"체결강도 {format(round(mousePoint.y(), 2), ',')}")
                    hLine2.setPos(mousePoint.y())
                    vLine1.setPos(mousePoint.x())
                    vLine2.setPos(mousePoint.x())
                    vLine3.setPos(mousePoint.x())
                elif sub_pg2.sceneBoundingRect().contains(pos):
                    mousePoint = sub_vb2.mapSceneToView(pos)
                    self.ct_labellll_05.setText(f"초당거래대금 {format(round(mousePoint.y(), 2), ',')}")
                    hLine3.setPos(mousePoint.y())
                    vLine1.setPos(mousePoint.x())
                    vLine3.setPos(mousePoint.x())
                    vLine2.setPos(mousePoint.x())

            main_pg.proxy = pyqtgraph.SignalProxy(main_pg.scene().sigMouseMoved, rateLimit=20, slot=mouseMoved)
            sub_pg1.proxy = pyqtgraph.SignalProxy(main_pg.scene().sigMouseMoved, rateLimit=20, slot=mouseMoved)
            sub_pg2.proxy = pyqtgraph.SignalProxy(main_pg.scene().sigMouseMoved, rateLimit=20, slot=mouseMoved)

        self.ctpg_01.clear()
        self.ctpg_02.clear()
        self.ctpg_03.clear()
        self.ctpg_01.plot(x=xticks, y=df['현재가'], pen=(200, 50, 50))
        self.ctpg_02.plot(x=xticks, y=df['체결강도'], pen=(50, 200, 50))
        self.ctpg_02.plot(x=xticks, y=df['체결강도평균'], pen=(50, 200, 200))
        self.ctpg_02.plot(x=xticks, y=df['최고체결강도'], pen=(200, 50, 50))
        self.ctpg_03.plot(x=xticks, y=df['초당거래대금'], pen=(200, 50, 50))
        self.ctpg_03.plot(x=xticks, y=df['초당거래대금평균'], pen=(50, 50, 200))
        self.ctpg_01.getAxis('bottom').setLabel(text=name)
        crosshair(main_pg=self.ctpg_01, sub_pg1=self.ctpg_02, sub_pg2=self.ctpg_03)
        self.ctpg_01.enableAutoRange(enable=True)
        self.ctpg_02.enableAutoRange(enable=True)
        self.ctpg_03.enableAutoRange(enable=True)
        self.chart_name = '000000'

    def DrawRealChart(self, data):
        df = data[1]
        name = data[2]

        if not self.dialog_chart.isVisible():
            sstgQ.put('000000')
            cstgQ.put('000000')
            return

        def crosshair(main_pg, sub_pg1, sub_pg2):
            vLine1 = pyqtgraph.InfiniteLine()
            vLine1.setPen(pyqtgraph.mkPen(color_cs_hr, width=1))
            vLine2 = pyqtgraph.InfiniteLine()
            vLine2.setPen(pyqtgraph.mkPen(color_cs_hr, width=1))
            vLine3 = pyqtgraph.InfiniteLine()
            vLine3.setPen(pyqtgraph.mkPen(color_cs_hr, width=1))

            hLine1 = pyqtgraph.InfiniteLine(angle=0)
            hLine1.setPen(pyqtgraph.mkPen(color_cs_hr, width=1))
            hLine2 = pyqtgraph.InfiniteLine(angle=0)
            hLine2.setPen(pyqtgraph.mkPen(color_cs_hr, width=1))
            hLine3 = pyqtgraph.InfiniteLine(angle=0)
            hLine3.setPen(pyqtgraph.mkPen(color_cs_hr, width=1))

            main_pg.addItem(vLine1, ignoreBounds=True)
            main_pg.addItem(hLine1, ignoreBounds=True)
            sub_pg1.addItem(vLine2, ignoreBounds=True)
            sub_pg1.addItem(hLine2, ignoreBounds=True)
            sub_pg2.addItem(vLine3, ignoreBounds=True)
            sub_pg2.addItem(hLine3, ignoreBounds=True)

            main_vb = main_pg.getViewBox()
            sub_vb1 = sub_pg1.getViewBox()
            sub_vb2 = sub_pg2.getViewBox()

            def mouseMoved(evt):
                pos = evt[0]
                if main_pg.sceneBoundingRect().contains(pos):
                    mousePoint = main_vb.mapSceneToView(pos)
                    self.ct_labellll_03.setText(f"현재가+  {format(round(mousePoint.y(), 2), ',')}")
                    hLine1.setPos(mousePoint.y())
                    vLine1.setPos(mousePoint.x())
                    vLine2.setPos(mousePoint.x())
                    vLine3.setPos(mousePoint.x())
                elif sub_pg1.sceneBoundingRect().contains(pos):
                    mousePoint = sub_vb1.mapSceneToView(pos)
                    self.ct_labellll_04.setText(f"체결강도+     {format(round(mousePoint.y(), 2), ',')}")
                    hLine2.setPos(mousePoint.y())
                    vLine1.setPos(mousePoint.x())
                    vLine2.setPos(mousePoint.x())
                    vLine3.setPos(mousePoint.x())
                elif sub_pg2.sceneBoundingRect().contains(pos):
                    mousePoint = sub_vb2.mapSceneToView(pos)
                    self.ct_labellll_05.setText(f"초당거래대금+     {format(round(mousePoint.y(), 2), ',')}")
                    hLine3.setPos(mousePoint.y())
                    vLine1.setPos(mousePoint.x())
                    vLine3.setPos(mousePoint.x())
                    vLine2.setPos(mousePoint.x())

            main_pg.proxy = pyqtgraph.SignalProxy(main_pg.scene().sigMouseMoved, rateLimit=20, slot=mouseMoved)
            sub_pg1.proxy = pyqtgraph.SignalProxy(main_pg.scene().sigMouseMoved, rateLimit=20, slot=mouseMoved)
            sub_pg2.proxy = pyqtgraph.SignalProxy(main_pg.scene().sigMouseMoved, rateLimit=20, slot=mouseMoved)

        if self.chart_name != name:
            self.ctpg_01.clear()
            self.ctpg_02.clear()
            self.ctpg_03.clear()
            xticks = [x.timestamp() - 32400 for x in df.index]
            self.chart1 = self.ctpg_01.plot(x=xticks, y=df['현재가'], pen=(200, 50, 50))
            self.chart2 = self.ctpg_02.plot(x=xticks, y=df['체결강도'], pen=(50, 200, 50))
            self.chart3 = self.ctpg_02.plot(x=xticks, y=df['체결강도평균'], pen=(50, 200, 200))
            self.chart4 = self.ctpg_02.plot(x=xticks, y=df['최고체결강도'], pen=(200, 50, 50))
            self.chart5 = self.ctpg_03.plot(x=xticks, y=df['초당거래대금'], pen=(200, 50, 50))
            self.chart6 = self.ctpg_03.plot(x=xticks, y=df['초당거래대금평균'], pen=(50, 50, 200))
            self.close_line = pg.InfiniteLine(angle=0)
            self.close_line.setPen(pg.mkPen(color_fg_bt))
            self.close_line.setPos(df['현재가'][-1])
            self.ctpg_01.addItem(self.close_line)
            self.chart1_data = df['현재가']
            self.chart2_data = df['체결강도']
            self.chart3_data = df['체결강도평균']
            self.chart4_data = df['최고체결강도']
            self.chart5_data = df['초당거래대금']
            self.chart6_data = df['초당거래대금평균']
            self.ctpg_01.getAxis('bottom').setLabel(text=name)
            crosshair(main_pg=self.ctpg_01, sub_pg1=self.ctpg_02, sub_pg2=self.ctpg_03)
            self.ctpg_01.enableAutoRange(enable=True)
            self.ctpg_02.enableAutoRange(enable=True)
            self.ctpg_03.enableAutoRange(enable=True)
            self.chart_name = name
        else:
            count = len(df)
            if len(self.chart1_data) >= 300:
                pdf1 = self.chart1_data[count:]
                pdf2 = self.chart2_data[count:]
                pdf3 = self.chart3_data[count:]
                pdf4 = self.chart4_data[count:]
                pdf5 = self.chart5_data[count:]
                pdf6 = self.chart6_data[count:]
            else:
                pdf1 = self.chart1_data
                pdf2 = self.chart2_data
                pdf3 = self.chart3_data
                pdf4 = self.chart4_data
                pdf5 = self.chart5_data
                pdf6 = self.chart6_data
            self.chart1_data = pdf1.append(df['현재가'])
            self.chart2_data = pdf2.append(df['체결강도'])
            self.chart3_data = pdf3.append(df['체결강도평균'])
            self.chart4_data = pdf4.append(df['최고체결강도'])
            self.chart5_data = pdf5.append(df['초당거래대금'])
            self.chart6_data = pdf6.append(df['초당거래대금평균'])
            xticks = [x.timestamp() - 32400 for x in self.chart1_data.index]
            self.chart1.setData(x=xticks, y=self.chart1_data)
            self.chart2.setData(x=xticks, y=self.chart2_data)
            self.chart3.setData(x=xticks, y=self.chart3_data)
            self.chart4.setData(x=xticks, y=self.chart4_data)
            self.chart5.setData(x=xticks, y=self.chart5_data)
            self.chart6.setData(x=xticks, y=self.chart6_data)
            self.close_line.setPos(df['현재가'][-1])
            self.ct_labellll_06.setText(f"현재가    {format(round(df['현재가'][-1], 2), ',')}")
            self.ct_labellll_07.setText(f"최고체결강도 {format(round(df['최고체결강도'][-1], 2), ',')}\n"
                                        f"체결강도평균 {format(round(df['체결강도평균'][-1], 2), ',')}\n"
                                        f"체결강도       {format(round(df['체결강도'][-1], 2), ',')}")
            self.ct_labellll_08.setText(f"초당거래대금평균 {format(df['초당거래대금평균'][-1], ',')}\n"
                                        f"초당거래대금       {format(df['초당거래대금'][-1], ',')}")

    def CheckboxChanged_01(self, state):
        if state == Qt.Checked:
            con = sqlite3.connect(DB_SETTING)
            df = pd.read_sql('SELECT * FROM sacc', con).set_index('index')
            con.close()
            if len(df) == 0 or df['아이디2'][0] == '':
                self.sj_main_checkBox_01.nextCheckState()
                QtWidgets.QMessageBox.critical(
                    self, '오류 알림', '두번째 계정이 설정되지 않아\n리시버를 선택할 수 없습니다.\n계정 설정 후 다시 선택하십시오.\n')
        else:
            if self.sj_main_checkBox_02.isChecked():
                self.sj_main_checkBox_02.nextCheckState()
            if self.sj_main_checkBox_03.isChecked():
                self.sj_main_checkBox_03.nextCheckState()

    def CheckboxChanged_02(self, state):
        if state == Qt.Checked:
            con = sqlite3.connect(DB_SETTING)
            df = pd.read_sql('SELECT * FROM sacc', con).set_index('index')
            con.close()
            if len(df) == 0 or df['아이디2'][0] == '':
                self.sj_main_checkBox_02.nextCheckState()
                QtWidgets.QMessageBox.critical(
                    self, '오류 알림', '두번째 계정이 설정되지 않아\n콜렉터를 선택할 수 없습니다.\n계정 설정 후 다시 선택하십시오.\n')
            elif not self.sj_main_checkBox_01.isChecked():
                self.sj_main_checkBox_01.nextCheckState()

    def CheckboxChanged_03(self, state):
        if state == Qt.Checked:
            con = sqlite3.connect(DB_SETTING)
            df = pd.read_sql('SELECT * FROM sacc', con).set_index('index')
            con.close()
            if len(df) == 0 or df['아이디1'][0] == '':
                self.sj_main_checkBox_03.nextCheckState()
                QtWidgets.QMessageBox.critical(
                    self, '오류 알림', '첫번째 계정이 설정되지 않아\n트레이더를 선택할 수 없습니다.\n계정 설정 후 다시 선택하십시오.\n')
            elif not self.sj_main_checkBox_01.isChecked():
                self.sj_main_checkBox_01.nextCheckState()

    def CheckboxChanged_04(self, state):
        if state != Qt.Checked:
            if self.sj_main_checkBox_05.isChecked():
                self.sj_main_checkBox_05.nextCheckState()
            if self.sj_main_checkBox_06.isChecked():
                self.sj_main_checkBox_06.nextCheckState()

    def CheckboxChanged_05(self, state):
        if state == Qt.Checked:
            if not self.sj_main_checkBox_04.isChecked():
                self.sj_main_checkBox_04.nextCheckState()

    def CheckboxChanged_06(self, state):
        if state == Qt.Checked:
            con = sqlite3.connect(DB_SETTING)
            df = pd.read_sql('SELECT * FROM cacc', con).set_index('index')
            con.close()
            if len(df) == 0 or df['Access_key'][0] == '':
                self.sj_main_checkBox_06.nextCheckState()
                QtWidgets.QMessageBox.critical(
                    self, '오류 알림', '업비트 계정이 설정되지 않아\n트레이더를 선택할 수 없습니다.\n계정 설정 후 다시 선택하십시오.\n')
            elif not self.sj_main_checkBox_04.isChecked():
                self.sj_main_checkBox_04.nextCheckState()

    def CheckboxChanged_07(self, state):
        if state != Qt.Checked:
            buttonReply = QtWidgets.QMessageBox.question(
                self, '장마감 후 저장',
                '실시간 저장을 해제하면 장마감 후 일괄 저장됩니다.\n계속하시겠습니까?\n',
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No
            )
            if buttonReply == QtWidgets.QMessageBox.No:
                self.sj_main_checkBox_07.nextCheckState()
        elif not self.sj_main_checkBox_08.isChecked():
            self.sj_main_checkBox_08.setChecked(True)

    def CheckboxChanged_08(self, state):
        if state != Qt.Checked:
            if self.sj_main_checkBox_07.isChecked():
                self.sj_main_checkBox_08.nextCheckState()
                QtWidgets.QMessageBox.critical(self, '오류 알림', '실시간 저장 방식은 전체 종목을 저장해야합니다.\n')
            else:
                buttonReply = QtWidgets.QMessageBox.question(
                    self, '당일거래목록만 저장',
                    '전체 종목 저장을 해제하면 당일거래목록만 저장됩니다.\n계속하시겠습니까?\n',
                    QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No
                )
                if buttonReply == QtWidgets.QMessageBox.No:
                    self.sj_main_checkBox_08.nextCheckState()

    def CheckboxChanged_09(self, state):
        if state != Qt.Checked:
            self.sj_main_checkBox_09.nextCheckState()
            QtWidgets.QMessageBox.critical(self, '오류 알림', '코인 틱데이터는 실시간 저장만 가능합니다.\n')

    def CheckboxChanged_10(self, state):
        if state != Qt.Checked:
            self.sj_main_checkBox_10.nextCheckState()
            QtWidgets.QMessageBox.critical(self, '오류 알림', '코인 틱데이터는 전체 종목 저장만 가능합니다.\n')

    def CheckboxChanged_11(self, state):
        if state != Qt.Checked and (self.trader_kiwoom_proc.is_alive() or self.trader_xing_proc.is_alive()):
            self.sj_stock_checkBox_01.nextCheckState()
            QtWidgets.QMessageBox.critical(self, '오류 알림', '장중에는 모의모드를 해제할 수 없습니다.\n')

    def CheckboxChanged_12(self, state):
        if state != Qt.Checked and self.trader_coin_proc.is_alive():
            self.sj_coin_checkBox_01.nextCheckState()
            QtWidgets.QMessageBox.critical(self, '오류 알림', '트레이더 실행 중에는 모의모드를 해제할 수 없습니다.\n')

    @QtCore.pyqtSlot(int, int)
    def CellClicked_01(self, row, col):
        tableWidget = None
        if self.focusWidget() == self.std_tableWidget:
            tableWidget = self.std_tableWidget
        elif self.focusWidget() == self.sgj_tableWidget:
            tableWidget = self.sgj_tableWidget
        elif self.focusWidget() == self.scj_tableWidget:
            tableWidget = self.scj_tableWidget
        elif self.focusWidget() == self.ctd_tableWidget:
            tableWidget = self.ctd_tableWidget
        elif self.focusWidget() == self.cgj_tableWidget:
            tableWidget = self.cgj_tableWidget
        elif self.focusWidget() == self.ccj_tableWidget:
            tableWidget = self.ccj_tableWidget
        if tableWidget is None:
            return
        item = tableWidget.item(row, 0)
        if item is None:
            return
        name = item.text()
        if self.ct_lineEdit_01.text() == '':
            self.ct_lineEdit_01.setText('60')
        self.ct_lineEdit_02.setText(name)
        tickcount = int(self.ct_lineEdit_01.text())
        self.ShowDialogChart()
        self.PutChart(name, tickcount, strf_time('%Y%m%d'), col)

    @QtCore.pyqtSlot(int)
    def CellClicked_02(self, row):
        item = self.sjg_tableWidget.item(row, 0)
        if item is None:
            return
        name = item.text()
        oc = comma2int(self.sjg_tableWidget.item(row, columns_jg.index('보유수량')).text())
        c = comma2int(self.sjg_tableWidget.item(row, columns_jg.index('현재가')).text())
        buttonReply = QtWidgets.QMessageBox.question(
            self, '주식 시장가 매도', f'{name} {oc}주를 시장가매도합니다.\n계속하시겠습니까?\n',
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No
        )
        if buttonReply == QtWidgets.QMessageBox.Yes:
            stockQ.put(['매도', self.dict_code[name], name, c, oc])

    @QtCore.pyqtSlot(int)
    def CellClicked_03(self, row):
        item = self.cjg_tableWidget.item(row, 0)
        if item is None:
            return
        code = item.text()
        oc = comma2float(self.cjg_tableWidget.item(row, columns_jg.index('보유수량')).text())
        c = comma2float(self.cjg_tableWidget.item(row, columns_jg.index('현재가')).text())
        buttonReply = QtWidgets.QMessageBox.question(
            self, '코인 시장가 매도', f'{code} {oc}개를 시장가매도합니다.\n계속하시겠습니까?\n',
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No
        )
        if buttonReply == QtWidgets.QMessageBox.Yes:
            coinQ.put(['매도', code, c, oc])

    @QtCore.pyqtSlot(int)
    def CellClicked_04(self, row):
        tableWidget = None
        searchdate = ''
        if self.focusWidget() == self.sds_tableWidget:
            tableWidget = self.sds_tableWidget
            searchdate = self.s_calendarWidget.selectedDate().toString('yyyyMMdd')
        elif self.focusWidget() == self.cds_tableWidget:
            tableWidget = self.cds_tableWidget
            searchdate = self.c_calendarWidget.selectedDate().toString('yyyyMMdd')
        if tableWidget is None:
            return
        item = tableWidget.item(row, 1)
        if item is None:
            return
        name = item.text()
        linetext = self.ct_lineEdit_01.text()
        tickcount = int(linetext) if linetext != '' else 60
        self.ShowDialogChart()
        self.PutChart(name, tickcount, searchdate, 4)

    def ShowDialogChart(self):
        if not self.dialog_chart.isVisible():
            self.dialog_chart.show()

    def ShowDialogHoga(self):
        if not self.dialog_hoga.isVisible():
            self.dialog_hoga.show()

    def ReturnPress_01(self):
        searchdate = self.ct_dateEdit.date().toString('yyyyMMdd')
        linetext = self.ct_lineEdit_01.text()
        tickcount = int(linetext) if linetext != '' else 60
        name = self.ct_lineEdit_02.text()
        if name == '':
            return
        self.PutChart(name, int(tickcount), searchdate, 4)

    def PutChart(self, name, tickcount, searchdate, col):
        coin = False
        if name in self.dict_code.keys():
            code = self.dict_code[name]
        elif name in self.dict_code.values():
            code = name
            name = self.dict_name[code]
        else:
            code = name
            coin = True

        if col < 4:
            if coin:
                cstgQ.put(code)
            else:
                sstgQ.put(code)
            self.ct_labellll_06.setVisible(True)
            self.ct_labellll_07.setVisible(True)
            self.ct_labellll_08.setVisible(True)

            if self.dialog_hoga.isVisible():
                if coin:
                    creceiv1Q.put(code)
                    creceiv2Q.put(code)
                else:
                    sreceivQ.put(code)
        else:
            chartQ.put([coin, code, name, tickcount, searchdate])
            self.ct_labellll_06.setVisible(False)
            self.ct_labellll_07.setVisible(False)
            self.ct_labellll_08.setVisible(False)

    def ButtonClicked_01(self):
        if self.main_tabWidget.currentWidget() == self.st_tab:
            if not self.s_calendarWidget.isVisible():
                boolean1 = False
                boolean2 = True
                self.tt_pushButton.setStyleSheet(style_bc_dk)
            else:
                boolean1 = True
                boolean2 = False
                self.tt_pushButton.setStyleSheet(style_bc_bt)
            self.stt_tableWidget.setVisible(boolean1)
            self.std_tableWidget.setVisible(boolean1)
            self.stj_tableWidget.setVisible(boolean1)
            self.sjg_tableWidget.setVisible(boolean1)
            self.sgj_tableWidget.setVisible(boolean1)
            self.scj_tableWidget.setVisible(boolean1)
            self.s_calendarWidget.setVisible(boolean2)
            self.sdt_tableWidget.setVisible(boolean2)
            self.sds_tableWidget.setVisible(boolean2)
            self.snt_pushButton_01.setVisible(boolean2)
            self.snt_pushButton_02.setVisible(boolean2)
            self.snt_pushButton_03.setVisible(boolean2)
            self.snt_tableWidget.setVisible(boolean2)
            self.sns_tableWidget.setVisible(boolean2)
        elif self.main_tabWidget.currentWidget() == self.ct_tab:
            if not self.c_calendarWidget.isVisible():
                boolean1 = False
                boolean2 = True
                self.tt_pushButton.setStyleSheet(style_bc_dk)
            else:
                boolean1 = True
                boolean2 = False
                self.tt_pushButton.setStyleSheet(style_bc_bt)
            self.ctt_tableWidget.setVisible(boolean1)
            self.ctd_tableWidget.setVisible(boolean1)
            self.ctj_tableWidget.setVisible(boolean1)
            self.cjg_tableWidget.setVisible(boolean1)
            self.cgj_tableWidget.setVisible(boolean1)
            self.ccj_tableWidget.setVisible(boolean1)
            self.c_calendarWidget.setVisible(boolean2)
            self.cdt_tableWidget.setVisible(boolean2)
            self.cds_tableWidget.setVisible(boolean2)
            self.cnt_pushButton_01.setVisible(boolean2)
            self.cnt_pushButton_02.setVisible(boolean2)
            self.cnt_pushButton_03.setVisible(boolean2)
            self.cnt_tableWidget.setVisible(boolean2)
            self.cns_tableWidget.setVisible(boolean2)
        else:
            QtWidgets.QMessageBox.warning(self, '오류 알림', '해당 버튼은 트레이더탭에서만 작동합니다.\n')

    # noinspection PyArgumentList
    def ButtonClicked_02(self):
        buttonReply = QtWidgets.QMessageBox.question(
            self, '주식 수동 시작', '주식 리시버 또는 트레이더를 시작합니다.\n계속하시겠습니까?\n',
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No
        )
        if buttonReply == QtWidgets.QMessageBox.Yes:
            if self.dict_set['아이디2'] is None:
                QtWidgets.QMessageBox.critical(
                    self, '오류 알림', '두번째 계정이 설정되지 않아\n리시버를 시작할 수 없습니다.\n계정 설정 후 다시 시작하십시오.\n')
            elif self.dict_set['주식리시버']:
                self.StockReceiverStart()
                QTest.qWait(20000)
            if self.dict_set['아이디1'] is None:
                QtWidgets.QMessageBox.critical(
                    self, '오류 알림', '첫번째 계정이 설정되지 않아\n트레이더를 시작할 수 없습니다.\n계정 설정 후 다시 시작하십시오.\n')
            elif self.dict_set['주식트레이더']:
                self.StockTraderStart()

    def ButtonClicked_03(self):
        if self.geometry().width() > 1000:
            self.setFixedSize(722, 383)
            self.zo_pushButton.setStyleSheet(style_bc_dk)
        else:
            self.setFixedSize(1403, 763)
            self.zo_pushButton.setStyleSheet(style_bc_bt)

    def ButtonClicked_04(self):
        buttonReply = QtWidgets.QMessageBox.warning(
            self, '데이터베이스 초기화', '체결목록, 잔고목록, 거래목록, 일별목록이 모두 초기화됩니다.\n계속하시겠습니까?\n',
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No
        )
        if buttonReply == QtWidgets.QMessageBox.Yes:
            query1Q.put([2, 'DELETE FROM s_jangolist'])
            query1Q.put([2, 'DELETE FROM s_tradelist'])
            query1Q.put([2, 'DELETE FROM s_chegeollist'])
            query1Q.put([2, 'DELETE FROM s_totaltradelist'])
            query1Q.put([2, 'DELETE FROM c_jangolist'])
            query1Q.put([2, 'DELETE FROM c_tradelist'])
            query1Q.put([2, 'DELETE FROM c_chegeollist'])
            query1Q.put([2, 'DELETE FROM c_totaltradelist'])
            self.dd_pushButton.setStyleSheet(style_bc_dk)

    def ButtonClicked_05(self):
        buttonReply = QtWidgets.QMessageBox.warning(
            self, '계정 설정 초기화', '계정 설정 항목이 모두 초기화됩니다.\n계속하시겠습니까?\n',
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No
        )
        if buttonReply == QtWidgets.QMessageBox.Yes:
            query1Q.put([1, 'DELETE FROM sacc'])
            query1Q.put([1, 'DELETE FROM cacc'])
            query1Q.put([1, 'DELETE FROM telegram'])
            self.sd_pushButton.setStyleSheet(style_bc_dk)

    def ButtonClicked_06(self, cmd):
        if '집계' in cmd:
            if 'S' in cmd:
                gubun = 'S'
                table = 's_totaltradelist'
            else:
                gubun = 'C'
                table = 'c_totaltradelist'
            con = sqlite3.connect(DB_TRADELIST)
            df = pd.read_sql(f'SELECT * FROM {table}', con)
            con.close()
            df = df[::-1]
            if len(df) > 0:
                sd = strp_time('%Y%m%d', df['index'][df.index[0]])
                ld = strp_time('%Y%m%d', df['index'][df.index[-1]])
                pr = str((sd - ld).days + 1) + '일'
                nbg, nsg = df['총매수금액'].sum(), df['총매도금액'].sum()
                sp = round((nsg / nbg - 1) * 100, 2)
                npg, nmg = df['총수익금액'].sum(), df['총손실금액'].sum()
                nsig = df['수익금합계'].sum()
                df2 = pd.DataFrame(columns=columns_nt)
                df2.at[0] = pr, nbg, nsg, npg, nmg, sp, nsig
                self.UpdateTablewidget([ui_num[f'{gubun}누적합계'], df2])
            else:
                QtWidgets.QMessageBox.critical(self, '오류 알림', '거래목록이 존재하지 않습니다.\n')
                return
            if cmd == '일별집계':
                df = df.rename(columns={'index': '일자'})
                self.UpdateTablewidget([ui_num[f'{gubun}누적상세'], df])
            elif cmd == '월별집계':
                df['일자'] = df['index'].apply(lambda x: x[:6])
                df2 = pd.DataFrame(columns=columns_nd)
                lastmonth = df['일자'][df.index[-1]]
                month = strf_time('%Y%m')
                while int(month) >= int(lastmonth):
                    df3 = df[df['일자'] == month]
                    if len(df3) > 0:
                        tbg, tsg = df3['총매수금액'].sum(), df3['총매도금액'].sum()
                        sp = round((tsg / tbg - 1) * 100, 2)
                        tpg, tmg = df3['총수익금액'].sum(), df3['총손실금액'].sum()
                        ttsg = df3['수익금합계'].sum()
                        df2.at[month] = month, tbg, tsg, tpg, tmg, sp, ttsg
                    month = str(int(month) - 89) if int(month[4:]) == 1 else str(int(month) - 1)
                self.UpdateTablewidget([ui_num[f'{gubun}누적상세'], df2])
            elif cmd == '연도별집계':
                df['일자'] = df['index'].apply(lambda x: x[:4])
                df2 = pd.DataFrame(columns=columns_nd)
                lastyear = df['일자'][df.index[-1]]
                year = strf_time('%Y')
                while int(year) >= int(lastyear):
                    df3 = df[df['일자'] == year]
                    if len(df3) > 0:
                        tbg, tsg = df3['총매수금액'].sum(), df3['총매도금액'].sum()
                        sp = round((tsg / tbg - 1) * 100, 2)
                        tpg, tmg = df3['총수익금액'].sum(), df3['총손실금액'].sum()
                        ttsg = df3['수익금합계'].sum()
                        df2.at[year] = year, tbg, tsg, tpg, tmg, sp, ttsg
                    year = str(int(year) - 1)
                self.UpdateTablewidget([ui_num[f'{gubun}누적상세'], df2])

    def Activated_01(self):
        strategy_name = self.ssb_comboBox.currentText()
        if strategy_name != '':
            con = sqlite3.connect(DB_STOCK_STRATEGY)
            df = pd.read_sql(f"SELECT * FROM buy WHERE `index` = '{strategy_name}'", con).set_index('index')
            con.close()
            self.ss_textEdit_01.clear()
            self.ss_textEdit_01.append(df['전략코드'][strategy_name])
            self.ssb_lineEdit.setText(strategy_name)

    def Activated_02(self):
        strategy_name = self.sss_comboBox.currentText()
        if strategy_name != '':
            con = sqlite3.connect(DB_STOCK_STRATEGY)
            df = pd.read_sql(f"SELECT * FROM sell WHERE `index` = '{strategy_name}'", con).set_index('index')
            con.close()
            self.ss_textEdit_02.clear()
            self.ss_textEdit_02.append(df['전략코드'][strategy_name])
            self.sss_lineEdit.setText(strategy_name)

    def Activated_03(self):
        strategy_name = self.csb_comboBox.currentText()
        if strategy_name != '':
            con = sqlite3.connect(DB_COIN_STRATEGY)
            df = pd.read_sql(f"SELECT * FROM buy WHERE `index` = '{strategy_name}'", con).set_index('index')
            con.close()
            self.cs_textEdit_01.clear()
            self.cs_textEdit_01.append(df['전략코드'][strategy_name])
            self.csb_lineEdit.setText(strategy_name)

    def Activated_04(self):
        strategy_name = self.css_comboBox.currentText()
        if strategy_name != '':
            con = sqlite3.connect(DB_COIN_STRATEGY)
            df = pd.read_sql(f"SELECT * FROM sell WHERE `index` = '{strategy_name}'", con).set_index('index')
            con.close()
            self.cs_textEdit_02.clear()
            self.cs_textEdit_02.append(df['전략코드'][strategy_name])
            self.css_lineEdit.setText(strategy_name)

    def ButtonClicked_07(self):
        con = sqlite3.connect(DB_STOCK_STRATEGY)
        df = pd.read_sql('SELECT * FROM buy', con).set_index('index')
        con.close()
        if len(df) > 0:
            self.ssb_comboBox.clear()
            for i, index in enumerate(df.index):
                self.ssb_comboBox.addItem(index)
                if i == 0:
                    self.ssb_lineEdit.setText(index)
            self.ssb_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_08(self):
        strategy_name = self.ssb_lineEdit.text()
        strategy = self.ss_textEdit_01.toPlainText()
        if strategy_name == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '매수전략의 이름이 공백 상태입니다.\n이름을 입력하십시오.\n')
        elif strategy == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '매수전략의 코드가 공백 상태입니다.\n코드를 작성하십시오.\n')
        else:
            query1Q.put([3, f"DELETE FROM buy WHERE `index` = '{strategy_name}'"])
            df = pd.DataFrame({'전략코드': [strategy]}, index=[strategy_name])
            query1Q.put([3, df, 'buy', 'append'])
            self.ssb_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_09(self):
        self.ss_textEdit_01.clear()
        self.ss_textEdit_01.append(stock_buy_var)
        self.ssb_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_10(self):
        strategy = self.ss_textEdit_01.toPlainText()
        if strategy == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '매수전략의 코드가 공백 상태입니다.\n')
        else:
            buttonReply = QtWidgets.QMessageBox.question(
                self, '전략시작',
                '10시전에는 장초전략, 이후는 장중전략으로 설정되어\n매수전략의 연산을 시작합니다. 계속하시겠습니까?\n',
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No
            )
            if buttonReply == QtWidgets.QMessageBox.Yes:
                sstgQ.put(['매수전략', strategy])
                self.ssb_pushButton_04.setStyleSheet(style_bc_dk)
                self.ssb_pushButton_16.setStyleSheet(style_bc_st)

    def ButtonClicked_11(self):
        self.ss_textEdit_01.append(stock_buy1)

    def ButtonClicked_12(self):
        self.ss_textEdit_01.append(stock_buy2)

    def ButtonClicked_13(self):
        self.ss_textEdit_01.append(stock_buy3)

    def ButtonClicked_14(self):
        self.ss_textEdit_01.append(stock_buy4)

    def ButtonClicked_15(self):
        self.ss_textEdit_01.append(stock_buy5)

    def ButtonClicked_16(self):
        self.ss_textEdit_01.append(stock_buy6)

    def ButtonClicked_17(self):
        self.ss_textEdit_01.append(stock_buy7)

    def ButtonClicked_18(self):
        self.ss_textEdit_01.append(stock_buy8)

    def ButtonClicked_19(self):
        self.ss_textEdit_01.append(stock_buy9)

    def ButtonClicked_20(self):
        self.ss_textEdit_01.append(stock_buy10)

    def ButtonClicked_21(self):
        self.ss_textEdit_01.append(stock_buy_signal)

    # noinspection PyMethodMayBeStatic
    def ButtonClicked_22(self):
        sstgQ.put(['매수전략중지', ''])
        self.ssb_pushButton_16.setStyleSheet(style_bc_dk)
        self.ssb_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_23(self):
        if self.backtester_proc is not None and self.backtester_proc.is_alive():
            QtWidgets.QMessageBox.critical(self, '오류 알림', '현재 백테스터가 실행중입니다.\n중복 실행할 수 없습니다.\n')
        else:
            startday = self.ssb_dateEdit_01.date().toString('yyyyMMdd')
            endday = self.ssb_dateEdit_02.date().toString('yyyyMMdd')
            starttime = self.ssb_lineEdit_01.text()
            endtime = self.ssb_lineEdit_02.text()
            betting = self.ssb_lineEdit_03.text()
            avgtime = self.ssb_lineEdit_04.text()
            multi = self.ssb_lineEdit_05.text()
            buystg = self.ssb_comboBox.currentText()
            sellstg = self.sss_comboBox.currentText()
            if startday == '' or endday == '' or starttime == '' or endtime == '' or betting == '' or \
                    avgtime == '' or multi == '':
                QtWidgets.QMessageBox.critical(self, '오류 알림', '일부 설정값이 공백 상태입니다.\n')
                return
            if buystg == '' or sellstg == '':
                QtWidgets.QMessageBox.critical(self, '오류 알림', '전략을 저장하고 콤보박스에서 선택하십시오.\n')
                return
            backQ.put([startday, endday, starttime, endtime, betting, avgtime, multi, buystg, sellstg])
            self.backtester_proc = Process(target=BackTesterStockStgMain, args=(windowQ, backQ))
            self.backtester_proc.start()
            self.ButtonClicked_91()

    def ButtonClicked_24(self):
        if self.backtester_proc is None or not self.backtester_proc.is_alive():
            buttonReply = QtWidgets.QMessageBox.question(
                self, '최적화 백테스터',
                'backtester/backtester_stock_vc.py 파일을\n본인의 전략에 맞게 수정 후 사용해야합니다.\n계속하시겠습니까?\n',
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No
            )
            if buttonReply == QtWidgets.QMessageBox.Yes:
                self.backtester_proc = Process(target=BacktesterStockVcMain, args=(windowQ,))
                self.backtester_proc.start()
                self.ButtonClicked_91()

    def ButtonClicked_25(self):
        con = sqlite3.connect(DB_STOCK_STRATEGY)
        df = pd.read_sql('SELECT * FROM sell', con).set_index('index')
        con.close()
        if len(df) > 0:
            self.sss_comboBox.clear()
            for i, index in enumerate(df.index):
                self.sss_comboBox.addItem(index)
                if i == 0:
                    self.sss_lineEdit.setText(index)
            self.sss_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_26(self):
        strategy_name = self.sss_lineEdit.text()
        strategy = self.ss_textEdit_02.toPlainText()
        if strategy_name == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '매도전략의 이름이 공백 상태입니다.\n이름을 입력하십시오.\n')
        elif strategy == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '매도전략의 코드가 공백 상태입니다.\n코드를 작성하십시오.\n')
        else:
            query1Q.put([3, f"DELETE FROM sell WHERE `index` = '{strategy_name}'"])
            df = pd.DataFrame({'전략코드': [strategy]}, index=[strategy_name])
            query1Q.put([3, df, 'sell', 'append'])
            self.sss_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_27(self):
        self.ss_textEdit_02.clear()
        self.ss_textEdit_02.append(stock_sell_var)
        self.sss_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_28(self):
        strategy = self.ss_textEdit_02.toPlainText()
        if strategy == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '매도전략의 코드가 공백 상태입니다.\n')
        else:
            buttonReply = QtWidgets.QMessageBox.question(
                self, '전략시작',
                '10시전에는 장초전략, 이후는 장중전략으로 설정되어\n매도전략의 연산을 시작합니다. 계속하시겠습니까?\n',
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No
            )
            if buttonReply == QtWidgets.QMessageBox.Yes:
                sstgQ.put(['매도전략', strategy])
                self.sss_pushButton_04.setStyleSheet(style_bc_dk)
                self.sss_pushButton_14.setStyleSheet(style_bc_st)

    def ButtonClicked_29(self):
        self.ss_textEdit_02.append(stock_sell1)

    def ButtonClicked_30(self):
        self.ss_textEdit_02.append(stock_sell2)

    def ButtonClicked_31(self):
        self.ss_textEdit_02.append(stock_sell3)

    def ButtonClicked_32(self):
        self.ss_textEdit_02.append(stock_sell4)

    def ButtonClicked_33(self):
        self.ss_textEdit_02.append(stock_sell5)

    def ButtonClicked_34(self):
        self.ss_textEdit_02.append(stock_sell6)

    def ButtonClicked_35(self):
        self.ss_textEdit_02.append(stock_sell7)

    def ButtonClicked_36(self):
        self.ss_textEdit_02.append(stock_sell8)

    def ButtonClicked_37(self):
        self.ss_textEdit_02.append(stock_sell_signal)

    # noinspection PyMethodMayBeStatic
    def ButtonClicked_38(self):
        sstgQ.put(['매도전략중지', ''])
        self.sss_pushButton_14.setStyleSheet(style_bc_dk)
        self.sss_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_39(self):
        con = sqlite3.connect(DB_COIN_STRATEGY)
        df = pd.read_sql('SELECT * FROM buy', con).set_index('index')
        con.close()
        if len(df) > 0:
            self.csb_comboBox.clear()
            for i, index in enumerate(df.index):
                self.csb_comboBox.addItem(index)
                if i == 0:
                    self.csb_lineEdit.setText(index)
            self.csb_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_40(self):
        strategy_name = self.csb_lineEdit.text()
        strategy = self.cs_textEdit_01.toPlainText()
        if strategy_name == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '매수전략의 이름이 공백 상태입니다.\n이름을 입력하십시오.\n')
        elif strategy == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '매수전략의 코드가 공백 상태입니다.\n코드를 작성하십시오.\n')
        else:
            query1Q.put([4, f"DELETE FROM buy WHERE `index` = '{strategy_name}'"])
            df = pd.DataFrame({'전략코드': [strategy]}, index=[strategy_name])
            query1Q.put([4, df, 'buy', 'append'])
            self.csb_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_41(self):
        self.cs_textEdit_01.clear()
        self.cs_textEdit_01.append(coin_buy_var)
        self.csb_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_42(self):
        strategy = self.cs_textEdit_01.toPlainText()
        if strategy == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '매수전략의 코드가 공백 상태입니다.\n')
        else:
            buttonReply = QtWidgets.QMessageBox.question(
                self, '전략시작',
                '10시전에는 장초전략, 이후는 장중전략으로 설정되어\n매수전략의 연산을 시작합니다. 계속하시겠습니까?\n',
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No
            )
            if buttonReply == QtWidgets.QMessageBox.Yes:
                cstgQ.put(['매수전략', strategy])
                self.csb_pushButton_04.setStyleSheet(style_bc_dk)
                self.csb_pushButton_16.setStyleSheet(style_bc_st)

    def ButtonClicked_43(self):
        self.cs_textEdit_01.append(coin_buy1)

    def ButtonClicked_44(self):
        self.cs_textEdit_01.append(coin_buy2)

    def ButtonClicked_45(self):
        self.cs_textEdit_01.append(coin_buy3)

    def ButtonClicked_46(self):
        self.cs_textEdit_01.append(coin_buy4)

    def ButtonClicked_47(self):
        self.cs_textEdit_01.append(coin_buy5)

    def ButtonClicked_48(self):
        self.cs_textEdit_01.append(coin_buy6)

    def ButtonClicked_49(self):
        self.cs_textEdit_01.append(coin_buy7)

    def ButtonClicked_50(self):
        self.cs_textEdit_01.append(coin_buy8)

    def ButtonClicked_51(self):
        self.cs_textEdit_01.append(coin_buy9)

    def ButtonClicked_52(self):
        self.cs_textEdit_01.append(coin_buy10)

    def ButtonClicked_53(self):
        self.cs_textEdit_01.append(coin_buy_signal)

    # noinspection PyMethodMayBeStatic
    def ButtonClicked_54(self):
        cstgQ.put(['매수전략중지', ''])
        self.csb_pushButton_16.setStyleSheet(style_bc_dk)
        self.csb_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_55(self):
        if self.backtester_proc is not None and self.backtester_proc.is_alive():
            QtWidgets.QMessageBox.critical(self, '오류 알림', '현재 백테스터가 실행중입니다.\n중복 실행할 수 없습니다.\n')
        else:
            startday = self.ssb_dateEdit_01.date().toString('yyyyMMdd')
            endday = self.ssb_dateEdit_02.date().toString('yyyyMMdd')
            starttime = self.csb_lineEdit_01.text()
            endtime = self.csb_lineEdit_02.text()
            betting = self.csb_lineEdit_03.text()
            avgtime = self.csb_lineEdit_04.text()
            multi = self.csb_lineEdit_05.text()
            buystg = self.csb_comboBox.currentText()
            sellstg = self.css_comboBox.currentText()
            if startday == '' or endday == '' or starttime == '' or endtime == '' or betting == '' or \
                    avgtime == '' or multi == '':
                QtWidgets.QMessageBox.critical(self, '오류 알림', '일부 설정값이 공백 상태입니다.\n')
                return
            if buystg == '' or sellstg == '':
                QtWidgets.QMessageBox.critical(self, '오류 알림', '전략을 저장하고 콤보박스에서 선택하십시오.\n')
                return
            backQ.put([startday, endday, starttime, endtime, betting, avgtime, multi, buystg, sellstg])
            self.backtester_proc = Process(target=BackTesterCoinStgMain, args=(windowQ, backQ))
            self.backtester_proc.start()
            self.ButtonClicked_93()

    def ButtonClicked_56(self):
        if self.backtester_proc is None or not self.backtester_proc.is_alive():
            buttonReply = QtWidgets.QMessageBox.question(
                self, '최적화 백테스터',
                'backtester/backtester_coin_vc.py 파일을\n본인의 전략에 맞게 수정 후 사용해야합니다.\n계속하시겠습니까?\n',
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No
            )
            if buttonReply == QtWidgets.QMessageBox.Yes:
                self.backtester_proc = Process(target=BacktesterCoinVcMain, args=(windowQ,))
                self.backtester_proc.start()
                self.ButtonClicked_93()

    def ButtonClicked_57(self):
        con = sqlite3.connect(DB_COIN_STRATEGY)
        df = pd.read_sql('SELECT * FROM sell', con).set_index('index')
        con.close()
        if len(df) > 0:
            self.css_comboBox.clear()
            for i, index in enumerate(df.index):
                self.css_comboBox.addItem(index)
                if i == 0:
                    self.css_lineEdit.setText(index)
            self.css_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_58(self):
        strategy_name = self.css_lineEdit.text()
        strategy = self.cs_textEdit_02.toPlainText()
        if strategy_name == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '매도전략의 이름이 공백 상태입니다.\n이름을 입력하십시오.\n')
        elif strategy == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '매도전략의 코드가 공백 상태입니다.\n코드를 작성하십시오.\n')
        else:
            query1Q.put([4, f"DELETE FROM sell WHERE `index` = '{strategy_name}'"])
            df = pd.DataFrame({'전략코드': [strategy]}, index=[strategy_name])
            query1Q.put([4, df, 'sell', 'append'])
            self.css_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_59(self):
        self.cs_textEdit_02.clear()
        self.cs_textEdit_02.append(coin_sell_var)
        self.css_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_60(self):
        strategy = self.cs_textEdit_02.toPlainText()
        if strategy == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '매도전략의 코드가 공백 상태입니다.\n')
        else:
            buttonReply = QtWidgets.QMessageBox.question(
                self, '전략시작',
                '10시전에는 장초전략, 이후는 장중전략으로 설정되어\n매도전략의 연산을 시작합니다. 계속하시겠습니까?\n',
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No
            )
            if buttonReply == QtWidgets.QMessageBox.Yes:
                cstgQ.put(['매도전략', strategy])
                self.css_pushButton_04.setStyleSheet(style_bc_dk)
                self.css_pushButton_14.setStyleSheet(style_bc_st)

    def ButtonClicked_61(self):
        self.cs_textEdit_02.append(coin_sell1)

    def ButtonClicked_62(self):
        self.cs_textEdit_02.append(coin_sell2)

    def ButtonClicked_63(self):
        self.cs_textEdit_02.append(coin_sell3)

    def ButtonClicked_64(self):
        self.cs_textEdit_02.append(coin_sell4)

    def ButtonClicked_65(self):
        self.cs_textEdit_02.append(coin_sell5)

    def ButtonClicked_66(self):
        self.cs_textEdit_02.append(coin_sell6)

    def ButtonClicked_67(self):
        self.cs_textEdit_02.append(coin_sell7)

    def ButtonClicked_68(self):
        self.cs_textEdit_02.append(coin_sell8)

    def ButtonClicked_69(self):
        self.cs_textEdit_02.append(coin_sell_signal)

    def ButtonClicked_70(self):
        cstgQ.put(['매도전략중지', ''])
        self.css_pushButton_14.setStyleSheet(style_bc_dk)
        self.css_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_71(self):
        con = sqlite3.connect(DB_SETTING)
        df = pd.read_sql('SELECT * FROM main', con).set_index('index')
        con.close()
        if len(df) > 0:
            self.sj_main_comboBox_01.setCurrentText(df['증권사'][0])
            self.sj_main_checkBox_01.setChecked(True) if df['주식리시버'][0] else self.sj_main_checkBox_01.setChecked(False)
            self.sj_main_checkBox_02.setChecked(True) if df['주식콜렉터'][0] else self.sj_main_checkBox_02.setChecked(False)
            self.sj_main_checkBox_03.setChecked(True) if df['주식트레이더'][0] else self.sj_main_checkBox_03.setChecked(False)
            self.sj_main_comboBox_02.setCurrentText(df['거래소'][0])
            self.sj_main_checkBox_04.setChecked(True) if df['코인리시버'][0] else self.sj_main_checkBox_04.setChecked(False)
            self.sj_main_checkBox_05.setChecked(True) if df['코인콜렉터'][0] else self.sj_main_checkBox_05.setChecked(False)
            self.sj_main_checkBox_06.setChecked(True) if df['코인트레이더'][0] else self.sj_main_checkBox_06.setChecked(False)
            self.sj_main_lineEdit_01.setText(str(df['주식순위시간'][0]))
            self.sj_main_lineEdit_02.setText(str(df['주식순위선정'][0]))
            self.sj_main_lineEdit_03.setText(str(df['코인순위시간'][0]))
            self.sj_main_lineEdit_04.setText(str(df['코인순위선정'][0]))
            self.sj_main_checkBox_07.setChecked(True) if df['주식실시간저장'][0] else self.sj_main_checkBox_07.setChecked(False)
            self.sj_main_checkBox_08.setChecked(True) if df['주식전체종목저장'][0] else self.sj_main_checkBox_08.setChecked(False)
            self.sj_main_lineEdit_05.setText(str(df['주식저장주기'][0]))
            self.sj_main_checkBox_09.setChecked(True)
            self.sj_main_checkBox_10.setChecked(True)
            self.sj_main_lineEdit_06.setText(str(df['코인저장주기'][0]))
            self.UpdateTexedit([ui_num['설정텍스트'], '시스템 기본 설정값 불러오기 완료'])
        else:
            QtWidgets.QMessageBox.critical(self, '오류 알림', '시스템 기본 설정값이\n존재하지 않습니다.\n')

    def ButtonClicked_72(self):
        con = sqlite3.connect(DB_SETTING)
        df = pd.read_sql('SELECT * FROM sacc', con).set_index('index')
        con.close()
        if len(df) > 0:
            self.sj_sacc_lineEdit_01.setText(df['아이디1'][0])
            self.sj_sacc_lineEdit_02.setText(df['비밀번호1'][0])
            self.sj_sacc_lineEdit_03.setText(df['인증서비밀번호1'][0])
            self.sj_sacc_lineEdit_04.setText(df['계좌비밀번호1'][0])
            self.sj_sacc_lineEdit_05.setText(df['아이디2'][0])
            self.sj_sacc_lineEdit_06.setText(df['비밀번호2'][0])
            self.sj_sacc_lineEdit_07.setText(df['인증서비밀번호2'][0])
            self.sj_sacc_lineEdit_08.setText(df['계좌비밀번호2'][0])
            self.UpdateTexedit([ui_num['설정텍스트'], '주식 계정 설정값 불러오기 완료'])
        else:
            QtWidgets.QMessageBox.critical(self, '오류 알림', '주식 계정 설정값이\n존재하지 않습니다.\n')

    def ButtonClicked_73(self):
        con = sqlite3.connect(DB_SETTING)
        df = pd.read_sql('SELECT * FROM cacc', con).set_index('index')
        con.close()
        if len(df) > 0:
            self.sj_cacc_lineEdit_01.setText(df['Access_key'][0])
            self.sj_cacc_lineEdit_02.setText(df['Secret_key'][0])
            self.UpdateTexedit([ui_num['설정텍스트'], '업비트 계정 설정값 불러오기 완료'])
        else:
            QtWidgets.QMessageBox.critical(self, '오류 알림', '업비트 계정 설정값이\n존재하지 않습니다.\n')

    def ButtonClicked_74(self):
        con = sqlite3.connect(DB_SETTING)
        df = pd.read_sql('SELECT * FROM telegram', con).set_index('index')
        con.close()
        if len(df) > 0:
            self.sj_tele_lineEdit_01.setText(df['str_bot'][0])
            self.sj_tele_lineEdit_02.setText(str(df['int_id'][0]))
            self.UpdateTexedit([ui_num['설정텍스트'], '텔레그램 봇토큰 및 사용자 아이디 설정값 불러오기 완료'])
        else:
            QtWidgets.QMessageBox.critical(self, '오류 알림', '텔레그램 봇토큰 및 사용자 아이디\n설정값이 존재하지 않습니다.\n')

    def ButtonClicked_75(self):
        con = sqlite3.connect(DB_SETTING)
        df = pd.read_sql('SELECT * FROM stock', con).set_index('index')
        con.close()
        con = sqlite3.connect(DB_STOCK_STRATEGY)
        dfb = pd.read_sql('SELECT * FROM buy', con).set_index('index')
        dfs = pd.read_sql('SELECT * FROM sell', con).set_index('index')
        con.close()
        if len(df) > 0:
            self.sj_stock_checkBox_01.setChecked(True) if df['주식모의투자'][0] else self.sj_stock_checkBox_01.setChecked(False)
            self.sj_stock_checkBox_02.setChecked(True) if df['주식알림소리'][0] else self.sj_stock_checkBox_02.setChecked(False)
            if len(dfb) > 0:
                self.sj_stock_comboBox_01.clear()
                self.sj_stock_comboBox_03.clear()
                for index in dfb.index:
                    self.sj_stock_comboBox_01.addItem(index)
                    self.sj_stock_comboBox_03.addItem(index)
                if df['주식장초매수전략'][0] != '':
                    self.sj_stock_comboBox_01.setCurrentText(df['주식장초매수전략'][0])
                if df['주식장중매수전략'][0] != '':
                    self.sj_stock_comboBox_03.setCurrentText(df['주식장중매수전략'][0])
            self.sj_stock_lineEdit_01.setText(str(df['주식장초평균값계산틱수'][0]))
            self.sj_stock_lineEdit_02.setText(str(df['주식장초최대매수종목수'][0]))
            if len(dfs) > 0:
                self.sj_stock_comboBox_02.clear()
                self.sj_stock_comboBox_04.clear()
                for index in dfs.index:
                    self.sj_stock_comboBox_02.addItem(index)
                    self.sj_stock_comboBox_04.addItem(index)
                if df['주식장초매도전략'][0] != '':
                    self.sj_stock_comboBox_02.setCurrentText(df['주식장초매도전략'][0])
                if df['주식장중매도전략'][0] != '':
                    self.sj_stock_comboBox_04.setCurrentText(df['주식장중매도전략'][0])
            self.sj_stock_lineEdit_03.setText(str(df['주식장중평균값계산틱수'][0]))
            self.sj_stock_lineEdit_04.setText(str(df['주식장중최대매수종목수'][0]))
            self.UpdateTexedit([ui_num['설정텍스트'], '주식 전략 설정값 불러오기 완료'])
        else:
            QtWidgets.QMessageBox.critical(self, '오류 알림', '주식 전략 설정값이\n존재하지 않습니다.\n')

    def ButtonClicked_76(self):
        con = sqlite3.connect(DB_SETTING)
        df = pd.read_sql('SELECT * FROM coin', con).set_index('index')
        con.close()
        con = sqlite3.connect(DB_COIN_STRATEGY)
        dfb = pd.read_sql('SELECT * FROM buy', con).set_index('index')
        dfs = pd.read_sql('SELECT * FROM sell', con).set_index('index')
        con.close()
        if len(df) > 0:
            self.sj_coin_checkBox_01.setChecked(True) if df['코인모의투자'][0] else self.sj_coin_checkBox_01.setChecked(False)
            self.sj_coin_checkBox_02.setChecked(True) if df['코인알림소리'][0] else self.sj_coin_checkBox_02.setChecked(False)
            if len(dfb) > 0:
                self.sj_coin_comboBox_01.clear()
                self.sj_coin_comboBox_03.clear()
                for index in dfb.index:
                    self.sj_coin_comboBox_01.addItem(index)
                    self.sj_coin_comboBox_03.addItem(index)
                if df['코인장초매수전략'][0] != '':
                    self.sj_coin_comboBox_01.setCurrentText(df['코인장초매수전략'][0])
                if df['코인장중매수전략'][0] != '':
                    self.sj_coin_comboBox_03.setCurrentText(df['코인장중매수전략'][0])
            self.sj_coin_lineEdit_01.setText(str(df['코인장초평균값계산틱수'][0]))
            self.sj_coin_lineEdit_02.setText(str(df['코인장초최대매수종목수'][0]))
            if len(dfs) > 0:
                self.sj_coin_comboBox_02.clear()
                self.sj_coin_comboBox_04.clear()
                for index in dfs.index:
                    self.sj_coin_comboBox_02.addItem(index)
                    self.sj_coin_comboBox_04.addItem(index)
                if df['코인장초매도전략'][0] != '':
                    self.sj_coin_comboBox_02.setCurrentText(df['코인장초매도전략'][0])
                if df['코인장중매도전략'][0] != '':
                    self.sj_coin_comboBox_04.setCurrentText(df['코인장중매도전략'][0])
            self.sj_coin_lineEdit_03.setText(str(df['코인장중평균값계산틱수'][0]))
            self.sj_coin_lineEdit_04.setText(str(df['코인장중최대매수종목수'][0]))
            self.UpdateTexedit([ui_num['설정텍스트'], '코인 전략 설정값 불러오기 완료'])
        else:
            QtWidgets.QMessageBox.critical(self, '오류 알림', '코인 전략 설정값이\n존재하지 않습니다.\n')

    def ButtonClicked_77(self):
        sg = self.sj_main_comboBox_01.currentText()
        sr = 1 if self.sj_main_checkBox_01.isChecked() else 0
        sc = 1 if self.sj_main_checkBox_02.isChecked() else 0
        st = 1 if self.sj_main_checkBox_03.isChecked() else 0
        cg = self.sj_main_comboBox_02.currentText()
        cr = 1 if self.sj_main_checkBox_04.isChecked() else 0
        cc = 1 if self.sj_main_checkBox_05.isChecked() else 0
        ct = 1 if self.sj_main_checkBox_06.isChecked() else 0
        smt = self.sj_main_lineEdit_01.text()
        smd = self.sj_main_lineEdit_02.text()
        cmt = self.sj_main_lineEdit_03.text()
        cmd = self.sj_main_lineEdit_04.text()
        sts = 1 if self.sj_main_checkBox_07.isChecked() else 0
        stt = 1 if self.sj_main_checkBox_08.isChecked() else 0
        std = self.sj_main_lineEdit_05.text()
        ctd = self.sj_main_lineEdit_06.text()
        if smt == '' or smd == '' or cmt == '' or cmd == '' or std == '' or ctd == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '일부 설정값이 입력되지 않았습니다.\n')
        else:
            smt, smd, cmt, cmd, std, ctd = int(smt), int(smd), int(cmt), int(cmd), int(std), int(ctd)
            data = [sg, sr, sc, st, cg, cr, cc, ct, smt, smd, cmt, cmd,
                    sts, stt, std, ctd]
            df = pd.DataFrame([data], columns=columns_sm, index=[0])
            query1Q.put([1, df, 'main', 'replace'])
            self.UpdateTexedit([ui_num['설정텍스트'], '시스템 기본 설정값 저장하기 완료'])

            self.dict_set['증권사'] = sg
            self.dict_set['주식리시버'] = sr
            self.dict_set['주식콜렉터'] = sc
            self.dict_set['주식트레이더'] = st
            self.dict_set['거래소'] = cg
            self.dict_set['코인리시버'] = cr
            self.dict_set['코인콜렉터'] = cc
            self.dict_set['코인트레이더'] = ct
            self.dict_set['주식순위시간'] = smt
            self.dict_set['주식순위선정'] = smd
            self.dict_set['코인순위시간'] = cmt
            self.dict_set['코인순위선정'] = cmd
            self.dict_set['주식실시간저장'] = sts
            self.dict_set['주식전체종목저장'] = stt
            self.dict_set['주식저장주기'] = std
            self.dict_set['코인저장주기'] = ctd
            sreceivQ.put(self.dict_set)
            creceiv1Q.put(self.dict_set)
            creceiv2Q.put(self.dict_set)
            tick1Q.put(self.dict_set)
            tick2Q.put(self.dict_set)
            tick3Q.put(self.dict_set)
            tick4Q.put(self.dict_set)
            tick5Q.put(self.dict_set)

    def ButtonClicked_78(self):
        id1 = self.sj_sacc_lineEdit_01.text()
        ps1 = self.sj_sacc_lineEdit_02.text()
        cp1 = self.sj_sacc_lineEdit_03.text()
        ap1 = self.sj_sacc_lineEdit_04.text()
        id2 = self.sj_sacc_lineEdit_05.text()
        ps2 = self.sj_sacc_lineEdit_06.text()
        cp2 = self.sj_sacc_lineEdit_07.text()
        ap2 = self.sj_sacc_lineEdit_08.text()
        if id1 == '' or ps1 == '' or cp1 == '' or ap1 == '' or id2 == '' or ps2 == '' or cp2 == '' or ap2 == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '일부 설정값이 입력되지 않았습니다.\n')
        else:
            df = pd.DataFrame([[id1, ps1, cp1, ap1, id2, ps2, cp2, ap2]], columns=columns_sk, index=[0])
            query1Q.put([1, df, 'sacc', 'replace'])
            self.UpdateTexedit([ui_num['설정텍스트'], '주식 계정 설정값 저장하기 완료'])

            self.dict_set['아이디1'] = id1
            self.dict_set['비밀번호1'] = ps1
            self.dict_set['인증서비밀번호1'] = cp1
            self.dict_set['계좌비밀번호1'] = ap1
            self.dict_set['아이디2'] = id2
            self.dict_set['비밀번호2'] = ps2
            self.dict_set['인증서비밀번호2'] = cp2
            self.dict_set['계좌비밀번호2'] = ap2

    def ButtonClicked_79(self):
        access_key = self.sj_cacc_lineEdit_01.text()
        secret_key = self.sj_cacc_lineEdit_02.text()
        if access_key == '' or secret_key == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '일부 설정값이 입력되지 않았습니다.\n')
        else:
            df = pd.DataFrame([[access_key, secret_key]], columns=columns_su, index=[0])
            query1Q.put([1, df, 'cacc', 'replace'])
            self.UpdateTexedit([ui_num['설정텍스트'], '업비트 계정 설정값 저장하기 완료'])

            self.dict_set['Access_key'] = access_key
            self.dict_set['Secret_key'] = secret_key

    def ButtonClicked_80(self):
        str_bot = self.sj_tele_lineEdit_01.text()
        int_id = self.sj_tele_lineEdit_02.text()
        if str_bot == '' or int_id == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '일부 설정값이 입력되지 않았습니다.\n')
        else:
            df = pd.DataFrame([[str_bot, int(int_id)]], columns=columns_st, index=[0])
            query1Q.put([1, df, 'telegram', 'replace'])
            self.UpdateTexedit([ui_num['설정텍스트'], '텔레그램 봇토큰 및 사용자 아이디 설정값 저장하기 완료'])

            self.dict_set['텔레그램봇토큰'] = str_bot
            self.dict_set['텔레그램사용자아이디'] = int(int_id)
            teleQ.put(self.dict_set)

    def ButtonClicked_81(self):
        me = 1 if self.sj_stock_checkBox_01.isChecked() else 0
        sd = 1 if self.sj_stock_checkBox_02.isChecked() else 0
        jcb = self.sj_stock_comboBox_01.currentText()
        jcs = self.sj_stock_comboBox_02.currentText()
        at1 = self.sj_stock_lineEdit_01.text()
        bc1 = self.sj_stock_lineEdit_02.text()
        jjb = self.sj_stock_comboBox_03.currentText()
        jjs = self.sj_stock_comboBox_04.currentText()
        at2 = self.sj_stock_lineEdit_03.text()
        bc2 = self.sj_stock_lineEdit_04.text()
        if jcb == '' or jcs == '' or at1 == '' or bc1 == '' or jjb == '' or jjs == '' or at2 == '' or bc2 == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '일부 변수값이 입력되지 않았습니다.\n')
        else:
            at1, bc1, at2, bc2 = int(at1), int(bc1), int(at2), int(bc2)
            query = f"UPDATE stock SET 주식모의투자 = {me}, 주식알림소리 = {sd}, 주식장초매수전략 = '{jcb}'," \
                    f"주식장초매도전략 = '{jcs}', 주식장초평균값계산틱수 = {at1}, 주식장초최대매수종목수 = {bc1}," \
                    f"주식장중매수전략 = '{jjb}', 주식장중매도전략 = '{jjs}', 주식장중평균값계산틱수 = {at2}," \
                    f"주식장중최대매수종목수 = {bc2}"
            query1Q.put([1, query])
            self.UpdateTexedit([ui_num['설정텍스트'], '주식 전략 설정값 저장하기 완료'])

            self.dict_set['주식모의투자'] = me
            self.dict_set['주식알림소리'] = sd
            self.dict_set['주식장초매수전략'] = jcb
            self.dict_set['주식장초매도전략'] = jcs
            self.dict_set['주식장초평균값계산틱수'] = at1
            self.dict_set['주식장초최대매수종목수'] = bc1
            self.dict_set['주식장중매수전략'] = jjb
            self.dict_set['주식장중매도전략'] = jjs
            self.dict_set['주식장중평균값계산틱수'] = at2
            self.dict_set['주식장중최대매수종목수'] = bc2
            sstgQ.put(self.dict_set)
            stockQ.put(self.dict_set)

    def ButtonClicked_82(self):
        me = 1 if self.sj_coin_checkBox_01.isChecked() else 0
        sd = 1 if self.sj_coin_checkBox_02.isChecked() else 0
        jcb = self.sj_coin_comboBox_01.currentText()
        jcs = self.sj_coin_comboBox_02.currentText()
        at1 = self.sj_coin_lineEdit_01.text()
        bc1 = self.sj_coin_lineEdit_02.text()
        jjb = self.sj_coin_comboBox_03.currentText()
        jjs = self.sj_coin_comboBox_04.currentText()
        at2 = self.sj_coin_lineEdit_03.text()
        bc2 = self.sj_coin_lineEdit_04.text()
        if jcb == '' or jcs == '' or at1 == '' or bc1 == '' or jjb == '' or jjs == '' or at2 == '' or bc2 == '':
            QtWidgets.QMessageBox.critical(self, '오류 알림', '일부 변수값이 입력되지 않았습니다.\n')
        else:
            at1, bc1, at2, bc2 = int(at1), int(bc1), int(at2), int(bc2)
            query = f"UPDATE coin SET 코인모의투자 = {me}, 코인알림소리 = {sd}, 코인장초매수전략 = '{jcb}'," \
                    f"코인장초매도전략 = '{jcs}', 코인장초평균값계산틱수 = {at1}, 코인장초최대매수종목수 = {bc1}," \
                    f"코인장중매수전략 = '{jjb}', 코인장중매도전략 = '{jjs}', 코인장중평균값계산틱수 = {at2}," \
                    f"코인장중최대매수종목수 = {bc2}"
            query1Q.put([1, query])
            self.UpdateTexedit([ui_num['설정텍스트'], '코인 전략 설정값 저장하기 완료'])

            self.dict_set['코인모의투자'] = me
            self.dict_set['코인알림소리'] = sd
            self.dict_set['코인장초매수전략'] = jcb
            self.dict_set['코인장초매도전략'] = jcs
            self.dict_set['코인장초평균값계산틱수'] = at1
            self.dict_set['코인장초최대매수종목수'] = bc1
            self.dict_set['코인장중매수전략'] = jjb
            self.dict_set['코인장중매도전략'] = jjs
            self.dict_set['코인장중평균값계산틱수'] = at2
            self.dict_set['코인장중최대매수종목수'] = bc2
            cstgQ.put(self.dict_set)
            coinQ.put(self.dict_set)

    def ButtonClicked_83(self):
        if self.sj_load_pushButton_00.text() == '계정 텍스트 보기':
            self.sj_sacc_lineEdit_01.setEchoMode(QtWidgets.QLineEdit.Normal)
            self.sj_sacc_lineEdit_02.setEchoMode(QtWidgets.QLineEdit.Normal)
            self.sj_sacc_lineEdit_03.setEchoMode(QtWidgets.QLineEdit.Normal)
            self.sj_sacc_lineEdit_04.setEchoMode(QtWidgets.QLineEdit.Normal)
            self.sj_sacc_lineEdit_05.setEchoMode(QtWidgets.QLineEdit.Normal)
            self.sj_sacc_lineEdit_06.setEchoMode(QtWidgets.QLineEdit.Normal)
            self.sj_sacc_lineEdit_07.setEchoMode(QtWidgets.QLineEdit.Normal)
            self.sj_sacc_lineEdit_08.setEchoMode(QtWidgets.QLineEdit.Normal)
            self.sj_cacc_lineEdit_01.setEchoMode(QtWidgets.QLineEdit.Normal)
            self.sj_cacc_lineEdit_02.setEchoMode(QtWidgets.QLineEdit.Normal)
            self.sj_tele_lineEdit_01.setEchoMode(QtWidgets.QLineEdit.Normal)
            self.sj_tele_lineEdit_02.setEchoMode(QtWidgets.QLineEdit.Normal)
            self.sj_load_pushButton_00.setText('계정 텍스트 가리기')
            self.sj_load_pushButton_00.setStyleSheet(style_bc_dk)
        else:
            self.sj_sacc_lineEdit_01.setEchoMode(QtWidgets.QLineEdit.Password)
            self.sj_sacc_lineEdit_02.setEchoMode(QtWidgets.QLineEdit.Password)
            self.sj_sacc_lineEdit_03.setEchoMode(QtWidgets.QLineEdit.Password)
            self.sj_sacc_lineEdit_04.setEchoMode(QtWidgets.QLineEdit.Password)
            self.sj_sacc_lineEdit_05.setEchoMode(QtWidgets.QLineEdit.Password)
            self.sj_sacc_lineEdit_06.setEchoMode(QtWidgets.QLineEdit.Password)
            self.sj_sacc_lineEdit_07.setEchoMode(QtWidgets.QLineEdit.Password)
            self.sj_sacc_lineEdit_08.setEchoMode(QtWidgets.QLineEdit.Password)
            self.sj_cacc_lineEdit_01.setEchoMode(QtWidgets.QLineEdit.Password)
            self.sj_cacc_lineEdit_02.setEchoMode(QtWidgets.QLineEdit.Password)
            self.sj_tele_lineEdit_01.setEchoMode(QtWidgets.QLineEdit.Password)
            self.sj_tele_lineEdit_02.setEchoMode(QtWidgets.QLineEdit.Password)
            self.sj_load_pushButton_00.setText('계정 텍스트 보기')
            self.sj_load_pushButton_00.setStyleSheet(style_bc_bt)

    def ButtonClicked_90(self):
        self.ss_textEdit_01.setVisible(True)
        self.ss_textEdit_02.setVisible(True)
        self.ss_textEdit_03.setVisible(False)
        self.sb_pushButton_03.setStyleSheet(style_bc_dk)
        self.sb_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_91(self):
        self.ss_textEdit_01.setVisible(False)
        self.ss_textEdit_02.setVisible(False)
        self.ss_textEdit_03.setVisible(True)
        self.sb_pushButton_03.setStyleSheet(style_bc_st)
        self.sb_pushButton_04.setStyleSheet(style_bc_dk)

    def ButtonClicked_92(self):
        self.cs_textEdit_01.setVisible(True)
        self.cs_textEdit_02.setVisible(True)
        self.cs_textEdit_03.setVisible(False)
        self.cb_pushButton_03.setStyleSheet(style_bc_dk)
        self.cb_pushButton_04.setStyleSheet(style_bc_st)

    def ButtonClicked_93(self):
        self.cs_textEdit_01.setVisible(False)
        self.cs_textEdit_02.setVisible(False)
        self.cs_textEdit_03.setVisible(True)
        self.cb_pushButton_03.setStyleSheet(style_bc_st)
        self.cb_pushButton_04.setStyleSheet(style_bc_dk)

    def CalendarClicked(self, gubun):
        if gubun == 'S':
            table = 's_tradelist'
            searchday = self.s_calendarWidget.selectedDate().toString('yyyyMMdd')
        else:
            table = 'c_tradelist'
            searchday = self.c_calendarWidget.selectedDate().toString('yyyyMMdd')
        con = sqlite3.connect(DB_TRADELIST)
        df = pd.read_sql(f"SELECT * FROM {table} WHERE 체결시간 LIKE '{searchday}%'", con).set_index('index')
        con.close()
        if len(df) > 0:
            df.sort_values(by=['체결시간'], ascending=True, inplace=True)
            df = df[['체결시간', '종목명', '매수금액', '매도금액', '주문수량', '수익률', '수익금']].copy()
            nbg, nsg = df['매수금액'].sum(), df['매도금액'].sum()
            sp = round((nsg / nbg - 1) * 100, 2)
            npg, nmg, nsig = df[df['수익금'] > 0]['수익금'].sum(), df[df['수익금'] < 0]['수익금'].sum(), df['수익금'].sum()
            df2 = pd.DataFrame(columns=columns_dt)
            df2.at[0] = searchday, nbg, nsg, npg, nmg, sp, nsig
        else:
            df = pd.DataFrame(columns=columns_dt)
            df2 = pd.DataFrame(columns=columns_dd)
        self.UpdateTablewidget([ui_num[f'{gubun}당일합계'], df2])
        self.UpdateTablewidget([ui_num[f'{gubun}당일상세'], df])

    def eventFilter(self, widget, event):
        if event.type() == QtCore.QEvent.KeyPress and event.key() == Qt.Key_Tab:
            if widget == self.ss_textEdit_01:
                self.ss_textEdit_01.insertPlainText('    ')
            elif widget == self.ss_textEdit_02:
                self.ss_textEdit_02.insertPlainText('    ')
            elif widget == self.cs_textEdit_01:
                self.cs_textEdit_01.insertPlainText('    ')
            elif widget == self.cs_textEdit_02:
                self.cs_textEdit_02.insertPlainText('    ')
            return True
        else:
            return QtWidgets.QMainWindow.eventFilter(self, widget, event)

    # noinspection PyArgumentList
    def closeEvent(self, a):
        buttonReply = QtWidgets.QMessageBox.question(
            self, "프로그램 종료", "프로그램을 종료합니다.",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No, QtWidgets.QMessageBox.No
        )
        if buttonReply == QtWidgets.QMessageBox.Yes:
            creceiv1Q.put('terminate')
            creceiv2Q.put('terminate')
            QTest.qWait(2000)
            sound_proc.kill()
            query_proc1.kill()
            query_proc2.kill()
            chart_proc.kill()
            hoga_proc.kill()
            tele_proc.kill()
            if self.dialog_chart.isVisible():
                self.dialog_chart.close()
            if self.dialog_hoga.isVisible():
                self.dialog_hoga.close()
            if self.qtimer1.isActive():
                self.qtimer1.stop()
            if self.qtimer2.isActive():
                self.qtimer2.stop()
            if self.qtimer3.isActive():
                self.qtimer3.stop()
            if self.writer.isRunning():
                self.writer.terminate()
            if self.trader_coin_proc.is_alive():
                self.trader_coin_proc.kill()
            if self.trader_kiwoom_proc.is_alive():
                self.trader_kiwoom_proc.kill()
            if self.trader_xing_proc.is_alive():
                self.trader_xing_proc.kill()
            if self.strategy_coin_proc.is_alive():
                self.strategy_coin_proc.kill()
            if self.strategy_stock_proc.is_alive():
                self.strategy_stock_proc.kill()
            if self.collector_coin_proc.is_alive():
                self.collector_coin_proc.kill()
            if self.collector_stock_proc1.is_alive():
                self.collector_stock_proc1.kill()
            if self.collector_stock_proc2.is_alive():
                self.collector_stock_proc2.kill()
            if self.collector_stock_proc3.is_alive():
                self.collector_stock_proc3.kill()
            if self.collector_stock_proc4.is_alive():
                self.collector_stock_proc4.kill()
            if self.receiver_kiwoom_proc.is_alive():
                self.receiver_kiwoom_proc.kill()
            if self.receiver_xing_proc.is_alive():
                self.receiver_xing_proc.kill()
            if self.receiver_coin_proc1.is_alive():
                self.receiver_coin_proc1.kill()
            if self.receiver_coin_proc2.is_alive():
                self.receiver_coin_proc2.kill()
            a.accept()
        else:
            a.ignore()


class Writer(QtCore.QThread):
    data1 = QtCore.pyqtSignal(list)
    data2 = QtCore.pyqtSignal(list)
    data3 = QtCore.pyqtSignal(list)
    data4 = QtCore.pyqtSignal(list)
    data5 = QtCore.pyqtSignal(list)

    def __init__(self):
        super().__init__()

    def run(self):
        while True:
            data = windowQ.get()
            if data[0] <= 10:
                self.data1.emit(data)
            elif data[0] < 20 or 42 <= data[0] <= 47:
                self.data2.emit(data)
            elif data[0] == 20:
                self.data3.emit(data)
            elif data[0] < 30:
                self.data2.emit(data)
            elif data[0] == 30:
                self.data3.emit(data)
            elif data[0] == 40:
                self.data4.emit(data)
            elif data[0] == 41:
                self.data5.emit(data)


if __name__ == '__main__':
    freeze_support()
    windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceiv1Q, creceiv2Q, stockQ, coinQ, sstgQ, cstgQ, tick1Q, \
        tick2Q, tick3Q, tick4Q, tick5Q, chartQ, backQ, hogaQ = Queue(), Queue(), Queue(), Queue(), Queue(), Queue(), \
        Queue(), Queue(), Queue(), Queue(), Queue(), Queue(), Queue(), Queue(), Queue(), Queue(), Queue(), Queue(), \
        Queue(), Queue()
    qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceiv1Q, creceiv2Q, stockQ, coinQ,
             sstgQ, cstgQ, tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, chartQ, hogaQ]

    sound_proc = Process(target=Sound, args=(qlist,), daemon=True)
    query_proc1 = Process(target=Query, args=(qlist,), daemon=True)
    query_proc2 = Process(target=QueryTick, args=(qlist,), daemon=True)
    chart_proc = Process(target=Chart, args=(qlist,), daemon=True)
    hoga_proc = Process(target=Hoga, args=(qlist,), daemon=True)
    tele_proc = Process(target=TelegramMsg, args=(qlist,), daemon=True)
    sound_proc.start()
    query_proc1.start()
    query_proc2.start()
    chart_proc.start()
    hoga_proc.start()
    tele_proc.start()

    app = QtWidgets.QApplication(sys.argv)
    app.setStyle(ProxyStyle())
    app.setStyle('fusion')
    palette = QtGui.QPalette()
    palette.setColor(QtGui.QPalette.Window, color_bg_bc)
    palette.setColor(QtGui.QPalette.Background, color_bg_bc)
    palette.setColor(QtGui.QPalette.WindowText, color_fg_bc)
    palette.setColor(QtGui.QPalette.Base, color_bg_bc)
    palette.setColor(QtGui.QPalette.AlternateBase, color_bg_dk)
    palette.setColor(QtGui.QPalette.Text, color_fg_bc)
    palette.setColor(QtGui.QPalette.Button, color_bg_bc)
    palette.setColor(QtGui.QPalette.ButtonText, color_fg_bc)
    palette.setColor(QtGui.QPalette.Link, color_fg_bk)
    palette.setColor(QtGui.QPalette.Highlight, color_fg_hl)
    palette.setColor(QtGui.QPalette.HighlightedText, color_bg_bk)
    app.setPalette(palette)
    window = Window()
    window.show()
    app.exec_()
