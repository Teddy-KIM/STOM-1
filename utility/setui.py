import pyqtgraph
from PyQt5.QtCore import Qt
from PyQt5 import QtCore, QtGui, QtWidgets
from utility import syntax
from utility.static import CustomViewBox
from utility.setting import qfont12, qfont14, style_bc_st, style_bc_bt, style_bc_dk, style_fc_bt, style_pgbar, \
    columns_tt, columns_td, columns_tj, columns_jg, columns_gj_, columns_cj, columns_dt, columns_dd, columns_nt, \
    columns_nd, ICON_PATH, style_bc_by, style_bc_sl, columns_hj, columns_hc, columns_hg, style_fc_dk


class TabBar(QtWidgets.QTabBar):
    def tabSizeHint(self, index):
        s = QtWidgets.QTabBar.tabSizeHint(self, index)
        s.setWidth(40)
        s.setHeight(40)
        s.transpose()
        return s

    def paintEvent(self, event):
        painter = QtWidgets.QStylePainter(self)
        opt = QtWidgets.QStyleOptionTab()

        for i in range(self.count()):
            self.initStyleOption(opt, i)
            painter.drawControl(QtWidgets.QStyle.CE_TabBarTabShape, opt)
            painter.save()

            s = opt.rect.size()
            s.transpose()
            r = QtCore.QRect(QtCore.QPoint(), s)
            r.moveCenter(opt.rect.center())
            opt.rect = r

            c = self.tabRect(i).center()
            painter.translate(c)
            painter.rotate(90)
            painter.translate(-c)
            painter.drawControl(QtWidgets.QStyle.CE_TabBarTabLabel, opt)
            painter.restore()


class TabWidget(QtWidgets.QTabWidget):
    def __init__(self, *args, **kwargs):
        QtWidgets.QTabWidget.__init__(self, *args, **kwargs)
        self.setTabBar(TabBar(self))
        self.setTabPosition(QtWidgets.QTabWidget.West)


class ProxyStyle(QtWidgets.QProxyStyle):
    def drawControl(self, element, opt, painter, widget=None):
        if element == QtWidgets.QStyle.CE_TabBarTabLabel:
            ic = self.pixelMetric(QtWidgets.QStyle.PM_TabBarIconSize)
            r = QtCore.QRect(opt.rect)
            w = 0 if opt.icon.isNull() else opt.rect.width() + ic
            r.setHeight(opt.fontMetrics.width(opt.text) + w)
            r.moveBottom(opt.rect.bottom())
            opt.rect = r
        QtWidgets.QProxyStyle.drawControl(self, element, opt, painter, widget)


def SetUI(self):
    def setPushbutton(name, box=None, click=None, cmd=None, icon=None, tip=None, color=0):
        if box is not None:
            pushbutton = QtWidgets.QPushButton(name, box)
        else:
            pushbutton = QtWidgets.QPushButton(name, self)
        if color == 0:
            pushbutton.setStyleSheet(style_bc_bt)
        elif color == 1:
            pushbutton.setStyleSheet(style_bc_st)
        elif color == 2:
            pushbutton.setStyleSheet(style_bc_by)
        elif color == 3:
            pushbutton.setStyleSheet(style_bc_sl)
        pushbutton.setFont(qfont12)
        if click is not None:
            if cmd is not None:
                pushbutton.clicked.connect(lambda: click(cmd))
            else:
                pushbutton.clicked.connect(click)
        if icon is not None:
            pushbutton.setIcon(icon)
        if tip is not None:
            pushbutton.setToolTip(tip)
        return pushbutton

    def setLine(tab, width):
        line = QtWidgets.QFrame(tab)
        line.setLineWidth(width)
        line.setStyleSheet(style_fc_dk)
        line.setFrameShape(QtWidgets.QFrame.HLine)
        return line

    def setTextEdit(tab):
        textedit = QtWidgets.QTextEdit(tab)
        textedit.setReadOnly(True)
        textedit.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        textedit.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        textedit.setStyleSheet(style_bc_dk)
        return textedit

    def setTextEdit2(tab):
        textedit = QtWidgets.QTextEdit(tab)
        textedit.setFont(qfont14)
        textedit.setStyleSheet(style_bc_dk)
        textedit.installEventFilter(self)
        syntax.PythonHighlighter(textedit)
        return textedit

    def setCombobox(tab, Activated=None):
        combobox = QtWidgets.QComboBox(tab)
        if Activated is not None:
            combobox.setFont(qfont14)
            combobox.currentTextChanged.connect(Activated)
        return combobox

    def setCheckBos(name, groupbox, changed=None):
        checkbox = QtWidgets.QCheckBox(name, groupbox)
        if changed is not None:
            checkbox.stateChanged.connect(changed)
        return checkbox

    def setLineedit(groupbox, enter=None, passhide=False):
        lineedit = QtWidgets.QLineEdit(groupbox)
        lineedit.setAlignment(Qt.AlignRight)
        lineedit.setStyleSheet(style_fc_bt)
        lineedit.setFont(qfont12)
        if enter:
            lineedit.returnPressed.connect(enter)
        if passhide:
            lineedit.setEchoMode(QtWidgets.QLineEdit.Password)
        return lineedit

    def setLineedit2(tab):
        lineedit = QtWidgets.QLineEdit(tab)
        lineedit.setStyleSheet(style_fc_bt)
        lineedit.setFont(qfont14)
        return lineedit

    def setTablewidget(tab, columns, rowcount, sectionsize=None, clicked=None):
        tableWidget = QtWidgets.QTableWidget(tab)
        if sectionsize is not None:
            tableWidget.verticalHeader().setDefaultSectionSize(sectionsize)
        else:
            tableWidget.verticalHeader().setDefaultSectionSize(23)
        tableWidget.verticalHeader().setVisible(False)
        tableWidget.setAlternatingRowColors(True)
        tableWidget.setSelectionMode(QtWidgets.QAbstractItemView.NoSelection)
        tableWidget.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        tableWidget.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        tableWidget.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        tableWidget.setColumnCount(len(columns))
        tableWidget.setRowCount(rowcount)
        tableWidget.setHorizontalHeaderLabels(columns)
        if columns[-1] == 'ch_high':
            if tab == self.st_tab:
                tableWidget.setColumnWidth(0, 122)
                tableWidget.setColumnWidth(1, 68)
                tableWidget.setColumnWidth(2, 68)
                tableWidget.setColumnWidth(3, 68)
                tableWidget.setColumnWidth(4, 68)
                tableWidget.setColumnWidth(5, 68)
                tableWidget.setColumnWidth(6, 68)
                tableWidget.setColumnWidth(7, 68)
                tableWidget.setColumnWidth(8, 68)
            else:
                tableWidget.setColumnWidth(0, 85)
                tableWidget.setColumnWidth(1, 55)
                tableWidget.setColumnWidth(2, 55)
                tableWidget.setColumnWidth(3, 90)
                tableWidget.setColumnWidth(4, 90)
                tableWidget.setColumnWidth(5, 126)
                tableWidget.setColumnWidth(6, 55)
                tableWidget.setColumnWidth(7, 55)
                tableWidget.setColumnWidth(8, 55)
        elif columns[0] in ['기간', '일자']:
            tableWidget.setColumnWidth(0, 100)
            tableWidget.setColumnWidth(1, 100)
            tableWidget.setColumnWidth(2, 100)
            tableWidget.setColumnWidth(3, 100)
            tableWidget.setColumnWidth(4, 100)
            tableWidget.setColumnWidth(5, 66)
            tableWidget.setColumnWidth(6, 100)
        elif tab == self.ct_tab and columns[1] == '매수금액':
            tableWidget.setColumnWidth(0, 96)
            tableWidget.setColumnWidth(1, 90)
            tableWidget.setColumnWidth(2, 90)
            tableWidget.setColumnWidth(3, 140)
            tableWidget.setColumnWidth(4, 70)
            tableWidget.setColumnWidth(5, 90)
            tableWidget.setColumnWidth(6, 90)
        elif tab == self.ct_tab and columns[1] == '매입가':
            tableWidget.setColumnWidth(0, 96)
            tableWidget.setColumnWidth(1, 105)
            tableWidget.setColumnWidth(2, 105)
            tableWidget.setColumnWidth(3, 90)
            tableWidget.setColumnWidth(4, 90)
            tableWidget.setColumnWidth(5, 90)
            tableWidget.setColumnWidth(6, 90)
            tableWidget.setColumnWidth(7, 140)
        elif tab == self.ct_tab and columns[1] == '주문구분':
            tableWidget.setColumnWidth(0, 96)
            tableWidget.setColumnWidth(1, 70)
            tableWidget.setColumnWidth(2, 140)
            tableWidget.setColumnWidth(3, 60)
            tableWidget.setColumnWidth(4, 105)
            tableWidget.setColumnWidth(5, 105)
            tableWidget.setColumnWidth(6, 90)
        elif columns == columns_hj:
            tableWidget.setColumnWidth(0, 140)
            tableWidget.setColumnWidth(1, 140)
            tableWidget.setColumnWidth(2, 140)
            tableWidget.setColumnWidth(3, 140)
            tableWidget.setColumnWidth(4, 140)
            tableWidget.setColumnWidth(5, 140)
            tableWidget.setColumnWidth(6, 140)
        elif columns == columns_hc or columns == columns_hg:
            tableWidget.setColumnWidth(0, 140)
            tableWidget.setColumnWidth(1, 140)
        else:
            tableWidget.setColumnWidth(0, 126)
            tableWidget.setColumnWidth(1, 90)
            tableWidget.setColumnWidth(2, 90)
            tableWidget.setColumnWidth(3, 90)
            tableWidget.setColumnWidth(4, 90)
            tableWidget.setColumnWidth(5, 90)
            tableWidget.setColumnWidth(6, 90)
        if clicked is not None:
            tableWidget.cellClicked.connect(clicked)
        return tableWidget

    icon_main = QtGui.QIcon(f'{ICON_PATH}/python.png')
    icon_stock = QtGui.QIcon(f'{ICON_PATH}/stock.png')
    icon_coin = QtGui.QIcon(f'{ICON_PATH}/coin.png')
    icon_set = QtGui.QIcon(f'{ICON_PATH}/set.png')
    icon_log = QtGui.QIcon(f'{ICON_PATH}/log.png')
    icon_total = QtGui.QIcon(f'{ICON_PATH}/total.png')
    icon_start = QtGui.QIcon(f'{ICON_PATH}/start.png')
    icon_zoom = QtGui.QIcon(f'{ICON_PATH}/zoom.png')
    icon_dbdel = QtGui.QIcon(f'{ICON_PATH}/dbdel.png')
    icon_accdel = QtGui.QIcon(f'{ICON_PATH}/accdel.png')
    icon_stocks = QtGui.QIcon(f'{ICON_PATH}/stocks.png')
    icon_coins = QtGui.QIcon(f'{ICON_PATH}/coins.png')

    self.icon_open = QtGui.QIcon(f'{ICON_PATH}/open.bmp')
    self.icon_high = QtGui.QIcon(f'{ICON_PATH}/high.bmp')
    self.icon_low = QtGui.QIcon(f'{ICON_PATH}/low.bmp')
    self.icon_up = QtGui.QIcon(f'{ICON_PATH}/up.bmp')
    self.icon_down = QtGui.QIcon(f'{ICON_PATH}/down.bmp')
    self.icon_vi = QtGui.QIcon(f'{ICON_PATH}/vi.bmp')
    self.icon_totals = QtGui.QIcon(f'{ICON_PATH}/totals.bmp')
    self.icon_totalb = QtGui.QIcon(f'{ICON_PATH}/totalb.bmp')

    self.setFont(qfont12)
    self.setWindowTitle('STOM')
    self.setWindowIcon(icon_main)

    self.main_tabWidget = TabWidget(self)
    self.st_tab = QtWidgets.QWidget()
    self.ct_tab = QtWidgets.QWidget()
    self.ss_tab = QtWidgets.QWidget()
    self.cs_tab = QtWidgets.QWidget()
    self.lg_tab = QtWidgets.QWidget()
    self.sj_tab = QtWidgets.QWidget()

    self.main_tabWidget.addTab(self.st_tab, '')
    self.main_tabWidget.addTab(self.ct_tab, '')
    self.main_tabWidget.addTab(self.ss_tab, '')
    self.main_tabWidget.addTab(self.cs_tab, '')
    self.main_tabWidget.addTab(self.lg_tab, '')
    self.main_tabWidget.addTab(self.sj_tab, '')
    self.main_tabWidget.setTabIcon(0, icon_stock)
    self.main_tabWidget.setTabIcon(1, icon_coin)
    self.main_tabWidget.setTabIcon(2, icon_stocks)
    self.main_tabWidget.setTabIcon(3, icon_coins)
    self.main_tabWidget.setTabIcon(4, icon_log)
    self.main_tabWidget.setTabIcon(5, icon_set)
    self.main_tabWidget.setTabToolTip(0, '  주식 트레이더')
    self.main_tabWidget.setTabToolTip(1, '  코인 트레이더')
    self.main_tabWidget.setTabToolTip(2, '  주식 전략 설정')
    self.main_tabWidget.setTabToolTip(3, '  코인 전략 설정')
    self.main_tabWidget.setTabToolTip(4, '  로그')
    self.main_tabWidget.setTabToolTip(5, '  설정')

    self.tt_pushButton = setPushbutton('', click=self.ButtonClicked_01, icon=icon_total, tip='  수익집계')
    self.ms_pushButton = setPushbutton('', click=self.ButtonClicked_02, icon=icon_start, tip='  주식수동시작')
    self.zo_pushButton = setPushbutton('', click=self.ButtonClicked_03, icon=icon_zoom, tip='  축소확대')
    self.dd_pushButton = setPushbutton('', click=self.ButtonClicked_04, icon=icon_dbdel, tip='  거래목록 데이터 삭제 및 초기화')
    self.sd_pushButton = setPushbutton('', click=self.ButtonClicked_05, icon=icon_accdel, tip='  모든 계정 설정 삭제 및 초기화')
    self.qs_pushButton = setPushbutton('', click=self.ShowQsize)
    self.ct_pushButton = setPushbutton('', click=self.ShowDialogChart)
    self.hg_pushButton = setPushbutton('', click=self.ShowDialogHoga)
    self.tt_pushButton.setShortcut('Alt+T')
    self.ms_pushButton.setShortcut('Alt+S')
    self.zo_pushButton.setShortcut('Alt+Z')
    self.dd_pushButton.setShortcut('Alt+X')
    self.sd_pushButton.setShortcut('Alt+A')
    self.qs_pushButton.setShortcut('Alt+Q')
    self.ct_pushButton.setShortcut('Alt+C')
    self.hg_pushButton.setShortcut('Alt+H')

    self.progressBar = QtWidgets.QProgressBar(self)
    self.progressBar.setAlignment(Qt.AlignCenter)
    self.progressBar.setOrientation(Qt.Vertical)
    self.progressBar.setRange(0, 100)
    self.progressBar.setStyleSheet(style_pgbar)

    self.stt_tableWidget = setTablewidget(self.st_tab, columns_tt, 1)
    self.std_tableWidget = setTablewidget(self.st_tab, columns_td, 13, clicked=self.CellClicked_01)
    self.stj_tableWidget = setTablewidget(self.st_tab, columns_tj, 1)
    self.sjg_tableWidget = setTablewidget(self.st_tab, columns_jg, 13, clicked=self.CellClicked_02)
    self.sgj_tableWidget = setTablewidget(self.st_tab, columns_gj_, 15, clicked=self.CellClicked_01)
    self.scj_tableWidget = setTablewidget(self.st_tab, columns_cj, 15, clicked=self.CellClicked_01)

    self.s_calendarWidget = QtWidgets.QCalendarWidget(self.st_tab)
    todayDate = QtCore.QDate.currentDate()
    self.s_calendarWidget.setCurrentPage(todayDate.year(), todayDate.month())
    self.s_calendarWidget.clicked.connect(lambda: self.CalendarClicked('S'))
    self.sdt_tableWidget = setTablewidget(self.st_tab, columns_dt, 1)
    self.sds_tableWidget = setTablewidget(self.st_tab, columns_dd, 19, clicked=self.CellClicked_04)

    self.snt_pushButton_01 = setPushbutton('일별집계', box=self.st_tab, click=self.ButtonClicked_06, cmd='S일별집계')
    self.snt_pushButton_02 = setPushbutton('월별집계', box=self.st_tab, click=self.ButtonClicked_06, cmd='S월별집계')
    self.snt_pushButton_03 = setPushbutton('연도별집계', box=self.st_tab, click=self.ButtonClicked_06, cmd='S연도별집계')
    self.snt_tableWidget = setTablewidget(self.st_tab, columns_nt, 1)
    self.sns_tableWidget = setTablewidget(self.st_tab, columns_nd, 28)

    self.s_calendarWidget.setVisible(False)
    self.sdt_tableWidget.setVisible(False)
    self.sds_tableWidget.setVisible(False)
    self.snt_pushButton_01.setVisible(False)
    self.snt_pushButton_02.setVisible(False)
    self.snt_pushButton_03.setVisible(False)
    self.snt_tableWidget.setVisible(False)
    self.sns_tableWidget.setVisible(False)

    self.ctt_tableWidget = setTablewidget(self.ct_tab, columns_tt, 1)
    self.ctd_tableWidget = setTablewidget(self.ct_tab, columns_td, 13, clicked=self.CellClicked_01)
    self.ctj_tableWidget = setTablewidget(self.ct_tab, columns_tj, 1)
    self.cjg_tableWidget = setTablewidget(self.ct_tab, columns_jg, 13, clicked=self.CellClicked_03)
    self.cgj_tableWidget = setTablewidget(self.ct_tab, columns_gj_, 15, clicked=self.CellClicked_01)
    self.ccj_tableWidget = setTablewidget(self.ct_tab, columns_cj, 15, clicked=self.CellClicked_01)

    self.c_calendarWidget = QtWidgets.QCalendarWidget(self.ct_tab)
    self.c_calendarWidget.setCurrentPage(todayDate.year(), todayDate.month())
    self.c_calendarWidget.clicked.connect(lambda: self.CalendarClicked('C'))
    self.cdt_tableWidget = setTablewidget(self.ct_tab, columns_dt, 1)
    self.cds_tableWidget = setTablewidget(self.ct_tab, columns_dd, 19, clicked=self.CellClicked_04)

    self.cnt_pushButton_01 = setPushbutton('일별집계', box=self.ct_tab, click=self.ButtonClicked_06, cmd='C일별집계')
    self.cnt_pushButton_02 = setPushbutton('월별집계', box=self.ct_tab, click=self.ButtonClicked_06, cmd='C월별집계')
    self.cnt_pushButton_03 = setPushbutton('연도별집계', box=self.ct_tab, click=self.ButtonClicked_06, cmd='C연도별집계')
    self.cnt_tableWidget = setTablewidget(self.ct_tab, columns_nt, 1)
    self.cns_tableWidget = setTablewidget(self.ct_tab, columns_nd, 28)

    self.c_calendarWidget.setVisible(False)
    self.cdt_tableWidget.setVisible(False)
    self.cds_tableWidget.setVisible(False)
    self.cnt_pushButton_01.setVisible(False)
    self.cnt_pushButton_02.setVisible(False)
    self.cnt_pushButton_03.setVisible(False)
    self.cnt_tableWidget.setVisible(False)
    self.cns_tableWidget.setVisible(False)

    self.ss_textEdit_01 = setTextEdit2(self.ss_tab)
    self.ss_textEdit_02 = setTextEdit2(self.ss_tab)
    self.ss_textEdit_03 = setTextEdit(self.ss_tab)
    self.ss_textEdit_03.setVisible(False)

    self.ssb_comboBox = setCombobox(self.ss_tab, self.Activated_01)
    self.ssb_lineEdit = setLineedit2(self.ss_tab)
    self.ssb_pushButton_01 = setPushbutton('매수전략 로딩', box=self.ss_tab, click=self.ButtonClicked_07, color=1)
    self.ssb_pushButton_02 = setPushbutton('매수전략 저장', box=self.ss_tab, click=self.ButtonClicked_08, color=1)
    self.ssb_pushButton_03 = setPushbutton('매수변수 로딩', box=self.ss_tab, click=self.ButtonClicked_09, color=1)
    self.ssb_pushButton_04 = setPushbutton('매수전략 시작', box=self.ss_tab, click=self.ButtonClicked_10, color=1)
    self.ssb_pushButton_05 = setPushbutton('VI해제시간비교', box=self.ss_tab, click=self.ButtonClicked_11)
    self.ssb_pushButton_06 = setPushbutton('VI아래5호가비교', box=self.ss_tab, click=self.ButtonClicked_12)
    self.ssb_pushButton_07 = setPushbutton('등락율제한', box=self.ss_tab, click=self.ButtonClicked_13)
    self.ssb_pushButton_08 = setPushbutton('고저평균대비등락율', box=self.ss_tab, click=self.ButtonClicked_14)
    self.ssb_pushButton_09 = setPushbutton('체결강도하한', box=self.ss_tab, click=self.ButtonClicked_15)
    self.ssb_pushButton_10 = setPushbutton('체결강도차이', box=self.ss_tab, click=self.ButtonClicked_16)
    self.ssb_pushButton_11 = setPushbutton('당일거래대금하한', box=self.ss_tab, click=self.ButtonClicked_17)
    self.ssb_pushButton_12 = setPushbutton('초당거래대금차이', box=self.ss_tab, click=self.ButtonClicked_18)
    self.ssb_pushButton_13 = setPushbutton('호가총잔량비교', box=self.ss_tab, click=self.ButtonClicked_19)
    self.ssb_pushButton_14 = setPushbutton('1호가잔량비교', box=self.ss_tab, click=self.ButtonClicked_20)
    self.ssb_pushButton_15 = setPushbutton('매수시그널', box=self.ss_tab, click=self.ButtonClicked_21, color=2)
    self.ssb_pushButton_16 = setPushbutton('매수전략 중지', box=self.ss_tab, click=self.ButtonClicked_22, color=1)

    text = '백테스트 기간설정                                         ~'
    self.ssb_labellll_01 = QtWidgets.QLabel(text, self.ss_tab)
    text = '백테스트 시간설정        시작시간                       종료시간'
    self.ssb_labellll_02 = QtWidgets.QLabel(text, self.ss_tab)
    text = '배팅(백만)                     평균틱수                          멀티수'
    self.ssb_labellll_03 = QtWidgets.QLabel(text, self.ss_tab)

    self.ssb_dateEdit_01 = QtWidgets.QDateEdit(self.ss_tab)
    self.ssb_dateEdit_01.setDate(QtCore.QDate.currentDate().addDays(-14))
    self.ssb_dateEdit_01.setCalendarPopup(True)
    self.ssb_dateEdit_02 = QtWidgets.QDateEdit(self.ss_tab)
    self.ssb_dateEdit_02.setDate(QtCore.QDate.currentDate())
    self.ssb_dateEdit_02.setCalendarPopup(True)
    self.ssb_lineEdit_01 = setLineedit(self.ss_tab)
    self.ssb_lineEdit_01.setText('90000')
    self.ssb_lineEdit_02 = setLineedit(self.ss_tab)
    self.ssb_lineEdit_02.setText('100000')
    self.ssb_lineEdit_03 = setLineedit(self.ss_tab)
    self.ssb_lineEdit_03.setText('10')
    self.ssb_lineEdit_04 = setLineedit(self.ss_tab)
    self.ssb_lineEdit_04.setText('60')
    self.ssb_lineEdit_05 = setLineedit(self.ss_tab)
    self.ssb_lineEdit_05.setText('6')
    self.sb_pushButton_01 = setPushbutton('백테스팅', box=self.ss_tab, click=self.ButtonClicked_23, color=1)
    self.sb_pushButton_02 = setPushbutton('최적화', box=self.ss_tab, click=self.ButtonClicked_24, color=1)
    self.sb_pushButton_03 = setPushbutton('전략편집기', box=self.ss_tab, click=self.ButtonClicked_90, color=1)
    self.sb_pushButton_04 = setPushbutton('백테스트 로그', box=self.ss_tab, click=self.ButtonClicked_91, color=1)
    self.sb_pushButton_03.setStyleSheet(style_bc_dk)

    self.sss_comboBox = setCombobox(self.ss_tab, self.Activated_02)
    self.sss_lineEdit = setLineedit2(self.ss_tab)
    self.sss_pushButton_01 = setPushbutton('매도전략 로딩', box=self.ss_tab, click=self.ButtonClicked_25, color=1)
    self.sss_pushButton_02 = setPushbutton('매도전략 저장', box=self.ss_tab, click=self.ButtonClicked_26, color=1)
    self.sss_pushButton_03 = setPushbutton('매도변수 로딩', box=self.ss_tab, click=self.ButtonClicked_27, color=1)
    self.sss_pushButton_04 = setPushbutton('매도전략 시작', box=self.ss_tab, click=self.ButtonClicked_28, color=1)
    self.sss_pushButton_05 = setPushbutton('손절라인청산', box=self.ss_tab, click=self.ButtonClicked_29)
    self.sss_pushButton_06 = setPushbutton('익절라인청산', box=self.ss_tab, click=self.ButtonClicked_30)
    self.sss_pushButton_07 = setPushbutton('수익율보존청산', box=self.ss_tab, click=self.ButtonClicked_31)
    self.sss_pushButton_08 = setPushbutton('보유시간기준청산', box=self.ss_tab, click=self.ButtonClicked_32)
    self.sss_pushButton_09 = setPushbutton('VI직전매도', box=self.ss_tab, click=self.ButtonClicked_33)
    self.sss_pushButton_10 = setPushbutton('고저평균등락율', box=self.ss_tab, click=self.ButtonClicked_34)
    self.sss_pushButton_11 = setPushbutton('최고체결강도비교', box=self.ss_tab, click=self.ButtonClicked_35)
    self.sss_pushButton_12 = setPushbutton('호가총잔량비교', box=self.ss_tab, click=self.ButtonClicked_36)
    self.sss_pushButton_13 = setPushbutton('매도시그널', box=self.ss_tab, click=self.ButtonClicked_37, color=3)
    self.sss_pushButton_14 = setPushbutton('매도전략 중지', box=self.ss_tab, click=self.ButtonClicked_38, color=1)

    self.cs_textEdit_01 = setTextEdit2(self.cs_tab)
    self.cs_textEdit_02 = setTextEdit2(self.cs_tab)
    self.cs_textEdit_03 = setTextEdit(self.cs_tab)
    self.cs_textEdit_03.setVisible(False)

    self.csb_comboBox = setCombobox(self.cs_tab, self.Activated_03)
    self.csb_lineEdit = setLineedit2(self.cs_tab)
    self.csb_pushButton_01 = setPushbutton('매수전략 로딩', box=self.cs_tab, click=self.ButtonClicked_39, color=1)
    self.csb_pushButton_02 = setPushbutton('매수전략 저장', box=self.cs_tab, click=self.ButtonClicked_40, color=1)
    self.csb_pushButton_03 = setPushbutton('매수변수 로딩', box=self.cs_tab, click=self.ButtonClicked_41, color=1)
    self.csb_pushButton_04 = setPushbutton('매수전략 시작', box=self.cs_tab, click=self.ButtonClicked_42, color=1)
    self.csb_pushButton_05 = setPushbutton('등락율제한', box=self.cs_tab, click=self.ButtonClicked_43)
    self.csb_pushButton_06 = setPushbutton('고저평균대비등락율', box=self.cs_tab, click=self.ButtonClicked_44)
    self.csb_pushButton_07 = setPushbutton('시가대비', box=self.cs_tab, click=self.ButtonClicked_45)
    self.csb_pushButton_08 = setPushbutton('체결강도하한', box=self.cs_tab, click=self.ButtonClicked_46)
    self.csb_pushButton_09 = setPushbutton('체결강도차이', box=self.cs_tab, click=self.ButtonClicked_47)
    self.csb_pushButton_10 = setPushbutton('평균값골든크로스', box=self.cs_tab, click=self.ButtonClicked_48)
    self.csb_pushButton_11 = setPushbutton('당일거래대금하한', box=self.cs_tab, click=self.ButtonClicked_49)
    self.csb_pushButton_12 = setPushbutton('초당거래대금차이', box=self.cs_tab, click=self.ButtonClicked_50)
    self.csb_pushButton_13 = setPushbutton('호가총잔량비교', box=self.cs_tab, click=self.ButtonClicked_51)
    self.csb_pushButton_14 = setPushbutton('1호가잔량비교', box=self.cs_tab, click=self.ButtonClicked_52)
    self.csb_pushButton_15 = setPushbutton('매수시그널', box=self.cs_tab, click=self.ButtonClicked_53, color=2)
    self.csb_pushButton_16 = setPushbutton('매수전략 중지', box=self.cs_tab, click=self.ButtonClicked_54, color=1)

    text = '백테스트 기간설정                                         ~'
    self.csb_labellll_01 = QtWidgets.QLabel(text, self.cs_tab)
    text = '백테스트 시간설정        시작시간                       종료시간'
    self.csb_labellll_02 = QtWidgets.QLabel(text, self.cs_tab)
    text = '배팅(백만)                     평균틱수                          멀티수'
    self.csb_labellll_03 = QtWidgets.QLabel(text, self.cs_tab)

    self.csb_dateEdit_01 = QtWidgets.QDateEdit(self.cs_tab)
    self.csb_dateEdit_01.setDate(QtCore.QDate.currentDate().addDays(-14))
    self.csb_dateEdit_01.setCalendarPopup(True)
    self.csb_dateEdit_02 = QtWidgets.QDateEdit(self.cs_tab)
    self.csb_dateEdit_02.setDate(QtCore.QDate.currentDate())
    self.csb_dateEdit_02.setCalendarPopup(True)
    self.csb_lineEdit_01 = setLineedit(self.cs_tab)
    self.csb_lineEdit_01.setText('90000')
    self.csb_lineEdit_02 = setLineedit(self.cs_tab)
    self.csb_lineEdit_02.setText('100000')
    self.csb_lineEdit_03 = setLineedit(self.cs_tab)
    self.csb_lineEdit_03.setText('10')
    self.csb_lineEdit_04 = setLineedit(self.cs_tab)
    self.csb_lineEdit_04.setText('60')
    self.csb_lineEdit_05 = setLineedit(self.cs_tab)
    self.csb_lineEdit_05.setText('6')
    self.cb_pushButton_01 = setPushbutton('백테스팅', box=self.cs_tab, click=self.ButtonClicked_55, color=1)
    self.cb_pushButton_02 = setPushbutton('최적화', box=self.cs_tab, click=self.ButtonClicked_56, color=1)
    self.cb_pushButton_03 = setPushbutton('전략편집기', box=self.cs_tab, click=self.ButtonClicked_92, color=1)
    self.cb_pushButton_04 = setPushbutton('백테스트 로그', box=self.cs_tab, click=self.ButtonClicked_93, color=1)
    self.cb_pushButton_03.setStyleSheet(style_bc_dk)

    self.css_comboBox = setCombobox(self.cs_tab, self.Activated_04)
    self.css_lineEdit = setLineedit2(self.cs_tab)
    self.css_pushButton_01 = setPushbutton('매도전략 로딩', box=self.cs_tab, click=self.ButtonClicked_57, color=1)
    self.css_pushButton_02 = setPushbutton('매도전략 저장', box=self.cs_tab, click=self.ButtonClicked_58, color=1)
    self.css_pushButton_03 = setPushbutton('매도변수 로딩', box=self.cs_tab, click=self.ButtonClicked_59, color=1)
    self.css_pushButton_04 = setPushbutton('매도전략 시작', box=self.cs_tab, click=self.ButtonClicked_60, color=1)
    self.css_pushButton_05 = setPushbutton('손절라인청산', box=self.cs_tab, click=self.ButtonClicked_61)
    self.css_pushButton_06 = setPushbutton('익절라인청산', box=self.cs_tab, click=self.ButtonClicked_62)
    self.css_pushButton_07 = setPushbutton('수익율보존청산', box=self.cs_tab, click=self.ButtonClicked_63)
    self.css_pushButton_08 = setPushbutton('보유시간기준청산', box=self.cs_tab, click=self.ButtonClicked_64)
    self.css_pushButton_09 = setPushbutton('체결강도차이', box=self.cs_tab, click=self.ButtonClicked_65)
    self.css_pushButton_10 = setPushbutton('최고체결강도비교', box=self.cs_tab, click=self.ButtonClicked_66)
    self.css_pushButton_11 = setPushbutton('고저평균대비등락율', box=self.cs_tab, click=self.ButtonClicked_67)
    self.css_pushButton_12 = setPushbutton('호가총잔량비교', box=self.cs_tab, click=self.ButtonClicked_68)
    self.css_pushButton_13 = setPushbutton('매도시그널', box=self.cs_tab, click=self.ButtonClicked_69, color=3)
    self.css_pushButton_14 = setPushbutton('매도전략 중지', box=self.cs_tab, click=self.ButtonClicked_70, color=1)

    self.st_textEdit = setTextEdit(self.lg_tab)
    self.ct_textEdit = setTextEdit(self.lg_tab)
    self.sc_textEdit = setTextEdit(self.lg_tab)
    self.cc_textEdit = setTextEdit(self.lg_tab)

    title = ' 증권사, 거래소, 프로세스 : 사용할 증권사 및 거래소를 선택하고 실행될 프로세스를 설정한다.'
    self.sj_groupBox_01 = QtWidgets.QGroupBox(title, self.sj_tab)
    title = ' 주식 계정 : 트레이더용 첫번째 계정과 리시버용 두번째 계정을 설정한다.'
    self.sj_groupBox_02 = QtWidgets.QGroupBox(title, self.sj_tab)
    title = ' 업비트 계정 : 업비트 주문 및 주문 확인용 Access 키와 Srcret 키를 설정한다.'
    self.sj_groupBox_03 = QtWidgets.QGroupBox(title, self.sj_tab)
    title = ' 텔레그램 : 봇토큰 및 사용자 채팅 아이디를 설정한다.'
    self.sj_groupBox_04 = QtWidgets.QGroupBox(title, self.sj_tab)
    title = ' 주식 : 모의투자 모드, 알림소리, 전략를 설정한다.'
    self.sj_groupBox_05 = QtWidgets.QGroupBox(title, self.sj_tab)
    title = ' 코인 : 모의투자 모드, 알림소리, 전략를 설정한다.'
    self.sj_groupBox_06 = QtWidgets.QGroupBox(title, self.sj_tab)
    self.sj_textEdit = setTextEdit(self.sj_tab)

    self.sj_main_comboBox_01 = setCombobox(self.sj_groupBox_01)
    self.sj_main_comboBox_01.addItem('키움증권')
    self.sj_main_comboBox_01.addItem('이베스트투자증권')
    self.sj_main_checkBox_01 = setCheckBos('주식 리시버', self.sj_groupBox_01, changed=self.CheckboxChanged_01)
    self.sj_main_checkBox_02 = setCheckBos('주식 콜렉터', self.sj_groupBox_01, changed=self.CheckboxChanged_02)
    self.sj_main_checkBox_03 = setCheckBos('주식 트레이더', self.sj_groupBox_01, changed=self.CheckboxChanged_03)

    self.sj_main_comboBox_02 = setCombobox(self.sj_groupBox_01)
    self.sj_main_comboBox_02.addItem('업비트')
    self.sj_main_checkBox_04 = setCheckBos('코인 리시버', self.sj_groupBox_01, changed=self.CheckboxChanged_04)
    self.sj_main_checkBox_05 = setCheckBos('코인 콜렉터', self.sj_groupBox_01, changed=self.CheckboxChanged_05)
    self.sj_main_checkBox_06 = setCheckBos('코인 트레이더', self.sj_groupBox_01, changed=self.CheckboxChanged_06)

    text = '주식 최근거래대금순위 집계시간(분)                         ' \
           '최근거래대금순위 선정등수                                 ' \
           '코인 최근거래대금순위 집계시간(분)                         ' \
           '최근거래대금순위 선정등수'
    self.sj_main_labellll_01 = QtWidgets.QLabel(text, self.sj_groupBox_01)
    self.sj_main_lineEdit_01 = setLineedit(self.sj_groupBox_01)
    self.sj_main_lineEdit_02 = setLineedit(self.sj_groupBox_01)
    self.sj_main_lineEdit_03 = setLineedit(self.sj_groupBox_01)
    self.sj_main_lineEdit_04 = setLineedit(self.sj_groupBox_01)

    self.sj_main_checkBox_07 = setCheckBos('주식 틱데이터 실시간 저장', self.sj_groupBox_01, changed=self.CheckboxChanged_07)
    self.sj_main_checkBox_08 = setCheckBos('전체 종목 저장    |', self.sj_groupBox_01, changed=self.CheckboxChanged_08)
    self.sj_main_labellll_03 = QtWidgets.QLabel('실시간 저장 주기(초)', self.sj_groupBox_01)
    self.sj_main_lineEdit_05 = setLineedit(self.sj_groupBox_01)

    self.sj_main_checkBox_09 = setCheckBos('코인 틱데이터 실시간 저장', self.sj_groupBox_01, changed=self.CheckboxChanged_09)
    self.sj_main_checkBox_10 = setCheckBos('전체 종목 저장    |', self.sj_groupBox_01, changed=self.CheckboxChanged_10)
    self.sj_main_labellll_04 = QtWidgets.QLabel('실시간 저장 주기(초)', self.sj_groupBox_01)
    self.sj_main_lineEdit_06 = setLineedit(self.sj_groupBox_01)

    text = '첫번째 계정 아이디                                                         ' \
           '비밀번호                                                           ' \
           '인증서비밀번호                                                       ' \
           '계좌비밀번호'
    self.sj_sacc_labellll_01 = QtWidgets.QLabel(text, self.sj_groupBox_02)
    self.sj_sacc_lineEdit_01 = setLineedit(self.sj_groupBox_02, passhide=True)
    self.sj_sacc_lineEdit_02 = setLineedit(self.sj_groupBox_02, passhide=True)
    self.sj_sacc_lineEdit_03 = setLineedit(self.sj_groupBox_02, passhide=True)
    self.sj_sacc_lineEdit_04 = setLineedit(self.sj_groupBox_02, passhide=True)

    text = '두번째 계정 아이디                                                         ' \
           '비밀번호                                                           ' \
           '인증서비밀번호                                                       ' \
           '계좌비밀번호'
    self.sj_sacc_labellll_02 = QtWidgets.QLabel(text, self.sj_groupBox_02)
    self.sj_sacc_lineEdit_05 = setLineedit(self.sj_groupBox_02, passhide=True)
    self.sj_sacc_lineEdit_06 = setLineedit(self.sj_groupBox_02, passhide=True)
    self.sj_sacc_lineEdit_07 = setLineedit(self.sj_groupBox_02, passhide=True)
    self.sj_sacc_lineEdit_08 = setLineedit(self.sj_groupBox_02, passhide=True)

    text = 'Access Key                                                                                              ' \
           '                                                Secret Key'
    self.sj_cacc_labellll_01 = QtWidgets.QLabel(text, self.sj_groupBox_03)
    self.sj_cacc_lineEdit_01 = setLineedit(self.sj_groupBox_03, passhide=True)
    self.sj_cacc_lineEdit_02 = setLineedit(self.sj_groupBox_03, passhide=True)

    text = 'Bot Token                                                                                              ' \
           '                                                  Chat Id'
    self.sj_tele_labellll_01 = QtWidgets.QLabel(text, self.sj_groupBox_04)
    self.sj_tele_lineEdit_01 = setLineedit(self.sj_groupBox_04, passhide=True)
    self.sj_tele_lineEdit_02 = setLineedit(self.sj_groupBox_04, passhide=True)

    self.sj_stock_checkBox_01 = setCheckBos('모의투자    |', self.sj_groupBox_05, changed=self.CheckboxChanged_11)
    self.sj_stock_checkBox_02 = QtWidgets.QCheckBox('알림소리    |', self.sj_groupBox_05)
    text = '장초전략 매수                                              '\
           '매도                                                       '\
           '평균값계산틱수                       최대매수종목수'
    self.sj_stock_labellll_01 = QtWidgets.QLabel(text, self.sj_groupBox_05)
    self.sj_stock_comboBox_01 = setCombobox(self.sj_groupBox_05)
    self.sj_stock_comboBox_02 = setCombobox(self.sj_groupBox_05)
    self.sj_stock_lineEdit_01 = setLineedit(self.sj_groupBox_05)
    self.sj_stock_lineEdit_02 = setLineedit(self.sj_groupBox_05)
    text = '장중전략 매수                                              '\
           '매도                                                       '\
           '평균값계산틱수                       최대매수종목수'
    self.sj_stock_labellll_02 = QtWidgets.QLabel(text, self.sj_groupBox_05)
    self.sj_stock_comboBox_03 = setCombobox(self.sj_groupBox_05)
    self.sj_stock_comboBox_04 = setCombobox(self.sj_groupBox_05)
    self.sj_stock_lineEdit_03 = setLineedit(self.sj_groupBox_05)
    self.sj_stock_lineEdit_04 = setLineedit(self.sj_groupBox_05)

    self.sj_coin_checkBox_01 = setCheckBos('모의투자    |', self.sj_groupBox_06, changed=self.CheckboxChanged_12)
    self.sj_coin_checkBox_02 = QtWidgets.QCheckBox('알림소리    |', self.sj_groupBox_06)
    text = '장초전략 매수                                              '\
           '매도                                                       '\
           '평균값계산틱수                       최대매수종목수'
    self.sj_coin_labellll_01 = QtWidgets.QLabel(text, self.sj_groupBox_06)
    self.sj_coin_comboBox_01 = setCombobox(self.sj_groupBox_06)
    self.sj_coin_comboBox_02 = setCombobox(self.sj_groupBox_06)
    self.sj_coin_lineEdit_01 = setLineedit(self.sj_groupBox_06)
    self.sj_coin_lineEdit_02 = setLineedit(self.sj_groupBox_06)
    text = '장중전략 매수                                              '\
           '매도                                                       '\
           '평균값계산틱수                       최대매수종목수'
    self.sj_coin_labellll_02 = QtWidgets.QLabel(text, self.sj_groupBox_06)
    self.sj_coin_comboBox_03 = setCombobox(self.sj_groupBox_06)
    self.sj_coin_comboBox_04 = setCombobox(self.sj_groupBox_06)
    self.sj_coin_lineEdit_03 = setLineedit(self.sj_groupBox_06)
    self.sj_coin_lineEdit_04 = setLineedit(self.sj_groupBox_06)

    self.sj_load_pushButton_01 = setPushbutton('불러오기', box=self.sj_groupBox_01, click=self.ButtonClicked_71)
    self.sj_load_pushButton_02 = setPushbutton('불러오기', box=self.sj_groupBox_02, click=self.ButtonClicked_72)
    self.sj_load_pushButton_03 = setPushbutton('불러오기', box=self.sj_groupBox_03, click=self.ButtonClicked_73)
    self.sj_load_pushButton_04 = setPushbutton('불러오기', box=self.sj_groupBox_04, click=self.ButtonClicked_74)
    self.sj_load_pushButton_05 = setPushbutton('불러오기', box=self.sj_groupBox_05, click=self.ButtonClicked_75)
    self.sj_load_pushButton_06 = setPushbutton('불러오기', box=self.sj_groupBox_06, click=self.ButtonClicked_76)

    self.sj_load_pushButton_00 = setPushbutton('계정 텍스트 보기', box=self.sj_groupBox_02, click=self.ButtonClicked_83)

    self.sj_save_pushButton_01 = setPushbutton('저장하기', box=self.sj_groupBox_01, click=self.ButtonClicked_77)
    self.sj_save_pushButton_02 = setPushbutton('저장하기', box=self.sj_groupBox_02, click=self.ButtonClicked_78)
    self.sj_save_pushButton_03 = setPushbutton('저장하기', box=self.sj_groupBox_03, click=self.ButtonClicked_79)
    self.sj_save_pushButton_04 = setPushbutton('저장하기', box=self.sj_groupBox_04, click=self.ButtonClicked_80)
    self.sj_save_pushButton_05 = setPushbutton('저장하기', box=self.sj_groupBox_05, click=self.ButtonClicked_81)
    self.sj_save_pushButton_06 = setPushbutton('저장하기', box=self.sj_groupBox_06, click=self.ButtonClicked_82)

    self.dialog_chart = QtWidgets.QDialog()
    self.dialog_chart.setWindowTitle('STOM CHART')
    self.dialog_chart.setWindowModality(Qt.NonModal)
    self.dialog_chart.setWindowIcon(QtGui.QIcon(f'{ICON_PATH}/python.png'))
    self.dialog_chart.geometry().center()

    self.ct_groupBox_01 = QtWidgets.QGroupBox(' ', self.dialog_chart)
    self.ct_groupBox_02 = QtWidgets.QGroupBox(' ', self.dialog_chart)

    self.ct_dateEdit = QtWidgets.QDateEdit(self.ct_groupBox_01)
    self.ct_dateEdit.setDate(QtCore.QDate.currentDate())
    self.ct_dateEdit.setCalendarPopup(True)
    self.ct_labellll_01 = QtWidgets.QLabel('평균값계산틱수', self.ct_groupBox_01)
    self.ct_lineEdit_01 = setLineedit(self.ct_groupBox_01)
    self.ct_lineEdit_01.setStyleSheet(style_bc_dk)
    self.ct_labellll_02 = QtWidgets.QLabel('종목명 또는 종목코드', self.ct_groupBox_01)
    self.ct_lineEdit_02 = setLineedit(self.ct_groupBox_01, enter=self.ReturnPress_01)
    self.ct_lineEdit_02.setStyleSheet(style_bc_dk)
    self.ct_pushButton_01 = setPushbutton('검색하기', box=self.ct_groupBox_01, click=self.ReturnPress_01)

    ctpg = pyqtgraph.GraphicsLayoutWidget()
    self.ctpg_01 = ctpg.addPlot(row=0, col=0, viewBox=CustomViewBox(), axisItems={'bottom': pyqtgraph.DateAxisItem()})
    self.ctpg_02 = ctpg.addPlot(row=1, col=0, viewBox=CustomViewBox(), axisItems={'bottom': pyqtgraph.DateAxisItem()})
    self.ctpg_03 = ctpg.addPlot(row=2, col=0, viewBox=CustomViewBox(), axisItems={'bottom': pyqtgraph.DateAxisItem()})
    self.ctpg_01.showAxis('left', False)
    self.ctpg_01.showAxis('right', True)
    self.ctpg_01.getAxis('right').setStyle(tickTextWidth=45, autoExpandTextSpace=False)
    self.ctpg_01.getAxis('right').setTickFont(qfont12)
    self.ctpg_01.getAxis('bottom').setTickFont(qfont12)
    self.ctpg_02.showAxis('left', False)
    self.ctpg_02.showAxis('right', True)
    self.ctpg_02.getAxis('right').setStyle(tickTextWidth=45, autoExpandTextSpace=False)
    self.ctpg_02.getAxis('right').setTickFont(qfont12)
    self.ctpg_02.getAxis('bottom').setTickFont(qfont12)
    self.ctpg_03.showAxis('left', False)
    self.ctpg_03.showAxis('right', True)
    self.ctpg_03.getAxis('right').setStyle(tickTextWidth=45, autoExpandTextSpace=False)
    self.ctpg_03.getAxis('right').setTickFont(qfont12)
    self.ctpg_03.getAxis('bottom').setTickFont(qfont12)
    self.ctpg_02.setXLink(self.ctpg_01)
    self.ctpg_03.setXLink(self.ctpg_01)
    qGraphicsGridLayout = ctpg.ci.layout
    qGraphicsGridLayout.setRowStretchFactor(0, 1)
    qGraphicsGridLayout.setRowStretchFactor(1, 1)
    qGraphicsGridLayout.setRowStretchFactor(2, 1)
    ctpg_vboxLayout = QtWidgets.QVBoxLayout(self.ct_groupBox_02)
    ctpg_vboxLayout.setContentsMargins(3, 6, 3, 3)
    ctpg_vboxLayout.addWidget(ctpg)

    self.ct_labellll_03 = QtWidgets.QLabel('', self.ct_groupBox_02)
    self.ct_labellll_04 = QtWidgets.QLabel('', self.ct_groupBox_02)
    self.ct_labellll_05 = QtWidgets.QLabel('', self.ct_groupBox_02)

    self.ct_labellll_06 = QtWidgets.QLabel('', self.ct_groupBox_02)
    self.ct_labellll_07 = QtWidgets.QLabel('', self.ct_groupBox_02)
    self.ct_labellll_08 = QtWidgets.QLabel('', self.ct_groupBox_02)

    self.dialog_hoga = QtWidgets.QDialog()
    self.dialog_hoga.setWindowTitle('STOM HOGA')
    self.dialog_hoga.setWindowModality(Qt.NonModal)
    self.dialog_hoga.setWindowIcon(QtGui.QIcon(f'{ICON_PATH}/python.png'))
    self.dialog_hoga.geometry().center()

    self.hj_tableWidget = setTablewidget(self.dialog_hoga, columns_hj, 1)
    self.hc_tableWidget = setTablewidget(self.dialog_hoga, columns_hc, 12)
    self.hg_tableWidget = setTablewidget(self.dialog_hoga, columns_hg, 12)
    self.hg_line = setLine(self.dialog_hoga, 1)

    self.setFixedSize(1403, 763)
    self.geometry().center()
    self.main_tabWidget.setGeometry(5, 5, 1393, 753)
    self.tt_pushButton.setGeometry(5, 250, 35, 32)
    self.ms_pushButton.setGeometry(5, 287, 35, 32)
    self.zo_pushButton.setGeometry(5, 324, 35, 32)
    self.progressBar.setGeometry(6, 361, 33, 320)
    self.dd_pushButton.setGeometry(5, 687, 35, 32)
    self.sd_pushButton.setGeometry(5, 724, 35, 32)
    self.qs_pushButton.setGeometry(0, 0, 0, 0)
    self.ct_pushButton.setGeometry(0, 0, 0, 0)
    self.hg_pushButton.setGeometry(0, 0, 0, 0)

    self.stt_tableWidget.setGeometry(5, 5, 668, 42)
    self.std_tableWidget.setGeometry(5, 52, 668, 320)
    self.stj_tableWidget.setGeometry(5, 377, 668, 42)
    self.sjg_tableWidget.setGeometry(5, 424, 668, 320)
    self.sgj_tableWidget.setGeometry(678, 5, 668, 367)
    self.scj_tableWidget.setGeometry(678, 377, 668, 367)

    self.s_calendarWidget.setGeometry(5, 5, 668, 245)
    self.sdt_tableWidget.setGeometry(5, 255, 668, 42)
    self.sds_tableWidget.setGeometry(5, 302, 668, 442)

    self.snt_pushButton_01.setGeometry(678, 5, 219, 30)
    self.snt_pushButton_02.setGeometry(902, 5, 219, 30)
    self.snt_pushButton_03.setGeometry(1126, 5, 220, 30)
    self.snt_tableWidget.setGeometry(678, 40, 668, 42)
    self.sns_tableWidget.setGeometry(678, 87, 668, 657)

    self.ctt_tableWidget.setGeometry(5, 5, 668, 42)
    self.ctd_tableWidget.setGeometry(5, 52, 668, 320)
    self.ctj_tableWidget.setGeometry(5, 377, 668, 42)
    self.cjg_tableWidget.setGeometry(5, 424, 668, 320)
    self.cgj_tableWidget.setGeometry(678, 5, 668, 367)
    self.ccj_tableWidget.setGeometry(678, 377, 668, 367)

    self.c_calendarWidget.setGeometry(5, 5, 668, 245)
    self.cdt_tableWidget.setGeometry(5, 255, 668, 42)
    self.cds_tableWidget.setGeometry(5, 302, 668, 442)

    self.cnt_pushButton_01.setGeometry(678, 5, 219, 30)
    self.cnt_pushButton_02.setGeometry(902, 5, 219, 30)
    self.cnt_pushButton_03.setGeometry(1126, 5, 220, 30)
    self.cnt_tableWidget.setGeometry(678, 40, 668, 42)
    self.cns_tableWidget.setGeometry(678, 87, 668, 657)

    self.sj_groupBox_01.setGeometry(5, 10, 1341, 120)
    self.sj_groupBox_02.setGeometry(5, 150, 1341, 90)
    self.sj_groupBox_03.setGeometry(5, 260, 1341, 65)
    self.sj_groupBox_04.setGeometry(5, 345, 1341, 65)
    self.sj_groupBox_05.setGeometry(5, 430, 1341, 90)
    self.sj_groupBox_06.setGeometry(5, 540, 1341, 90)
    self.sj_textEdit.setGeometry(5, 640, 1341, 103)

    self.ss_textEdit_01.setGeometry(5, 5, 1000, 463)
    self.ss_textEdit_02.setGeometry(5, 473, 1000, 270)
    self.ss_textEdit_03.setGeometry(5, 5, 1000, 738)

    self.ssb_comboBox.setGeometry(1010, 5, 165, 25)
    self.ssb_lineEdit.setGeometry(1180, 5, 165, 25)
    self.ssb_pushButton_01.setGeometry(1010, 35, 165, 30)
    self.ssb_pushButton_02.setGeometry(1180, 35, 165, 30)
    self.ssb_pushButton_03.setGeometry(1010, 70, 165, 30)
    self.ssb_pushButton_04.setGeometry(1180, 70, 165, 30)
    self.ssb_pushButton_05.setGeometry(1010, 105, 165, 30)
    self.ssb_pushButton_06.setGeometry(1180, 105, 165, 30)
    self.ssb_pushButton_07.setGeometry(1010, 140, 165, 30)
    self.ssb_pushButton_08.setGeometry(1180, 140, 165, 30)
    self.ssb_pushButton_09.setGeometry(1010, 175, 165, 30)
    self.ssb_pushButton_10.setGeometry(1180, 175, 165, 30)
    self.ssb_pushButton_11.setGeometry(1010, 210, 165, 30)
    self.ssb_pushButton_12.setGeometry(1180, 210, 165, 30)
    self.ssb_pushButton_13.setGeometry(1010, 245, 165, 30)
    self.ssb_pushButton_14.setGeometry(1180, 245, 165, 30)
    self.ssb_pushButton_15.setGeometry(1010, 280, 165, 30)
    self.ssb_pushButton_16.setGeometry(1180, 280, 165, 30)

    self.ssb_labellll_01.setGeometry(1010, 320, 335, 20)
    self.ssb_labellll_02.setGeometry(1010, 345, 335, 20)
    self.ssb_labellll_03.setGeometry(1010, 370, 335, 20)

    self.ssb_dateEdit_01.setGeometry(1110, 320, 110, 20)
    self.ssb_dateEdit_02.setGeometry(1235, 320, 110, 20)
    self.ssb_lineEdit_01.setGeometry(1175, 345, 55, 20)
    self.ssb_lineEdit_02.setGeometry(1290, 345, 55, 20)
    self.ssb_lineEdit_03.setGeometry(1065, 370, 55, 20)
    self.ssb_lineEdit_04.setGeometry(1175, 370, 55, 20)
    self.ssb_lineEdit_05.setGeometry(1290, 370, 55, 20)
    self.sb_pushButton_01.setGeometry(1010, 400, 165, 30)
    self.sb_pushButton_02.setGeometry(1180, 400, 165, 30)
    self.sb_pushButton_03.setGeometry(1010, 435, 165, 30)
    self.sb_pushButton_04.setGeometry(1180, 435, 165, 30)

    self.sss_comboBox.setGeometry(1010, 473, 165, 25)
    self.sss_lineEdit.setGeometry(1180, 473, 165, 25)
    self.sss_pushButton_01.setGeometry(1010, 503, 165, 30)
    self.sss_pushButton_02.setGeometry(1180, 503, 165, 30)
    self.sss_pushButton_03.setGeometry(1010, 538, 165, 30)
    self.sss_pushButton_04.setGeometry(1180, 538, 165, 30)
    self.sss_pushButton_05.setGeometry(1010, 573, 165, 30)
    self.sss_pushButton_06.setGeometry(1180, 573, 165, 30)
    self.sss_pushButton_07.setGeometry(1010, 608, 165, 30)
    self.sss_pushButton_08.setGeometry(1180, 608, 165, 30)
    self.sss_pushButton_09.setGeometry(1010, 643, 165, 30)
    self.sss_pushButton_10.setGeometry(1180, 643, 165, 30)
    self.sss_pushButton_11.setGeometry(1010, 678, 165, 30)
    self.sss_pushButton_12.setGeometry(1180, 678, 165, 30)
    self.sss_pushButton_13.setGeometry(1010, 713, 165, 30)
    self.sss_pushButton_14.setGeometry(1180, 713, 165, 30)

    self.cs_textEdit_01.setGeometry(5, 5, 1000, 463)
    self.cs_textEdit_02.setGeometry(5, 473, 1000, 270)
    self.cs_textEdit_03.setGeometry(5, 5, 1000, 738)

    self.csb_comboBox.setGeometry(1010, 5, 165, 25)
    self.csb_lineEdit.setGeometry(1180, 5, 165, 25)
    self.csb_pushButton_01.setGeometry(1010, 35, 165, 30)
    self.csb_pushButton_02.setGeometry(1180, 35, 165, 30)
    self.csb_pushButton_03.setGeometry(1010, 70, 165, 30)
    self.csb_pushButton_04.setGeometry(1180, 70, 165, 30)
    self.csb_pushButton_05.setGeometry(1010, 105, 165, 30)
    self.csb_pushButton_06.setGeometry(1180, 105, 165, 30)
    self.csb_pushButton_07.setGeometry(1010, 140, 165, 30)
    self.csb_pushButton_08.setGeometry(1180, 140, 165, 30)
    self.csb_pushButton_09.setGeometry(1010, 175, 165, 30)
    self.csb_pushButton_10.setGeometry(1180, 175, 165, 30)
    self.csb_pushButton_11.setGeometry(1010, 210, 165, 30)
    self.csb_pushButton_12.setGeometry(1180, 210, 165, 30)
    self.csb_pushButton_13.setGeometry(1010, 245, 165, 30)
    self.csb_pushButton_14.setGeometry(1180, 245, 165, 30)
    self.csb_pushButton_15.setGeometry(1010, 280, 165, 30)
    self.csb_pushButton_16.setGeometry(1180, 280, 165, 30)

    self.csb_labellll_01.setGeometry(1010, 320, 335, 20)
    self.csb_labellll_02.setGeometry(1010, 345, 335, 20)
    self.csb_labellll_03.setGeometry(1010, 370, 335, 20)

    self.csb_dateEdit_01.setGeometry(1110, 320, 110, 20)
    self.csb_dateEdit_02.setGeometry(1235, 320, 110, 20)
    self.csb_lineEdit_01.setGeometry(1175, 345, 55, 20)
    self.csb_lineEdit_02.setGeometry(1290, 345, 55, 20)
    self.csb_lineEdit_03.setGeometry(1065, 370, 55, 20)
    self.csb_lineEdit_04.setGeometry(1175, 370, 55, 20)
    self.csb_lineEdit_05.setGeometry(1290, 370, 55, 20)
    self.cb_pushButton_01.setGeometry(1010, 400, 165, 30)
    self.cb_pushButton_02.setGeometry(1180, 400, 165, 30)
    self.cb_pushButton_03.setGeometry(1010, 435, 165, 30)
    self.cb_pushButton_04.setGeometry(1180, 435, 165, 30)

    self.css_comboBox.setGeometry(1010, 473, 165, 25)
    self.css_lineEdit.setGeometry(1180, 473, 165, 25)
    self.css_pushButton_01.setGeometry(1010, 503, 165, 30)
    self.css_pushButton_02.setGeometry(1180, 503, 165, 30)
    self.css_pushButton_03.setGeometry(1010, 538, 165, 30)
    self.css_pushButton_04.setGeometry(1180, 538, 165, 30)
    self.css_pushButton_05.setGeometry(1010, 573, 165, 30)
    self.css_pushButton_06.setGeometry(1180, 573, 165, 30)
    self.css_pushButton_07.setGeometry(1010, 608, 165, 30)
    self.css_pushButton_08.setGeometry(1180, 608, 165, 30)
    self.css_pushButton_09.setGeometry(1010, 643, 165, 30)
    self.css_pushButton_10.setGeometry(1180, 643, 165, 30)
    self.css_pushButton_11.setGeometry(1010, 678, 165, 30)
    self.css_pushButton_12.setGeometry(1180, 678, 165, 30)
    self.css_pushButton_13.setGeometry(1010, 713, 165, 30)
    self.css_pushButton_14.setGeometry(1180, 713, 165, 30)

    self.st_textEdit.setGeometry(5, 5, 668, 367)
    self.ct_textEdit.setGeometry(678, 5, 668, 367)
    self.sc_textEdit.setGeometry(5, 377, 668, 367)
    self.cc_textEdit.setGeometry(678, 377, 668, 367)

    self.sj_main_comboBox_01.setGeometry(10, 30, 140, 22)
    self.sj_main_checkBox_01.setGeometry(170, 30, 90, 20)
    self.sj_main_checkBox_02.setGeometry(270, 30, 90, 20)
    self.sj_main_checkBox_03.setGeometry(370, 30, 90, 20)

    self.sj_main_comboBox_02.setGeometry(500, 30, 140, 22)
    self.sj_main_checkBox_04.setGeometry(660, 30, 90, 20)
    self.sj_main_checkBox_05.setGeometry(760, 30, 90, 20)
    self.sj_main_checkBox_06.setGeometry(860, 30, 90, 20)

    self.sj_main_labellll_01.setGeometry(10, 60, 1000, 20)
    self.sj_main_lineEdit_01.setGeometry(200, 60, 50, 20)
    self.sj_main_lineEdit_02.setGeometry(410, 60, 50, 20)
    self.sj_main_lineEdit_03.setGeometry(690, 60, 50, 20)
    self.sj_main_lineEdit_04.setGeometry(900, 60, 50, 20)

    self.sj_main_checkBox_07.setGeometry(10, 90, 160, 20)
    self.sj_main_checkBox_08.setGeometry(180, 90, 125, 20)
    self.sj_main_labellll_03.setGeometry(295, 90, 105, 20)
    self.sj_main_lineEdit_05.setGeometry(410, 90, 50, 20)

    self.sj_main_checkBox_09.setGeometry(500, 90, 160, 20)
    self.sj_main_checkBox_10.setGeometry(670, 90, 125, 20)
    self.sj_main_labellll_04.setGeometry(785, 90, 105, 20)
    self.sj_main_lineEdit_06.setGeometry(900, 90, 50, 20)

    self.sj_sacc_labellll_01.setGeometry(10, 30, 1000, 20)
    self.sj_sacc_lineEdit_01.setGeometry(115, 30, 130, 20)
    self.sj_sacc_lineEdit_02.setGeometry(330, 30, 130, 20)
    self.sj_sacc_lineEdit_03.setGeometry(585, 30, 130, 20)
    self.sj_sacc_lineEdit_04.setGeometry(820, 30, 130, 20)
    self.sj_sacc_labellll_02.setGeometry(10, 60, 1000, 20)
    self.sj_sacc_lineEdit_05.setGeometry(115, 60, 130, 20)
    self.sj_sacc_lineEdit_06.setGeometry(330, 60, 130, 20)
    self.sj_sacc_lineEdit_07.setGeometry(585, 60, 130, 20)
    self.sj_sacc_lineEdit_08.setGeometry(820, 60, 130, 20)

    self.sj_cacc_labellll_01.setGeometry(10, 30, 1000, 20)
    self.sj_cacc_lineEdit_01.setGeometry(85, 30, 375, 20)
    self.sj_cacc_lineEdit_02.setGeometry(575, 30, 375, 20)

    self.sj_tele_labellll_01.setGeometry(10, 30, 1000, 20)
    self.sj_tele_lineEdit_01.setGeometry(85, 30, 375, 20)
    self.sj_tele_lineEdit_02.setGeometry(575, 30, 375, 20)

    self.sj_stock_checkBox_01.setGeometry(10, 30, 90, 20)
    self.sj_stock_checkBox_02.setGeometry(10, 60, 90, 20)

    self.sj_stock_labellll_01.setGeometry(100, 30, 910, 20)
    self.sj_stock_comboBox_01.setGeometry(175, 30, 125, 22)
    self.sj_stock_comboBox_02.setGeometry(335, 30, 125, 22)
    self.sj_stock_lineEdit_01.setGeometry(580, 30, 50, 20)
    self.sj_stock_lineEdit_02.setGeometry(725, 30, 50, 20)
    self.sj_stock_labellll_02.setGeometry(100, 60, 910, 20)
    self.sj_stock_comboBox_03.setGeometry(175, 60, 125, 22)
    self.sj_stock_comboBox_04.setGeometry(335, 60, 125, 22)
    self.sj_stock_lineEdit_03.setGeometry(580, 60, 50, 20)
    self.sj_stock_lineEdit_04.setGeometry(725, 60, 50, 20)

    self.sj_coin_checkBox_01.setGeometry(10, 30, 90, 20)
    self.sj_coin_checkBox_02.setGeometry(10, 60, 90, 20)

    self.sj_coin_labellll_01.setGeometry(100, 30, 910, 20)
    self.sj_coin_comboBox_01.setGeometry(175, 30, 125, 22)
    self.sj_coin_comboBox_02.setGeometry(335, 30, 125, 22)
    self.sj_coin_lineEdit_01.setGeometry(580, 30, 50, 20)
    self.sj_coin_lineEdit_02.setGeometry(725, 30, 50, 20)
    self.sj_coin_labellll_02.setGeometry(100, 60, 910, 20)
    self.sj_coin_comboBox_03.setGeometry(175, 60, 125, 22)
    self.sj_coin_comboBox_04.setGeometry(335, 60, 125, 22)
    self.sj_coin_lineEdit_03.setGeometry(580, 60, 50, 20)
    self.sj_coin_lineEdit_04.setGeometry(725, 60, 50, 20)

    self.sj_load_pushButton_00.setGeometry(1180, 60, 150, 22)
    self.sj_load_pushButton_01.setGeometry(1180, 30, 70, 22)
    self.sj_load_pushButton_02.setGeometry(1180, 30, 70, 22)
    self.sj_load_pushButton_03.setGeometry(1180, 30, 70, 22)
    self.sj_load_pushButton_04.setGeometry(1180, 30, 70, 22)
    self.sj_load_pushButton_05.setGeometry(1180, 30, 70, 22)
    self.sj_load_pushButton_06.setGeometry(1180, 30, 70, 22)

    self.sj_save_pushButton_01.setGeometry(1260, 30, 70, 22)
    self.sj_save_pushButton_02.setGeometry(1260, 30, 70, 22)
    self.sj_save_pushButton_03.setGeometry(1260, 30, 70, 22)
    self.sj_save_pushButton_04.setGeometry(1260, 30, 70, 22)
    self.sj_save_pushButton_05.setGeometry(1260, 30, 70, 22)
    self.sj_save_pushButton_06.setGeometry(1260, 30, 70, 22)

    self.dialog_chart.setFixedSize(760, 1000)
    self.ct_groupBox_01.setGeometry(5, -10, 750, 62)
    self.ct_groupBox_02.setGeometry(5, 40, 750, 955)

    self.ct_dateEdit.setGeometry(10, 25, 160, 30)
    self.ct_labellll_01.setGeometry(190, 25, 90, 30)
    self.ct_lineEdit_01.setGeometry(290, 25, 80, 30)
    self.ct_labellll_02.setGeometry(390, 25, 120, 30)
    self.ct_lineEdit_02.setGeometry(520, 25, 120, 30)
    self.ct_pushButton_01.setGeometry(650, 25, 95, 30)

    self.ct_labellll_03.setGeometry(20, 40, 200, 15)
    self.ct_labellll_04.setGeometry(20, 345, 200, 15)
    self.ct_labellll_05.setGeometry(20, 650, 200, 15)

    self.ct_labellll_06.setGeometry(20, 65, 200, 15)
    self.ct_labellll_07.setGeometry(20, 375, 200, 40)
    self.ct_labellll_08.setGeometry(20, 680, 200, 25)

    self.dialog_hoga.setFixedSize(572, 355)
    self.hj_tableWidget.setGeometry(5, 5, 562, 42)
    self.hc_tableWidget.setGeometry(5, 52, 282, 297)
    self.hg_tableWidget.setGeometry(285, 52, 282, 297)
    self.hg_line.setGeometry(5, 209, 562, 1)
