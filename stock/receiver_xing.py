import os
import sys
import sqlite3
from PyQt5 import QtCore
from PyQt5 import QtWidgets
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.xing import *
from utility.static import now, strf_time, strp_time, timedelta_sec
from utility.setting import ui_num, dict_oper, DICT_SET, DB_TRADELIST, DB_STOCK_TICK


class Updater(QtCore.QThread):
    data1 = QtCore.pyqtSignal(str)
    data2 = QtCore.pyqtSignal(dict)

    def __init__(self, sreceivQ):
        super().__init__()
        self.sreceivQ = sreceivQ

    def run(self):
        while True:
            data = self.sreceivQ.get()
            if type(data) == str:
                self.data1.emit(data)
            elif type(data) == dict:
                self.data2.emit(data)


class ReceiverXing:
    def __init__(self, qlist):
        app = QtWidgets.QApplication(sys.argv)
        """
                    0        1       2        3       4       5          6          7        8      9
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceiv1Q, creceiv2Q, stockQ, coinQ,
                 sstgQ, cstgQ, tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, chartQ, hogaQ]
                   10    11      12      13      14      15      16      17     18
        """
        self.windowQ = qlist[0]
        self.query1Q = qlist[2]
        self.query2Q = qlist[3]
        self.sreceivQ = qlist[5]
        self.stockQ = qlist[8]
        self.sstgQ = qlist[10]
        self.tick1Q = qlist[12]
        self.tick2Q = qlist[13]
        self.tick3Q = qlist[14]
        self.tick4Q = qlist[15]
        self.hogaQ = qlist[18]
        self.dict_set = DICT_SET

        self.dict_bool = {
            '리시버시작': False,
            '실시간조건검색시작': False,
            '실시간조건검색중단': False,
            '장중단타전략시작': False,
        }
        self.dict_cdjm = {}
        self.dict_vipr = {}
        self.dict_tick = {}
        self.dict_hoga = {}
        self.dict_name = {}
        self.dict_code = {}
        self.dict_sghg = {}

        self.list_gsjm1 = []
        self.list_gsjm2 = []
        self.list_jang = []
        self.list_alertnum = []
        self.list_cond = None
        self.list_prmt = None
        self.list_kosd = None
        self.list_code = None
        self.list_code1 = None
        self.list_code2 = None
        self.list_code3 = None
        self.list_code4 = None
        self.hoga_code = None

        self.df_mt = pd.DataFrame(columns=['거래대금순위'])
        self.df_mc = pd.DataFrame(columns=['최근거래대금'])
        self.operation = 1
        self.str_tday = strf_time('%Y%m%d')
        self.str_jcct = self.str_tday + '090000'
        self.df_pc = None
        self.dt_mtct = None

        remaintime = (strp_time('%Y%m%d%H%M%S', self.str_tday + '090100') - now()).total_seconds()
        self.dict_time = {
            '휴무종료': timedelta_sec(remaintime) if remaintime > 0 else timedelta_sec(600),
            '거래대금순위기록': now(),
            '거래대금순위저장': now()
        }

        self.xas = XASession()
        self.xaq = XAQuery(self)

        self.xar_op = XAReal(self)
        self.xar_vi = XAReal(self)
        self.xar_cp = XAReal(self)
        self.xar_cd = XAReal(self)
        self.xar_hp = XAReal(self)
        self.xar_hd = XAReal(self)

        self.xar_op.RegisterRes('JIF')
        self.xar_vi.RegisterRes('VI_')
        self.xar_cp.RegisterRes('S3_')
        self.xar_cd.RegisterRes('K3_')
        self.xar_hp.RegisterRes('H1_')
        self.xar_hd.RegisterRes('HA_')

        self.XingLogin()

        self.updater = Updater(self.sreceivQ)
        self.updater.data1.connect(self.UpdateJangolist)
        self.updater.data2.connect(self.UpdateDictset)
        self.updater.start()

        self.qtimer1 = QtCore.QTimer()
        self.qtimer1.setInterval(1000)
        self.qtimer1.timeout.connect(self.Scheduler)
        self.qtimer1.start()

        self.qtimer2 = QtCore.QTimer()
        self.qtimer2.setInterval(10000)
        self.qtimer2.timeout.connect(self.MoneyTopSearch)

        app.exec_()

    def __del__(self):
        self.xar_op.RemoveAllRealData()
        self.xar_vi.RemoveAllRealData()
        self.xar_cp.RemoveAllRealData()
        self.xar_hp.RemoveAllRealData()
        self.xar_cd.RemoveAllRealData()
        self.xar_hd.RemoveAllRealData()
        if len(self.list_alertnum) > 0:
            for alertnum in self.list_alertnum:
                self.xaq.RemoveService(alertnum)

    def XingLogin(self):
        self.xas.Login(self.dict_set['아이디2'], self.dict_set['비밀번호2'], self.dict_set['인증서비밀번호2'])

        con = sqlite3.connect(DB_STOCK_TICK)
        df = pd.read_sql("SELECT name FROM sqlite_master WHERE TYPE = 'table'", con)
        con.close()
        table_list = list(df['name'].values)

        df = []
        df2 = self.xaq.BlockRequest("t8430", gubun=2)
        df2.rename(columns={'shcode': 'index', 'hname': '종목명', 'jnilclose': '전일종가'}, inplace=True)
        df2 = df2.set_index('index')
        df.append(df2)

        self.list_kosd = list(df2.index)

        df2 = self.xaq.BlockRequest("t8430", gubun=1)
        df2.rename(columns={'shcode': 'index', 'hname': '종목명', 'jnilclose': '전일종가'}, inplace=True)
        df2 = df2.set_index('index')
        df.append(df2)

        df = pd.concat(df)
        df[['전일종가']] = df[['전일종가']].astype(int)
        self.df_pc = df[['전일종가']].copy()
        df = df[['종목명']].copy()

        for code in list(df.index):
            name = df['종목명'][code]
            self.dict_name[code] = name
            self.dict_code[name] = code
            if code not in table_list:
                query = f'CREATE TABLE "{code}" ("index" TEXT, "현재가" REAL, "시가" REAL, "고가" REAL,' \
                         '"저가" REAL, "등락율" REAL, "당일거래대금" REAL, "체결강도" REAL, "초당매수수량" REAL,' \
                         '"초당매도수량" REAL, "VI해제시간" TEXT, "VI아래5호가" REAL, "매도총잔량" REAL, "매수총잔량" REAL,' \
                         '"매도호가5" REAL, "매도호가4" REAL, "매도호가3" REAL, "매도호가2" REAL, "매도호가1" REAL,' \
                         '"매수호가1" REAL, "매수호가2" REAL, "매수호가3" REAL, "매수호가4" REAL, "매수호가5" REAL,' \
                         '"매도잔량5" REAL, "매도잔량4" REAL, "매도잔량3" REAL, "매도잔량2" REAL, "매도잔량1" REAL,' \
                         '"매수잔량1" REAL, "매수잔량2" REAL, "매수잔량3" REAL, "매수잔량4" REAL, "매수잔량5" REAL)'
                self.query2Q.put([1, query])
                query = f'CREATE INDEX "ix_{code}_index" ON "{code}"("index")'
                self.query2Q.put([1, query])
        self.query2Q.put('주식디비트리거시작')
        self.query1Q.put([1, df, 'codename', 'replace'])

        try:
            df = self.xaq.BlockRequest('t1866', user_id=self.dict_set['아이디2'], gb='2', group_name='STOM')
            self.list_cond = [[df.index[0], df['query_name'][0]], [df.index[1], df['query_name'][1]]]
        except Exception as e:
            print(f'시스템 명령 오류 알림 - 조건검색식 불러오기 실패 {e}')
            print('HTS로 조건검색식을 두개 만들어 전략관리 메뉴에서 전략서버에 업로드해야합니다.')
            print('첫번째는 트레이더가 사용할 관심종목용이고')
            print('두번째는 리시버가 사용할 감시종목용입니다.')
            print('전략서버에 그룹명을 반드시 STOM으로 생성하십시오.')
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - OpenAPI 로그인 완료'])

    def UpdateJangolist(self, data):
        if '잔고편입' in data or '잔고청산' in data:
            code = data.split(' ')[1]
            if '잔고편입' in data and code not in self.list_jang:
                self.list_jang.append(code)
                if code not in self.list_gsjm2:
                    self.sstgQ.put(['조건진입', code])
                    self.list_gsjm2.append(code)
            elif '잔고청산' in data and code in self.list_jang:
                self.list_jang.remove(code)
                if code not in self.list_gsjm1 and code in self.list_gsjm2:
                    self.sstgQ.put(['조건이탈', code])
                    self.list_gsjm2.remove(code)
        else:
            self.hoga_code = data

    def UpdateDictset(self, data):
        self.dict_set = data

    def Scheduler(self):
        if not self.dict_bool['리시버시작']:
            self.OperationRealreg()
        if self.operation == 1 and now() > self.dict_time['휴무종료']:
            self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 리시버 종료'])
            sys.exit()
        if self.operation == 21:
            if int(strf_time('%H%M%S')) < 100000:
                if not self.dict_bool['실시간조건검색시작']:
                    self.ConditionSearchStart()
            if 100000 <= int(strf_time('%H%M%S')):
                if self.dict_bool['실시간조건검색시작'] and not self.dict_bool['실시간조건검색중단']:
                    self.ConditionSearchStop()
                if not self.dict_bool['장중단타전략시작']:
                    self.StartJangjungStrategy()
        if self.operation == 41 and int(strf_time('%H%M%S')) >= 153500:
            self.RemoveAllRealreg()
            self.SaveTickData()
            self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 리시버 종료'])
            sys.exit()

        if now() > self.dict_time['거래대금순위기록']:
            if len(self.list_gsjm1) > 0:
                self.UpdateMoneyTop()
            self.dict_time['거래대금순위기록'] = timedelta_sec(1)

    def OperationRealreg(self):
        self.dict_bool['리시버시작'] = True
        self.xar_op.AddRealData('0')
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 장운영시간 등록 완료'])

        self.xar_vi.AddRealData('000000')
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - VI발동/해제 등록 완료'])

        df = self.xaq.BlockRequest('t1857', sRealFlag='0', sSearchFlag='S', query_index=self.list_cond[1][0])
        codes = list(df['shcode'].values)
        del codes[0]
        self.list_code = codes
        self.list_code1 = [x for i, x in enumerate(self.list_code) if i % 4 == 0]
        self.list_code2 = [x for i, x in enumerate(self.list_code) if i % 4 == 1]
        self.list_code3 = [x for i, x in enumerate(self.list_code) if i % 4 == 2]
        self.list_code4 = [x for i, x in enumerate(self.list_code) if i % 4 == 3]
        for code in self.list_code:
            if code not in self.list_kosd:
                self.xar_cp.AddRealData(code)
                self.xar_hp.AddRealData(code)
            else:
                self.xar_cd.AddRealData(code)
                self.xar_hd.AddRealData(code)
        self.windowQ.put([ui_num['S단순텍스트'], f'시스템 명령 실행 알림 - 실시간 등록 완료 종목개수[{len(self.list_code)}]'])
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 리시버 시작'])

    def ConditionSearchStart(self):
        self.dict_bool['실시간조건검색시작'] = True
        df = self.xaq.BlockRequest('t1857', sRealFlag='1', sSearchFlag='S', query_index=self.list_cond[0][0])
        self.list_alertnum.append(df['AlertNum'].iloc[0])
        codes = list(df['shcode'].values)
        del codes[0]
        if len(codes) > 0:
            for code in codes:
                self.InsertGsjmlist(code)
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 실시간조건검색 등록 완료'])

    def ConditionSearchStop(self):
        self.dict_bool['실시간조건검색중단'] = True
        if len(self.list_alertnum) > 0:
            for alertnum in self.list_alertnum:
                self.xaq.RemoveService(alertnum)
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 실시간조건검색 중단 완료'])

    def StartJangjungStrategy(self):
        self.dict_bool['장중단타전략시작'] = True
        self.df_mc.sort_values(by=['최근거래대금'], ascending=False, inplace=True)
        list_top = list(self.df_mc.index[:self.dict_set['주식순위선정']])
        insert_list = set(list_top) - set(self.list_gsjm1)
        if len(insert_list) > 0:
            for code in list(insert_list):
                self.InsertGsjmlist(code)
        delete_list = set(self.list_gsjm1) - set(list_top)
        if len(delete_list) > 0:
            for code in list(delete_list):
                self.DeleteGsjmlist(code)
        self.list_prmt = list_top
        self.qtimer2.start()
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 장중 단타 전략 시작'])

    def RemoveAllRealreg(self):
        self.xar_op.RemoveAllRealData()
        self.xar_vi.RemoveAllRealData()
        self.xar_cp.RemoveAllRealData()
        self.xar_hp.RemoveAllRealData()
        self.xar_cd.RemoveAllRealData()
        self.xar_hd.RemoveAllRealData()
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 실시간 데이터 중단 완료'])

    def SaveTickData(self):
        con = sqlite3.connect(DB_TRADELIST)
        df = pd.read_sql(f"SELECT * FROM s_tradelist WHERE 체결시간 LIKE '{self.str_tday}%'", con).set_index('index')
        con.close()
        codes = []
        for index in df.index:
            code = self.dict_code[df['종목명'][index]]
            if code not in codes:
                codes.append(code)
        self.tick1Q.put(['콜렉터종료', codes])
        self.tick2Q.put(['콜렉터종료', codes])
        self.tick3Q.put(['콜렉터종료', codes])
        self.tick4Q.put(['콜렉터종료', codes])

    def UpdateMoneyTop(self):
        timetype = '%Y%m%d%H%M%S'
        list_text = ';'.join(self.list_gsjm1)
        curr_strtime = self.str_jcct
        curr_datetime = strp_time(timetype, curr_strtime)
        if self.dt_mtct is not None:
            gap_seconds = (curr_datetime - self.dt_mtct).total_seconds()
            while gap_seconds > 1:
                gap_seconds -= 1
                pre_time = strf_time(timetype, timedelta_sec(-gap_seconds, curr_datetime))
                self.df_mt.at[pre_time] = list_text
        if curr_datetime != self.dt_mtct:
            self.df_mt.at[curr_strtime] = list_text
            self.dt_mtct = curr_datetime

        if now() > self.dict_time['거래대금순위저장']:
            self.query2Q.put([1, self.df_mt, 'moneytop', 'append'])
            self.df_mt = pd.DataFrame(columns=['거래대금순위'])
            self.dict_time['거래대금순위저장'] = timedelta_sec(10)

    def MoneyTopSearch(self):
        self.df_mc.sort_values(by=['최근거래대금'], ascending=False, inplace=True)
        list_top = list(self.df_mc.index[:self.dict_set['주식순위선정']])
        insert_list = set(list_top) - set(self.list_prmt)
        if len(insert_list) > 0:
            for code in list(insert_list):
                self.InsertGsjmlist(code)
        delete_list = set(self.list_prmt) - set(list_top)
        if len(delete_list) > 0:
            for code in list(delete_list):
                self.DeleteGsjmlist(code)
        self.list_prmt = list_top

    def InsertGsjmlist(self, code):
        if code not in self.list_gsjm1:
            self.list_gsjm1.append(code)
        if code not in self.list_jang and code not in self.list_gsjm2:
            if self.dict_set['주식트레이더']:
                self.sstgQ.put(['조건진입', code])
            self.list_gsjm2.append(code)

    def DeleteGsjmlist(self, code):
        if code in self.list_gsjm1:
            self.list_gsjm1.remove(code)
        if code not in self.list_jang and code in self.list_gsjm2:
            if self.dict_set['주식트레이더']:
                self.sstgQ.put(['조건이탈', code])
            self.list_gsjm2.remove(code)

    def OnReceiveOperData(self, data):
        try:
            gubun = data['jangubun']
            status = int(data['jstatus'])
        except Exception as e:
            self.windowQ.put([ui_num['S단순텍스트'], f'OnReceiveOperData {e}'])
        else:
            if gubun == '1':
                self.operation = status
                self.windowQ.put([ui_num['S단순텍스트'], f'장운영 시간 수신 알림 - {dict_oper[status]}'])

    def OnReceiveVIData(self, data):
        try:
            code = data['ref_shcode']
            gubun = data['vi_gubun']
            name = self.dict_name[code]
        except Exception as e:
            self.windowQ.put([ui_num['S단순텍스트'], f'OnReceiveVIData VI발동/해제 {e}'])
        else:
            if gubun == '1' and code in self.list_code and \
                    (code not in self.dict_vipr.keys() or
                     (self.dict_vipr[code][0] and now() > self.dict_vipr[code][1])):
                self.UpdateViPrice(code, name)

    def OnReceiveSearchRealData(self, data):
        if int(strf_time('%H%M%S')) > 100000:
            return

        code = data['code']
        gubun = data['gubun']
        if gubun in ['N', 'R']:
            self.InsertGsjmlist(code)
        elif gubun == 'O':
            self.DeleteGsjmlist(code)

    def OnReceiveRealData(self, data):
        try:
            code = data['shcode']
            c = int(data['price'])
            o = int(data['open'])
            h = int(data['high'])
            low = int(data['low'])
            v = int(data['cvolume'])
            gubun = data['cgubun']
            per = float(data['drate'])
            dm = int(data['value'])
            ch = float(data['cpower'])
            dt = self.str_tday + data['chetime']
            name = self.dict_name[code]
        except Exception as e:
            self.windowQ.put([ui_num['S단순텍스트'], f'OnReceiveRealData {e}'])
        else:
            if self.operation == 1:
                self.operation = 21
            if dt != self.str_jcct and int(dt) > int(self.str_jcct):
                self.str_jcct = dt

            if code not in self.dict_vipr.keys():
                self.InsertViPrice(code, o)
            elif not self.dict_vipr[code][0] and now() > self.dict_vipr[code][1]:
                self.UpdateViPrice(code, c)

            try:
                predt, bid_volumns, ask_volumns = self.dict_tick[code]
            except KeyError:
                predt, bid_volumns, ask_volumns = None, 0, 0
            if gubun == '+':
                self.dict_tick[code] = [dt, bid_volumns + v, ask_volumns]
            elif gubun == '-':
                self.dict_tick[code] = [dt, bid_volumns, ask_volumns + v]
                v = -v

            if self.hoga_code == code:
                self.hogaQ.put([code, c, per, self.dict_vipr[code][2], o, h, low])
                self.hogaQ.put([code, v, ch])

            if dt != predt:
                try:
                    bids, asks = self.dict_tick[code][1:]
                except KeyError:
                    bids, asks = 0, 0
                self.dict_tick[code] = [dt, 0, 0]
                if code in self.dict_hoga.keys():
                    self.UpdateTickData(code, name, c, o, h, low, per, dm, ch, bids, asks, dt, now())

    def OnReceiveHogaData(self, data):
        try:
            code = data['shcode']
            tsjr, tbjr = int(data['totofferrem']), int(data['totbidrem'])
            s5hg, b5hg, s5jr, b5jr = int(data['offerho5']), int(data['bidho5']), int(data['offerrem5']), int(data['bidrem5'])
            s4hg, b4hg, s4jr, b4jr = int(data['offerho4']), int(data['bidho4']), int(data['offerrem4']), int(data['bidrem4'])
            s3hg, b3hg, s3jr, b3jr = int(data['offerho3']), int(data['bidho3']), int(data['offerrem3']), int(data['bidrem3'])
            s2hg, b2hg, s2jr, b2jr = int(data['offerho2']), int(data['bidho2']), int(data['offerrem2']), int(data['bidrem2'])
            s1hg, b1hg, s1jr, b1jr = int(data['offerho1']), int(data['bidho1']), int(data['offerrem1']), int(data['bidrem1'])
        except Exception as e:
            self.windowQ.put([ui_num['S단순텍스트'], f'OnReceiveRealData 주식호가잔량 {e}'])
        else:
            self.dict_hoga[code] = [tsjr, tbjr,
                                    s5hg, s4hg, s3hg, s2hg, s1hg, b1hg, b2hg, b3hg, b4hg, b5hg,
                                    s5jr, s4jr, s3jr, s2jr, s1jr, b1jr, b2jr, b3jr, b4jr, b5jr]

            if self.hoga_code == code:
                if code not in self.dict_sghg.keys():
                    shg, hhg = self.GetSangHahanga(code)
                    self.dict_sghg[code] = [shg, hhg]
                self.hogaQ.put([code] + self.dict_hoga[code] + self.dict_sghg[code])

    def InsertViPrice(self, code, o):
        uvi, dvi, vid5price = self.GetVIPrice(code, o)
        self.dict_vipr[code] = [True, timedelta_sec(-3600), uvi, dvi, vid5price]

    def GetVIPrice(self, code, std_price):
        uvi = std_price * 1.1
        x = self.GetHogaunit(code, uvi)
        if uvi % x != 0:
            uvi = uvi + (x - uvi % x)
        vid5price = uvi - x * 5
        dvi = std_price * 0.9
        x = self.GetHogaunit(code, dvi)
        if dvi % x != 0:
            dvi = dvi - dvi % x
        return int(uvi), int(dvi), int(vid5price)

    def GetHogaunit(self, code, price):
        if price < 1000:
            x = 1
        elif 1000 <= price < 5000:
            x = 5
        elif 5000 <= price < 10000:
            x = 10
        elif 10000 <= price < 50000:
            x = 50
        elif code in self.list_kosd:
            x = 100
        elif 50000 <= price < 100000:
            x = 100
        elif 100000 <= price < 500000:
            x = 500
        else:
            x = 1000
        return x

    def UpdateViPrice(self, code, key):
        if type(key) == str:
            try:
                self.dict_vipr[code][:2] = False, timedelta_sec(5)
            except KeyError:
                self.dict_vipr[code] = [False, timedelta_sec(5), 0, 0, 0]
            self.windowQ.put([ui_num['S로그텍스트'], f'변동성 완화 장치 발동 - [{code}] {key}'])
        elif type(key) == int:
            uvi, dvi, vid5price = self.GetVIPrice(code, key)
            self.dict_vipr[code] = [True, timedelta_sec(5), uvi, dvi, vid5price]

    def UpdateTickData(self, code, name, c, o, h, low, per, dm, ch, bids, asks, dt, receivetime):
        dt_ = dt[:13]
        if code not in self.dict_cdjm.keys():
            columns = ['10초누적거래대금', '10초전당일거래대금']
            self.dict_cdjm[code] = pd.DataFrame([[0, dm]], columns=columns, index=[dt_])
        elif dt_ != self.dict_cdjm[code].index[-1]:
            predm = self.dict_cdjm[code]['10초전당일거래대금'][-1]
            self.dict_cdjm[code].at[dt_] = dm - predm, dm
            if len(self.dict_cdjm[code]) == self.dict_set['주식순위시간'] * 6:
                if per > 0:
                    self.df_mc.at[code] = self.dict_cdjm[code]['10초누적거래대금'].sum()
                elif code in self.df_mc.index:
                    self.df_mc.drop(index=code, inplace=True)
                self.dict_cdjm[code].drop(index=self.dict_cdjm[code].index[0], inplace=True)

        vitime = self.dict_vipr[code][1]
        vid5price = self.dict_vipr[code][4]
        data = [c, o, h, low, per, dm, ch, bids, asks, vitime, vid5price]
        data += self.dict_hoga[code] + [code, dt, receivetime]

        if self.dict_set['주식트레이더'] and code in self.list_gsjm2:
            injango = code in self.list_jang
            self.sstgQ.put(data + [name, injango])
            if injango:
                self.stockQ.put([code, name, c])

        if self.dict_set['주식콜렉터']:
            data[9] = strf_time('%Y%m%d%H%M%S', vitime)
            if code in self.list_code1:
                self.tick1Q.put(data)
            elif code in self.list_code2:
                self.tick2Q.put(data)
            elif code in self.list_code3:
                self.tick3Q.put(data)
            elif code in self.list_code4:
                self.tick4Q.put(data)

    def GetSangHahanga(self, code):
        predayclose = self.df_pc['전일종가'][code]
        uplimitprice = predayclose * 1.30
        x = self.GetHogaunit(code, uplimitprice)
        if uplimitprice % x != 0:
            uplimitprice -= uplimitprice % x
        downlimitprice = predayclose * 0.70
        x = self.GetHogaunit(code, downlimitprice)
        if downlimitprice % x != 0:
            downlimitprice += x - downlimitprice % x
        return int(uplimitprice), int(downlimitprice)
