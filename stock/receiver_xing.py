import os
import sys
import time
import sqlite3
from PyQt5 import QtWidgets
from PyQt5.QtCore import QTimer

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.xing import *
from utility.static import now, strf_time, strp_time, timedelta_sec
from utility.setting import ui_num, DICT_SET, DB_TRADELIST, DB_STOCK_TICK

MONEYTOP_MINUTE = 10  # 최근거래대금순위을 집계할 시간
MONEYTOP_RANK = 20  # 최근거래대금순위중 관심종목으로 선정할 순위


class ReceiverXing:
    app = QtWidgets.QApplication(sys.argv)

    def __init__(self, qlist):
        """
                    0        1       2        3       4       5          6        7      8      9     10
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceivQ, stockQ, coinQ, sstgQ, cstgQ,
                 tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, wsk1Q, wsk2Q]
                   11       12      13     14      15     16     17
        """
        self.windowQ = qlist[0]
        self.query2Q = qlist[3]
        self.sreceivQ = qlist[5]
        self.stockQ = qlist[7]
        self.sstgQ = qlist[9]
        self.tick1Q = qlist[11]
        self.tick2Q = qlist[12]
        self.tick3Q = qlist[13]
        self.tick4Q = qlist[14]

        self.dict_bool = {
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

        self.list_gsjm1 = []
        self.list_gsjm2 = []
        self.list_jang = []
        self.list_cond = None
        self.list_prmt = None
        self.list_kosd = None
        self.list_code = None
        self.list_code1 = None
        self.list_code2 = None
        self.list_code3 = None
        self.list_code4 = None

        self.df_mt = pd.DataFrame(columns=['거래대금순위'])
        self.df_mc = pd.DataFrame(columns=['최근거래대금'])
        self.operation = 1
        self.str_tday = strf_time('%Y%m%d')
        self.str_jcct = self.str_tday + '090000'
        self.time_mcct = None

        remaintime = (strp_time('%Y%m%d%H%M%S', self.str_tday + '090100') - now()).total_seconds()
        self.dict_time = {
            '휴무종료': timedelta_sec(remaintime) if remaintime > 0 else timedelta_sec(600),
            '거래대금순위기록': now(),
            '거래대금순위저장': now()
        }

        self.timer = QTimer()
        self.timer.setInterval(10000)
        self.timer.timeout.connect(self.ConditionSearch)

        self.xas = XASession()
        self.xaq = XAQuery()

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

        self.Start()

    def Start(self):
        self.XingLogin()
        self.EventLoop()

    def XingLogin(self):
        self.xas.Login(DICT_SET['아이디2'], DICT_SET['비밀번호2'], DICT_SET['인증서비밀번호2'])

        con = sqlite3.connect(DB_STOCK_TICK)
        df = pd.read_sql("SELECT name FROM sqlite_master WHERE TYPE = 'table'", con)
        con.close()
        table_list = list(df['name'].values)

        df = []
        df2 = self.xaq.BlockRequest("t8430", gubun=2)
        df2.rename(columns={'shcode': 'index', 'hname': '종목명'}, inplace=True)
        df2 = df2.set_index('index')
        self.list_kosd = list(df2.index)
        df.append(df2)

        df2 = self.xaq.BlockRequest("t8430", gubun=1)
        df2.rename(columns={'shcode': 'index', 'hname': '종목명'}, inplace=True)
        df2 = df2.set_index('index')
        df.append(df2)
        df = pd.concat(df)
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
                         '"매수잔량1" REAL, "매수잔량2" REAL, "매수잔량3" REAL, "매수잔량4" REAL, "매수잔량5" REAL);'
                self.query2Q.put([1, query])
                query = f'CREATE INDEX "ix_{code}_index" ON "{code}"("index");'
                self.query2Q.put([1, query])
        self.query2Q.put([1, df, 'codename', 'replace'])
        self.query2Q.put('주식디비트리거시작')

        df = self.xaq.BlockRequest('t1866', user_id=DICT_SET['아이디2'], gb='2', group_name='STOM')
        if len(df) >= 2:
            self.list_cond = [[df.index[0], df['query_name'][0]], [df.index[1], df['query_name'][1]]]
        else:
            self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 오류 알림 - 조건검색식 불러오기 실패'])
            self.windowQ.put([ui_num['S단순텍스트'], 'HTS로 조건검색식을 만들어 서버에 업로드해야하여 그룹명은 STOM으로 설정하십시오.'])
            self.windowQ.put([ui_num['S단순텍스트'], '조건검색식은 두개가 필요하며 첫번째는 트레이더 및 전략연산이 사용할 관심종목용이고'])
            self.windowQ.put([ui_num['S단순텍스트'], '두번째는 리시버 및 콜렉터가 사용할 틱데이터 수집용입니다.'])
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - OpenAPI 로그인 완료'])

    def EventLoop(self):
        self.OperationRealreg()
        while True:
            if not self.sreceivQ.empty():
                data = self.sreceivQ.get()
                if type(data) == list:
                    self.UpdateRealreg(data)
                elif type(data) == str:
                    self.UpdateJangolist(data)
                continue

            if self.operation == 1 and now() > self.dict_time['휴무종료']:
                break
            if self.operation == 21:
                if int(strf_time('%H%M%S')) < 100000:
                    if not self.dict_bool['실시간조건검색시작']:
                        self.ConditionSearchStart()
                if 100000 <= int(strf_time('%H%M%S')):
                    if self.dict_bool['실시간조건검색시작'] and not self.dict_bool['실시간조건검색중단']:
                        self.ConditionSearchStop()
                    if not self.dict_bool['장중단타전략시작']:
                        self.StartJangjungStrategy()
            if self.operation == 41:
                if int(strf_time('%H%M%S')) >= 153500:
                    self.RemoveAllRealreg()
                    self.SaveTickData()
                    break

            if now() > self.dict_time['거래대금순위기록']:
                if len(self.list_gsjm1) > 0:
                    self.UpdateMoneyTop()
                self.dict_time['거래대금순위기록'] = timedelta_sec(1)

            time_loop = timedelta_sec(0.25)
            while now() < time_loop:
                pythoncom.PumpWaitingMessages()
                time.sleep(0.0001)

        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 리시버 종료'])

    def UpdateRealreg(self, rreg):
        gubun = rreg[0]
        code = rreg[1]
        if gubun == 'AddReal':
            if code not in self.list_kosd:
                self.xar_cp.AddRealData(code)
                self.xar_hp.AddRealData(code)
            else:
                self.xar_cd.AddRealData(code)
                self.xar_hd.AddRealData(code)
        elif gubun == 'RemoveAllReal':
            self.xar_cp.RemoveAllRealData()
            self.xar_hp.RemoveAllRealData()
            self.xar_cd.RemoveAllRealData()
            self.xar_hd.RemoveAllRealData()

    def UpdateJangolist(self, data):
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

    def OperationRealreg(self):
        self.xar_op.AddRealData()
        self.xar_vi.AddRealData('000000')
        codes = self.xaq.BlockRequest('t1857', sRealFlag='0', sSearchFlag='S', query_index=self.list_cond[1][0])
        self.list_code = codes
        self.list_code1 = [x for i, x in enumerate(self.list_code) if i % 4 == 0]
        self.list_code2 = [x for i, x in enumerate(self.list_code) if i % 4 == 1]
        self.list_code3 = [x for i, x in enumerate(self.list_code) if i % 4 == 2]
        self.list_code4 = [x for i, x in enumerate(self.list_code) if i % 4 == 3]

        for code in self.list_code:
            self.sreceivQ.put(['AddReal', code])
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 장운영시간 등록 완료'])
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 전종목 실시간 등록 완료'])

    def ConditionSearchStart(self):
        self.dict_bool['실시간조건검색시작'] = True
        codes = self.xaq.BlockRequest('t1857', sRealFlag='1', sSearchFlag='S', query_index=self.list_cond[0][0])
        if len(codes) > 0:
            for code in codes:
                self.InsertGsjmlist(code)
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 실시간조건검색 등록 완료'])

    def ConditionSearchStop(self):
        self.dict_bool['실시간조건검색중단'] = True
        self.xaq.RemoveService()
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 실시간조건검색 중단 완료'])

    def StartJangjungStrategy(self):
        self.dict_bool['장중단타전략시작'] = True
        self.df_mc.sort_values(by=['최근거래대금'], ascending=False, inplace=True)
        list_top = list(self.df_mc.index[:MONEYTOP_RANK])
        insert_list = set(list_top) - set(self.list_gsjm1)
        if len(insert_list) > 0:
            for code in list(insert_list):
                self.InsertGsjmlist(code)
        delete_list = set(self.list_gsjm1) - set(list_top)
        if len(delete_list) > 0:
            for code in list(delete_list):
                self.DeleteGsjmlist(code)
        self.list_prmt = list_top
        self.timer.start()

    def ConditionSearch(self):
        self.df_mc.sort_values(by=['최근거래대금'], ascending=False, inplace=True)
        list_top = list(self.df_mc.index[:MONEYTOP_RANK])
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
            if DICT_SET['이베스트트레이더']:
                self.sstgQ.put(['조건진입', code])
            self.list_gsjm2.append(code)

    def DeleteGsjmlist(self, code):
        if code in self.list_gsjm1:
            self.list_gsjm1.remove(code)
        if code not in self.list_jang and code in self.list_gsjm2:
            if DICT_SET['이베스트트레이더']:
                self.sstgQ.put(['조건이탈', code])
            self.list_gsjm2.remove(code)

    def RemoveAllRealreg(self):
        self.sreceivQ.put(['RemoveAllReal'])
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
        curr_strftime = self.str_jcct
        curr_datetime = strp_time(timetype, curr_strftime)
        if self.time_mcct is not None:
            gap_seconds = (curr_datetime - self.time_mcct).total_seconds()
            while gap_seconds > 1:
                gap_seconds -= 1
                pre_time = strf_time(timetype, timedelta_sec(-gap_seconds, curr_datetime))
                self.df_mt.at[pre_time] = list_text
        self.df_mt.at[curr_strftime] = list_text
        self.time_mcct = curr_datetime

        if now() > self.dict_time['거래대금순위저장']:
            self.query2Q.put([1, self.df_mt, 'moneytop', 'append'])
            self.df_mt = pd.DataFrame(columns=['거래대금순위'])
            self.dict_time['거래대금순위저장'] = timedelta_sec(10)

    def OnReceiveOperData(self, data):
        dict_oper = {
            25: '장시작 10분전전입니다.',
            24: '장시작 5분전전입니다.',
            23: '장시작 1분전전입니다.',
            22: '장시작 10초전입니다.',
            21: '장시작',
            44: '장마감 5분전전입니다.',
            43: '장마감 1분전전입니다.',
            42: '장마감 10초전전입니다.',
            41: '장마감'
        }
        try:
            gubun = int(data['jangubun'])
            status = int(data['jstatus'])
        except Exception as e:
            self.windowQ.put([ui_num['S단순텍스트'], f'OnReceiveOperData {e}'])
        else:
            if gubun == 1:
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
            print(code, name, gubun)
            if gubun == '1' and code in self.list_code and \
                    (code not in self.dict_vipr.keys() or
                     (self.dict_vipr[code][0] and now() > self.dict_vipr[code][1])):
                self.UpdateViPrice(code, name)

    def OnReceiveSearchRealData(self, field):
        pass
        """
        TODO 실시간 조건검색식 진입 및 이탈 확인 후 처리
        """

    def OnReceiveRealData(self, data):
        try:
            code = data['shcode']
            c = int(data['price'])
            o = int(data['open'])
            v = int(data['cvolume'])
            gubun = data['cgubun']
            dt = self.str_tday + data['chetime']
        except Exception as e:
            self.windowQ.put([ui_num['S단순텍스트'], f'OnReceiveRealData {e}'])
        else:
            if self.operation == 1:
                self.operation = 21
            if dt != self.str_jcct and int(dt) > int(self.str_jcct):
                self.str_jcct = dt
            if code not in self.dict_vipr.keys():
                self.InsertViPrice(code, o)
            if code in self.dict_vipr.keys() and not self.dict_vipr[code][0] and now() > self.dict_vipr[code][1]:
                self.UpdateViPrice(code, c)
            try:
                predt = self.dict_tick[code][0]
                bid_volumns = self.dict_tick[code][1]
                ask_volumns = self.dict_tick[code][2]
            except KeyError:
                predt = None
                bid_volumns = 0
                ask_volumns = 0
            if gubun == '+':
                self.dict_tick[code] = [dt, bid_volumns + v, ask_volumns]
            else:
                self.dict_tick[code] = [dt, bid_volumns, ask_volumns + v]
            if dt != predt:
                bids = self.dict_tick[code][1]
                asks = self.dict_tick[code][2]
                self.dict_tick[code] = [dt, 0, 0]
                try:
                    h = int(data['high'])
                    low = int(data['low'])
                    per = float(data['drate'])
                    dm = int(data['value'])
                    ch = float(data['cpower'])
                    name = self.dict_name[code]
                except Exception as e:
                    self.windowQ.put([ui_num['S단순텍스트'], f'OnReceiveRealData {e}'])
                else:
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
            if len(self.dict_cdjm[code]) == MONEYTOP_MINUTE * 6:
                if per > 0:
                    self.df_mc.at[code] = self.dict_cdjm[code]['10초누적거래대금'].sum()
                elif code in self.df_mc.index:
                    self.df_mc.drop(index=code, inplace=True)
                self.dict_cdjm[code].drop(index=self.dict_cdjm[code].index[0], inplace=True)

        vitime = self.dict_vipr[code][1]
        vid5price = self.dict_vipr[code][4]
        data = [c, o, h, low, per, dm, ch, bids, asks, vitime, vid5price]
        data += self.dict_hoga[code] + [code, dt, receivetime]

        if DICT_SET['이베스트트레이더'] and code in self.list_gsjm2:
            injango = code in self.list_jang
            self.sstgQ.put(data + [name, injango])
            if injango:
                self.stockQ.put([code, name, c])

        data[9] = strf_time('%Y%m%d%H%M%S', vitime)
        if code in self.list_code1:
            self.tick1Q.put(data)
        elif code in self.list_code2:
            self.tick2Q.put(data)
        elif code in self.list_code3:
            self.tick3Q.put(data)
        elif code in self.list_code4:
            self.tick4Q.put(data)
