import os
import sys
import time
import sqlite3
from PyQt5 import QtWidgets
from PyQt5.QtCore import QTimer
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.xing import *
from utility.static import now, strf_time, strp_time, timedelta_sec
from utility.setting import ui_num, DICT_SET, DB_TRADELIST

MONEYTOP_MINUTE = 10        # 최근거래대금순위을 집계할 시간
MONEYTOP_RANK = 20          # 최근거래대금순위중 관심종목으로 선정할 순위


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

            '로그인': False,
            'TR수신': False,
            'TR다음': False,
            'CD수신': False,
            'CR수신': False
        }
        self.dict_cdjm = {}
        self.dict_vipr = {}
        self.dict_tick = {}
        self.dict_hoga = {}
        self.dict_cond = {}
        self.dict_name = {}
        self.dict_code = {}

        self.list_gsjm = []
        self.list_gsjm2 = []
        self.list_trcd = []
        self.list_jang = []
        self.pre_top = []
        self.list_kosd = None
        self.list_kosp = None
        self.list_code = None
        self.list_code1 = None
        self.list_code2 = None
        self.list_code3 = None
        self.list_code4 = None

        self.operation = 1
        self.df_mt = pd.DataFrame(columns=['거래대금순위'])
        self.df_mc = pd.DataFrame(columns=['최근거래대금'])
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

        self.xa_session = XASession()
        self.xa_query = XAQuery()

        self.xa_real_op = XAReal()
        self.xa_real_vi = XAReal()
        self.xa_real_jcp = XAReal()
        self.xa_real_jcd = XAReal()
        self.xa_real_hgp = XAReal()
        self.xa_real_hgd = XAReal()

        self.xa_real_op.RegisterRes('JIF')
        self.xa_real_vi.RegisterRes('VI_')
        self.xa_real_jcp.RegisterRes('S3_')
        self.xa_real_jcd.RegisterRes('K3_')
        self.xa_real_hgp.RegisterRes('H1_')
        self.xa_real_hgd.RegisterRes('HA_')

        self.Start()

    def Start(self):
        self.XingLogin()
        self.EventLoop()

    def XingLogin(self):
        self.xa_session.Login(DICT_SET['아이디2'], DICT_SET['비밀번호2'], DICT_SET['인증서비밀번호2'])

        df = []
        df2 = self.xa_query.BlockRequest("t8430", gubun=2).set_index('shcode')
        df.append(df2)
        self.list_kosd = list(df2.index)
        df2 = self.xa_query.BlockRequest("t8430", gubun=1).set_index('shcode')
        self.list_kosp = list(df2.index)
        df.append(df2)
        df = pd.concat(df)

        for code in df.index:
            name = df['hname'][code]
            self.dict_name[code] = name
            self.dict_code[name] = code
        self.query2Q.put([1, df, 'codename', 'replace'])

        """
        self.dict_bool['CD수신'] = False
        self.ocx.dynamicCall('GetConditionLoad()')
        while not self.dict_bool['CD수신']:
            pythoncom.PumpWaitingMessages()

        data = self.ocx.dynamicCall('GetConditionNameList()')
        conditions = data.split(';')[:-1]
        for condition in conditions:
            cond_index, cond_name = condition.split('^')
            self.dict_cond[int(cond_index)] = cond_name
        """

        self.windowQ.put([ui_num['S단순텍스트'], self.dict_cond])
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
                if len(self.list_gsjm) > 0:
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
            self.xa_real_vi.AddRealData(code)
            if code not in self.list_kosd:
                self.xa_real_jcp.AddRealData(code)
                self.xa_real_hgp.AddRealData(code)
            else:
                self.xa_real_jcd.AddRealData(code)
                self.xa_real_hgd.AddRealData(code)
        elif gubun == 'RemoveAllReal':
            self.xa_real_vi.RemoveAllRealData()
            self.xa_real_jcp.RemoveAllRealData()
            self.xa_real_hgp.RemoveAllRealData()
            self.xa_real_jcd.RemoveAllRealData()
            self.xa_real_hgd.RemoveAllRealData()

    def UpdateJangolist(self, data):
        code = data.split(' ')[1]
        if '잔고편입' in data and code not in self.list_jang:
            self.list_jang.append(code)
            if code not in self.list_gsjm2:
                self.sstgQ.put(['조건진입', code])
                self.list_gsjm2.append(code)
        elif '잔고청산' in data and code in self.list_jang:
            self.list_jang.remove(code)
            if code not in self.list_gsjm and code in self.list_gsjm2:
                self.sstgQ.put(['조건이탈', code])
                self.list_gsjm2.remove(code)

    def OperationRealreg(self):
        self.xa_real_op.AddRealData('1')
        for code in self.list_kosp + self.list_kosd:
            self.sreceivQ.put(['AddReal', code])
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 장운영시간 등록 완료'])

    def ConditionSearchStart(self):
        """
        self.dict_bool['실시간조건검색시작'] = True
        codes = self.SendCondition(sn_cond, self.dict_cond[0], 0, 1)
        if len(codes) > 0:
            for code in codes:
                self.InsertGsjmlist(code)
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 실시간조건검색 등록 완료'])
        """
        pass

    def ConditionSearchStop(self):
        """
        self.dict_bool['실시간조건검색중단'] = True
        self.ocx.dynamicCall("SendConditionStop(QString, QString, int)", sn_cond, self.dict_cond[0], 0)
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 실시간조건검색 중단 완료'])
        """
        pass

    def StartJangjungStrategy(self):
        self.dict_bool['장중단타전략시작'] = True
        self.df_mc.sort_values(by=['최근거래대금'], ascending=False, inplace=True)
        list_top = list(self.df_mc.index[:MONEYTOP_RANK])
        insert_list = set(list_top) - set(self.list_gsjm)
        if len(insert_list) > 0:
            for code in list(insert_list):
                self.InsertGsjmlist(code)
        delete_list = set(self.list_gsjm) - set(list_top)
        if len(delete_list) > 0:
            for code in list(delete_list):
                self.DeleteGsjmlist(code)
        self.pre_top = list_top
        self.timer.start()

    def ConditionSearch(self):
        self.df_mc.sort_values(by=['최근거래대금'], ascending=False, inplace=True)
        list_top = list(self.df_mc.index[:MONEYTOP_RANK])
        insert_list = set(list_top) - set(self.pre_top)
        if len(insert_list) > 0:
            for code in list(insert_list):
                self.InsertGsjmlist(code)
        delete_list = set(self.pre_top) - set(list_top)
        if len(delete_list) > 0:
            for code in list(delete_list):
                self.DeleteGsjmlist(code)
        self.pre_top = list_top

    def InsertGsjmlist(self, code):
        if code not in self.list_gsjm:
            self.list_gsjm.append(code)
        if code not in self.list_jang and code not in self.list_gsjm2:
            if DICT_SET['키움트레이더']:
                self.sstgQ.put(['조건진입', code])
            self.list_gsjm2.append(code)

    def DeleteGsjmlist(self, code):
        if code in self.list_gsjm:
            self.list_gsjm.remove(code)
        if code not in self.list_jang and code in self.list_gsjm2:
            if DICT_SET['키움트레이더']:
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
        list_text = ';'.join(self.list_gsjm)
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
            25: '장개시 10분전',
            24: '장개시 5분전',
            23: '장개시 1분전',
            22: '장개시 10초전',
            21: '장시작',
            44: '장마감 5분전',
            43: '장마감 1분전',
            42: '장마감 10초전',
            41: '장마감'
        }
        try:
            status = int(data['jstatus'])
        except Exception as e:
            self.windowQ.put([ui_num['S단순텍스트'], f'OnReceiveOperData {e}'])
        else:
            self.operation = status
            self.windowQ.put([ui_num['S단순텍스트'], f'장운영 시간 수신 알림 - {dict_oper[status]}'])

    def OnReceiveVIData(self, data):
        try:
            code = data['shcode']
            gubun = data['vi_gubun']
            name = self.dict_name[code]
        except Exception as e:
            self.windowQ.put([ui_num['S단순텍스트'], f'OnReceiveVIData VI발동/해제 {e}'])
        else:
            if gubun == '1' and code in self.list_code and \
                    (code not in self.dict_vipr.keys() or
                     (self.dict_vipr[code][0] and now() > self.dict_vipr[code][1])):
                self.UpdateViPrice(code, name)

    def OnReceiveSearchRealData(self, field):
        pass

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
            tsjr = int(data['totofferrem'])
            tbjr = int(data['totbidrem'])
            s5hg = int(data['offerho5'])
            s4hg = int(data['offerho4'])
            s3hg = int(data['offerho3'])
            s2hg = int(data['offerho2'])
            s1hg = int(data['offerho1'])
            b1hg = int(data['bidho1'])
            b2hg = int(data['bidho2'])
            b3hg = int(data['bidho3'])
            b4hg = int(data['bidho4'])
            b5hg = int(data['bidho5'])
            s5jr = int(data['offerrem5'])
            s4jr = int(data['offerrem4'])
            s3jr = int(data['offerrem3'])
            s2jr = int(data['offerrem2'])
            s1jr = int(data['offerrem1'])
            b1jr = int(data['bidrem1'])
            b2jr = int(data['bidrem2'])
            b3jr = int(data['bidrem3'])
            b4jr = int(data['bidrem4'])
            b5jr = int(data['bidrem5'])
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
