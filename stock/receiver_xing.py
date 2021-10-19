import os
import sys
import time
import sqlite3
import pythoncom
import pandas as pd
import win32com.client
from PyQt5 import QtWidgets
from PyQt5.QtCore import QTimer
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.static import now, strf_time, strp_time, timedelta_sec, parse_res
from utility.setting import ui_num, DICT_SET, DB_TRADELIST, E_OPENAPI_PATH

USER_ID = ''
PASSWORD = ''
CERT_PASS = ''

MONEYTOP_MINUTE = 10        # 최근거래대금순위을 집계할 시간
MONEYTOP_RANK = 20          # 최근거래대금순위중 관심종목으로 선정할 순위


class XASession:
    def __init__(self):
        self.com_obj = win32com.client.Dispatch("XA_Session.XASession")
        win32com.client.WithEvents(self.com_obj, XASessionEvents).connect(self)
        self.connected = False

    def Login(self, user_id, password, cert):
        self.com_obj.ConnectServer('hts.ebestsec.co.kr', 20001)
        self.com_obj.Login(user_id, password, cert, 0, 0)
        while not self.connected:
            pythoncom.PumpWaitingMessages()
        print('XASession 로그인 완료')


class XAQuery:
    def __init__(self):
        self.com_obj = win32com.client.Dispatch("XA_DataSet.XAQuery")
        win32com.client.WithEvents(self.com_obj, XAQueryEvents).connect(self)
        self.received = False

    def BlockRequest(self, *args, **kwargs):
        self.received = False
        res_name = args[0]
        res_file = res_name + '.res'
        res_path = E_OPENAPI_PATH + res_file
        self.com_obj.ResFileName = res_path
        with open(res_path, encoding='euc-kr') as f:
            res_lines = f.readlines()
        res_data = parse_res(res_lines)
        inblock_code = list(res_data['inblock'][0].keys())[0]
        inblock_field = list(res_data['inblock'][0].values())[0]
        for k in kwargs:
            self.com_obj.SetFieldData(inblock_code, k, 0, kwargs[k])
            if k not in inblock_field:
                print('inblock field error')
        self.com_obj.Request(False)
        while not self.received:
            pythoncom.PumpWaitingMessages()
        ret = []
        for outblock in res_data['outblock']:
            outblock_code = list(outblock.keys())[0]
            outblock_field = list(outblock.values())[0]
            data = []
            rows = self.com_obj.GetBlockCount(outblock_code)
            for i in range(rows):
                elem = {k: self.com_obj.GetFieldData(outblock_code, k, i) for k in outblock_field}
                print(elem)
                data.append(elem)
            df = pd.DataFrame(data=data)
            ret.append(df)
        return ret


class XAReal:
    def __init__(self):
        self.com_obj = win32com.client.Dispatch("XA_DataSet.XAReal")
        win32com.client.WithEvents(self.com_obj, XARealEvents).connect(self)
        self.res = {}

    def RegisterResReal(self, res_file):
        res_path = E_OPENAPI_PATH + res_file
        self.com_obj.ResFileName = res_path

    def AddRealData(self, field, code):
        self.com_obj.SetFieldData('InBlock', field, code)
        self.com_obj.AdviseRealData()

    def RemoveRealData(self, code):
        self.com_obj.UnadviseRealDataWithKey(code)

    def RemoveAllRealData(self):
        self.com_obj.UnadviseRealData()


class XASessionEvents:
    def __init__(self):
        self.user_obj = None

    def connect(self, user_obj):
        self.user_obj = user_obj

    def OnLogin(self, code, msg):
        if code == '0000':
            self.user_obj.connected = True


class XAQueryEvents:
    def __init__(self):
        self.user_obj = None

    def connect(self, user_obj):
        self.user_obj = user_obj

    def OnReceiveData(self, code):
        self.user_obj.received = True


class XARealEvents:
    def __init__(self):
        self.user_obj = None

    def connect(self, user_obj):
        self.user_obj = user_obj

    def OnReceiveRealData(self, trcode):
        res_data = self.user_obj.res.get(trcode)
        out_data = {}
        out_block = res_data['outblock'][0]
        for field in out_block['OutBlock']:
            data = self.user_obj.get_field_data(field)
            out_data[field] = data
        out_data_list = [out_data]
        df = pd.DataFrame(data=out_data_list)
        self.user_obj.OnReceiveRealData((trcode, df))


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
        self.xa_real = XAReal()

        self.Start()

    def Start(self):
        self.XingLogin()
        self.EventLoop()

    def XingLogin(self):
        self.xa_session.Login(USER_ID, PASSWORD, CERT_PASS)

        df = []
        df2 = self.xa_query.BlockRequest("t8430", gubun=2)
        df2 = df2.rename(columns={'shcode': 'index'}).set_index('index')
        df.append(df2)

        self.list_kosd = list(df2.index)

        df2 = self.xa_query.BlockRequest("t8430", gubun=1)
        df2 = df2.rename(columns={'shcode': 'index'}).set_index('index')
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
        self.ViRealreg()
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
            if self.operation == 3:
                if int(strf_time('%H%M%S')) < 100000:
                    if not self.dict_bool['실시간조건검색시작']:
                        self.ConditionSearchStart()
                if 100000 <= int(strf_time('%H%M%S')):
                    if self.dict_bool['실시간조건검색시작'] and not self.dict_bool['실시간조건검색중단']:
                        self.ConditionSearchStop()
                    if not self.dict_bool['장중단타전략시작']:
                        self.StartJangjungStrategy()
            if self.operation == 8:
                self.AllRemoveRealreg()
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
        """
        sn = rreg[0]
        if len(rreg) == 2:
            self.ocx.dynamicCall('SetRealRemove(QString, QString)', rreg)
            self.windowQ.put([ui_num['S단순텍스트'], f'실시간 알림 중단 완료 - 모든 실시간 데이터 수신 중단'])
        elif len(rreg) == 4:
            ret = self.ocx.dynamicCall('SetRealReg(QString, QString, QString, QString)', rreg)
            result = '완료' if ret == 0 else '실패'
            if sn == sn_oper:
                self.windowQ.put([ui_num['S단순텍스트'], f'실시간 알림 등록 {result} - 장운영시간 [{sn}]'])
            else:
                text = f"실시간 알림 등록 {result} - [{sn}] 종목갯수 {len(rreg[1].split(';'))}"
                self.windowQ.put([ui_num['S단순텍스트'], text])
        """
        pass

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
        """
        self.sreceivQ.put([sn_oper, ' ', '215;20;214', 0])
        self.list_code = self.SendCondition(sn_oper, self.dict_cond[1], 1, 0)
        self.list_code1 = [code for i, code in enumerate(self.list_code) if i % 4 == 0]
        self.list_code2 = [code for i, code in enumerate(self.list_code) if i % 4 == 1]
        self.list_code3 = [code for i, code in enumerate(self.list_code) if i % 4 == 2]
        self.list_code4 = [code for i, code in enumerate(self.list_code) if i % 4 == 3]
        k = 0
        for i in range(0, len(self.list_code), 100):
            self.sreceivQ.put([sn_recv + k, ';'.join(self.list_code[i:i + 100]), '10;12;14;30;228;41;61;71;81', 1])
            k += 1
        """
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 장운영시간 등록 완료'])

    def ViRealreg(self):
        """
        self.Block_Request('opt10054', 시장구분='000', 장전구분='1', 종목코드='', 발동구분='1', 제외종목='111111011',
                           거래량구분='0', 거래대금구분='0', 발동방향='0', output='발동종목', next=0)
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - VI발동해제 등록 완료'])
        self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 콜렉터 시작 완료'])
        """
        pass

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

    def AllRemoveRealreg(self):
        self.sreceivQ.put(['ALL', 'ALL'])
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

        if DICT_SET['키움트레이더'] and code in self.list_gsjm2:
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
