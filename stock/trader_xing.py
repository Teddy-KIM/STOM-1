import os
import sys
import time
import sqlite3
from PyQt5 import QtWidgets
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.xing import *
from utility.static import now, strf_time, strp_time, timedelta_sec
from utility.setting import columns_cj, columns_tj, columns_jg, columns_td, columns_tt, ui_num, DB_TRADELIST, DICT_SET


class TraderXing:
    app = QtWidgets.QApplication(sys.argv)

    def __init__(self, qlist):
        """
                    0        1       2        3       4       5          6        7      8      9     10
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceivQ, stockQ, coinQ, sstgQ, cstgQ,
                 tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, wsk1Q, wsk2Q]
                   11       12      13     14      15     16     17
        """
        self.windowQ = qlist[0]
        self.soundQ = qlist[1]
        self.query1Q = qlist[2]
        self.teleQ = qlist[4]
        self.sreceivQ = qlist[5]
        self.stockQ = qlist[7]
        self.sstgQ = qlist[9]

        self.df_cj = pd.DataFrame(columns=columns_cj)   # 체결목록
        self.df_jg = pd.DataFrame(columns=columns_jg)   # 잔고목록
        self.df_tj = pd.DataFrame(columns=columns_tj)   # 잔고평가
        self.df_td = pd.DataFrame(columns=columns_td)   # 거래목록
        self.df_tt = pd.DataFrame(columns=columns_tt)   # 실현손익
        self.df_tr = None

        self.dict_name = {}     # key: 종목코드, value: 종목명
        self.dict_vipr = {}     # key: 종목코드, value: [갱신여부, 발동시간+5초, uvi, dvi, uvid5]
        self.dict_buyt = {}     # key: 종목코드, value: datetime
        self.dict_sidt = {}     # key: 종목코드, value: datetime
        self.dict_intg = {
            '장운영상태': 1,
            '예수금': 0,
            '추정예수금': 0,
            '추정예탁자산': 0,
            '종목당투자금': 0
        }
        self.dict_strg = {
            '당일날짜': strf_time('%Y%m%d'),
            '계좌번호': ''
        }
        self.dict_bool = {
            '장초전략잔고청산': False,
            '장중전략잔고청산': False,
            '로그인': False,
            'TR수신': False,
            'TR다음': False
        }
        remaintime = (strp_time('%Y%m%d%H%M%S', self.dict_strg['당일날짜'] + '090100') - now()).total_seconds()
        self.dict_time = {
            '휴무종료': timedelta_sec(remaintime) if remaintime > 0 else timedelta_sec(600),
            '거래정보': now()
        }
        self.dict_item = None
        self.list_kosd = None
        self.list_buy = []
        self.list_sell = []

        self.xa_session = XASession()
        self.xa_query = XAQuery()
        self.xa_real_od = XAReal(self)
        self.xa_real_cg = XAReal(self)

        self.xa_real_od.RegisterRes('SC0_')
        self.xa_real_cg.RegisterRes('SC1_')

        self.Start()

    def Start(self):
        self.LoadDatabase()
        self.XingLogin()
        self.EventLoop()

    def LoadDatabase(self):
        con = sqlite3.connect(DB_TRADELIST)
        df = pd.read_sql(f"SELECT * FROM s_chegeollist WHERE 체결시간 LIKE '{self.dict_strg['당일날짜']}%'", con)
        self.df_cj = df.set_index('index').sort_values(by=['체결시간'], ascending=False)
        df = pd.read_sql(f"SELECT * FROM s_tradelist WHERE 체결시간 LIKE '{self.dict_strg['당일날짜']}%'", con)
        self.df_td = df.set_index('index').sort_values(by=['체결시간'], ascending=False)
        df = pd.read_sql(f'SELECT * FROM s_jangolist', con)
        self.df_jg = df.set_index('index').sort_values(by=['매입금액'], ascending=False)
        con.close()

        if len(self.df_cj) > 0:
            self.windowQ.put([ui_num['S체결목록'], self.df_cj])
        if len(self.df_td) > 0:
            self.windowQ.put([ui_num['S거래목록'], self.df_td])

        self.windowQ.put([ui_num['S로그텍스트'], '시스템 명령 실행 알림 - 데이터베이스 정보 불러오기 완료'])

    def XingLogin(self):
        self.xa_session.Login(DICT_SET['아이디1'], DICT_SET['비밀번호1'], DICT_SET['인증서비밀번호1'])
        self.dict_strg['계좌번호'] = self.xa_session.GetAccountList(0)

        df = []
        df2 = self.xa_query.BlockRequest("t8430", gubun=2)
        df2.rename(columns={'shcode': 'index', 'hname': '종목명'}, inplace=True)
        df2 = df2.set_index('index')
        df.append(df2)

        df2 = self.xa_query.BlockRequest("t8430", gubun=1)
        df2.rename(columns={'shcode': 'index', 'hname': '종목명'}, inplace=True)
        df2 = df2.set_index('index')
        df.append(df2)
        df = pd.concat(df)
        df = df[['종목명']].copy()

        df = pd.concat(df)
        for code in df.index:
            name = df['종목명'][code]
            self.dict_name[code] = name

        self.windowQ.put([ui_num['S로그텍스트'], '시스템 명령 실행 알림 - OpenAPI 로그인 완료'])

        if len(self.df_jg) > 0:
            for code in self.df_jg.index:
                df = self.df_cj[(self.df_cj['주문구분'] == '매수') & (self.df_cj['종목명'] == self.dict_name[code])]
                if len(df) > 0:
                    self.dict_buyt[code] = strp_time('%Y%m%d%H%M%S%f', df['체결시간'].iloc[0])
                else:
                    self.dict_buyt[code] = now()
                self.sreceivQ.put(f'잔고편입 {code}')

        if int(strf_time('%H%M%S')) > 90000:
            self.dict_intg['장운영상태'] = 3

    def EventLoop(self):
        self.GetAccountjanGo()
        self.OperationRealreg()
        while True:
            if not self.stockQ.empty():
                data = self.stockQ.get()
                if type(data) == list:
                    if len(data) == 10:
                        self.SendOrder(data)
                    elif len(data) == 5:
                        self.BuySell(data[0], data[1], data[2], data[3], data[4])
                        continue
                    elif len(data) == 2:
                        if data[0] == 'VI정보':
                            self.dict_vipr = data[1]
                    elif len(data) == 3:
                        self.UpdateJango(data[0], data[1], data[2])
                elif type(data) == str:
                    self.TelegramCmd(data)

            if self.dict_intg['장운영상태'] == 1 and now() > self.dict_time['휴무종료']:
                break
            if int(strf_time('%H%M%S')) >= 100000 and not self.dict_bool['장초전략잔고청산']:
                self.JangoChungsan1()
            if int(strf_time('%H%M%S')) >= 152900 and not self.dict_bool['장중전략잔고청산']:
                self.JangoChungsan2()
            if self.dict_intg['장운영상태'] == 8:
                self.AllRemoveRealreg()
                self.SaveDayData()
                break

            if now() > self.dict_time['거래정보']:
                self.UpdateTotaljango()
                self.dict_time['거래정보'] = timedelta_sec(1)

            time_loop = timedelta_sec(0.25)
            while now() < time_loop:
                pythoncom.PumpWaitingMessages()
                time.sleep(0.0001)

        self.sstgQ.put('전략프로세스종료')
        self.windowQ.put([ui_num['S로그텍스트'], '시스템 명령 실행 알림 - 트레이더를 종료합니다.'])
        if DICT_SET['주식알림소리']:
            self.soundQ.put('주식 트레이더를 종료합니다.')
        self.teleQ.put('주식 트레이더를 종료하였습니다.')

    def SendOrder(self, order):
        pass
        """
        TODO 주문 전송
        """

    def BuySell(self, gubun, code, name, c, oc):
        if gubun == '매수':
            if code in self.df_jg.index:
                self.sstgQ.put(['매수취소', code])
                return
            if code in self.list_buy:
                self.sstgQ.put(['매수취소', code])
                self.windowQ.put([ui_num['S로그텍스트'], '매매 시스템 오류 알림 - 현재 매도 주문중인 종목입니다.'])
                return
            if self.dict_intg['추정예수금'] < oc * c:
                if code not in self.dict_sidt.keys() or now() > self.dict_sidt[code]:
                    self.Order('시드부족', code, name, c, oc)
                    self.dict_sidt[code] = timedelta_sec(180)
                self.sstgQ.put(['매수취소', code])
                return
        elif gubun == '매도':
            if code not in self.df_jg.index:
                self.sstgQ.put(['매도취소', code])
                return
            if code in self.list_sell:
                self.sstgQ.put(['매도취소', code])
                self.windowQ.put([ui_num['S로그텍스트'], '매매 시스템 오류 알림 - 현재 매수 주문중인 종목입니다.'])
                return

        self.Order(gubun, code, name, c, oc)

    def Order(self, gubun, code, name, c, oc):
        on = 0
        if gubun == '매수':
            self.dict_intg['추정예수금'] -= oc * c
            self.list_buy.append(code)
            on = 1
        elif gubun == '매도':
            self.list_sell.append(code)
            on = 2

        if DICT_SET['주식모의투자'] or gubun == '시드부족':
            self.UpdateChejanData(code, name, '체결', gubun, c, c, oc, 0, strf_time('%Y%m%d%H%M%S%f'))
        else:
            self.stockQ.put([gubun, '4989', self.dict_strg['계좌번호'], on, code, oc, 0, '03', '', name])

    def TelegramCmd(self, work):
        if work == '/당일체결목록':
            if len(self.df_cj) > 0:
                self.teleQ.put(self.df_cj)
            else:
                self.teleQ.put('현재는 거래목록이 없습니다.')
        elif work == '/당일거래목록':
            if len(self.df_td) > 0:
                self.teleQ.put(self.df_td)
            else:
                self.teleQ.put('현재는 거래목록이 없습니다.')
        elif work == '/계좌잔고평가':
            if len(self.df_jg) > 0:
                self.teleQ.put(self.df_jg)
            else:
                self.teleQ.put('현재는 잔고목록이 없습니다.')
        elif work == '/잔고청산주문':
            if not self.dict_bool['장초전략잔고청산']:
                self.JangoChungsan1()
            elif not self.dict_bool['장중전략잔고청산']:
                self.JangoChungsan2()

    def GetAccountjanGo(self):
        while True:
            """
            TODO 예수금 및 추정예탁자산 조회
            """
            df = self.xa_query.BlockRequest(
                't0424', accno=self.dict_strg['계좌번호'], passwd=DICT_SET['계좌비밀번호1'],
                prcgb='1', chegb='2', dangb='0', charge='1', cts_expcode=' '
            )
            if df['D+2추정예수금'][0] != '':
                if DICT_SET['주식모의투자']:
                    con = sqlite3.connect(DB_TRADELIST)
                    df = pd.read_sql('SELECT * FROM s_tradelist', con)
                    con.close()
                    self.dict_intg['예수금'] = 100000000 - self.df_jg['매입금액'].sum() + df['수익금'].sum()
                else:
                    self.dict_intg['예수금'] = int(df['D+2추정예수금'][0])
                self.dict_intg['추정예수금'] = self.dict_intg['예수금']
                break
            else:
                self.windowQ.put([ui_num['S로그텍스트'], '시스템 명령 오류 알림 - 오류가 발생하여 계좌평가현황을 재조회합니다.'])
                time.sleep(3.35)

        while True:
            """
            TODO 잔고 조회
            """
            if df['추정예탁자산'][0] != '':
                if int(strf_time('%H%M%S')) < 100000:
                    maxbuycount = DICT_SET['주식장초최대매수종목수']
                else:
                    maxbuycount = DICT_SET['주식장중최대매수종목수']
                if DICT_SET['주식모의투자']:
                    self.dict_intg['추정예탁자산'] = self.dict_intg['예수금'] + self.df_jg['평가금액'].sum()
                else:
                    self.dict_intg['추정예탁자산'] = int(df['추정예탁자산'][0])

                self.dict_intg['종목당투자금'] = int(self.dict_intg['추정예탁자산'] * 0.99 / maxbuycount)
                self.sstgQ.put(self.dict_intg['종목당투자금'])

                if DICT_SET['주식모의투자']:
                    self.df_tj.at[self.dict_strg['당일날짜']] = \
                        self.dict_intg['추정예탁자산'], self.dict_intg['예수금'], 0, 0, 0, 0, 0
                else:
                    tsp = float(int(df['총수익률(%)'][0]) / 100)
                    tsg = int(df['총평가손익금액'][0])
                    tbg = int(df['총매입금액'][0])
                    tpg = int(df['총평가금액'][0])
                    self.df_tj.at[self.dict_strg['당일날짜']] = \
                        self.dict_intg['추정예탁자산'], self.dict_intg['예수금'], 0, tsp, tsg, tbg, tpg
                self.windowQ.put([ui_num['S잔고평가'], self.df_tj])
                break
            else:
                self.windowQ.put([ui_num['S로그텍스트'], '시스템 명령 오류 알림 - 오류가 발생하여 계좌평가결과를 재조회합니다.'])
                time.sleep(3.35)

        if len(self.df_td) > 0:
            self.UpdateTotaltradelist(first=True)

    def OperationRealreg(self):
        self.windowQ.put([ui_num['S로그텍스트'], '시스템 명령 실행 알림 - 장운영시간 등록 완료'])
        self.windowQ.put([ui_num['S로그텍스트'], '시스템 명령 실행 알림 - 트레이더 시작 완료'])

    def JangoChungsan1(self):
        self.dict_bool['장초전략잔고청산'] = True
        if len(self.df_jg) > 0:
            for code in self.df_jg.index:
                if code in self.list_sell:
                    continue
                c = self.df_jg['현재가'][code]
                oc = self.df_jg['보유수량'][code]
                name = self.dict_name[code]
                if DICT_SET['주식모의투자']:
                    self.list_sell.append(code)
                    self.UpdateChejanData(code, name, '체결', '매도', c, c, oc, 0, strf_time('%Y%m%d%H%M%S%f'))
                else:
                    self.Order('매도', code, name, c, oc)
        if DICT_SET['주식알림소리']:
            self.soundQ.put('장초전략 잔고청산 주문을 전송하였습니다.')
        self.windowQ.put([ui_num['S로그텍스트'], '시스템 명령 실행 알림 - 장초전략 잔고청산 주문 완료'])

    def JangoChungsan2(self):
        self.dict_bool['장중전략잔고청산'] = True
        if len(self.df_jg) > 0:
            for code in self.df_jg.index:
                if code in self.list_sell:
                    continue
                c = self.df_jg['현재가'][code]
                oc = self.df_jg['보유수량'][code]
                name = self.dict_name[code]
                if DICT_SET['주식모의투자']:
                    self.list_sell.append(code)
                    self.UpdateChejanData(code, name, '체결', '매도', c, c, oc, 0, strf_time('%Y%m%d%H%M%S%f'))
                else:
                    self.Order('매도', code, name, c, oc)
        if DICT_SET['주식알림소리']:
            self.soundQ.put('장중전략 잔고청산 주문을 전송하였습니다.')
        self.windowQ.put([ui_num['S로그텍스트'], '시스템 명령 실행 알림 - 장중전략 잔고청산 주문 완료'])

    def AllRemoveRealreg(self):
        self.stockQ.put(['ALL', 'ALL'])
        if DICT_SET['주식알림소리']:
            self.soundQ.put('실시간 데이터의 수신을 중단하였습니다.')
        self.windowQ.put([ui_num['S로그텍스트'], f'시스템 명령 실행 알림 - 실시간 데이터 중단 완료'])

    def SaveDayData(self):
        if len(self.df_td) > 0:
            df = self.df_tt[['총매수금액', '총매도금액', '총수익금액', '총손실금액', '수익률', '수익금합계']].copy()
            self.query1Q.put([2, df, 's_totaltradelist', 'append'])
        if DICT_SET['주식알림소리']:
            self.soundQ.put('일별실현손익를 저장하였습니다.')
        self.windowQ.put([ui_num['S로그텍스트'], '시스템 명령 실행 알림 - 일별실현손익 저장 완료'])

    def UpdateTotaljango(self):
        if len(self.df_jg) > 0:
            tsg = self.df_jg['평가손익'].sum()
            tbg = self.df_jg['매입금액'].sum()
            tpg = self.df_jg['평가금액'].sum()
            bct = len(self.df_jg)
            tsp = round(tsg / tbg * 100, 2)
            ttg = self.dict_intg['예수금'] + tpg
            self.df_tj.at[self.dict_strg['당일날짜']] = \
                ttg, self.dict_intg['예수금'], bct, tsp, tsg, tbg, tpg
        else:
            self.df_tj.at[self.dict_strg['당일날짜']] = \
                self.dict_intg['예수금'], self.dict_intg['예수금'], 0, 0.0, 0, 0, 0
        self.windowQ.put([ui_num['S잔고목록'], self.df_jg])
        self.windowQ.put([ui_num['S잔고평가'], self.df_tj])

    def UpdateJango(self, code, name, c):
        try:
            prec = self.df_jg['현재가'][code]
        except KeyError:
            return

        if prec != c:
            bg = self.df_jg['매입금액'][code]
            oc = int(self.df_jg['보유수량'][code])
            pg, sg, sp = self.GetPgSgSp(bg, oc * c)
            columns = ['현재가', '수익률', '평가손익', '평가금액']
            self.df_jg.at[code, columns] = c, sp, sg, pg
            if code in self.dict_buyt.keys():
                self.sstgQ.put([code, name, sp, oc, c, self.dict_buyt[code]])

    # noinspection PyMethodMayBeStatic
    def GetPgSgSp(self, bg, cg):
        gtexs = cg * 0.0023
        gsfee = cg * 0.00015
        gbfee = bg * 0.00015
        texs = gtexs - (gtexs % 1)
        sfee = gsfee - (gsfee % 10)
        bfee = gbfee - (gbfee % 10)
        pg = int(cg - texs - sfee - bfee)
        sg = pg - bg
        sp = round(sg / bg * 100, 2)
        return pg, sg, sp

    def OnReceiveOperData(self, data):
        try:
            gubun = int(data['jangubun'])
            status = int(data['jstatus'])
        except Exception as e:
            self.windowQ.put([ui_num['S단순텍스트'], f'OnReceiveOperData {e}'])
        else:
            if gubun == 1:
                self.dict_intg['장운영상태'] = status
                self.OperationAlert(status)

    def OperationAlert(self, status):
        if DICT_SET['주식알림소리']:
            if status == 25:
                self.soundQ.put('장시작 10분 전입니다.')
            elif status == 24:
                self.soundQ.put('장시작 5분 전입니다.')
            elif status == 23:
                self.soundQ.put('장시작 1분 전입니다.')
            elif status == 22:
                self.soundQ.put('장시작 10초 전입니다.')
            elif status == 21:
                self.soundQ.put(f"{self.dict_strg['당일날짜'][:4]}년 {self.dict_strg['당일날짜'][4:6]}월 "
                                f"{self.dict_strg['당일날짜'][6:]}일 장이 시작되었습니다.")
            elif status == 44:
                self.soundQ.put('장마감 5분 전입니다.')
            elif status == 43:
                self.soundQ.put('장마감 1분 전입니다.')
            elif status == 42:
                self.soundQ.put('장마감 10초 전입니다.')
            elif status == 41:
                self.soundQ.put(f"{self.dict_strg['당일날짜'][:4]}년 {self.dict_strg['당일날짜'][4:6]}월 "
                                f"{self.dict_strg['당일날짜'][6:]}일 장이 종료되었습니다.")

    def OnReceiveChegeolData(self, data):
        if DICT_SET['주식모의투자']:
            return

        try:
            code = data['lsuno']
            name = self.dict_name[code]
            on = data['ordno']
            og = '매도' if data['bnstp'] == '1' else '매수'
            op = int(data['ordprc'])
            oc = int(data['ordqty'])
            cp = int(data['execprc'])
            cc = int(data['execqty'])
            omc = oc - cc
        except Exception as e:
            self.windowQ.put([ui_num['S로그텍스트'], f'OnReceiveChejanData {e}'])
        else:
            self.UpdateChejanData(code, name, '체결', og, op, cp, oc, omc, on)

    def UpdateChejanData(self, code, name, ot, og, op, cp, oc, omc, on):
        if ot == '체결' and omc == 0 and cp != 0:
            if og == '매수':
                self.UpdateChegeoljango(code, name, og, oc, cp)
                self.dict_buyt[code] = now()
                self.list_buy.remove(code)
                self.sreceivQ.put(f'잔고편입 {code}')
                self.sstgQ.put(['매수완료', code])
                self.dict_intg['예수금'] -= oc * cp
                self.dict_intg['추정예수금'] = self.dict_intg['예수금']
                self.windowQ.put([ui_num['S로그텍스트'], f'매매 시스템 체결 알림 - {name} {oc}주 {og}'])
            elif og == '매도':
                bp = self.df_jg['매입가'][code]
                bg = bp * oc
                pg, sg, sp = self.GetPgSgSp(bg, oc * cp)
                self.UpdateChegeoljango(code, name, og, oc, cp)
                self.UpdateTradelist(name, oc, sp, sg, bg, pg, on)
                self.list_sell.remove(code)
                self.sreceivQ.put(f'잔고청산 {code}')
                if int(strf_time('%H%M%S')) < 100000:
                    self.dict_intg['종목당투자금'] = \
                        int(self.df_tj['추정예탁자산'][self.dict_strg['당일날짜']] * 0.99 / DICT_SET['주식장중최대매수종목수'])
                else:
                    self.dict_intg['종목당투자금'] = \
                        int(self.df_tj['추정예탁자산'][self.dict_strg['당일날짜']] * 0.99 / DICT_SET['주식장중최대매수종목수'])
                self.sstgQ.put(self.dict_intg['종목당투자금'])
                self.sstgQ.put(['매도완료', code])
                self.dict_intg['예수금'] += pg
                self.dict_intg['추정예수금'] = self.dict_intg['예수금']
                self.windowQ.put([ui_num['S로그텍스트'],
                                  f"매매 시스템 체결 알림 - {name} {oc}주 {og}, 수익률 {sp}% 수익금{format(sg, ',')}원"])
        self.UpdateChegeollist(name, og, oc, omc, op, cp, on)

    def UpdateChegeoljango(self, code, name, og, oc, cp):
        if og == '매수':
            if code not in self.df_jg.index:
                bg = oc * cp
                pg, sg, sp = self.GetPgSgSp(bg, oc * cp)
                self.df_jg.at[code] = name, cp, cp, sp, sg, bg, pg, oc
            else:
                jc = self.df_jg['보유수량'][code]
                bg = self.df_jg['매입금액'][code]
                jc = jc + oc
                bg = bg + oc * cp
                bp = int(bg / jc)
                pg, sg, sp = self.GetPgSgSp(bg, jc * cp)
                self.df_jg.at[code] = name, bp, cp, sp, sg, bg, pg, jc
        elif og == '매도':
            jc = self.df_jg['보유수량'][code]
            if jc - oc == 0:
                self.df_jg.drop(index=code, inplace=True)
            else:
                bp = self.df_jg['매입가'][code]
                jc = jc - oc
                bg = jc * bp
                pg, sg, sp = self.GetPgSgSp(bg, jc * cp)
                self.df_jg.at[code] = name, bp, cp, sp, sg, bg, pg, jc

        columns = ['매입가', '현재가', '평가손익', '매입금액']
        self.df_jg[columns] = self.df_jg[columns].astype(int)
        self.df_jg.sort_values(by=['매입금액'], inplace=True)
        self.query1Q.put([2, self.df_jg, 's_jangolist', 'replace'])
        if DICT_SET['주식알림소리']:
            self.soundQ.put(f'{name} {oc}주를 {og}하였습니다')

    def UpdateTradelist(self, name, oc, sp, sg, bg, pg, on):
        dt = strf_time('%Y%m%d%H%M%S%f')
        if DICT_SET['주식모의투자'] and len(self.df_td) > 0:
            if dt in self.df_td['체결시간'].values:
                while dt in self.df_td['체결시간'].values:
                    dt = str(int(dt) + 1)
                on = dt

        self.df_td.at[on] = name, bg, pg, oc, sp, sg, dt
        self.df_td.sort_values(by=['체결시간'], ascending=False, inplace=True)
        self.windowQ.put([ui_num['S거래목록'], self.df_td])

        df = pd.DataFrame([[name, bg, pg, oc, sp, sg, dt]], columns=columns_td, index=[on])
        self.query1Q.put([2, df, 's_tradelist', 'append'])
        self.UpdateTotaltradelist()

    def UpdateTotaltradelist(self, first=False):
        tsg = self.df_td['매도금액'].sum()
        tbg = self.df_td['매수금액'].sum()
        tsig = self.df_td[self.df_td['수익금'] > 0]['수익금'].sum()
        tssg = self.df_td[self.df_td['수익금'] < 0]['수익금'].sum()
        sg = self.df_td['수익금'].sum()
        sp = round(sg / self.dict_intg['추정예탁자산'] * 100, 2)
        tdct = len(self.df_td)

        self.df_tt = pd.DataFrame(
            [[tdct, tbg, tsg, tsig, tssg, sp, sg]], columns=columns_tt, index=[self.dict_strg['당일날짜']]
        )
        self.windowQ.put([ui_num['S실현손익'], self.df_tt])
        if not first:
            self.teleQ.put(
                f"거래횟수 {len(self.df_td)}회 / 총매수금액 {format(int(tbg), ',')}원 / "
                f"총매도금액 {format(int(tsg), ',')}원 / 총수익금액 {format(int(tsig), ',')}원 / "
                f"총손실금액 {format(int(tssg), ',')}원 / 수익률 {sp}% / 수익금합계 {format(int(sg), ',')}원")

    def UpdateChegeollist(self, name, og, oc, omc, op, cp, on):
        dt = strf_time('%Y%m%d%H%M%S%f')
        if DICT_SET['주식모의투자'] and len(self.df_cj) > 0:
            if dt in self.df_cj['체결시간'].values:
                while dt in self.df_cj['체결시간'].values:
                    dt = str(int(dt) + 1)
                on = dt

        if on in self.df_cj.index:
            self.df_cj.at[on, ['미체결수량', '체결가', '체결시간']] = omc, cp, dt
        else:
            if og == '시드부족':
                self.df_cj.at[on] = name, og, oc, 0, op, 0, dt
            else:
                self.df_cj.at[on] = name, og, oc, omc, op, cp, dt
        self.df_cj.sort_values(by=['체결시간'], ascending=False, inplace=True)
        self.windowQ.put([ui_num['S체결목록'], self.df_cj])

        if omc == 0:
            df = pd.DataFrame([[name, og, oc, omc, op, cp, dt]], columns=columns_cj, index=[on])
            self.query1Q.put([2, df, 's_chegeollist', 'append'])
