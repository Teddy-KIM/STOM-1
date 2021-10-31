import os
import sys
import time
import pyupbit
import sqlite3
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.static import now, strf_time, strp_time, timedelta_sec
from utility.setting import columns_cj, columns_tj, columns_jg, columns_td, columns_tt, ui_num, DB_TRADELIST, DICT_SET


class TraderUpbit:
    def __init__(self, qlist):
        """
                    0        1       2        3       4       5          6          7        8      9
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceiv1Q, creceiv2Q, stockQ, coinQ,
                 sstgQ, cstgQ, tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, chartQ, hogaQ]
                   10    11      12      13      14      15      16      17     18
        """
        self.windowQ = qlist[0]
        self.soundQ = qlist[1]
        self.query1Q = qlist[2]
        self.teleQ = qlist[4]
        self.creceiv1Q = qlist[6]
        self.coinQ = qlist[9]
        self.cstgQ = qlist[11]
        self.dict_set = DICT_SET

        self.upbit = None                               # 매도수 주문 및 체결 확인용 객체
        self.buy_uuid = {}                              # 매수 주문 저장용 딕셔너리 key : 티커명 value : uuid
        self.sell_uuid = {}                             # 매도 주문 저장용 딕셔너리 key : 티커명 value : uuid

        self.df_cj = pd.DataFrame(columns=columns_cj)   # 체결목록
        self.df_jg = pd.DataFrame(columns=columns_jg)   # 잔고목록
        self.df_tj = pd.DataFrame(columns=columns_tj)   # 잔고평가
        self.df_td = pd.DataFrame(columns=columns_td)   # 거래목록
        self.df_tt = pd.DataFrame(columns=columns_tt)   # 실현손익

        self.str_today = strf_time('%Y%m%d')

        self.dict_buyt = {}                             # 매수시간 기록용
        self.dict_sidt = {}                             # 시드부족 기록용
        self.dict_intg = {
            '예수금': 0,                                 # 실졔예수금 - 체결확인 시 증감한다.
            '추정예수금': 0,                              # 추정예수금 - 매수주문 시 감소한다.
            '종목당투자금': 0,                            # 종목당 투자금은 int((예수금 + 매입금액) * 0.99 / 최대매수종목수)로 계산
            '업비트수수료': 0.0005                        # 0.05%
        }
        self.dict_bool = {
            '최소주문금액': False,                        # 업비트 주문가능 최소금액, 종목당투자금이 5천원 미만일 경우 False
            '실현손익저장': False,
            '장초전략잔고청산': False if 90000 < int(strf_time('%H%M%S')) < 100000 else True,
            '장중전략잔고청산': True if 90000 < int(strf_time('%H%M%S')) < 100000 else False
        }
        self.dict_time = {
            '매수체결확인': now(),                        # 0.5초 마다 매수 체결 확인용
            '매도체결확인': now(),                        # 0.5초 마다 매도 체결 확인용
            '거래정보': now()                            # 잔고목록 및 잔고평가 갱신용
        }
        self.Start()

    def Start(self):
        self.LoadDatabase()
        self.GetKey()
        self.GetBalances()
        self.EventLoop()

    def LoadDatabase(self):
        """
        프로그램 구동 시 당일 체결목록, 당일 거래목록, 잔고목록을 불러온다.
        잔고보유 중 프로그램 구동시 리시버 프로세스가 잔고를 인식할 수 있도록 잔고편입 신호를 보내고
        디비 체결리스트에 기록된 매수시간을 매수시간 기록용 딕셔너리에 삽입한다.
        """
        con = sqlite3.connect(DB_TRADELIST)
        df = pd.read_sql(f"SELECT * FROM c_chegeollist WHERE 체결시간 LIKE '{self.str_today}%'", con)
        self.df_cj = df.set_index('index').sort_values(by=['체결시간'], ascending=False)
        df = pd.read_sql(f'SELECT * FROM c_jangolist', con)
        self.df_jg = df.set_index('index').sort_values(by=['매입금액'], ascending=False)
        df = pd.read_sql(f"SELECT * FROM c_tradelist WHERE 체결시간 LIKE '{self.str_today}%'", con)
        self.df_td = df.set_index('index').sort_values(by=['체결시간'], ascending=False)
        con.close()

        if len(self.df_cj) > 0:
            self.windowQ.put([ui_num['C체결목록'], self.df_cj])
        if len(self.df_td) > 0:
            self.windowQ.put([ui_num['C거래목록'], self.df_td])
        if len(self.df_jg) > 0:
            for code in self.df_jg.index:
                df = self.df_cj[(self.df_cj['주문구분'] == '매수') & (self.df_cj['종목명'] == code)]
                if len(df) > 0:
                    self.dict_buyt[code] = strp_time('%Y%m%d%H%M%S%f', df['체결시간'].iloc[0])
                else:
                    self.dict_buyt[code] = now()
                self.creceiv1Q.put(['잔고편입', code])

        self.windowQ.put([ui_num['C로그텍스트'], '시스템 명령 실행 알림 - 데이터베이스 불러오기 완료'])

    def GetKey(self):
        """ 매도수 주문 및 체결확인용 self.upbit 객체 생성 """
        self.upbit = pyupbit.Upbit(self.dict_set['Access_key'], self.dict_set['Secret_key'])
        self.windowQ.put([ui_num['C로그텍스트'], '시스템 명령 실행 알림 - 주문 및 체결확인용 업비트 객체 생성 완료'])

    def GetBalances(self):
        """ 예수금 조회 및 종목당투자금 계산, 계산된 종목당투자금은 전략연산프로세스로 보낸다. """
        if 90000 < int(strf_time('%H%M%S')) < 100000:
            maxbuycount = self.dict_set['코인장초최대매수종목수']
        else:
            maxbuycount = self.dict_set['코인장중최대매수종목수']
        if self.dict_set['코인모의투자']:
            con = sqlite3.connect(DB_TRADELIST)
            df = pd.read_sql('SELECT * FROM c_tradelist', con)
            con.close()
            tbg = df['매수금액'].sum()
            tsg = df['매도금액'].sum()
            tcg = df['수익금'].sum()
            bfee = int(round(tbg * self.dict_intg['업비트수수료']))
            sfee = int(round(tsg * self.dict_intg['업비트수수료']))
            cbg = self.df_jg['매입금액'].sum()
            cfee = int(round(cbg * self.dict_intg['업비트수수료']))
            chujeonjasan = 100000000 + tcg - bfee - sfee
            self.dict_intg['예수금'] = int(chujeonjasan - cbg - cfee)
            self.dict_intg['종목당투자금'] = int(chujeonjasan * 0.99 / maxbuycount)
        elif self.upbit is not None:
            cbg = self.df_jg['매입금액'].sum()
            self.dict_intg['예수금'] = int(float(self.upbit.get_balances()[0]['balance']))
            self.dict_intg['종목당투자금'] = int((self.dict_intg['예수금'] + cbg) * 0.99 / maxbuycount)
        else:
            self.windowQ.put([ui_num['C로그텍스트'], '시스템 명령 오류 알림 - 업비트 키값이 설정되지 않았습니다.'])

        self.dict_intg['추정예수금'] = self.dict_intg['예수금']
        self.cstgQ.put(self.dict_intg['종목당투자금'])
        self.dict_bool['최소주문금액'] = True if self.dict_intg['종목당투자금'] > 5000 else False

        if len(self.df_td) > 0:
            self.UpdateTotaltradelist(first=True)
        self.windowQ.put([ui_num['C로그텍스트'], '시스템 명령 실행 알림 - 예수금 조회 완료'])

    def EventLoop(self):
        self.windowQ.put([ui_num['C로그텍스트'], '시스템 명령 실행 알림 - 트레이더 시작'])
        while True:
            """ 주문 및 잔고갱신용 큐를 감시한다. """
            if not self.coinQ.empty():
                data = self.coinQ.get()
                if type(data) == dict:
                    self.dict_set = data
                elif type(data) == list:
                    if data[0] in self.df_jg.index:
                        self.UpdateJango(data[0], data[1])
                        continue
                    elif data[0] == '매수':
                        self.Buy(data[1], data[2], data[3])
                    elif data[0] == '매도':
                        self.Sell(data[1], data[2], data[3])

            """ 주문의 체결확인은 0.5초마다 반복한다. """
            if len(self.buy_uuid) > 0 and now() > self.dict_time['매수체결확인']:
                self.CheckBuyChegeol()
                self.dict_time['매수체결확인'] = timedelta_sec(0.5)
            if len(self.sell_uuid) > 0 and now() > self.dict_time['매도체결확인']:
                self.CheckSellChegeol()
                self.dict_time['매도체결확인'] = timedelta_sec(0.5)

            """ 9시와 10시 전략이 바뀌는 시점에 잔고청산한다. """
            if int(strf_time('%H%M%S')) >= 100000 and not self.dict_bool['장초전략잔고청산']:
                self.JangoCheongsan1()
            if 90000 < int(strf_time('%H%M%S')) < 100000 and not self.dict_bool['장중전략잔고청산']:
                self.JangoCheongsan2()

            """ 잔고평가 및 잔고목록 갱신도 1초마다 반복한다. """
            if now() > self.dict_time['거래정보']:
                self.UpdateTotaljango()
                self.dict_time['거래정보'] = timedelta_sec(1)

            """ 0시 1분 초기화 """
            if 0 < int(strf_time('%H%M%S')) < 100 and not self.dict_bool['실현손익저장']:
                self.SaveTotalGetbalDelcjtd()

            time.sleep(0.0001)

    """
    모의투자 시 실제 매도수 주문을 전송하지 않고 바로 체결목록, 잔고목록 등을 갱신한다.
    실매매 시 매도수 아이디 및 티커명을 매도, 매수 구분하여 딕셔너리에 저장하고
    딕셔너리의 길이가 0이상일 경우 get_order 함수로 체결확인을 1초마다 반복실행한다.
    체결이 완료되면 관련목록을 갱신하고 딕셔너리에서 삭제한다.
    체결확인 후 잔고목록를 갱신 한 이후에 전략 연산 프로세스로 체결완료 신호를 보낸다.
    모든 목록은 갱신될 때마다 쿼리 프로세스로 보내어 DB에 실시간으로 기록된다.
    매수 주문은 예수금 부족인지 아닌지를 우선 확인하여 예수금 부족일 경우 주문구분을 시드부족으로 체결목록에 기록한다.
    예수금 부족 상태이며 잔고목록에 없는 상태일 경우 전략 프로세스에서 지속적으로 매수 신호가 발생할 수 있다.
    그러므로 재차 시드부족이 발생한 종목은 체결목록에서 마지막 체결시간이 3분이내면 체결목록에 기록하지 않는다.
    """
    def Buy(self, code, c, oc):
        if not self.dict_bool['최소주문금액']:
            self.windowQ.put([ui_num['C로그텍스트'], '매매 시스템 오류 알림 - 종목당 투자금이 5천원 미만이라 주문할 수 없습니다.'])
            self.cstgQ.put(['매수취소', code])
            return
        if code in self.buy_uuid.keys() or code in self.df_jg.index:
            self.cstgQ.put(['매수취소', code])
            return
        if self.dict_intg['추정예수금'] < c * oc:
            if code not in self.dict_sidt.keys() or now() > self.dict_sidt[code]:
                self.UpdateBuy(code, c, oc, cancle=True)
                self.dict_sidt[code] = timedelta_sec(180)
            self.cstgQ.put(['매수취소', code])
            return

        if self.dict_set['코인모의투자']:
            self.UpdateBuy(code, c, oc)
        elif self.upbit is not None:
            ret = self.upbit.buy_market_order(code, self.dict_intg['종목당투자금'])
            if ret is not None:
                if self.CheckError(ret):
                    self.dict_intg['추정예수금'] -= c * oc
                    self.buy_uuid[code] = ret['uuid']
            else:
                self.cstgQ.put(['매수취소', code])
                self.windowQ.put([ui_num['C로그텍스트'], f'매매 시스템 오류 알림 - 주문 실패 {code}'])

        if self.dict_bool['실현손익저장'] and int(strf_time('%H%M%S')) > 100:
            self.dict_bool['실현손익저장'] = False

    def Sell(self, code, c, oc):
        if code in self.sell_uuid.keys() or code not in self.df_jg.index:
            self.cstgQ.put(['매도취소', code])
            return

        if self.dict_set['코인모의투자']:
            self.UpdateSell(code, c, oc)
        elif self.upbit is not None:
            ret = self.upbit.sell_market_order(code, oc)
            if ret is not None:
                if self.CheckError(ret):
                    self.sell_uuid[code] = ret['uuid']
            else:
                self.cstgQ.put(['매도취소', code])
                self.windowQ.put([ui_num['C로그텍스트'], f'매매 시스템 오류 알림 - 주문 실패 {code}'])

    def JangoCheongsan1(self):
        self.dict_bool['장초전략잔고청산'] = True
        self.dict_bool['장중전략잔고청산'] = False
        for code in self.df_jg.index:
            c = self.df_jg['현재가'][code]
            oc = self.df_jg['보유수량'][code]
            if self.dict_set['코인모의투자']:
                self.UpdateSell(code, c, oc)
            elif self.upbit is not None and code not in self.sell_uuid.keys():
                ret = self.upbit.sell_market_order(code, oc)
                if ret is not None:
                    if self.CheckError(ret):
                        self.sell_uuid[code] = ret['uuid']
                else:
                    self.cstgQ.put(['매도취소', code])
                    self.windowQ.put([ui_num['C로그텍스트'], f'매매 시스템 오류 알림 - 주문 실패 {code}'])
            time.sleep(0.15)

        self.windowQ.put([ui_num['C로그텍스트'], '시스템 명령 실행 알림 - 장초전략 잔고청산 주문 완료'])
        if self.dict_set['코인알림소리']:
            self.soundQ.put('코인 장초전략 잔고청산 주문을 전송하였습니다.')

    def JangoCheongsan2(self):
        self.dict_bool['장중전략잔고청산'] = True
        self.dict_bool['장초전략잔고청산'] = False
        for code in self.df_jg.index:
            c = self.df_jg['현재가'][code]
            oc = self.df_jg['보유수량'][code]
            if self.dict_set['코인모의투자']:
                self.UpdateSell(code, c, oc)
            elif self.upbit is not None and code not in self.sell_uuid.keys():
                ret = self.upbit.sell_market_order(code, oc)
                if ret is not None:
                    if self.CheckError(ret):
                        self.sell_uuid[code] = ret['uuid']
                else:
                    self.cstgQ.put(['매도취소', code])
                    self.windowQ.put([ui_num['C로그텍스트'], f'매매 시스템 오류 알림 - 주문 실패 {code}'])
            time.sleep(0.15)

        self.windowQ.put([ui_num['C로그텍스트'], '시스템 명령 실행 알림 - 장중전략 잔고청산 주문 완료'])
        if self.dict_set['코인알림소리']:
            self.soundQ.put('코인 장중전략 잔고청산 주문을 전송하였습니다.')

    """ 리시버가 보내온 현재가와 잔고목록의 현재가가 틀릴 경우만 잔고목록을 갱신하고 매도전략 확인용 데이터를 전략연산 프로세스로 보낸다. """
    def UpdateJango(self, code, c):
        try:
            prec = self.df_jg['현재가'][code]
        except KeyError:
            return

        if prec != c:
            bg = self.df_jg['매입금액'][code]
            oc = self.df_jg['보유수량'][code]
            pg, sg, sp, bfee, sfee = self.GetPgSgSp(bg, oc * c)
            columns = ['현재가', '수익률', '평가손익', '평가금액']
            self.df_jg.at[code, columns] = c, sp, sg, pg
            self.cstgQ.put([code, sp, oc, c, self.dict_buyt[code]])

    """ 시장가 주문의 체결확인은 리턴값중 체결수량만 확인하여 그 수량이 0을 초과할 경우 매도수를 기록한다. """
    def CheckBuyChegeol(self):
        buy_list = []
        for code, uuid in self.buy_uuid.items():
            ret = self.upbit.get_order(uuid)
            if ret is not None and self.CheckError(ret):
                trades = ret['trades']
                tg, cc = 0, 0
                for i in range(len(trades)):
                    tg += float(trades[i]['price']) * float(trades[i]['volume'])
                    cc += float(trades[i]['volume'])
                if cc > 0:
                    cp = round(tg / cc, 2)
                    cc = round(cc, 8)
                    buy_list.append([code, cp, cc])
            time.sleep(0.2)
        if len(buy_list) > 0:
            for code, cp, cc in buy_list:
                self.UpdateBuy(code, cp, cc)

    def CheckSellChegeol(self):
        sell_list = []
        for code, uuid in self.sell_uuid.items():
            ret = self.upbit.get_order(uuid)
            if ret is not None and self.CheckError(ret):
                trades = ret['trades']
                tg, cc = 0, 0
                for i in range(len(trades)):
                    tg += float(trades[i]['price']) * float(trades[i]['volume'])
                    cc += float(trades[i]['volume'])
                if cc > 0:
                    cp = round(tg / cc, 2)
                    cc = round(cc, 8)
                    sell_list.append([code, cp, cc])
            time.sleep(0.2)
        if len(sell_list) > 0:
            for code, cp, cc in sell_list:
                self.UpdateSell(code, cp, cc)

    """ 주문과 체결확인의 리턴값에 에러가 있을 경우 에러명과 메세지를 로그에 기록한다."""
    def CheckError(self, ret):
        if list(ret.keys())[0] == 'error':
            self.windowQ.put([ui_num['C로그텍스트'], f"{ret['error']['name']} : {ret['error']['message']}"])
            return False
        return True

    """
    매도수 체결완료시 큐로 신호가 인풋되는 과정은 다소 복잡하다.
    잔고목록의 원장은 트레이더 프로세스가 가지고 있지만,
    잦은 매수 시그널 발생을 방지하기 위해서 리시버도 잔고목록을 가지고 있어야한다.
    그래야 리시버가 전략연산 프로세스로 데이터를 보낼때 보유유무 데이터를 보내어
    전략연산 프로세스가 보유종목에 대해 매수신호 발생을 하지 않도록 한다.
    그래서 잔고편입 시 트레이더가 리시버로 잔고편입 신호를 보낸다.

    또한 주문은 트레이더가 하지만, 주문목록은 전략연산 프로세스가 가지고 있다.
    그러므로 트레이더가 체결확인하여 전략연산 프로세스로 매수완료 신호를 보내고
    그걸 받은 전략연산 프로세스는 주문목록에서 삭제한다.

    체결확인 외 시드부족, 주문실패, 최소주문금액오류, 잔고종목매수신호가 발생하였을 경우도
    전략연산 프로세스가 가진 주문목록을 삭제하기 위해 매도수 취소 신호를 보낸다. 
    """
    def UpdateBuy(self, code, cp, cc, cancle=False):
        dt = strf_time('%Y%m%d%H%M%S%f')
        if dt in self.df_cj.index:
            while dt in self.df_cj.index:
                dt = str(int(dt) + 1)

        order_gubun = '매수' if not cancle else '시드부족'
        if cancle:
            self.df_cj.at[dt] = code, order_gubun, cc, 0, cp, 0, dt
        else:
            self.df_cj.at[dt] = code, order_gubun, cc, 0, cp, cp, dt
        self.df_cj.sort_values(by=['체결시간'], ascending=False, inplace=True)
        self.windowQ.put([ui_num['C체결목록'], self.df_cj])

        if not cancle:
            bg = cp * cc
            pg, sg, sp, bfee, sfee = self.GetPgSgSp(bg, bg)
            self.df_jg.at[code] = code, cp, cp, sp, sg, bg, pg, cc

            if code in self.buy_uuid.keys():
                del self.buy_uuid[code]
            self.dict_buyt[code] = now()
            self.dict_intg['예수금'] -= bg + bfee
            self.dict_intg['추정예수금'] = self.dict_intg['예수금']
            self.cstgQ.put(['매수완료', code])
            self.creceiv1Q.put(['잔고편입', code])
            self.query1Q.put([2, self.df_jg, 'c_jangolist', 'replace'])
            self.windowQ.put([ui_num['C로그텍스트'], f'매매 시스템 체결 알림 - [매수] {code} 코인 {cp}원 {cc}개'])

            if self.dict_set['코인알림소리']:
                self.soundQ.put(f'{code[4:]} 코인을 매수하였습니다.')
            self.teleQ.put(f'매수 알림 - {code} {cp} {cc}')

        df = pd.DataFrame([[code, order_gubun, cc, 0, cp, cp, dt]], columns=columns_cj, index=[dt])
        self.query1Q.put([2, df, 'c_chegeollist', 'append'])

    def UpdateSell(self, code, cp, cc):
        dt = strf_time('%Y%m%d%H%M%S%f')
        if dt in self.df_cj.index:
            while dt in self.df_cj.index:
                dt = str(int(dt) + 1)

        bp = self.df_jg['매입가'][code]
        bg = bp * cc
        pg, sg, sp, bfee, sfee = self.GetPgSgSp(bg, cp * cc)
        self.dict_intg['예수금'] += bg + sg - sfee
        self.dict_intg['추정예수금'] = self.dict_intg['예수금']

        self.df_jg.drop(index=code, inplace=True)
        self.df_cj.at[dt] = code, '매도', cc, 0, cp, cp, dt
        self.df_td.at[dt] = code, bg, pg, cc, sp, sg, dt
        self.df_cj.sort_values(by=['체결시간'], ascending=False, inplace=True)
        self.df_td.sort_values(by=['체결시간'], ascending=False, inplace=True)

        if code in self.sell_uuid.keys():
            del self.sell_uuid[code]

        if 90000 < int(strf_time('%H%M%S')) < 100000:
            self.dict_intg['종목당투자금'] = \
                int(self.df_tj['추정예탁자산'][self.str_today] * 0.99 / self.dict_set['코인장초최대매수종목수'])
        else:
            self.dict_intg['종목당투자금'] = \
                int(self.df_tj['추정예탁자산'][self.str_today] * 0.99 / self.dict_set['코인장중최대매수종목수'])
        self.cstgQ.put(self.dict_intg['종목당투자금'])

        self.cstgQ.put(['매도완료', code])
        self.creceiv1Q.put(['잔고청산', code])
        self.windowQ.put([ui_num['C체결목록'], self.df_cj])
        self.windowQ.put([ui_num['C거래목록'], self.df_td])
        self.windowQ.put([ui_num['C로그텍스트'], f'매매 시스템 체결 알림 - [매도] {code} 코인 {cp}원 {cc}개'])
        if self.dict_set['코인알림소리']:
            self.soundQ.put(f'{code[4:]} 코인을 매도하였습니다.')

        self.query1Q.put([2, self.df_jg, 'c_jangolist', 'replace'])
        df = pd.DataFrame([[code, '매도', cc, 0, cp, cp, dt]], columns=columns_cj, index=[dt])
        self.query1Q.put([2, df, 'c_chegeollist', 'append'])
        df = pd.DataFrame([[code, bg, pg, cc, sp, sg, dt]], columns=columns_td, index=[dt])
        self.query1Q.put([2, df, 'c_tradelist', 'append'])

        self.teleQ.put(f'매도 알림 - {code} {cp} {cc}')
        self.UpdateTotaltradelist()

    """ 실현손익은 매도체결이 있을 때만, 갱신하여 UI로 보낸다. """
    def UpdateTotaltradelist(self, first=False):
        tsg = self.df_td['매도금액'].sum()
        tbg = self.df_td['매수금액'].sum()
        tsig = self.df_td[self.df_td['수익금'] > 0]['수익금'].sum()
        tssg = self.df_td[self.df_td['수익금'] < 0]['수익금'].sum()
        sg = self.df_td['수익금'].sum()
        sp = round(sg / tbg * 100, 2)
        tdct = len(self.df_td)

        self.df_tt = pd.DataFrame([[tdct, tbg, tsg, tsig, tssg, sp, sg]], columns=columns_tt, index=[self.str_today])
        self.windowQ.put([ui_num['C실현손익'], self.df_tt])
        if not first:
            self.teleQ.put(f'손익 알림 - 총매수금액 {tbg}, 총매도금액 {tsg}, 수익 {tsig}, 손실 {tssg}, 수익금합계 {sg}')

    def GetPgSgSp(self, bg, cg):
        sfee = cg * self.dict_intg['업비트수수료']
        bfee = bg * self.dict_intg['업비트수수료']
        pg = int(round(cg))
        sg = int(round(pg - bg))
        sp = round(sg / bg * 100, 2)
        return pg, sg, sp, bfee, sfee

    """ 1초에 한번 잔고평가를 계산하고 잔고목록 및 잔고평가 데이터를 UI로 보낸다. """
    def UpdateTotaljango(self):
        if len(self.df_jg) > 0:
            tsg = self.df_jg['평가손익'].sum()
            tbg = self.df_jg['매입금액'].sum()
            tpg = self.df_jg['평가금액'].sum()
            bct = len(self.df_jg)
            tsp = round(tsg / tbg * 100, 2)
            ttg = self.dict_intg['예수금'] + tpg
            self.df_tj = pd.DataFrame(
                [[ttg, self.dict_intg['예수금'], bct, tsp, tsg, tbg, tpg]],
                columns=columns_tj, index=[self.str_today]
            )
        else:
            self.df_tj = pd.DataFrame(
                [[self.dict_intg['예수금'], self.dict_intg['예수금'], 0, 0.0, 0, 0, 0]],
                columns=columns_tj, index=[self.str_today]
            )
        self.windowQ.put([ui_num['C잔고목록'], self.df_jg])
        self.windowQ.put([ui_num['C잔고평가'], self.df_tj])

    """
    일별 일현손익 저장, 날짜 변경, 종목당투자금 재계산, 체결목록 및 거래목록 초기화가 진행된다.
    저장확인용 변수 self.bool_save는 0시 1분 이후 첫번째 매수 주문시 False로 재변경된다.
    """
    def SaveTotalGetbalDelcjtd(self):
        df = self.df_tt[['총매수금액', '총매도금액', '총수익금액', '총손실금액', '수익률', '수익금합계']].copy()
        self.query1Q.put([2, df, 'c_totaltradelist', 'append'])
        self.str_today = strf_time('%Y%m%d')
        self.df_cj = pd.DataFrame(columns=columns_cj)
        self.df_td = pd.DataFrame(columns=columns_td)
        self.GetBalances()
        self.dict_bool['실현손익저장'] = True
