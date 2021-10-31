import os
import sys
import time
import pyupbit
import pandas as pd
from threading import Timer
from pyupbit import WebSocketManager
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.setting import ui_num, DICT_SET
from utility.static import now, strf_time, strp_time, timedelta_hour, timedelta_sec


class WebsTicker:
    def __init__(self, qlist):
        """
                    0        1       2        3       4       5          6          7        8      9
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceiv1Q, creceiv2Q, stockQ, coinQ,
                 sstgQ, cstgQ, tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, chartQ]
                   10    11      12      13      14      15      16      17
        """
        self.windowQ = qlist[0]
        self.query2Q = qlist[3]
        self.creceiv1Q = qlist[6]
        self.coinQ = qlist[9]
        self.cstgQ = qlist[11]
        self.tick5Q = qlist[16]
        self.hogaQ = qlist[18]
        self.dict_set = DICT_SET

        self.dict_cdjm = {}
        self.dict_time = {
            '티커리스트재조회': now(),
            '거래대금순위기록': now(),
            '거래대금순위저장': now()
        }

        self.list_gsjm1 = []
        self.list_gsjm2 = []
        self.list_jang = []
        self.pre_top = []

        self.df_mt = pd.DataFrame(columns=['거래대금순위'])
        self.df_mc = pd.DataFrame(columns=['최근거래대금'])

        self.str_jcct = strf_time('%Y%m%d') + '000000'
        self.dt_mtct = None
        self.hoga_code = None
        self.websQ_ticker = None
        self.codes = None

        Timer(10, self.MoneyTopSearch).start()

        self.Start()

    def Start(self):
        """ get_tickers 리턴 리스트의 갯수가 다른 버그 발견, 1초 간격 3회 조회 후 길이가 긴 리스트를 티커리스트로 정한다 """
        self.windowQ.put([ui_num['C단순텍스트'], '시스템 명령 실행 알림 - 리시버 시작'])
        dict_tsbc = {}
        self.GetTickers()
        self.websQ_ticker = WebSocketManager('ticker', self.codes)
        while True:
            if not self.creceiv1Q.empty():
                data = self.creceiv1Q.get()
                if type(data) == str:
                    if data == 'terminate':
                        self.websQ_ticker.terminate()
                        break
                    else:
                        self.hoga_code = data
                elif type(data) == list:
                    self.UpdateJangolist(data)
                elif type(data) == dict:
                    self.dict_set = data

            data = self.websQ_ticker.get()
            if data == 'ConnectionClosedError':
                self.windowQ.put([ui_num['C단순텍스트'], '시스템 명령 오류 알림 - WebsTicker 연결 끊김으로 다시 연결합니다.'])
                self.websQ_ticker = WebSocketManager('ticker', self.codes)
            else:
                code = data['code']
                v = data['trade_volume']
                gubun = data['ask_bid']
                dt = data['trade_date'] + data['trade_time']
                dt = strf_time('%Y%m%d%H%M%S', timedelta_hour(9, strp_time('%Y%m%d%H%M%S', dt)))
                if dt != self.str_jcct and int(dt) > int(self.str_jcct):
                    self.str_jcct = dt

                try:
                    pret, bids, asks = dict_tsbc[code]
                except KeyError:
                    pret, bids, asks = None, 0, 0
                if gubun == 'BID':
                    dict_tsbc[code] = [dt, bids + v, asks]
                else:
                    dict_tsbc[code] = [dt, bids, asks + v]
                    v = -v

                tbids = data['acc_bid_volume']
                tasks = data['acc_ask_volume']
                c = data['trade_price']
                o = data['opening_price']
                h = data['high_price']
                low = data['low_price']
                per = round(data['signed_change_rate'] * 100, 2)
                dm = data['acc_trade_price']
                try:
                    ch = round(tbids / tasks * 100, 2)
                except ZeroDivisionError:
                    ch = 500.
                if ch > 500:
                    ch = 500.

                if self.hoga_code == code:
                    self.hogaQ.put([code, c, per, 0, o, h, low])
                    self.hogaQ.put([code, v, ch])

                if dt != pret:
                    bids = dict_tsbc[code][1]
                    asks = dict_tsbc[code][2]
                    dict_tsbc[code] = [dt, 0, 0]
                    self.UpdateTickData(c, o, h, low, per, dm, ch, bids, asks, tbids, tasks, code, dt, now())

                if now() > self.dict_time['거래대금순위기록']:
                    if len(self.list_gsjm1) > 0:
                        self.UpdateMoneyTop()
                    self.dict_time['거래대금순위기록'] = timedelta_sec(1)

                if now() > self.dict_time['티커리스트재조회']:
                    codes = pyupbit.get_tickers(fiat="KRW")
                    if len(codes) > len(self.codes):
                        self.codes = codes
                        self.websQ_ticker.terminate()
                        time.sleep(2)
                        self.websQ_ticker = WebSocketManager('ticker', self.codes)
                    self.dict_time['티커리스트재조회'] = timedelta_sec(600)

    def GetTickers(self):
        codes = pyupbit.get_tickers(fiat="KRW")
        time.sleep(1)
        codes2 = pyupbit.get_tickers(fiat="KRW")
        codes = codes2 if len(codes2) > len(codes) else codes
        time.sleep(1)
        codes2 = pyupbit.get_tickers(fiat="KRW")
        codes = codes2 if len(codes2) > len(codes) else codes
        self.codes = codes

        df_mc = pd.DataFrame(columns=['최근거래대금'])
        for code in self.codes:
            if 90000 < int(strf_time('%H%M%S')) < 100000:
                df = pyupbit.get_ohlcv(ticker=code, interval='minute1', count=1)
                if df is not None:
                    df_mc.at[code] = df['close'][0] * df['volume'][0]
            else:
                df = pyupbit.get_ohlcv(ticker=code, interval='minute3', count=1)
                if df is not None:
                    df_mc.at[code] = df['close'][0] * df['volume'][0]
            time.sleep(0.05)
        df_mc.sort_values(by=['최근거래대금'], ascending=False, inplace=True)
        list_top = list(df_mc.index[:self.dict_set['코인순위선정']])
        for code in list_top:
            self.InsertGsjmlist(code)

    def UpdateJangolist(self, data):
        code = data[1]
        if '잔고편입' in data and code not in self.list_jang:
            self.list_jang.append(code)
            if code not in self.list_gsjm2:
                self.cstgQ.put(['조건진입', code])
                self.list_gsjm2.append(code)
        elif '잔고청산' in data and code in self.list_jang:
            self.list_jang.remove(code)
            if code not in self.list_gsjm1 and code in self.list_gsjm2:
                self.cstgQ.put(['조건이탈', code])
                self.list_gsjm2.remove(code)

    def MoneyTopSearch(self):
        self.df_mc.sort_values(by=['최근거래대금'], ascending=False, inplace=True)
        list_top = list(self.df_mc.index[:self.dict_set['코인순위선정']])
        insert_list = set(list_top) - set(self.pre_top)
        if len(insert_list) > 0:
            for code in list(insert_list):
                self.InsertGsjmlist(code)
        delete_list = set(self.pre_top) - set(list_top)
        if len(delete_list) > 0:
            for code in list(delete_list):
                self.DeleteGsjmlist(code)
        self.pre_top = list_top
        Timer(10, self.MoneyTopSearch).start()

    def InsertGsjmlist(self, code):
        if code not in self.list_gsjm1:
            self.list_gsjm1.append(code)
        if code not in self.list_jang and code not in self.list_gsjm2:
            if self.dict_set['코인트레이더']:
                self.cstgQ.put(['조건진입', code])
            self.list_gsjm2.append(code)

    def DeleteGsjmlist(self, code):
        if code in self.list_gsjm1:
            self.list_gsjm1.remove(code)
        if code not in self.list_jang and code in self.list_gsjm2:
            if self.dict_set['코인트레이더']:
                self.cstgQ.put(['조건이탈', code])
            self.list_gsjm2.remove(code)

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
            self.query2Q.put([2, self.df_mt, 'moneytop', 'append'])
            self.df_mt = pd.DataFrame(columns=['거래대금순위'])
            self.dict_time['거래대금순위저장'] = timedelta_sec(10)

    def UpdateTickData(self, c, o, h, low, per, dm, ch, bids, asks, tbids, tasks, code, dt, receivetime):
        dt_ = dt[:13]
        if code not in self.dict_cdjm.keys():
            columns = ['10초누적거래대금', '10초전당일거래대금']
            self.dict_cdjm[code] = pd.DataFrame([[0, dm]], columns=columns, index=[dt_])
        elif dt_ != self.dict_cdjm[code].index[-1]:
            predm = self.dict_cdjm[code]['10초전당일거래대금'][-1]
            self.dict_cdjm[code].at[dt_] = dm - predm, dm
            if len(self.dict_cdjm[code]) == self.dict_set['코인순위시간'] * 6:
                if per > 0:
                    self.df_mc.at[code] = self.dict_cdjm[code]['10초누적거래대금'].sum()
                elif code in self.df_mc.index:
                    self.df_mc.drop(index=code, inplace=True)
                self.dict_cdjm[code].drop(index=self.dict_cdjm[code].index[0], inplace=True)

        data = [c, o, h, low, per, dm, ch, bids, asks, tbids, tasks, code, dt, receivetime]
        if self.dict_set['코인트레이더'] and code in self.list_gsjm2:
            injango = code in self.list_jang
            self.cstgQ.put(data + [injango])
            if injango:
                self.coinQ.put([code, c])

        if self.dict_set['코인콜렉터']:
            del data[6]
            self.tick5Q.put(data)


class WebsOrderbook:
    def __init__(self, qlist):
        """
                    0        1       2        3       4       5          6          7        8      9
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceiv1Q, creceiv2Q, stockQ, coinQ,
                 sstgQ, cstgQ, tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, chartQ, hogaQ]
                   10    11      12      13      14      15      16      17     18
        """
        self.windowQ = qlist[0]
        self.creceiv2Q = qlist[7]
        self.coinQ = qlist[9]
        self.cstgQ = qlist[11]
        self.tick5Q = qlist[16]
        self.hogaQ = qlist[18]
        self.dict_set = DICT_SET
        self.time_tickers = now()
        self.hoga_code = None
        self.websQ_order = None
        self.Start()

    def Start(self):
        """ get_tickers 리턴 리스트의 갯수가 다른 버그 발견, 1초 간격 3회 조회 후 길이가 긴 리스트를 티커리스트로 정한다 """
        codes = pyupbit.get_tickers(fiat="KRW")
        time.sleep(1)
        codes2 = pyupbit.get_tickers(fiat="KRW")
        codes = codes2 if len(codes2) > len(codes) else codes
        time.sleep(1)
        codes2 = pyupbit.get_tickers(fiat="KRW")
        codes = codes2 if len(codes2) > len(codes) else codes
        self.websQ_order = WebSocketManager('orderbook', codes)
        while True:
            if not self.creceiv2Q.empty():
                data = self.creceiv2Q.get()
                if type(data) == str:
                    if data == 'terminate':
                        self.websQ_order.terminate()
                        break
                    else:
                        self.hoga_code = data
                elif type(data) == dict:
                    self.dict_set = data

            data = self.websQ_order.get()
            if data == 'ConnectionClosedError':
                self.windowQ.put([ui_num['C단순텍스트'], '시스템 명령 오류 알림 - WebsOrderbook 연결 끊김으로 다시 연결합니다.'])
                self.websQ_order = WebSocketManager('orderbook', codes)
            else:
                code = data['code']
                tsjr = data['total_ask_size']
                tbjr = data['total_bid_size']
                s5hg = data['orderbook_units'][4]['ask_price']
                s4hg = data['orderbook_units'][3]['ask_price']
                s3hg = data['orderbook_units'][2]['ask_price']
                s2hg = data['orderbook_units'][1]['ask_price']
                s1hg = data['orderbook_units'][0]['ask_price']
                b1hg = data['orderbook_units'][0]['bid_price']
                b2hg = data['orderbook_units'][1]['bid_price']
                b3hg = data['orderbook_units'][2]['bid_price']
                b4hg = data['orderbook_units'][3]['bid_price']
                b5hg = data['orderbook_units'][4]['bid_price']
                s5jr = data['orderbook_units'][4]['ask_size']
                s4jr = data['orderbook_units'][3]['ask_size']
                s3jr = data['orderbook_units'][2]['ask_size']
                s2jr = data['orderbook_units'][1]['ask_size']
                s1jr = data['orderbook_units'][0]['ask_size']
                b1jr = data['orderbook_units'][0]['bid_size']
                b2jr = data['orderbook_units'][1]['bid_size']
                b3jr = data['orderbook_units'][2]['bid_size']
                b4jr = data['orderbook_units'][3]['bid_size']
                b5jr = data['orderbook_units'][4]['bid_size']
                data = [code, tsjr, tbjr,
                        s5hg, s4hg, s3hg, s2hg, s1hg, b1hg, b2hg, b3hg, b4hg, b5hg,
                        s5jr, s4jr, s3jr, s2jr, s1jr, b1jr, b2jr, b3jr, b4jr, b5jr]
                if self.dict_set['코인콜렉터']:
                    self.tick5Q.put(data)
                if self.dict_set['코인트레이더']:
                    self.cstgQ.put(data)
                if self.hoga_code == code:
                    self.hogaQ.put(data)

            if now() > self.time_tickers:
                codes2 = pyupbit.get_tickers(fiat="KRW")
                if len(codes2) > len(codes):
                    self.websQ_order.terminate()
                    time.sleep(2)
                    self.websQ_order = WebSocketManager('orderbook', codes)
                self.time_tickers = timedelta_sec(600)
