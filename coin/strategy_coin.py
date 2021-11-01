import os
import sys
import sqlite3
import numpy as np
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.static import now, strf_time, timedelta_sec, float2str1p6, strp_time
from utility.setting import DB_COIN_STRATEGY, DICT_SET, ui_num, columns_gj


class StrategyCoin:
    def __init__(self, qlist):
        """
                    0        1       2        3       4       5          6          7        8      9
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceiv1Q, creceiv2Q, stockQ, coinQ,
                 sstgQ, cstgQ, tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, chartQ, hogaQ]
                   10    11      12      13      14      15      16      17     18
        """
        self.windowQ = qlist[0]
        self.query2Q = qlist[3]
        self.coinQ = qlist[9]
        self.cstgQ = qlist[11]
        self.chartQ = qlist[17]
        self.dict_set = DICT_SET
        self.chart_code = None

        con = sqlite3.connect(DB_COIN_STRATEGY)
        dfb = pd.read_sql('SELECT * FROM buy', con).set_index('index')
        dfs = pd.read_sql('SELECT * FROM sell', con).set_index('index')
        con.close()

        self.buystrategy1 = None
        if self.dict_set['코인장초매수전략'] != '':
            self.buystrategy1 = compile(dfb['전략코드'][self.dict_set['코인장초매수전략']], '<string>', 'exec')
        self.sellstrategy1 = None
        if self.dict_set['코인장초매도전략'] != '':
            self.sellstrategy1 = compile(dfs['전략코드'][self.dict_set['코인장초매도전략']], '<string>', 'exec')

        self.buystrategy2 = None
        if self.dict_set['코인장중매수전략'] != '':
            self.buystrategy2 = compile(dfb['전략코드'][self.dict_set['코인장중매수전략']], '<string>', 'exec')
        self.sellstrategy2 = None
        if self.dict_set['코인장중매도전략'] != '':
            self.sellstrategy2 = compile(dfs['전략코드'][self.dict_set['코인장중매도전략']], '<string>', 'exec')

        self.list_buy = []      # 매수주문리스트
        self.list_sell = []     # 매도주문리스트
        self.int_tujagm = 0     # 종목당 투자금

        self.dict_gsjm = {}     # key: 종목코드, value: DataFrame
        self.dict_hgjr = {}     # key: 종목코드, value: list
        self.dict_data = {}     # key: 종목코드, value: list
        self.dict_high = {}     # key: 종목코드, value: float
        self.dict_bool = {
            '장초전략시작': True if 90000 < int(strf_time('%H%M%S')) < 100000 else False,
            '장중전략시작': False if 90000 < int(strf_time('%H%M%S')) < 100000 else True
        }
        self.dict_time = {
            '관심종목': now(),
            '연산시간': now(),
            '거래대금순위기록': now(),
            '거래대금순위저장': now()
        }
        self.Start()

    def Start(self):
        self.windowQ.put([ui_num['C단순텍스트'], '시스템 명령 실행 알림 - 전략 연산 시작'])
        while True:
            data = self.cstgQ.get()
            if type(data) == str:
                self.chart_code = data
            elif type(data) == int:
                self.int_tujagm = data
            elif type(data) == list:
                if len(data) == 2:
                    self.UpdateList(data[0], data[1])
                elif len(data) == 23:
                    self.UpdateOrderbook(data)
                elif len(data) == 15:
                    self.BuyStrategy(data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                                     data[8], data[9], data[10], data[11], data[12], data[13], data[14])
                elif len(data) == 5:
                    self.SellStrategy(data[0], data[1], data[2], data[3], data[4])
            elif type(data) == dict:
                self.dict_set = data
                self.UpdateStrategy()

            if now() > self.dict_time['관심종목']:
                self.windowQ.put([ui_num['C관심종목'], self.dict_gsjm])
                self.dict_time['관심종목'] = timedelta_sec(1)

    def UpdateList(self, gubun, code):
        if gubun == '조건진입':
            if code not in self.dict_gsjm.keys():
                data = np.zeros((301, len(columns_gj))).tolist()
                df = pd.DataFrame(data, columns=columns_gj)
                df['체결시간'] = strf_time('%Y%m%d%H%M%S')
                self.dict_gsjm[code] = df.copy()
        elif gubun == '조건이탈':
            if code in self.dict_gsjm.keys():
                del self.dict_gsjm[code]
        elif gubun in ['매수완료', '매수취소']:
            if code in self.list_buy:
                self.list_buy.remove(code)
        elif gubun in ['매도완료', '매도취소']:
            if code in self.list_sell:
                self.list_sell.remove(code)
            if code in self.dict_high.keys():
                del self.dict_high[code]
        elif gubun == '매수전략':
            if 90000 < int(strf_time('%H%M%S')) < 100000:
                self.buystrategy1 = compile(code, '<string>', 'exec')
            else:
                self.buystrategy2 = compile(code, '<string>', 'exec')
        elif gubun == '매도전략':
            if 90000 < int(strf_time('%H%M%S')) < 100000:
                self.sellstrategy1 = compile(code, '<string>', 'exec')
            else:
                self.sellstrategy2 = compile(code, '<string>', 'exec')
        elif gubun == '매수전략중지':
            if 90000 < int(strf_time('%H%M%S')) < 100000:
                self.buystrategy1 = None
            else:
                self.buystrategy2 = None
        elif gubun == '매도전략중지':
            if 90000 < int(strf_time('%H%M%S')) < 100000:
                self.sellstrategy1 = None
            else:
                self.sellstrategy2 = None

    def UpdateOrderbook(self, data):
        code = data[0]
        data.remove(code)
        self.dict_hgjr[code] = data

    def BuyStrategy(self, 현재가, 시가, 고가, 저가, 등락율, 당일거래대금, 체결강도, 초당매수수량, 초당매도수량,
                    누적매수량, 누적매도량, 종목명, 체결시간, 수신시간, 잔고종목):
        if 종목명 not in self.dict_gsjm.keys():
            return

        self.CheckStrategy()

        if 90000 < int(strf_time('%H%M%S')) < 100000:
            평균값계산틱수 = self.dict_set['코인장초평균값계산틱수']
        else:
            평균값계산틱수 = self.dict_set['코인장중평균값계산틱수']

        고저평균 = (고가 + 저가) / 2
        고저평균대비등락율 = round((현재가 / 고저평균 - 1) * 100, 2)
        직전당일거래대금 = self.dict_gsjm[종목명]['당일거래대금'][0]
        초당거래대금 = 0 if 직전당일거래대금 == 0 else int(당일거래대금 - 직전당일거래대금)

        self.dict_gsjm[종목명] = self.dict_gsjm[종목명].shift(1)
        if self.dict_gsjm[종목명]['체결강도'][평균값계산틱수] != 0.:
            초당거래대금평균 = int(self.dict_gsjm[종목명]['초당거래대금'][1:평균값계산틱수 + 1].mean())
            체결강도평균 = round(self.dict_gsjm[종목명]['체결강도'][1:평균값계산틱수 + 1].mean(), 2)
            최고체결강도 = round(self.dict_gsjm[종목명]['체결강도'][1:평균값계산틱수 + 1].max(), 2)
            self.dict_gsjm[종목명].at[0] = 등락율, 고저평균대비등락율, 초당거래대금, 초당거래대금평균, 당일거래대금, \
                체결강도, 체결강도평균, 최고체결강도, 현재가, 체결시간
            if self.chart_code == 종목명:
                self.chartQ.put([self.dict_gsjm[종목명], 종목명])

            매수 = True
            직전체결강도 = self.dict_gsjm[종목명]['체결강도'][1]
            매도총잔량, 매수총잔량, \
                매도호가5, 매도호가4, 매도호가3, 매도호가2, 매도호가1, 매수호가1, 매수호가2, 매수호가3, 매수호가4, 매수호가5, \
                매도잔량5, 매도잔량4, 매도잔량3, 매도잔량2, 매도잔량1, 매수잔량1, 매수잔량2, 매수잔량3, 매수잔량4, 매수잔량5 = \
                self.dict_hgjr[종목명]
            self.dict_data[종목명] = [
                현재가, 시가, 고가, 저가, 등락율, 고저평균대비등락율, 당일거래대금, 초당거래대금, 초당거래대금평균, 초당매수수량,
                초당매도수량, 누적매수량, 누적매도량, 체결강도, 체결강도평균, 최고체결강도, 직전체결강도, 매도총잔량, 매수총잔량,
                매도호가5, 매도호가4, 매도호가3, 매도호가2, 매도호가1, 매수호가1, 매수호가2, 매수호가3, 매수호가4, 매수호가5,
                매도잔량5, 매도잔량4, 매도잔량3, 매도잔량2, 매도잔량1, 매수잔량1, 매수잔량2, 매수잔량3, 매수잔량4, 매수잔량5
            ]
            if 잔고종목:
                return
            if 종목명 in self.list_buy:
                return

            if 90000 < int(strf_time('%H%M%S')) < 100000:
                if self.buystrategy1 is not None:
                    try:
                        exec(self.buystrategy1, None, locals())
                    except Exception as e:
                        self.windowQ.put([ui_num['C단순텍스트'], f'전략스 설정 오류 알림 - BuyStrategy {e}'])
            else:
                if self.buystrategy2 is not None:
                    try:
                        exec(self.buystrategy2, None, locals())
                    except Exception as e:
                        self.windowQ.put([ui_num['C단순텍스트'], f'전략스 설정 오류 알림 - BuyStrategy {e}'])
        else:
            self.dict_gsjm[종목명].at[0] = 등락율, 고저평균대비등락율, 초당거래대금, 0, 당일거래대금, \
                체결강도, 0., 0., 현재가, 체결시간

        if now() > self.dict_time['연산시간']:
            gap = float2str1p6((now() - 수신시간).total_seconds())
            self.windowQ.put([ui_num['C단순텍스트'], f'전략스 연산 시간 알림 - 수신시간과 연산시간의 차이는 [{gap}]초입니다.'])
            self.dict_time['연산시간'] = timedelta_sec(60)

    def SellStrategy(self, 종목명, 수익률, 보유수량, 현재가, 매수시간):
        if 종목명 not in self.dict_gsjm.keys() or 종목명 not in self.dict_hgjr.keys() or 종목명 not in self.dict_data.keys():
            return
        if 종목명 in self.list_sell:
            return

        매도 = False
        현재가, 시가, 고가, 저가, 등락율, 고저평균대비등락율, 당일거래대금, 초당거래대금, 초당거래대금평균, 초당매수수량, \
            초당매도수량, 누적매수량, 누적매도량, 체결강도, 체결강도평균, 최고체결강도, 직전체결강도, 매도총잔량, 매수총잔량, \
            매도호가5, 매도호가4, 매도호가3, 매도호가2, 매도호가1, 매수호가1, 매수호가2, 매수호가3, 매수호가4, 매수호가5, \
            매도잔량5, 매도잔량4, 매도잔량3, 매도잔량2, 매도잔량1, 매수잔량1, 매수잔량2, 매수잔량3, 매수잔량4, 매수잔량5 = \
            self.dict_data[종목명]

        if 종목명 not in self.dict_high.keys():
            self.dict_high[종목명] = 수익률
        elif 수익률 > self.dict_high[종목명]:
            self.dict_high[종목명] = 수익률
        최고수익률 = self.dict_high[종목명]

        if 90000 < int(strf_time('%H%M%S')) < 100000:
            if self.sellstrategy1 is not None:
                try:
                    exec(self.sellstrategy1, None, locals())
                except Exception as e:
                    self.windowQ.put([ui_num['C단순텍스트'], f'전략스 설정 오류 알림 - SellStrategy {e}'])
        else:
            if self.sellstrategy2 is not None:
                try:
                    exec(self.sellstrategy2, None, locals())
                except Exception as e:
                    self.windowQ.put([ui_num['C단순텍스트'], f'전략스 설정 오류 알림 - SellStrategy {e}'])

    def CheckStrategy(self):
        if 90000 < int(strf_time('%H%M%S')) < 100000:
            if not self.dict_bool['장초전략시작']:
                self.dict_bool['장초전략시작'] = True
                self.dict_bool['장중전략시작'] = False
        else:
            if not self.dict_bool['장중전략시작']:
                self.dict_bool['장중전략시작'] = True
                self.dict_bool['장초전략시작'] = False

    def UpdateStrategy(self):
        con = sqlite3.connect(DB_COIN_STRATEGY)
        dfb = pd.read_sql('SELECT * FROM buy', con).set_index('index')
        dfs = pd.read_sql('SELECT * FROM sell', con).set_index('index')
        con.close()
        self.buystrategy1 = compile(dfb['전략코드'][self.dict_set['코인장초매수전략']], '<string>', 'exec')
        self.sellstrategy1 = compile(dfs['전략코드'][self.dict_set['코인장초매도전략']], '<string>', 'exec')
        self.buystrategy2 = compile(dfb['전략코드'][self.dict_set['코인장중매수전략']], '<string>', 'exec')
        self.sellstrategy2 = compile(dfs['전략코드'][self.dict_set['코인장중매도전략']], '<string>', 'exec')
