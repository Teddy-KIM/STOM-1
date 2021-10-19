import os
import sys
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.setting import ui_num
from utility.static import timedelta_sec, now, float2str1p6


class CollectorCoin:
    def __init__(self, qlist):
        """
                    0        1       2        3       4       5          6        7      8      9     10
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceivQ, stockQ, coinQ, sstgQ, cstgQ,
                 tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, wsk1Q, wsk2Q]
                   11       12      13     14      15     16     17
        """
        self.windowQ = qlist[0]
        self.query2Q = qlist[3]
        self.tick5Q = qlist[15]
        self.dict_df = {}                   # 틱데이터 저장용 딕셔너리 key: code, value: datafame
        self.dict_ob = {}                   # 오더북 저장용 딕셔너리
        self.time_save = timedelta_sec(60)  # 틱데이터 저장주기 확인용
        self.Start()

    def Start(self):
        while True:
            data = self.tick5Q.get()
            self.UpdateTickData(data)

    def UpdateTickData(self, data):
        if len(data) == 13:
            code = data[-3]
            dt = data[-2]
            receivetime = data[-1]
            del data[-3:]

            if code not in self.dict_ob.keys():
                return

            data += self.dict_ob[code]
            if code not in self.dict_df.keys():
                columns = [
                    '현재가', '시가', '고가', '저가', '등락율', '당일거래대금', '초당매수수량', '초당매도수량',
                    '누적매수량', '누적매도량', '매도총잔량', '매수총잔량',
                    '매도호가5', '매도호가4', '매도호가3', '매도호가2', '매도호가1',
                    '매수호가1', '매수호가2', '매수호가3', '매수호가4', '매수호가5',
                    '매도잔량5', '매도잔량4', '매도잔량3', '매도잔량2', '매도잔량1',
                    '매수잔량1', '매수잔량2', '매수잔량3', '매수잔량4', '매수잔량5'
                ]
                self.dict_df[code] = pd.DataFrame([data], columns=columns, index=[dt])
            else:
                self.dict_df[code].at[dt] = data

            if now() > self.time_save:
                gap = float2str1p6((now() - receivetime).total_seconds())
                self.windowQ.put([ui_num['C단순텍스트'], f'콜렉터 수신 기록 알림 - 수신시간과 기록시간의 차이는 [{gap}]초입니다.'])
                self.query2Q.put([2, self.dict_df])
                self.dict_df = {}
                self.time_save = timedelta_sec(60)
        elif len(data) == 23:
            code = data[0]
            del data[0]
            self.dict_ob[code] = data
