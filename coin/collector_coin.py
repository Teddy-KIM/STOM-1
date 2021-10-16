import os
import sys
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.static import timedelta_sec, now
from utility.setting import columns_cc, ui_num


class CollectorCoin:
    def __init__(self, qlist):
        """
                    0        1       2        3       4       5          6        7      8      9     10
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceivQ, stockQ, coinQ, sstgQ, cstgQ,
                 tick1Q, tick2Q, tick3Q, tick4Q, tick5Q]
                   11       12      13     14      15
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
                self.dict_df[code] = pd.DataFrame([data], columns=columns_cc, index=[dt])
            else:
                self.dict_df[code].at[dt] = data

            if now() > self.time_save:
                gap = (now() - receivetime).total_seconds()
                self.windowQ.put([ui_num['C단순텍스트'], f'콜렉터 수신 기록 알림 - 수신시간과 기록시간의 차이는 [{gap}]초입니다.'])
                self.query2Q.put([2, self.dict_df])
                self.dict_df = {}
                self.time_save = timedelta_sec(60)
        elif len(data) == 23:
            code = data[0]
            del data[0]
            self.dict_ob[code] = data
