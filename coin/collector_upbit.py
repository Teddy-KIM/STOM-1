import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.static import *
from utility.setting import *


class CollectorUpbit:
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
        self.dict_orderbook = {}            # 오더북 저장용 딕셔너리
        self.time_save = timedelta_sec(60)  # 틱데이터 저장주기 확인용
        self.Start()

    def Start(self):
        while True:
            data = self.tick5Q.get()
            if len(data) == 13:
                self.UpdateTickData(data)
            elif len(data) == 23:
                self.UpdateOrderbook(data)

    def UpdateTickData(self, data):
        code = data[-3]
        dt = data[-2]
        receivetime = data[-1]
        del data[-3:]

        if code not in self.dict_orderbook.keys():
            return

        data += self.dict_orderbook[code]

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

    def UpdateOrderbook(self, data):
        code = data[0]
        del data[0]
        self.dict_orderbook[code] = data
