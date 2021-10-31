import os
import sys
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.setting import ui_num, DICT_SET
from utility.static import now, strf_time, timedelta_sec, float2str1p6


class CollectorStock:
    def __init__(self, gubun, qlist):
        """
                    0        1       2        3       4       5          6          7        8      9
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceiv1Q, creceiv2Q, stockQ, coinQ,
                 sstgQ, cstgQ, tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, chartQ, hogaQ]
                   10    11      12      13      14      15      16      17     18
        """
        self.gubun = gubun
        self.windowQ = qlist[0]
        self.query2Q = qlist[3]
        if self.gubun == 1:
            self.tickQ = qlist[12]
        elif self.gubun == 2:
            self.tickQ = qlist[13]
        elif self.gubun == 3:
            self.tickQ = qlist[14]
        elif self.gubun == 4:
            self.tickQ = qlist[15]
        self.dict_set = DICT_SET

        self.dict_df = {}
        self.dict_time = {
            '기록시간': now(),
            '저장시간': timedelta_sec(int(self.dict_set['주식저장주기']))
        }
        self.time_info = now()
        self.str_tday = strf_time('%Y%m%d')
        self.Start()

    def Start(self):
        if self.gubun == 4:
            self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 콜렉터 시작'])
        while True:
            data = self.tickQ.get()
            if type(data) == list:
                if len(data) != 2:
                    self.UpdateTickData(data)
                elif data[0] == '콜렉터종료':
                    if not self.dict_set['주식실시간저장']:
                        self.SaveTickData(data[1])
                    break
            elif type(data) == dict:
                self.dict_set = data

        if self.gubun == 4:
            self.windowQ.put([ui_num['S단순텍스트'], '시스템 명령 실행 알림 - 콜렉터 종료'])
        sys.exit()

    def UpdateTickData(self, data):
        code = data[-3]
        dt = data[-2]
        receivetime = data[-1]
        del data[-3:]

        if code not in self.dict_df.keys():
            columns = [
                '현재가', '시가', '고가', '저가', '등락율', '당일거래대금', '체결강도',
                '초당매수수량', '초당매도수량', 'VI해제시간', 'VI아래5호가', '매도총잔량', '매수총잔량',
                '매도호가5', '매도호가4', '매도호가3', '매도호가2', '매도호가1',
                '매수호가1', '매수호가2', '매수호가3', '매수호가4', '매수호가5',
                '매도잔량5', '매도잔량4', '매도잔량3', '매도잔량2', '매도잔량1',
                '매수잔량1', '매수잔량2', '매수잔량3', '매수잔량4', '매수잔량5'
            ]
            self.dict_df[code] = pd.DataFrame([data], columns=columns, index=[dt])
        else:
            self.dict_df[code].at[dt] = data

        if self.gubun == 4 and now() > self.dict_time['기록시간']:
            gap = float2str1p6((now() - receivetime).total_seconds())
            self.windowQ.put([ui_num['S단순텍스트'], f'콜렉터 수신 기록 알림 - 수신시간과 기록시간의 차이는 [{gap}]초입니다.'])
            self.dict_time['기록시간'] = timedelta_sec(60)

        if self.dict_set['주식실시간저장'] and now() > self.dict_time['저장시간']:
            self.query2Q.put([1, self.dict_df])
            self.dict_df = {}
            self.dict_time['저장시간'] = timedelta_sec(int(self.dict_set['주식저장주기']))

    def SaveTickData(self, codes):
        if not self.dict_set['주식전체종목저장']:
            for code in list(self.dict_df.keys()):
                if code not in codes:
                    del self.dict_df[code]
        self.query2Q.put([1, self.dict_df, '장마감후저장'])
