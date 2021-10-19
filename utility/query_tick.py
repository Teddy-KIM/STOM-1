import sqlite3
from utility.static import now, timedelta_sec, float2str1p6
from utility.setting import ui_num, DB_STOCK_TICK, DB_COIN_TICK


class QueryTick:
    def __init__(self, qlist):
        """
                    0        1       2        3       4       5          6        7      8      9     10
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceivQ, stockQ, coinQ, sstgQ, cstgQ,
                 tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, wsk1Q, wsk2Q]
                   11       12      13     14      15     16     17
        """
        self.windowQ = qlist[0]
        self.query2Q = qlist[3]
        self.con1 = sqlite3.connect(DB_STOCK_TICK)
        self.con2 = sqlite3.connect(DB_COIN_TICK)
        self.Start()

    def __del__(self):
        self.con1.close()
        self.con2.close()

    def Start(self):
        writetime = now()
        k = 0
        while True:
            query = self.query2Q.get()
            if query[0] == 1:
                try:
                    if len(query) == 2:
                        start = now()
                        for code in list(query[1].keys()):
                            query[1][code].to_sql(code, self.con1, if_exists='append', chunksize=1000)
                        k += 1
                        if k % 4 == 0 and now() > writetime:
                            save_time = float2str1p6((now() - start).total_seconds())
                            text = f'시스템 명령 실행 알림 - 틱데이터 저장 쓰기소요시간은 [{save_time}]초입니다.'
                            self.windowQ.put([ui_num['S단순텍스트'], text])
                            writetime = timedelta_sec(60)
                    elif len(query) == 4:
                        query[1].to_sql(query[2], self.con1, if_exists=query[3], chunksize=1000)
                except Exception as e:
                    self.windowQ.put([ui_num['S단순텍스트'], f'시스템 명령 오류 알림 - to_sql {e}'])
            elif query[0] == 2:
                try:
                    if len(query) == 2:
                        start = now()
                        for code in list(query[1].keys()):
                            query[1][code].to_sql(code, self.con2, if_exists='append', chunksize=1000)
                        save_time = float2str1p6((now() - start).total_seconds())
                        text = f'시스템 명령 실행 알림 - 틱데이터 저장 쓰기소요시간은 [{save_time}]초입니다.'
                        self.windowQ.put([ui_num['C단순텍스트'], text])
                    elif len(query) == 4:
                        query[1].to_sql(query[2], self.con2, if_exists=query[3], chunksize=1000)
                except Exception as e:
                    self.windowQ.put([ui_num['C단순텍스트'], f'시스템 명령 오류 알림 - to_sql {e}'])
