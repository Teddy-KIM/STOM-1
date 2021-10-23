import sqlite3
import pandas as pd
from utility.static import strp_time
from utility.setting import ui_num, DB_COIN_TICK, DB_STOCK_TICK


class Chart:
    def __init__(self, qlist):
        """
                    0        1       2        3       4       5          6        7      8      9     10
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceivQ, stockQ, coinQ, sstgQ, cstgQ,
                 tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, wsk1Q, wsk2Q, chartQ]
                   11       12      13     14      15     16     17      18
        """
        self.windowQ = qlist[0]
        self.chartQ = qlist[18]
        self.Start()

    def Start(self):
        while True:
            data = self.chartQ.get()
            coin = data[0]
            code = data[1]
            name = data[2]
            tickcount = data[3]
            searchdate = data[4]

            if coin:
                con = sqlite3.connect(DB_COIN_TICK)
            else:
                con = sqlite3.connect(DB_STOCK_TICK)

            try:
                df = pd.read_sql(f"SELECT * FROM '{code}' WHERE `index` LIKE '{searchdate}%'", con).set_index('index')
                con.close()
            except pd.io.sql.DatabaseError:
                self.windowQ.put([ui_num['차트'], '차트오류', name, ''])
            else:
                if len(df) == 0:
                    self.windowQ.put([ui_num['차트'], '차트오류', name, ''])
                else:
                    try:
                        if coin:
                            df['체결강도'] = df['누적매수량'] / df['누적매도량'] * 100
                            df['체결강도'] = df['체결강도'].apply(lambda x: 500 if x > 500 else round(x, 2))
                        df['직전체결강도'] = df['체결강도'].shift(1)
                        df['직전당일거래대금'] = df['당일거래대금'].shift(1)
                        df = df.fillna(method='bfill')
                        df['초당거래대금'] = df['당일거래대금'] - df['직전당일거래대금']
                        df.at[df.index[0], '초당거래대금'] = 0
                        df['직전초당거래대금'] = df['초당거래대금'].shift(1)
                        df = df.fillna(method='bfill')
                        df['초당거래대금평균'] = df['직전초당거래대금'].rolling(window=tickcount).mean()
                        df['체결강도평균'] = df['직전체결강도'].rolling(window=tickcount).mean()
                        df['최고체결강도'] = df['직전체결강도'].rolling(window=tickcount).max()
                        df['체결시간'] = df.index
                        df['체결시간'] = df['체결시간'].apply(lambda x: strp_time('%Y%m%d%H%M%S', x))
                        df = df.set_index('체결시간')
                        df = df[['현재가', '체결강도', '체결강도평균', '최고체결강도', '초당거래대금', '초당거래대금평균']].copy()
                        unix_ts = [x.timestamp() - 32400 for x in df.index]
                        self.windowQ.put([ui_num['차트'], df, name, unix_ts])
                    except Exception as e:
                        text = f'시스템 명령 오류 알림 - Chart {e}'
                        if coin:
                            self.windowQ.put([ui_num['C단순텍스트'], text])
                        else:
                            self.windowQ.put([ui_num['S단순텍스트'], text])
