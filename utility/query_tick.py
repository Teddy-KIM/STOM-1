import sqlite3
import pandas as pd
from utility.static import now, float2str1p6
from utility.setting import ui_num, DB_STOCK_TICK, DB_COIN_TICK


class QueryTick:
    def __init__(self, qlist):
        """
                    0        1       2        3       4       5          6          7        8      9
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceiv1Q, creceiv2Q, stockQ, coinQ,
                 sstgQ, cstgQ, tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, chartQ, hogaQ]
                   10    11      12      13      14      15      16      17     18
        """
        self.windowQ = qlist[0]
        self.query2Q = qlist[3]
        self.con1 = sqlite3.connect(DB_STOCK_TICK)
        self.cur1 = self.con1.cursor()
        self.cur1.execute('pragma journal_mode=WAL')
        self.cur1.execute('pragma synchronous=normal')
        self.cur1.execute('pragma temp_store=memory')
        self.con2 = sqlite3.connect(DB_COIN_TICK)
        self.cur2 = self.con2.cursor()
        self.cur2.execute('pragma journal_mode=WAL')
        self.cur2.execute('pragma synchronous=normal')
        self.cur2.execute('pragma temp_store=memory')
        self.list_coin_table = []
        self.stock_trigger = False
        self.remove_trigger1()
        self.remove_trigger2()
        self.Start()

    def __del__(self):
        self.con1.close()
        self.con2.close()

    def Start(self):
        k, j = 0, 0
        dfs = pd.DataFrame()
        self.create_trigger2()
        while True:
            query = self.query2Q.get()
            if query == '주식디비트리거시작':
                self.create_trigger1()
                self.stock_trigger = True
            elif query[0] == 1:
                try:
                    if len(query) == 2:
                        if type(query[1]) == str:
                            self.cur1.execute(query[1])
                            self.con1.commit()
                        else:
                            k += 1
                            for code in list(query[1].keys()):
                                query[1][code]['종목코드'] = code
                                dfs = dfs.append(query[1][code])
                            if k % 4 == 0 and self.stock_trigger:
                                start = now()
                                dfs.to_sql("temp", self.con1, if_exists='append', chunksize=1000, method='multi')
                                self.cur1.execute('INSERT INTO "dist" ("cnt") values (1);')
                                save_time = float2str1p6((now() - start).total_seconds())
                                text = f'시스템 명령 실행 알림 - 틱데이터 저장 쓰기소요시간은 [{save_time}]초입니다.'
                                self.windowQ.put([ui_num['S단순텍스트'], text])
                                dfs = pd.DataFrame()
                    elif len(query) == 3:
                        start = now()
                        j += 1
                        last = len(list(query[1].keys()))
                        for i, code in enumerate(list(query[1].keys())):
                            text = f'시스템 명령 실행 알림 - 시스템 명령 실행 알림 - 틱데이터 저장 중 ... [{j}]{i+1}/{last}'
                            self.windowQ.put([ui_num['S단순텍스트'], text])
                            query[1][code].to_sql(code, self.con1, if_exists='append', chunksize=1000, method='multi')
                        save_time = float2str1p6((now() - start).total_seconds())
                        text = f'시스템 명령 실행 알림 - 틱데이터 저장 쓰기소요시간은 [{save_time}]초입니다.'
                        self.windowQ.put([ui_num['S단순텍스트'], text])
                    elif len(query) == 4:
                        query[1].to_sql(query[2], self.con1, if_exists=query[3], chunksize=1000, method='multi')
                except Exception as e:
                    self.windowQ.put([ui_num['S단순텍스트'], f'시스템 명령 오류 알림 - QueryTick con1 {e}'])
            elif query[0] == 2:
                try:
                    if len(query) == 2:
                        start = now()
                        new_codes = set(list(query[1].keys())) - set(self.list_coin_table)
                        if len(new_codes) > 0:
                            for code in list(query[1].keys()):
                                query[1][code].to_sql(code, self.con2, if_exists='append', chunksize=1000, method='multi')
                            self.remove_trigger2()
                            self.create_trigger2()
                        else:
                            dfc = pd.DataFrame()
                            for code in list(query[1].keys()):
                                query[1][code]['종목코드'] = code
                                dfc = dfc.append(query[1][code])
                            dfc.to_sql("temp", self.con2, if_exists='append', chunksize=1000, method='multi')
                            self.cur2.execute('INSERT INTO "dist" ("cnt") values (1);')
                        save_time = float2str1p6((now() - start).total_seconds())
                        text = f'시스템 명령 실행 알림 - 틱데이터 저장 쓰기소요시간은 [{save_time}]초입니다.'
                        self.windowQ.put([ui_num['C단순텍스트'], text])
                    elif len(query) == 4:
                        query[1].to_sql(query[2], self.con2, if_exists=query[3], chunksize=1000, method='multi')
                except Exception as e:
                    self.windowQ.put([ui_num['C단순텍스트'], f'시스템 명령 오류 알림 - QueryTick con2 {e}'])

    def create_trigger1(self):
        res = self.cur1.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_list = []
        for name in res.fetchall():
            table_list.append(name[0])

        const_str = '"index", 현재가, 시가, 고가, 저가, 등락율, 당일거래대금, 체결강도, 초당매수수량, 초당매도수량, VI해제시간,' \
                    'VI아래5호가, 매도총잔량, 매수총잔량, 매도호가5, 매도호가4, 매도호가3, 매도호가2, 매도호가1, 매수호가1, 매수호가2,' \
                    '매수호가3, 매수호가4, 매수호가5, 매도잔량5, 매도잔량4, 매도잔량3, 매도잔량2, 매도잔량1, 매수잔량1, 매수잔량2,' \
                    '매수잔량3, 매수잔량4, 매수잔량5'

        list_stock_table = []
        for table_name in table_list:
            if len(table_name) == 6:
                list_stock_table.append(table_name)

        query_create_temp = \
            'CREATE TABLE IF NOT EXISTS "temp" ("index" TEXT, "종목코드" TEXT, "현재가" REAL, "시가" REAL, "고가" REAL,' \
            '"저가" REAL, "등락율" REAL, "당일거래대금" REAL, "체결강도" REAL, "초당매수수량" REAL, "초당매도수량" REAL,' \
            '"VI해제시간" TEXT, "VI아래5호가" REAL, "매도총잔량" REAL, "매수총잔량" REAL, "매도호가5" REAL, "매도호가4" REAL,' \
            '"매도호가3" REAL, "매도호가2" REAL, "매도호가1" REAL, "매수호가1" REAL, "매수호가2" REAL, "매수호가3" REAL,' \
            '"매수호가4" REAL, "매수호가5" REAL, "매도잔량5" REAL, "매도잔량4" REAL, "매도잔량3" REAL, "매도잔량2" REAL,' \
            '"매도잔량1" REAL, "매수잔량1" REAL, "매수잔량2" REAL, "매수잔량3" REAL, "매수잔량4" REAL, "매수잔량5" REAL);'
        query_create_dist = \
            'CREATE TABLE IF NOT EXISTS "dist" (uid integer primary key autoincrement, cnt integer,' \
            ' reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL);'
        query_create_distchk = \
            'CREATE TABLE IF NOT EXISTS "dist_chk" (uid integer primary key autoincrement, cnt integer,' \
            'reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL);'

        s = 'CREATE TRIGGER IF NOT EXISTS "dist_trigger" INSERT ON "dist" BEGIN INSERT INTO "dist_chk" ("cnt") values (1);\n'
        for i in range(len(list_stock_table)):
            s += 'INSERT INTO "' + list_stock_table[i] + '" SELECT ' + const_str + ' FROM temp WHERE 종목코드 = "' + \
                list_stock_table[i] + '";\n'
        s += 'DELETE FROM temp;\n'
        s += 'INSERT INTO "dist_chk" ("cnt") values (2);\n'  # 디버깅 속도측정용
        s += 'END;\n'
        query_create_trigger = s

        self.cur1.execute(query_create_temp)
        self.cur1.execute(query_create_dist)
        self.cur1.execute(query_create_distchk)
        self.cur1.execute(query_create_trigger)

    def create_trigger2(self):
        res = self.cur2.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_list = []
        for name in res.fetchall():
            table_list.append(name[0])

        const_str = '"index", 현재가, 시가, 고가, 저가, 등락율, 당일거래대금, 초당매수수량, 초당매도수량, 누적매수량, 누적매도량,' \
                    '매도총잔량, 매수총잔량, 매도호가5, 매도호가4, 매도호가3, 매도호가2, 매도호가1, 매수호가1, 매수호가2, 매수호가3,' \
                    '매수호가4, 매수호가5, 매도잔량5, 매도잔량4, 매도잔량3, 매도잔량2, 매도잔량1, 매수잔량1, 매수잔량2, 매수잔량3,' \
                    '매수잔량4, 매수잔량5'

        self.list_coin_table = []
        for table_name in table_list:
            if 'KRW' in table_name:
                self.list_coin_table.append(table_name)

        query_create_temp = \
            'CREATE TABLE IF NOT EXISTS "temp" ("index" TEXT, "종목코드" TEXT, "현재가" REAL, "시가" REAL, "고가" REAL,' \
            '"저가" REAL, "등락율" REAL, "당일거래대금" REAL, "초당매수수량" REAL, "초당매도수량" REAL, "누적매수량" REAL,' \
            '"누적매도량" REAL, "매도총잔량" REAL, "매수총잔량" REAL, "매도호가5" REAL, "매도호가4" REAL, "매도호가3" REAL,' \
            '"매도호가2" REAL, "매도호가1" REAL, "매수호가1" REAL, "매수호가2" REAL, "매수호가3" REAL, "매수호가4" REAL,' \
            '"매수호가5" REAL, "매도잔량5" REAL, "매도잔량4" REAL, "매도잔량3" REAL, "매도잔량2" REAL, "매도잔량1" REAL,' \
            '"매수잔량1" REAL, "매수잔량2" REAL, "매수잔량3" REAL, "매수잔량4" REAL, "매수잔량5" REAL);'
        query_create_dist = \
            'CREATE TABLE IF NOT EXISTS "dist" (uid integer primary key autoincrement, cnt integer,' \
            ' reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL);'
        query_create_distchk = \
            'CREATE TABLE IF NOT EXISTS "dist_chk" (uid integer primary key autoincrement, cnt integer,' \
            'reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL);'

        s = 'CREATE TRIGGER IF NOT EXISTS "dist_trigger" INSERT ON "dist" BEGIN INSERT INTO "dist_chk" ("cnt") values (1);\n'
        for i in range(len(self.list_coin_table)):
            s += 'INSERT INTO "' + self.list_coin_table[i] + '" SELECT ' + const_str + ' FROM temp WHERE 종목코드 = "' + \
                 self.list_coin_table[i] + '";\n'
        s += 'DELETE FROM temp;\n'
        s += 'INSERT INTO "dist_chk" ("cnt") values (2);\n'
        s += 'END;\n'
        query_create_trigger = s

        self.cur2.execute(query_create_temp)
        self.cur2.execute(query_create_dist)
        self.cur2.execute(query_create_distchk)
        self.cur2.execute(query_create_trigger)

    def remove_trigger1(self):
        try:
            self.cur1.execute('drop trigger dist_trigger;')
        except sqlite3.OperationalError:
            pass

    def remove_trigger2(self):
        try:
            self.cur2.execute('drop trigger dist_trigger;')
        except sqlite3.OperationalError:
            pass
