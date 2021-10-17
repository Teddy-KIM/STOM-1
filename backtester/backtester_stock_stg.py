import os
import sys
import sqlite3
import datetime
import pandas as pd
from matplotlib import pyplot as plt
from multiprocessing import Process, Queue
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.setting import DB_STOCK_STRETEGY, DB_STOCK_TICK, DB_BACKTEST
from utility.static import strf_time, strp_time, timedelta_day, timedelta_sec

BETTING = 10000000     # 종목당 배팅금액


class BackTesterStockStg:
    def __init__(self, q_, code_list_, var_, buystg_, sellstg_, df1_, df2_):
        self.q = q_
        self.code_list = code_list_
        self.df_name = df1_
        self.df_mt = df2_

        self.testperiod = var_[0]
        self.totaltime = var_[1]
        self.avgtime = var_[2]
        self.starttime = var_[3]
        self.endtime = var_[4]

        conn = sqlite3.connect(DB_STOCK_STRETEGY)
        dfs = pd.read_sql('SELECT * FROM buy', conn).set_index('index')
        buystrategy = dfs['전략코드'][buystg_].split('if 매수:')[0] + 'if 매수:\n    self.Buy()'
        self.buystrategy = compile(buystrategy, '<string>', 'exec')
        dfs = pd.read_sql('SELECT * FROM sell', conn).set_index('index')
        sellstrategy = dfs['전략코드'][sellstg_].split('if 매도:')[0] + 'if 매도:\n    self.Sell()'
        self.sellstrategy = compile(sellstrategy, '<string>', 'exec')
        conn.close()

        self.code = None
        self.df = None

        self.totalcount = 0
        self.totalcount_p = 0
        self.totalcount_m = 0
        self.totalholdday = 0
        self.totaleyun = 0
        self.totalper = 0.

        self.hold = False
        self.buytime = None
        self.buycount = 0
        self.buyprice = 0
        self.sellprice = 0
        self.highper = 0
        self.index = 0
        self.indexb = 0
        self.indexn = 0
        self.ccond = 0

        self.Start()

    def Start(self):
        conn = sqlite3.connect(DB_STOCK_TICK)
        tcount = len(self.code_list)
        int_daylimit = int(strf_time('%Y%m%d', timedelta_day(-self.testperiod)))
        for k, code in enumerate(self.code_list):
            self.code = code
            self.df = pd.read_sql(f"SELECT * FROM '{code}'", conn).set_index('index')
            self.df['고저평균대비등락율'] = (self.df['현재가'] / ((self.df['고가'] + self.df['저가']) / 2) - 1) * 100
            self.df['고저평균대비등락율'] = self.df['고저평균대비등락율'].round(2)
            self.df['직전체결강도'] = self.df['체결강도'].shift(1)
            self.df['직전당일거래대금'] = self.df['당일거래대금'].shift(1)
            self.df = self.df.fillna(0)
            self.df['초당거래대금'] = self.df['당일거래대금'] - self.df['직전당일거래대금']
            self.df['직전초당거래대금'] = self.df['초당거래대금'].shift(1)
            self.df = self.df.fillna(0)
            self.df['초당거래대금평균'] = self.df['직전초당거래대금'].rolling(window=self.avgtime).mean()
            self.df['체결강도평균'] = self.df['직전체결강도'].rolling(window=self.avgtime).mean()
            self.df['최고체결강도'] = self.df['직전체결강도'].rolling(window=self.avgtime).max()
            self.df = self.df.fillna(0)

            self.totalcount = 0
            self.totalcount_p = 0
            self.totalcount_m = 0
            self.totalholdday = 0
            self.totaleyun = 0
            self.totalper = 0.

            self.hold = False
            self.buytime = None
            self.buycount = 0
            self.buyprice = 0
            self.sellprice = 0
            self.highper = 0
            self.index = 0
            self.indexb = 0
            self.indexn = 0
            self.ccond = 0

            lasth = len(self.df) - 1
            for h, index in enumerate(self.df.index):
                if h != 0 and index[:8] != self.df.index[h - 1][:8]:
                    self.ccond = 0
                if int(index[:8]) < int_daylimit or \
                        (not self.hold and (int(index[8:]) < self.starttime or self.endtime <= int(index[8:]))):
                    continue
                self.index = index
                self.indexn = h
                if not self.hold and self.starttime < int(index[8:]) < self.endtime:
                    self.BuyTerm()
                elif self.hold and self.starttime < int(index[8:]) < self.endtime:
                    self.SellTerm()
                elif self.hold and (h == lasth or int(index[8:]) >= self.endtime > int(self.df.index[h - 1][8:])):
                    self.LastSell()
            self.Report(k + 1, tcount)
        conn.close()

    def BuyTerm(self):
        if type(self.df['현재가'][self.index]) == pd.Series:
            return False
        try:
            if self.code not in self.df_mt['거래대금순위'][self.index]:
                self.ccond = 0
            else:
                self.ccond += 1
        except KeyError:
            return False
        if self.ccond < self.avgtime + 1:
            return False

        def now():
            return strp_time('%Y%m%d%H%M%S', self.index)

        매수 = True
        종목코드 = self.code

        현재가 = self.df['현재가'][self.index]
        시가 = self.df['시가'][self.index]
        고가 = self.df['고가'][self.index]
        저가 = self.df['저가'][self.index]
        등락율 = self.df['등락율'][self.index]
        고저평균대비등락율 = self.df['고저평균대비등락율'][self.index]
        당일거래대금 = self.df['당일거래대금'][self.index]
        VI해제시간 = strp_time('%Y%m%d%H%M%S', self.df['VI해제시간'][self.index])
        VI아래5호가 = self.df['VI아래5호가'][self.index]
        초당거래대금 = self.df['초당거래대금'][self.index]
        초당거래대금평균 = self.df['초당거래대금평균'][self.index]
        체결강도 = self.df['체결강도'][self.index]
        직전체결강도 = self.df['직전체결강도'][self.index]
        체결강도평균 = self.df['체결강도평균'][self.index]
        최고체결강도 = self.df['최고체결강도'][self.index]
        초당매수수량 = self.df['초당매수수량'][self.index]
        초당매도수량 = self.df['초당매도수량'][self.index]

        매도총잔량 = self.df['매도총잔량'][self.index]
        매수총잔량 = self.df['매수총잔량'][self.index]
        매도호가5 = self.df['매도호가5'][self.index]
        매도호가4 = self.df['매도호가4'][self.index]
        매도호가3 = self.df['매도호가3'][self.index]
        매도호가2 = self.df['매도호가2'][self.index]
        매도호가1 = self.df['매도호가1'][self.index]
        매수호가1 = self.df['매수호가1'][self.index]
        매수호가2 = self.df['매수호가2'][self.index]
        매수호가3 = self.df['매수호가3'][self.index]
        매수호가4 = self.df['매수호가4'][self.index]
        매수호가5 = self.df['매수호가5'][self.index]
        매도잔량5 = self.df['매도잔량5'][self.index]
        매도잔량4 = self.df['매도잔량4'][self.index]
        매도잔량3 = self.df['매도잔량3'][self.index]
        매도잔량2 = self.df['매도잔량2'][self.index]
        매도잔량1 = self.df['매도잔량1'][self.index]
        매수잔량1 = self.df['매수잔량1'][self.index]
        매수잔량2 = self.df['매수잔량2'][self.index]
        매수잔량3 = self.df['매수잔량3'][self.index]
        매수잔량4 = self.df['매수잔량4'][self.index]
        매수잔량5 = self.df['매수잔량5'][self.index]

        exec(self.buystrategy, None, locals())

    def Buy(self):
        매도호가5 = self.df['매도호가5'][self.index]
        매도호가4 = self.df['매도호가4'][self.index]
        매도호가3 = self.df['매도호가3'][self.index]
        매도호가2 = self.df['매도호가2'][self.index]
        매도호가1 = self.df['매도호가1'][self.index]
        매도잔량5 = self.df['매도잔량5'][self.index]
        매도잔량4 = self.df['매도잔량4'][self.index]
        매도잔량3 = self.df['매도잔량3'][self.index]
        매도잔량2 = self.df['매도잔량2'][self.index]
        매도잔량1 = self.df['매도잔량1'][self.index]
        현재가 = self.df['현재가'][self.index]
        매수수량 = round(BETTING / 현재가, 8)
        if 매수수량 > 0.00000001:
            남은수량 = 매수수량
            직전남은수량 = 매수수량
            매수금액 = 0
            호가정보 = {매도호가1: 매도잔량1}
            for 매도호가, 매도잔량 in 호가정보.items():
                남은수량 -= 매도잔량
                if 남은수량 <= 0:
                    매수금액 += 매도호가 * 직전남은수량
                    break
                else:
                    매수금액 += 매도호가 * 매도잔량
                    직전남은수량 = 남은수량
            if 남은수량 <= 0:
                예상체결가 = round(매수금액 / 매수수량, 2)
                self.buyprice = 예상체결가
                self.buycount = 매수수량
                self.hold = True
                self.indexb = self.indexn
                self.buytime = strp_time('%Y%m%d%H%M%S', self.index)

    def SellTerm(self):
        if type(self.df['현재가'][self.index]) == pd.Series:
            return False

        def now():
            return strp_time('%Y%m%d%H%M%S', self.index)

        bg = self.buycount * self.buyprice
        cg = self.buycount * self.df['현재가'][self.index]
        eyun, 수익률 = self.GetEyunPer(bg, cg)
        if 수익률 > self.highper:
            self.highper = 수익률
        최고수익률 = self.highper

        매도 = False
        종목명 = self.df_name['종목명'][self.code]
        종목코드 = self.code
        보유수량 = self.buycount
        매수시간 = self.buytime

        현재가 = self.df['현재가'][self.index]
        시가 = self.df['시가'][self.index]
        고가 = self.df['고가'][self.index]
        저가 = self.df['저가'][self.index]
        등락율 = self.df['등락율'][self.index]
        고저평균대비등락율 = self.df['고저평균대비등락율'][self.index]
        당일거래대금 = self.df['당일거래대금'][self.index]
        VI해제시간 = strp_time('%Y%m%d%H%M%S', self.df['VI해제시간'][self.index])
        VI아래5호가 = self.df['VI아래5호가'][self.index]
        초당거래대금 = self.df['초당거래대금'][self.index]
        초당거래대금평균 = self.df['초당거래대금평균'][self.index]
        체결강도 = self.df['체결강도'][self.index]
        직전체결강도 = self.df['직전체결강도'][self.index]
        체결강도평균 = self.df['체결강도평균'][self.index]
        최고체결강도 = self.df['최고체결강도'][self.index]
        초당매수수량 = self.df['초당매수수량'][self.index]
        초당매도수량 = self.df['초당매도수량'][self.index]

        매도총잔량 = self.df['매도총잔량'][self.index]
        매수총잔량 = self.df['매수총잔량'][self.index]
        매도호가5 = self.df['매도호가5'][self.index]
        매도호가4 = self.df['매도호가4'][self.index]
        매도호가3 = self.df['매도호가3'][self.index]
        매도호가2 = self.df['매도호가2'][self.index]
        매도호가1 = self.df['매도호가1'][self.index]
        매수호가1 = self.df['매수호가1'][self.index]
        매수호가2 = self.df['매수호가2'][self.index]
        매수호가3 = self.df['매수호가3'][self.index]
        매수호가4 = self.df['매수호가4'][self.index]
        매수호가5 = self.df['매수호가5'][self.index]
        매도잔량5 = self.df['매도잔량5'][self.index]
        매도잔량4 = self.df['매도잔량4'][self.index]
        매도잔량3 = self.df['매도잔량3'][self.index]
        매도잔량2 = self.df['매도잔량2'][self.index]
        매도잔량1 = self.df['매도잔량1'][self.index]
        매수잔량1 = self.df['매수잔량1'][self.index]
        매수잔량2 = self.df['매수잔량2'][self.index]
        매수잔량3 = self.df['매수잔량3'][self.index]
        매수잔량4 = self.df['매수잔량4'][self.index]
        매수잔량5 = self.df['매수잔량5'][self.index]

        exec(self.sellstrategy, None, locals())

    def Sell(self):
        매수호가1 = self.df['매수호가1'][self.index]
        매수호가2 = self.df['매수호가2'][self.index]
        매수호가3 = self.df['매수호가3'][self.index]
        매수호가4 = self.df['매수호가4'][self.index]
        매수호가5 = self.df['매수호가5'][self.index]
        매수잔량1 = self.df['매수잔량1'][self.index]
        매수잔량2 = self.df['매수잔량2'][self.index]
        매수잔량3 = self.df['매수잔량3'][self.index]
        매수잔량4 = self.df['매수잔량4'][self.index]
        매수잔량5 = self.df['매수잔량5'][self.index]
        남은수량 = self.buycount
        직전남은수량 = 남은수량
        매도금액 = 0
        호가정보 = {매수호가1: 매수잔량1, 매수호가2: 매수잔량2, 매수호가3: 매수잔량3, 매수호가4: 매수잔량4, 매수호가5: 매수잔량5}
        for 매수호가, 매수잔량 in 호가정보.items():
            남은수량 -= 매수잔량
            if 남은수량 <= 0:
                매도금액 += 매수호가 * 직전남은수량
                break
            else:
                매도금액 += 매수호가 * 매수잔량
                직전남은수량 = 남은수량
        if 남은수량 <= 0:
            예상체결가 = round(매도금액 / self.buycount, 2)
            self.sellprice = 예상체결가
            self.hold = False
            self.CalculationEyun()
            self.highper = 0
            self.indexb = 0

    def LastSell(self):
        self.sellprice = self.df['현재가'][self.index]
        self.hold = False
        self.CalculationEyun()
        self.highper = 0
        self.indexb = 0

    def CalculationEyun(self):
        self.totalcount += 1
        bg = self.buycount * self.buyprice
        cg = self.buycount * self.sellprice
        eyun, per = self.GetEyunPer(bg, cg)
        self.totalper = round(self.totalper + per, 2)
        self.totaleyun = int(self.totaleyun + eyun)
        self.totalholdday += self.indexn - self.indexb
        if per > 0:
            self.totalcount_p += 1
        else:
            self.totalcount_m += 1
        self.q.put([self.index, self.code, per, eyun])

    # noinspection PyMethodMayBeStatic
    def GetEyunPer(self, bg, cg):
        gtexs = cg * 0.0023
        gsfee = cg * 0.00015
        gbfee = bg * 0.00015
        texs = gtexs - (gtexs % 1)
        sfee = gsfee - (gsfee % 10)
        bfee = gbfee - (gbfee % 10)
        pg = int(cg - texs - sfee - bfee)
        eyun = pg - bg
        per = round(eyun / bg * 100, 2)
        return eyun, per

    def Report(self, count, tcount):
        if self.totalcount > 0:
            plus_per = round((self.totalcount_p / self.totalcount) * 100, 2)
            avgholdday = round(self.totalholdday / self.totalcount, 2)
            self.q.put([self.code, self.totalcount, avgholdday, self.totalcount_p, self.totalcount_m,
                        plus_per, self.totalper, self.totaleyun])
            totalcount, avgholdday, totalcount_p, totalcount_m, plus_per, totalper, totaleyun = \
                self.GetTotal(plus_per, avgholdday)
            print(f" 종목코드 {self.code} | 평균보유기간 {avgholdday}초 | 거래횟수 {totalcount}회 | "
                  f" 익절 {totalcount_p}회 | 손절 {totalcount_m}회 | 승률 {plus_per}% |"
                  f" 수익률 {totalper}% | 수익금 {totaleyun}원 [{count}/{tcount}]")
        else:
            self.q.put([self.code, 0, 0, 0, 0, 0., 0., 0])

    def GetTotal(self, plus_per, avgholdday):
        totalcount = str(self.totalcount)
        totalcount = '  ' + totalcount if len(totalcount) == 1 else totalcount
        totalcount = ' ' + totalcount if len(totalcount) == 2 else totalcount
        avgholdday = str(avgholdday)
        avgholdday = '    ' + avgholdday if len(avgholdday.split('.')[0]) == 1 else avgholdday
        avgholdday = '   ' + avgholdday if len(avgholdday.split('.')[0]) == 2 else avgholdday
        avgholdday = '  ' + avgholdday if len(avgholdday.split('.')[0]) == 3 else avgholdday
        avgholdday = ' ' + avgholdday if len(avgholdday.split('.')[0]) == 4 else avgholdday
        avgholdday = avgholdday + '0' if len(avgholdday.split('.')[1]) == 1 else avgholdday
        totalcount_p = str(self.totalcount_p)
        totalcount_p = '  ' + totalcount_p if len(totalcount_p) == 1 else totalcount_p
        totalcount_p = ' ' + totalcount_p if len(totalcount_p) == 2 else totalcount_p
        totalcount_m = str(self.totalcount_m)
        totalcount_m = '  ' + totalcount_m if len(totalcount_m) == 1 else totalcount_m
        totalcount_m = ' ' + totalcount_m if len(totalcount_m) == 2 else totalcount_m
        plus_per = str(plus_per)
        plus_per = '  ' + plus_per if len(plus_per.split('.')[0]) == 1 else plus_per
        plus_per = ' ' + plus_per if len(plus_per.split('.')[0]) == 2 else plus_per
        plus_per = plus_per + '0' if len(plus_per.split('.')[1]) == 1 else plus_per
        totalper = str(self.totalper)
        totalper = '   ' + totalper if len(totalper.split('.')[0]) == 1 else totalper
        totalper = '  ' + totalper if len(totalper.split('.')[0]) == 2 else totalper
        totalper = ' ' + totalper if len(totalper.split('.')[0]) == 3 else totalper
        totalper = totalper + '0' if len(totalper.split('.')[1]) == 1 else totalper
        totaleyun = format(self.totaleyun, ',')
        if len(totaleyun.split(',')) == 1:
            totaleyun = '         ' + totaleyun if len(totaleyun.split(',')[0]) == 1 else totaleyun
            totaleyun = '        ' + totaleyun if len(totaleyun.split(',')[0]) == 2 else totaleyun
            totaleyun = '       ' + totaleyun if len(totaleyun.split(',')[0]) == 3 else totaleyun
            totaleyun = '      ' + totaleyun if len(totaleyun.split(',')[0]) == 4 else totaleyun
        elif len(totaleyun.split(',')) == 2:
            totaleyun = '     ' + totaleyun if len(totaleyun.split(',')[0]) == 1 else totaleyun
            totaleyun = '    ' + totaleyun if len(totaleyun.split(',')[0]) == 2 else totaleyun
            totaleyun = '   ' + totaleyun if len(totaleyun.split(',')[0]) == 3 else totaleyun
            totaleyun = '  ' + totaleyun if len(totaleyun.split(',')[0]) == 4 else totaleyun
        elif len(totaleyun.split(',')) == 3:
            totaleyun = ' ' + totaleyun if len(totaleyun.split(',')[0]) == 1 else totaleyun
        return totalcount, avgholdday, totalcount_p, totalcount_m, plus_per, totalper, totaleyun


class Total:
    def __init__(self, q_, last_, df1_, totaltime_):
        super().__init__()
        self.q = q_
        self.last = last_
        self.df_name = df1_
        self.totaltime = totaltime_
        self.Start()

    def Start(self):
        columns = ['거래횟수', '평균보유기간', '익절', '손절', '승률', '수익률', '수익금']
        df_back = pd.DataFrame(columns=columns)
        df_tsg = pd.DataFrame(columns=['종목명', 'per', 'ttsg'])
        k = 0
        while True:
            data = self.q.get()
            if len(data) == 4:
                name = self.df_name['종목명'][data[1]]
                if data[0] in df_tsg.index:
                    df_tsg.at[data[0]] = df_tsg['종목명'][data[0]] + ';' + name, \
                                         df_tsg['per'][data[0]] + data[2], \
                                         df_tsg['ttsg'][data[0]] + data[3]
                else:
                    df_tsg.at[data[0]] = name, data[2], data[3]
            else:
                df_back.at[data[0]] = data[1], data[2], data[3], data[4], data[5], data[6], data[7]
                k += 1
            if k == self.last:
                break

        if len(df_back) > 0:
            tc = df_back['거래횟수'].sum()
            if tc != 0:
                pc = df_back['익절'].sum()
                mc = df_back['손절'].sum()
                pper = round(pc / tc * 100, 2)
                df_back_ = df_back[df_back['평균보유기간'] != 0]
                avghold = round(df_back_['평균보유기간'].sum() / len(df_back_), 2)
                avgsp = round(df_back['수익률'].sum() / tc, 2)
                tsg = int(df_back['수익금'].sum())
                onedaycount = round(tc / self.totaltime, 4)
                onegm = int(BETTING * onedaycount * avghold)
                if onegm < BETTING:
                    onegm = BETTING
                tsp = round(tsg / onegm * 100, 4)
                text = f" 종목당 배팅금액 {format(BETTING, ',')}원, 필요자금 {format(onegm, ',')}원, "\
                       f" 종목출현빈도수 {onedaycount}개/초, 거래횟수 {tc}회, 평균보유기간 {avghold}초,\n 익절 {pc}회, "\
                       f" 손절 {mc}회, 승률 {pper}%, 평균수익률 {avgsp}%, 수익률합계 {tsp}%, 수익금합계 {format(tsg, ',')}원"
                print(text)
                conn = sqlite3.connect(DB_BACKTEST)
                df_back.to_sql(f"stock_stg_{strf_time('%Y%m%d')}_1", conn, if_exists='append', chunksize=1000)
                conn.close()

        if len(df_tsg) > 0:
            df_tsg['체결시간'] = df_tsg.index
            df_tsg.sort_values(by=['체결시간'], inplace=True)
            df_tsg['ttsg_cumsum'] = df_tsg['ttsg'].cumsum()
            df_tsg[['ttsg', 'ttsg_cumsum']] = df_tsg[['ttsg', 'ttsg_cumsum']].astype(int)
            conn = sqlite3.connect(DB_BACKTEST)
            df_tsg.to_sql(f"stock_stg_{strf_time('%Y%m%d')}_2", conn, if_exists='append', chunksize=1000)
            conn.close()
            df_tsg.plot(figsize=(12, 9), rot=45)
            plt.show()


if __name__ == "__main__":
    start = datetime.datetime.now()

    con = sqlite3.connect(DB_STOCK_TICK)
    df = pd.read_sql("SELECT name FROM sqlite_master WHERE TYPE = 'table'", con)
    df1 = pd.read_sql('SELECT * FROM codename', con).set_index('index')
    df2 = pd.read_sql('SELECT * FROM moneytop', con).set_index('index')
    con.close()

    table_list = list(df['name'].values)
    table_list.remove('moneytop')
    table_list.remove('codename')
    last = len(table_list)

    q = Queue()

    if len(table_list) > 0:
        testperiod = int(sys.argv[1])
        totaltime = int(sys.argv[2])
        avgtime = int(sys.argv[3])
        starttime = int(sys.argv[4])
        endtime = int(sys.argv[5])
        var = [testperiod, totaltime, avgtime, starttime, endtime]

        buystg = sys.argv[7]
        sellstg = sys.argv[8]

        w = Process(target=Total, args=(q, last, df1, totaltime))
        w.start()
        procs = []
        workcount = int(last / int(sys.argv[6])) + 1
        for j in range(0, last, workcount):
            code_list = table_list[j:j + workcount]
            p = Process(target=BackTesterStockStg, args=(q, code_list, var, buystg, sellstg, df1, df2))
            procs.append(p)
            p.start()
        for p in procs:
            p.join()
        w.join()

    q.close()
    end = datetime.datetime.now()
    print(f" 백테스팅 소요시간 {end - start}")
