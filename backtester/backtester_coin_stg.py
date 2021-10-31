import os
import sys
import sqlite3
import datetime
import pandas as pd
from matplotlib import gridspec
from matplotlib import pyplot as plt
from multiprocessing import Process, Queue
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.static import strf_time, strp_time, timedelta_sec
from utility.setting import DB_COIN_STRATEGY, DB_COIN_TICK, DB_BACKTEST, ui_num


class BackTesterCoinStg:
    def __init__(self, q_, wq_, code_list_, var_, buystg_, sellstg_, df1_):
        self.q = q_
        self.wq = wq_
        self.code_list = code_list_
        self.df_mt = df1_

        self.startday = var_[0]
        self.endday = var_[1]
        self.starttime = var_[2]
        self.endtime = var_[3]
        self.betting = var_[4]
        self.avgtime = var_[5]

        conn = sqlite3.connect(DB_COIN_STRATEGY)
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
        conn = sqlite3.connect(DB_COIN_TICK)
        tcount = len(self.code_list)
        for k, code in enumerate(self.code_list):
            self.code = code
            self.df = pd.read_sql(f"SELECT * FROM '{code}'", conn).set_index('index')
            self.df['고저평균대비등락율'] = (self.df['현재가'] / ((self.df['고가'] + self.df['저가']) / 2) - 1) * 100
            self.df['고저평균대비등락율'] = self.df['고저평균대비등락율'].round(2)
            self.df['체결강도'] = self.df['누적매수량'] / self.df['누적매도량'] * 100
            self.df['체결강도'] = self.df['체결강도'].apply(lambda x: 500 if x > 500 else round(x, 2))
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
                if int(index[:8]) < self.startday or int(index[:8]) > self.endday or \
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
        try:
            if type(self.df['현재가'][self.index]) == pd.Series or type(self.df_mt['거래대금순위'][self.index]) == pd.Series:
                return False
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
        종목명 = self.code

        현재가 = self.df['현재가'][self.index]
        시가 = self.df['시가'][self.index]
        고가 = self.df['고가'][self.index]
        저가 = self.df['저가'][self.index]
        등락율 = self.df['등락율'][self.index]
        고저평균대비등락율 = self.df['고저평균대비등락율'][self.index]
        당일거래대금 = self.df['당일거래대금'][self.index]
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
        매수수량 = round(self.betting / 현재가, 8)
        if 매수수량 > 0.00000001:
            남은수량 = 매수수량
            직전남은수량 = 매수수량
            매수금액 = 0
            호가정보 = {매도호가1: 매도잔량1, 매도호가2: 매도잔량2}
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
                self.q.put(self.index)

    def SellTerm(self):
        self.q.put(self.index)
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
        종목명 = self.code
        보유수량 = self.buycount
        매수시간 = self.buytime

        현재가 = self.df['현재가'][self.index]
        시가 = self.df['시가'][self.index]
        고가 = self.df['고가'][self.index]
        저가 = self.df['저가'][self.index]
        등락율 = self.df['등락율'][self.index]
        고저평균대비등락율 = self.df['고저평균대비등락율'][self.index]
        당일거래대금 = self.df['당일거래대금'][self.index]
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
        self.q.put([self.code, self.df.index[self.indexb], self.index, self.buyprice, self.sellprice, per, eyun])

    # noinspection PyMethodMayBeStatic
    def GetEyunPer(self, bg, cg):
        sfee = cg * 0.0005
        bfee = bg * 0.0005
        pg = int(cg - sfee - bfee)
        eyun = pg - bg
        per = round(eyun / bg * 100, 2)
        return eyun, per

    def Report(self, count, tcount):
        if self.totalcount > 0:
            plus_per = round((self.totalcount_p / self.totalcount) * 100, 2)
            self.q.put([self.code, self.totalcount, self.totalholdday, self.totalcount_p, self.totalcount_m,
                        plus_per, self.totalper, self.totaleyun])
            totalcount, totalholdday, totalcount_p, totalcount_m, plus_per, totalper, totaleyun = \
                self.GetTotal(plus_per, self.totalholdday)
            test = f"보유기간합계  {totalholdday}초 | 거래횟수 {totalcount}회 | " \
                   f"익절 {totalcount_p}회 | 손절 {totalcount_m}회 | 승률   {plus_per}% | " \
                   f"수익률 {totalper}% | 수익금 {totaleyun}원 | 종목코드 {self.code} [{count}/{tcount}]"
            self.wq.put([ui_num['C백테스트'], test])
        else:
            self.q.put([self.code, 0, 0, 0, 0, 0., 0., 0])

    def GetTotal(self, plus_per, totalholdday):
        totalcount = str(self.totalcount)
        totalcount = '    ' + totalcount if len(totalcount) == 1 else totalcount
        totalcount = '   ' + totalcount if len(totalcount) == 2 else totalcount
        totalholdday = str(totalholdday)
        totalholdday = '      ' + totalholdday if len(totalholdday) == 1 else totalholdday
        totalholdday = '    ' + totalholdday if len(totalholdday) == 2 else totalholdday
        totalholdday = '  ' + totalholdday if len(totalholdday) == 3 else totalholdday
        totalholdday = totalholdday + '0' if len(totalholdday) == 1 else totalholdday
        totalcount_p = str(self.totalcount_p)
        totalcount_p = '    ' + totalcount_p if len(totalcount_p) == 1 else totalcount_p
        totalcount_p = '  ' + totalcount_p if len(totalcount_p) == 2 else totalcount_p
        totalcount_m = str(self.totalcount_m)
        totalcount_m = '    ' + totalcount_m if len(totalcount_m) == 1 else totalcount_m
        totalcount_m = '  ' + totalcount_m if len(totalcount_m) == 2 else totalcount_m
        plus_per = str(plus_per)
        plus_per = '    ' + plus_per if len(plus_per.split('.')[0]) == 1 else plus_per
        plus_per = '  ' + plus_per if len(plus_per.split('.')[0]) == 2 else plus_per
        plus_per = plus_per + '0' if len(plus_per.split('.')[1]) == 1 else plus_per
        totalper = str(self.totalper)
        totalper = '      ' + totalper if len(totalper.split('.')[0]) == 1 else totalper
        totalper = '    ' + totalper if len(totalper.split('.')[0]) == 2 else totalper
        totalper = '  ' + totalper if len(totalper.split('.')[0]) == 3 else totalper
        totalper = totalper + '0' if len(totalper.split('.')[1]) == 1 else totalper
        totalper = ' ' + totalper if '-' in totalper else totalper
        totaleyun = format(self.totaleyun, ',')
        if len(totaleyun.split(',')) == 1:
            totaleyun = '                  ' + totaleyun if len(totaleyun.split(',')[0]) == 1 else totaleyun
            totaleyun = '                ' + totaleyun if len(totaleyun.split(',')[0]) == 2 else totaleyun
            totaleyun = '              ' + totaleyun if len(totaleyun.split(',')[0]) == 3 else totaleyun
            totaleyun = '            ' + totaleyun if len(totaleyun.split(',')[0]) == 4 else totaleyun
        elif len(totaleyun.split(',')) == 2:
            totaleyun = '          ' + totaleyun if len(totaleyun.split(',')[0]) == 1 else totaleyun
            totaleyun = '        ' + totaleyun if len(totaleyun.split(',')[0]) == 2 else totaleyun
            totaleyun = '      ' + totaleyun if len(totaleyun.split(',')[0]) == 3 else totaleyun
            totaleyun = '    ' + totaleyun if len(totaleyun.split(',')[0]) == 4 else totaleyun
        elif len(totaleyun.split(',')) == 3:
            totaleyun = '  ' + totaleyun if len(totaleyun.split(',')[0]) == 1 else totaleyun
        totaleyun = ' ' + totaleyun if '-' in totaleyun else totaleyun
        return totalcount, totalholdday, totalcount_p, totalcount_m, plus_per, totalper, totaleyun


class Total:
    def __init__(self, q_, wq_, last_, betting_):
        super().__init__()
        self.q = q_
        self.wq = wq_
        self.last = last_
        self.betting = betting_
        self.Start()

    def Start(self):
        columns = ['거래횟수', '보유기간합계', '익절', '손절', '승률', '수익률', '수익금']
        df_back = pd.DataFrame(columns=columns)
        df_bct = pd.DataFrame(columns=['hold_count'])
        df_tsg = pd.DataFrame(columns=['종목명', '매수시간', '매도시간', '매수가', '매도가', '수익률', 'sgm'])
        k = 0
        while True:
            data = self.q.get()
            if type(data) == str:
                if data in df_bct.index:
                    df_bct.at[data] = df_bct['hold_count'][data] + 1
                else:
                    df_bct.at[data] = 1
            elif len(data) == 7:
                if data[2] in df_tsg.index:
                    df_tsg.at[data[2]] = df_tsg['종목명'][data[2]] + ';' + data[0], \
                                         df_tsg['매수시간'][data[2]] + ';' + data[1], \
                                         df_tsg['매도시간'][data[2]] + ';' + data[2], \
                                         df_tsg['매수가'][data[2]] + ';' + str(data[3]), \
                                         df_tsg['매도가'][data[2]] + ';' + str(data[4]), \
                                         df_tsg['수익률'][data[2]] + ';' + str(data[5]), \
                                         df_tsg['sgm'][data[2]] + data[6]
                else:
                    df_tsg.at[data[2]] = data[0], data[1], data[2], str(data[3]), str(data[4]), str(data[5]), data[6]
            else:
                df_back.at[data[0]] = data[1], data[2], data[3], data[4], data[5], data[6], data[7]
                k += 1
            if k == self.last:
                break

        if len(df_back) > 0:
            df_back = df_back[df_back['거래횟수'] > 0]
            tc = df_back['거래횟수'].sum()
            if tc != 0:
                pc = df_back['익절'].sum()
                mc = df_back['손절'].sum()
                pper = round(pc / tc * 100, 2)
                avghold = round(df_back['보유기간합계'].sum() / tc, 2)
                avgsp = round(df_back['수익률'].sum() / tc, 2)
                tsg = int(df_back['수익금'].sum())
                avgholdcount = round(df_bct['hold_count'].max(), 2)
                onegm = int(self.betting * avgholdcount)
                if onegm < self.betting:
                    onegm = self.betting
                tsp = round(tsg / onegm * 100, 4)
                text = f"종목당 배팅금액 {format(self.betting, ',')}원, 필요자금 {format(onegm, ',')}원, "\
                       f"거래횟수 {tc}회, 최대보유종목수 {avgholdcount}개, 평균보유기간 {avghold}초,\n 익절 {pc}회, "\
                       f"손절 {mc}회, 승률 {pper}%, 평균수익률 {avgsp}%, 수익률합계 {tsp}%, 수익금합계 {format(tsg, ',')}원"
                self.wq.put([ui_num['C백테스트'], text])
                conn = sqlite3.connect(DB_BACKTEST)
                df_back.to_sql(f"coin_vj_code_{strf_time('%Y%m%d')}", conn, if_exists='append', chunksize=1000)
                conn.close()
                if len(df_tsg) == 0:
                    df_bct = pd.DataFrame(columns=['hold_count'])

        if len(df_tsg) > 0:
            df_tsg.sort_values(by=['매도시간'], inplace=True)
            df_tsg['sgm_cumsum'] = df_tsg['sgm'].cumsum()
            df_tsg[['sgm', 'sgm_cumsum']] = df_tsg[['sgm', 'sgm_cumsum']].astype(int)
            df_bct['index'] = df_bct.index
            df_bct.sort_values(by=['index'], inplace=True)
            df_bct = df_bct.set_index('index')

            conn = sqlite3.connect(DB_BACKTEST)
            df_bct.to_sql(f"coin_vj_hold_{strf_time('%Y%m%d')}", conn, if_exists='append', chunksize=1000)
            df_tsg.to_sql(f"coin_vj_time_{strf_time('%Y%m%d')}", conn, if_exists='append', chunksize=1000)
            conn.close()

            plt.figure(figsize=(12, 10))
            gs = gridspec.GridSpec(nrows=2, ncols=1, height_ratios=[3, 1])
            plt.subplot(gs[0])
            plt.plot(df_tsg.index, df_tsg['sgm'], label='sgm')
            plt.plot(df_tsg.index, df_tsg['sgm_cumsum'], label='sgm_cumsum')
            plt.xticks([])
            plt.legend(loc='best')
            plt.grid()
            plt.subplot(gs[1])
            plt.plot(df_bct.index, df_bct['hold_count'], color='g', label='hold_count')
            plt.xticks(list(df_tsg.index[::12]), rotation=45)
            plt.legend(loc='best')
            plt.tight_layout()
            plt.show()


def BackTesterCoinStgMain(wq, bq):
    start = datetime.datetime.now()

    con = sqlite3.connect(DB_COIN_TICK)
    df = pd.read_sql("SELECT name FROM sqlite_master WHERE TYPE = 'table'", con)
    df1 = pd.read_sql('SELECT * FROM moneytop', con).set_index('index')
    con.close()

    table_list = list(df['name'].values)
    table_list.remove('moneytop')
    if 'dist' in table_list:
        table_list.remove('dist')
    if 'dist_chk' in table_list:
        table_list.remove('dist_chk')
    if 'sqlite_sequence' in table_list:
        table_list.remove('sqlite_sequence')
    if 'temp' in table_list:
        table_list.remove('temp')
    last = len(table_list)

    q = Queue()

    if len(table_list) > 0:
        data = bq.get()
        startday = int(data[0])
        endday = int(data[1])
        starttime = int(data[2])
        endtime = int(data[3])
        betting = float(data[4]) * 1000000
        avgtime = int(data[5])
        var = [startday, endday, starttime, endtime, betting, avgtime]

        buystg = data[7]
        sellstg = data[8]

        w = Process(target=Total, args=(q, wq, last, betting))
        w.start()
        procs = []
        workcount = int(last / int(data[6])) + 1
        for j in range(0, last, workcount):
            code_list = table_list[j:j + workcount]
            p = Process(target=BackTesterCoinStg, args=(q, wq, code_list, var, buystg, sellstg, df1))
            procs.append(p)
            p.start()
        for p in procs:
            p.join()
        w.join()

    q.close()
    end = datetime.datetime.now()
    wq.put([ui_num['C백테스트'], f"백테스팅 소요시간 {end - start}"])
