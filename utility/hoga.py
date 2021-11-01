import pandas as pd
from utility.setting import ui_num, columns_hj
from utility.static import now, timedelta_sec


class Hoga:
    def __init__(self, qlist):
        """
                    0        1       2        3       4       5          6          7        8      9
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceiv1Q, creceiv2Q, stockQ, coinQ,
                 sstgQ, cstgQ, tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, chartQ, hogaQ]
                   10    11      12      13      14      15      16      17     18
        """
        self.windowQ = qlist[0]
        self.hogaQ = qlist[18]
        self.hoga_name = None
        self.df_hj = None
        self.df_hc = None
        self.df_hg = None
        self.bool_hjup = False
        self.bool_hcup = False
        self.bool_hgup = False
        self.time_uphg = now()
        self.InitHoga()
        self.Start()

    def Start(self):
        while True:
            data = self.hogaQ.get()
            if len(data) == 7:
                self.UpdateHogaJongmok(data)
            elif len(data) == 3:
                self.UpdateChegeolcount(data)
            else:
                self.UpdateHogajalryang(data)

            if now() > self.time_uphg:
                if self.bool_hjup and self.df_hc is not None:
                    if 'KRW' in self.hoga_name:
                        self.windowQ.put([ui_num['C호가종목'], self.df_hj])
                    else:
                        self.windowQ.put([ui_num['S호가종목'], self.df_hj])
                    self.bool_hjup = False
                if self.bool_hcup and self.df_hc is not None:
                    if 'KRW' in self.hoga_name:
                        self.windowQ.put([ui_num['C호가체결'], self.df_hc])
                    else:
                        self.windowQ.put([ui_num['S호가체결'], self.df_hc])
                    self.bool_hcup = False
                if self.bool_hgup and self.df_hg is not None:
                    if 'KRW' in self.hoga_name:
                        self.windowQ.put([ui_num['C호가잔량'], self.df_hg])
                    else:
                        self.windowQ.put([ui_num['S호가잔량'], self.df_hg])
                    self.bool_hgup = False
                self.time_uphg = timedelta_sec(0.25)

    def InitHoga(self):
        cc = [0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.]
        ch = [0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.]
        self.df_hj = pd.DataFrame({'종목명': [''], '현재가': [0.], '등락율': [0.], 'UVI': [0],
                                   '시가': [0], '고가': [0], '저가': [0]})
        self.df_hc = pd.DataFrame({'체결수량': cc, '체결강도': ch})
        self.df_hg = pd.DataFrame({'잔량': cc, '호가': cc})
        self.windowQ.put([ui_num['S호가종목'], self.df_hj])
        self.windowQ.put([ui_num['S호가체결'], self.df_hc])
        self.windowQ.put([ui_num['S호가잔량'], self.df_hg])
        self.hoga_name = ''

    def UpdateHogaJongmok(self, data):
        if self.hoga_name != data[0]:
            self.InitHoga()
            self.hoga_name = data[0]
        self.df_hj = pd.DataFrame([data], columns=columns_hj)
        self.bool_hjup = True

    def UpdateChegeolcount(self, data):
        if self.hoga_name != data[0]:
            self.InitHoga()
            self.hoga_name = data[0]
        v = data[1]
        ch = data[2]
        if v > 0:
            if 'KRW' in self.hoga_name:
                tbc = round(self.df_hc['체결수량'][0] + v, 8)
                tsc = round(self.df_hc['체결수량'][11], 8)
            else:
                tbc = self.df_hc['체결수량'][0] + v
                tsc = self.df_hc['체결수량'][11]
        else:
            if 'KRW' in self.hoga_name:
                tbc = round(self.df_hc['체결수량'][0], 8)
                tsc = round(self.df_hc['체결수량'][11] + abs(v), 8)
            else:
                tbc = self.df_hc['체결수량'][0]
                tsc = self.df_hc['체결수량'][11] + abs(v)
        hch = self.df_hc['체결강도'][0]
        lch = self.df_hc['체결강도'][11]
        if hch < ch:
            hch = ch
        if lch == 0 or lch > ch:
            lch = ch
        self.df_hc = self.df_hc.shift(1)
        self.df_hc.at[0, ['체결수량', '체결강도']] = tbc, hch
        self.df_hc.at[1, ['체결수량', '체결강도']] = v, ch
        self.df_hc.at[11, ['체결수량', '체결강도']] = tsc, lch
        self.bool_hcup = True

    def UpdateHogajalryang(self, data):
        if self.hoga_name != data[0]:
            self.InitHoga()
            self.hoga_name = data[0]
        if 'KRW' in self.hoga_name:
            jr = data[1:2] + data[13:] + data[2:3]
            hg = [self.df_hj['고가'][0]] + data[3:13] + [self.df_hj['저가'][0]]
        else:
            jr = data[1:2] + data[13:23] + data[2:3]
            hg = data[23:24] + data[3:13] + data[24:25]
        try:
            self.df_hg = pd.DataFrame({'잔량': jr, '호가': hg})
            self.bool_hgup = True
        except ValueError:
            print(jr)
            print(hg)
