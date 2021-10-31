import os
import sqlite3
import pandas as pd
from PyQt5.QtGui import QFont, QColor

K_OPENAPI_PATH = 'D:/OpenAPI'
E_OPENAPI_PATH = 'D:/xingAPI'
# SYSTEM_PATH = 'D:/PythonProjects/STOM'
SYSTEM_PATH = os.getcwd()
ICON_PATH = f'{SYSTEM_PATH}/utility/icon'
LOGIN_PATH = f'{SYSTEM_PATH}/stock/login_kiwoom'
GRAPH_PATH = f'{SYSTEM_PATH}/backtester/graph'
DB_SETTING = f'{SYSTEM_PATH}/database/setting.db'
DB_BACKTEST = f'{SYSTEM_PATH}/database/backtest.db'
DB_TRADELIST = f'{SYSTEM_PATH}/database/tradelist.db'
DB_STOCK_TICK = f'{SYSTEM_PATH}/database/stock_tick.db'
DB_COIN_TICK = f'{SYSTEM_PATH}/database/coin_tick.db'
DB_STOCK_STRATEGY = f'{SYSTEM_PATH}/database/stock_strategy.db'
DB_COIN_STRATEGY = f'{SYSTEM_PATH}/database/coin_strategy.db'

connn = sqlite3.connect(DB_SETTING)
df_m = pd.read_sql('SELECT * FROM main', connn).set_index('index')
df_s = pd.read_sql('SELECT * FROM stock', connn).set_index('index')
df_c = pd.read_sql('SELECT * FROM coin', connn).set_index('index')
df_k = pd.read_sql('SELECT * FROM sacc', connn).set_index('index')
df_u = pd.read_sql('SELECT * FROM cacc', connn).set_index('index')
df_t = pd.read_sql('SELECT * FROM telegram', connn).set_index('index')
connn.close()

DICT_SET = {
    '증권사': df_m['증권사'][0],
    '주식리시버': df_m['주식리시버'][0],
    '주식콜렉터': df_m['주식콜렉터'][0],
    '주식트레이더': df_m['주식트레이더'][0],
    '거래소': df_m['거래소'][0],
    '코인리시버': df_m['코인리시버'][0],
    '코인콜렉터': df_m['코인콜렉터'][0],
    '코인트레이더': df_m['코인트레이더'][0],
    '주식순위시간': df_m['주식순위시간'][0],
    '주식순위선정': df_m['주식순위선정'][0],
    '코인순위시간': df_m['코인순위시간'][0],
    '코인순위선정': df_m['코인순위선정'][0],
    '주식실시간저장': df_m['주식실시간저장'][0],
    '주식전체종목저장': df_m['주식전체종목저장'][0],
    '주식저장주기': df_m['주식저장주기'][0],
    '코인저장주기': df_m['코인저장주기'][0],

    '아이디1': df_k['아이디1'][0] if len(df_k) > 0 and df_k['아이디1'][0] != '' else None,
    '비밀번호1': df_k['비밀번호1'][0] if len(df_k) > 0 and df_k['비밀번호1'][0] != '' else None,
    '인증서비밀번호1': df_k['인증서비밀번호1'][0] if len(df_k) > 0 and df_k['인증서비밀번호1'][0] != '' else None,
    '계좌비밀번호1': df_k['계좌비밀번호1'][0] if len(df_k) > 0 and df_k['계좌비밀번호1'][0] != '' else None,
    '아이디2': df_k['아이디2'][0] if len(df_k) > 0 and df_k['아이디2'][0] != '' else None,
    '비밀번호2': df_k['비밀번호2'][0] if len(df_k) > 0 and df_k['비밀번호2'][0] != '' else None,
    '인증서비밀번호2': df_k['인증서비밀번호2'][0] if len(df_k) > 0 and df_k['인증서비밀번호2'][0] != '' else None,
    '계좌비밀번호2': df_k['계좌비밀번호2'][0] if len(df_k) > 0 and df_k['계좌비밀번호2'][0] != '' else None,

    'Access_key': df_u['Access_key'][0] if len(df_u) > 0 and df_u['Access_key'][0] != '' else None,
    'Secret_key': df_u['Secret_key'][0] if len(df_u) > 0 and df_u['Secret_key'][0] != '' else None,

    '텔레그램봇토큰': df_t['str_bot'][0] if len(df_t) > 0 and df_t['str_bot'][0] != '' else None,
    '텔레그램사용자아이디': int(df_t['int_id'][0]) if len(df_t) > 0 and df_t['int_id'][0] != '' else None,

    '주식모의투자': df_s['주식모의투자'][0],
    '주식알림소리': df_s['주식알림소리'][0],
    '주식장초매수전략': df_s['주식장초매수전략'][0],
    '주식장초매도전략': df_s['주식장초매도전략'][0],
    '주식장초평균값계산틱수': df_s['주식장초평균값계산틱수'][0],
    '주식장초최대매수종목수': df_s['주식장초최대매수종목수'][0],
    '주식장중매수전략': df_s['주식장중매수전략'][0],
    '주식장중매도전략': df_s['주식장중매도전략'][0],
    '주식장중평균값계산틱수': df_s['주식장중평균값계산틱수'][0],
    '주식장중최대매수종목수': df_s['주식장중최대매수종목수'][0],

    '코인모의투자': df_c['코인모의투자'][0],
    '코인알림소리': df_c['코인알림소리'][0],
    '코인장초매수전략': df_c['코인장초매수전략'][0],
    '코인장초매도전략': df_c['코인장초매도전략'][0],
    '코인장초평균값계산틱수': df_c['코인장초평균값계산틱수'][0],
    '코인장초최대매수종목수': df_c['코인장초최대매수종목수'][0],
    '코인장중매수전략': df_c['코인장중매수전략'][0],
    '코인장중매도전략': df_c['코인장중매도전략'][0],
    '코인장중평균값계산틱수': df_c['코인장중평균값계산틱수'][0],
    '코인장중최대매수종목수': df_c['코인장중최대매수종목수'][0],
}

qfont12 = QFont()
qfont12.setFamily('나눔고딕')
qfont12.setPixelSize(12)

qfont14 = QFont()
qfont14.setFamily('나눔고딕')
qfont14.setPixelSize(14)

sn_brrq = 1000
sn_brrd = 1001
sn_cond = 1002
sn_oper = 1003
sn_recv = 2000

color_fg_bt = QColor(230, 230, 235)
color_fg_bc = QColor(190, 190, 195)
color_fg_dk = QColor(150, 150, 155)
color_fg_bk = QColor(110, 110, 115)
color_fg_hl = QColor(110, 110, 255)

color_bg_bt = QColor(50, 50, 55)
color_bg_bc = QColor(40, 40, 45)
color_bg_dk = QColor(30, 30, 35)
color_bg_bk = QColor(20, 20, 25)

color_bf_bt = QColor(110, 110, 115)
color_bf_dk = QColor(70, 70, 75)

color_cs_hr = QColor(230, 230, 0)

style_fc_bt = 'color: rgb(230, 230, 235);'
style_fc_dk = 'color: rgb(150, 150, 155);'
style_bc_st = 'background-color: rgb(70, 70, 75);'
style_bc_bt = 'background-color: rgb(50, 50, 55);'
style_bc_dk = 'background-color: rgb(30, 30, 35);'
style_bc_by = 'background-color: rgb(100, 70, 70);'
style_bc_sl = 'background-color: rgb(70, 70, 100);'
style_pgbar = 'QProgressBar {background-color: #28282d;} QProgressBar::chunk {background-color: #5a5a5f;}'

ui_num = {'설정텍스트': 0, 'S단순텍스트': 1, 'S로그텍스트': 2, 'S종목명딕셔너리': 3,
          'C단순텍스트': 4, 'C로그텍스트': 5, 'S백테스트': 6, 'C백테스트': 7,
          'S실현손익': 11, 'S거래목록': 12, 'S잔고평가': 13, 'S잔고목록': 14, 'S체결목록': 15,
          'S당일합계': 16, 'S당일상세': 17, 'S누적합계': 18, 'S누적상세': 19, 'S관심종목': 20,
          'C실현손익': 21, 'C거래목록': 22, 'C잔고평가': 23, 'C잔고목록': 24, 'C체결목록': 25,
          'C당일합계': 26, 'C당일상세': 27, 'C누적합계': 28, 'C누적상세': 29, 'C관심종목': 30,
          '차트': 40, '실시간차트': 41,
          'S호가종목': 42, 'S호가체결': 43, 'S호가잔량': 44,
          'C호가종목': 45, 'C호가체결': 46, 'C호가잔량': 47}

columns_tt = ['거래횟수', '총매수금액', '총매도금액', '총수익금액', '총손실금액', '수익률', '수익금합계']
columns_td = ['종목명', '매수금액', '매도금액', '주문수량', '수익률', '수익금', '체결시간']
columns_tj = ['추정예탁자산', '추정예수금', '보유종목수', '수익률', '총평가손익', '총매입금액', '총평가금액']
columns_jg = ['종목명', '매입가', '현재가', '수익률', '평가손익', '매입금액', '평가금액', '보유수량']
columns_cj = ['종목명', '주문구분', '주문수량', '미체결수량', '주문가격', '체결가', '체결시간']
columns_gj = ['등락율', '고저평균대비등락율', '초당거래대금', '초당거래대금평균', '당일거래대금',
              '체결강도', '체결강도평균', '최고체결강도', '현재가']
columns_gj_ = ['종목명', 'per', 'hlml_per', 's_money', 'sm_avg', 'd_money', 'ch', 'ch_avg', 'ch_high']

columns_dt = ['거래일자', '누적매수금액', '누적매도금액', '누적수익금액', '누적손실금액', '수익률', '누적수익금']
columns_dd = ['체결시간', '종목명', '매수금액', '매도금액', '주문수량', '수익률', '수익금']
columns_nt = ['기간', '누적매수금액', '누적매도금액', '누적수익금액', '누적손실금액', '수익률', '누적수익금']
columns_nd = ['일자', '총매수금액', '총매도금액', '총수익금액', '총손실금액', '수익률', '수익금합계']

columns_sm = ['증권사', '주식리시버', '주식콜렉터', '주식트레이더', '거래소', '코인리시버', '코인콜렉터', '코인트레이더', '주식순위시간',
              '주식순위선정', '코인순위시간', '코인순위선정', '주식실시간저장', '주식전체종목저장', '주식저장주기', '코인저장주기']
columns_sk = ['아이디1', '비밀번호1', '인증서비밀번호1', '계좌비밀번호1', '아이디2', '비밀번호2', '인증서비밀번호2', '계좌비밀번호2']
columns_su = ['Access_key', 'Secret_key']
columns_st = ['str_bot', 'int_id']

columns_hj = ['종목명', '현재가', '등락율', 'UVI', '시가', '고가', '저가']
columns_hc = ['체결수량', '체결강도']
columns_hg = ['잔량', '호가']

stock_buy_var = '''"""
def BuyStrategy(self, *args)
매수(True), 종목명(str), 종목코드(str), 현재가(int), 시가(int), 고가(int), 저가(int), 등락율(float), 고저평균대비등락율(float), 당일거래대금(int),
초당거래대금(int), 초당거래대금평균(int), 체결강도(float), 직전체결강도(float), 체결강도평균(float), 최고체결강도(float), VI해제시간(datetime),
VI아래5호가(int), 초당매수수량(int), 초당매도수량(int), 매도총잔량(int), 매수총잔량(int), 매도호가5(int), 매도호가4(int), 매도호가3(int),
매도호가2(int), 매도호가1(int), 매수호가1(int), 매수호가2(int), 매수호가3(int), 매수호가4(int), 매수호가5(int), 매도잔량5(int), 매도잔량4(int),
매도잔량3(int), 매도잔량2(int), 매도잔량1(int), 매수잔량1(int), 매수잔량2(int), 매수잔량3(int), 매수잔량4(int), 매수잔량5(int)
"""'''
stock_sell_var = '''"""
def SellStrategy(self, *args)
매도(False), 종목명(str), 종목코드(str), 수익률(float), 최고수익률(float), 보유수량(int), 매수시간(datetime), 현재가(int), 시가(int), 고가(int),
저가(int), 등락율(float), 고저평균대비등락율(float), 초당거래대금(int), 초당거래대금평균(int), 체결강도(float), 직전체결강도(float), 체결강도평균(float),
최고체결강도(float), VI해제시간(datetime), VI아래5호가(int), 초당매수수량(int), 초당매도수량(int), 매도총잔량(int), 매수총잔량(int), 매도호가5(int),
매도호가4(int), 매도호가3(int), 매도호가2(int), 매도호가1(int), 매수호가1(int), 매수호가2(int), 매수호가3(int), 매수호가4(int), 매수호가5(int),
매도잔량5(int), 매도잔량4(int), 매도잔량3(int), 매도잔량2(int), 매도잔량1(int), 매수잔량1(int), 매수잔량2(int), 매수잔량3(int), 매수잔량4(int),
매수잔량5(int)
"""'''
stock_buy_signal = '''
if 매수:
    매수수량 = int(self.int_tujagm / 현재가)
    if 매수수량 > 0:
        남은수량 = 매수수량
        직전남은수량 = 매수수량
        매수금액 = 0
        # 호가정보 딕셔너리에 넣은 잔량의 합이 매수수량보다 클 경우에만 매수주문됩니다.
        # 기본 호가정보의 수를 변경할 경우 반드시 백테스터 self.Buy() 내에서도 호가정보를 수정해야 백테스팅이 올바르게 진행됩니다.
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
            self.list_buy.append(종목코드)
            self.stockQ.put(['매수', 종목코드, 종목명, 예상체결가, 매수수량])'''
stock_sell_signal = '''
if 매도:
    남은수량 = 보유수량
    직전남은수량 = 보유수량
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
        예상체결가 = round(매도금액 / 보유수량, 2)
        self.list_sell.append(종목코드)
        self.stockQ.put(['매도', 종목코드, 종목명, 예상체결가, 보유수량])'''
coin_buy_var = '''"""
def BuyStrategy(self, *args)
매수(True), 종목명(str), 현재가(int), 시가(int), 고가(int), 저가(int), 등락율(float), 고저평균대비등락율(float), 당일거래대금(int), 초당거래대금(int),
초당거래대금평균(int), 체결강도(float), 직전체결강도(float), 체결강도평균(float), 최고체결강도(float), 초당매수수량(int), 초당매도수량(int),
매도총잔량(float), 매수총잔량(float), 매도호가5(float), 매도호가4(float), 매도호가3(float), 매도호가2(float), 매도호가1(float), 매수호가1(float),
매수호가2(float), 매수호가3(float), 매수호가4(float), 매수호가5(float), 매도잔량5(float), 매도잔량4(float), 매도잔량3(float), 매도잔량2(float),
매도잔량1(float), 매수잔량1(float), 매수잔량2(float), 매수잔량3(float), 매수잔량4(float), 매수잔량5(float)
"""'''
coin_sell_var = '''"""
def SellStrategy(self, *args)
매도(False), 종목명(str), 수익률(float), 최고수익률(float), 보유수량(float), 매수시간(datetime), 현재가(float), 시가(float), 고가(float), 저가(float),
등락율(float), 당일거래대금(int), 초당거래대금평균(int), 초당매수수량(float), 초당매도수량(float), 누적매수량(float), 누적매도량(float), 체결강도(float),
체결강도평균(float), 최고체결강도(float), 직전체결강도(float), 매도총잔량(float), 매수총잔량(float), 매도호가5(float), 매도호가4(float), 매도호가3(float),
매도호가2(float), 매도호가1(float), 매수호가1(float), 매수호가2(float), 매수호가3(float), 매수호가4(float), 매수호가5(float), 매도잔량5(float), 매도잔량4(float),
매도잔량3(float), 매도잔량2(float), 매도잔량1(float), 매수잔량1(float), 매수잔량2(float), 매수잔량3(float), 매수잔량4(float), 매수잔량5(float)
"""'''
coin_buy_signal = '''
if 매수:
    매수수량 = round(self.int_tujagm / 현재가, 8)
    if 매수수량 > 0.00000001:
        남은수량 = 매수수량
        직전남은수량 = 매수수량
        매수금액 = 0
        # 호가정보 딕셔너리에 넣은 잔량의 합이 매수수량보다 클 경우에만 매수주문됩니다.
        # 기본 호가정보의 수를 변경할 경우 반드시 백테스터 self.Buy() 내에서도 호가정보를 수정해야 백테스팅이 올바르게 진행됩니다.
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
            self.list_buy.append(종목명)
            self.coinQ.put(['매수', 종목명, 예상체결가, 매수수량])'''
coin_sell_signal = '''
if 매도:
    남은수량 = 보유수량
    직전남은수량 = 보유수량
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
        예상체결가 = round(매도금액 / 보유수량, 2)
        self.list_sell.append(종목명)
        self.coinQ.put(['매도', 종목명, 예상체결가, 보유수량])'''

stock_buy1 = '''if now() < timedelta_sec(180, VI해제시간):\n    매수 = False'''
stock_buy2 = '''if 현재가 >= VI아래5호가:\n    매수 = False'''
stock_buy3 = '''if 등락율 < 3 or 등락율 > 25:\n    매수 = False'''
stock_buy4 = '''if 고저평균대비등락율 < 0:\n    매수 = False'''
stock_buy5 = '''if 체결강도 < 100:\n    매수 = False'''
stock_buy6 = '''if 체결강도 < 체결강도평균 + 5:\n    매수 = False'''
stock_buy7 = '''if 당일거래대금 < 1000:\n    매수 = False'''
stock_buy8 = '''if 초당거래대금 < 초당거래대금평균 + 90:\n    매수 = False'''
stock_buy9 = '''if 매도총잔량 < 매수총잔량:\n    매수 = False'''
stock_buy10 = '''if 매도잔량1 < 매수잔량1 * 2:\n    매수 = False'''

stock_sell1 = '''if 수익률 <= -2:\n    매도 = True'''
stock_sell2 = '''if 수익률 >= 3:\n    매도 = True'''
stock_sell3 = '''if 최고수익률 > 3 and 수익률 < 최고수익률 * 0.75:\n    매도 = True'''
stock_sell4 = '''if now() > timedelta_sec(1800, 매수시간):\n    매도 = True'''
stock_sell5 = '''if 현재가 > VI아래5호가 * 1.003:\n    매도 = True'''
stock_sell6 = '''if 고저평균대비등락율 < 0:\n    매도 = True'''
stock_sell7 = '''if 체결강도 <= 최고체결강도 - 5:\n    매도 = True'''
stock_sell8 = '''if 매도총잔량 < 매수총잔량:\n    매도 = True'''

coin_buy1 = '''if 등락율 < 3 or 등락율 > 25:\n    매수 = False'''
coin_buy2 = '''if 고저평균대비등락율 < 0:\n    매수 = False'''
coin_buy3 = '''if 현재가 < 시가:\n    매수 = False'''
coin_buy4 = '''if 체결강도 < 100:\n    매수 = False'''
coin_buy5 = '''if 체결강도 < 체결강도평균 + 5:\n    매수 = False'''
coin_buy6 = '''if 직전체결강도 > 체결강도평균 or 체결강도평균 >= 체결강도:\n    매수 = False'''
coin_buy7 = '''if 당일거래대금 < 10000000000:\n    매수 = False'''
coin_buy8 = '''if 초당거래대금 < 초당거래대금평균 + 10000000:\n    매수 = False'''
coin_buy9 = '''if 매도총잔량 < 매수총잔량:\n    매수 = False'''
coin_buy10 = '''if 매도잔량1 < 매수잔량1 * 2:\n    매수 = False'''

coin_sell1 = '''if 수익률 <= -2:\n    매도 = True'''
coin_sell2 = '''if 수익률 >= 3:\n    매도 = True'''
coin_sell3 = '''if 최고수익률 > 3 and 수익률 < 최고수익률 * 0.75:\n    매도 = True'''
coin_sell4 = '''if now() > timedelta_sec(1800, 매수시간):\n    매도 = True'''
coin_sell5 = '''if 체결강도 < 체결강도평균 + 5:\n    매도 = True'''
coin_sell6 = '''if 체결강도 <= 최고체결강도 - 5:\n    매도 = True'''
coin_sell7 = '''if 고저평균대비등락율 < 0:\n    매도 = True'''
coin_sell8 = '''if 매도총잔량 < 매수총잔량:\n    매도 = True'''

dict_oper = {
    25: '장시작 10분전전입니다.',
    24: '장시작 5분전전입니다.',
    23: '장시작 1분전전입니다.',
    22: '장시작 10초전입니다.',
    21: '장시작',
    44: '장마감 5분전전입니다.',
    43: '장마감 1분전전입니다.',
    42: '장마감 10초전전입니다.',
    41: '장마감',
    31: '장후동시호가개시',
    51: '시간외종가매매개시',
    52: '시간외종가매매종료, 시간외단일가매매개시',
    54: '시간외단일가매매종료',
    61: '서킷브레이크1단계발동',
    62: '서킷브레이크1단계해제, 호가접수개시',
    63: '서킷브레이크1단계, 동시호가종료',
    64: '사이드카 매도발동',
    65: '사이드카 매도해제',
    66: '사이드카 매수발동',
    67: '사이드카 매수해제',
    68: '서킷브레이크2단계발동',
    69: '서킷브레이크3단계발동, 당일 장종료',
    70: '서킷브레이크2단계해제, 호가접수개시',
    71: '서킷브레이크2단계, 동시호가종료'
}
