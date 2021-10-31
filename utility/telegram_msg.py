import telegram
import pandas as pd
from telegram.ext import Updater, MessageHandler, Filters
from utility.setting import ui_num, DICT_SET


class TelegramMsg:
    def __init__(self, qlist):
        """
                    0        1       2        3       4       5          6          7        8      9
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceiv1Q, creceiv2Q, stockQ, coinQ,
                 sstgQ, cstgQ, tick1Q, tick2Q, tick3Q, tick4Q, tick5Q, chartQ, hogaQ]
                   10    11      12      13      14      15      16      17     18
        """
        self.windowQ = qlist[0]
        self.teleQ = qlist[4]
        self.stockQ = qlist[8]
        self.coinQ = qlist[9]
        self.dict_set = None
        self.updater = None
        self.bot = None
        self.UpdateBot(DICT_SET)
        self.Start()

    def Start(self):
        while True:
            data = self.teleQ.get()
            if type(data) == str:
                self.SendMsg(data)
            elif type(data) == pd.DataFrame:
                self.UpdateDataframe(data)
            elif type(data) == dict:
                if self.updater is not None:
                    self.updater.stop()
                    self.updater = None
                self.UpdateBot(data)

    def __del__(self):
        if self.updater is not None:
            self.updater.stop()

    def UpdateBot(self, dict_set):
        self.dict_set = dict_set
        if self.updater is None and self.dict_set['텔레그램봇토큰'] is not None:
            self.bot = telegram.Bot(self.dict_set['텔레그램봇토큰'])
            self.SetCustomButton()
        else:
            self.bot = None

    def SetCustomButton(self):
        custum_button = [['/당일체결목록', '/당일거래목록', '/계좌잔고평가', '/잔고청산주문']]
        reply_markup = telegram.ReplyKeyboardMarkup(custum_button)
        self.bot.send_message(chat_id=self.dict_set['텔레그램사용자아이디'],
                              text='사용자버튼 설정을 완료하였습니다.',
                              reply_markup=reply_markup)
        self.updater = Updater(self.dict_set['텔레그램봇토큰'])
        self.updater.dispatcher.add_handler(MessageHandler(Filters.text, self.ButtonClicked))
        self.updater.start_polling(drop_pending_updates=True)

    def ButtonClicked(self, update, context):
        if context == '':
            return
        self.stockQ.put(update.message.text)

    def SendMsg(self, msg):
        if self.bot is not None:
            try:
                self.bot.sendMessage(chat_id=self.dict_set['텔레그램사용자아이디'], text=msg)
            except Exception as e:
                self.windowQ.put([ui_num['설정텍스트'], f'시스템 명령 오류 알림 - SendMsg {e}'])
        else:
            self.windowQ.put([ui_num['설정텍스트'], '시스템 명령 오류 알림 - 텔레그램 봇이 설정되지 않아 메세지를 보낼 수 없습니다.'])

    def UpdateDataframe(self, df):
        if df.columns[1] == '매수금액':
            df = df[::-1]
            text = ''
            for index in df.index:
                ct = df['체결시간'][index][8:10] + ':' + df['체결시간'][index][10:12]
                per = str(df['수익률'][index]) + '%'
                if len(per.split('.')[0]) == 1:
                    per = '  ' + per
                sg = format(int(df['수익금'][index]), ',') + '원'
                if len(sg.split(',')[0]) == 2:
                    sg = '    ' + sg
                elif len(sg.split(',')[0]) == 3:
                    sg = '  ' + sg
                name = df['종목명'][index]
                text += f'{ct} {per} {sg} {name}\n'
            self.SendMsg(text)
        elif df.columns[1] == '매입가':
            text = ''
            for index in df.index:
                per = str(df['수익률'][index]) + '%'
                if len(per.split('.')[0]) == 1:
                    per = ' ' + per
                sg = format(int(df['평가손익'][index]), ',') + '원'
                if len(sg.split(',')[0]) == 2:
                    sg = '    ' + sg
                elif len(sg.split(',')[0]) == 3:
                    sg = '  ' + sg
                name = df['종목명'][index]
                text += f'{per} {sg} {name}\n'
            tbg = format(int(df['매입금액'].sum()), ',') + '원'
            tpg = format(int(df['평가금액'].sum()), ',') + '원'
            tsg = format(int(df['평가손익'].sum()), ',') + '원'
            tsp = str(round(df['평가손익'].sum() / df['매입금액'].sum() * 100, 2)) + '%'
            text += f'{tbg} {tpg} {tsp} {tsg}\n'
            self.SendMsg(text)
        elif df.columns[1] == '주문구분':
            df = df[::-1]
            text = ''
            for index in df.index:
                ct = df['체결시간'][index][8:10] + ':' + df['체결시간'][index][10:12]
                bs = df['주문구분'][index]
                bp = int(df['체결가'][index])
                name = df['종목명'][index]
                text += f'{ct} {bs} {bp} {name}\n'
            self.SendMsg(text)
