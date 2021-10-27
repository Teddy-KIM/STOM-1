import pythoncom
import pandas as pd
import win32com.client
from utility.setting import E_OPENAPI_PATH
from utility.static import now, timedelta_sec


def parse_block(data):
    block_info = data[0]
    tokens = block_info.split(",")
    block_code, block_type = tokens[0], tokens[-1][:-1]
    field_codes = []
    fields = data[2:]
    for line in fields:
        if len(line) > 0:
            field_code = line.split(',')[1].strip()
            field_codes.append(field_code)
    ret_data = {block_code: field_codes}
    return block_type, ret_data


def parseRes(lines):
    lines = [line.strip() for line in lines]
    info_index = [i for i, x in enumerate(lines) if x.startswith((".Func", ".Feed"))][0]
    begin_indices = [i - 1 for i, x in enumerate(lines) if x == "begin"]
    end_indices = [i for i, x in enumerate(lines) if x == "end"]
    block_indices = zip(begin_indices, end_indices)
    ret_data = {"trcode": None, "inblock": [], "outblock": []}
    tr_code = lines[info_index].split(',')[2].strip()
    ret_data["trcode"] = tr_code
    for start, end in block_indices:
        block_type, block_data = parse_block(lines[start:end])
        if block_type == "input":
            ret_data["inblock"].append(block_data)
        else:
            ret_data["outblock"].append(block_data)
    return ret_data


class XASession:
    def __init__(self):
        self.com_obj = win32com.client.Dispatch("XA_Session.XASession")
        win32com.client.WithEvents(self.com_obj, XASessionEvents).connect(self)
        self.connected = False

    def Login(self, user_id, password, cert):
        self.com_obj.ConnectServer('hts.ebestsec.co.kr', 20001)
        self.com_obj.Login(user_id, password, cert, 0, 0)
        while not self.connected:
            pythoncom.PumpWaitingMessages()

    def GetAccountList(self, index):
        account = self.com_obj.GetAccountList(index)
        return account


class XAQuery:
    def __init__(self, user_class):
        self.com_obj = win32com.client.Dispatch("XA_DataSet.XAQuery")
        win32com.client.WithEvents(self.com_obj, XAQueryEvents).connect(self, user_class)
        self.received = False

    def BlockRequest(self, *args, **kwargs):
        self.received = False
        res_name = args[0]
        res_path = E_OPENAPI_PATH + '/Res/' + res_name + '.res'
        self.com_obj.ResFileName = res_path
        with open(res_path, encoding='euc-kr') as f:
            res_lines = f.readlines()
        res_data = parseRes(res_lines)
        inblock_code = list(res_data['inblock'][0].keys())[0]
        for k in kwargs:
            self.com_obj.SetFieldData(inblock_code, k, 0, kwargs[k])
        if res_name == 't1857':
            self.com_obj.RequestService(res_name, '')
        else:
            self.com_obj.Request(False)
        if res_name in ['t1857', 't1866']:
            sleeptime = timedelta_sec(1)
        elif res_name in ['t0424', 't8430']:
            sleeptime = timedelta_sec(0.5)
        else:
            sleeptime = timedelta_sec(0.05)
        while not self.received or now() < sleeptime:
            pythoncom.PumpWaitingMessages()
        df = []
        for outblock in res_data['outblock']:
            outblock_code = list(outblock.keys())[0]
            outblock_field = list(outblock.values())[0]
            data = []
            rows = self.com_obj.GetBlockCount(outblock_code)
            for i in range(rows):
                elem = {k: self.GetFieldData(outblock_code, k, i) for k in outblock_field}
                data.append(elem)
            df2 = pd.DataFrame(data=data)
            df.append(df2)
        df = pd.concat(df)
        if res_name == 't1866':
            df = df.set_index('query_index')
            df = df[['query_name']].copy()
            df = df.dropna()
        return df

    def GetFieldData(self, outblock_code, k, i):
        return self.com_obj.GetFieldData(outblock_code, k, i)

    def RemoveService(self, alertnum):
        self.com_obj.RemoveService('t1857', alertnum)

    def GetFieldSearchRealData(self, field):
        return self.com_obj.GetFieldSearchRealData('t1857OutBlock1', field)


class XAReal:
    def __init__(self, user_class):
        self.com_obj = win32com.client.Dispatch("XA_DataSet.XAReal")
        win32com.client.WithEvents(self.com_obj, XARealEvents).connect(self, user_class)
        self.res = {}

    def RegisterRes(self, res_name):
        res_path = E_OPENAPI_PATH + '/Res/' + res_name + '.res'
        self.com_obj.ResFileName = res_path
        with open(res_path, encoding="euc-kr") as f:
            res_lines = f.readlines()
            res_data = parseRes(res_lines)
            self.res[res_name] = res_data

    def AddRealData(self, code=None):
        if code is not None:
            if code == '0':
                self.com_obj.SetFieldData('InBlock', 'jangubun', code)
            else:
                self.com_obj.SetFieldData('InBlock', 'shcode', code)
        self.com_obj.AdviseRealData()

    def RemoveRealData(self, code):
        self.com_obj.UnadviseRealDataWithKey(code)

    def RemoveAllRealData(self):
        self.com_obj.UnadviseRealData()

    def GetFieldData(self, field):
        return self.com_obj.GetFieldData('OutBlock', field)


class XASessionEvents:
    def __init__(self):
        self.com_class = None

    def connect(self, com_class):
        self.com_class = com_class

    def OnLogin(self, code, msg):
        if code == '0000':
            self.com_class.connected = True


class XAQueryEvents:
    def __init__(self):
        self.com_class = None
        self.user_class = None

    def connect(self, com_class, user_class):
        self.com_class = com_class
        self.user_class = user_class

    def OnReceiveData(self, trcode):
        self.com_class.received = True

    def OnReceiveSearchRealData(self, trcode):
        out_data = {'code': self.com_class.GetFieldSearchRealData('shcode'),
                    'gubun': self.com_class.GetFieldSearchRealData('JobFlag')}
        self.user_class.OnReceiveSearchRealData(out_data)


class XARealEvents:
    def __init__(self):
        self.com_class = None
        self.user_class = None

    def connect(self, com_class, user_class):
        self.com_class = com_class
        self.user_class = user_class

    def OnReceiveRealData(self, trcode):
        res_data = self.com_class.res.get(trcode)
        out_data = {}
        out_block = res_data['outblock'][0]
        for field in out_block['OutBlock']:
            data = self.com_class.GetFieldData(field)
            out_data[field] = data
        if trcode == 'JIF':
            self.user_class.OnReceiveOperData(out_data)
        elif trcode == 'VI_':
            self.user_class.OnReceiveVIData(out_data)
        elif trcode in ['S3_', 'K3_']:
            self.user_class.OnReceiveRealData(out_data)
        elif trcode in ['H1_', 'HA_']:
            self.user_class.OnReceiveHogaData(out_data)
        elif trcode == 'SC1':
            self.user_class.OnReceiveChegeolData(out_data)
