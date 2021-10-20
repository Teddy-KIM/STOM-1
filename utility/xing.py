import pythoncom
import pandas as pd
import win32com.client
from utility.setting import E_OPENAPI_PATH


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


class XAQuery:
    def __init__(self):
        self.com_obj = win32com.client.Dispatch("XA_DataSet.XAQuery")
        win32com.client.WithEvents(self.com_obj, XAQueryEvents).connect(self)
        self.received = False

    def BlockRequest(self, *args, **kwargs):
        self.received = False
        res_name = args[0]
        res_path = E_OPENAPI_PATH + res_name + '.res'
        self.com_obj.ResFileName = res_path
        with open(res_path, encoding='euc-kr') as f:
            res_lines = f.readlines()
        res_data = parseRes(res_lines)
        inblock_code = list(res_data['inblock'][0].keys())[0]
        inblock_field = list(res_data['inblock'][0].values())[0]
        for k in kwargs:
            self.com_obj.SetFieldData(inblock_code, k, 0, kwargs[k])
            if k not in inblock_field:
                print('inblock field error')
        self.com_obj.Request(False)
        while not self.received:
            pythoncom.PumpWaitingMessages()
        df = []
        for outblock in res_data['outblock']:
            outblock_code = list(outblock.keys())[0]
            outblock_field = list(outblock.values())[0]
            data = []
            rows = self.com_obj.GetBlockCount(outblock_code)
            for i in range(rows):
                elem = {k: self.com_obj.GetFieldData(outblock_code, k, i) for k in outblock_field}
                data.append(elem)
            df2 = pd.DataFrame(data=data)
            df.append(df2)
        df = pd.concat(df)
        return df


class XAReal:
    def __init__(self):
        self.com_obj = win32com.client.Dispatch("XA_DataSet.XAReal")
        win32com.client.WithEvents(self.com_obj, XARealEvents).connect(self)
        self.res = {}

    def RegisterRes(self, res_name):
        res_path = E_OPENAPI_PATH + res_name + '.res'
        self.com_obj.ResFileName = res_path
        with open(res_path, encoding="euc-kr") as f:
            res_lines = f.readlines()
            res_data = parseRes(res_lines)
            self.res[res_name] = res_data

    def AddRealData(self, code=None):
        if code is not None:
            self.com_obj.SetFieldData('InBlock', 'shcode', code)
        self.com_obj.AdviseRealData()

    def RemoveRealData(self, code):
        self.com_obj.UnadviseRealDataWithKey(code)

    def RemoveAllRealData(self):
        self.com_obj.UnadviseRealData()

    def GetFielfData(self, field):
        return self.com_obj.GetFieldData("OutBlock", field)


class XASessionEvents:
    def __init__(self):
        self.user_obj = None

    def connect(self, user_obj):
        self.user_obj = user_obj

    def OnLogin(self, code, msg):
        if code == '0000':
            self.user_obj.connected = True


class XAQueryEvents:
    def __init__(self):
        self.user_obj = None

    def connect(self, user_obj):
        self.user_obj = user_obj

    def OnReceiveData(self, code):
        self.user_obj.received = True


class XARealEvents:
    def __init__(self):
        self.user_obj = None

    def connect(self, user_obj):
        self.user_obj = user_obj

    def OnReceiveRealData(self, trcode):
        res_data = self.user_obj.res.get(trcode)
        out_data = {}
        out_block = res_data['outblock'][0]
        for field in out_block['OutBlock']:
            data = self.user_obj.GetFielfData(field)
            out_data[field] = data
        if trcode == 'JIF':
            self.user_obj.OnReceiveOperData(out_data)
        elif trcode == 'VI_':
            self.user_obj.OnReceiveVIData(out_data)
        elif trcode in ['S3_', 'K3_']:
            self.user_obj.OnReceiveRealData(out_data)
        elif trcode in ['H1_', 'HA_']:
            self.user_obj.OnReceiveHogaData(out_data)
        elif trcode == 'SC1_':
            self.user_obj.OnReceiveChegeolData(out_data)

    def OnReceiveSearchRealData(self, trcode, data):
        self.user_obj.OnReceiveSearchRealData(data)


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
