import sqlite3
import pandas as pd
from utility.setting import DB_STOCK_TICK
############################################################################
# 2021년 10월 21일 VI해제시간 기록에 오류가 있습니다.
# 시간 뒤에 .0이 붙어있으니 이 파일을 실행하여 업데이트 하시길 바랍니다.
############################################################################

con = sqlite3.connect(DB_STOCK_TICK)
df = pd.read_sql("SELECT name FROM sqlite_master WHERE TYPE = 'table'", con)
table_list = list(df['name'].values)
table_list.remove('moneytop')
table_list.remove('codename')
if 'dist' in table_list:
    table_list.remove('dist')
if 'dist_chk' in table_list:
    table_list.remove('dist_chk')
if 'sqlite_sequence' in table_list:
    table_list.remove('sqlite_sequence')
if 'temp' in table_list:
    table_list.remove('temp')

print(f'데이터베이스 VI해제시간 오류 업데이트 시작')
last = len(table_list)
for i, code in enumerate(table_list):
    print(f'데이터베이스 VI해제시간 업데이트 중 ... [{i+1}/{last}]')
    df = pd.read_sql(f"SELECT * FROM '{code}'", con)
    df = df.set_index('index')
    df['VI해제시간'] = df['VI해제시간'].apply(lambda x: str(x).split('.')[0] if type(x) == float else x)
    df.to_sql(code, con, if_exists='replace', chunksize=1000, method='multi')
con.close()
print(f'데이터베이스 VI해제시간 오류 업데이트 완료')
