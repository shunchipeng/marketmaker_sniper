import pymongo
import urllib
import pandas as pd
import requests
from io import StringIO
from datetime import datetime
host = 'je0cr26llk.myqnapcloud.com'
port = '9997'
user = 'tigerjump'
password = urllib.parse.quote_plus('UDyX6Lk%&P4ehGqcL9dABP^2no%*')

database_name = 'stock'
collection_name = 'twse'

client = pymongo.MongoClient('mongodb://%s:%s@%s:%s' % (user, password, host, port))
db = client[database_name]
collection = db[collection_name]

# 拿大盤加權收盤值
a = requests.get('https://openapi.twse.com.tw/v1/indicesReport/MI_5MINS_HIST')
df = pd.DataFrame(a.json())

# 日期修正 民國年轉西元年 再轉字串
for i in range(len(df)):
    df['Date'].iloc[i] = df['Date'].iloc[i].replace(df['Date'].iloc[i][0:3], str(int(df['Date'].iloc[i][0:3]) + 1911))
# 將Date欄位type從str轉datetime
df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
df['Date'] = df['Date'].dt.strftime('%Y/%m/%d')
insert_result = collection.insert_many(df.to_dict(orient='records'))


