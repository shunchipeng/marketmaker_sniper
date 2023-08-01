import pymongo
import urllib
import pandas as pd
from datetime import datetime

def get_current_option_period():
    result = collection.find_one({}, sort=[("交易日期", pymongo.DESCENDING)]).get("到期月份(週別)")
    return result

def get_last_twse_close():
    host = 'je0cr26llk.myqnapcloud.com'
    port = '9997'
    user = 'tigerjump'
    password = urllib.parse.quote_plus('UDyX6Lk%&P4ehGqcL9dABP^2no%*')

    database_name = 'stock'
    collection_name = 'twse'

    client = pymongo.MongoClient('mongodb://%s:%s@%s:%s' % (user, password, host, port))
    db = client[database_name]
    collection = db[collection_name]
    result = int(collection.find_one({},sort=[("Date", pymongo.DESCENDING)]).get('ClosingIndex'))
    return result

host = 'je0cr26llk.myqnapcloud.com'
port = '9997'
user = 'tigerjump'
password = urllib.parse.quote_plus('UDyX6Lk%&P4ehGqcL9dABP^2no%*')

database_name = 'stock'
collection_name = 'option_txo'

client = pymongo.MongoClient('mongodb://%s:%s@%s:%s' % (user, password, host, port))
db = client[database_name]
collection = db[collection_name]

current_contract = get_current_option_period()
current_twse_close = get_last_twse_close()

day = datetime.strftime(datetime(2023, 7, 28).date(), '%Y/%m/%d')
pip = [{"$project": {
                    "交易日期": 1,
                    "到期月份(週別)": 1,
                    "買賣權": 1,
                    "交易時段": 1,
                    "未沖銷契約數": 1,
                    "履約價": 1
                    }
        },
       {"$match": {
                    "買賣權": "買權",
                    "交易時段": "一般",
                    "交易日期": {"$gte": day},
                    "到期月份(週別)": current_contract
                    }
       }]

cur = collection.aggregate(pip)
data = pd.DataFrame(cur)
data['未沖銷契約數'] = data['未沖銷契約數'].astype(int)
data['spread'] = (data['履約價']-current_twse_close)*data['未沖銷契約數']