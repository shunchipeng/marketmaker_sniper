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
collection_name = 'option'

client = pymongo.MongoClient('mongodb://%s:%s@%s:%s' % (user, password, host, port))
db = client[database_name]
collection = db[collection_name]

a = requests.post(
    "https://www.taifex.com.tw/cht/3/dlOptDataDown",
    data={
        "down_type": 1,
        "commodity_id": "TXO",
        "commodity_id2": "",
        "queryStartDate": "2023/07/27",
        "queryEndDate": "2023/07/27"
    })
b = pd.read_csv(StringIO(a.text),index_col=False)
c = b[b['契約']=='TXO']
d = c.to_dict(orient='records')

# data_to_insert = [{'hihi': 'here'}, {'qwe': 'rty'}]
# insert_result = collection.insert_many(d)
# print('result', insert_result.acknowledged)
# print('result', insert_result.inserted_ids)

day = datetime.strftime(datetime(2023, 7, 28).date(), '%Y/%m/%d')
pip = [{"$project": {
                    "交易日期": 1,
                    "到期月份(週別)": 1,
                    "買賣權": 1,
                    "交易時段": 1,
                    "未沖銷契約數": 1
                    }
        },
       {"$match": {
                    "買賣權": "買權",
                    "交易時段": "一般",
                    "交易日期": {"$gte": day}
                    }
       }]

cur = collection.aggregate(pip)
# cur = collection.find({'date':{'$gte':datetime(2023,7,28)}})
data = pd.DataFrame(cur)