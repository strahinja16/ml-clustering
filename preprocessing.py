import pandas as pd
from datetime import datetime

FILE_PATH = './2019-Oct.csv'
PROCESSED_FILE_NAME = './processed_data1.csv'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S UTC'

TYPE_MAP = {
    'view': 0,
    'cart': 1,
    'purchase': 2,
    'remove_from_cart': 3
}

BRAND_COUNTER = 0
BRAND_MAP = {}
SESSION_COUNTER = 0
SESSION_MAP =  {}

data = pd.read_csv(FILE_PATH)

def get_hour_from_date(value: str):
    date = datetime.strptime(value, DATE_FORMAT)
    return date.hour


def get_event_type(value: str):
    if value in TYPE_MAP.keys():
        return TYPE_MAP[value]
    else:
        return -1


def get_brand(value: str):
    global BRAND_COUNTER
    if value in BRAND_MAP.keys():
        return BRAND_MAP[value]
    else:
        BRAND_MAP[value] = BRAND_COUNTER
        BRAND_COUNTER += 1
        return BRAND_MAP[value]

def get_session_id(value: str):
    global SESSION_COUNTER
    if value in SESSION_MAP.keys():
        return SESSION_MAP[value]
    else:
        SESSION_MAP[value] = SESSION_COUNTER
        SESSION_COUNTER += 1
        return SESSION_MAP[value]

new_data_columns = [
    'event_time',
    'event_type',
    'product_id',
    'category_id',
    'brand', 
    'price', 
    'user_id',
    'user_session',
]

BATCH_SIZE = 10000
counter = 0
cnt_2 = 1
record_count = data.shape[0]
new_data = []

for index, row in data.iterrows():
    counter = counter + 1
    obj = {
        'event_time': get_hour_from_date(row["event_time"]),
        'event_type': get_event_type(row["event_type"]),
        'product_id': row["product_id"],
        'category_id': row["category_id"],
        'brand': get_brand(row["brand"]),
        'price': row["price"],
        'user_id': row["user_id"],
        'user_session': get_session_id(row["user_session"]),
    }
    new_data.append(obj)
    if counter == BATCH_SIZE:
        print('Status ', ((cnt_2 * BATCH_SIZE)/record_count * 100), ' %')
        cnt_2 = cnt_2 + 1
        counter = 0
        df2 = pd.DataFrame(new_data)
        new_data = []
        df2.to_csv(PROCESSED_FILE_NAME, mode='a')

new_data = pd.DataFrame(new_data)
new_data.to_csv(PROCESSED_FILE_NAME, mode='a')

