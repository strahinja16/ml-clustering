import pandas as pd
from datetime import datetime
from os import path, remove
import json

FILE_PATH = './2019-Nov.csv'
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
PRODUCT_COUNTER = 0
PRODUCT_MAP = {}
USER_COUNTER = 0
USER_MAP = {}
CATEGORY_COUNTER = 0
CATEGORY_MAP = {}

data = pd.read_csv(FILE_PATH)

def get_date_time(value: str):
    date = datetime.strptime(value, DATE_FORMAT)
    return date.day, date.weekday(), date.hour


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

def get_user_id(value: str):
    global USER_COUNTER
    if value in USER_MAP.keys():
        return USER_MAP[value]
    else:
        USER_MAP[value] = USER_COUNTER
        USER_COUNTER += 1
        return USER_MAP[value]

def get_category_id(value: str):
    global CATEGORY_COUNTER
    if value in CATEGORY_MAP.keys():
        return CATEGORY_MAP[value]
    else:
        CATEGORY_MAP[value] = CATEGORY_COUNTER
        CATEGORY_COUNTER += 1
        return CATEGORY_MAP[value]

def get_product_id(value: str):
    global PRODUCT_COUNTER
    if value in PRODUCT_MAP.keys():
        return PRODUCT_MAP[value]
    else:
        PRODUCT_MAP[value] = PRODUCT_COUNTER
        PRODUCT_COUNTER += 1
        return PRODUCT_MAP[value]

new_data_columns = [
    'day',
    'weekday',
    'hour',
    'event_type',
    'product_id',
    'category_id',
    'brand', 
    'price', 
    'user_id',
    'user_session',
]

data = data.dropna(subset=["brand"])
BATCH_SIZE = 10000
counter = 0
cnt_2 = 1
record_count = data.shape[0]
new_data = []

if path.exists(PROCESSED_FILE_NAME):
    remove(PROCESSED_FILE_NAME)

df_columns = pd.DataFrame(columns=new_data_columns)
df_columns.to_csv(PROCESSED_FILE_NAME, mode='a', header=True, index=False)

for index, row in data.iterrows():
    counter = counter + 1
    day, weekday, hour = get_date_time(row["event_time"])
    obj = {
        'day': day,
        'weekday': weekday,
        'hour': hour,
        'event_type': get_event_type(row["event_type"]),
        'product_id': get_product_id(row["product_id"]),
        'category_id': get_category_id(row["category_id"]),
        'brand': get_brand(row["brand"]),
        'price': row["price"],
        'user_id': get_user_id(row["user_id"]),
        'user_session': get_session_id(row["user_session"]),
    }
    new_data.append(obj)
    if counter == BATCH_SIZE:
        print('Status ', (round((cnt_2 * BATCH_SIZE)/record_count * 100,2)), ' %')
        cnt_2 = cnt_2 + 1
        counter = 0
        df2 = pd.DataFrame(new_data)
        new_data = []
        df2.to_csv(PROCESSED_FILE_NAME, mode='a', header=False, index=False)

JSON_FILE  = './dictionaries.json'

if path.exists(JSON_FILE):
    remove(JSON_FILE)

dict_for_json = {
    'brand_count' : BRAND_COUNTER,
    'brand_map': BRAND_MAP,
    'session_count': SESSION_COUNTER,
    'session_map': SESSION_MAP,
    'product_count': PRODUCT_COUNTER,
    'product_map': PRODUCT_MAP,
    'user_count': USER_COUNTER,
    'user_map': USER_MAP,
    'category_count': CATEGORY_COUNTER,
    'category_map': CATEGORY_MAP,
}

with open(JSON_FILE, 'w') as f_desc:
    json.dump(dict_for_json, f_desc, indent=4)
