import logging
import os
import sys
from datetime import datetime, timedelta
import time
import base64
import hashlib
import requests
from pymongo import MongoClient
import datetime
from queue import Queue
import threading

#获取全国主要城市的二手房挂牌数量


lian_jia = {
    'ua': 'HomeLink7.7.6; Android 7.0',
    'app_id': '20161001_android',
    'app_secret': '7df91ff794c67caee14c3dacd5549b35'}
flag = False #退出标志
data_queue = Queue() # 存放解析数据的queue


# 获取token
def get_token(params):
    data = list(params.items())
    data.sort()
    token = lian_jia['app_secret']
    for entry in data:
        token += '{}={}'.format(*entry)
    token = hashlib.sha1(token.encode()).hexdigest()
    token = '{}:{}'.format(lian_jia['app_id'], token)
    token = base64.b64encode(token.encode()).decode()
    return token


# 解析数据
def parse_data(response):
    as_json = response.json()
    if as_json['errno']:
        # 发生了错误
        raise Exception('请求出错了: ' + as_json['error'])
    else:
        return as_json['data']


# 获取数据
def get_data(url, payload, method='GET', session=None):
    payload['request_ts'] = int(time.time())

    headers = {
        'User-Agent': lian_jia['ua'],
        'Authorization': get_token(payload)
    }
    if session:
        if method == 'GET':
            r = session.get(url, params=payload, headers=headers)
        else:
            r = session.post(url, data=payload, headers=headers)
    else:
        func = requests.get if method == 'GET' else requests.post
        r = func(url, payload, headers=headers)

    return parse_data(r)

#获取二手房数量
def get_ershoufang_count(city_id,condition):
    url = "https://app.api.lianjia.com/house/ershoufang/searchv4"
    offset = 0
    params = {
        'ad_recommend': 1,
        'city_id': city_id,
        'has_recommend': 1,
        'limit_offset': offset,  # 请求数
        'limit_count': 20,  # 单次请求数量
        'condition': condition,  # 筛选条件
    }
    data = get_data(url, params)
    total_count = data['total_count']
    return total_count

def get_allcity():
    """
    获取城市信息
    """
    url = 'http://app.api.lianjia.com/config/config/initData'

    payload = {
        'params': '{{"city_id": {}, "mobile_type": "android", "version": "8.0.1"}}'.format(510100),
        'fields': '{"city_info": "", "city_config_all": ""}'
    }

    data = get_data(url, payload, method='POST')
    allcity = []
    for a_city in data['city_config_all']['list']:
        allcity.append({'city_id': a_city['city_id'], 'city_name': a_city['city_name'], 'abbr': a_city['abbr']
                        })

    return allcity


def main():
    city=get_allcity()
    conn = MongoClient('127.0.0.1', 27017)
    db = conn.链家网  # 连接mydb数据库，没有则自动创建
    db2 = db['全国主要城市二手房挂牌数量(API)']

    db2.update_one({'日期': str(datetime.date.today())},
                   {'$set': {'更新时间': datetime.datetime.now()}
                    }, upsert=True)

    for c in city:
        ershoufang_count=get_ershoufang_count(c['city_id'],'')
        db2.update_one({'日期': str(datetime.date.today())},
                       {'$set': {c['city_name']: ershoufang_count}
                        }, upsert=True)
        print(datetime.datetime.now(),c['city_name'],'({})'.format(c['city_id']),'挂牌数量：{}'.format(ershoufang_count) )



if __name__ == '__main__':
    print('\n正在获取全国主要城市挂牌数量：')
    d1=datetime.datetime.now()
    main()
    d2=datetime.datetime.now()
    print('总用时：{}，完成时间：{}'.format(d2-d1,datetime.datetime.now()))

