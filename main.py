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

lian_jia = {
    'ua': 'HomeLink7.7.6; Android 7.0',
    'app_id': '20161001_android',
    'app_secret': '7df91ff794c67caee14c3dacd5549b35'}


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


# 获取城市信息（某个）
def get_city_info(city_id):
    """
    获取城市信息
    """
    url = 'http://app.api.lianjia.com/config/config/initData'

    payload = {
        'params': '{{"city_id": {}, "mobile_type": "android", "version": "8.0.1"}}'.format(city_id),
        'fields': '{"city_info": "", "city_config_all": ""}'
    }

    data = get_data(url, payload, method='POST')
    city_info = data['city_info']['info'][0]

    for a_city in data['city_config_all']['list']:
        if a_city['city_id'] == city_id:
            # 查找城市名称缩写
            city_info['city_abbr'] = a_city['abbr']
            break

    else:
        logging.error(f'# 抱歉, 链家网暂未收录该城市~')
        sys.exit(1)
    '''
    city_id 城市id
    city_name 城市名称
    subway_line 地铁线
    district  行政区
    city_abbr 城市简称
    以及
    bizcircle_id 商圈id
    bizcircle_name 商圈名

    '''
    return city_info



# 获取全部城市信息
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


# 获取租房成交数据(城市id,)
# 成都:510100
def get_rented(city_id, condition):
    url = "https://app.api.lianjia.com/house/rented/search"
    rented=[]
    offset = 0
    total_count = get_rented_count(city_id, condition) #  该商圈的总记录数
    r_total_count=total_count#最后返回时，记录总数

    if total_count <=2000 and total_count!=0:
        #总数小于2000，且总数不为0
        offset=0
        while offset<total_count:
            params = {
                'limit_offset': offset,  # 请求数
                'city_id': city_id,
                'limit_count': 100,  # 单次请求数量
                'condition': condition} # 筛选条件

            data = get_data(url, params)
            print('         获取成交房源进度：{:.2f}%'.format(offset / total_count * 100))
            for d in data['list']:
                rented.append(d)
            offset += 100

    elif total_count>2000:
        # 总数大于2000条
        #以500为间隔，来获取数据
        #注意，以500为间，可能部分价格区间无数据
        for i in range(0, 5000, 500):
            offset = 0
            #以500为间
            if i <4500:
                #价格在4500以下，以500为间隔，4500以上，直接计算
                total_count_p = get_rented_count(city_id, condition+'brp{}erp{}'.format(i,i + 499))  # 该商圈/价格的总记录数
                if total_count_p<=2000 and total_count_p!=0:
                    while offset<total_count_p:
                        params = {
                            'limit_offset': offset,  # 请求数
                            'city_id': city_id,
                            'limit_count': 100,  # 单次请求数量
                            'condition': condition + 'brp{}erp{}'.format(i, i + 499)  # 筛选条件
                        }
                        data = get_data(url, params)
                        print('         获取成交房源进度：{:.2f}%'.format(offset /total_count_p  * 100),' 总数：{}'.format(total_count_p),' 价格：{}-{}'.format(i,i+499))
                        for d in data['list']:
                            rented.append(d)
                        offset += 100
                else:
                    print('         该区间无记录或记录超过2000条')

            else:#价格大于等于4500
                total_count_p = get_rented_count(city_id, condition+'brp{}erp{}'.format(i,999999))  # 该商圈/价格的总记录数
                if total_count_p <= 2000 and total_count_p != 0:
                    while offset < total_count_p:
                        params = {
                            'limit_offset': offset,  # 请求数
                            'city_id': city_id,
                            'limit_count': 100,  # 单次请求数量
                            'condition': condition + 'brp{}erp{}'.format(i, 999999)  # 筛选条件
                        }
                        data = get_data(url, params)
                        print('         获取成交房源进度：{:.2f}%'.format(offset /total_count_p  * 100),' 总数：{}'.format(total_count_p),' 价格：{}-{}'.format(i,999999))
                        for d in data['list']:
                            rented.append(d)
                        offset += 100
                else:
                    print('         该区间无记录或记录超过2000条')


    print('             总记录数:', r_total_count, '返回记录数', len(rented))
    return rented

#获取租房成交总数
def get_rented_count(city_id, condition):
    url = "https://app.api.lianjia.com/house/rented/search"
    offset = 0
    params = {
        'limit_offset': offset,  # 请求数
        'city_id': city_id,
        'limit_count': 100,  # 单次请求数量
        'condition': condition,  # 筛选条件

    }
    data = get_data(url, params)
    total_count = data['total_count']  # 总记录数
    return total_count


if __name__ == '__main__':

    cityid = 510100  # 设置城市id
    cityinfo = get_city_info(cityid)
    conn = MongoClient('127.0.0.1', 27017)
    db = conn.链家网  # 连接mydb数据库，没有则自动创建
    db1 = db[cityinfo['city_name'] + '行政区域及商圈信息']
    db2 = db[cityinfo['city_name'] + '租房成交信息']

    # allcity = get_allcity()

    print('正在获取 {}【{}】行政区域及商圈信息\n'.format(cityid, cityinfo['city_name']))

    for i in cityinfo['district']:
        # 遍历行政区，获取商圈，写入数据库
        db1.update_one({'district_id': i['district_id']},
                       {'$set': i},
                       upsert=True)
        db1.update_one({'district_id': i['district_id']},
                       {'$set': {'city_name': cityinfo['city_name'],
                                 'city_id': cityinfo['city_id'],
                                 '更新时间': datetime.datetime.now()

                                 }},
                       upsert=True)
        print('{}>{} 商圈数：{}'.format(cityinfo['city_name'], i['district_name'], len(i['bizcircle'])))

        if len(i['bizcircle']) != 0:  # 判断商圈不为0
            bizcircle_js = 0  # 默认商圈记数
            for ii in i['bizcircle']:
                bizcircle_js += 1
                print('     当前 {} > {}，进度：{:.2f}%'.format(
                    i['district_name'], ii['bizcircle_name'], bizcircle_js / len(i['bizcircle']) * 100))

                rented = get_rented(cityid, ii['bizcircle_quanpin']+'/')  # 传入商圈全拼，获取出租成交信息

                for r in rented:
                    # 遍历成交信息
                    # 写入数据库

                    db2.update_one({'house_code': r['house_code']},
                                   {'$set': r},
                                   upsert=True)
                    db2.update_one({'house_code': r['house_code']},
                                   {'$set': {'城市': cityinfo['city_name'],
                                             '城市ID': cityinfo['city_id'],
                                             '商圈名称': ii['bizcircle_name'],
                                             '商圈全拼': ii['bizcircle_quanpin'],
                                             '商圈ID': ii['bizcircle_id'],
                                             '更新时间': datetime.datetime.now()

                                             }},
                                   upsert=True)

                #print('                 写入完成数量:', len(rented), '\n')

    print('\n全部结束\n')
