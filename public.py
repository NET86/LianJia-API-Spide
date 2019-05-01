import base64
import hashlib
import logging
import sys
import time
from queue import Queue

import requests
#将公用方法放到这个文件

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
    try:
        city_info = data['city_info']['info'][0]
    except:
        print('ID：{} 无对应城市'.format(city_id))
        return {}

    for a_city in data['city_config_all']['list']:
        if a_city['city_id'] == city_id:
            # 查找城市名称缩写
            city_info['city_abbr'] = a_city['abbr']
            break
    else:
        logging.error(f'# 抱歉, 链家网暂未收录该城市~')
        sys.exit(1)

    return city_info

# 获取城市信息（全部）
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

# 获取租房成交数据(城市id,参数)，房源数量应控制在2000以下
def get_rented(city_id, condition):
    url = "https://app.api.lianjia.com/house/rented/search"
    rented = []
    offset = 0
    total_count = get_rented_count(city_id, condition)  # 该商圈的总记录数
    # 总数小于2000，且总数不为0
    while offset < total_count:
        params = {
            'limit_offset': offset,  # 请求数
            'city_id': city_id,
            'limit_count': 100,  # 单次请求数量
            'condition': condition}  # 筛选条件
        data = get_data(url, params)
        # print('获取成交房源进度：{:.2f}%'.format(offset / total_count * 100))
        for d in data['list']:
            rented.append(d)
        offset += 100
    return rented

# 获取租房成交总数
def get_rented_count(city_id, condition):
    url = "https://app.api.lianjia.com/house/rented/search"
    offset = 0
    params = {
        'limit_offset': offset,  # 请求数
        'city_id': city_id,
        'limit_count': 20,  # 单次请求数量
        'condition': condition,  # 筛选条件

    }
    data = get_data(url, params)
    total_count = data['total_count']  # 总记录数
    return total_count

# 传入城市ID，参数，价格区间，返回该区间的成交数，以及价格拆分为二
# {'rented_count': 2835, 'bpep': {0: 125, 125: 250}}
def get_rented_2000(cityid, condition, bp, ep):
    condition2 = condition + 'brp{}erp{}'.format(bp, ep)
    rented_count2 = get_rented_count(cityid, condition2)
    if rented_count2 <= 2000 and rented_count2 > 0:
        return {'rented_count': rented_count2, 'bpep': {bp: ep}}
    elif rented_count2 > 2000:
        #返回的rented_count2为一组'bpep': {0: 125, 125: 250}的总数量
        return {'rented_count': rented_count2, 'bpep': {bp: int((ep+bp) / 2), int((ep+bp) / 2): ep}}
    elif rented_count2 == 0:
        return {'rented_count': rented_count2}

def do_rented_2000(city_id, condition):
    rented_info = []
    price_split = []  # 将价格拆分为可使用的分段
    # 把0，6000带入，返回成交数量信息{'rented_count': 2902, 'bpep': {0: 250, 250: 500}},4096至99999为最上限
    rented_info.append(get_rented_2000(city_id, condition, 0, 4096))
    rented_info.append(get_rented_2000(city_id, condition, 4096, 99999))

    for rented in rented_info:
        if rented['rented_count'] > 2000:  # 如果数量大于2000，则进行拆分
            # 遍历超过2000的区间
            for cj in rented['bpep']:
                # print('遍历:',cj,rented['bpep'][cj])
                # 将超过2000的结果，放入rented_info
                rented_info.append(get_rented_2000(city_id, condition, cj, rented['bpep'][cj]))
        elif rented['rented_count'] <= 2000 and rented['rented_count'] != 0:
            # print('该价位区间没有超过2000:',rented['bpep'])
            price_split.append(rented['bpep'])
        else:
            pass  # 区间为0的情况
    return price_split




# 获取二手房成交数据(城市id,)
def get_chengjiao(city_id, condition):
    url = "https://app.api.lianjia.com/house/chengjiao/searchv2"
    chengjiao = []
    offset = 0
    total_count = get_chengjiao_count(city_id, condition)  # 该商圈的总记录数
    # 总数小于2000，且总数不为0
    while offset < total_count:
        params = {
            'channel': 'sold',
            'limit_offset': offset,  # 请求数
            'city_id': city_id,
            'limit_count': 100,  # 单次请求数量
            'condition': condition}  # 筛选条件
        data = get_data(url, params)
        # print('         获取成交房源进度：{:.2f}%'.format(offset / total_count * 100))
        for d in data['list']:
            chengjiao.append(d)
        offset += 100
    # print('             总记录数:',total_count,len(chengjiao))
    return chengjiao

# 获取二手房成交总数
def get_chengjiao_count(city_id, condition):
    url = "https://app.api.lianjia.com/house/chengjiao/searchv2"
    offset = 0
    params = {
        'channel': 'sold',
        'limit_offset': offset,  # 请求数
        'city_id': city_id,
        'limit_count': 20,  # 单次请求数量
        'condition': condition}  # 筛选条件
    try:
        data = get_data(url, params)
        total_count = data['total_count']  # 总记录数
    except:
        total_count = 0
        print('数量获取失败，返回0')
    return total_count

# 传入城市ID，参数，价格区间，返回该区间的成交数，以及价格拆分为二
# {'chengjiao_count': 2835, 'bpep': {0: 125, 125: 250}}
def get_chengjiao_2000(cityid, condition, bp, ep):
    condition2 = condition + 'bp{}ep{}'.format(bp, ep)
    chengjiao_count2 = get_chengjiao_count(cityid, condition2)
    if chengjiao_count2 <= 2000 and chengjiao_count2 > 0:
        return {'chengjiao_count': chengjiao_count2, 'bpep': {bp: ep}}
    elif chengjiao_count2 > 2000:
        return {'chengjiao_count': chengjiao_count2, 'bpep': {bp: int((ep+bp) / 2), int((ep+bp) / 2): ep}}
    elif chengjiao_count2 == 0:
        return {'chengjiao_count': chengjiao_count2}

def do_chengjiao_2000(city_id, condition):
    chengjiao_info = []
    price_split = []  # 将价格拆分为可使用的分段
    # 把0，500万带入，返回成交数量信息{'chengjiao_count': 2902, 'bpep': {0: 250, 250: 500}},500至99999为最上限
    chengjiao_info.append(get_chengjiao_2000(city_id, condition, 0, 512))
    chengjiao_info.append(get_chengjiao_2000(city_id, condition, 512, 99999))

    for chengjiao in chengjiao_info:
        if chengjiao['chengjiao_count'] > 2000:  # 如果数量大于2000，则进行拆分
            # 遍历超过2000的区间
            for cj in chengjiao['bpep']:
                # print('遍历:',cj,chengjiao['bpep'][cj])
                # 将超过2000的结果，放入chengjiao_info
                chengjiao_info.append(get_chengjiao_2000(city_id, condition, cj, chengjiao['bpep'][cj]))
        elif chengjiao['chengjiao_count'] <= 2000 and chengjiao['chengjiao_count'] != 0:
            # print('该价位区间没有超过2000:',chengjiao['bpep'])
            price_split.append(chengjiao['bpep'])
        else:
            pass  # 区间为0的情况

    return price_split
