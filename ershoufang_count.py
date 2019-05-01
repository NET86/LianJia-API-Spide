import threading
from datetime import datetime
from queue import Queue

from public import *
from pymongo import MongoClient


# 思路：先获取商圈信息
# 通过商圈信息，多线程去获取成交信息，根据成交信息的数量，把ID及参数放入变量
# 根据这个变量，多线程去最终获取
# 'bizcircle_id': '1100000602', 商圈ID
# 'bizcircle_quanpin': 'chengnanyijia', 商圈全拼
# 'bizcircle_name': '城南宜家', 商圈名
# 'city_id': 510100, 城市Id
# 'city_name': '成都', 城市名称
# 'district_id': '510104', 行政区ID
# 'district_name': '锦江'，行政区名称
# 'condition'：获取api的参数
# subway_line 地铁线
# city_abbr 城市简称


# 多线程采集，传入原始商圈信息，往队列里写入可直接抓取的商圈信息
class Crawl_thread(threading.Thread):
    '''
    抓取线程类，注意需要继承线程类Thread
    '''

    def __init__(self, thread_id, queue):
        threading.Thread.__init__(self)  # 需要对父类的构造函数进行初始化
        self.thread_id = thread_id
        self.queue = queue  # 任务队列

    def run(self):
        '''
        线程在调用过程中就会调用对应的run方法
        :return:
        '''
        print('启动采集线程：', self.thread_id)
        try:
            self.crawl_spider()
        except:
            print('采集线程错误1')

        print('退出采集线程：', self.thread_id)

    def crawl_spider(self):
        while True:
            if self.queue.empty():  # 如果队列为空，则跳出
                print('队列为空，跳出')
                break
            else:
                bizcircle = self.queue.get()
                total_count = get_chengjiao_count(bizcircle['city_id'], bizcircle['condition'])
                # 根据返回的总数，分割成不同的ID和参数，放入变量
                if total_count <= 2000 and total_count != 0:
                    data_queue.put(bizcircle, unique=True)
                elif total_count > 2000:
                    # 如果大于2000条，调用do_chengjiao_2000分段
                    chengjiao_split = do_chengjiao_2000(bizcircle['city_id'], bizcircle['condition'])
                    for rs in chengjiao_split:
                        # 此处有大坑，bizcircle_tmp应在此处赋值，如在之前赋值会有问题
                        bizcircle_tmp = bizcircle.copy()  # 复制一个对象出来，用COPY，不能用=
                        bp = list(rs.keys())[0]
                        ep = list(rs.values())[0]
                        bizcircle_tmp['condition'] = bizcircle['condition'] + 'bp{}ep{}'.format(bp, ep)
                        data_queue.put(bizcircle_tmp, unique=True)

                print('     采集线程ID：', self.thread_id, "  {}>{}>{}>{}  {}条记录，bizcircle_queue队列余量：{}".format
                (bizcircle['city_name'], bizcircle['district_name'], bizcircle['bizcircle_name'],
                 bizcircle['condition'], total_count, self.queue.qsize()))


# 多线程处理，传入商圈信息，直接进行最后抓取
class Parser_thread(threading.Thread):
    '''
    解析网页的类，就是对采集结果进行解析，也是多线程方式进行解析
    '''

    def __init__(self, thread_id, queue, db):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.queue = queue
        self.db = db

    def run(self):
        print('启动解析线程：', self.thread_id)
        while not flag:
            try:
                item = self.queue.get()  # get参数为false时队列为空，会抛出异常
                if not item:
                    pass
                self.parse_data(item)
                self.queue.task_done()  # 每当发出一次get操作，就会提示是否堵塞
            except Exception as e:
                pass
        print('退出解析线程：', self.thread_id)

    def parse_data(self, item):
        '''
        解析网页内容的函数
        :param item:
        :return:
        '''

        self.chengjiao = get_chengjiao(item['city_id'], item['condition'])
        # print('item', item)

        for r in self.chengjiao:
            r_copy = r.copy()
            r_copy.update(item)
            r_copy.update({
                '更新时间': datetime.now()})
            self.db.update_one({'house_code': r_copy['house_code']}, {'$set': r_copy}, upsert=True)
        print('         解析线程ID：', self.thread_id,
              "写入：{}>{}>{}>{} {}条记录，parse_data队列余量：{}".format(item['city_name'], item['district_name'],
                                                              item['bizcircle_name'],
                                                              item['condition'], len(self.chengjiao), self.queue.qsize()))


flag = False  # 退出标志
data_queue = Queue()  # 存放解析数据的queue


def main():
    cityid = 510100  # 设置城市id
    bizcircle_queue = Queue()  # 存放商圈数据到queue
    cityinfo = get_city_info(cityid)
    print('正在获取 {}【{}】行政区域及商圈信息'.format(cityid, cityinfo['city_name']))
    condition_list = []
    for city in cityinfo['district']:
        # 遍历行政区
        print('{}>{} 商圈数：{}'.format(cityinfo['city_name'], city['district_name'], len(city['bizcircle'])))
        for biz in city['bizcircle']:
            # 遍历商圈
            # 写入城市ID，城市名，区域ID，区域名，商圈信息
            if biz['bizcircle_quanpin'] not in condition_list:
                biz_ad = biz.copy()
                biz_ad['city_id'] = cityinfo['city_id']
                biz_ad['city_name'] = cityinfo['city_name']
                biz_ad['district_id'] = city['district_id']
                biz_ad['district_name'] = city['district_name']
                biz_ad['condition'] = biz['bizcircle_quanpin'] + '/'
                condition_list.append(biz['bizcircle_quanpin'])
                bizcircle_queue.put(biz_ad, unique=True)
                print('    商圈名称：{}'.format(biz['bizcircle_name']))
    print('\n' + '    商圈抓取完毕，数量为 {}'.format(bizcircle_queue.qsize()) + '\n')

    conn = MongoClient('127.0.0.1', 27017)
    db = conn.链家网  # 连接mydb数据库，没有则自动创建
    db2 = db[cityinfo['city_name'] + '二手房成交信息(多线程/API)']

    # 初始化采集线程
    crawl_threads = []
    for thread_id in range(10):
        # 传入线程ID，
        thread = Crawl_thread(thread_id, bizcircle_queue)  # 启动爬虫线程
        thread.start()  # 启动线程
        crawl_threads.append(thread)

    # 初始化解析线程
    parse_thread = []
    for thread_id in range(20):  #
        thread = Parser_thread(thread_id, data_queue, db2)
        thread.start()  # 启动线程
        parse_thread.append(thread)

    # 等待队列情况，先进行网页的抓取
    while not bizcircle_queue.empty():  # 判断是否为空
        pass  # 不为空，则继续阻塞
    print('标记0')

    # 等待所有线程结束
    for t in crawl_threads:
        t.join()

    print('标记1')

    # 等待队列情况，对采集的页面队列中的页面进行解析，等待所有页面解析完成
    while not data_queue.empty():
        pass
    print('标记2')
    # 通知线程退出
    global flag
    flag = True
    for t in parse_thread:
        t.join()  # 等待所有线程执行到此处再继续往下执行

    print('退出主线程')


if __name__ == '__main__':
    d1 = datetime.now()
    print('开始抓取二手房成交信息')
    main()
    d2 = datetime.now()
    print('总用时：', d2 - d1)
