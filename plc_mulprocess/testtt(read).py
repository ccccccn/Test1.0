import json
import os
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import numpy as np
from django.http import JsonResponse
from snap7.client import Client
from BidirectionQueue import BidirectionQueue
from sortedcontainers import SortedList
from 工具类.数据处理 import get_data, file_data
from 工具类.数据处理 import JsonCache

# 设置线程池和锁
lock = threading.Lock()
lockk = threading.Lock()  # 将 asyncio.Lock 改为 threading.Lock
Data_queue = BidirectionQueue()
# 传输至redis上的hash
pie_info = {
    "frequency_dis": {},
    "duration_dis": {},
    "soc_dis": {}
}
rack_info = {}
soc_bins = np.linspace(-100,100,5)
frequency_bins = np.linspace(-100,100,5)
duration_bins = np.linspace(-100,100,5)
soc_value = np.zeros(20)
frequency_value = np.zeros(20)
duration_value =np.zeros(20)


def calculate_dis(value_name, Intervel):
    hist = np.histogram(value_name, bins=Intervel)
    hist_json = json.dumps((hist / 20).tolist())
    return JsonResponse(hist_json, safe=False)


# 数据解析示例
def parse_data(plc_ip,data, db):
    global soc_value, frequency_value, duration_value
    flc_num = str(plc_ip.split('.')[-1])
    print(f"当前解析ip为{plc_ip},飞轮舱号为{flc_num}\n", end='')
    # 解析并处理数据
    data_dir = {}
    cache_data = cache.cache.get(f"DBDB{db}")
    for key in cache_data:
        cache_adata = cache_data[key]
        value = round(get_data(cache_adata, data), 3)
        data_dir[key] = value
        if key[4:] =="_EMS_DC_voltage":
            soc_value[int(flc_num) - 1] = value
            soc_hist = np.histogram(soc_value, bins=soc_bins)
            print(f"当前{flc_num}soc_value：{soc_value}\n", end='')
        if key[4:] == "_PCS_电网线电压VAB":
            frequency_value[int(flc_num) - 1] = value
            frequency_hist = np.histogram(frequency_value, bins=frequency_bins)
            print(f"当前{flc_num}fre:{frequency_value}\n", end='')
        if key[4:] =="_PCS_电网频率":
            duration_value[int(flc_num) - 1] = value
            duration_hist = np.histogram(duration_value, bins=duration_bins)
            print(f"当前{flc_num}dur:{duration_value}\n", end='')
        # print(f"{key}:{value}")
    # print(f"当前DB块数据为：{data_dir}")

    data_dir_json = json.dumps(data_dir)

    show_center_data = {}

    print(f"当前{flc_num}DB{db}块解析结束时间为：{datetime.now()}\n", end='')
    # return soc_hist, data_dir_json


# PLC数据采集任务
def read_plc_data(plc_ip, dbs, start, lengths):
    try:
        ## 真实数据 一次性获取所有db下的值，顺序执行
        # data = plc.db_read(db, 0, length)
        ## 模拟测试数据
        for db, length in zip(dbs, lengths):
            data = os.urandom(length)
            random_data = bytearray(data)
            parse_data(plc_ip,random_data, db)
        print(f"DB{db}块获取到数据:{datetime.now()}\n", end='')
    except Exception as e:
        print(f"读取PLC数据错误：{e}")
        return None


# 采集每个PLC数据
def get_plc_data(plc_ip, plc_rack, plc_slot, dbs, starts, lengths, executor):
    plc = Client()
    plc1 = Client()
    try:
        plc.connect(plc_ip, plc_rack, plc_slot)
        plc1.connect(plc_ip, plc_rack, plc_slot)
    except Exception as e:
        print(f"连接PLC失败: {e}")
        return

    if plc.get_connected():
        print(f"PLC {plc_ip} 连接成功")

    # 将 DB 块分成两部分
    dbs_plc = dbs[:5]  # 前 5 个 DB 块由 plc 处理
    starts_plc = starts[:5]
    lengths_plc = lengths[:5]

    dbs_plc1 = dbs[5:]  # 后 6 个 DB 块由 plc1 处理
    starts_plc1 = starts[5:]
    lengths_plc1 = lengths[5:]

    # 使用线程池执行读取任务
    futures = []
    Start_time = datetime.now()
    # 使用 plc 处理前 5 个 DB 块
    for db, start, length in zip(dbs_plc, starts_plc, lengths_plc):
        future = executor.submit(read_plc_data, plc, db, start, length)
        futures.append(future)

    # 使用 plc1 处理后 6 个 DB 块
    for db, start, length in zip(dbs_plc1, starts_plc1, lengths_plc1):
        future = executor.submit(read_plc_data, plc1, db, start, length)
        futures.append(future)

    # 等待所有任务完成
    for future in as_completed(futures):
        future.result()  # 等待每个任务的结果，触发异常时会被捕获

    end_time = datetime.now()
    print(f"PLC{plc_ip}本次采集所用：{end_time - Start_time} \n", end='')


# 外部线程负责管理每个PLC设备
def plc_thread(plc_ip, rack, slot, dbs, starts, lengths, executor):
    # 每个外部线程负责不断地进行数据采集
    while True:
        Start_time = datetime.now()
        print(f"PLC {plc_ip} 数据采集开始于：{Start_time}\n",end='')
        read_plc_data(plc_ip, dbs, starts, lengths)
        # get_plc_data(plc_ip, rack, slot, dbs, starts, lengths, executor)
        print(f"PLC {plc_ip} 数据采集完成\n",end='')
        end_time = datetime.now()
        print(f"PLC {plc_ip} 本次循环所用时间：{end_time - Start_time}")


# 多线程执行数据采集
def run_data_collection(plcs):
    # 创建外部线程池
    with ThreadPoolExecutor(max_workers=21) as executor:  # 外部线程池，最大同时处理5个PLC
        threads = []

        for plc in plcs:
            # 每个 PLC 使用内部线程池来管理其 DB 数据采集任务
            plc_thread_instance = threading.Thread(target=plc_thread, args=(
                plc['ip'], plc['rack'], plc['slot'], plc['dbs'], plc['starts'], plc['lengths'], executor))
            threads.append(plc_thread_instance)
            plc_thread_instance.start()

        # 等待所有外部线程完成
        for thread in threads:
            thread.join()


if __name__ == '__main__':
    folder_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "工具类", "FEMSjson文件")
    cache = JsonCache()
    cache.load_from_dir(folder_path)

    db, start, lengths = [], [], []
    for file_path in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_path)
        data = file_data(file_path)
        db.append(data['DB'])
        start.append(data['starts'])
        lengths.append(data['lengths'])

    base_config = {
        'rack': 0, 'slot': 1,
        'dbs': db, 'starts': start, 'lengths': lengths
    }

    ip_addresses = ['192.168.110.{}'.format(i) for i in range(1,21)]  # 示例PLC IP地址
    collect_plcs = [{'ip': ip, **base_config} for ip in ip_addresses]

    try:
        run_data_collection(collect_plcs)
    except KeyboardInterrupt:
        print("采集任务已终止")
