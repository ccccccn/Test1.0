import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime
from multiprocessing import Pool
import asyncio
import random
import tracemalloc
from time import sleep

from snap7.client import Client

from plc_mulprocess.BidirectionQueue import BidirectionQueue
from 前期代码.定时迁移数据 import check_run
from 工具类.数据处理 import file_data, get_data

# 定义线程局部变量实现函数嵌套任务
lock = threading.Lock()
lockk = asyncio.Lock()

# 新建全局队列，存储暂时数据
Data_queue = BidirectionQueue()

# # 将输入输出重定向到日志记录中
# sys.stdout = StreamToLogger(logging.getLogger('STDOUT'), logging.INFO)
# sys.stderr = StreamToLogger(logging.getLogger('STDERR'), logging.ERROR)
from 工具类.数据处理 import JsonCache


# 中心函数
def get_plc_data(selected_ip, **kwargs):
    global Data_queue
    plc_ip = kwargs['ip']
    plc_rack = kwargs['rack']
    plc_slot = kwargs['slot']
    dbs = kwargs['dbs']
    starts = kwargs['starts']
    lengths = kwargs['lengths']
    try:
        plc = Client()
        plc.connect(plc_ip, plc_rack, plc_slot)
        plc.connect(kwargs['ip'], kwargs['rack'], kwargs['slot'])
    except Exception as e:
        print("当前错误为：", e)
    if plc.get_connected():
        print(f'PLC {plc_ip} 连接成功')
    while True:
        try:
            Start_time = datetime.now()
            a = 1
            print(f"INFO:{plc_ip} collecting...\t当前时间为：{Start_time}\n", end='')
            for db, start, length in zip(dbs, starts, lengths):
                # lock.locked()
                # data = plc.db_read(db, start, length)
                a += 1
                data = bytearray(random.randint(0, 255) for _ in range(length))
                cache_data = cache.cache.get(f"DB{db}block")
                for key in cache_data:
                    name = key.replace("飞轮舱1", f"飞轮舱{str(plc_ip.split('.')[-1])}")
                    cache_adata = cache_data[key]
                    value = get_data(cache_adata, data)
                    # print(f"{name}:{value}\n", end='')
            end_time = datetime.now()
            # print(f"我是{plc_ip},我的资源是{a}\n",end='')
            # print("本次采集所用：{} \n".format(end_time - Start_time),end='')
            # await asyncio.sleep(1)  # 每次读取后休眠 1 毫秒
            sleep(1)  # 每次读取后休眠 1 毫秒
        except Exception as e:
            print(f"采集错误: {e}")
            pass


# 多线程分布函数
def run_data_collection(plcs, selected_ip):
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = {executor.submit(get_plc_data, selected_ip, **plc): plc for plc in plcs}
        Start_time = datetime.now().microsecond
        if as_completed(futures):
            end_time = datetime.now().microsecond
            print("本次采集所用：{} \n".format(end_time - Start_time), end='')
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                Type = type(e)
                print(f"线程执行错误：{e},{Type}")


def run_transfer():
    with Pool(processes=8) as pool:
        pool.map(check_run, [None])

def main():
    tracemalloc.start()
    folder_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "工具类", "json文件")
    # 生成程序可阅读较小的缓存
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
    # 辅助列表，包含所有需要的IP地址
    ip_addresses = ['192.99.7.{}'.format(i + 1) for i in range(100)]
    # 使用列表生成式创建最终的PLC列表
    collect_plcs = [{'ip': ip, **base_config} for ip in ip_addresses]

    # 方案二：无规律采用直接建表
    select_ip = [plc['ip'] for plc in collect_plcs]
    # freeze_support()
    try:
        run_data_collection(collect_plcs, select_ip)
        current, peak = tracemalloc.get_traced_memory()
        print(f"Current memory usage: {current / 1024 ** 2}MB")
        print(f"Peak memory usage: {peak / 1024 ** 2}MB")
    except KeyboardInterrupt:
        print("采集任务已终止")
    finally:
        tracemalloc.stop()
# else:
#     print(f'PLC {plc_ip} 连接失败')


if __name__ == '__main__':
    tracemalloc.start()
    folder_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "工具类", "json文件")
    # 生成程序可阅读较小的缓存
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
    # 辅助列表，包含所有需要的IP地址
    ip_addresses = ['192.99.7.{}'.format(i + 1) for i in range(100)]
    # 使用列表生成式创建最终的PLC列表
    collect_plcs = [{'ip': ip, **base_config} for ip in ip_addresses]

    # 方案二：无规律采用直接建表
    select_ip = [plc['ip'] for plc in collect_plcs]
    # freeze_support()
    try:
        run_data_collection(collect_plcs, select_ip)
        current, peak = tracemalloc.get_traced_memory()
        print(f"Current memory usage: {current / 1024 ** 2}MB")
        print(f"Peak memory usage: {peak / 1024 ** 2}MB")
    except KeyboardInterrupt:
        print("采集任务已终止")
    finally:
        tracemalloc.stop()

