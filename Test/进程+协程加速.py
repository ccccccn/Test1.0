# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: 进程+协程加速.py
 @DateTime: 2024/9/4 14:34
 @SoftWare: PyCharm
"""
import asyncio
import datetime
import logging
import sys
from datetime import time
from multiprocessing import Pool
import snap7
from snap7.util import get_int
from typing import List
import random
import time

from 工具类.记录日志 import StreamToLogger

sys.stdout = StreamToLogger(logging.getLogger('STDOUT'), logging.INFO)
sys.stderr = StreamToLogger(logging.getLogger('STDERR'), logging.ERROR)

# 同步数据采集函数
def fetch_snap7_data(ip: str, db_number: int, start_address: int, size: int):
    try:
        # client = snap7.client.Client()
        # client.connect(ip, 0, 1)  # 连接到 PLC
        # data = client.db_read(db_number, start_address, size)
        # 假设读取的数据为整数
        data = [random.randint(0, 100) for i in range(10)]
        # value = get_int(data, 0)  # 读取第一个整数
        print(f"从 {ip} DB{db_number} 地址 {start_address} 读取数据：{data}\n", end='')
        return data
    except Exception as e:
        print(f"Snap7 异常：{e}")
        return None
    # finally:
    #     # client.disconnect()


# 异步任务执行器
async def fetch_all_data(ip: str, tasks: List[tuple]):
    loop = asyncio.get_event_loop()
    tasks = [loop.run_in_executor(None, fetch_snap7_data, ip, db_number, start_address, size) for
             db_number, start_address, size in tasks]
    results = await asyncio.gather(*tasks)
    return results


# 进程中的数据采集函数
def process_ip(ip: str, tasks: List[tuple]):
    asyncio.run(fetch_all_data(ip, tasks))


def run_data_collection(ips: List[str], tasks: List[tuple]):
    # 创建进程池
    with Pool(processes=16) as pool:
        # 每个进程处理一部分 IP
        chunk_size = (len(ips) + 15) // 16  # 保证每个进程分配到的 IP 数量大致相等
        ip_chunks = [ips[i:i + chunk_size] for i in range(0, len(ips), chunk_size)]

        # 启动进程池，分配 IP 和任务
        pool.starmap(process_ip, [(ip, tasks) for ip_chunk in ip_chunks for ip in ip_chunk])

def main():

    ips = [f"192.168.1.{i}" for i in range(1, 129)]  # 示例 IP 地址列表
    tasks = [
        (1, 0, 10),  # DB 号 1，从地址 0 读取 10 字节数据
        (2, 10, 10),  # DB 号 2，从地址 10 读取 10 字节数据
        # 可以添加更多的 (DB_number, start_address, size) 任务
    ]

    try:
        while True:
            run_data_collection(ips, tasks)
            print("一次数据采集完成，等待下一轮...")
            time.sleep(5)  # 等待一段时间后继续下一轮采集，可根据需要调整时间间隔
    except KeyboardInterrupt:
        print("数据采集被用户终止。")


if __name__ == "__main__":
    start_time = datetime.datetime.now()
    main()
    end_time = datetime.datetime.now()
    print("总共用时：", end_time - start_time)
