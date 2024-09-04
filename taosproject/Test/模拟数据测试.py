# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: 模拟数据测试.py
 @DateTime: 2024/9/4 15:21
 @SoftWare: PyCharm
"""
import logging
import multiprocessing
import struct
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import threading
import time
from datetime import datetime
from multiprocessing import Pool, freeze_support
import asyncio
import random

from datadesrip.CreateTableTest import create_db, create_tables, getconnect
from plc_mulprocess.BidirectionQueue import BidirectionQueue
from plc_mulprocess.data_name import DataName, DataName_en
from 工具类.记录日志 import StreamToLogger

# 定义线程局部变量实现函数嵌套任务
lock = threading.Lock()
# 日志设置
sys.stdout = StreamToLogger(logging.getLogger('STDOUT'), logging.INFO)
sys.stderr = StreamToLogger(logging.getLogger('STDERR'), logging.ERROR)

# 异步模拟获取 PLC 数据函数 (生成随机数据)
async def get_plc_data_async(plc_ip, plc_rack, plc_slot, db_numbers, start_addresses, data_lengths, selected_ip):
    print(f"开始模拟采集PLC {plc_ip} 的数据")

    conn = getconnect()
    conn.select_db("threadTestt3")
    cursor = conn.cursor()

    while True:
        count = 1
        try:
            for db, start, length in zip(db_numbers, start_addresses, data_lengths):
                sys_time = datetime.now()
                data_list = []

                # 模拟生成随机数据
                # for _ in range(length // 4):  # 每个 float 占用 4 个字节
                #     selreal = round(random.uniform(0, 100), 6)  # 随机生成 0 到 100 之间的浮点数
                #     data_list.append(selreal)
                data_list = [round(random.uniform(-1, 1), 3) for i in range(15)]

                # 存储数据到数据库（模拟）
                if plc_ip in selected_ip:
                    plc_ip_name = plc_ip.split('.')[-1]
                    data_table_name = f'plc{plc_ip_name}_db{db}_MBC{count}'
                    count += 1

                    with lock:
                        cursor.execute(f"SELECT COUNT(*) FROM `{data_table_name}`")
                        var_num = cursor.fetchall()[0][0]
                        if var_num is None:
                            var_num = 0
                        db_id_tag = var_num + 1
                        create_tables(data_table_name, conn, data_list, db_id_tag)
                        print(f"模拟 PLC {plc_ip} DB{db} 数据: {data_list} 时间: {sys_time}\n", end='')
            await asyncio.sleep(0.01)  # 每次采集后等待 1 秒 (模拟数据采集延迟)
        except Exception as e:
            print(f"采集错误: {e}")
            break


# 进程中的协程任务执行器
def process_ip(plc_info, selected_ip):
    ip = plc_info['ip']
    rack = plc_info['rack']
    slot = plc_info['slot']
    db_numbers = plc_info['dbs']
    start_addresses = plc_info['starts']
    data_lengths = plc_info['lengths']

    # 使用 asyncio.run 来启动协程
    asyncio.run(get_plc_data_async(ip, rack, slot, db_numbers, start_addresses, data_lengths, selected_ip))


# 多进程任务分发
def run_data_collection(plcs, selected_ip):
    with Pool(processes=8) as pool:
        pool.starmap(process_ip, [(plc, selected_ip) for plc in plcs])


# 创建数据库和数据表
def batcr_table(plcs):
    conn = getconnect()
    conn.select_db("threadTestt3")
    cursor = conn.cursor()

    dq = BidirectionQueue()
    dq_en = BidirectionQueue()
    for varname, en_varname in zip(DataName, DataName_en):
        dq.append_left(varname.name)
        dq_en.append_left(en_varname.name)
    length = plcs[0].get('lengths')

    for plc in plcs:
        value_list = []
        for key, value in plc.items():
            value_list.append(value)
        plc_ip = value_list[0]
        plc_ip_suffix = value_list[0].split('.')[-1].lower()
        db_name_iter = iter(value_list[3])
        for i in range(len(value_list[3])):
            db_name = next(db_name_iter)
            creat_sql = (f"CREATE TABLE IF NOT EXISTS `plc{plc_ip_suffix}_db{db_name}_MBC{i + 1}` "
                         f"USING data_collect_DATA tags ('{plc_ip}',{db_name},{i + 1})")
            cursor.execute(creat_sql)
    print("{}采集子表已建立完成".format(plc_ip))


# 主函数
if __name__ == '__main__':
    start_time = datetime.now()


    # 创建数据库并选择
    db_name = "threadTestt3"
    conn = create_db(db_name)
    conn.select_db(db_name)
    cursor = conn.cursor()

    # 配置采集的 PLC (使用随机生成的数据)
    base_config = {
        'rack': 0, 'slot': 1,
        'dbs': [41] * 8,
        'starts': [58, 584, 1110, 1636, 2162, 2688, 3214, 3740],
        'lengths': [15] * 8  # 每次读取 15 个字节的数据 (每个 float 4 字节)
    }
    ip_addresses = [f'192.99.7.22']  # 示例 IP 列表
    collect_plcs = [{'ip': ip, **base_config} for ip in ip_addresses]

    stable_sql1 = f"CREATE STABLE IF NOT EXISTS data_collect_DATA (`ts` TIMESTAMP,`ID` INT ,"
    length = collect_plcs[0].get('lengths')
    # print(length)

    # 方案一：有规律采用超级表，创建数据采集超级表
    count1 = 0
    for varname, varname2 in zip(DataName, DataName_en):
        field_name1 = varname.name
        field_name2 = varname2.name
        if count1 < 14:
            stable_sql1 += f"`{field_name1}` float , "
            count1 += 1
        else:
            stable_sql1 += f"`{field_name1}` float )"
    stable_sql1 += f"TAGS (`ip` NCHAR(20),`db_block` INT,`MBC_id` INT)"
    cursor.execute(stable_sql1)
    print("成功创建数据采集超级表！")

    # 建立数据表
    batcr_table(collect_plcs)

    # 运行数据采集任务 (模拟随机数据)
    freeze_support()
    try:
        run_data_collection(collect_plcs, selected_ip=ip_addresses)
    except KeyboardInterrupt:
        print("采集任务已终止。")

    end_time = datetime.now()
    print("总共用时：", end_time - start_time)
