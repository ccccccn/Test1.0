import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime, time
from multiprocessing import Pool
import asyncio
import random
import tracemalloc
from time import sleep
from turtle import delay

import pandas as pd
from snap7.client import Client

from datadesrip.CreateTableTest import create_db, connect_mysql, create_tables, get_connection
from plc_mulprocess.BidirectionQueue import BidirectionQueue
from 前期代码.定时迁移数据 import check_run
from 工具类.数据处理 import file_data, get_data
import setting

# 定义线程局部变量实现函数嵌套任务
lock = threading.Lock()
lockk = asyncio.Lock()

# 新建全局队列，存储暂时数据
Data_queue = BidirectionQueue()

# # 将输入输出重定向到日志记录中
# sys.stdout = StreamToLogger(logging.getLogger('STDOUT'), logging.INFO)
# sys.stderr = StreamToLogger(logging.getLogger('STDERR'), logging.ERROR)
from 工具类.数据处理 import JsonCache

DB_NAME = 'threadtest'
DATA_DIR_IDX = {}  # 确保获取的数据是点对点对应，并生成一个cache


# 中心函数
def get_plc_data(selected_ip, var_length, max_retries=None, **kwargs):
    con = get_connection("localhost", 'root', 'taosdata')
    con.select_db(DB_NAME)
    cur = conn.cursor()
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
        # plc.connect(kwargs['ip'], kwargs['rack'], kwargs['slot'])
    except Exception as e:
        print("当前错误为：", e)
    if plc.get_connected():
        print(f'PLC {plc_ip} 连接成功')
    while True:
        try:
            if not plc.get_connected():
                retries = 0
                while retries < max_retries:
                    if plc.get_connected():
                        print("连接成功")
                        return True
                    else:
                        print(f"连接失败，尝试重连...（{retries + 1}/{max_retries}）")
                        retries += 1
                        sleep(1) # 等待一段时间后重试
                print("重连失败，已达最大重试次数")
                return False
            if datetime.now().hour == 0 and datetime.now().minute == 0 and datetime.now().second == 0:
                plc.disconnect()
                sleep(10)
                continue
            Start_time = datetime.now()
            print(f"INFO:{plc_ip} collecting...\t当前时间为：{Start_time}\n", end='')
            suffix_ip = plc_ip.split('.')[-1]
            data_table_name = 'flc_' + f"{suffix_ip}"
            data_list = [0] * var_length
            for db, start, length in zip(dbs, starts, lengths):
                # lock.locked()
                data = plc.db_read(db, 0, length)
                # data = bytearray(random.randint(0, 255) for _ in range(length))
                cache_data = cache.cache.get(f"DBDB{db}")
                for key in cache_data:
                    name = key.replace("飞轮舱1", f"飞轮舱{str(plc_ip.split('.')[-1])}")
                    cache_adata = cache_data[key]
                    idx = DATA_DIR_IDX[name]  ## 获取值列表的索引，能够获取到完成的序列值
                    value = get_data(cache_adata, data)
                    data_list[idx] = value
                    # print(f"{name}:{value}\n", end='')
            create_tables(data_table_name, con, data_list, suffix_ip)
            end_time = datetime.now()
            # print(f"我是{plc_ip},我的资源是{a}\n",end='')
            # print("本次采集所用：{} \n".format(end_time - Start_time),end='')
            # await asyncio.sleep(1)  # 每次读取后休眠 1 毫秒
            sleep(0.01)  # 每次读取后休眠 1 毫秒
        except Exception as e:
            print(f"采集错误: {e}")
            pass


# 多线程分布函数
def run_data_collection(plcs, selected_ip, var_length):
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = {executor.submit(get_plc_data, selected_ip, var_length, **plc): plc for plc in plcs}
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


def type_transport(typee):
    type_mapper = {
        "SHORT": 'int',
        "USHORT": 'int',
        "FLOAT": 'float',
        "LONG": 'bigint',
        "BIT": 'bool'
    }
    return type_mapper[typee]


def create_stable(plc_ips):
    global DATA_DIR_IDX
    var_length = 0
    # 确保文件路径正确
    excel_path = os.path.join(os.path.dirname(__file__), '500点位.xlsx')
    # 检查文件是否是zip文件
    if not zipfile.is_zipfile(excel_path):
        raise ValueError("文件不是一个有效的zip文件")
    data_dir = {}

    create_stable_sql = 'CREATE STABLE IF NOT EXISTS data_collect_super_table (`ts` timestamp'
    # 读取Excel文件
    try:
        df = pd.read_excel(excel_path, engine='openpyxl', sheet_name='Sheet1')
        var_lengths = len(df)
        print(var_length)
        for index, row in df.iterrows():
            typee = type_transport(row['ItemDataType'])  # 确保这个函数是定义的
            data_dir_key = (row['TagName']).split('_')[1:]
            data_dir_key = '_'.join(data_dir_key)
            data_dir[data_dir_key] = typee
            if len(DATA_DIR_IDX) != var_lengths:
                DATA_DIR_IDX[data_dir_key] = index
        print("数据字典缓存已生成!")
    except Exception as e:
        print(f"读取Excel文件时发生错误: {e}")
        exit()
    # 构建超级表的SQL语句
    for k, v in data_dir.items():
        create_stable_sql += f", `{k}` {v}"
    create_stable_sql += ') tags (`ip` nchar(20))'
    # 执行SQL语句
    try:
        cursor.execute(create_stable_sql)
        print("超级表成功建立！！")
    except Exception as e:
        print(f"执行超级表创建SQL时发生错误: {e}")
    # 建立子表
    try:
        for plc_ip in plc_ips:  # 确保plc_ips是定义的
            idx = 1
            table_name = "FLC_" + f"{plc_ip.split('.')[-1]}"
            child_create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} using data_collect_super_table tags ({plc_ip.split('.')[-1]})"
            cursor.execute(child_create_sql)
            print(f"成功建立子表{table_name}")
    except Exception as e:
        print(f"执行子表创建SQL时发生错误: {e}")

    return var_lengths


if __name__ == '__main__':
    conn = create_db(DB_NAME)
    conn.select_db(DB_NAME)
    cursor = conn.cursor()

    # tracemalloc.start()
    folder_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "工具类", "500FEMS_json文件")
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
    # # 辅助列表，包含所有需要的IP地址
    ip_addresses = ['192.168.110.{}'.format(i) for i in range(1,21)]
    var_lengths = create_stable(ip_addresses)
    # # 使用列表生成式创建最终的PLC列表
    collect_plcs = [{'ip': ip, **base_config} for ip in ip_addresses]

    # 方案二：无规律采用直接建表
    select_ip = [plc['ip'] for plc in collect_plcs]
    # freeze_support()
    try:
        run_data_collection(collect_plcs, select_ip, var_lengths)
        current, peak = tracemalloc.get_traced_memory()
        print(f"Current memory usage: {current / 1024 ** 2}MB")
        print(f"Peak memory usage: {peak / 1024 ** 2}MB")
    except KeyboardInterrupt:
        print("采集任务已终止")
    finally:
        tracemalloc.stop()
