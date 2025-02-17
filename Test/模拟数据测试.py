# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: 模拟数据测试.py
 @DateTime: 2024/9/4 15:21
 @SoftWare: PyCharm
"""
from collections import deque
import threading
from datetime import datetime
from multiprocessing import Pool, freeze_support, Process
import asyncio
import random

import taos

from datadesrip.CreateTableTest import create_db, getconnect
from plc_mulprocess.BidirectionQueue import BidirectionQueue
from plc_mulprocess.data_name import DataName, DataName_en
from 前期代码.定时迁移数据 import check_run

# 定义线程局部变量实现函数嵌套任务
lock = threading.Lock()
Flag = False
data_block = deque()
# # 日志设置
# sys.stdout = StreamToLogger(logging.getLogger('STDOUT'), logging.INFO)
# sys.stderr = StreamToLogger(logging.getLogger('STDERR'), logging.ERROR)


# 异步模拟获取 PLC 数据函数 (生成随机数据)
async def get_plc_data_async(plc_ip, plc_rack, plc_slot, db_numbers, start_addresses, data_lengths, selected_ip):
    print(f"开始模拟采集PLC {plc_ip} 的数据")

    data_dir = {}

    while True:
        count = 1
        try:
            for db, start, length in zip(db_numbers, start_addresses, data_lengths):
                sys_time = datetime.now()
                print(f"db{db}_MBC{count}正在执行------{sys_time.second}")
                data_list = []
                data_list.append(sys_time.strftime("%Y-%m-%d %H:%M:%S.%MS"))
                if plc_ip in selected_ip:
                    plc_ip_name = plc_ip.split('.')[-1]
                    data_table_name = f'plc{plc_ip_name}_db{db}_MBC{count}'
                    count += 1
                    # with lock:
                    #     cursor.execute(f"SELECT COUNT(*) FROM `{data_table_name}`")
                    #     var_num = cursor.fetchall()[0][0]
                    #     if var_num is None:
                    #         var_num = 0
                    #     db_id_tag = var_num + 1
                    #     data_list.append(db_id_tag)
                    db_id_tag = random.randint(1,10000)
                    data_list.append(db_id_tag)
                    data_list.append((random.randint(4500,10000)))
                    for i in range(16):
                        data_list.append(round(random.uniform(-1, 1), 3))
                    if f"MBC{count}" not in data_dir:
                        data_dir[f'MBC{count}'] = [data_list]
                    else:
                        data_dir[f'MBC{count}'].append(data_list)
                    # 存储数据到数据库（模拟）
                    column_place = ','.join(['%s'] * 19)
                    # cursor.execute(f"desc `{data_table_name}`")
                    # description = cursor.fetchall()
                    # column_name = tuple([desc[0] for desc in description[:-3]])
                    # print(column_name)
                    Insert_sql = (f"INSERT INTO `{data_table_name}` ('ts', 'ID', '飞轮转速', '飞轮舱温度', '积分电流_x1', "
                                  f"'积分电流_y1', '积分电流_x2', '积分电流_y2', '积分电流_z', '同步震动半径_x1', '同步震动半径_y1', "
                                  f"'同步震动半径_x2', '同步震动半径_y2', '同步震动半径_z', '同步电流_x1', '同步电流_y1', '同步电流_x2', "
                                  f"'同步电流_y2', '同步电流_z') VALUES")
                    # print(Insert_sql)
                    if datetime.now().second == 15:
                        conn = getconnect()
                        conn.select_db("plcback")
                        cursor = conn.cursor()
                        newdata = tuple(data_dir[f'MBC{count}'])
                        # print("当前插入数据集为：", data_dir[f'MBC{count}'])
                        cursor.execute_many(Insert_sql, newdata)
                        del data_dir[f'MBC{count}']
                        print("成功向数据库插入数据！并删除字典")
                # if plc_ip in selected_ip:
                #     plc_ip_name = plc_ip.split('.')[-1]
                #     data_table_name = f'plc{plc_ip_name}_db{db}_MBC{count}'
                #     count += 1
                #     with lock:
                #         cursor.execute(f"SELECT COUNT(*) FROM `{data_table_name}`")
                #         var_num = cursor.fetchall()[0][0]
                #         if var_num is None:
                #             var_num = 0
                #         db_id_tag = var_num + 1
                #         create_tables(data_table_name, conn, data_list, db_id_tag)
                #         print(f"当前进程名:{multiprocessing.current_process()}\n", end='')
                #         print(f"模0拟 PLC {plc_ip} DB{db} 数据: {data_list} 时间: {sys_time}\n", end='')
            await asyncio.sleep(0.01)  # 每次采集后等待 1 秒 (模拟数据采集延迟)
        except Exception as e:
            print(f"采集错误: {e}")
            break


# 进程中的协程任务执行器4**#
def process_ip(plc_info, selected_ip):
    try:
        ip = plc_info['ip']
        rack = plc_info['rack']
        slot = plc_info['slot']
        db_numbers = plc_info['dbs']
        start_addresses = plc_info['starts']
        data_lengths = plc_info['lengths']

        # 使用 asyncio.run 来启动协程
        asyncio.run(get_plc_data_async(ip, rack, slot, db_numbers, start_addresses, data_lengths, selected_ip))
    except KeyboardInterrupt:
        pass


# 多进程任务分发
def run_data_collection(plcs, selected_ip):
    try:
        with Pool(processes=8) as pool:
            pool.starmap(process_ip, [(plc, selected_ip) for plc in plcs])
    except:
        pass


def run_transfer():
    with Pool(processes=8) as pool:
        pool.map(check_run, [None])


# 创建数据库和数据表
def batcr_table(plcs):
    conn = getconnect()
    conn.select_db("plcback")
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
                         f"USING data_collect_DATA tags ('{plc_ip}',{i},{i + 1})")
            cursor.execute(creat_sql)
    print("{}采集子表已建立完成".format(plc_ip))


# 主函数
if __name__ == '__main__':
    start_time = datetime.now()
    # 创建数据库并选择

    # TODO:为从机模拟数据
    db_name = "plcback"
    conn = taos.connect(
        host="192.168.44.23",
        user="root",
        password="taosdata",
        port=6030
    )
    print("连接从机成功")
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
    stable_sql1 += ",".join(f"`{field_name1.name}` float" for field_name1 in DataName)
    stable_sql1 += f") TAGS (`ip` NCHAR(20),`db_block` INT,`MBC_id` INT)"
    print(stable_sql1)
    cursor.execute(stable_sql1)
    print("成功创建数据采集超级表！")

    # 建立数据表
    batcr_table(collect_plcs)

    # 运行数据采集任务 (模拟随机数据)
    freeze_support()
    try:
        process1 = Process(target=run_data_collection, args=(collect_plcs, ip_addresses))
        # process2 = Process(target=check_run)

        process1.start()
        # process2.start()

        process1.join()
        # process2.join()
    except KeyboardInterrupt:
        print("采集任务已终止。")

    cursor.close()
    conn.close()

    end_time = datetime.now()
    print("总共用时：", end_time - start_time)
