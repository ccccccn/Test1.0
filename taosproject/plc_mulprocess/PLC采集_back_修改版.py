import multiprocessing
import struct
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import threading
import time
from datetime import datetime
from multiprocessing import Process, Pool, cpu_count, Manager, freeze_support

import schedule
from schedula import counter
from snap7.client import Client, Area
from snap7 import util
import random

from Test.allTest_intertable import data_analysis_job
from datadesrip.CreateTableTest import create_db, creat_sql, create_tables, getconnect
from datadesrip.dataanalysis import data_per
from plc_mulprocess.BidirectionQueue import BidirectionQueue
from plc_mulprocess.data_name import DataName, DataName_en

# 定义线程局部变量实现函数嵌套任务
lock = threading.Lock()
import asyncio


def get_plc_data(selected_ip, plc_ip, plc_rack, plc_slot, db_numbers, start_addresses, data_lengths):
    try:
        plc = Client()
        plc.connect(plc_ip, plc_rack, plc_slot)
    except Exception as e:
        print("当前错误为：", e)
    if plc.get_connected():
        print(f'PLC {plc_ip} 连接成功')
    conn = getconnect()
    conn.select_db("threadTestt")
    cursor = conn.cursor()
    try:
        while True:
            count = 1
            for db, start, length in zip(db_numbers, start_addresses, data_lengths):
                Sys_time = datetime.now()
                # lock.locked()
                data_list = []
                cnt = int(length / 5)
                for idx in range(cnt):
                    data = plc.read_area(Area.DB, db, start + (idx * 28), 20)
                    for j in range(5):
                        tag = 1
                        if data[j * 4:j * 4 + 4][0] & 0x80:
                            tag = 0

                        selreal = struct.unpack('>f', bytes(data[j * 4:j * 4 + 4]))[0]
                        round_selreal = round(selreal, 6)
                        data_list.append(selreal)
                if plc_ip in selected_ip:
                    plc_ip_name = plc_ip.split('.')[-1]
                    data_table_name = f'plc{plc_ip_name}_db{db}_MBC{count}'
                    count += 1
                    # lock.acquire()
                    with lock:
                        db_id_sql = f'select count(*) from `{data_table_name}`'
                        cursor.execute(db_id_sql)
                        var_num = cursor.fetchall()[0][0]
                        if var_num is None:
                            var_num = 0
                        db_id_tag = var_num + 1
                        create_tables(data_table_name, conn, data_list, db_id_tag)
                        # lock.release()
                        print(
                            f'PLC {plc_ip} DB{db}\t\t读取的数据为:{data_list}\t\t当前时间为:{Sys_time} \n',
                            end='')
                    pass
            time.sleep(0.01)  # 每次读取后休眠 1 毫秒
    except KeyboardInterrupt:
        print("用户终止程序！")


# else:
#     print(f'PLC {plc_ip} 连接失败')

def batcr_table(plcs, select_ip, select_db):
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

    #     # 建立统计分析子表
    # data_length = value_list[-1][1]
    # for j in range(8):
    #     table_name = 'MBC{}'.format(j + 1)
    #     table_sql = (f"CREATE TABLE IF NOT EXISTS `plc{plc_ip_suffix}_db{db_name}_{table_name}` "
    #                     f"USING data_analysis TAGS ({j + 1},{db_name},'{plc_ip}') ")
    #     conn.execute(table_sql)
    # print("成功批量建立子表！")
    print("成功批量建立数据采集表")


if __name__ == '__main__':
    db_name = "threadTestt"
    conn = create_db(db_name)
    conn.select_db(db_name)
    cursor = conn.cursor()

    # 获取到当前采集数据的总个数以及等长区间
    interval_list = data_per(100)
    interval_list_length = len(interval_list)
    base_config = {
        'rack': 0, 'slot': 1,
        'dbs': [41] * 8,
        'starts': [58, 584, 1110, 1636, 2162, 2688, 3214, 3740],
        'lengths': [15] * 8
    }
    # 辅助列表，包含所有需要的IP地址
    ip_addresses = ['192.99.7.22']
    # 使用列表生成式创建最终的PLC列表
    collect_plcs = [{'ip': ip, **base_config} for ip in ip_addresses]
    # 创建相对的超级表模板
    stable_sql = f"CREATE STABLE IF NOT EXISTS data_analysis (`ts` TIMESTAMP,"
    stable_sql1 = f"CREATE STABLE IF NOT EXISTS data_collect_DATA (`ts` TIMESTAMP,`ID` INT ,"
    # count = 0
    # for item in DataName:
    #     for i in range(interval_list_length):
    #         field_name1 = "inter{}_count{}".format(i + 1, item.name)
    #         field_name2 = "inter{}_per{}".format(i + 1, item.name)
    #         if count != interval_list_length * 20 - 1:
    #             stable_sql += f"`{field_name1}` INT, `{field_name2}` FLOAT,"
    #             count += 1
    #         else:
    #             stable_sql += f"`{field_name1}` INT, `{field_name2}` FLOAT)"
    # stable_sql += f" TAGS (`module` INT,`F_location` INT,`ip` NCHAR(50))"
    # cursor.execute(stable_sql)
    # print(stable_sql)
    # print("成功创建数据统计分析超级表！")

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

    # 方案二：无规律采用直接建表
    select_ip = [plc['ip'] for plc in collect_plcs]
    select_plcs = [plc for plc in collect_plcs if plc['ip'] in select_ip]
    batcr_table(plcs=collect_plcs, select_ip=select_plcs, select_db=None)
    print("所有统计表单于采集表单已建立完成")

    freeze_support()
    pool = Pool(processes=8)
    results = []
    try:
        for plc in collect_plcs:
            arges = (
                select_ip, plc['ip'], plc['rack'], plc['slot'],
                plc['dbs'], plc['starts'], plc['lengths']
            )
            results.append(pool.apply_async(get_plc_data, args=arges))
    except KeyboardInterrupt as e:
        print(f"客户终止了程序：{e}")
    finally:
        pool.close()
        pool.join()
        print("over!")
    for result in results:
        try:
            print(result.get(timeout=5))  # 设置超时检查
        except Exception as e:
            print(f"Error: {e}")
