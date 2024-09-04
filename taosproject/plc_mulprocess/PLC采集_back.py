import threading
import time
from datetime import datetime
from multiprocessing import Process

import schedule
from snap7.client import Client, Area
import random

from Test.allTest_intertable import data_analysis_job
from datadesrip.CreateTableTest import create_db, creat_sql, create_tables
from datadesrip.dataanalysis import data_per

# 定义线程局部变量实现函数嵌套任务
local_data = threading.local()


def get_plc_data(plc_ip, plc_rack, plc_slot, db_numbers, start_addresses, data_lengths, select_ip):
    # plc = Client()
    # plc.connect(plc_ip, plc_rack, plc_slot)
    #
    # if plc.get_connected():
    print(f'PLC {plc_ip} 连接成功\n', end='')
    # lock = threading.Lock()
    data_table_name_list = 'aaa'

    schedule.every(Time_inter).seconds.do(
        data_analysis_job,conn=conn, interval_list=interval_list, var_num=data_lengths,
        data_table_name=data_table_name_list, time_inter=Time_inter,db_name=db_name)

    while True:
        # schedule.run_pending()
        for db, start, length in zip(db_numbers, start_addresses, data_lengths):
            Sys_time = datetime.now()
            # lock.locked()
            # data = plc.read_area(Area.DB, db, start, length)
            # data_list = list(data)
            data_list = [random.randint(0, 255) for i in range(length)]
            # 设置函数嵌套局部数据采集变量
            local_data.data_list = data_list
            if plc_ip in select_ip:
                data_table_name = f'plc{plc_ip[-2:]}_db{db}'
                create_tables(data_table_name, conn, data_list)
            print(f'PLC {plc_ip} DB{db}\t\t读取的数据为:{data_list}\t\t当前时间为:{Sys_time} \n', end='')

        time.sleep(0.001)  # 每次读取后休眠 1 毫秒
    # else:
    #     print(f'PLC {plc_ip} 连接失败')


def batcr_table(plcs, select_ip, select_db):
    for plc in plcs:
        value_list = []
        for key, value in plc.items():
            value_list.append(value)
        print(value_list)
        plc_ip = value_list[0]
        plc_ip_suffix = value_list[0][-2:]
        db_name_iter = iter(value_list[3])
        for i in range(len(value_list[3])):
            db_name = next(db_name_iter)
            creat_sql = (f"CREATE TABLE IF NOT EXISTS `plc{plc_ip_suffix}_db{db_name}`"
                         f"USING data_collect TAGS ('{plc_ip}',{db_name})")
            conn.execute(creat_sql)
            table_sql = f"CREATE TABLE "
            data_length = value_list[-1][1]
            # for j in range(data_length):
            #     table_name = "module%d" % (j + 1)
            #     table_sql += (f"IF NOT EXISTS `plc{plc_ip_suffix}_db{db_name}_{table_name}` "
            #                   f"USING data_analysis TAGS ({j + 1},{db_name},'{plc_ip}') ")
            #     conn.execute(table_sql)
    #         print("成功批量建立子表！")
    # print("成功批量建立数据采集表")


if __name__ == '__main__':
    db_name = "threadTest"
    conn = create_db(db_name)
    conn.select_db(db_name)
    cursor = conn.cursor()

    # 获取到当前采集数据的总个数以及等长区间
    interval_list = data_per(40)
    interval_list_length = len(interval_list)

    # 创建相对的超级表模板
    stable_sql = f"CREATE STABLE IF NOT EXISTS data_analysis (`ts` TIMESTAMP,"
    stable_sql1 = f"CREATE STABLE IF NOT EXISTS data_collect (`ts` TIMESTAMP,"
    for i in range(interval_list_length):
        field_name1 = "inter%d_count" % (i + 1)
        field_name2 = "inter%d_per" % (i + 1)
        if i + 1 != interval_list_length:
            stable_sql += f"`{field_name1}` INT, `{field_name2}` FLOAT,"
        else:
            stable_sql += f"`{field_name1}` INT, `{field_name2}` FLOAT)"
    stable_sql += f"TAGS (`module` INT,`F_location` INT,`ip` NCHAR(50))"
    cursor.execute(stable_sql)
    print("成功创建数据统计分析超级表！")

    # TODO: 设置数据采集长度
    for i in range(100):
        field_name1 = "value%d" % (i + 1)
        if i + 1 != 100:
            stable_sql1 += f"`{field_name1}` INT,"
        else:
            stable_sql1 += f"`{field_name1}` INT)"
    stable_sql1 += f"TAGS (`ip` NCHAR(20),`db_block` INT)"
    cursor.execute(stable_sql1)
    print("成功创建数据采集超级表！")

    collect_plcs = [
        {'ip': '192.168.100.62', 'rack': 0, 'slot': 1, 'dbs': [8, 12, 20], 'starts': [200, 200, 200],
         'lengths': [100, 100, 100]},
        {'ip': '192.168.100.61', 'rack': 0, 'slot': 1, 'dbs': [8, 12, 20], 'starts': [200, 200, 200],
         'lengths': [100, 100, 100]},
        {'ip': '192.168.100.41', 'rack': 0, 'slot': 1, 'dbs': [8, 12, 20], 'starts': [200, 200, 200],
         'lengths': [100, 100, 100]},
    ]
    collect_plcs = [
        {'ip': '192.168.100.61', 'rack': 0, 'slot': 1,
         'dbs': [41]*16,
         'starts': [58, 324, 586, 852, 1114, 1380, 1642, 1908, 2170, 2436, 2698, 2964, 3226, 3492, 3754, 4020],
         'lengths': [72, 16]*8},
        {'ip': '192.168.100.62', 'rack': 0, 'slot': 1,
         'dbs': [41]*16,
         'starts': [58, 324, 586, 852, 1114, 1380, 1642, 1908, 2170, 2436, 2698, 2964, 3226, 3492, 3754, 4020],
         'lengths': [72, 16]*8},
        {'ip': '192.168.100.41', 'rack': 0, 'slot': 1,
         'dbs': [41]*16,
         'starts': [58, 324, 586, 852, 1114, 1380, 1642, 1908, 2170, 2436, 2698, 2964, 3226, 3492, 3754, 4020],
         'lengths': [72, 16]*8}
    ]
    select_ip = ['192.168.100.61', '192.168.100.62', '192.168.100.41']
    select_plcs = [plc for plc in collect_plcs if plc['ip'] in select_ip]
    print("所选ip为：", select_plcs)
    # if not select_ip:
    #     select_plcs = collect_plcs
    #     select_ip = [plc['ip'] for plc in collect_plcs]
    batcr_table(plcs=collect_plcs, select_ip=select_plcs, select_db=None)
    print("所有统计表单于采集表单已建立完成")

    # 设置统计区间间隔
    # Time_inter = int(input("请输入所需要的时间间隔："))

    # 创建并启动线程
    threads = []
    #
    for plc in collect_plcs:
        t = threading.Thread(target=get_plc_data, args=(
            plc['ip'], plc['rack'], plc['slot'], plc['dbs'], plc['starts'], plc['lengths'], select_ip))
        threads.append(t)
        t.start()

    # 等待所有线程结束
    for t in threads:
        t.join()
