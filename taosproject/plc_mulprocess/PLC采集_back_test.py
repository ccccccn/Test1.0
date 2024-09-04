import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import threading
import time
from datetime import datetime
from multiprocessing import Process, Pool, cpu_count, Manager

import schedule
from schedula import counter
from snap7.client import Client, Area
import random

from Test.allTest_intertable import data_analysis_job
from datadesrip.CreateTableTest import create_db, creat_sql, create_tables
from datadesrip.dataanalysis import data_per
from plc_mulprocess.BidirectionQueue import BidirectionQueue
from plc_mulprocess.data_name import DataName

# 定义线程局部变量实现函数嵌套任务
local_data = threading.local()
lock = threading.Lock()
import asyncio


# def get_plc_data(cur, selected_ip, plc_ip, db_numbers, start_addresses, data_lengths):
def get_plc_data(cursor, selected_ip, plc_ip, plc_rack, plc_slot, db_numbers, start_addresses, data_lengths):
    # plc = Client()
    # plc.connect(plc_ip, plc_rack, plc_slot)

    # if plc.get_connected():
    print(f'PLC {plc_ip} 连接成功\n', end='')
    # schedule.every(Time_inter).seconds.do(
    #     data_analysis_job, conn=conn, interval_list=interval_list, var_num=varname_field,
    #     data_table_name=tbname, time_inter=Time_inter, db_name=db_name)

    while True:
        # schedule.run_pending()
        count = 1
        for db, start, length in zip(db_numbers, start_addresses, data_lengths):
            Sys_time = datetime.now()
            # lock.locked()
            #     data = plc.read_area(Area.DB, db, start, length)
            #     data_list = list(data)
            data_list = [random.randint(0, 255) for data in range(length)]
            # 设置函数嵌套局部数据采集变量
            # local_data.data_list = data_list

            if plc_ip in selected_ip:
                plc_ip_name = plc_ip.split('.')[-1]
                data_table_name = f'plc{plc_ip_name}_db{db}_block{count}'
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
                pass
                # lock.release()
            print(
                f'id为：{db_id_tag},PLC {plc_ip} DB{db}\t\t读取的数据为:{data_list}\t\t当前时间为:{Sys_time} \n',
                end='')
    time.sleep(0.01)  # 每次读取后休眠 1 毫秒


# else:
#     print(f'PLC {plc_ip} 连接失败')

def batcr_table(plcs, select_ip, select_db):
    dq = BidirectionQueue()
    for varname in DataName:
        dq.append_left(varname.name)
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
            creat_sql = f"CREATE TABLE IF NOT EXISTS `plc{plc_ip_suffix}_db{db_name}_block{i + 1}` ( `ts` TIMESTAMP, `ID` INT,"
            for num in range(length[i]):
                if num == length[i] - 1:
                    creat_sql += "`{}` INT)".format(dq.pop_right())
                else:
                    creat_sql += "`{}` INT,".format(dq.pop_right())
            conn.execute(creat_sql)
        print("{}采集表已建立完成".format(plc_ip))

    #     # 建立统计分析子表
    #     data_length = value_list[-1][1]
    #     for j in range(8):
    #         table_name = 'MBC{}'.format(j + 1)
    #         table_sql = (f"CREATE TABLE IF NOT EXISTS `plc{plc_ip_suffix}_db{db_name}_{table_name}` "
    #                      f"USING data_analysis TAGS ({j + 1},{db_name},'{plc_ip}') ")
    #         conn.execute(table_sql)
    # print("成功批量建立子表！")
    print("成功批量建立数据采集表")


def work(i, n):
    now = datetime.now()
    print("----{}-----:开始执行{},当前时间为：{}\n".format(i, n, now), end='')
    return n * 3


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
        'dbs': [41] * 16,
        'starts': [58, 324, 586, 852, 1114, 1380, 1642, 1908, 2170, 2436, 2698, 2964, 3226, 3492, 3754, 4020],
        'lengths': [15, 5] * 8
    }
    # 辅助列表，包含所有需要的IP地址
    ip_addresses = [
        f'192.168.100.{i}' for i in range(10)
        # '192.168.100.61', '192.168.100.62'
        # , '192.168.100.41', '192.168.100.42',
        # '192.168.100.43', '192.168.100.44', '192.168.100.45', '192.168.100.46',
        # '192.168.100.47', '192.168.100.48', '192.168.100.49', '192.168.100.50',
        # '192.168.100.51', '192.168.100.52', '192.168.100.53', '192.168.100.54',
        # '192.168.100.55', '192.168.100.56', '192.168.100.57', '192.168.100.58'
    ]
    # 使用列表生成式创建最终的PLC列表
    collect_plcs = [{'ip': ip, **base_config} for ip in ip_addresses]
    # 创建相对的超级表模板
    stable_sql = f"CREATE STABLE IF NOT EXISTS data_analysis (`ts` TIMESTAMP,"
    stable_sql1 = f"CREATE STABLE IF NOT EXISTS data_collect_DATA (`ts` TIMESTAMP,`ID` INT INCREMENT,"
    count = 0
    for item in DataName:
        for i in range(interval_list_length):
            field_name1 = "inter{}_count{}".format(i + 1, item.name)
            field_name2 = "inter{}_per{}".format(i + 1, item.name)
            if count != interval_list_length * 20 - 1:
                stable_sql += f"`{field_name1}` INT, `{field_name2}` FLOAT,"
                count += 1
            else:
                stable_sql += f"`{field_name1}` INT, `{field_name2}` FLOAT)"
    stable_sql += f"TAGS (`module` INT,`F_location` INT,`ip` NCHAR(50))"
    cursor.execute(stable_sql)
    # print(stable_sql)
    print("成功创建数据统计分析超级表！")

    length = collect_plcs[0].get('lengths')
    print(length)

    # # 方案一：有规律采用超级表，创建数据采集超级表
    # count = 0
    # for varname in DataName:
    #     field_name1 = 'MBC{}'.format(i + 1) + varname.name
    #     if count <= 15:
    #         if count != 15:
    #             stable_sql1 += f"`{field_name1}` INT,"
    #             count += 1
    #         else:
    #             stable_sql1 += f"`{field_name1}` INT)"
    #             stable_sql1 = f"CREATE STABLE IF NOT EXISTS data_collect_Numline (`ts` TIMESTAMP,`ID` INT INCREMENT,"
    #     else:
    #         stable_sql1 += f"`{field_name1}` INT"
    #
    # stable_sql1 += f"TAGS (`ip` NCHAR(20),`db_block` INT)"
    # cursor.execute(stable_sql1)
    # print("成功创建数据采集超级表！")

    # 方案二：无规律采用直接建表
    select_ip = [plc['ip'] for plc in collect_plcs]
    select_plcs = [plc for plc in collect_plcs if plc['ip'] in select_ip]
    batcr_table(plcs=collect_plcs, select_ip=select_plcs, select_db=None)
    print("所有统计表单于采集表单已建立完成")

    # 设置统计区间间隔
    # Time_inter = int(input("请输入所需要的时间间隔："))
    Time_inter = 2

    # 创建并启动线程
    # threads = []
    # # #
    # for plc in collect_plcs:
    #     t = threading.Thread(target=get_plc_data, args=(
    #         cursor, select_ip, plc['ip'], plc['rack'], plc['slot'], plc['dbs'], plc['starts'], plc['lengths']))
    #     threads.append(t)
    #     t.start()
    # # 等待所有线程结束
    # for t in threads:
    #     t.join()
    #
    # cursor.close()
    # conn.close()
    # 使用线程池优化
    # with ThreadPoolExecutor(max_workers=128) as executor:
    #     # 为每个PLC配置创建一个任务
    #     try:
    #         futures = {
    #             executor.submit(get_plc_data, cursor, select_ip, plc['ip'], plc['rack'], plc['slot'], plc['dbs'],
    #                             plc['starts'], plc['lengths']): plc for plc in collect_plcs}
    #     except KeyboardInterrupt:
    #         print("用户终止程序")
    #         pass

    # 使用进程循环执行
    # multiprocessing.freeze_support()
    # for plc in collect_plcs:
    #     args = (cursor, select_ip, plc['ip'], plc['rack'], plc['slot'], plc['dbs'], plc['starts'], plc['lengths'])
    #     p = Process(target=get_plc_data,
    #                 args=args)
    #     p.start()
    #     p.join()

    pool = Pool(processes=cpu_count())
    for plc in collect_plcs:
        arges = (cursor, select_ip, plc['ip'], plc['rack'], plc['slot'], plc['dbs'], plc['starts'], plc['lengths'])
        pool.apply_async(get_plc_data, args=arges)

    pool.close()
    pool.join()

    print("over!")