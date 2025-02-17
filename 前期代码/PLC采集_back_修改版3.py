import logging
import queue
import struct
import sys
from collections import deque
import threading
from datetime import datetime
from decimal import getcontext, Decimal
from multiprocessing import Process, Pool, freeze_support
import asyncio
from snap7.client import Client, Area

from datadesrip.CreateTableTest import create_db, create_tables, getconnect, connect_mysql
from datadesrip.dataanalysis import data_per
from plc_mulprocess.BidirectionQueue import BidirectionQueue
from plc_mulprocess.data_name import DataName, DataName_en
from 前期代码.定时迁移数据 import check_run
from 工具类.记录日志 import StreamToLogger

# 定义线程局部变量实现函数嵌套任务
lock = threading.Lock()
lockk = asyncio.Lock()
import asyncio

# 新建全局队列，存储暂时数据
Data_queue = BidirectionQueue()

# 将输入输出重定向到日志记录中
sys.stdout = StreamToLogger(logging.getLogger('STDOUT'), logging.INFO)
sys.stderr = StreamToLogger(logging.getLogger('STDERR'), logging.ERROR)


async def get_plc_data(selected_ip, plc_ip, plc_rack, plc_slot, db_numbers, start_addresses, data_lengths, *args):
    global Data_queue

    try:
        plc = Client()
        plc.connect(plc_ip, plc_rack, plc_slot)
    except Exception as e:
        print("当前错误为：", e)
    if plc.get_connected():
        print(f'PLC {plc_ip} 连接成功')

    dbarr = [8] * 8
    start_arr = [16, 88, 160, 232, 304, 376, 448, 520]
    dbqueue = queue.Queue()
    start_q = queue.Queue()
    dbqueue.put(dbarr[i] for i in range(len(dbarr)))
    start_q.put(start_arr[i] for i in range(len(start_arr)))

    while True:
        count = 1
        try:
            for db, start, length in zip(db_numbers, start_addresses, data_lengths):
                # lock.locked()
                # data_list = []
                temp_list = []
                async with lockk:
                    db = dbqueue.get()
                    r_start = start_q.get()
                    t_start = start_q.get()
                    rotate = plc.read_area(Area.DB, db, r_start, 4)
                    dbqueue.put(db)
                    start_q.put(r_start)
                    temperature = plc.read_area(Area.DB, db, t_start, 2)
                    start_q.put(t_start)
                    temp_list.extend((rotate, temperature))
                    for data in temp_list:
                        selreal = struct.unpack('>f', bytes(data))[0]
                        data_list.append(selreal)
                cnt = length // 5
                # print("当前获取到的长度lengths为：",length)
                Sys_time = datetime.now()
                data_list = deque()
                data_list.put(Sys_time)
                for idx in range(cnt):
                    start_address = start + (idx * 28)
                    data = plc.read_area(Area.DB, db, start_address, 20)
                    for j in range(5):
                        start_idx = j * 4
                        end_idx = start_idx + 4
                        tag = 1
                        if data[j * 4:j * 4 + 4][0] & 0x80:
                            tag = 0
                        selreal = struct.unpack('>f', bytes(data[start_idx:end_idx]))[0]
                        # 设置小数精度位数
                        getcontext().prec = 10
                        num = Decimal(selreal)
                        # 使用 normalize 方法直接保留指定位数的小数部分
                        num = num.quantize(Decimal('1.00000'))  # 保留4位小数
                        data_list.append(num)
                if plc_ip in selected_ip:
                    plc_ip_name = plc_ip.split('.')[-1]
                    data_table_name = f'plc{plc_ip_name}_db{db}_MBC{count}'
                    count += 1
                    # lock.acquire()
                    async with lockk:
                        # 减少连接可能
                        conn = getconnect()
                        conn.select_db("threadTestt3")
                        cursor = conn.cursor()

                        db_id_sql = f'select count(*) from `{data_table_name}`'
                        cursor.execute(db_id_sql)
                        var_num = cursor.fetchall()[0][0]
                        if var_num is None:
                            var_num = 0
                        db_id_tag = var_num + 1
                        data_list.append(db_id_tag)
                        create_tables(data_table_name, conn, data_list, db_id_tag)
                        # lock.release()
                        print(
                            f'PLC {plc_ip} DB{db}\t\t读取的数据为:{data_list}\t\t当前时间为:{Sys_time} \n',
                            end='')
            await asyncio.sleep(0.01)  # 每次读取后休眠 1 毫秒
        except Exception as e:
            print(f"采集错误: {e}")
            break


def process_ip(plc_info, selected_ip):
    ip = plc_info['ip']
    rack = plc_info['rack']
    slot = plc_info['slot']
    db_numbers = plc_info['dbs']
    start_addresses = plc_info['starts']
    data_lengths = plc_info['lengths']

    # 使用 asyncio.run 来启动协程
    asyncio.run(get_plc_data(selected_ip, ip, rack, slot, db_numbers, start_addresses, data_lengths))
    asyncio.run(get_plc_data(selected_ip, plc_info))


def run_data_collection(plcs, selected_ip):
    with Pool(processes=8) as pool:
        pool.starmap(process_ip, [(plc, selected_ip) for plc in plcs])


def run_transfer():
    with Pool(processes=8) as pool:
        pool.map(check_run, [None])


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
        # db_name_iter = iter(value_list[3])
        for i in range(len(value_list[3])):
            # db_name = next(db_name_iter)
            creat_sql = (f"CREATE TABLE IF NOT EXISTS `plc{plc_ip_suffix}_MBC{i + 1}` "
                         f"USING data_collect_DATA tags ('{plc_ip}',{i + 1})")
            cursor.execute(creat_sql)
    print("{}采集子表已建立完成".format(plc_ip))


if __name__ == '__main__':
    db_name = "threadTestt3"
    conn = create_db(db_name)
    conn.select_db(db_name)
    cursor = conn.cursor()
    try:
        sql_conn = connect_mysql()
        my_cur = sql_conn.cursor()
    except Exception as e:
        print("Mysql连接失败：", e)
    print("Connect!")

    # 定时转移数据任务

    # 获取到当前采集数据的总个数以及等长区间
    interval_list = data_per(100)
    interval_list_length = len(interval_list)
    base_config = {
        'rack': 0, 'slot': 1,
        'dbs': [41] * 8,
        'rt_bds': [8] * 8,
        'starts': [58, 584, 1110, 1636, 2162, 2688, 3214, 3740],
        'rt_starts': [16, 88, 160, 232, 304, 376, 448, 520],
        'lengths': [15] * 8

    }
    # 辅助列表，包含所有需要的IP地址
    ip_addresses = ['192.99.7.22']
    # 使用列表生成式创建最终的PLC列表
    collect_plcs = [{'ip': ip, **base_config} for ip in ip_addresses]
    # 创建相对的超级表模板
    stable_sql1 = f"CREATE STABLE IF NOT EXISTS data_collect_DATA (`ts` TIMESTAMP,`ID` INT ,"
    length = collect_plcs[0].get('lengths')
    # print(length)

    # 方案一：有规律采用超级表，创建数据采集超级表
    stable_sql1 += ",".join(f"`{field_name1.name}` float" for field_name1 in DataName)
    stable_sql1 += f") TAGS (`ip` NCHAR(20),`MBC_id` INT)"
    cursor.execute(stable_sql1)
    print("成功创建数据采集超级表！")

    # 方案二：无规律采用直接建表
    select_ip = [plc['ip'] for plc in collect_plcs]
    select_plcs = [plc for plc in collect_plcs if plc['ip'] in select_ip]
    batcr_table(plcs=collect_plcs, select_ip=select_plcs, select_db=None)
    print("所有统计表单于采集表单已建立完成")

    freeze_support()

    try:
        # run_transfer()
        # run_data_collection(collect_plcs, selected_ip=ip_addresses)
        process1 = Process(target=run_data_collection, args=(collect_plcs, select_ip))
        # process2 = Process(target=check_run)

        process1.start()
        # process2.start()

        process1.join()
        # process2.join()
    except KeyboardInterrupt:
        print("采集任务已终止")
