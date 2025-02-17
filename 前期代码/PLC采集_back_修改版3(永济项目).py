import os
import threading
from datetime import datetime
from multiprocessing import Process, Pool, freeze_support
import asyncio
from snap7.client import Client

from datadesrip.CreateTableTest import create_db
from plc_mulprocess.BidirectionQueue import BidirectionQueue
from plc_mulprocess.data_name import DataName, DataName_en
from 前期代码.定时迁移数据 import check_run
from 工具类.数据处理 import file_data, get_data

# 定义线程局部变量实现函数嵌套任务
lock = threading.Lock()
lockk = asyncio.Lock()
import asyncio

# 新建全局队列，存储暂时数据
Data_queue = BidirectionQueue()

# # 将输入输出重定向到日志记录中
# sys.stdout = StreamToLogger(logging.getLogger('STDOUT'), logging.INFO)
# sys.stderr = StreamToLogger(logging.getLogger('STDERR'), logging.ERROR)
from 工具类.数据处理 import JsonCache


async def get_plc_data(selected_ip, **kwargs):
    global Data_queue
    plc_ip = kwargs['ip']
    plc_rack = kwargs['rack']
    plc_slot = kwargs['slot']
    dbs = kwargs['db']
    starts = kwargs['start']
    lengths = kwargs['length']

    try:
        plc = Client()
        plc.connect(plc_ip, plc_rack, plc_slot)
        plc.connect(kwargs['ip'], kwargs['rack'], kwargs['slot'])
    except Exception as e:
        print("当前错误为：", e)
    if plc.get_connected():
        print(f'PLC {plc_ip} 连接成功')

    data_dir = {}
    while True:
        try:
            Start_time = datetime.now()
            for db, start, length in zip(dbs, starts, lengths):
                # lock.locked()
                data = plc.db_read(db, start, length)
                cache_data = cache.cache.get(f"DB{db}block")
                for key in cache_data:
                    cache_adata = cache_data[key]
                    value = get_data(cache_adata, data)
                    print(f"{key}:{value}")
            end_time = datetime.now()
            print("本次采集所用：{}".format(end_time - Start_time))
            await asyncio.sleep(0.005)  # 每次读取后休眠 1 毫秒
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
    with Pool(processes=16) as pool:
        pool.starmap_async(process_ip, [(plc, selected_ip) for plc in plcs])


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

    folder_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "工具类", "json文件")
    # 生成程序可阅读较小的缓存
    cache = JsonCache()
    cache.load_from_dir(folder_path)
    db = []
    start = []
    lengths = []
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
    ip_addresses = ['192.99.7.22']
    # 使用列表生成式创建最终的PLC列表
    collect_plcs = [{'ip': ip, **base_config} for ip in ip_addresses]

    # 方案二：无规律采用直接建表
    select_ip = [plc['ip'] for plc in collect_plcs]
    freeze_support()

    try:
        process1 = Process(target=run_data_collection, args=(collect_plcs, select_ip))
        process1.start()
        process1.join()
    except KeyboardInterrupt:
        print("采集任务已终止")
