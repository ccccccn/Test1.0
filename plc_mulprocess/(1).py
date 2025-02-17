import os
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from snap7.client import Client
from snap7.type import S7DataItem, Areas

from BidirectionQueue import BidirectionQueue
from 工具类.数据处理 import get_data, file_data
from 工具类.数据处理 import JsonCache
from 前期代码.定时迁移数据 import check_run

# 设置线程池和锁
lock = threading.Lock()
lockk = asyncio.Lock()
Data_queue = BidirectionQueue()

for _ in ra

# 数据解析示例
async def parse_data(data, db):
    # print(f"--------------------{db}解析结果------------------")
    cache_data = cache.cache.get(f"DBDB{db}")
    for key in cache_data:
        # name = key.replace("飞轮舱1", f"飞轮舱{str(db)}")
        cache_adata = cache_data[key]
        value = get_data(cache_adata, data)
        # print(f"{key}:{value}")
    # print(f"DB{db}块解析结束时间为：{datetime.now()}")
    # print("-"*50)

# PLC数据采集任务
async def read_plc_data(plc, db, start, length):
    try:
        # print(f"----------------{plc}----------------")
        data = plc.db_read(db, 0, length)
        # print(f"DB{db}块获取到数据:{datetime.now()}")
        await parse_data(data, db)
    except Exception as e:
        print(f"读取PLC数据错误：{e}")
        return None



# 采集每个PLC数据
async def get_plc_data(**kwargs):
    global Data_queue
    plc_ip = kwargs['ip']
    plc_rack = kwargs['rack']
    plc_slot = kwargs['slot']
    dbs = kwargs['dbs']
    starts = kwargs['starts']
    lengths = kwargs['lengths']
    plc = Client()
    plc1 = Client()
    try:
        plc.connect(plc_ip, plc_rack, plc_slot)
        plc1.connect(plc_ip, plc_rack, plc_slot)
    except Exception as e:
        print(f"连接PLC失败: {e}")
        return

    if plc.get_connected():
        print(f"PLC {plc_ip} 连接成功")

    # 将 DB 块分成两部分
    dbs_plc = dbs[:5]  # 前 5 个 DB 块由 plc 处理
    starts_plc = starts[:5]
    lengths_plc = lengths[:5]

    dbs_plc1 = dbs[5:]  # 后 6 个 DB 块由 plc1 处理
    starts_plc1 = starts[5:]
    lengths_plc1 = lengths[5:]

    while True:
        Start_time = datetime.now()
        # print(f"INFO:{plc_ip} collecting...\t当前时间为：{Start_time}\n", end='')
        # 启动所有DB块的异步任务
        tasks = []

        # 使用 plc 处理前 5 个 DB 块
        for db, start, length in zip(dbs_plc, starts_plc, lengths_plc):
            # print(f"first:{datetime.now()}")
            task = asyncio.create_task(read_plc_data(plc, db, start, length))
            tasks.append(task)

        # 等待所有DB块任务完成
        await asyncio.gather(*tasks)
        end_time = datetime.now()
        print(f"PLC{plc_ip}本次采集所用：{end_time - Start_time} \n", end='')
        # print(f"PLC {plc_ip} 数据采集完成")


# 多线程执行数据采集
def run_data_collection(plcs):
    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = {executor.submit(asyncio.run, get_plc_data(**plc)): plc for plc in plcs}

        # 获取每个线程的执行结果
        for future in as_completed(futures):
            try:
                future.result()  # 获取任务的返回结果
            except Exception as e:
                print(f"线程执行错误：{e}")


if __name__ == '__main__':
    folder_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "工具类", "FEMSjson文件")
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

    ip_addresses = ['192.168.110.1']
    # ip_addresses = ['192.168.110.{}'.format(i) for i in range(1, 21)]
    collect_plcs = [{'ip': ip, **base_config} for ip in ip_addresses]

    try:
        run_data_collection(collect_plcs)
    except KeyboardInterrupt:
        print("采集任务已终止")
