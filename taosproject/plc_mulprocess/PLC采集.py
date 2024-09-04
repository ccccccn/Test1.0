# from snap7.client import Client, Areas
# import time
#
# # 连接PLC
# plc_ip = '192.168.100.60'  # PLC的IP地址
# plc_rack = 0  # 机架号，默认为0
# plc_slot = 1  # 插槽号，默认为1
# plc = Client()
# plc.connect(plc_ip, plc_rack, plc_slot)
#
# if plc.get_connected():
#     print('PLC连接成功')
# else:
#     print('PLC连接失败')
#     exit()
#
# try:
#     while True:
#         # 读取PLC数据
#         db_number = 16  # 数据块号
#         start_address = 0  # 起始地址
#         data_length = 1000  # 数据长度，即要读取的USInt数据个数
#         data = plc.read_area(Areas.DB, db_number, start_address, data_length)  # 一个USInt数据占用1个字节
#
#         # 将读取的数据解析为USInt类型数据列表
#         data_list = list(data)  # 每个字节对应一个USInt数据
#         print('读取的数据为:', data_list)
#
#         # 每次读取后休眠10ms
#         time.sleep(0.001)
#
# except KeyboardInterrupt:
#     print("用户终止程序")

# 断开PLC连接
# plc.disconnect()

import threading
import time
from datetime import datetime

from snap7.client import Client, Area


def get_plc_data(plc_ip, plc_rack, plc_slot, db_numbers, start_addresses, data_lengths):
    plc = Client()
    plc.connect(plc_ip, plc_rack, plc_slot)

    if plc.get_connected():
        print(f'PLC {plc_ip} 连接成功')
        while True:
            for db, start, length in zip(db_numbers, start_addresses, data_lengths):
                data = plc.read_area(Area.DB, db, start, length)
                data_list = list(data)
                print(f'PLC{plc_ip} DB{db} 读取的数据为: {data_list}\n', end='')
            time.sleep(1)  # 每次读取后休眠 1 毫秒
    else:
        print(f'PLC {plc_ip} 连接失败')


plcs = [
        {'ip': '192.168.100.61', 'rack': 0, 'slot': 1,
         'dbs': [41] * 16,
         'starts': [58, 324, 586, 852, 1114, 1380, 1642, 1908, 2170, 2436, 2698, 2964, 3226, 3492, 3754, 4020],
         'lengths': [15, 5] * 8},
        {'ip': '192.168.100.62', 'rack': 0, 'slot': 1,
         'dbs': [41] * 16,
         'starts': [58, 324, 586, 852, 1114, 1380, 1642, 1908, 2170, 2436, 2698, 2964, 3226, 3492, 3754, 4020],
         'lengths': [15, 5] * 8},
        {'ip': '192.168.100.41', 'rack': 0, 'slot': 1,
         'dbs': [41] * 16,
         'starts': [58, 324, 586, 852, 1114, 1380, 1642, 1908, 2170, 2436, 2698, 2964, 3226, 3492, 3754, 4020],
         'lengths': [15, 5] * 8},
    ]
# 创建并启动线程
threads = []
for plc in plcs:
    t = threading.Thread(target=get_plc_data, args=(
        plc['ip'], plc['rack'], plc['slot'], plc['dbs'], plc['starts'], plc['lengths']))
    threads.append(t)
    t.start()

# 等待所有线程结束
for t in threads:
    t.join()
