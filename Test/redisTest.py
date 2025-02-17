import datetime
import json
import time
import random

import redis
from sortedcontainers import SortedList

from Test.allTest import cost_time

# 连接到Redis服务器
r = redis.StrictRedis(host='localhost', port=6379, db=1)

# 定义一个函数来创建树状结构
# def create_tree(redis_client, root_key, s_path, value=None):
#     current_key = root_key
#     for idx, part in enumerate(s_path):
#         current_second_key = f"{current_key}:{part}"
#         # if current_second_key not in redis_client:
#
#         for count in range(10):
#             data = {
#                 "msg": count
#             }
#             r.hset(current_second_key, f"current_key_{count}", data)
#         r.expire(current_second_key, 86400)
#
#
#
# # 创建树状结构
# create_tree(r, 'showcenter', ['piedata', 'rackinfo'])


# # 定义一个函数来递归查询树状结构
# def get_tree(redis_client, key, level=0):
#     indent = '    ' * level
#     fields = redis_client.hgetall(key)
#     # print(f"{indent}{key} ({fields['count'].decode('utf-8')})")
#     next_key = f"{key}:"
#     cursor = '0'
#     while True:
#         cursor, keys = redis_client.scan(cursor=cursor, match=next_key, count=1000)
#         for k in keys:
#             get_tree(redis_client, k.decode('utf-8'), level + 1)
#         if cursor == '0':
#             break
#
#
# # 查询树状结构
# get_tree(r, 'db1')
print("over")
# start = datetime.datetime.now()
# print(f"当前时间为：{start}")
# test_list = SortedList()
# test_list.add(("飞轮舱4_实际SOC", 3))
# test_list.add(("飞轮舱1_实际SOC", 3))
# test_list.add(("飞轮舱3_实际SOC", 3))
# print("before列表：")
# for k, v in test_list:
#     print(f"{k}:{v}")
# test_list.remove(("飞轮舱1_实际SOC", test_list["飞轮舱1_实际SOC"]))
# test_list.add(("飞轮舱1_实际SOC", 6))
# endtime = datetime.datetime.now()
# print("after列表：")
# for k, v in test_list:
#     print(f"{k}:{v}")
# print(f"数据处理结束：{endtime - start}")

import numpy as np

# 创建一个数组
# arr = np.array([random.randint(-100, 100) for _ in range(1000)])
# bins = np.arange(-100, 100, 20)
# # print(f"处理数据：{arr}")
# # print(f'区间：{bins}')
# # 定义区间
#
# # 计算每个区间的元素数量
# try:
#     while True:
#         random_idx = random.randint(0, 999)
#         random_value = random.randint(-100, 100)
#         arr[random_idx] = random_value
#         hist, bin_edges = np.histogram(arr, bins=bins)
#         start = datetime.datetime.now()
#         hist_json_str = json.dumps((hist / 1000).tolist())
#         hist_json = json.loads(hist_json_str)
#         print(f'元素分布情况：{hist / 1000},数据类型{type(hist_json)},\n'
#               f'序列化后{hist_json}')
#         end = datetime.datetime.now()
#         print(f"数据处理花费：{(end - start)}s")
#         time.sleep(1)
# except KeyboardInterrupt as e:
#     print("用户终止了操作")
#     pass
#
# print(f'区间边界：{bin_edges}')

soc_value = np.zeros(20)
frequency_value = np.zeros(20)
duration_value = np.zeros(20)
soc_bins = np.linspace(-100, 100, 10)

@cost_time
def send_to_redis_channel(channel_name, CABIN_NUM=20):
    random_idx_arr = [random.randint(0, 19) for _ in range(3)]
    print("random_idx:",random_idx_arr)
    random_value_arr = [random.randint(-100, 100) for _ in range(3)]
    print("random_value:",random_value_arr)
    cnt = 0
    for idx, value in zip(random_idx_arr, random_value_arr):
        if cnt == 0:
            soc_value[idx] = value
        if cnt == 1:
            frequency_value[idx] = value
        if cnt == 2:
            duration_value[idx] = value
        cnt += 1
    soc_hist, bin_edges = np.histogram(soc_value, bins=soc_bins)
    frequency_hist, bin_edges = np.histogram(frequency_value, bins=soc_bins)
    duration_hist, bin_edges = np.histogram(duration_value, bins=soc_bins)
    data = {
        "title": "pie_info",
        "message": {
            "soc": (soc_hist / CABIN_NUM * 100).tolist(),
            "frequency": (frequency_hist / CABIN_NUM * 100).tolist(),
            "duration": (duration_hist / CABIN_NUM * 100).tolist(),
        }
    }
    print("------------new------------")
    print(f"soc:{soc_value}")
    print(f"frequency:{frequency_value}")
    print(f"duration:{duration_value}")
    print(f"data:{data}")


if __name__ == "__main__":
    print(f"soc:{soc_value}")
    print(f"frequency:{frequency_value}")
    print(f"duration:{duration_value}")
    print(f"bins:{soc_bins}")
    while True:
        send_to_redis_channel("show")
        time.sleep(1)
