# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: BidirectionQueue.py
 @DateTime: 2024/8/20 9:21
 @SoftWare: PyCharm
"""

import os
import random
import time


def read_file(file_name):
    if os.path.exists(file_name):
        with open(file_name, 'r') as f:
            value = int(f.readline())
            return value
    return 100


def write_file(file_name, value):
    with open(file_name, 'w') as f:
        f.write(str(value))


value = read_file("虚拟消耗天数.txt")

def tttt(*args,**kwargs):
    day = args+args
def sub_value():
    global value
    value -= 1


if __name__ == '__main__':
    for i in range(7):
        sub_value()
    write_file("虚拟消耗天数.txt", value)
    print("消耗成功")
    data_dir = {}
    try:
        while True:
            for i in range(8):
                print("这是我执行的第{}次".format(i))
                data_list = [random.randint(0, 100) for i in range(15)]
                if f"MBC{i + 1}" not in data_dir:
                    data_dir[f'MBC{i + 1}'] = [data_list]
                else:
                    data_dir[f'MBC{i + 1}'].append(data_list)
                time.sleep(0.1)
    except:
        print("用户终止了程序。。。。")
        pass
    finally:
        for key, value in data_dir.items():
            print(key + ":" + str(value))
        # print(data_dir)
