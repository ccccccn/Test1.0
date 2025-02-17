# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: 数据暂存.py
 @DateTime: 2024/9/9 9:54
 @SoftWare: PyCharm
"""
import time
from collections import deque
from datetime import datetime, timedelta
import random

data_block = deque()


def data_store(date, data, interval):
    global data_block
    if data_block:
        while True:
            first_data_time = data_block[0]['date']
            # first_data_time = datetime(first_data['date'])
            if date - first_data_time > interval:
                data_block.popleft()
            else:
                break
        # else:
        #     data_block.appendleft(first_data)
        data_block.append({"date": date, "value": data})
    else:
        data_block.append({"date": date, "value": data})


if __name__ == "__main__":
    start_time = datetime.now()
    try:
        while True:
            now = datetime.now()
            print("程序正在运行-->", now.strftime("%S"))
            dataa = [random.randint(1, 5) for _ in range(15)]
            interval = timedelta(days=1)
            # data_block.append({'date': now, 'value': data})
            data_store(now, dataa, interval)
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("用户终止了程序")
        pass
    finally:
        print("开始时间为：",start_time.strftime("%Y-%m-%d %H:%M:%S"))
        for item in data_block:
            print(f"Date: {item['date'].strftime('%Y-%m-%d %H:%M:%S')}, Data: {item['value']}")
