# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: 性能测试.py
 @DateTime: 2024/9/13 14:12
 @SoftWare: PyCharm
"""
import psutil
import os
import time

while True:
    pid = os.getpid()
    current_process = psutil.Process(pid)

    cpu_usage = current_process.cpu_percent(interval=1)
    memory = current_process.memory_info()
    memory_usage = memory.rss / 1024 / 1024
    mem = psutil.virtual_memory()
    memory_usage = current_process.memory_info().rss / 1024 / 1024
    print(
        f"当前cpu利用率: {cpu_usage}\t\t"
        f"当前内存使用: {mem.total / (1024 ** 3):.2f}GB,{mem.used / (1024 ** 3):.2f}GB,{mem.percent:.2f}%\n",
        end='')
