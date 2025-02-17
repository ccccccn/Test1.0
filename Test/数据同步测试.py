# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: 数据同步测试.py
 @DateTime: 2024/9/19 13:40
 @SoftWare: PyCharm
"""
import taos

conn = taos.connect(
    host="192.168.44.131",
    user="root",
    password="taosdata",
    port=6030
)
print("成功连接从机")
cur = conn.cursor()
cur.execute("show databases")
databases = cur.fetchall()
print(databases)

cur.close()
conn.close()