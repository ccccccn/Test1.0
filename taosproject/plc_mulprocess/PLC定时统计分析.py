# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: PLC定时统计分析.py
 @DateTime: 2024/8/29 18:11
 @SoftWare: PyCharm
"""
from datetime import datetime, timedelta

import MySQLdb

from datadesrip.CreateTableTest import getconnect


def create_tables(index, day):
    conn = MySQLdb.connect(host='192.168.111.93', user='root', passwd='lys0520', db='testt')
    conn1 = MySQLdb.connect(host='192.168.111.93', user='root', passwd='lys0520', db='sywk')
    cur = conn.cursor()
    cur1 = conn1.cursor()
    sql1 = 'DESC flc_1_mbc_1_2024_08_07'
    cur1.execute(sql1)
    print("获取成功\n", end='')
    result1 = [row[0] for row in cur1]
    start_time = datetime.now()
    # day = datetime.now().day
    hours = datetime.now().hour
    table_name = f'flc_MBC{index}_2024_08_{day}_{hours}'
    sql4 = f'drop table if exists {table_name}'
    try:
        cur.execute(sql4)
    except Exception as e:
        print(e)
    print("删除表成功\n", end='')
    sql2 = f'create table if not exists {table_name} (time datetime not null,'
    for item in result1[1:17]:
        sql2 += f'`{item}` float,'
    sql2 += '飞轮舱号 varchar(255),MBC编号 varchar(255))'
    try:
        cur.execute(sql2)
    except Exception as e:
        print("这里是第一个错误：{}\n".format(e), end='')
    print("创建新表成功！\n", end='')
    sql_alter = f'ALTER TABLE {table_name} CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci'
    try:
        cur.execute(sql_alter)
    except Exception as e:
        print("当前错误为：", e)
    finally:
        print("修改成功！\n", end='')

    COUNT = 0
    while COUNT < 50000:
        now = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
        sql3 = "insert into {} values ('{}',".format(table_name, now)
        for j in range(len(result1[1:17])):
            value = 2
            word = "{}---》{}".format(result1[1:17][j], value)
            sql3 += f'{value},'
        sql3 += f"'飞轮舱1','MBC{index}')"
        try:
            cur.execute(sql3)
            conn.commit()
        except Exception as e:
            print("当前错误为：\n", e, end='')
        COUNT += 1
        print(f"向{table_name}插入一条数据！\n", end='')
    end_time = datetime.now()
    print(f"已批量向{table_name}中插入数据,共计消耗：{end_time - start_time}")