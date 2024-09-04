import itertools
import math
import time
# import pandas as pd
import datetime

import taos
from taos import TaosConnection
from datadesrip.CreateTableTest import create_db, get_connection
from datadesrip.CreateTableTest_dump import creat_rule_sql


# today = datetime.date.today()
# print(today)


# TODO:获取当前数据库表单元个数
def data_count(table_name, db_name, time_inte):
    conn = get_connection('localhost', 'root', 'taosdata')
    conn.select_db(db_name)
    sql = f"SELECT COUNT(*) FROM {db_name}.`{table_name}` WHERE `ts` >= NOW() - {time_inte}S"
    cur = conn.cursor()
    cur.execute(sql)
    number = cur.fetchall()[0][0]
    return number

# 等差区间数据统计
def data_per(step):
    """获取统计区间（左闭右开,最后一层左闭右闭）"""
    interval_list = []
    inter = itertools.count(start=0, step=step)
    # max = cur.execute(f"select max(*) from {table_name}")
    count = math.ceil(255 / step)
    # 计算统计区间
    for i in range(count):
        start = next(inter)
        if start + step < 255:
            interval_list.append([start, start + step])
        else:
            interval_list.append([start, 255])

    return interval_list


if __name__ == "__main__":
    conn = create_db("muldata")
    cur = conn.cursor()
    count = data_count('tx', "muldata")
    print(count)
    data_list = [0, 1, 2, 3, 4, 5, 6, 7]
    suffix = ["count", "per"]
    sql = creat_rule_sql("data_analysis", data_list, suffix)
    conn.execute(sql)
    print("成功创建数据库表data_analysis")
    cur.close()
    conn.close()
