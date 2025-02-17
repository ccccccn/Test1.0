# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: Fault_recording.py
 @DateTime: 2024/11/7 8:56
 @SoftWare: PyCharm
"""
import itertools
from datetime import datetime, timedelta
import math
import pymysql
import setting
from datadesrip.CreateTableTest import connect_mysql, getconnect

DB_CONFIG = setting.mysql_config
inter_dir = setting.inter_dir
state = setting.states
tags = setting.tags
DB_NAME = 'threadtest'

try:
    pyconn = connect_mysql(
        host_ip=DB_CONFIG['host'],
        username=DB_CONFIG['username'],
        password=DB_CONFIG['password'],
        databases='sywk'
    )
    print("Connected to mysql_localhost!")
    tconn = getconnect()
    tconn.select_db(DB_NAME)
    print("Connected to taos_localhost!")
except pymysql.err as e:
    print("Error %d: %s" % (e.args[0], e.args[1]))


def Fault_Recording_info():
    pass


#
def list_index(n):
    idx = 0
    while idx < n:
        yield idx
        idx += 1


if __name__ == '__main__':
    tcur = tconn.cursor()
    pycur = pyconn.cursor()
    flag = 1
    tcur.execute(f"desc `data_collect_data`")
    column_name = [column_name[0] for column_name in tcur.fetchall()][3:-2]
    condition = []
    index = list_index(15)
    for inters in inter_dir['故障']:
        for inter in inters:
            condition.append(f"`{column_name[next(index)]}` between {inter[0]} and {inter[1]}")
    condition_sql = ' or '.join(condition)

    cnt = 0
    tmp_ts = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')

    ## 首先获取当天的所有数据
    tcur.execute(f"select * from `plc22_MBC4` where ts between '2024-11-06 16:18:10.322' and '2024-11-06 16:18:25.489'")
    now_data = tcur.fetchall()
    off_set = 0
    fault_inter = [item for sublist in inter_dir['故障'] for item in sublist]
    print(fault_inter)

    Time = 1
    for data in now_data[off_set:]:
        index = list_index(15)
        cnt = next(index)
        if math.fabs(data[3 + cnt]) > fault_inter[cnt][0] and data[2] > 240:

            ## 依据if获取当前数据出现故障点，并获取当前故障点前1s的所有数据
            tmp_ts = data[0] + timedelta(milliseconds=-10)
            end_ts = data[0] + timedelta(seconds=-1)
            tcur.execute(f"select * from `plc22_MBC4` where ts between '{end_ts}' and '{tmp_ts}'")
            pre_data = tcur.fetchall()
            pre_data = [tuple(list(pre_data_item) + [Time]) for pre_data_item in pre_data]

            ## 设置MySQL插入语句
            My_column_name = "ts,飞轮舱温度,飞轮转速,"+','.join(column_name) + ',次数'
            column_place = ','.join(['%s'] * 19)
            insert_sql = f"insert into fault_recording ({My_column_name}) values ({column_place})"
            print(insert_sql)
            pycur.executemany(insert_sql, pre_data)

            while True:
                if math.fabs(data[2 + next(index)]) < fault_inter[cnt][0]:
                    off_set = now_data.index(data)
                pass
    ## 然后开始遍历所有数据集
    while True:
        row = tcur.fetchall_row()
        if row is None:
            break
        print(row)

    tcur.close()
    tconn.close()
