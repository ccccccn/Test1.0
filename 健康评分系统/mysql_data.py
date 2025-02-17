# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: mysql_data.py
 @DateTime: 2024-12-16 10:22
 @SoftWare: PyCharm
"""
import random
from datetime import date, timedelta, datetime

import yesterday

from datadesrip.CreateTableTest import connect_mysql

def my_data_get():
    my_conn = connect_mysql("192.168.101.144", "root", "wkfl.777", "sywk")
    my_cursor = my_conn.cursor()
    # yesterday = date.today() - timedelta(days=1)
    # today = date.today()
    FLAG_list = [f"飞轮舱{cang}_FW_{FW}" for cang in range(1, 21) for FW in range(1, 9)]
    yesterday = "2024-02-24 08:19:36"
    today = "2024-02-27 08:19:36"


    search_sql = (
        f"SELECT IOServerName FROM alarm WHERE GroupName = '飞轮报警' "
        f"and EventType='报警' and EventTime between '{yesterday}' and '{today}'"
    )
    # print(search_sql)

    # 假设 my_cursor 已经是一个有效的数据库游标对象
    my_cursor.execute(search_sql)
    result = my_cursor.fetchall()

    FW_warning_cnt = {FW: 0 for FW in FLAG_list}
    for row in result:
        which_FW_flag = "_".join(row[0].split("_")[:3])
        FW_warning_cnt[which_FW_flag] += 1

    return my_conn,FW_warning_cnt

if __name__ == '__main__':
    my_conn = connect_mysql("192.168.101.144", "root", "wkfl.777", "test3")
    my_cursor = my_conn.cursor()
    # yesterday = date.today() - timedelta(days=1)
    # today = date.today()
    # FLAG_list = [f"飞轮舱{cang}_FW_{FW}" for cang in range(1,21) for FW in range(1,9)]
    # yesterday = "2024-02-24 08:19:36"
    # today = "2024-02-27 08:19:36"
    #
    # search_sql = (
    #     f"select a.IOServerName,COUNT(*) as Count from alarm a where a.GroupName='飞轮报警'"
    #     f"and a.EventType='报警' and EventTime between '{yesterday}' and '{today}'"
    #     f"GROUP BY a.IOServerName"
    # )
    # print(search_sql)
    #
    # # 假设 my_cursor 已经是一个有效的数据库游标对象
    # my_cursor.execute(search_sql)
    # result = my_cursor.fetchall()
    #
    # # print(result)
    # FW_warning_cnt = {FW:0 for FW in FLAG_list}
    # for row in result:
    #     which_FW_flag = "_".join(row[0].split("_")[:3])
    #     FW_warning_cnt[which_FW_flag] += row[1]
    # print(FW_warning_cnt)

    my_cursor.execute('describe flc_score')
    filed_names = my_cursor.fetchall()
    fied_name_list = []
    for each in filed_names:
        fied_name_list.append(each[0])
    print(fied_name_list)
    #

    for i in range(15):
        for j in range(1,21):
            fl_scores = []
            FW1_fault_score = [random.randint(0, 2) for i in range(8)]
            FW1_drop_score = [random.randint(0, 1) for i in range(8)]
            MBC_score = [random.randint(0, 5) for i in range(8)]
            for fault, drop, mbc in zip(FW1_fault_score, FW1_drop_score, MBC_score):
                fl_scores.append(100 - fault * 0.2 - drop * 1 - mbc * 0.2)
            # fl_scores = (round(num,2) for num in fl_scores)
            flc_score = min(fl_scores)
            sql_value_list = []
            for fl_score, fault, drop, mbc in zip(fl_scores, FW1_fault_score, FW1_drop_score, MBC_score):
                sql_value_list.append(fl_score)
                sql_value_list.append(fault)
                sql_value_list.append(drop)
                sql_value_list.append(mbc)
            print(sql_value_list)
            sql_value_list.insert(0,j)
            sql_value_list.insert(1,flc_score)
            sql_value_list.append((datetime.now()+timedelta(days=-i)).strftime("%Y-%m-%d %H:%M:%S"))
            insert_sql = f"insert into flc_score_copy1 values {tuple(sql_value_list)}"
            # print(insert_sql)
            my_cursor.execute(insert_sql)
    my_conn.commit()
    my_cursor.close()
    my_conn.close()
