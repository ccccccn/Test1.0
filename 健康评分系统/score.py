# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: score.py
 @DateTime: 2024-12-16 10:25
 @SoftWare: PyCharm
"""
import math
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timedelta
import re
from datadesrip.CreateTableTest import connect_mysql, getconnect
from 健康评分系统.mysql_data import my_data_get
from 健康评分系统.taos_data import taos_data_analysis

tag = ["积分电流x1y1x2y2z", "同步震动半径x1y1x2y2z", "同步电流x1y1x2y2z"]
strname = []
for j in range(1, 4):
    for i in range(1, 6):
        strname.append(f"MBC_1_{j}_{tag[j - 1]}_{i}")
strname_last = ",".join(strname) + ",FW_1_飞轮转速,FW_1_飞轮_TE"
tmp_coumln_name = strname_last


def score_caculate(*args):
    FW_fault_dir = args[0]
    FW_drop_dir = args[1]
    MBC_table = args[2]
    flc_no = MBC_table.split("_")[1]
    score = [0] * 34
    score[0] = flc_no
    score_conn = connect_mysql("192.168.101.144", 'root', 'wkfl.777', 'test3')
    my_cur = score_conn.cursor()
    # today = datetime.now().strftime('%Y-%m-%d') + ' 00:00:00'
    # yesterday = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d') + " 00:00:01"
    today = "2024-11-27 00:00:00.000"
    yesterday = "2024-11-26 00:00:00.000"
    MBC_search_sql = f"""
    select a.flc_no,	
    SUM(a.n_x1) + SUM(a.n_x2) + SUM(a.n_y1) + SUM(a.n_y2) + SUM(a.n_z) as total_sum,
    qujian as FW_state
    FROM {MBC_table} a
    where time between '{yesterday}' and '{today}'
    GROUP BY a.qujian,a.flc_no"""
    my_cur.execute(MBC_search_sql)
    result = my_cur.fetchall()
    MBC_dir = {}
    for row in result:
        if row[0] not in MBC_dir:
            MBC_dir[row[0]] = {}
        MBC_dir[row[0]][row[2]] = int(row[1])
    # print(f"flc_{flc_no}:{MBC_dir}\n",end=" ")
    FW_fault = []
    FW_drop = []
    for key, value in FW_fault_dir.items():
        if (key.split("_")[0][3:] == flc_no):
            FW_fault.append(value)
    for key, value in FW_drop_dir.items():
        if (key.split("_")[0][3:] == flc_no):
            FW_drop.append(value)
    tmp_fl_score = []
    for idx, fault, drop, mbc_value in zip(range(2, 34, 4), FW_fault, FW_drop, MBC_dir.values()):
        score[idx + 1] = fault
        score[idx + 2] = drop
        score[idx + 3] = mbc_value['故障'] * 1 + mbc_value['预警']
        score[idx] = 100 - 0.2 * fault - 1 * drop - 0.2 * mbc_value['预警'] - mbc_value['故障']
        tmp_fl_score.append(score[idx])
    score[1] = min(tmp_fl_score)
    # print(f"飞轮舱{flc_no}\n", score, len(score), end='')
    return score


def run_score_thread(cnt_dir, drop_times, flc_fenxi_tables):
    """

    @param cnt_dir: 飞轮舱故障次数字典
    @param flc_fenxi_table: MBC分析总表，类型list
    @param drop_times:飞轮跌落次数
    """
    print(f"开始计算评分：{datetime.now()}")
    with ThreadPoolExecutor(max_workers=25) as excutor:
        futures = [excutor.submit(score_caculate, cnt_dir, drop_times, flc_fenxi_table) for flc_fenxi_table in
                   flc_fenxi_tables]
    wait(futures)
    result = []
    for future in futures:
        result.append(future.result())
    print(f"result:{result}")
    return result
    print(f"评分结束：{datetime.now()}")


if __name__ == '__main__':
    # 连接本地mysql
    my_conn = connect_mysql("192.168.101.144", "root", "wkfl.777", "test3")
    my_cursor = my_conn.cursor()
    # 连接涛思
    ts_conn = getconnect()
    ts_cursor = ts_conn.cursor()
    my_create_sql = "create table if not exists flc_score (flc_no int, flc_score float,"
    str_list = []
    for i in range(1, 9):
        str_list.append(f"FW{i}_score float,FW{i}_fault_score float,FW{i}_drop_score float,MBC{i}_score float")
    my_create_sql += ",".join(str_list) + ")engine = InnoDB DEFAULT CHARSET= utf8"
    my_cursor.execute(my_create_sql)
    print("建表完成！")

    table_num = 21
    ## 完成报警数量的获取，5轴数值的分析
    conn, cnt_dir = my_data_get()
    taos_data_analysis(table_num)
    drop_dir = cnt_dir
    """
    8轮子为一个整体，20套，每个轮子含有4个值，其中第一个值为计算得出
    轮子的健康模型：score = 100 - 故障次数*0.5 - 跌落次数 *1 - MBC预警次数*0.2 - MBC故障次数*1
    MBC数据可以建立一个视图仅存储，预警和故障的次数以及飞轮号
    
    ## 获取MBC预警故障个数，计算评分
    select a.flc_no,	
    SUM(a.n_x1) + SUM(a.n_x2) + SUM(a.n_y1) + SUM(a.n_y2) + SUM(a.n_z) as total_sum,
    qujian as FW_state
    FROM flc_1_fenxi a
    where time between '2024-11-25 00:00:00.000' and '2024-11-26 00:00:00.000'
    GROUP BY a.qujian,a.flc_no;
    """
    flc_fenxi_table = ["flc_{}_fenxi".format(i) for i in range(1, table_num)]
    result = run_score_thread(cnt_dir, drop_dir, flc_fenxi_table)
    my_cursor.execute(f"desc `flc_score`")
    description = my_cursor.fetchall()
    Insert_column_names = ([desc[0] for desc in description])
    formatted_list = ', '.join(Insert_column_names)
    colunm_places = ",".join(['%s'] * len(Insert_column_names))
    Insert_sql = f"INSERT INTO flc_score ({formatted_list}) VALUES ({colunm_places})"
    my_cursor.executemany(Insert_sql, result)
    print("数据插入结束")
    my_conn.commit()
    my_cursor.close()
    my_conn.close()
    ts_cursor.close()
    ts_conn.close()
