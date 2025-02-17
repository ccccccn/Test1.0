# -*- coding  = utf-8 -*-
# @Time : 2024/8/30 8:54
# @Author : cc
# @File : 定时迁移数据.py
# @Soft-name : PyCharm
import itertools
from asyncio import as_completed
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import time

import pymysql
import os

from Test.allTest import Test

sub_day = 100
# 连接MySQL数据库
from datadesrip.CreateTableTest import getconnect, connect_mysql
from 工具类.数据交换类 import comparable

tag = ["积分电流x1y1x2y2z", "同步震动半径x1y1x2y2z", "同步电流x1y1x2y2z"]
column_names = []
for j in range(1, 4):
    for i in range(1, 6):
        column_names.append(f"MBC_1_{j}_{tag[j - 1]}_{i}")
print("组合前：", column_names)
"""
str类型用于sql查询
list类型用于循环匹配
"""
column_names.append("FW_1_飞轮转速")
column_names.append("FW_1_飞轮_TE")


def load_value(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            value = int(f.readline())
            return value
    return 100


def save_value(file_path, value):
    with open(file_path, 'w') as f:
        f.write(str(value))


sub_days = load_value("虚拟消耗天数.txt")

# 5轴数据分析
def migrate_table_data(mysql_conn, sub_table, mysql_table, sub_day):
    global sub_days
    try:
        try:
            try:
                taos_conn = getconnect()
                taos_conn.select_db("threadtest")
                tdsql_cursor = taos_conn.cursor()
            except  Exception as e:
                print("涛思数据库连接失败：", e)
                exit(1)
            # 从涛思数据库子表查询数据
            start = '2024-11-25 16:15:26.152'
            end = '2024-12-03 11:21:01.211'

            # column_names_match_test = [column_name.split("_")[3][:4]+"_{}" for for column_name in column_names[:-2]]
            # TODO:修改数据库名
            for mbc_no in range(1,9):
                print(f"当前迁移{mbc_no}....")
                tag = ["积分电流x1y1x2y2z", "同步震动半径x1y1x2y2z", "同步电流x1y1x2y2z"]
                column_names = []
                for j in range(1, 4):
                    for i in range(1, 6):
                        column_names.append(f"MBC_{mbc_no}_{j}_{tag[j - 1]}_{i}")
                # print("组合前：", column_names)
                column_names.append(f"FW_{mbc_no}_飞轮转速")
                column_names.append(f"FW_{mbc_no}_飞轮_TE")
                suffix = itertools.cycle(["x1", "y1", "x2", "y2", "z"])
                column_names_match_test = [
                    column_name.split("_")[3][:-9] + f"_{next(suffix)}"
                    for column_name in column_names[:-2]
                ]
                column_names_match_test.append(column_names[-2])
                column_names_match_test.append(column_names[-1])
                print("aaaa")
                query = "UNION ALL ".join([
                    f"""
                    SELECT '{datetime.now().strftime('%Y-%m-%d')}' AS `ts`,
                    '{column_name_tag}' AS `biaozhi`,
                    MAX(`{column_name_select}`) AS `max`,
                    MIN(`{column_name_select}`) AS `min`,
                    MAX(`{column_names[15]}`) AS `R_max`,
                    MAX(`{column_names[16]}`) AS `T_max`,
                    {mbc_no} AS `mbc_no`
                    FROM threadtest.`{sub_table}`
                    WHERE ts BETWEEN '{start}' AND '{end}'
                    interval(21s)
                    """ for column_name_tag, column_name_select in zip(column_names_match_test[:-2], column_names[:-2])])
                # 动态生成插入语句

                column_placeholder = ', '.join(['%s'] * 7)  # 根据列的数量生成占位符
                insert_sql = f"INSERT INTO `{mysql_table}` (time,biaozhi,max,min,R_max,T_max,mbc_no) VALUES ({column_placeholder})"
                print(query)

                try:
                    tdsql_cursor.execute(query)
                except Exception as e:
                    print(e)
                    pass
                print("bbbbbb")
                result = tdsql_cursor.fetchall()
                # print(result)
                # data = Insert_or_Update(mysql_conn, mysql_table_day)
                # 对采集数据排序
                sort_result = sorted(result, key=lambda x: x[1])
                """
                继续处理排序数据 （最大值最小值）
                如果同号判定正负，同为正不变，同为负交换位置，异号对比绝对值，大的为最大值，小的为最小值
                """
                compa_result = []
                for item in sort_result:
                    temp_list = list(item)  # 将元组转换为列表
                    temp_list[2], temp_list[3] = comparable(temp_list[2], temp_list[3])  # 交换索引2和索引3的元素
                    compa_result.append(tuple(temp_list))  # 转回元组并添加到新列表
                compa_results = []
                tmp_date = datetime.now()
                diff_day = 0
                # 模拟24天数据
                print("cccccccccc")
                for item in compa_result:
                    temp_list = list(item)
                    if diff_day < 367:
                        temp_list[0] = (tmp_date + timedelta(days=-diff_day)).strftime('%Y-%m-%d')+" 00:00:01"
                        diff_day += 1
                    else:
                        diff_day = 0
                        temp_list[0] = (tmp_date + timedelta(days=-diff_day)).strftime('%Y-%m-%d')+" 00:00:01"
                        diff_day += 1
                    compa_results.append(tuple(temp_list))

                # print("原数据：", result)
                # print("对比前：", sort_result)
                # print("对比后：", compa_results)
                try:
                    with mysql_conn.cursor() as mysql_cursor:
                        mysql_cursor.executemany(insert_sql, compa_results)
                        mysql_conn.commit()
                        print("commit!")
                        # mysql_conn.commit()
                except Exception as e:
                    print("Mysql执行出现错误！", e)
            print(f"{sub_table} 的数据已成功迁移到 {mysql_table}")
            tdsql_cursor.close()
            taos_conn.close()
        except Exception as e:
            print("涛思数据库出错！", e)

            # 将数据插入到MySQL表中


    except Exception as e:
        print(f"数据迁移出错: {e}")


def Insert_or_Update(mysql_conn, mysql_table):
    # 获取当前数据表的最后一条time
    cur = mysql_conn.cursor()
    cur.execute(f'SELECT * '
                f'FROM {mysql_table} '
                f'WHERE time IN '
                f'(SELECT MAX(time) FROM {mysql_table} '
                f'GROUP BY DATE(time))')
    data = cur.fetchall()
    return data


# 如果时间相同就覆盖 否则插入


def check_run():
    taos_conn = getconnect()
    # TODO:修改涛思数据库名称
    taos_conn.select_db("threadtestt4")
    mysql_conn = connect_mysql('192.168.111.93', 'test3')
    mysql_conn_pri = connect_mysql('192.168.111.93', 'testt2')
    cur = taos_conn.cursor()
    sql_cur = mysql_conn.cursor()
    sqlpro_cur = mysql_conn_pri.cursor()
    # cur.execute(f'show tables')
    sqlpro_cur.execute(f'show tables')
    table_list = [tname[0] for tname in cur.fetchall()]
    sort_table = sorted(table_list)
    mysql_table = [f"flc_1_mbc_{item + 1}_trend_fenxi" for item in range(8)]
    mysql_table_day = [f"{item}_day" for item in mysql_table]
    while True:
        day = datetime.today().strftime('%Y_%m_%d')
        hour = str(datetime.now().hour).ljust(2, '0')
        now = datetime.now()
        print("我确实在执行！-------{}".format(now.second))
        # TODO:调整时间
        if now.second == 30:
            print("检测到整点")
            for i in range(len(mysql_table)):
                create_sql(sql_cur, mysql_table[i])
                create_sql(sql_cur, mysql_table_day[i])
            for sub_tab in sort_table:
                for i in range(24):  # 修复生成器使用，逐个传递 in
                    index = sort_table.index(sub_tab)
                    # cursor = sql_conn.cursor()
                    # create_sql(cursor, mysql_table)
                    migrate_table_data(taos_conn, mysql_conn, sub_tab, mysql_table[index], i)
            time.sleep(10)
        time.sleep(1)


def create_sql(cursor, mysql_table):
    # sql = "drop table if exists " + mysql_table
    c_sql = (
        f"create  table if not exists {mysql_table} (`time` DATETIME NOT NULL,"
        f"`biaozhi` VARCHAR(20),"
        f"`max` Decimal(4,3),"
        f"`min` DECIMAL(4,3), "
        f"`T_max` FLOAT, "
        f"`R_max` bigint, "
        f"`mbc_no` VARCHAR(20))"
        f"character set = utf8"
    )
    cursor.execute(c_sql)
    print("成功建表！")


def main():
    start_time = datetime.now()

    try:
        sql_conn = connect_mysql('192.168.101.144', 'root', 'wkfl.777', 'test3')
        # mysql_conn_pri = connect_mysql('192.168.111.93', 'testt2')
        print("Connect!")
    except Exception as e:
        print("Mysql连接失败：", e)


    hour = datetime.now().hour
    day = datetime.now().date().strftime("%Y_%m_%d")
    table_list = ["flc_{}".format(i) for i in range(1, 21)]
    mysql_table = [f"flc_{item}_trend_fenxi" for item in range(1, 21)]
    # check_run()
    # 数据库表是否存在
    try:
        count = 0
        for table, mysql_table in zip(table_list, mysql_table):
            if count == 20:
                break
            # for i in range(20):  # 修复生成器使用，逐个传递 i
            cursor = sql_conn.cursor()
            create_sql(cursor, mysql_table)
            # migrate_table_data(taos_conn, sql_conn, table, mysql_table, i)
            migrate_table_data(sql_conn, table, mysql_table, sub_days)
            count += 1

        # save_value('虚拟消耗', sub_days)

    except KeyboardInterrupt:
        print("用户终止程序")
    print("全部完成迁移")

    sql_conn.close()
    end_time = datetime.now()
    print("本次用时：", end_time - start_time)

if __name__ == "__main__":
    main()

