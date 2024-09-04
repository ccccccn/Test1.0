# -*- coding  = utf-8 -*-
# @Time : 2024/8/30 8:54
# @Author : cc
# @File : 定时迁移数据.py
# @Soft-name : PyCharm
from datetime import datetime,timedelta
import time

import pymysql

# 连接MySQL数据库
from datadesrip.CreateTableTest import getconnect, connect_mysql


def migrate_table_data(tdsql_conn, mysql_conn, sub_table, mysql_table, startt):
    try:
        try:
            # 从涛思数据库子表查询数据
            tdsql_cursor = tdsql_conn.cursor()
            # select_sql = f"SELECT *  " \
            #              f"CASE WHEN `ts`>= NOW()-60s THEN  END " \
            #              f"FROM {sub_table}"
            day = '2024-08-31'
            start = f'{day} {startt}:00:00'
            end = f'{day} {startt + 1}:00:00'
            if startt == 23:
                startt = -1
                date = datetime.strptime(day, '%Y-%m-%d')
                day = date + timedelta(days=1)
                end = f'{day}'
            select_sql = (f"select * from `{sub_table}` "
                          f"where ts between '{start}' and '{end}'")
            tdsql_cursor.execute(select_sql)
            data = tdsql_cursor.fetchall()

            # 获取列名
            column_names = [desc[0] for desc in tdsql_cursor.description[2:]]

            # 动态生成插入语句
            column_placeholder = ', '.join(['%s'] * (len(column_names) + 2))  # 根据列的数量生成占位符
            columns = ', '.join(column_names)
            columnses = "time,飞轮转速," + columns
            insert_sql = f"INSERT INTO {mysql_table} ({columnses}) VALUES ({column_placeholder})"
        except Exception as e:
            print("涛思数据库出错！", e)

        # 将数据插入到MySQL表中
        try:
            with mysql_conn.cursor() as mysql_cursor:
                mysql_cursor.executemany(insert_sql, data)
                mysql_conn.commit()
                print(f"{sub_table} 的数据已成功迁移到 {mysql_table}")
                mysql_conn.commit()
        except Exception as e:
            print("Mysql执行出现错误！", e)

    except Exception as e:
        print(f"数据迁移出错: {e}")


def check_run(sub_table, mysql_table):
    tdsql_conn = getconnect()
    taos_conn.select_db("threadtestt2")
    mysql_conn = connect_mysql()
    while True:
        now = datetime.datetime.now()
        print("我确实在执行！-------{}".format(now.second))
        if now.second == 0:
            print("检测到整点")
            for sub_tab in sub_table:
                index = sub_table.index(sub_tab)
                migrate_table_data(tdsql_conn, mysql_conn, sub_tab, mysql_table[index])
            time.sleep(10)
        time.sleep(1)


def create_sql(cursor, mysql_table):
    c_sql = (
        f"create  table if not exists {mysql_table} (`time` DATETIME NOT NULL,飞轮转速 FLOAT,`积分电流_X1` FLOAT,`积分电流_Y1` FLOAT,"
        f"`积分电流_X2` FLOAT,"
        f"`积分电流_Y2` FLOAT,"
        f"`积分电流_Z` FLOAT,"
        f"`同步震动半径_X1` FLOAT,"
        f"`同步震动半径_Y1` FLOAT,"
        f"`同步震动半径_X2` FLOAT,"
        f"`同步震动半径_Y2` FLOAT,"
        f"`同步震动半径_Z` FLOAT,"
        f"`同步电流_X1` FLOAT,"
        f"`同步电流_Y1` FLOAT,"
        f"`同步电流_X2` FLOAT,"
        f"`同步电流_Y2` FLOAT,"
        f"`同步电流_Z` FLOAT,"
        f"飞轮舱号 VARCHAR(255),"
        f"`MBC编号` VARCHAR(255))"
    )
    cursor.execute(c_sql)
    print("成功建表！")


if __name__ == "__main__":
    start_time = datetime.now()
    try:
        taos_conn = getconnect()
        taos_conn.select_db("threadtestt")
    except  Exception as e:
        print("涛思数据库连接失败：", e)
        exit(1)
    try:
        sql_conn = connect_mysql()
    except Exception as e:
        print("Mysql连接失败：", e)
    print("Connect!")

    table_list = [f'plc22_db41_MBC{i + 1}' for i in range(8)]
    hour = datetime.now().hour
    # day = datetime.now().date().strftime("%Y_%m_%d")
    day = '2024_08_31'
    mysql_table = [f"flc_1_mbc{item + 1}_{day}" for item in range(8)]
    # check_run(table_list, mysql_table)
    # 数据库表是否存在

    try:
        for table, mysql_table in zip(table_list, mysql_table):
            temp = mysql_table
            for i in range(0,14):  # 修复生成器使用，逐个传递 i 值
                cursor = sql_conn.cursor()
                mysql_table = f"{temp}_{i}"
                create_sql(cursor, mysql_table)
                migrate_table_data(taos_conn, sql_conn, table, mysql_table, i)
    except KeyboardInterrupt:
        print("用户终止程序")
    print("全部完成迁移")
    taos_conn.close()
    sql_conn.close()
    end_time = datetime.now()
    print("本次用时：", end_time - start_time)
