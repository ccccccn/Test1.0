# -*- coding  = utf-8 -*-
# @Time : 2024/8/30 8:54
# @Author : cc
# @File : 定时迁移数据.py
# @Soft-name : PyCharm
from datetime import datetime, timedelta
import time

import pymysql
import os

sub_day = 100
# 连接MySQL数据库
from datadesrip.CreateTableTest import getconnect, connect_mysql
from 工具类.数据交换类 import comparable


def load_value(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            value = int(f.readline())
            return value
    return 100

def save_value(file_path, value):
    with open(file_path,'w') as f:
        f.write(str(value))

sub_days = load_value("虚拟消耗天数.txt")


def migrate_table_data(tdsql_conn, mysql_conn, sub_table, mysql_table, startt, sub_day):
    global sub_days
    try:
        try:
            # 从涛思数据库子表查询数据
            tdsql_cursor = tdsql_conn.cursor()
            new_start = str(startt).rjust(2,'0')
            new_end = str(startt+1).rjust(2,'0')
            day = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')
            start = f'{day} {new_start}:00:00'
            end = f'{day} {new_end}:00:00'
            if startt == 23:
                startt = -1
                date = datetime.strptime(day, '%Y-%m-%d')
                end = f'{date + timedelta(days=1)}'
            try:
                tdsql_cursor.execute(f"desc `{sub_table}`")
            except Exception as e:
                print(e)
                pass
            finally:
                print("taos执行结束！")
            # 获取列名
            description = tdsql_cursor.fetchall()
            column_names = [desc[0] for desc in description[2:-3]]
            # TODO:修改数据库名
            query = "UNION ALL ".join([
                f"""
                SELECT '{start}' AS `ts`,
                '{column_name}' AS `biaozhi`, 
                MAX(`{column_name}`) AS `max`,
                MIN(`{column_name}`) AS `min`
                FROM threadtestt3.`{sub_table}`
                WHERE ts BETWEEN '{start}' AND '{end}'
                """ for column_name in column_names])
            # 动态生成插入语句
            column_placeholder = ', '.join(['%s'] * 4)  # 根据列的数量生成占位符
            insert_sql = f"INSERT INTO `{mysql_table}` (time,biaozhi,max,min) VALUES ({column_placeholder})"
            print(insert_sql+"------------------------------")
            tdsql_cursor.execute(query)
            result = tdsql_cursor.fetchall()
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
                temp_list[0] = (datetime.now() + timedelta(days=-(sub_day - startt))).strftime('%Y-%m-%d')+" 00:00:01"
                compa_result.append(tuple(temp_list))  # 转回元组并添加到新列表
            # # 模拟24天数据
            # for item in compa_result:
            #     temp_list = list(item)
            #     temp_list[0] = (datetime.now() + timedelta(days=-(sub_day - startt))).strftime('%Y-%m-%d')
            #     sub_days -= 1
            #     compa_result.append(tuple(temp_list))

            print("原数据：", result)
            print("对比前：", sort_result)
            print("对比后：", compa_result)

        except Exception as e:
            print("涛思数据库出错！", e)

        # 将数据插入到MySQL表中
        try:
            with mysql_conn.cursor() as mysql_cursor:
                mysql_cursor.executemany(insert_sql, compa_result)
                mysql_conn.commit()
                print(f"{sub_table} 的数据已成功迁移到 {mysql_table}")
                mysql_conn.commit()
        except Exception as e:
            print("Mysql执行出现错误！", e)

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
    mysql_conn = connect_mysql('192.168.111.93', 'testt3')
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
        f"`min` DECIMAL(4,3)) character set = utf8"
    )
    cursor.execute(c_sql)
    print("成功建表！")


if __name__ == "__main__":
    start_time = datetime.now()
    try:
        taos_conn = getconnect()
        taos_conn.select_db("threadtestt3")
    except  Exception as e:
        print("涛思数据库连接失败：", e)
        exit(1)
    try:
        sql_conn = connect_mysql('192.168.111.93', 'testt3')
        mysql_conn_pri = connect_mysql('192.168.111.93', 'testt2')
    except Exception as e:
        print("Mysql连接失败：", e)
    print("Connect!")

    taos_cur = taos_conn.cursor()
    taos_cur.execute(f"show tables")
    table_list = [tbname[0] for tbname in taos_cur.fetchall()]
    hour = datetime.now().hour
    day = datetime.now().date().strftime("%Y_%m_%d")
    mysql_table = [f"flc_1_mbc_{item + 1}_trend_fenxi" for item in range(8)]
    # check_run()
    # 数据库表是否存在
    try:
        for table, mysql_table in zip(table_list, mysql_table):
            for i in range(24):  # 修复生成器使用，逐个传递 i
                cursor = sql_conn.cursor()
                create_sql(cursor, mysql_table)
                # migrate_table_data(taos_conn, sql_conn, table, mysql_table, i)
                migrate_table_data(taos_conn, sql_conn, table, mysql_table, i, sub_days)
        save_value('虚拟消耗', sub_days)

    except KeyboardInterrupt:
        print("用户终止程序")
    print("全部完成迁移")
    taos_conn.close()
    sql_conn.close()
    end_time = datetime.now()
    print("本次用时：", end_time - start_time)
