"""
 @Author: CNN
 @Email: xxx@xxx.com
 @FileName: CreateTableTest.py
 @DateTime: 2024/7/18 9:47
 @SoftWare: PyCharm
"""

import datetime
import random
import re
import time

import MySQLdb
import pymysql
import taos


# TODO：获取数据库连接
def get_connection(localhost):
    conn = taos.connect(
        localhost=localhost,
        user="root",
        password="taosdata",
        port=6030,
    )
    return conn


# TODO:创建数据库
def create_db(db):
    # conn = get_connection("localhost", "root", "root")
    print("!!!")
    conn = taos.connect(
        localhost="localhost",
        user="root",
        password="taosdata",
        port=6030
    )
    print("~~~~")
    # conn.execute(f"DROP DATABASE IF EXISTS {db}")
    # conn.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
    try:
        conn.execute(
            f"CREATE DATABASE IF NOT EXISTS {db} vgroups 10 BUFFER 256 CACHEMODEL 'BOTH' PAGES 128 PAGESIZE 16")
    except Exception as e:
        print(e)
    print("数据库建立完成！")
    return conn


def getconnect():
    # conn = get_connection("localhost", "root", "root")
    conn = taos.connect(
        host="localhost",
        user="root",
        password="taosdata",
        port=6030
    )
    return conn


# TODO:检查表是否存在,并创建表单
def connect_mysql(host_ip, username, password, databases):
    conn = pymysql.connect(
        host=f'{host_ip}',
        user=f'{username}',
        password=f'{password}',
        db=f'{databases}',
        charset='utf8'
    )
    return conn


def create_tables(table_name, conn, data_list,tag):
    """conn.execute("create stable alldata values (`ts` TIMESTAMP, `value` FLOAT) TAGS(`id` INT)")"""
    # now = datetime.datetime.now()
    # sql = creat_sql(table_name, data_list)
    # conn.execute(sql)
    insert_sql = Insert_sql(table_name, data_list,tag)
    # print(insert_sql)
    conn.execute(insert_sql)
    # print(f"{table_name}数据插入成功！\t", end='')


# TODO:获取创建数据库sql
def creat_sql(table_name, data_list_length):
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (`ts` TIMESTAMP,"
    for i in range(data_list_length):
        var_name = "value%i" % (i + 1)
        if (i + 1 < data_list_length):
            sql += f"`{var_name}` INT,"
        else:
            sql += f"`{var_name}` INT )"
    return sql


# TODO:获取插入数据库sql
def Insert_sql(table_name, data_list,tags):
    # USING
    # meters
    # TAGS("California.SanFrancisco", 2)
    now = datetime.datetime.now()
    Insert_sql = f"INSERT INTO threadtest.`{table_name}` USING `data_collect_super_table` TAGS ({tags})"
    Insert_sql += f" VALUES ('{now}',"
    sql = ','.join(map(str, data_list))
    Insert_sql = Insert_sql + sql + ')'
    # lenth = len(data_list)
    # for i in range(lenth):
    #     if (i + 1 < len(data_list)):
    #         Insert_sql += f"{data_list[i]}, "
    #     else:
    #         Insert_sql += f"{data_list[i]}) "
    # print("当前插入语句为：%s\n"%Insert_sql,end='')
    return Insert_sql


# 获取数据库表单个数sql
# def select_sql(table_name, data_list):
#

if __name__ == "__main__":
    db = "tabletest"
    conn = create_db(db)
    stable_name = "data_table"
    # conn.execute(f"CREATE STABLE {stable_name} (`ts` TIMESTAMP, `value` FLOAT,`module` INT) TAGS (w"
    data_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    for _ in range(2):
        create_tables(conn, data_list)
    conn.close()
