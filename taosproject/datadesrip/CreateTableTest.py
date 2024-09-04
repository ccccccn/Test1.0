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
def get_connection(localhost, username, password):
    conn = taos.connect(
        localhost=localhost,
        user=username,
        password=password,
        port=6030,
    )
    return conn


# TODO:创建数据库
def create_db(db):
    # conn = get_connection("localhost", "root", "root")
    conn = taos.connect(
        localhost="localhost",
        user="root",
        password="taosdata",
        port=6030
    )
    # conn.execute(f"DROP DATABASE IF EXISTS {db}")
    conn.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
    conn.select_db(db)
    print("数据库建立完成！")
    return conn


def getconnect():
    # conn = get_connection("localhost", "root", "root")
    conn = taos.connect(
        localhost="localhost",
        user="root",
        password="taosdata",
        port=6030
    )
    return conn


# TODO:检查表是否存在,并创建表单
def connect_mysql():
    conn = pymysql.connect(
        host='192.168.111.93',
        user='root',
        password='lys0520',
        db='testt',
        charset='utf8mb4'
    )
    return conn


def create_tables(table_name, conn, data_list, db_id):
    """conn.execute("create stable alldata values (`ts` TIMESTAMP, `value` FLOAT) TAGS(`id` INT)")"""
    # now = datetime.datetime.now()
    # sql = creat_sql(table_name, data_list)
    # conn.execute(sql)
    insert_sql = Insert_sql(table_name, data_list, db_id)
    conn.execute(insert_sql)
    print(f"{table_name}数据插入成功！\t", end='')


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
def Insert_sql(table_name, data_list, db_id):
    now = datetime.datetime.now()
    Insert_sql = f"INSERT INTO `{table_name}` VALUES ('{now}',{db_id},"
    lenth = len(data_list)
    for i in range(lenth):
        if (i + 1 < len(data_list)):
            Insert_sql += f"{data_list[i]}, "
        else:
            Insert_sql += f"{data_list[i]}) "
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
