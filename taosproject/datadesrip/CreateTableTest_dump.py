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

import taos
from datadesrip.CreateTableTest import creat_sql

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
    try:
        conn = get_connection("localhost", "root", "taosdata")
        # conn.execute(f"DROP DATABASE IF EXISTS {db}")
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        conn.select_db(db)
        print("数据库建立完成并完成连接！")
    except Exception as e:
        print("账号密码输入有误！")
        pass
    return conn


# TODO:检查表是否存在,并创建表单
def create_tables(conn, data_list):
    """conn.execute("create stable alldata values (`ts` TIMESTAMP, `value` FLOAT) TAGS(`id` INT)")"""
    # now = datetime.datetime.now()
    table_name = 'tx'
    sql = creat_sql(table_name, data_list)
    conn.execute(sql)
    insert_sql = Insert_sql(table_name, data_list)
    conn.execute(insert_sql)
    print("数据插入成功！")


# TODO:获取创建数据库sql
def creat_rule_sql(table_name, data_list, suffix):
    var_name = []
    for index in data_list:
        suffix_iter = iter(suffix)
        var_name.append(
            ["value%i_%s" % (index + 1 , suffix_iter.__next__()), "value%i_%s" % (index + 1 , suffix_iter.__next__())])
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (`ts` TIMESTAMP,"
    lenth = len(data_list)
    for i in range(lenth):
        if (i + 1 < len(data_list)):
            sql += f"`{var_name[i][0]}` INT,`{var_name[i][1]}` INT,"
        else:
            sql += f"`{var_name[i][0]}` INT,`{var_name[i][1]}` INT)"
    return sql


# TODO:获取插入数据库sql
def Insert_sql(table_name, data_list):
    now = datetime.datetime.now()
    Insert_sql = f"INSERT INTO {table_name} VALUES ('{now}',"
    lenth = len(data_list)
    for i in range(lenth):
        if (i + 1 < len(data_list)):
            Insert_sql += f"{data_list[i]}, "
        else:
            Insert_sql += f"{data_list[i]}) "
    return Insert_sql

# TODO:查询表单下的特定属性
# def select_sql():
#     sql = f"SELECT * FROM {}"

if __name__ == "__main__":
    db = "tabletest"
    conn = create_db(db)
    stable_name = "data_table"
    # conn.execute(f"CREATE STABLE {stable_name} (`ts` TIMESTAMP, `value` FLOAT,`module` INT) TAGS (w"
    data_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    for _ in range(2):
        create_tables(conn, data_list)
    conn.close()
