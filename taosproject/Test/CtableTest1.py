"""
 @Author: CNN
 @Email: xxx@xxx.com
 @FileName: CreateTableTest.py
 @DateTime: 2024/7/18 9:47
 @SoftWare: PyCharm
"""
import datetime

import taos


def create_tables(conn, data_list):
    now = datetime.datetime.now()
    for data in data_list:
        table_name = 'TX%d' % (data_list.index(data) + 1)
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (`ts` TIMESTAMP,`value` INT)")
        conn.execute(f"INSERT INTO {table_name} VALUES ('%s', %d)" % (now, data))
        print(f"表{table_name}已成功插入数据")

    print("当前读取数据单表建立完成！")


if __name__ == "__main__":
    conn = taos.connect(
        host='localhost',
        user='root',
        password='taosdata',
        port=6030,
    )
    data_list = [1, 2, 3, 4, 5, 6]
    # create_db(conn, "plcdata")
    for i in range(2):
        create_tables(conn, data_list)
    conn.close()
