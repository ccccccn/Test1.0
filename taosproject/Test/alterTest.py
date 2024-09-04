import datetime

import taos

from datadesrip.CreateTableTest import create_db

if __name__ == '__main__':
    conn = create_db("alterTest")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS test (`ts` TIMESTAMP, `value` FLOAT)"
    )
    now = datetime.datetime.now()
    print("建表结束！:当前时间为：",now)

    conn.execute(
        f"INSERT INTO test VALUES ('{now}',13)"
    )
    print("插入结束！")

    conn.close()
