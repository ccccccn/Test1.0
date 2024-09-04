import datetime
import random
import time

import numpy as np
import schedula

from data_analysis.interval_table import data_analysis_job
from datadesrip.CreateTableTest import create_db, create_tables, creat_sql
from datadesrip.CreateTableTest_dump import creat_rule_sql


# from datadesrip.CreateTableTest2 import

def data_analysis_job(conn, interval_list, var_num):
    cursor = conn.cursor()
    all_count = []
    all_per = []
    for j in range(var_num):
        var_count = []
        var_per = []
        var_name = "value%d" % (j + 1)
        for i in range(len(interval_list)):
            l_interval = interval_list[i][0]
            r_interval = interval_list[i][1]
            if i + 1 == len(interval_list):
                count_sql = \
                    (
                        f"SELECT count(CASE WHEN `{var_name}` >=  {l_interval} AND `{var_name}` < {r_interval} THEN `{var_name}` END) "
                        f"AS interval_{l_interval}_{r_interval} FROM `{data_table_name}` ")
            else:
                count_sql = \
                    (
                        f"SELECT count(CASE WHEN `{var_name}` >=  {l_interval} AND `{var_name}` <= {r_interval} THEN `{var_name}` END) "
                        f"AS interval_{l_interval}_{r_interval} FROM `{data_table_name}` ")
            # print("当前执行的sql语句为：", count_sql)
            cursor.execute(count_sql)

            # 汇总统计量以及占比
            count = cursor.fetchall()[0][0]
            percent3 = float(str((count / num) * 100)[:5])
            var_count.append(count)
            var_per.append(percent3)
        all_count.append(var_count)
        all_per.append(var_per)

        return all_count, all_per

    # 开始向子表插入数据
    now = datetime.datetime.now()
    interval_list_length = len(interval_list)
    for i in range(var_num):
        table_name = "module_%d" % (i + 1)
        Insert_table_sql = f"INSERT INTO {table_name} USING data_analysis TAGS ({i + 1},1) VALUES ('{now}',"
        for j in range(interval_list_length):
            if (j + 1) == interval_list_length:
                Insert_table_sql += f"{all_count[i][j]},{all_per[i][j]})"
            else:
                Insert_table_sql += f"{all_count[i][j]},{all_per[i][j]},"
        cursor.execute(Insert_table_sql)
        print("成功向module%d定时插入数据！" % (i + 1))

    pass
def Test(conn, data_table_name, interval_list):
    """
    :param conn:
    :param count:  表示当前测试一次性写入的数据量
    :return:
    """
    module_length = 10
    cursor = conn.cursor()

    """准备工作"""
    # 建立数据采集表单
    create_tables_sql = creat_sql(data_table_name, module_length)
    cursor.execute(create_tables_sql)
    print("成功建立数据采集表单")

    # 通过超级表批量建立多个变量表单
    table_sql = f"CREATE TABLE "
    for i in range(module_length):
        table_name = "module_%d" % (i + 1)
        table_sql += f"IF NOT EXISTS `{table_name}` USING data_analysis TAGs({i + 1},1) "
    conn.execute(table_sql)
    print("成功批量建立子表！")

    try:
        while True:
            # data_tmp = schedula.every(10).second.do(get_interval_data(module_length, data_table_name, cursor))
            count , per = schedula.every(10).minute.do(data_analysis_job(conn,interval_list,module_length))
            data_list = []
            for i in range(module_length):
                data = random.randint(0, 256)
                data_list.append(data)
            print('数据生成结束：', data_list)
            create_tables(data_table_name, conn, data_list)
            # 获取所需数据的区间个数

            # 获取所得到的变量个数
            time.sleep(1)

    except KeyboardInterrupt:
        print("用户终止程序！")


def get_interval_data(module_length, data_table_name, cursor):
    all_data = []
    for i in range(module_length):
        var_name = "value%d" % (i + 1)
        var_sql = f"select {var_name} from `{data_table_name}` where `ts` >= NOW() - 10S"
        cursor.execute(var_sql)
        value_count = [t[0] for t in cursor.fetchall()]
        print("当前模块的值为：", value_count)
        all_data.append(value_count)
    return all_data


if __name__ == "__main__":
    conn = create_db("muldata")
    table_name = "data_summary"
    interval_list = [[0, 40], [40, 80], [80, 160], [160, 320]]
    Test(conn=conn, data_table_name=table_name, interval_list=interval_list)
    conn.close()
