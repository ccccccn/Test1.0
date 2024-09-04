import datetime
import random
import time

import numpy as np
import schedule

from data_analysis.interval_table import data_analysis_job
from datadesrip.CreateTableTest import create_db, create_tables, creat_sql
from datadesrip.CreateTableTest_dump import creat_rule_sql
from datadesrip.dataanalysis import data_per, data_count


# from datadesrip.CreateTableTest2 import


# 数据分析函数
def data_analysis_job(conn, interval_list, var_num, data_table_name, time_inter, db_name):
    """
    :param conn:数据库连接接口
    :param interval_list: 区间列表
    :param var_num: 变量个数
    :param data_table_name: 数据表
    :param time_inter: 时间间隔
    :param db_name: 数据库名称
    :return:
    """
    cursor = conn.cursor()
    da_table_name = list(data_table_name)[0][0]
    num = data_count(da_table_name, db_name, time_inter)
    all_count = []
    all_per = []
    for var_name in var_num:
        var_count = []
        var_per = []
        # var_name = "value%d" % (j + 1)
        for i in range(len(interval_list)):
            l_interval = interval_list[i][0]
            r_interval = interval_list[i][1]
            if i + 1 == len(interval_list):
                count_sql = \
                    (
                        f"SELECT count(CASE WHEN `{var_name}` >=  {l_interval} AND `{var_name}` < {r_interval} AND `ts`>= NOW()-{time_inter}S THEN `{var_name}` END) "
                        f"AS interval_{l_interval}_{r_interval} FROM `{da_table_name}` ")
            else:
                count_sql = \
                    (
                        f"SELECT count(CASE WHEN `{var_name}` >=  {l_interval} AND `{var_name}` <= {r_interval} AND`ts`>= NOW()-{time_inter}S THEN `{var_name}` END) "
                        f"AS interval_{l_interval}_{r_interval} FROM `{da_table_name}` ")
            # print("当前执行的sql语句为：", count_sql)
            cursor.execute(count_sql)

            # 汇总统计量以及占比
            count = cursor.fetchall()[0][0]
            percent3 = float(str((count / num) * 100)[:5])
            var_count.append(count)
            var_per.append(percent3)
        all_count.append(var_count)
        all_per.append(var_per)

    # 开始向子表插入数据
    now = datetime.datetime.now()
    interval_list_length = len(interval_list)

    group = 0
    for i in range(8):
        plc_ip = '192.168.100.{}'.format(da_table_name[3:5])
        # print("-------------{}-------------".format(plc_ip))
        table_name = da_table_name + "_MBC{}".format(i + 1)
        Insert_table_sql = f"INSERT INTO `{table_name}` VALUES ('{now}',"
        for count in range(group*20,int(len(var_num))):
            for j in range(interval_list_length):
                if (j + 1) == interval_list_length and count + 1 == int(len(var_num) / 8):
                    Insert_table_sql += f"{all_count[count][j]},{all_per[count][j]})"
                else:
                    Insert_table_sql += f"{all_count[count][j]},{all_per[count][j]},"
            # if count + 1 % 20 == 0:
            #     group += 1
            #     break
        cursor.execute(Insert_table_sql)
        print("成功向{}定时插入数据！\n".format(table_name), end='')

    return all_count, all_per


# 测试函数
def Test(conn, data_table_name, interval_list, Time_inter):
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

    schedule.every(Time_inter).seconds.do(
        data_analysis_job, conn=conn, interval_list=interval_list, var_num=module_length,
        data_table_name=data_table_name, time_inter=Time_inter, db_name=db_name)
    try:
        while True:
            # data_tmp = schedula.every(10).second.do(get_interval_data(module_length, data_table_name, cursor))
            schedule.run_pending()
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


# # 获取时间段数据测试
# def get_interval_data(module_length, data_table_name, cursor):
#     all_data = []
#     for i in range(module_length):
#         var_name = "value%d" % (i + 1)
#         var_sql = f"select {var_name} from `{data_table_name}` where `ts` >= NOW() - 10S"
#         cursor.execute(var_sql)
#         value_count = [t[0] for t in cursor.fetchall()]
#         print("当前模块的值为：", value_count)
#         all_data.append(value_count)
#     return all_data


if __name__ == "__main__":
    db_name = "muldata"
    table_name = "data_summary"
    conn = create_db(db_name)
    conn.select_db(db_name)
    cursor = conn.cursor()

    # 获取到当前采集数据的总个数以及等长区间
    interval_list = data_per(40)
    interval_list_length = len(interval_list)

    # 创建相对的超级表模板
    print("a")
    stable_sql = f"CREATE STABLE IF NOT EXISTS data_analysis (`ts` TIMESTAMP,"
    stable_sql1 = f"CREATE STABLE IF NOT EXISTS data_collect (`ts` TIMESTAMP,"
    for i in range(interval_list_length):
        field_name1 = "inter%d_count" % (i + 1)
        field_name2 = "inter%d_per" % (i + 1)
        if i + 1 != interval_list_length:
            stable_sql += f"`{field_name1}` INT, `{field_name2}` FLOAT,"
        else:
            stable_sql += f"`{field_name1}` INT, `{field_name2}` FLOAT)"
    stable_sql += f"TAGS (`module` INT,`F_location` INT)"
    cursor.execute(stable_sql)
    print("成功创建数据统计分析超级表！")
    for i in range(10):
        field_name1 = "value%d_count" % (i + 1)
        if i + 1 != 10:
            stable_sql1 += f"`{field_name1}` INT,"
        else:
            stable_sql1 += f"`{field_name1}` INT)"
    stable_sql1 += f"TAGS (`module` INT,`ip` NCHAR(20),`DB_block` INT)"
    cursor.execute(stable_sql1)
    print("成功创建数据采集超级表！")

    Test(conn=conn, data_table_name=table_name,
         interval_list=interval_list, Time_inter=5)
    # num = data_count(table_name, db_name)
