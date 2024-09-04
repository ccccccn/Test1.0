import datetime

import numpy as np
import schedula
import taos
from datadesrip.CreateTableTest import create_db
from datadesrip.dataanalysis import data_count, data_per
from Test.allTest import Test


def data_analysis_job(conn, interval_list):
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


if __name__ == '__main__':
    db_name = "cou_pertest"
    data_table_name = "data_summary"
    conn = create_db(db_name)
    conn.select_db(db_name)
    cursor = conn.cursor()

    Test(conn=conn, table_name=data_table_name)

    # 获取到当前采集数据的总个数以及等长区间
    num = data_count(data_table_name, db_name)
    interval_list = data_per(40)
    interval_list_length = len(interval_list)

    # 获取所得到的变量个数
    cursor.execute(f"describe {data_table_name}")
    var_num = len(cursor.fetchall()) - 1

    # 创建相对的超级表模板
    stable_sql = f"CREATE STABLE IF NOT EXISTS data_analysis (`ts` TIMESTAMP,"
    for i in range(interval_list_length):
        field_name1 = "inter%d_count" % (i + 1)
        field_name2 = "inter%d_per" % (i + 1)
        if i + 1 != interval_list_length:
            stable_sql += f"`{field_name1}` INT, `{field_name2}` FLOAT,"
        else:
            stable_sql += f"`{field_name1}` INT, `{field_name2}` FLOAT)"
    stable_sql += f"TAGS (`module` INT,`F_location` INT)"
    cursor.execute(stable_sql)
    print("成功创建超级表！")

    # 通过超级表批量建立多个变量表单
    table_sql = f"CREATE TABLE "
    for i in range(var_num):
        table_name = "module_%d" % (i + 1)
        table_sql += f"IF NOT EXISTS `{table_name}` USING data_analysis TAGs({i + 1},1) "
    conn.execute(table_sql)
    print("成功批量建立子表！")

    # 获取所需数据的区间个数
    data_dict = {}
    interval_count = np.zeros(len(interval_list))
    for i in range(var_num):
        var_name = "value%d" % (i + 1)
        var_sql = f"select {var_name} from `{data_table_name}`"
        cursor.execute(var_sql)
        value_count = [t[0] for t in cursor.fetchall()]
        print("当前模块的值为：", value_count)

    # 获取总体数据统计量以及数据占比
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
        # print("module%d的分布情况为：" % (j + 1), var_count)
        # print("module%d的占比情况为：" % (j + 1), var_per)
    # print("总体数据统计量为：", all_count)
    # print("总体数据占比量为：", all_per)

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
    # schedula.every(10).seconds.do()

    cursor.close()
    conn.close()
