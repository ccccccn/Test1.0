import json
import random
import time
from datetime import datetime

import numpy as np
import schedula

from data_analysis.interval_table import data_analysis_job
from datadesrip.CreateTableTest import create_db, create_tables, creat_sql, connect_mysql
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
            count, per = schedula.every(10).minute.do(data_analysis_job(conn, interval_list, module_length))
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

def cost_time(func):
    def fun(*args, **kwargs):
        t = time.perf_counter()
        result = func(*args, **kwargs)
        print(f"func{func.__name__} cost time :{time.perf_counter()-t:.8f}s")
        return result
    return fun

# @cost_time
# def initial_arr():
soc_value = np.zeros(20)
frequency_value = np.zeros(20)
duration_value = np.zeros(20)
soc_bins = np.linspace(-100,100,21)


    # soc_value = np.array([random.randint(-100, 100) for _ in range(20)])
    # frequency_value = np.array([random.randint(-100, 100) for _ in range(20)])
    # duration_value = np.array([random.randint(-100, 100) for _ in range(20)])
    # soc_bins = np.arange(-100, 101, 20)
def send_to_redis_channel(channel_name, CABIN_NUM=20):
    random_idx_arr = [random.randint(0, 19) for _ in range(3)]
    random_value_arr = [random.randint(-100, 100) for _ in range(3)]

    for idx,value in zip(random_idx_arr,random_value_arr):
        soc_value[idx] = value
        frequency_value[idx] = value
        duration_value[idx] = value

    soc_hist, bin_edges = np.histogram(soc_value, bins=soc_bins)
    frequency_hist, bin_edges = np.histogram(frequency_value, bins=soc_bins)
    duration_hist, bin_edges = np.histogram(duration_value, bins=soc_bins)
    # soc_hist_json_str = json.dumps((soc_hist / CABIN_NUM).tolist())
    # soc_hist_json = json.loads(soc_hist_json_str)
    # fre_hist_json_str = json.dumps((frequency_hist / CABIN_NUM).tolist())
    # fre_hist_json = json.loads(fre_hist_json_str)
    # dur_hist_json_str = json.dumps((duration_hist / CABIN_NUM).tolist())
    # dur_hist_json = json.loads(dur_hist_json_str)
    data = {
        "title": "pie_info",
        "message": {
            "soc": (soc_hist/CABIN_NUM).tolist(),
            "frequency":(frequency_hist/CABIN_NUM).tolist(),
            "duration": (duration_hist/CABIN_NUM).tolist(),
        }
    }
    """
    向 Redis 的指定 channel 发送数据。
    :param channel_name: Redis channel 的名称
    :param data: 要发送的数据（通常是字典或字符串）
    """
    try:
        # 如果数据是字典，可以将其转换为 JSON 格式
        if isinstance(data, dict):
            data = json.dumps(data)

        # 发送数据到指定的 channel
        r.publish(channel_name, data)
        print(f"Data sent to channel '{channel_name}': {data}")
        # 设置退出条件
    except Exception as e:
        print(f"Error sending data to Redis channel: {e}")

if __name__ == "__main__":
    # conn = create_db("muldata")
    # table_name = "data_summary"
    # interval_list = [[0, 40], [40, 80], [80, 160], [160, 320]]
    # # Test(conn=conn, data_table_name=table_name, interval_list=interval_list)
    # conn.close()
    #
    # day = datetime.now()
    #
    # print("day",day)
    # sql_conn = connect_mysql('192.168.101.144', 'root', 'wkfl.777', 'test3')
    # cursor = sql_conn.cursor()
    # for i in range(1, 21):
    #     del_sql = f"drop table IF EXISTS flc_{i}_trend_fenxi"
    #     cursor.execute(del_sql)
    # sql_conn.commit()
    # print("Done!")
    # cursor.close()
    # sql_conn.close()
    initial_arr()
