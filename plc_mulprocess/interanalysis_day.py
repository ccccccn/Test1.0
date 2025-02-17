# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: 区间分析.py
 @DateTime: 2024/9/13 18:31
 @SoftWare: PyCharm
"""
import math
import sched
import time
from datetime import datetime, timedelta

import setting
from datadesrip.CreateTableTest import getconnect, connect_mysql

scheduler = sched.scheduler(time.time, time.sleep)

inter_dir = setting.inter_dir
states = setting.states
tags = setting.tags


def create_analysi(*args):
    """
        :param args:0:飞轮舱总数
    """
    try:
        py_conn = connect_mysql('192.168.101.144', 'root', 'wkfl.777', 'test3')
    except Exception as e:
        print("MySQL数据连接失败")
        pass

    mysql_tables = ["flc_{}_fenxi".format(i) for i in range(1, args[0])]
    for mysql_table in mysql_tables:
        create_sql = (f"create table if not exists {mysql_table} (`time` DATETIME,"
                      f"`qujian` varchar(20),`tag` varchar(20),"
                      f"`p_x1` DECIMAL(4,3),`n_x1` BIGINT,"
                      f"`p_y1` DECIMAL(4,3),`n_y1` BIGINT,"
                      f"`p_x2` DECIMAL(4,3),`n_x2` BIGINT,"
                      f"`p_y2` DECIMAL(4,3),`n_y2` BIGINT,"
                      f"`p_z` DECIMAL(4,3),`n_z` BIGINT,"
                      f"`T_max` float,`R_max` float,`T_min` float,`R_min` float,`flc_no` int) "
                      f"engine = InnoDB DEFAULT CHARSET= utf8")
        pcur = py_conn.cursor()
        pcur.execute(create_sql)
        py_conn.commit()
    print("建表成功！")
    pcur.close()
    py_conn.close()


def data_analysis(column_names, table_name):
    # if now.minute == 0 and now.second == 0:
    try:
        taos_conn = getconnect()
        taos_conn.select_db("threadtest")
        pyconn = connect_mysql('192.168.101.144', 'root', 'wkfl.777', 'test3')
        pcur = pyconn.cursor()
    except Exception as e:
        print("涛思数据库连接失败")
        pass
    tcur = taos_conn.cursor()
    for sub_table in table_name:
        day_before = "2024-11-26 00:00:00.000"
        day_now = "2024-11-27 00:00:00.000"
        # day_now = datetime.now().strftime('%Y-%m-%d') + ' 00:00:00'
        # day_before = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d') + " 00:00:01"
        try:
            ## 缓存列名“积分电流_x1.....”
            if not column_names:
                tcur.execute(f"desc `{sub_table}`")
                tmp_column_name = [name[0] for name in tcur.fetchall()]
        except Exception as e:
            print(e)
            pass

        # finally:
        #     # print("taos执行结束！")
        # 获取各变量个数
        for num in range(1, 9):
            tmp_column_name = []
            for column_name in column_names:
                name_list = column_name.split("_")
                name_list[1] = str(num)
                column_name = '_'.join(name_list)
                tmp_column_name.append(column_name)
            tcur.execute(
                f"select count(*) from `{sub_table}` WHERE ts BETWEEN '{day_before}' AND '{day_now}' and `{column_names[15]}` > 4800")
            cnt = tcur.fetchall()[0][0]
            all_query = []
            for state in states:
                for i, tag in zip(range(0, len(tmp_column_name) - 3, 5), tags):
                    query = f"select '{day_before.split(' ')[0]}' as `ts`,'{state}' as `qujian`,'{tag}' as `tag`,"
                    condition = []
                    conditions = []
                    if state == '故障' and tag == '积分电流':
                        continue
                    tmp_list = inter_dir.get(state)
                    tmp_i = i
                    idx = math.ceil((tmp_i - 4) / 5)
                    tmp_inter = tmp_list[idx]
                    ## 设置暂时变量，防止全局变量被修改

                    if tag == '积分电流':
                        """
                        数据采集需求
                        """
                        for idx, (low, high) in enumerate(tmp_inter):
                            suffix = f"{tmp_column_name[i]}".split("_")[-1]
                            conditions.append(f"abs(`{tmp_column_name[i]}`) between {low} and {high}")
                            condition.append(
                                f"(CAST(count(case when abs(`{tmp_column_name[i]}`) between {low} and {high} "
                                f"then `{tmp_column_name[i]}` end) as float)/{cnt}) as `{suffix}_per`,"
                                f"count(case when abs(`{tmp_column_name[i]}`) between {low} and {high} "
                                f"then `{tmp_column_name[i]}` end) as `{suffix}_num`")
                            i += 1
                        query = (query + " , ".join(condition) +
                                 f",MAX(`{tmp_column_name[15]}`) as `tempetature_max`,MAX(`{tmp_column_name[16]}`) as `rotate_max`,"
                                 f"MIN(`{tmp_column_name[15]}`) as `tempetature_min`,MIN(`{tmp_column_name[16]}`) as `rotate_min`"
                                 f" from threadtest.`{sub_table}` "
                                 f"where ts BETWEEN '{day_before}' and '{day_now}' and `{tmp_column_name[15]}` > 4800 and (" + " or ".join(
                                    conditions) + ")")
                        all_query.append(query)
                    else:
                        for idx, (low, high) in enumerate(tmp_inter):
                            suffix = f"{tmp_column_name[i]}".split("_")[-1]
                            condition.append(
                                f"(CAST(count(case when `{tmp_column_name[i]}` between {low} and {high} "
                                f"then `{tmp_column_name[i]}` end) as float)/{cnt}) as `{suffix}_per`,"
                                f"count(case when abs(`{tmp_column_name[i]}`) between {low} and {high} "
                                f"then `{tmp_column_name[i]}` end) as `{suffix}_num`")
                            i += 1
                        query = (query + " , ".join(condition) +
                                 f",MAX(`{tmp_column_name[15]}`) as `tempetature_max`,MAX(`{tmp_column_name[16]}`) as `rotate_max`,"
                                 f"MIN(`{tmp_column_name[15]}`) as `tempetature_min`,MIN(`{tmp_column_name[16]}`) as `rotate_min`"
                                 f" from threadtest.`{sub_table}` "
                                 f"where ts BETWEEN '{day_before}' and '{day_now}' and `{tmp_column_name[15]}` > 4800")
                        all_query.append(query)
                    # print("当前查询语句为:", query)
                    tcur.execute(query)
                    data = tcur.fetchall()
                    # print("当前查询出的数据为：", data, type(data[0]))
            print('------------------------------------------------------')
            ex_query = " UNION ALL ".join(
                query for query in all_query
            )
            # print("当前整合查询语句为：", ex_query)
            tcur.execute(ex_query)
            data = tcur.fetchall()
            sort_data = sorted(data, key=lambda x: x[1])
            # TODO:aaaaaaaaaaaaa
            update_data = [tuple(x if x is not None else 0 for x in item) for item in sort_data]
            update_data = [tuple(list(t) + [num]) for t in update_data]
            # print("当前查询出的数据为：", update_data)
            Inser_table = f"{sub_table}"+"_fenxi"
            pcur.execute(f"desc `{Inser_table}`")
            description = pcur.fetchall()
            Insert_column_names = ([desc[0] for desc in description])
            formatted_list = ', '.join(Insert_column_names)
            colunm_places = ",".join(['%s'] * len(Insert_column_names))
            # print("列名为,", formatted_list)
            # for data in update_data:
            Insert_sql = f"INSERT INTO {Inser_table} ({formatted_list}) VALUES ({colunm_places})"
            # print("当前插入语句为：", Insert_sql)
            pcur.executemany(Insert_sql, update_data)
            pyconn.commit()
            print(f"成功执行 ----{num}")
    taos_conn.close()
    tcur.close()
    pcur.close()
    pyconn.close()

    # else:
    #     next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    #     delay = (next_hour - now).total_seconds()
    #     scheduler.enter(delay, 1, data_analysis)


if __name__ == "__main__":
    taos_conn = getconnect()
    # TODO:修改数据库名
    taos_conn.select_db("threadtest")
    tcur = taos_conn.cursor()
    tcur.execute("show tables")
    table_name = [name[0] for name in tcur.fetchall()]

    create_analysi(21)
    # data_analysis()
