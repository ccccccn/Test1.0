# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: 区间分析.py
 @DateTime: 2024/9/13 18:31
 @SoftWare: PyCharm
"""
import sched
import time
from datetime import datetime, timedelta

from datadesrip.CreateTableTest import getconnect, connect_mysql

scheduler = sched.scheduler(time.time, time.sleep)
INF = 1000


def create_analysi():
    try:
        py_conn = connect_mysql('192.168.111.93','testt3')
    except Exception as e:
        print("涛思数据库连接失败")
        pass

    mysql_tables = ["flc_1_mbc_{}_fenxi".format(i) for i in range(1, 9)]
    for mysql_table in mysql_tables:
        create_sql = (f"create table if not exists {mysql_table} (`time` DATETIME,"
                      f"`qujian` varchar(20),`tag` varchar(20),"
                      f"`p_x1` DECIMAL(4,3),`p_y1` DECIMAL(4,3),`p_x2` DECIMAL(4,3),`p_y2` DECIMAL(4,3),`p_z` DECIMAL(4,3),"
                      f"`T_max` float,`R_max` float,`T_min` float,`R_min` float) engine = InnoDB DEFAULT CHARSET= utf8mb4")
        pcur = py_conn.cursor()
        pcur.execute(create_sql)
    # names = []
    # str_list = ['x1', 'x2', 'y1', 'y2', 'z']
    # su_ffix = ['maxt', 'mint', 'maxr', 'minr']
    # for stt in str_list:
    #     for suf in su_ffix:
    #         names.append(stt + '_' + suf)
    # create_sql = (f"create table if not exists flc_mbc_data (`ts` timestamp,"
    #               f"`qujian` nchar(20),`tag` nchar(20),`p_x1` float,`p_y1` float,`p_x2` float,`p_y2` float,`p_z` float,"
    #               f"`T_max` float,`T_min` float,`R_max` float,`R_min` float")
    print("建表成功！")
    pcur.close()
    py_conn.close()

def data_analysis():
    now = datetime.now()
    tmp_column_name = []
    # if now.minute == 0 and now.second == 0:
    try:
        taos_conn = getconnect()
        taos_conn.select_db("threadtestt4")
        pyconn = connect_mysql('192.168.111.93','testt3')
        pcur = pyconn.cursor()
    except Exception as e:
        print("涛思数据库连接失败")
        pass
    tcur = taos_conn.cursor()
    for sub_table in table_name:
        for startt in range(10,11):
            new_start = str(startt).rjust(2, '0')
            new_end = str(startt + 1).rjust(2, '0')
            day = (datetime.now()+timedelta(days=-1)).strftime('%Y-%m-%d')
            start = f'{day} {new_start}:00:01'
            end = f'{day} {new_end}:00:00'
            if startt == 23:
                startt = -1
                date = datetime.strptime(day, '%Y-%m-%d')
                end = f'{date + timedelta(days=1)}'
            try:
                if not tmp_column_name:
                    tcur.execute(f"desc `{sub_table}`")
                    tmp_column_name = [name[0] for name in tcur.fetchall()]
            except Exception as e:
                print(e)
                pass
            # finally:
            #     # print("taos执行结束！")
            # 获取各变量个数
            tcur.execute(f"select count(*) from `{sub_table}` WHERE ts BETWEEN '{start}' AND '{end}'")
            cnt = tcur.fetchall()[0][0]
            # 设置各区间值,用字典找区间
            inter_dir = {"健康": [[(-1.8, 1.8), (-1.8, 1.8), (-1.1, 1.1), (-1.1, 1.1), (-1.5, 2.9)],
                                  [(0, 2), (0, 2), (0, 1.5), (0, 1.5), (0, 0.5)],
                                  [(0, 3.2), (0, 3.2), (0, 3.2), (0, 3.2), (0, 0.5)]],  # 积分电流，同步震动半径，同步电流的健康区间
                         "预警": [[(1.8, INF), (1.8, INF), (1.8, INF), (1.8, INF), (2.9, INF)],
                                  [(3.2, 4), (3.2, 4), (3.2, 4), (3.2, 4), (0.5, 1)],
                                  [(2.0, 2.9), (2.9, 2.9), (2.0, 2.9), (2.0, 2.9), (0.5, 2.9)]],
                         # 积分电流，同步震动半径，同步电流的预警区间
                         "故障": [[(3.2, INF), (3.2, INF), (3.2, INF), (3.2, INF), (3.2, INF)],
                                  [(2.9, INF), (2.9, INF), (2.9, INF), (2.9, INF), (2.9, INF)],
                                  [(4, INF), (4, INF), (4, INF), (4, INF), (1, INF)]]}  # 积分电流，同步震动半径，同步电流的故障区间

            # 统计分析历史数据
            # TODO:修改状态与名称
            states = ['健康', '预警', '故障']
            tags = ['积分电流', '震动半径', '同步电流']
            all_query = []
            for state in states:
                for i, tag in zip(range(4, len(tmp_column_name) - 3, 5), tags):
                    query = (f"select '{start}' as `ts`,'{state}' as `qujian`,'{tag}' as `tag`,"
                             f"(CAST(count(`{tmp_column_name[i]}`) AS float)/{cnt}) as `x1_per`,"
                             f"(CAST(count(`{tmp_column_name[i + 1]}`) AS float)/{cnt}) as `y1_per`, "
                             f"(CAST(count(`{tmp_column_name[i + 2]}`) AS float)/{cnt}) as `x2_per`,"
                             f"(CAST(count(`{tmp_column_name[i + 3]}`) AS float)/{cnt}) as `y2_per`,"
                             f"(CAST(count(`{tmp_column_name[i + 4]}`) AS float)/{cnt})as `z1_per`,"
                             f"MAX(`飞轮舱温度`) as `tempetature_max`,MAX(`飞轮转速`) as `rotate_max`,"
                             f"MIN(`飞轮舱温度`) as `tempetature_min`,MIN(`飞轮转速`) as `rotate_min`"
                             f" from threadtestt4.`{sub_table}` "
                             f"where ts BETWEEN '{start}' and '{end}' ")
                    condition = []
                    if state == '故障' and tag == '积分电流':
                        continue
                    tmp_list = inter_dir.get(state)
                    idx = int((i - 4) / 5)
                    tmp_inter = tmp_list[idx]
                    tmp_i = i - 4
                    if state == '积分电流':
                        for idx, (low, high) in enumerate(tmp_inter):
                            condition.append(f"ABS(`{tmp_column_name[tmp_i + 4]}`) between {low} and {high}")
                            tmp_i += 1
                        query = query + "and " + " and ".join(condition) + " and `飞轮转速` > 4800"
                        all_query.append(query)
                    else:
                        for idx, (low, high) in enumerate(tmp_inter):
                            condition.append(f"`{tmp_column_name[tmp_i + 4]}` between {low} and {high}")
                            tmp_i += 1
                        query = query + "and " + " and ".join(condition) + " and `飞轮转速` > 4800"
                        all_query.append(query)
                    # print("当前查询语句为:", query)
                    # tcur.execute(query)
                    # data = tcur.fetchall()
                    # print("当前查询出的数据为：",data)
            print('------------------------------------------------------')
            ex_query = " UNION ALL ".join(
                query for query in all_query
            )
            print("当前整合查询语句为：", ex_query)
            tcur.execute(ex_query)
            data = tcur.fetchall()
            sort_data = sorted(data, key=lambda x: x[1])
            # TODO:aaaaaaaaaaaaa
            update_data = [tuple(x if x is not None else 0 for x in item) for item in sort_data]
            # print("当前查询出的数据为：", update_data)
            Inser_table = "flc_1_mbc_" + sub_table.split("_")[2][3:] + "_fenxi"
            pcur.execute(f"desc `{Inser_table}`")
            description = pcur.fetchall()
            column_names = ([desc[0] for desc in description])
            formatted_list = ', '.join(column_names)
            colunm_places = ",".join(['%s'] * len(column_names))
            # print("列名为,", formatted_list)
            # for data in update_data:
            Insert_sql = f"INSERT INTO {Inser_table} ({formatted_list}) VALUES ({colunm_places})"
            print("当前插入语句为：", Insert_sql)
            pcur.executemany(Insert_sql,update_data)
            pyconn.commit()
            # print(f"成功执行 ----{startt}")
    pcur.close()
    pyconn.close()

    # else:
    #     next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    #     delay = (next_hour - now).total_seconds()
    #     scheduler.enter(delay, 1, data_analysis)


if __name__ == "__main__":
    taos_conn = getconnect()
    # TODO:修改数据库名
    taos_conn.select_db("threadtestt4")
    tcur = taos_conn.cursor()
    tcur.execute("show tables")
    table_name = [name[0] for name in tcur.fetchall()]
    print("connnnn")

    create_analysi()
    data_analysis()
