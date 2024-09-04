import datetime

import schedule
import taos
from snap7.client import Client, Area
import time
from CreateTableTest import create_tables, creat_sql
from CreateTableTest import create_db
from Test.allTest_intertable import data_analysis_job
from datadesrip.dataanalysis import data_per


# 连接PLC
def connec_PLC(conn, data_table_name, interval_list, Time_inter,db_name):  # 使用计数器整合数据列表
    plc_ip = '192.168.100.41'  # PLC的IP地址
    # 41.42.61.62.81.82
    plc_rack = 0  # 机架号，默认为0
    plc_slot = 1  # 插槽号，默认为1
    plc = Client()
    plc.connect(plc_ip, plc_rack, plc_slot)

    if plc.get_connected():
        print('PLC连接成功')
    else:
        print('PLC连接失败')
        exit()

    data_length = 20
    cursor = conn.cursor()

    """准备工作"""
    # 建立数据采集表单
    create_tables_sql = creat_sql(data_table_name, data_length)
    cursor.execute(create_tables_sql)
    print("成功建立数据采集表单")

    # 通过超级表批量建立多个变量表单
    table_sql = f"CREATE TABLE "
    for i in range(data_length):
        table_name = "module_%d" % (i + 1)
        table_sql += f"IF NOT EXISTS `{table_name}` USING data_analysis TAGs({i + 1},1) "
    conn.execute(table_sql)
    print("成功批量建立子表！")

    schedule.every(Time_inter).seconds.do(
        data_analysis_job, conn=conn, interval_list=interval_list, var_num=data_length,
        data_table_name=data_table_name, time_inter=Time_inter,db_name=db_name)
    try:
        while True:
            # 定时任务
            schedule.run_pending()
            # 读取PLC数据
            db_number = 41 # 数据块号
            start_address = 58  # 起始地址
            # data_length = 12  # 数据长度，即要读取的USInt数据个数
            data = plc.read_area(Area.DB, db_number, start_address, data_length)  # 一个USInt数据占用1个字节
            data_list = list(data)  # 每个字节对应一个USInt数据
            # 将读取的数据解析为USInt类型数据列表
            print('读取的数据为:', data_list)
            create_tables(data_table_name, conn, data_list,1)

            # 每次读取后休眠10ms
            time.sleep(0.0001)

    except KeyboardInterrupt:
        print("用户终止程序")

    # 断开PLC连接
    plc.disconnect()


if __name__ == "__main__":
    db_name = "plcdata"
    conn = create_db("plcdata")
    table_name = "data_summary"
    conn.select_db(db_name)
    cursor = conn.cursor()

    # 获取到当前采集数据的总个数以及等长区间
    interval_list = data_per(20)
    interval_list_length = len(interval_list)

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
    connec_PLC(conn, table_name, interval_list, 5,db_name)

    cursor.close()
    conn.close()
