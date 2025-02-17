import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime
from multiprocessing import freeze_support
import asyncio

from datadesrip.CreateTableTest import create_db, create_tables, getconnect, connect_mysql
from plc_mulprocess.BidirectionQueue import BidirectionQueue
from plc_mulprocess.data_name import DataName, DataName_en

# 定义线程局部变量实现函数嵌套任务
lock = threading.Lock()
lockk = asyncio.Lock()
import asyncio

# 新建全局队列，存储暂时数据
Data_queue = BidirectionQueue()

# 将输入输出重定向到日志记录中
# sys.stdout = StreamToLogger(logging.getLogger('STDOUT'), logging.INFO)
# sys.stderr = StreamToLogger(logging.getLogger('STDERR'), logging.ERROR)
DB_NAME = "threadtest"


def get_plc_data(selected_ip, **kwargs):
    conn = getconnect()
    conn.select_db(DB_NAME)
    cursor = conn.cursor()
    plc_ip = kwargs['ip']
    plc_rack = kwargs['rack']
    plc_slot = kwargs['slot']
    dbs = kwargs['dbs']
    rt_dbs = kwargs['rt_dbs']
    starts = kwargs['starts']
    rt_starts = kwargs['rt_starts']
    lengths = kwargs['lengths']

    # try:
    #     plc = Client()
    #     plc.connect(plc_ip, plc_rack, plc_slot)
    # except Exception as e:
    #     print("当前错误为：", e)
    # if plc.get_connected():
    #     print(f'PLC {plc_ip} 连接成功')
    # #
    while True:
        count = 1
        try:
            for db, rt_db, start, rt_start, length in zip(dbs, rt_dbs, starts, rt_starts, lengths):
                Sys_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                data_list = []
                # data_list.append(Sys_time)
                # data = plc.read_area(Area.DB, db, start, 76)
                # rt_data = plc.read_area(Area.DB, rt_db, rt_start, 10)
                # # 根据DB块直接获取整段数据
                # for i in range(3):
                #     start = start + i * 28
                #     for j in range(5):
                #         value = util.get_real(data, j * 4)
                #         data_list.append(value)
                # temperature = util.get_real(rt_data, 0)
                # rotate = util.get_int(rt_data, 8)
                # data_list.extend(temperature, rotate)
                for _ in range(17):
                    data = random.randint(0,255)
                    data_list.append(data)
                if plc_ip in selected_ip:
                    plc_ip_name = plc_ip.split('.')[-1]
                    data_table_name = f'plc{plc_ip_name}_MBC{count}'
                    count += 1
                    with lock:
                        # # 减少连接可能
                        create_tables(data_table_name, conn, data_list)
                        print(f'PLC {plc_ip} DB{db}\t\t读取的数据为:{data_list}\t\t当前时间为:{Sys_time} \n', end='')
            asyncio.sleep(0.01)  # 每次读取后休眠 1 毫秒
        except Exception as e:
            print(f"采集错误: {e}\n", end=' ')


def run_data_collection(plcs, selected_ip):
    with ThreadPoolExecutor(max_workers=200) as exe:
        futures = {exe.submit(get_plc_data, selected_ip, **plc): plc for plc in plcs}
        Start_time = datetime.now().microsecond
        if as_completed(futures):
            end_time = datetime.now().microsecond
            print("本次采集所用：{} \n".format(end_time - Start_time), end='')
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                Type = type(e)
                print(f"线程执行错误：{e},{Type}")


def batcr_table(plcs, select_ip, select_db):
    dq = BidirectionQueue()
    dq_en = BidirectionQueue()
    for varname, en_varname in zip(DataName, DataName_en):
        dq.append_left(varname.name)
        dq_en.append_left(en_varname.name)
    length = plcs[0].get('lengths')

    for plc in plcs:
        value_list = []
        for key, value in plc.items():
            value_list.append(value)
        plc_ip = value_list[0]
        plc_ip_suffix = value_list[0].split('.')[-1].lower()
        # db_name_iter = iter(value_list[3])
        for i in range(len(value_list[3])):
            # db_name = next(db_name_iter)
            creat_sql = (f"CREATE TABLE IF NOT EXISTS `plc{plc_ip_suffix}_MBC{i + 1}` "
                         f"USING data_collect_DATA tags ('{plc_ip}',{i + 1})")
            cursor.execute(creat_sql)
    print("{}采集子表已建立完成".format(plc_ip))


if __name__ == '__main__':
    print("aaaa")
    conn = create_db(DB_NAME)
    print("aaaaaa")
    conn.select_db(DB_NAME)
    cursor = conn.cursor()
    try:
        sql_conn = connect_mysql()
        my_cur = sql_conn.cursor()
    except Exception as e:
        print("Mysql连接失败：", e)
    print("Connect!")

    # 获取到当前采集数据的总个数以及等长区间
    base_config = {
        'rack': 0, 'slot': 1,
        'dbs': [41] * 8,
        'rt_dbs': [8] * 8,
        'starts': [58, 584, 1110, 1636, 2162, 2688, 3214, 3740],
        'rt_starts': [16, 88, 160, 232, 304, 376, 448, 520],
        'lengths': [15] * 8

    }
    # 辅助列表，包含所有需要的IP地址
    ip_addresses = ['192.99.7.22']
    # 使用列表生成式创建最终的PLC列表
    collect_plcs = [{'ip': ip, **base_config} for ip in ip_addresses]
    # 创建相对的超级表模板
    stable_sql1 = f"CREATE STABLE IF NOT EXISTS data_collect_DATA (`ts` TIMESTAMP ,"
    length = collect_plcs[0].get('lengths')

    # 方案一：有规律采用超级表，创建数据采集超级表
    stable_sql1 += ",".join(f"`{field_name1.name}` float" for field_name1 in DataName)
    stable_sql1 += f") TAGS (`ip` NCHAR(20),`MBC_id` INT)"
    cursor.execute(stable_sql1)
    print("成功创建数据采集超级表！")

    # 方案二：无规律采用直接建表
    select_ip = [plc['ip'] for plc in collect_plcs]
    select_plcs = [plc for plc in collect_plcs if plc['ip'] in select_ip]
    batcr_table(plcs=collect_plcs, select_ip=select_plcs, select_db=None)
    print("所有统计表单于采集表单已建立完成")

    freeze_support()
    try:
        run_data_collection(collect_plcs, selected_ip=ip_addresses)
    except KeyboardInterrupt:
        print("采集任务已终止")
    except Exception as e:
        print(e)
