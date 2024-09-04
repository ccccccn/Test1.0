# from datadesrip.CreateTableTest import create_db
#
# db_name = "threadTestt"
# conn = create_db(db_name)
# conn.select_db(db_name)
#
# cursor = conn.cursor()
# sql = f'select tbname from {db_name}.data_collect'
# cursor.execute(sql)
# tbname = list(set(cursor.fetchall()))
# for i in tbname:
#     print(i)
#
# sql1 = f'show tables'
# cursor.execute(sql1)
# tbls = cursor.fetchall()
# print(tbls)
# tb_analysis = list(set(tbls)-set(tbname))
# print(tb_analysis)
#
# varname_field = [varname[0] for varname in [item for item in cursor.fetchall_row()[1:-2]]]
# print(varname_field)
# from plc_mulprocess.BidirectionQueue import BidirectionQueue
# # 测试yield挂起枚举类成员
#
# from plc_mulprocess.data_name import DataName
# from collections import deque
#
# dq = BidirectionQueue()
# number = [1, 2, 3, 4, 5, 6, 7, 8, 9]
# for varname in DataName:
#     dq.append_left(varname.value)
#
# print(dq)
# for index in range(len(number)):
#     print("-----这是第{}次-----:".format(index + 1), end='\t\t')
#     for num in range(number[index]):
#         print(dq.pop_right(),end=',')
#     print()