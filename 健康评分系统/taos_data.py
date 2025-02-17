# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: taos_data.py
 @DateTime: 2024-12-16 10:22
 @SoftWare: PyCharm
"""
import itertools

from datadesrip.CreateTableTest import getconnect
from interanalysis_day import data_analysis, create_analysi

tag = ["积分电流x1y1x2y2z", "同步震动半径x1y1x2y2z", "同步电流x1y1x2y2z"]
strname_list = []
for j in range(1, 4):
    for i in range(1, 6):
        strname_list.append(f"MBC_1_{j}_{tag[j - 1]}_{i}")
print("组合前：", strname_list)
"""
str类型用于sql查询
list类型用于循环匹配
"""
strname_str = ",".join(strname_list) + ",FW_1_飞轮转速,FW_1_飞轮_TE"
strname_list.append("FW_1_飞轮转速")
strname_list.append("FW_1_飞轮_TE")
# print("str:", strname_str)
# print("list:", strname_list)
# print(len(strname_list))

suffix = itertools.cycle(["x1", "y1", "x2", "y2", "z"])
column_names_match_test = []
for column_name in strname_list[:-2]:
    column_names_match_test.append(column_name.split("_")[3][:-9] + f"_{next(suffix)}")
column_names_match_test.append(strname_list[-2])
column_names_match_test.append(strname_list[-1])
print("column_names_match_test:", column_names_match_test)

# ts数据接口封装
def taos_data_analysis(table_num):
    getconnect()
    #  获取单舱8飞轮各项数据
    table_name = ["flc_{}".format(j) for j in range(1, table_num)]
    print(table_name)
    create_analysi(table_num)
    data_analysis(strname_list, table_name)

if __name__ == '__main__':
    pass
    # getconnect()
    #  获取单舱8飞轮各项数据
    # table_name = ["flc_{}".format(j) for j in range(1, 2)]
    # # create_analysi(21)
    # data_analysis(strname_list, table_name)
