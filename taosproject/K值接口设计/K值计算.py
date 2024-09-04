# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: K值计算.py
 @DateTime: 2024/8/27 9:40
 @SoftWare: PyCharm
"""
import math


def cal_K1(Td) -> float:
    Tnd = 1
    K1 = 2 - Td / Tnd
    return K1


def cal_K2(f, T, P1, P2, P3, P4) -> float:
    Vni = float(input("请输入标准功率："))
    P0 = (2 * math.pi * f) / T
    Vij = ((P1 - P0) + (P2 - P0) / 2 + (P3 - P0) / 3 + (P4 - P0) / 4) / 4
    K2 = 2 - abs((Vni - Vij) / Vni) - abs((Vni - Vij) / Vni)
    return K2, P0


def cal_K3(P0,Pij):
    K3 = 2 - 2*abs((Pij-P0)/P0)
    return K3
