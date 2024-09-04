# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: data_name.py
 @DateTime: 2024/8/19 9:39
 @SoftWare: PyCharm
"""

from enum import Enum, unique


@unique
class DataName(Enum):
    积分电流_x1 = 1
    积分电流_y1 = 2
    积分电流_x2 = 3
    积分电流_y2 = 4
    积分电流_z = 5
    同步震动半径_x1 = 6
    同步震动半径_y1 = 7
    同步震动半径_x2 = 8
    同步震动半径_y2 = 9
    同步震动半径_z = 10
    同步电流_x1 = 11
    同步电流_y1 = 12
    同步电流_x2 = 13
    同步电流_y2 = 14
    同步电流_z = 15
    # Y_X1 = 16
    # Y_Y1 = 17
    # Y_X2 = 18
    # Y_Y2 = 19
    # Y_Z = 20

@unique
class DataName_en(Enum):
    Int_current_x1 = 1
    Int_current_y1 = 2
    Int_current_x2 = 3
    Int_current_y2 = 4
    Int_current_z = 5
    Syn_vib_radius_x1 = 6
    Syn_vib_radius_y1 = 7
    Syn_vib_radius_x2 = 8
    Syn_vib_radius_y2 = 9
    Syn_vib_radius_z = 10
    Syn_current_x1 = 11
    Syn_current_y1 = 12
    Syn_current_x2 = 13
    Syn_current_y2 = 14
    Syn_current_z = 15
    # Y_X1 = 16
    # Y_Y1 = 17
    # Y_X2 = 18
    # Y_Y2 = 19
    # Y_Z = 20