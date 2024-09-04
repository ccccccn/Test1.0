# -*- coding:UTF-8 -*-
"""
 @Author: CNN
 @FileName: BidirectionQueue.py
 @DateTime: 2024/8/20 9:21
 @SoftWare: PyCharm
"""

from collections import deque


class BidirectionQueue:
    def __init__(self):
        self.queue = deque()

    def append_left(self, item):
        self.queue.appendleft(item)

    def append_right(self, item):
        self.queue.append(item)

    def pop_left(self):
        return self.queue.popleft()

    def pop_right(self):
        value = self.queue.pop()
        self.queue.appendleft(value)
        return value
