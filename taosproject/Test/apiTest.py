# 装饰器算子测试
import math


def showname(func):
    def subfunc(*args, **kwarg):
        print("FUNCTION {} called.".format(func.__name__))
        return func(*args, **kwarg)

    return subfunc


@showname
def pyrint(data="Python"):
    return data.upper()


# 迭代器
import itertools

p = itertools.count(start=1, step=0.5)
print(p.__next__(), p.__next__())

p = itertools.cycle(list("AB"))
print(next(p), next(p))


# 生成器
def iterFib():
    n, former, later = 0, 0, 1
    while True:
        yield later
        former, later = later, former + later


print(list(itertools.islice(iterFib(), 0, 10)))


# 构造函数
class Myclass:
    def __init__(self, animal):
        self.animal = animal


a = Myclass("dog")
print(a.animal)


# __dir__属性使用
class Myclass3:
    def __init__(self):
        self.cat = "MEWMEW"
        self.dog = "Mwoof"

    __slot__ = ("mewmew", "woof")


a = Myclass3()
print(Myclass3.__dict__)

from dataclasses import dataclass


@dataclass
class Person:
    name: str
    id: int


interval_list = []
step = 20
inter = itertools.count(start=0, step=step)
count = math.ceil(100 / step)
for i in range(count):
    start = next(inter)
    interval_list.append([start, start + step])
for item in interval_list:
    print("[%d,%d)"%(item[0], item[1]))
