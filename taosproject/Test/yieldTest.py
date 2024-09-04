# def base_code_pool():
#     for i in range(3):
#         yield 'BASE-%s' % str(i + 1)
#
#
# def outer_code_pool():
#     for i in range(30):
#         yield 'OUTERBASE-%s' % str(i+1)
#
#
# def team_code_pool():
#     """将内部使用结束"""
#     yield from base_code_pool()
#     print("内部使用结束")
#     yield from outer_code_pool()
#
#
# if __name__ == '__main__':
#     gen = base_code_pool()
#     # print(next(gen))
#     # print(next(gen))
#     # print(next(gen))
#     team_member = team_code_pool()
#     print(team_member.__next__())
#     print(list(team_member))
# import collections
#
# from tqdm import tqdm
#
# print("当前进度：", end='')
# total_iterations = 10000000
# progress_bar = tqdm(total=total_iterations)
# for i in range(total_iterations):
#     progress_bar.update(1)
# progress_bar.close()
#
# # 计数器多现成应用
# collections.Counter()


a = [1, 2, 3, 4, 5, 6, 7, 8, 9]
b = [11, 12, 13, 14, 15, 16, 17, 18, 19]

for item1,item2 in zip(a,b):
    print(item1,item2)
