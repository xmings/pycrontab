#-*- coding:utf-8 -*-

def cal(x):
    return x*x

result = 0
for i in range(100):
    result += cal(i)
print(result)