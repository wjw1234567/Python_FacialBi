

'''

def yield_example():
    yield 1
    yield 2
    yield 3

# generator = yield_example()
# print(next(generator))  # 输出 1
# print(next(generator))  # 输出 2
# print(next(generator))  # 输出 3

for i in yield_example():
    print(i)


'''



import site

print(site.getsitepackages())

