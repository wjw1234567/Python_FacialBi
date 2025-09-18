
from datetime import datetime


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

# print(site.getsitepackages())

'''
[(1, 'Region_1'), (2, 'Region_2'), (3, 'Region_3'), (4, 'Region_4'), (5, 'Region_5'), (6, 'Region_6'), (7, 'Region_7'), (8, 'Region_8'), (9, 'Region_9'), (10, 'Region_10'), (11, 'Region_11'), (12, 'Region_12'), (13, 'Region_13'), (14, 'Region_14'), (15, 'Region_15')]

'''

region_count=15
regions = [(i, f"Region_{i}") for i in range(1, region_count + 1)]

regions=[(1,'Zone P1A'),(2,'Zone P1B'),(3,'Zone P1C'),(4,'Zone P2A'),(5,'Zone P2B'),(6,'Zone P2C'),
(7,'Zone P1D'),(8,'Zone P1E'),(9,'Zone P1F'),(10,'Zone P2D'),(11,'Zone P2E'),(12,'Zone P2F'),(3,'Zone P2G')]

# print(regions)

print(datetime.now())


diff_min = (datetime.strptime("2028-08-25 15:30:34", "%Y-%m-%d %H:%M:%S")-datetime.strptime("2028-08-25 15:48:21", "%Y-%m-%d %H:%M:%S")).total_seconds() / 60
print(diff_min)
print(((7 // 3 +1) ) )


if diff_min >=0 and diff_min <=15:
    off_bin=(int(diff_min // 5)+1) * 5
elif diff_min >=-15 and diff_min <=-1:
    off_bin = (int(diff_min // 5) ) * 5
elif diff_min >=16 and diff_min <=60:
    off_bin = (int(diff_min // 15) + 1) * 15
elif diff_min >=-60 and diff_min <=-16:
    off_bin = (int(diff_min // 15) ) * 15

# print(str(1))


nested_dict = {-60: {'region_name': 'Zone P1A', 'diff_min': -45.43333333333333}
             , -45: {'region_name': 'Zone P1E', 'diff_min': -36.75}
             , 5: {'region_name': 'Zone P2G', 'diff_min': 4.6}}

for main_key, sub_dict in nested_dict.items():
    region = sub_dict['region_name']
    diff = sub_dict['diff_min']
    print(f"主键 {main_key}: 区域={region}, 差值={diff}")



# print([age for age in range(20,75)])


cam_dict={1: {"last_group_id": 'AA', "last_time": 'AAA'},2:{"last_group_id": 'BB', "last_time": 'BBB'}}
cam_dict.get(0)

