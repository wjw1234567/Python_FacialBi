import pandas as pd
import random
from datetime import datetime, timedelta
from clickhouse_driver import Client

# --------------------------
# 1. 造数据
# --------------------------
random.seed(42)

num_groups = 50  # 一共造 50 批访客
base_time = datetime(2025, 3, 1, 10, 0, 0)  # 起始时间

data = []
person_counter = 1

for g in range(num_groups):
    # 随机决定组大小（1~7人，>5人算 "5人以上"）
    group_size = random.choice([1, 2, 3, 4, 5, 6, 7])

    # 每批之间的间隔（10~60秒）
    group_start = base_time + timedelta(seconds=random.randint(10, 60))
    base_time = group_start

    for i in range(group_size):
        # 同一组的人时间差控制在3秒内
        capture_time = group_start + timedelta(seconds=random.randint(0, 3))
        data.append((f"P{person_counter:04d}", capture_time))
        person_counter += 1

df = pd.DataFrame(data, columns=["person_id", "capture_time"]).sort_values("capture_time")

print(df.head(10))  # 看一下数据样子

# --------------------------
# 2. 写入 ClickHouse
# --------------------------
client =  Client(
            host='localhost',
            port='9000',
            user='default',
            password='ck_test',
            database='Facial'
        )  # ⚠️ 改成你的 ClickHouse 地址

# 建表（如果没有表就建一个）
client.execute('''
    CREATE TABLE IF NOT EXISTS visitors (
        person_id String,
        capture_time DateTime
    ) ENGINE = MergeTree()
    ORDER BY capture_time
''')

# 批量插入数据
client.execute(
    'INSERT INTO visitors (person_id, capture_time) VALUES',
    df.values.tolist()
)

print("数据写入完成！总行数:", len(df))
