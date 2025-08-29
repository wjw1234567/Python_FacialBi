import random
import datetime
from clickhouse_driver import Client
# from datetime import datetime

# 连接 ClickHouse
client = Client(
    host='localhost',
    port=9000,
    user='default',
    password='ck_test',
    database='Facial',
    send_receive_timeout=60   # 避免写入超时
)

# 表名
table_name = "dwd_user_capture_heatmap"



client.execute(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    profile_id String,
    region_id  String,
    region_name String,
    capture_time DateTime,
    age  Int16,
    member_tier String,
    gender Int16 ,
    batch_time DateTime  
) ENGINE = MergeTree()
ORDER BY (capture_time, profile_id)
""")



# 商场区域配置
# region_count = 15
# regions = [(i, f"Region_{i}") for i in range(1, region_count + 1)]
regions = [('1','Zone P1A'),('2','Zone P1B'),('3','Zone P1C'),('4','Zone P2A'),('5','Zone P2B'),('6','Zone P2C'),
('7','Zone P1D'),('8','Zone P1E'),('9','Zone P1F'),('10','Zone P2D'),('11','Zone P2E'),('12','Zone P2F'),('3','Zone P2G')]

age_range=[age for age in range(20,75)]
member_tier_range=['Basic', 'Silver' ,'Gold' , 'Diamond']
gender_range=[0,1]


# 生成日期范围
start_date = datetime.date(2025, 8, 20)
end_date = datetime.date(2025, 8, 26)
date_list = [start_date + datetime.timedelta(days=i) for i in range((end_date - start_date).days + 1)]

# 模拟人数
num_people = 600   # 假设 500 个人
captures_per_day = 8  # 平均每天抓拍 8 次
total_records = 30000

records = []

# 造数据
for day in date_list:
    for person in range(1, num_people + 1):
        n_captures = max(8, int(random.gauss(mu=captures_per_day, sigma=2)))  # 每人每天捕获次数
        age=random.choice(age_range)
        gender=random.choice(gender_range)
        member_tier=random.choice(member_tier_range)
        for _ in range(n_captures):
            region_id, region_name = random.choice(regions)
            capture_time = datetime.datetime.combine(
                day,
                datetime.time(
                    hour=random.randint(0, 23),   # 商场营业时间 8:00 ~ 21:59
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )
            )
            records.append(('p'+str(person), region_id, region_name, capture_time,age,member_tier,gender,datetime.datetime.now()))

# 随机打乱，避免顺序性太强
random.shuffle(records)

# 控制数据量在 20000 左右
records = records[:total_records]

print(f"生成数据总量: {len(records)} 条")

# 分批写入 ClickHouse
batch_size = 1000
for i in range(0, len(records), batch_size):
    batch = records[i:i+batch_size]
    client.execute(
        f"INSERT INTO {table_name} (profile_id, region_id, region_name, capture_time,age,member_tier,gender,batch_time) VALUES",
        batch
    )
    print(f"已写入 {i + len(batch)} 条数据")

print("✅ 数据写入完成！")
