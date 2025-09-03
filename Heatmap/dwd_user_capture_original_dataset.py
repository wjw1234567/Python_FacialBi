import random
from datetime import datetime, timedelta
from clickhouse_driver import Client
from datetime import datetime

# ---------------------
# 配置 ClickHouse 连接
# ---------------------
client = Client(
    host="localhost",  # 替换为你的 ClickHouse 地址
    port=9000,  # TCP 端口
    user="default",
    password="ck_test",
    database="Facial"
)


# ---------------------
# 数据生成器（yield）
# ---------------------
def mock_data_generator(num_users=300, total_records=30000, start_date="2025-08-01", days=3):
    regions = [
        (1, 'Zone P1A'), (2, 'Zone P1B'), (3, 'Zone P1C'), (4, 'Zone P2A'),
        (5, 'Zone P2B'), (6, 'Zone P2C'), (7, 'Zone P1D'), (8, 'Zone P1E'),
        (9, 'Zone P1F'), (10, 'Zone P2D'), (11, 'Zone P2E'), (12, 'Zone P2F'),
        (13, 'Zone P2G')
    ]
    member_tiers = ['Basic', 'Silver', 'Gold', 'Diamond']
    start_date = datetime.strptime(start_date, "%Y-%m-%d")

    # 用户基本属性
    users = []
    for uid in range(1, num_users + 1):
        profile_type = random.choice([1, 2, 3, 4])
        member_tier = random.choice(member_tiers) if profile_type == 1 else ''
        age = random.randint(18, 70)
        gender = random.choice([1, 2])
        users.append((uid, profile_type, member_tier, age, gender))

    avg_records = total_records // num_users

    for uid, profile_type, member_tier, age, gender in users:
        for day_offset in range(days):
            day_start = start_date + timedelta(days=day_offset)
            num_captures = random.randint(avg_records // days // 2, avg_records // days * 2)

            capture_time = day_start + timedelta(hours=random.randint(0, 23))
            for _ in range(num_captures):
                region_id, region_name = random.choice(regions)
                camera_id = int(f"{region_id}{random.randint(1, 5)}")
                yield (
                    uid, profile_type, member_tier, age, gender,
                    camera_id, region_id, region_name, capture_time,datetime.now()
                )
                # 往后推进 1~20 分钟
                capture_time += timedelta(minutes=random.randint(1, 20))
                if capture_time.date() != day_start.date():
                    break


# ---------------------
# 批量写入（消费生成器）
# ---------------------
def insert_in_batches(client, table_name, generator, batch_size=5000):
    insert_sql = f"""
    INSERT INTO {table_name} 
    (profile_id, profile_type, member_tier, age, gender, camera_id, region_id, region_name, capture_time,batch_time)
    VALUES
    """
    batch = []
    total = 0
    for record in generator:
        batch.append(record)
        if len(batch) >= batch_size:
            client.execute(insert_sql, batch)
            total += len(batch)
            print(f"已写入 {total} 条")
            batch = []
    # 处理最后不足一批的数据
    if batch:
        client.execute(insert_sql, batch)
        total += len(batch)
        print(f"已写入 {total} 条 (完成)")


# ---------------------
# 主流程
# ---------------------
if __name__ == "__main__":
    gen = mock_data_generator(
        num_users=300,
        total_records=50000,
        start_date="2025-08-25",
        days=3
    )
    insert_in_batches(client, "dwd_user_capture_original", gen, batch_size=5000)
