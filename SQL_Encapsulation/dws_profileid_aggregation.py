import os
import pandas as pd
import numpy as np
from clickhouse_driver import Client
from datetime import datetime, timedelta

# 配置ClickHouse连接
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'your_clickhouse_host')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 9000))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'your_username')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'your_password')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'your_database')
CASINO_ENTRANCE_CAMERA_ID = 'casino_entrance_camera_id'  # 替换为实际赌场入口摄像头ID

# 连接配置
CONNECTION_SETTINGS = {
    'host': CLICKHOUSE_HOST,
    'port': CLICKHOUSE_PORT,
    'user': CLICKHOUSE_USER,
    'password': CLICKHOUSE_PASSWORD,
    'database': CLICKHOUSE_DATABASE,
    'send_receive_timeout': 300,
    'compression': True,
}


def get_clickhouse_client():
    """获取ClickHouse客户端"""
    if not hasattr(get_clickhouse_client, 'client'):
        try:
            get_clickhouse_client.client = Client(**CONNECTION_SETTINGS)
            print("成功连接到ClickHouse")
        except Exception as e:
            print(f"连接ClickHouse失败: {e}")
            raise
    return get_clickhouse_client.client


def create_target_table(client):
    """创建目标结果表"""
    client.execute('''
    CREATE TABLE IF NOT EXISTS dwd_user_behavior_analysis (
        profile_id String,
        profile_type String,
        member_tier String,
        age Int32,
        gender String,
        camera_id String,
        region_id String,
        region_name String,
        stay_time Int32,  -- 该区域的停留时间（秒）
        group_num Int32   -- 所属群组编号（仅赌场入口有效）
    ) ENGINE = MergeTree()
    PARTITION BY toDate(toDateTime(capture_time))
    ORDER BY (profile_id, region_id, camera_id)
    ''')
    print("目标结果表创建/检查完成")


def get_date_range(client):
    """获取表中数据的日期范围"""
    result = client.execute('''
    SELECT 
        toDate(min(capture_time)) AS min_date,
        toDate(max(capture_time)) AS max_date
    FROM dwd_user_capture_original
    ''')
    if result and result[0][0] and result[0][1]:
        return result[0][0], result[0][1]
    return None, None


def process_daily_data(client, date):
    """处理单日数据"""
    print(f"开始处理 {date} 的数据...")

    # 读取当天数据，额外包含前一天的最后一小时数据用于处理跨天停留
    next_day = date + timedelta(days=1)
    query = f'''
    SELECT 
        profile_id, profile_type, member_tier, age, gender,
        camera_id, region_id, region_name, capture_time
    FROM dwd_user_capture_original
    WHERE capture_time >= toDateTime('{date} 00:00:00') 
      AND capture_time < toDateTime('{next_day} 00:00:00')
    ORDER BY profile_id, capture_time
    '''

    # 执行查询并转换为DataFrame
    result = client.execute(query)
    if not result:
        print(f"{date} 没有数据，跳过处理")
        return

    columns = ['profile_id', 'profile_type', 'member_tier', 'age', 'gender',
               'camera_id', 'region_id', 'region_name', 'capture_time']
    df = pd.DataFrame(result, columns=columns)

    # 转换时间格式
    df['capture_time'] = pd.to_datetime(df['capture_time'])

    # 1. 计算每个人在各区域的停留时间
    df = calculate_stay_time(df)

    # 2. 标记赌场入口摄像头的人员群组
    df = mark_casino_groups(df)

    # 过滤掉当天之前的数据（只保留当天数据）
    df = df[df['capture_time'].dt.date == date]

    # 准备写入数据（移除临时列）
    if 'next_region_time' in df.columns:
        df = df.drop(columns=['next_region_time', 'next_region_id'])

    # 转换为ClickHouse可接受的格式
    df['capture_time_str'] = df['capture_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # 提取需要写入的字段
    write_data = df[['profile_id', 'profile_type', 'member_tier', 'age', 'gender',
                     'camera_id', 'region_id', 'region_name', 'stay_time', 'group_num']]

    # 转换为元组列表
    data_tuples = [tuple(row) for row in write_data.values]

    # 写入ClickHouse
    if data_tuples:
        client.execute('''
        INSERT INTO dwd_user_behavior_analysis 
        (profile_id, profile_type, member_tier, age, gender, 
         camera_id, region_id, region_name, stay_time, group_num)
        VALUES
        ''', data_tuples)
        print(f"成功写入 {len(data_tuples)} 条记录到目标表")


def calculate_stay_time(df):
    """计算每个人在各区域的停留时间"""
    # 按人员ID分组，获取下一个区域和时间
    df = df.sort_values(['profile_id', 'capture_time'])

    # 使用shift获取下一行数据
    df['next_region_id'] = df.groupby('profile_id')['region_id'].shift(-1)
    df['next_region_time'] = df.groupby('profile_id')['capture_time'].shift(-1)

    # 计算停留时间（秒）
    df['stay_time'] = np.where(
        (df['next_region_id'].notna()) & (df['next_region_id'] != df['region_id']),
        (df['next_region_time'] - df['capture_time']).dt.total_seconds().astype(int),
        0
    )

    return df


def mark_casino_groups(df):
    """标记赌场入口摄像头的人员群组"""
    # 初始化group_num为0
    df['group_num'] = 0

    # 筛选赌场入口摄像头的数据
    casino_mask = df['camera_id'] == CASINO_ENTRANCE_CAMERA_ID
    casino_df = df[casino_mask].copy()

    if len(casino_df) == 0:
        return df

    # 按时间排序
    casino_df = casino_df.sort_values('capture_time')

    # 计算与前一个人的时间差
    casino_df['time_diff'] = casino_df['capture_time'].diff().dt.total_seconds()

    # 标记新群组起点（第一条记录或时间差>3秒）
    casino_df['is_new_group'] = (casino_df['time_diff'] > 3) | (casino_df['time_diff'].isna())

    # 计算群组编号（累计求和）
    casino_df['group_num'] = casino_df['is_new_group'].cumsum()

    # 将计算结果合并回原始DataFrame
    df.loc[casino_mask, 'group_num'] = casino_df['group_num'].values

    return df


def main():
    client = get_clickhouse_client()
    try:
        create_target_table(client)

        # 获取日期范围
        min_date, max_date = get_date_range(client)
        if not min_date or not max_date:
            print("表中没有数据，退出程序")
            return

        print(f"数据日期范围: {min_date} 至 {max_date}")

        # 按天处理数据
        current_date = min_date
        while current_date <= max_date:
            process_daily_data(client, current_date)
            current_date += timedelta(days=1)

        print("所有日期数据处理完成")

    finally:
        client.disconnect()
        print("已断开与ClickHouse的连接")


if __name__ == "__main__":
    main()
