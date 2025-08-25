import pandas as pd
import numpy as np
from datetime import datetime

# 定义区域列表
zones = [
    "Zone P2F", "Zone P2D", "Zone P2E", "Zone P2C",
    "Zone P2B", "Zone P2A", "Zone P2G", "Zone P1D",
    "Zone P1C", "Zone P1B", "Zone P1A", "Zone P1F", "Zone P1E"
]

# 生成2024年和2025年的每小时时间戳
start_2024 = datetime(2024, 1, 1, 0, 0)
end_2024 = datetime(2024, 12, 31, 23, 0)
start_2025 = datetime(2025, 1, 1, 0, 0)
end_2025 = datetime(2025, 12, 31, 23, 0)

# 生成每小时的时间序列
timestamps_2024 = pd.date_range(start=start_2024, end=end_2024, freq='H')
timestamps_2025 = pd.date_range(start=start_2025, end=end_2025, freq='H')
all_timestamps = timestamps_2024.union(timestamps_2025)

# 准备数据
data = []
for timestamp in all_timestamps:
    for zone in zones:
        # 为不同区域设置不同的销售基准，使数据更具区分度
        if "P1" in zone:
            # P1区域销售基准较低
            base = 300 + (ord(zone[-1]) - ord('A')) * 30  # 不同子区域有差异
            std_dev = 150
        else:
            # P2区域销售基准较高
            base = 600 + (ord(zone[-1]) - ord('A')) * 40  # 不同子区域有差异
            std_dev = 200

        # 生成随机销售额，确保为正数
        sales = max(0, np.random.normal(base, std_dev))
        # 保留两位小数
        sales = round(sales, 2)

        data.append({
            '订单时间': timestamp,
            '区域': zone,
            '销售额': sales
        })

# 创建DataFrame
df = pd.DataFrame(data)

# 显示数据基本信息
print(f"生成的数据总量: {len(df)} 行")
print(f"时间范围: {df['订单时间'].min()} 至 {df['订单时间'].max()}")
print("\n数据示例:")
print(df.head())

# 保存到Excel文件
excel_file = "区域销售数据_2024-2025.xlsx"
df.to_excel(excel_file, index=False, engine='openpyxl')
print(f"\n数据已成功写入到 {excel_file}")
