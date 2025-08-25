import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# 设置随机数种子，保证结果可复现
np.random.seed(42)
random.seed(42)

# 模拟参数
num_records = 1000  # 生成1000条订单数据
start_date = datetime(2021, 1, 1)  # 近三年
end_date = datetime(2023, 12, 31)

# 产品池：名称 + 单价范围
products = {
    "笔记本电脑": (4000, 9000),
    "手机": (2000, 6000),
    "耳机": (200, 800),
    "办公桌": (500, 2000),
    "办公椅": (300, 1500),
    "显示器": (800, 3000),
    "打印机": (1000, 5000),
    "咖啡机": (300, 2000)
}

# 地区池
regions = ["华东", "华南", "华北", "西南", "西北", "东北"]

# 生成数据
data = []
for i in range(1, num_records + 1):
    order_id = f"ORD{i:05d}"
    customer_id = f"CUST{random.randint(1, 200):04d}"
    product = random.choice(list(products.keys()))
    price_range = products[product]
    unit_price = round(random.uniform(*price_range), 2)
    quantity = random.randint(1, 10)
    sales = round(unit_price * quantity, 2)
    region = random.choice(regions)
    order_date = start_date + timedelta(
        days=random.randint(0, (end_date - start_date).days)
    )

    data.append([order_id, customer_id, product, unit_price,
                 quantity, sales, region, order_date])

# 转为 DataFrame
df = pd.DataFrame(data, columns=[
    "订单ID", "客户ID", "产品名称", "单价", "数量", "销售额", "地区", "订单日期"
])

# 保存为 Excel 文件
file_name = "销售订单数据.xlsx"
df.to_excel(file_name, index=False)

print(f"数据已生成并保存为 {file_name}")
