from clickhouse_driver import Client
from datetime  import datetime
import pandas as pd
client = Client(
    host="localhost",  # 替换为你的 ClickHouse 地址
    port=9000,  # TCP 端口
    user="default",
    password="ck_test",
    database="Facial"
)


# result=client.execute("select count(1) from (select profile_id,region_id,region_name,capture_time,age,member_tier,gender,batch_time from dwd_user_capture_original)")

# print(type(result))
# print( result[0][0])
# print(col for col in result[0])


# date=datetime.strptime("2025-08-25", "%Y-%m-%d").date()
# print(date.date())

t1=pd.Timestamp("2025-08-25 14:31:21")
t1_h=t1.strftime("%H:00")

condict={"date":'2025-08-25'}

print(condict.get("date",''))

# for key ,value in condict.items():
#     print(key)
#     print(value)

# print(t1_h)

