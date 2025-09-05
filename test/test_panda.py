import pandas as pd
import numpy as np
from clickhouse_driver import Client
from datetime import datetime
from joblib import Parallel, delayed
from collections import deque


# ==========================
# 1. ClickHouse 连接
# ==========================
client = Client(
    host='localhost',
    port=9000,
    user='default',
    password='ck_test',
    database='Facial',
    send_receive_timeout=60
)




# ==========================
# 3. 主流程：整天拉取 → groupby 并行 → 写回
# ==========================
def main():
    # 拿到所有 distinct date
    # date_list = client.execute("SELECT DISTINCT toDate(capture_time) as d FROM dwd_user_capture_heatmap ORDER BY d")

    # 获取全部日期
    date_list = ['2025-08-25']


    target_bins = [-60, -45, -30, -15, -10, -5, 0, 5, 10, 15, 30, 45, 60]

    for date in date_list:
        query = f"""
             SELECT profile_id, profile_type, member_tier, age, gender,
               camera_id, region_id, region_name, capture_time
        FROM dwd_user_capture_original
        WHERE toDate(capture_time) = '{date}' and profile_id=1
        ORDER BY profile_id, capture_time
        """
        rows, cols = client.execute(query, with_column_types=True)
        columns = [c[0] for c in cols]
        df = pd.DataFrame(rows, columns=columns)

        df = df.sort_values(["profile_id", "capture_time"]).copy()

        result_list = []

        for pid, g in df.groupby("profile_id"):
            g = g.sort_values("capture_time")

            # 去掉连续相同的区域，只保留第一次进入
            mask = g["region_name"] != g["region_name"].shift(1)
            g_unique = g[mask].copy()
            print(g[mask])
            print(g[['profile_id','region_name','capture_time']])
            print(g_unique[['profile_id','region_name','capture_time']])



'''
        if df_day.empty:
            continue

        # groupby 并行
        grouped = [g for _, g in df_day.groupby("profile_id", sort=False)]
        print(grouped[0]["age"].to_numpy()[0])

        # dfs = Parallel(n_jobs=8)(delayed(process_one_person)(g, target_bins) for g in grouped)

        # dfs = [d for d in dfs if d is not None]

'''



if __name__ == "__main__":
    main()