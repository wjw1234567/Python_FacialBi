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
    date_list = client.execute("SELECT DISTINCT toDate(capture_time) as d FROM dwd_user_capture_heatmap ORDER BY d")

    # 获取全部日期
    date_list = [r[0] for r in date_list]


    target_bins = [-60, -45, -30, -15, -10, -5, 0, 5, 10, 15, 30, 45, 60]

    for date in date_list:
        query = f"""
            SELECT toDate(capture_time) as date,
                   profile_id,
                   region_name,
                   capture_time,
                   age,
                   member_tier,
                   gender                  
            FROM dwd_user_capture_heatmap
            WHERE toDate(capture_time) = '{date}'
            ORDER BY profile_id, capture_time
        """
        rows, cols = client.execute(query, with_column_types=True)



        columns = [c[0] for c in cols]
        df_day = pd.DataFrame(rows, columns=columns)



        if df_day.empty:
            continue

        # groupby 并行
        grouped = [g for _, g in df_day.groupby("profile_id", sort=False)]
        print(grouped[0]["age"].to_numpy()[0])

        # dfs = Parallel(n_jobs=8)(delayed(process_one_person)(g, target_bins) for g in grouped)

        # dfs = [d for d in dfs if d is not None]


if __name__ == "__main__":
    main()