import pandas as pd
import numpy as np
from clickhouse_driver import Client
from datetime import datetime
from joblib import Parallel, delayed
from collections import deque
from Logger import Logger



class TrackHeatmap:

    def __init__(self, host=['localhost','localhost'], port=[9000,9000], user=['default','default'], password=['',''], database=['default','default'],prefix=None):

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

        self.client = Client(host=host[0], port=port[0], user=user[0], password=password[0], database=database[0])
        self.wd_client = Client(host=host[1], port=port[1], user=user[1], password=password[1], database=database[1])


        self.prefix=prefix
        self.logger = Logger(log_dir="./logs", prefix=self.prefix)

    # ==========================
    # 2. 单个 profile_id 的处理逻辑
    # ==========================
    @staticmethod
    def process_one_person(df, target_bins):
        """
        df: 单个 profile_id 的所有记录（已按 capture_time 排序）
        使用 NumPy 数组而不是 to_dict
        """
        if df.empty:
            return None

        times = df["capture_time"].to_numpy()
        areas = df["region_name"].to_numpy()
        age=df["age"].to_numpy()[0]
        member_tier=df["member_tier"].to_numpy()[0]
        gender=df["gender"].to_numpy()[0]


        age_range=""

        # 处理age
        if age >=0 and age <=20 :
            age_range= "0-20"
        elif age >=21 and age <=39 :
            age_range = "21-39"
        elif age >=40 and age <=65 :
            age_range = "40-65"
        elif age >65 :
            age_range = "65+"


        n = len(times)

        results = []
        window = deque()
        right = 0

        for i in range(n):
            t1 = times[i]
            base_area = areas[i]

            # --- 1) 滑动右指针，把 <= t1+60 的点加入 deque
            while right < n and times[right] <= t1 + np.timedelta64(60, "m"):
                window.append(right)
                right += 1

            # --- 2) 滑动左指针，把 < t1-60 的点踢出 deque
            while window and times[window[0]] < t1 - np.timedelta64(60, "m"):
                window.popleft()

            bucket_best = {}

            # --- 3) 遍历窗口候选，更新每个 off_bin 最近点 (argMin)
            for idx  in window:
                diff_min = (times[idx] - t1).astype("timedelta64[m]").astype(int)
                if idx == i and diff_min == 0:
                    off_bin = 0
                elif 0 <= diff_min <= 15:
                    off_bin = (diff_min // 5 +1) * 5
                elif -15 <= diff_min < 0:
                    off_bin = (diff_min // 5) * 5
                elif 16 <= diff_min <= 60:
                    off_bin = (diff_min // 15 + 1) * 15
                elif -60 <= diff_min <= -16:
                    off_bin = (diff_min // 15) * 15
                else:
                    continue

                if off_bin not in target_bins:
                    continue

                if (off_bin not in bucket_best or abs(diff_min) < abs(bucket_best[off_bin]["diff_min"])):
                    bucket_best[off_bin] = {"region_name": areas[idx], "diff_min": diff_min}

            for off_bin, value in bucket_best.items():
                results.append({
                    "date": df["date"].iloc[0],
                    "profile_id": df["profile_id"].iloc[0],
                    "region_name": base_area,
                    "z0_time": pd.Timestamp(t1),
                    "date_hour":pd.Timestamp(t1).strftime("%H:00"),
                    "off_bin": off_bin,
                    "area_at_off": value["region_name"],
                    "member_tier":member_tier,
                    "age_range": age_range,
                    "gender":gender,
                    "batch_time": datetime.now()
                })

        if not results:
            return None
        return pd.DataFrame(results)



    # ==========================
    # 4. DataFrame 批量写入 ClickHouse
    # ==========================
    def insert_df(self,client, table: str, df: pd.DataFrame):
        if df.empty:
            return
        columns = list(df.columns)
        escaped = [f"`{c}`" for c in columns]
        data = [tuple(r) for r in df.itertuples(index=False, name=None)]
        insert_sql = f"INSERT INTO {table} ({','.join(escaped)}) VALUES"
        # print(insert_sql)
        client.execute(insert_sql, data)


    # ==========================
    # 3. 主流程：整天拉取 → groupby 并行 → 写回
    # ==========================
    def process_main(self,target_table,date_list):
        # 拿到所有 distinct date
        # date_list = self.client.execute("SELECT DISTINCT toDate(capture_time) as d FROM dwd_user_capture_original ORDER BY d")
        # date_list = [r[0] for r in date_list]

        target_bins = [-60, -45, -30, -15, -10, -5, 0, 5, 10, 15, 30, 45, 60]

        for date in date_list:
            try:
                delete_sql=f"alter table {target_table} delete where date = %(date)s"
                self.wd_client.execute(delete_sql,params={"date":date})


                query = f"""
                    SELECT toDate(capture_time) as date,
                           profile_id,
                           region_name,
                           capture_time,
                           age,
                           member_tier,
                           gender                  
                    FROM dwd_user_capture_original
                    WHERE toDate(capture_time) = %(date)s
                    ORDER BY profile_id, capture_time
                """
                rows, cols = self.client.execute(query, params={"date":date},with_column_types=True)
                columns = [c[0] for c in cols]
                df_day = pd.DataFrame(rows, columns=columns)

                if df_day.empty:
                    continue

                # groupby 并行
                grouped = [g for _, g in df_day.groupby("profile_id", sort=False)]
                # 使用多线程处理每个用户
                dfs = Parallel(n_jobs=8)(delayed(self.process_one_person) (g, target_bins) for g in grouped)

                print(f"{date} 已删除 {len(df_day)} 行")
                self.logger.log(f"{date} 已删除 {len(df_day)} 行")


                dfs = [d for d in dfs if d is not None]
                if dfs:
                    big_df = pd.concat(dfs, ignore_index=True)
                    total_rows=len(big_df)
                    batch_size=10000 #批量写入

                    for i in range(0,total_rows,batch_size):
                        batch_df = big_df.iloc[i:i + batch_size]  # 截取当前批次数据
                        self.insert_df(self.wd_client, target_table, batch_df)
                        print(f"{date} 第 {i // batch_size + 1} 批写入 {len(batch_df)} 行，累计 {min(i + batch_size, total_rows)}/{total_rows} 行")
                        self.logger.log(
                            f"{date} 第 {i // batch_size + 1} 批写入 {len(batch_df)} 行，累计 {min(i + batch_size, total_rows)}/{total_rows} 行"
                        )

                    print(f"{date} 总条数写入 {len(big_df)} 行")

                    self.logger.log(f"{date} 总条数写入 {len(big_df)} 行")


            except Exception as e:

                self.logger.error(f"{date} 执行出错: {type(e).__name__}: {str(e)}")


if __name__ == "__main__":


    target_table="dws_visitor_path_track_heatmap"
    date_list=['2025-08-25','2025-08-26','2025-08-27']
    trackheatmap = TrackHeatmap(host=["localhost", "localhost"], port=[9000, 9000], user=["default", "default"],
                                  password=["ck_test", "ck_test"], database=["Facial", "Facial"],prefix=target_table)
    trackheatmap.process_main(target_table, date_list)
