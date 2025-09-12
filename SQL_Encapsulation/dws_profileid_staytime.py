from clickhouse_driver import Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from joblib import Parallel, delayed



class StayTimeProcessor:
    def __init__(self, host="localhost", port=9000, user="default", password="", database="default"):
        self.client = Client(host=host, port=port, user=user, password=password, database=database)

        # 写连接
        self.write_client = Client(
            host=host, port=port, user=user, password=password, database=database
        )


    def read_data(self, date: str) -> pd.DataFrame:
        """
        从ClickHouse读取某一天的数据
        """
        sql = f"""
        SELECT profile_id, profile_type, member_tier, age, gender,
               camera_id, region_id, region_name, capture_time
        FROM dwd_user_capture_original
        WHERE toDate(capture_time) = toDate('{date}') 
        ORDER BY profile_id, capture_time
        """


        data,cols = self.client.execute(sql,with_column_types=True)
        if not data:
            return pd.DataFrame()

        columns = [c[0] for c in cols]

        print(f"data={data[0]},cols={cols}")

        df = pd.DataFrame(data, columns=columns)
        df["capture_time"] = pd.to_datetime(df["capture_time"])
        return df




    @staticmethod
    def process_one_person(g: pd.DataFrame, date: str) -> pd.DataFrame:
        """
        处理一个 profile_id 的停留时间
        """
        g = g.sort_values("capture_time").copy()


        # 去掉连续相同的区域，只保留第一次进入,向下移一位
        mask = g["region_name"] != g["region_name"].shift(1)
        g_unique = g[mask].copy()

        # 计算相邻区域进入时间差
        g_unique["next_time"] = g_unique["capture_time"].shift(-1)
        g_unique["stay_time"] = (g_unique["next_time"] - g_unique["capture_time"]).dt.total_seconds()


        g_unique["date"] = (pd.to_datetime(g_unique['capture_time'])).dt.floor('D')
        g_unique["date_casino"] = (pd.to_datetime(g_unique['capture_time']) + pd.Timedelta(hours=6)).dt.floor('D')
        g_unique["batch_time"]=datetime.now()

        bins = [0, 21, 40, 65, float('inf')]  # 区间边界：0-20，21-39，40+（可根据需要调整）
        labels = ['0-20', '21-40', '40-65', '65+']  # 对应区间的标签

        g_unique['Age_range'] = pd.cut(
            g_unique['age'],
            bins=bins,
            labels=labels,
            include_lowest=True  # 包含左边界（0也会被分到0-20区间）
        )

        g_unique.dropna(subset=["stay_time"])


        if g_unique.empty:
            return None

        result = g_unique.groupby(
            ["date","date_casino", "profile_id", "profile_type", "member_tier", "Age_range", "gender",
             "region_id", "region_name","batch_time"],
            as_index=False,observed=True
        )["stay_time"].sum()

        # print(result.columns.tolist())

        return result

    def process_day_parallel(self, df: pd.DataFrame, date: str, n_jobs: int = -1) -> pd.DataFrame:
        """
        并行处理一天的数据
        """
        if df.empty:
            return pd.DataFrame()

        grouped = [g for _, g in df.groupby("profile_id", sort=False)]

        results = Parallel(n_jobs=n_jobs, verbose=5)(
            delayed(self.process_one_person)(g, date) for g in grouped
        )

        # 过滤掉 None
        results = [r for r in results if r is not None]

        if not results:
            return pd.DataFrame()

        return pd.concat(results, ignore_index=True)

    def write_data(self, df: pd.DataFrame, target_table: str, batch_size: int = 100000):
        """
        分批写入ClickHouse，避免一次性数据量过大
        """
        if df.empty:
            return

        # columns = ["date", "date_casino","profile_id", "profile_type", "member_tier", "Age_range", "gender",
        #            "camera_id", "region_id", "region_name", "stay_time","batch_time"]

        columns = df.columns.tolist()

        insert_sql = f"""
        INSERT INTO {target_table} ({",".join(columns)})
        VALUES
        """

        data = [tuple(x) for x in df[columns].to_numpy()]

        for i in range(0, len(data), batch_size):
            chunk = data[i:i+batch_size]
            self.client.execute(insert_sql, chunk)
            print(f"Inserted {len(chunk)} rows into {target_table}")


    def process_one_day(self, date: str, target_table: str, n_jobs: int = -1):
        """
        入口函数：并行处理一天的数据
        """

        self.write_client.execute(f"alter table {target_table} delete where date = %(date)s", {"date": date})
        print(f"已执行完删除{date}的数据")

        df = self.read_data(date)
        result = self.process_day_parallel(df, date, n_jobs=n_jobs)
        self.write_data(result, target_table)


if __name__ == "__main__":
    processor = StayTimeProcessor(
        host="localhost",
        port=9000,
        user="default",
        password="ck_test",
        database="Facial"
    )

    # 跑昨天的数据
    # yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    date="2025-08-25"

    processor.process_one_day(date, "dws_profileid_staytime", n_jobs=8)  # n_jobs=-1 表示用满所有CPU
