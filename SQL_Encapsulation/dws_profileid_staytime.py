from clickhouse_driver import Client
import pandas as pd
import numpy as np
from datetime import datetime

class UserCaptureProcessor:
    def __init__(self, host="localhost", port=9000, user="default", password="", database="default"):
        self.client = Client(host=host, port=port, user=user, password=password, database=database)

        # 读连接
        self.read_client = Client(
            host=host, port=port, user=user, password=password, database=database
        )
        # 写连接
        self.write_client = Client(
            host=host, port=port, user=user, password=password, database=database
        )



    def read_data_by_date_stream(self, date_str: str, chunk_size: int = 100000):
        """
        按天流式读取 ClickHouse 数据，yield DataFrame
        """
        sql = f"""
        SELECT profile_id, profile_type, member_tier, age, gender,
               camera_id, region_id, region_name, capture_time
        FROM dwd_user_capture_original
        WHERE toDate(capture_time) = '{date_str}' 
        ORDER BY profile_id, capture_time
        """
        cursor = self.read_client.execute_iter(sql, with_column_types=True)
        # 第一条是列名+类型
        first = next(cursor)
        columns = [c[0] for c in first]
        print(columns)

        batch = []
        for row in cursor:
            batch.append(row)
            if len(batch) >= chunk_size:
                yield pd.DataFrame(batch, columns=columns)
                batch = []
        if batch:
            yield pd.DataFrame(batch, columns=columns)

    def calc_stay_time(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算每人每区域的停留时间：
        - 连续相同 region_name 只取第一次进入的时间
        - 停留时间 = 当前区域进入时间 与 下一个区域进入时间 的时间差
        """
        df = df.sort_values(["profile_id", "capture_time"]).copy()

        result_list = []

        for pid, g in df.groupby("profile_id"):

            g = g.sort_values("capture_time")

            # 去掉连续相同的区域，只保留第一次进入,向下移一位
            mask = g["region_name"] != g["region_name"].shift(1)
            g_unique = g[mask].copy()


            # 计算相邻区域进入时间差
            g_unique["next_time"] = g_unique["capture_time"].shift(-1)
            g_unique["stay_time"] = (g_unique["next_time"] - g_unique["capture_time"]).dt.total_seconds()

            # 只保留原始需要的列 + stay_time
            g = g.merge(
                g_unique[["camera_id","capture_time", "stay_time"]],
                on=["capture_time", "camera_id"], #根据capture_time关联回去
                how="left"
            )

            result_list.append(g)


        return pd.concat(result_list, ignore_index=True)




    def write_data(self, df: pd.DataFrame, target_table: str, batch_size: int = 10000):
        """
        批量写入 ClickHouse
        """

        """
               写入 ClickHouse：丢掉 stay_time 为 None 的行，去掉 capture_time 列
        """

        df = df.copy()

        # 只保留 stay_time 有值的行
        df = df.dropna(subset=["stay_time"], how="all")

        df["date"] = (pd.to_datetime(df['capture_time'])).dt.floor('D')
        df["date_casino"] = (pd.to_datetime(df['capture_time']) + pd.Timedelta(hours=6)).dt.floor('D')
        df["batch_time"]=datetime.now()

        bins = [0, 21, 39, 65,float('inf')]  # 区间边界：0-20，21-39，40+（可根据需要调整）
        labels = ['0-20', '21-39','39-65' ,'65+']  # 对应区间的标签

        df['Age_range'] = pd.cut(
            df['age'],
            bins=bins,
            labels=labels,
            include_lowest=True  # 包含左边界（0也会被分到0-20区间）
        )


        # 删除 capture_time
        if "capture_time" in df.columns:
            df = df.drop(columns=["capture_time","camera_id","age"])

        columns = ["date", "date_casino", "profile_id", "profile_type", "member_tier", "Age_range", "gender",
                   "region_id", "region_name", "stay_time","batch_time"]



        df = df[columns]
        for start in range(0, len(df), batch_size):
            batch_df = df.iloc[start:start + batch_size]
            data = [tuple(None if pd.isna(x) else x for x in row) for row in batch_df.to_numpy()]
            self.write_client.execute(
                f"INSERT INTO {target_table} ({','.join(columns)}) VALUES",
                data
            )



    def process_one_day_stream(self, date_str: str, target_table: str, chunk_size: int = 100000):
        """
        按天流式处理：边读边算边写
        """

        self.client.execute(f"alter table {target_table} delete where date = %(date)s",{"date":date_str})
        print(f"已执行完删除{date_str}的数据")



        leftover = pd.DataFrame()

        for chunk_df in self.read_data_by_date_stream(date_str, chunk_size=chunk_size):
            if chunk_df.empty:
                continue

            # 拼接 leftover，保证跨 chunk 正确
            if not leftover.empty:
                chunk_df = pd.concat([leftover, chunk_df], ignore_index=True)



            processed = self.calc_stay_time(chunk_df)



            if not leftover.empty:
                # 清除其他批次的第一条
                processed.drop(processed.index[0])

            # 更新 leftover（每个 profile_id 最后一条）
            leftover = processed.tail(1)
            leftover=leftover.drop(columns=["stay_time"])


            if not processed.empty:
                self.write_data(processed, target_table)

        # 处理最后一批 leftover
        if not processed.empty:
            self.write_data(processed, target_table)

        print(f"{date_str} 数据处理完成，写入 {target_table}")




if __name__ == "__main__":
    processor = UserCaptureProcessor(host="localhost", port=9000,user="default", password="ck_test", database="Facial")
    # 举例：处理 2025-09-01 的数据
    processor.process_one_day_stream(
        date_str="2025-08-25",
        target_table="dws_profileid_staytime",
        chunk_size=10000
    )

