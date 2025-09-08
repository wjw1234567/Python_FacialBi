from clickhouse_driver import Client
import pandas as pd
import numpy as np

class CaptureGroupProcessor:
    def __init__(self, host="localhost", port=9000, user="default", password="", database="default"):
        self.client = Client(host=host, port=port, user=user, password=password, database=database)

    def read_data(self, date: str):
        """
        从 ClickHouse 读取某一天的数据
        """
        sql = f"""
        SELECT profile_id, profile_type, member_tier, age, gender, camera_id, region_id, region_name, capture_time
        FROM dwd_user_capture_original
        WHERE toDate(capture_time) = '{date}'
        ORDER BY camera_id, capture_time
        """
        rows = self.client.execute(sql)
        if not rows:
            return pd.DataFrame()
        columns = ["profile_id", "profile_type", "member_tier", "age", "gender",
                   "camera_id", "region_id", "region_name", "capture_time"]
        df = pd.DataFrame(rows, columns=columns)
        df["capture_time"] = pd.to_datetime(df["capture_time"])
        df["date"] = df["capture_time"].dt.date.astype(str)
        return df

    def process_grouping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        在固定 camera_id 下，判断是否组成群组
        """
        if df.empty:
            return df

        result_list = []

        # 按 camera_id 分组
        for cam, g in df.groupby("camera_id"):
            g = g.sort_values("capture_time").reset_index(drop=True)
            group_ids = []
            current_group = 0
            prev_time = None

            for idx, row in g.iterrows():
                if prev_time is None:
                    current_group += 1
                else:
                    # 时间差小于等于2秒 → 属于同一个群组
                    if (row["capture_time"] - prev_time).total_seconds() <= 2:
                        pass
                    else:
                        current_group += 1
                group_ids.append(current_group)
                prev_time = row["capture_time"]

            g["group_id"] = group_ids
            result_list.append(g)

        result = pd.concat(result_list, ignore_index=True)

        # 按 date, profile_id, region_name 聚合 (示例用 group_id 取最小值表示群组标识)
        agg_df = (
            result.groupby(["date", "profile_id", "profile_type", "member_tier",
                            "age", "gender", "camera_id", "region_id", "region_name"], as_index=False)
            .agg({"group_id": "min"})
        )
        return agg_df

    def write_data(self, df: pd.DataFrame, target_table: str, batch_size: int = 100000):
        """
        分批写数据到 ClickHouse
        """
        if df.empty:
            print("No data to insert.")
            return

        columns = df.columns.tolist()
        col_str = ",".join(columns)

        # 转换为列表的元组
        data = [tuple(row) for row in df.to_numpy()]

        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            sql = f"INSERT INTO {target_table} ({col_str}) VALUES"
            self.client.execute(sql, batch)
            print(f"Inserted {len(batch)} rows into {target_table}")


    def _format_value(self, v):
        if v is None:
            return "NULL"
        if isinstance(v, str):
            return f"'{v}'"
        return str(v)

    def process_one_day(self, date: str, target_table: str):
        df = self.read_data(date)
        print(f"Read {len(df)} rows for {date}")
        if df.empty:
            return
        result = self.process_grouping(df)
        self.write_data(result, target_table)
        print(f"Wrote {len(result)} rows to {target_table}")

# 使用示例
if __name__ == "__main__":
    processor = CaptureGroupProcessor(host="localhost", database="test")
    processor.process_one_day("2025-09-01", "dws_user_capture_grouped")
