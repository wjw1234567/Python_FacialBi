from clickhouse_driver import Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from Logger import Logger


class StayTimeProcessor:
    def __init__(self,  host=["localhost","localhost"], port=[9000,9000], user=["default","default"], password=["",""], database=["default","default"],prefix=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

        self.client = Client(host=host[0], port=port[0], user=user[0], password=password[0], database=database[0])
        self.wd_client = Client(host=host[1], port=port[1], user=user[1], password=password[1], database=database[1])

        self.prefix = prefix
        self.logger = Logger(log_dir="./logs", prefix=self.prefix)


    def read_data(self, date: str):
        """按天读取 clickhouse 数据"""
        sql = f"""
        SELECT profile_id
             , profile_type
             , member_tier
             , case when age between 0 and 20 then '0-20'
                             when age between 21 and 39 then '21-39'
                             when age between 40 and 65 then '40-65'
                             when age >65 then  '65+'
                       end  Age_range
             , gender
             , region_id
             , region_name
             , capture_time
            -- , region_type
        FROM dwd_user_capture_original
        WHERE toDate(capture_time) = '{date}'
        ORDER BY profile_id, capture_time
        """
        data, columns = self.client.execute(sql, with_column_types=True)
        cols = [c[0] for c in columns]
        return pd.DataFrame(data, columns=cols)





    def calc_stay_time_segments(self,df: pd.DataFrame):
        if df.empty:
            return pd.DataFrame()

        df["capture_time"] = pd.to_datetime(df["capture_time"])
        df["date"] = pd.to_datetime(df['capture_time']).dt.floor('D')
        df["date_casino"] = (pd.to_datetime(df['capture_time']) + pd.Timedelta(hours=6)).dt.floor('D')

        df["region_type"] = ''

        # 排序
        df = df.sort_values(["profile_id", "date", "capture_time"]).reset_index(drop=True)

        # 判断是否进入新的停留段：region变化 → 新段
        df["new_segment"] = (
            (df["region_id"] != df.groupby(["profile_id", "date","date_casino"])["region_id"].shift())
        ).astype(int)

        # 给每个用户的每一天分段编号，类似于SQL的sum() over()...逐行累计结果
        df["segment_id"] = df.groupby(["profile_id", "date","date_casino"])["new_segment"].cumsum()

        # 每个 segment 求 min/max 时间
        segments = (
            df.groupby(["profile_id", "date","date_casino", "region_id", "segment_id"], as_index=False)
            .agg(
                start_time=("capture_time", "min"),
                profile_type=("profile_type", "first"),
                member_tier=("member_tier", "first"),
                Age_range=("Age_range", "first"),
                gender=("gender", "first"),
                region_name=("region_name", "first"),
                region_type=("region_type", "first")
            )
        )

        # 对segments进行重新排序
        segments=segments.sort_values(["date","date_casino","profile_id", "start_time"]).reset_index(drop=True)


        # 下一段的 start_time
        segments["next_start"] = segments.groupby(["profile_id", "date","date_casino"])["start_time"].shift(-1)

        # 停留时长
        segments["stay_time"] = (segments["next_start"] - segments["start_time"]).dt.total_seconds()

        # 每人每天每区域取最大停留时长
        result = (
            segments.groupby(["profile_id", "date","date_casino", "region_id"], as_index=False)
            .agg({
                "stay_time": "max",
                "profile_type": "first",
                "member_tier": "first",
                "Age_range": "first",
                "gender": "first",
                "region_name": "first",
                "region_type": "first"
            })
        )



        # 缺失值填充：用当天该区域平均值 用当前分组的stay_time平均值填充该组内的NaN
        region_mean = (
            result.groupby(["date","date_casino", "region_id"])["stay_time"]
            .transform(lambda x: x.fillna(x.mean()).fillna(0))
        )


        result["stay_time"] = result["stay_time"].fillna(region_mean)


        result["batch_time"]=datetime.now()

        return result



    def write_data(self, df: pd.DataFrame, target_table: str, date: str, batch_size: int = 10000):

        try:


            if df.empty:
                return

            columns = df.columns.tolist()

            # 转成 list of tuples
            records = [tuple(x) for x in df[columns].to_numpy()]

            sql = f"""
           INSERT INTO {target_table} ({",".join(columns)}) VALUES
            """

            # 分批写入
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                self.wd_client.execute(sql, batch)
                print(f"Inserted batch {i // batch_size + 1}, size={len(batch)}")
                self.logger.log(f"Inserted batch {i // batch_size + 1}, size={len(batch)}")

        except Exception as e:
            self.logger.error(f"执行write_data语句出错: {type(e).__name__}: {str(e)}")





    def process_one_day(self, date: str, target_table: str):

        self.wd_client.execute(f"alter table {target_table} delete where date = %(date)s", {"date": date})
        print(f"已执行完删除{date}的数据")

        df = self.read_data(date)
        result_df = self.calc_stay_time_segments(df)
        # result_df.to_excel("result_df1.xlsx",sheet_name="result_df", index=False)
        self.write_data(result_df, target_table, date)


if __name__ == "__main__":

    date_list = ['2025-08-25','2025-08-26','2025-08-27']  # 默认处理昨天
    target_table="dws_profileid_staytime"
    processor = StayTimeProcessor(host=["localhost", "localhost"], port=[9000, 9000], user=["default", "default"],
                                  password=["ck_test", "ck_test"], database=["Facial", "Facial"], prefix=target_table)


    for date in date_list:
        processor.process_one_day(date, target_table)



