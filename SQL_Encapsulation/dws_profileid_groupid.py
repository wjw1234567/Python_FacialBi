from clickhouse_driver import Client
import pandas as pd
import numpy as np
from datetime import datetime
from Logger import Logger



class CaptureGroupProcessor:
    def __init__(self, host=["localhost","localhost"], port=[9000,9000], user=["default","default"], password=["",""], database=["default","default"],prefix=None):

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
        """
        从 ClickHouse 读取某一天的数据
        """
        sql = f"""
        SELECT profile_id, profile_type, member_tier, age, gender, camera_id, region_id, region_name, capture_time
        FROM dwd_user_capture_original
        WHERE toDate(capture_time) = '{date}' and camera_id = 94
        ORDER BY camera_id, capture_time
        """
        rows,cols = self.client.execute(sql,with_column_types=True)
        if not rows:
            return pd.DataFrame()

        columns = [c[0] for c in cols]
        df = pd.DataFrame(rows, columns=columns)
        df["capture_time"] = pd.to_datetime(df["capture_time"])


        # df["date_hour"] = df["capture_time"].dt.date.astype(str)

        df["date_hour"] = df["capture_time"].dt.floor('h')
        df["date_casino_hour"] = df["capture_time"].dt.floor('h') - pd.Timedelta(hours=6)

        return df

    def process_grouping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        在固定 camera_id 下，判断是否组成群组 (向量化优化版)
        """
        if df.empty:
            return df

        result_list = []

        for cam, g in df.groupby("camera_id"):

            g = g.sort_values("capture_time").reset_index(drop=True)

            # 计算与上一行的时间差（秒）
            time_diff = g["capture_time"].diff().dt.total_seconds().fillna(9999)

            # 如果时间差 > 2 秒，标记为新群组 判断是否需要开启新群组，用 0/1 标记。
            new_group = (time_diff > 2).astype(int)
            # 累积求和得到 group_id 通过累积求和生成唯一的群组 ID，实现连续记录的分组。
            g["group_id"] = new_group.cumsum()


            '''
            示例： 
            
            capture_time	time_diff（第一步结果）	new_group（第二步结果）	group_id（第三步结果）
            08:00:00	    9999（填充值）	        1（9999>2）	            1（累积 1）
            08:00:01	    1（间隔 1 秒）	        0（1≤2）	                1（累积 1+0）
            08:00:02	    1（间隔 1 秒）	        0	                    1
            08:00:05	    3（间隔 3 秒）	        1（3>2）	                2（累积 1+0+0+1）
            08:00:06	    1（间隔 1 秒）	        0	                    2
            
            '''

            result_list.append(g)

        result = pd.concat(result_list, ignore_index=True)


        # 获取用户在每个小时内最大的capture_time.默认用户在一个小时内进赌场一次
        # 相当于row_number() over (partition by (formatDateTime(capture_time,'%Y-%m-%d %H:00:00'),profile_id) order by capture_time desc)
        max_time_idx = result.groupby(["date_hour", "profile_id"])["capture_time"].idxmax()
        #通过索引提取对应的记录（即每组最新的记录）
        result = result.loc[max_time_idx].sort_index()  # sort_index()保持原数据顺序


        # 根据"date_hour", "date_casino_hour", "group_id"进行分组获取每个组的人数
        agg_df = (
            result.groupby(
                ["date_hour", "date_casino_hour", "group_id"],
                as_index=False, observed=True
            ).agg(
                group_size=("profile_id", "nunique"),
            )
        )

        bins = [0,1, 2, 3, 4,5, float('inf')]  # 区间边界：0-20，21-39，40+（可根据需要调整）,bin一定比label多一位
        labels = ['1', '2', '3', '4','5','5+']  # 对应区间的标签

        agg_df['group_type'] = pd.cut(
            agg_df['group_size'],
            bins=bins,
            labels=labels,
            include_lowest=True  # 包含左边界（0也会被分到0-1区间）
        )

        agg_df["batch_time"]=datetime.now()

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

        # 转换为列表的二维元组
        data = [tuple(row) for row in df.to_numpy()]

        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            sql = f"INSERT INTO {target_table} ({col_str}) VALUES"
            self.wd_client.execute(sql, batch)
            print(f"Inserted {len(batch)} rows into {target_table}")
            self.logger.log(f"Inserted {len(batch)} rows into {target_table}")




    def process_one_day(self, date: str, target_table: str):


        detele_sql=f"alter table  {target_table} delete where toDate(date_hour)= %(date)s "
        self.wd_client.execute(detele_sql,params={"date":date})
        print(f"已清除{target_table} {date} 的数据")
        self.logger.log(f"已清除{target_table} {date} 的数据")


        df = self.read_data(date)
        print(f"Read {len(df)} rows for {date}")
        if df.empty:
            return
        result = self.process_grouping(df)
        # result.to_excel("result.xlsx",sheet_name="sheet_result", index=False)
        self.write_data(result, target_table)
        print(f"Wrote {len(result)} rows to {target_table}")
        self.logger.log(f"Wrote {len(result)} rows to {target_table}")



# 使用示例
if __name__ == "__main__":


    date_list = ['2025-08-25', '2025-08-26', '2025-08-27']  # 默认处理昨天
    target_table = "dws_profileid_group"

    processor = CaptureGroupProcessor(host=["localhost","localhost"], port=[9000,9000], user=["default","default"], password=["ck_test","ck_test"], database=["Facial","Facial"], prefix=target_table)

    for date in date_list:
        processor.process_one_day(date, target_table)


