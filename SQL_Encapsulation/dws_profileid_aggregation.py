from clickhouse_driver import Client
import pandas as pd
import numpy as np

class UserCaptureProcessor:
    def __init__(self, host="localhost", port=9000, user="default", password="", database="default"):
        self.client = Client(host=host, port=port, user=user, password=password, database=database)

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
        cursor = self.client.execute_iter(sql, with_column_types=True)
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



    def assign_group_num(
            self,
            df: pd.DataFrame,
            entrance_camera_ids: list,
            prev_group_state: dict = None
    ):
        """
        按 camera_id 分组，在同一个入口摄像头上，判断 profile_id 是否属于同一组
        支持跨 chunk：通过 prev_group_state 传递每个摄像头的最后 group_id 和最后时间

        参数:
            df: 当前 chunk 的 DataFrame
            entrance_camera_ids: 入口摄像头 id 列表
            prev_group_state: dict，格式 {camera_id: {"last_group_id": int, "last_time": timestamp}}

        返回:
            df: 带 group_num 的 DataFrame
            new_group_state: dict，更新后的状态
        """
        if prev_group_state is None:
            prev_group_state = {}

        df = df.sort_values("capture_time").copy()
        df["group_num"] = None

        # 仅处理入口摄像头的数据
        mask = df["camera_id"].isin(entrance_camera_ids)
        entrance_df = df[mask].copy()

        if entrance_df.empty:
            return df, prev_group_state

        results = []

        # 每个 camera_id 单独分组处理
        for cam_id, g in entrance_df.groupby("camera_id", group_keys=False):
            g = g.sort_values("capture_time")
            # print(f"cam_id={cam_id}")

            state = prev_group_state.get(cam_id, {"last_group_id": 0, "last_time": None})
            group_id = state["last_group_id"]
            prev_t = state["last_time"]

            group_ids = []
            for t in g["capture_time"]:
                if prev_t is None or (t - prev_t).total_seconds() > 3:
                    group_id += 1
                group_ids.append(group_id)
                prev_t = t

            # 增一列的列名为 group_num，这一列的值来自变量 group_ids
            g = g.assign(group_num=group_ids)

            # 更新 state
            prev_group_state[cam_id] = {"last_group_id": group_id, "last_time": prev_t}
            results.append(g)

        # 合并分组结果
        if results:
            entrance_df = pd.concat(results, ignore_index=True)
            df.loc[mask, "group_num"] = entrance_df["group_num"].values

        return df, prev_group_state






    def write_data(self, df: pd.DataFrame, target_table: str, batch_size: int = 10000):
        """
        批量写入 ClickHouse
        """
        columns = ["profile_id", "profile_type", "member_tier", "age", "gender",
                   "camera_id", "region_id", "region_name", "stay_time", "group_num"]
        df = df[columns]
        for start in range(0, len(df), batch_size):
            batch_df = df.iloc[start:start + batch_size]
            data = [tuple(None if pd.isna(x) else x for x in row) for row in batch_df.to_numpy()]
            self.client.execute(
                f"INSERT INTO {target_table} ({','.join(columns)}) VALUES",
                data
            )



    def process_one_day_stream(self, date_str: str, target_table: str, entrance_camera_ids: list,
                                   chunk_size: int = 100000):


            leftover = pd.DataFrame()
            prev_group_id = 0
            prev_entrance_time = None

            for chunk_df in self.read_data_by_date_stream(date_str, chunk_size=chunk_size):
                if chunk_df.empty:
                    continue

                # 拼接 leftover，保证 profile_id & group_num 跨 chunk 正确
                if not leftover.empty:
                    chunk_df = pd.concat([leftover, chunk_df], ignore_index=True)

                processed = self.calc_stay_time(chunk_df)

                # 更新 leftover：每个 profile 最后一条
                leftover = processed.groupby("profile_id").tail(1)

                # main_df 去掉 leftover
                main_df = processed.drop(leftover.index)

                if not main_df.empty:
                    main_df, prev_group_id, prev_entrance_time = self.assign_group_num(
                        main_df, entrance_camera_ids, prev_group_id, prev_entrance_time
                    )
                    self.write_data(main_df, target_table)


            # 最后一批 leftover
            if not leftover.empty:
                leftover, prev_group_id, prev_entrance_time = self.assign_group_num(
                    leftover, entrance_camera_ids, prev_group_id, prev_entrance_time
                )
                self.write_data(leftover, target_table)

            print(f"{date_str} 数据处理完成，写入 {target_table}")




if __name__ == "__main__":
    processor = UserCaptureProcessor(host="localhost", port=9000,user="default", password="ck_test", database="Facial")
    # 举例：处理 2025-09-01 的数据
    # processor.process_one_day(
    #     date_str="2025-09-01",
    #     target_table="dws_user_stay_and_group",
    #     entrance_camera_ids=[1001, 1002]  # 赌场入口摄像头ID
    # )






    entrance_camera_ids = [33, 82,133,84,62,53,21,83,12]

    result=processor.read_data_by_date_stream('2025-08-25')
    result_list = []
    for r in result:
        rc=processor.calc_stay_time(r)
        # print(rc[['profile_id','region_name','capture_time','stay_time']].head(10))
        df, prev_group_state =processor.assign_group_num(rc,entrance_camera_ids,{})
        df=df.dropna(subset=["stay_time", "group_num"], how="all")
        print(f"df={df[['profile_id','region_name','camera_id','capture_time','stay_time','group_num']]},prev_group_state={prev_group_state}")
        # result_list.append(df)
    # pd_all=pd.concat(result_list)
    # pd_all.to_excel('data.xlsx', sheet_name='表1', index=False)