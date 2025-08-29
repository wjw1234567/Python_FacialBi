import pandas as pd
from collections import deque
from clickhouse_driver import Client
from datetime import datetime



# ==========================
# 1. ClickHouse 连接
# ==========================


# 连接 ClickHouse
client = Client(
    host='localhost',
    port=9000,
    user='default',
    password='ck_test',
    database='Facial',
    send_receive_timeout=60   # 避免写入超时
)


# ==========================
# 2. 滑窗处理函数 (deque 优化版)
# ==========================
def process_partition(date, person_id, client):
    """
    针对单个 (date, person_id) 分区进行滑窗处理（deque优化版）
    """
    query = f"""
    
    SELECT toDate(capture_time) date
       ,profile_id
       ,region_name
       ,capture_time
FROM  dwd_user_capture_original_example
where date='{date}'  and profile_id= '{person_id}'
order by capture_time;
    
    
    
    """


    df,columns_with_types = client.execute(query, with_column_types=True)
    columns = [col[0] for col in columns_with_types]
    df = pd.DataFrame(df, columns=columns)

    # print(df)

    if df.empty:
        return None

    df["capture_time"] = pd.to_datetime(df["capture_time"])
    records = df.to_dict("records")

    target_bins = [-60, -45, -30, -15, -10, -5, 0, 5, 10, 15, 30, 45, 60]
    results = []

    window = deque()   # 存放 (idx, record)
    n = len(records)

    right = 0
    for i, v1 in enumerate(records):
        t1 = v1["capture_time"]
        base_area = v1["region_name"]

        # print(f"v1={v1}")
        '''
            遍历每个点 (i, v1)
            以当前点 t1 为“中心”，维护一个时间窗口。

            右指针扩张窗口
            把 capture_time 在 [t1, t1+60分钟] 内的点加入 deque。
            👉 保证右边不会漏掉未来 1 小时内的点。

            左指针收缩窗口
            把 < t1-60分钟 的点移出窗口。
            👉 保证窗口内的点始终在 [t1-60分钟, t1+60分钟]。

            窗口结果
            此时 window 中的所有点，都是与当前点 t1 时间差不超过 60 分钟的记录。
            你可以在这里对窗口里的点做聚合、计数、分组等操作。
        '''

        # --- 1) 滑动右指针，把 <= t1+60 的点加入 deque
        while right < n and records[right]["capture_time"] <= t1 + pd.Timedelta(minutes=60):
            window.append((right, records[right]))
            right += 1

        # --- 2) 滑动左指针，把 < t1-60 的点踢出 deque
        while window and window[0][1]["capture_time"] < t1 - pd.Timedelta(minutes=60):
            window.popleft()

        # print("window=",window)

        # --- 3) 遍历窗口候选，更新每个 off_bin 最近点 (argMin)
        bucket_best = {}

        for idx, v2 in window:
            # if idx == i:  # 不跳过自己
            #     continue
            diff_min = (v2["capture_time"] - t1).total_seconds() / 60
            # off_bin = int(diff_min // 15) * 15

            if diff_min >= 0 and diff_min <= 15 and v2["capture_time"] != t1:
                off_bin = (int(diff_min // 5) + 1) * 5
            elif diff_min == 0 and v2["capture_time"] == t1:
                off_bin =0
            elif diff_min >= -15 and diff_min < 0:
                off_bin = (int(diff_min // 5)) * 5
            elif diff_min >= 16 and diff_min <= 60:
                off_bin = (int(diff_min // 15) + 1) * 15
            elif diff_min >= -60 and diff_min <= -16:
                off_bin = (int(diff_min // 15)) * 15


            if off_bin not in target_bins:
                continue
            if (off_bin not in bucket_best or abs(diff_min) < abs(bucket_best[off_bin]["diff_min"])):
                bucket_best[off_bin] = {"region_name": v2["region_name"], "diff_min": diff_min}
        # print(t1, base_area, bucket_best)




        # --- 4) 生成输出行
        for off_bin,value in bucket_best.items():
            row = {
                "date": date,
                "profile_id": person_id,
                "region_name": base_area,
                "z0_time": t1,
                "off_bin":off_bin,
                "area_at_off":value["region_name"],
                "batch_time":datetime.now()
            }
            results.append(row)




        # --- 4) 生成输出行
        # row = {
        #     "date": date,
        #     "profile_id": person_id,
        #     "region_name": base_area,
        #     "z0_time": t1
        # }
        # for off in target_bins:
        #     if off == 0:
        #         row["z0"] = base_area
        #     else:
        #         row[f"z{off:+d}"] = bucket_best.get(off, {}).get("region_name")
        # results.append(row)

    if not results:
        return None
    return pd.DataFrame(results)



def insert_df(client, table: str, df: pd.DataFrame):
    """
    将 pandas.DataFrame 批量写入 ClickHouse
    """
    if df.empty:
        return

    # DataFrame 列名（必须和 ClickHouse 表结构一致）
    columns = list(df.columns)

    escaped_columns = [f"`{col}`" for col in columns]

    # 转换成 list[tuple]
    data = [tuple(row) for row in df.itertuples(index=False, name=None)]

    insert_sql = f"INSERT INTO {table} ({','.join(escaped_columns)}) VALUES"
    client.execute(insert_sql, data)  # 第二个参数直接传入数据列表


    # 调用 clickhouse-connect 的 insert
    # client.insert(table, data, column_names=columns)




# ==========================
# 3. 主流程：分批读取 → 处理 → 写回
# ==========================

# where date=toDate('2025-08-20') and profile_id='p103'

def main():
    # 先拿到所有分区键

    query = f"""
             SELECT DISTINCT toDate(capture_time) date, profile_id
             FROM dwd_user_capture_original_example
             
             ORDER BY date, profile_id
         """

    # partitions = client.execute(query, with_column_types=True)

    partitions, columns_with_types = client.execute(query, with_column_types=True)
    columns = [col[0] for col in columns_with_types]

    partitions = pd.DataFrame(partitions, columns=columns)

    batch_size = 5000  # 每批写 500 行
    buffer = []

    for _, row in partitions.iterrows():
        date = row["date"]
        person_id = row["profile_id"]

        df_out = process_partition(date, person_id, client)
        if df_out is None or df_out.empty:
            continue

        buffer.append(df_out)

        # --- 分批写回 ClickHouse
        if sum(len(x) for x in buffer) >= batch_size:
            big_df = pd.concat(buffer, ignore_index=True)
            insert_df(client,"dws_visitor_path_track_heatmap", big_df)
            print(f"写入 {len(big_df)} 行")
            buffer.clear()

    # --- 处理剩余数据
    if buffer:
        big_df = pd.concat(buffer, ignore_index=True)
        insert_df(client,"dws_visitor_path_track_heatmap", big_df)
        print(f"写入 {len(big_df)} 行 (最后一批)")






if __name__ == "__main__":
    main()
