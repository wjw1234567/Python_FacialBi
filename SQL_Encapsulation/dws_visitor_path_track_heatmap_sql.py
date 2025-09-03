from datetime  import datetime
from ClickHouseHandler import ClickHouseHandler


if __name__ == "__main__":


    # date=datetime.strptime("2025-08-25", "%Y-%m-%d").date()

    date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]

    target_table = "dws_visitor_path_track_heatmap"
    delete_sql = f"alter table  {target_table} delete where  date=%(date)s"

    source_sql = f"""
                
  SELECT
        toDate(v1.capture_time) date,
        formatDateTime(v1.capture_time,'%%H:00') date_hour,
        v1.profile_id,
        v1.region_name,
        v1.capture_time z0_time,
         /* 2) 计算动态 offset，并分桶 判定*/
        case when
               v1.capture_time != v2.capture_time and
                 dateDiff('minute', v1.capture_time, v2.capture_time) between 0 and 15
            then  (intDiv(dateDiff('minute', v1.capture_time, v2.capture_time), 5)+1) * 5

             when dateDiff('minute', v1.capture_time, v2.capture_time) >= -15
                      and  dateDiff('minute', v1.capture_time, v2.capture_time) < 0
            then  (intDiv(dateDiff('minute', v1.capture_time, v2.capture_time), 5)-1) * 5

             when dateDiff('minute', v1.capture_time, v2.capture_time) between 16 and 60
             then  (intDiv(dateDiff('minute', v1.capture_time, v2.capture_time), 15)+1) * 15
             when dateDiff('minute', v1.capture_time, v2.capture_time) between -60 and -16
             then  (intDiv(dateDiff('minute', v1.capture_time, v2.capture_time), 15)-1) * 15
            when dateDiff('minute', v1.capture_time, v2.capture_time) = 0 then 0
            end  AS off_bin

        /*
         找出每个区域作为基准点最近时间差的区域
         */
        ,argMin(v2.region_name, abs(dateDiff('minute', v1.capture_time, v2.capture_time))) AS area_at_off

        , v1.member_tier
        , case when v1.age between 0 and 20 then '0-20'
             when v1.age between 21 and 39 then '21-39'
            when v1.age between 40 and 65 then '40-65'
            when v1.age>65 then '65+'
          end age_range
         , v1.gender

            FROM (select profile_id,capture_time,member_tier,age,gender,region_name
                  from  Facial.dwd_user_capture_original
                  where toDate(capture_time) =  %(date)s
                ) v1
            JOIN  (select profile_id,capture_time,member_tier,age,gender,region_name
                  from  Facial.dwd_user_capture_original
                  where toDate(capture_time) =  %(date)s
                ) v2
                ON v1.profile_id = v2.profile_id
               AND toDate(v1.capture_time)      = toDate(v2.capture_time)
               AND abs(dateDiff('minute', v1.capture_time, v2.capture_time)) <= 60
--     where v1.profile_id ='p103'  and toDate(v1.capture_time)=toDate('2025-08-20')
            and off_bin IN [-60, -45, -30, -15, -10, -5, 0, 5, 10, 15, 30, 45, 60]
            GROUP BY date,date_hour, v1.profile_id, v1.region_name, v1.capture_time, off_bin,v1.member_tier,age_range,v1.gender

    """



    ch = ClickHouseHandler(host='localhost', port=9000, user='default', password='ck_test', database='Facial')

    for date in date_list:

        ch.delete_partition(delete_sql, target_table,{"date":date})
        ch._insert_into_select(source_sql, target_table,{"date":date})
        # ch.stream_query_insert(source_sql, target_table,{"date":date},1000)








