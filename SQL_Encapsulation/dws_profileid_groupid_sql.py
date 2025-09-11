from datetime  import datetime
from ClickHouseHandler import ClickHouseHandler


if __name__ == "__main__":


    # date=datetime.strptime("2025-08-25", "%Y-%m-%d").date()
    date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]



    target_table = "dws_profileid_group"
    delete_sql = f"alter table  {target_table} delete where  toDate(date_hour)=%(date)s"

    source_sql = f"""
            
with groupid_tab as  (
select AA.profile_id
     , camera_id
     , region_name
     , capture_time
     , timediff
     ,  sum(is_new_group) OVER (ORDER BY capture_time) AS group_id
 from
 (
    SELECT profile_id
         ,camera_id
         ,region_name
         , capture_time
         , capture_time - lag(capture_time, 1, capture_time) OVER (ORDER BY capture_time) timediff
         ,multiIf(
               capture_time - lag(capture_time, 1, capture_time) OVER (ORDER BY capture_time) > 2
               -- and  capture_time - lead(capture_time, 1, capture_time) OVER (ORDER BY capture_time) > 3
                ,1, 0) AS is_new_group

        FROM

        (
        select profile_id,camera_id,region_name,capture_time
             ,row_number() over (partition by (formatDateTime(capture_time,'%%Y-%%m-%%d %%H:00:00'),profile_id) order by capture_time desc) rn
             from
        dwd_user_capture_original t1
        WHERE toDate(capture_time) = %(date)s and camera_id = 94
        ORDER BY  capture_time ) s1 where s1.rn=1
    ) AA

)



select formatDateTime(capture_time,'%%Y-%%m-%%d %%H:00:00') date_hour
      ,formatDateTime(capture_time+21600,'%%Y-%%m-%%d %%H:00:00') date_casino_hour
      ,group_id

      ,CASE
            WHEN count(distinct profile_id) = 1 THEN '1'
            WHEN count(distinct profile_id) = 2 THEN '2'
            WHEN count(distinct profile_id) = 3 THEN '3'
            WHEN count(distinct profile_id) = 4 THEN '4'
            WHEN count(distinct profile_id) = 5 THEN '5'
            ELSE '5+'
        END AS group_type
      ,count(distinct profile_id) group_size
      ,now() batch_time

from groupid_tab
group by date_hour,date_casino_hour,group_id order by group_id



    """



    ch = ClickHouseHandler(host='localhost', port=9000, user='default', password='ck_test', database='Facial',prefix=target_table)

    for date in date_list:

        ch.delete_partition(delete_sql, target_table,{"date":date})
        ch._insert_into_select(source_sql, target_table,{"date":date})
        # ch.stream_query_insert(source_sql, target_table,{"date":date},1000)









