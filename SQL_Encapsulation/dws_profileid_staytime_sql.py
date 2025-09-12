from datetime  import datetime
from ClickHouseHandler import ClickHouseHandler


if __name__ == "__main__":


    # date=datetime.strptime("2025-08-25", "%Y-%m-%d").date()

    date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]

    target_table = "dws_profileid_staytime"


    delete_sql = f"alter table  {target_table} delete where  date=%(date)s"


    source_sql = f"""
            select toDate(t1.capture_time) date
      ,toDate(t1.capture_time+21600 )  date_casino
      ,t1.profile_id profile_id
      ,t1.region_id  region_id
      ,t1.region_name region_name
      ,t1.region_type
      ,t1.member_tier
      ,gender
      ,case when age between 0 and 20 then '0-20'
            when age between 21 and 39 then '21-39'
            when age between 40 and 65 then '40-65'
            when age >65 then  '65+'
        end  Age_range
      ,  profile_type
     -- ,t1.camera_id  camera_id
     ,count(t1.camera_id) capture_count

--      如果在该区域无显示时间，则取该区域的平均时间作为这个人的停留时间
      ,case when toDecimal64(max(dateDiff(second , t1.capture_time,coalesce(t1.next_capture_time,t1.capture_time))),2)
              =toDecimal64(0,2)  then toDecimal64(max(t2.stay_time_avg),2)
           else toDecimal64(max(dateDiff(second , t1.capture_time,coalesce(t1.next_capture_time,t1.capture_time))),2)
          end stay_time
      ,now() batch_time
     from Facial.dwd_user_capture_detail t1
     left join (
        select
           AA.date
         , AA.date_casino
         , AA.region_id
         ,round(sum(stay_time)/count(distinct profile_id),2) stay_time_avg
        from
            (select  toDate(capture_time) date
                     ,toDate(capture_time+21600 ) date_casino
                       ,region_id
                       ,profile_id
                       ,max(dateDiff(second , capture_time,coalesce(next_capture_time,capture_time))) stay_time

                from Facial.dwd_user_capture_detail  where   next_capture_time is not null and 
                 toDate(capture_time) = %(date)s
                group by date,date_casino,region_id,profile_id) AA
        group by AA.date,AA.region_id, AA.date_casino
            )  t2
         on         toDate(t1.capture_time) = t2.date
                and t1.region_id=t2.region_id
                and toDate(t1.capture_time+21600 )= t2.date_casino
where toDate(t1.capture_time) = %(date)s
group by date,profile_id,region_id,date_casino,region_name,gender,Age_range,profile_type,t1.member_tier,region_type
        """


    ch = ClickHouseHandler(host='localhost', port=9000, user='default', password='ck_test', database='Facial',prefix=target_table)


    for date in date_list:

        ch.delete_partition(delete_sql, target_table,{"date":date})
        ch._insert_into_select(source_sql, target_table,{"date":date})
        # ch.stream_query_insert(source_sql, target_table,{"date":date},1000)







