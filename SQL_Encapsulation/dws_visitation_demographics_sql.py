from datetime  import datetime
from ClickHouseHandler import ClickHouseHandler


if __name__ == "__main__":


    # date=datetime.strptime("2025-08-25", "%Y-%m-%d").date()

    date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]

    target_table = "dws_visitation_demographics"
    delete_sql = f"alter table  {target_table} delete where  date=%(date)s"

    source_sql = f"""
                  
                select
                       toDate(capture_time) date
                      ,region_id
                      ,region_name
                      ,0 region_type
                      ,gender
                      , case when age between 0 and 20 then '0-20'
                             when age between 21 and 39 then '21-39'
                             when age between 40 and 65 then '40-65'
                             when age >65 then  '65+'
                       end  Age_range
                      ,profile_type
                      ,member_tier
                      , count(distinct profile_id) visitors_num
                      , now() batch_time
                 from dwd_user_capture_detail
                 where date=%(date)s
                group by date,region_id,region_name,gender,Age_range,profile_type,member_tier
                
    """



    ch = ClickHouseHandler(host='localhost', port=9000, user='default', password='ck_test', database='Facial')

    for date in date_list:

        ch.delete_partition(delete_sql, target_table,{"date":date})
        ch._insert_into_select(source_sql, target_table,{"date":date})
        # ch.stream_query_insert(source_sql, target_table,{"date":date},1000)








