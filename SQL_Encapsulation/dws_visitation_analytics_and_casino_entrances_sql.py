from datetime  import datetime
from ClickHouseHandler import ClickHouseHandler


if __name__ == "__main__":


    date=datetime.strptime("2025-08-25", "%Y-%m-%d").date()

    target_table = "dws_visitation_analytics_and_casino_entrances"
    delete_sql = f"alter table  {target_table} delete where  date=%(date)s"

    source_sql = f"""
                select formatDateTime(capture_time,'%Y-%m-%d %H:00:00') date
                      ,region_id
                      ,region_name
                      ,region_type
                      , count(distinct person_id) visitors_num
                      , now() batch_time
                 from Facial.dwd_user_capture_detail
                group by date,region_id,region_name,region_type

    """



    ch = ClickHouseHandler(host='localhost', port=9000, user='default', password='ck_test', database='Facial')

    ch.delete_partition(delete_sql, target_table,{"date":date})
    ch._insert_into_select(source_sql, target_table,{"date":date})
    # ch.stream_query_insert(source_sql, target_table,{"date":date},1000)









