from datetime  import datetime
from ClickHouseHandler import ClickHouseHandler


if __name__ == "__main__":


    date=datetime.strptime("2025-08-25", "%Y-%m-%d").date()

    target_table = "dwd_user_capture_detail"
    delete_sql = f"alter table {target_table} delete where toDate(capture_time)=%(date)s"

    source_sql = f"""
                 select
           profile_id
          , person_id
          ,profile_type
          ,member_tier
          ,member_id
          ,is_delete
          ,person_status
          ,album_id
          ,merge_count
          ,face_count
          ,identify_num
          ,card_type
          ,address
          ,name
          ,age
          ,gender
          ,capture_id
          ,region_id
          ,region_name
          ,region_type
          ,camera_id
          ,capture_time
         , lead(toNullable(capture_time),1,toNullable(null)) over(partition by (toDate(capture_time),A1.profile_id)  ORDER BY profile_id,capture_time) next_capture_time
         , now() batch_time
        from
    (
        select
              person_id
          ,camera_id
          ,capture_time
          , profile_id
          ,profile_type
          ,member_tier
          ,member_id
          ,is_delete
          ,person_status
          ,album_id
          ,merge_count
          ,face_count
          ,identify_num
          ,card_type
          ,address
          ,name
          ,age
          ,gender
          ,capture_id
          ,region_id
          ,region_name
          ,'' region_type
          , lag(region_id,1,0) over(partition by (toDate(capture_time),t1.profile_id) order by profile_id,capture_time) last_region_id
    from dwd_user_capture_original t1
    where toDate(t1.capture_time)=%(date)s
        order by profile_id,capture_time
        ) A1 where region_id <> last_region_id order by profile_id,capture_time

    """



    ch = ClickHouseHandler(host='localhost', port=9000, user='default', password='ck_test', database='Facial')

    ch.delete_partition(delete_sql, target_table,{"date":date})
    ch._insert_into_select(source_sql, target_table,{"date":date})
    # ch.stream_query_insert(source_sql, target_table,{"date":date},1000)









