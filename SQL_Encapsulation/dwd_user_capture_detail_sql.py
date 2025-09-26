from datetime  import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream import ClickHouseHandler


class dwd_user_capture_detail:

    def __init__(self, host=["localhost","localhost"], port=[9000,9000], user=["default","default"], password=["",""], database=["default","default"],target_table=None,date_list=[]):

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.target_table = target_table
        self.date_list = date_list



    def main(self):

        # date=datetime.strptime("2025-08-25", "%Y-%m-%d").date()

        # date_list=["2025-08-25","2025-08-26","2025-08-27"]
        # target_table = "dwd_user_capture_detail"



        delete_sql = f"alter table {self.target_table} delete where toDate(capture_time)=%(date)s"

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



        ch = ClickHouseHandler(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,prefix=self.target_table)

        for date in self.date_list:

            ch.delete_partition(delete_sql, self.target_table,{"date":date})
            ch.stream_query_insert(source_sql, self.target_table,{"date":date})









