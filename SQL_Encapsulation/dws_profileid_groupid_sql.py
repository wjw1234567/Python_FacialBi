from datetime  import datetime
from ClickHouseHandler_stream import ClickHouseHandler




class dws_profileid_group:


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
        # date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]
        # target_table = "dws_profileid_group"



        delete_sql = f"alter table  {self.target_table} delete where  toDate(date_hour)=%(date)s"

        source_sql = f"""
                
    with groupid_tab as  (
    select AA.profile_id
         , camera_id
         , region_id
         , region_name
         , capture_time
         , timediff
         ,  sum(is_new_group) OVER (ORDER BY capture_time) AS group_id
     from
     (
        SELECT profile_id
             ,camera_id
             ,region_id
             ,region_name
             , capture_time
             , capture_time - lag(capture_time, 1, capture_time) OVER (ORDER BY capture_time) timediff
             ,multiIf(
                   capture_time - lag(capture_time, 1, capture_time) OVER (ORDER BY capture_time) > 2
                   -- and  capture_time - lead(capture_time, 1, capture_time) OVER (ORDER BY capture_time) > 3
                    ,1, 0) AS is_new_group
    
            FROM
    
            (
            select profile_id,camera_id,region_name,capture_time,region_id
                 ,row_number() over (partition by (formatDateTime(capture_time,'%%Y-%%m-%%d %%H:00:00'),profile_id) order by capture_time desc) rn
                 from
            dwd_user_capture_original t1
            WHERE toDate(capture_time) = %(date)s and camera_id = 94
            ORDER BY  capture_time ) s1 where s1.rn=1
        ) AA
    
    )
    
    
    
    select toDateTime(formatDateTime(capture_time,'%%Y-%%m-%%d %%H:00:00')) date_hour
          ,toDateTime(formatDateTime(capture_time - 21600,'%%Y-%%m-%%d %%H:00:00')) date_casino_hour
          ,profile_id
          ,region_id
          ,region_name
          ,group_id
          ,capture_time
          ,now() batch_time
    
    from groupid_tab
     order by group_id
    
    
    
        """



        ch = ClickHouseHandler(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,prefix=self.target_table)

        for date in self.date_list:

            ch.delete_partition(delete_sql, self.target_table,{"date":date})
            ch.stream_query_insert(source_sql, self.target_table,{"date":date})
            # ch.stream_query_insert(source_sql, target_table,{"date":date},1000)


if __name__ == "__main__":
    host = ['localhost', 'localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database = ['Facial', 'Facial']
    target_table = ["dwd_user_capture_detail", "dws_profileid_group"]
    date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]

    pg = dws_profileid_group(host=host, port=port, user=user, password=password, database=database,
                               target_table=target_table[1], date_list=date_list)
    pg.main()





