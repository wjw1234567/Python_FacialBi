from datetime  import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream import ClickHouseHandler

# if __name__ == "__main__":
class dws_profileid_staytime:


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
        # target_table = "dws_profileid_staytime"


        delete_sql = f"alter table  {self.target_table} delete where  date=%(date)s"


        source_sql = f"""
                select toDate(t1.capture_time) date
          ,toDate(t1.capture_time-21600 )  date_casino
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
         from dwd_user_capture_detail t1
         left join (
            select
               AA.date
             , AA.date_casino
             , AA.region_id
             ,round(sum(stay_time)/count(distinct profile_id),2) stay_time_avg
            from
                (select  toDate(capture_time) date
                         ,toDate(capture_time-21600 ) date_casino
                           ,region_id
                           ,profile_id
                           ,max(dateDiff(second , capture_time,coalesce(next_capture_time,capture_time))) stay_time
    
                    from dwd_user_capture_detail  where   next_capture_time is not null and 
                     toDate(capture_time) = %(date)s
                    group by date,date_casino,region_id,profile_id) AA
            group by AA.date,AA.region_id, AA.date_casino
                )  t2
             on         toDate(t1.capture_time) = t2.date
                    and t1.region_id=t2.region_id
                    and toDate(t1.capture_time-21600 )= t2.date_casino
    where toDate(t1.capture_time) = %(date)s
    group by date,profile_id,region_id,date_casino,region_name,gender,Age_range,profile_type,t1.member_tier,region_type
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
    target_table = ["dwd_user_capture_detail", "dws_profileid_group", "dws_profileid_NewOrReturn_visitor","dws_profileid_staytime"]
    date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]

    ps = dws_profileid_staytime(host=host, port=port, user=user, password=password, database=database,
                                            target_table=target_table[3], date_list=date_list)
    ps.main()



