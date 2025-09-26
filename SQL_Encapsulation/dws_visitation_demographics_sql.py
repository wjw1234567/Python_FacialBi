from datetime  import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream import ClickHouseHandler




class dws_visitation_demographics:


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
        # target_table = "dws_visitation_demographics"


        delete_sql = f"alter table  {self.target_table} delete where  date=%(date)s"

        source_sql = f"""
        
    select
               s1.date
             , s1.date_casino
             , s1.region_id
             , s1.region_name
             , s1.region_type
             , s1.gender
             , s1.Age_range
             , s1.profile_type
             , s1.member_tier
             , sum(s1.visitors_num)   visitors_num
             , sum(s1.stay_time)    stay_time
             , sum(s1.less15min_visitor_num)    less15min_visitor_num
             , sum(new_zone_num)  new_zone_num
             , sum(return_zone_num)  return_zone_num
             , sum(new_casino_num)  new_casino_num
             , sum(return_casino_num)  return_casino_num
    
             , now() batch_time
    
        from
    (select
                           toDate(capture_time) date
                          ,toDate(capture_time-21600) date_casino
                          ,region_id
                          ,region_name
                          ,region_type
                          ,gender
                          , case when age between 0 and 20 then '0-20'
                                 when age between 21 and 39 then '21-39'
                                 when age between 40 and 65 then '40-65'
                                 when age >65 then  '65+'
                           end  Age_range
                          ,profile_type
                          ,member_tier
                          , count(distinct profile_id) visitors_num
                          , toDecimal32(0,2) stay_time
                          , 0 less15min_visitor_num
    
                          , 0 new_zone_num
                          , 0 return_zone_num
                          , 0 new_casino_num
                          , 0 return_casino_num
    
    
    
    
    
                     from dwd_user_capture_detail
                     where date=%(date)s
                    group by date,region_id,region_name,gender,Age_range,profile_type,member_tier,region_type,date_casino
    union all
                     select  t1.date
                            ,t1.date_casino
                            , region_id
                            , region_name
                            , region_type
                            , gender
                            , Age_range
                            , profile_type
                            , member_tier
                            , 0 visitors_num
                            , sum(stay_time)    
                            , count(distinct  case when stay_time<15*60 then profile_id end) less15min_visitor_num
    
                            , 0 new_zone_num
                            , 0 return_zone_num
                            , 0 new_casino_num
                            , 0 return_casino_num
    
                     from dws_profileid_staytime t1
                      where date=%(date)s
                    group by date,region_id,region_name,gender,Age_range,profile_type,member_tier,region_type,date_casino
    
    
     union all
                 select
                      date
                     ,date_casino
                     ,region_id
                     ,region_name
                     ,region_type
                     ,gender
                     ,Age_range
                     ,profile_type
                     ,member_tier
                     , 0 visitors_num
                     , toDecimal32(0,2) stay_time
                     , 0 less15min_visitor_num
                     ,count(distinct case when is_return_zone = 0 then a1.profile_id end) new_zone_num
                     ,count(distinct case when is_return_zone = 1 then a1.profile_id end) return_zone_num
                     ,count(distinct case when is_return_casino = 0 then a1.profile_id end) new_casino_num
                     ,count(distinct case when is_return_casino = 1 then a1.profile_id end) return_casino_num
    
                from dws_profileid_NewOrReturn_visitor a1
                where date = %(date)s
                group by date,date_casino,region_id,region_name,region_type,gender,Age_range,profile_type,member_tier
    
        ) s1
    group by  s1.date, date_casino,region_id, region_name, region_type, gender, Age_range, profile_type, member_tier
    
    
                                  
        """

        ch = ClickHouseHandler(host=self.host, port=self.port, user=self.user, password=self.password,database=self.database, prefix=self.target_table)

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
    target_table = ["dwd_user_capture_detail", "dws_profileid_group", "dws_profileid_NewOrReturn_visitor",
                    "dws_profileid_staytime", "dws_visitation_analytics_and_casino_entrances","dws_visitation_demographics"]
    date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]

    vd = dws_visitation_demographics(host=host, port=port, user=user, password=password, database=database,target_table=target_table[5], date_list=date_list)
    vd.main()





