from datetime  import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream import ClickHouseHandler

if __name__ == "__main__":


    # date=datetime.strptime("2025-08-25", "%Y-%m-%d").date()

    date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]

    target_table = "dws_visitation_demographics"
    delete_sql = f"alter table  {target_table} delete where  date=%(date)s"

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
                      ,toDate(capture_time+21600) date_casino
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

                  a1.date
                 ,a1.date_casino
                 ,a1.region_id
                 ,a1.region_name
                 ,a1.region_type
                 ,a1.gender
                 ,a1.Age_range
                 ,a1.profile_type
                 ,a1.member_tier
                 , 0 visitors_num
                 , toDecimal32(0,2) stay_time
                 , 0 less15min_visitor_num

                 ,count(distinct case when is_return_zone = 0 then a1.profile_id end) new_zone_num
                 ,count(distinct case when is_return_zone = 1 then a1.profile_id end) return_zone_num
                 ,count(distinct case when is_return_casino = 0 then a1.profile_id end) new_casino_num
                 ,count(distinct case when is_return_casino = 1 then a1.profile_id end) return_casino_num

                from
                (
                    select s1.date date
                     ,s1.date_casino date_casino
                     ,s1.profile_id profile_id
                     ,s1.region_id region_id
                     ,s1.region_name region_name
                     ,s1.region_type region_type
                     ,s1.gender gender
                     ,s1.Age_range Age_range
                     ,s1.profile_type profile_type
                     , s1.member_tier member_tier
                     ,s2.is_return is_return_zone
                     ,s3.is_return is_return_casino

                from (select * from dws_profileid_staytime where date = %(date)s) s1
                 left join (select toString(date) date,profile_id,region_id,count(distinct profile_id) is_return from dws_profileid_staytime group by date,profile_id,region_id) s2
                    on s2.date=toString(s1.date- interval 2 month) and s2.profile_id=s1.profile_id and s2.region_id=s1.region_id
                 left join (select toString(date_casino) date_casino,profile_id,region_id,count(distinct profile_id) is_return from dws_profileid_staytime group by date_casino,profile_id,region_id) s3
                    on s3.date_casino=toString(s1.date- interval 1 DAY) and s3.profile_id=s1.profile_id and s3.region_id=s1.region_id ) a1

            group by a1.date,a1.date_casino,a1.region_id,a1.region_name,a1.region_type,a1.gender,a1.Age_range,a1.profile_type,a1.member_tier

    ) s1
group by  s1.date, date_casino,region_id, region_name, region_type, gender, Age_range, profile_type, member_tier


                              
    """



    ch = ClickHouseHandler(host=['localhost','localhost'], port=[9000,9000], user=['default','default'], password=['ck_test','ck_test'], database=['Facial','Facial'],prefix=target_table)

    for date in date_list:

        ch.delete_partition(delete_sql, target_table,{"date":date})
        ch.stream_query_insert(source_sql, target_table,{"date":date})
        # ch.stream_query_insert(source_sql, target_table,{"date":date},1000)








