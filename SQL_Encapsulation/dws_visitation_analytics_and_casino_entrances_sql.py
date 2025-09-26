from datetime  import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream import ClickHouseHandler


# if __name__ == "__main__":


class dws_visitation_analytics_and_casino_entrances:



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
        # target_table = "dws_visitation_analytics_and_casino_entrances"





        delete_sql = f"alter table  {self.target_table} delete where  toDate(date_hour)=%(date)s"

        source_sql = f"""
                    
                 
    with
    
    tab_split as (
        select
    
             profile_id
            ,region_type
            ,region_id
            ,region_name
            ,gender
            , case when age between 0 and 20 then '0-20'
                                     when age between 21 and 39 then '21-39'
                                     when age between 40 and 65 then '40-65'
                                     when age >65 then  '65+'
                               end  Age_range
            ,profile_type
            ,member_tier
            ,capture_time
            ,coalesce(next_capture_time,capture_time)
            ,dateDiff('hour',capture_time,coalesce(next_capture_time,capture_time)) hour_diff
            ,range(0, hour_diff + 1) AS split_indexes
            ,split_index
    
             ,if(
                split_index = 0,
                capture_time,
                toStartOfHour(capture_time) + split_index * 3600  -- 整点时间（秒为单位累加）
            ) AS corrected_start
            -- 修正后的结束时间：
            -- - 最后1行（index=hour_diff）用原始end_time
            -- - 中间行用下一小时的整点时间
            ,if(
                split_index = hour_diff,
                coalesce(next_capture_time,capture_time),
                toStartOfHour(capture_time) + (split_index + 1) * 3600  -- 下一小时整点
            ) AS corrected_end
    
    
        from dwd_user_capture_detail  t1
         ARRAY JOIN  split_indexes as split_index
        where  toDate(t1.capture_time)= %(date)s
    
    
        )
    
    
    select
    
           toDateTime(formatDateTime(corrected_start,'%%Y-%%m-%%d %%H:00:00')) date_hour
         , toDateTime(formatDateTime(corrected_start-21600,'%%Y-%%m-%%d %%H:00:00')) date_casino_hour
         , t1.profile_id profile_id
         , t1.region_type region_type
         , t1.region_id region_id
         , t1.region_name region_name
         , t1.gender gender
         , t1.Age_range Age_range
         , t1.profile_type profile_type
         , t1.member_tier member_tier
         , t1.capture_time capture_time
         , t1.hour_diff hour_diff
         , t1.corrected_start corrected_start
         , t1.corrected_end corrected_end
         , t2.group_id  group_id
         , t3.stay_time stay_time
         , t4.is_less15min_visitor is_less15min_visitor
         , t4.is_return_casino is_return_casino
         , t4.is_return_zone is_return_zone
         , now() batch_time
    from tab_split t1
    left join dws_profileid_group t2 on t1.corrected_start=t2.capture_time
                                              and t1.profile_id=t2.profile_id
                                              and t1.region_id=t2.region_id
    left join dws_profileid_staytime t3 on toDate(t1.corrected_start) = t3.date
                                              and toDate(t1.corrected_start - 21600) = t3.date_casino
                                              and t1.profile_id=t3.profile_id
                                              and t1.region_id=t3.region_id
    left join dws_profileid_NewOrReturn_visitor t4 on toDate(t1.corrected_start) = t4.date
                                              and toDate(t1.corrected_start - 21600) = t4.date_casino
                                              and t1.profile_id=t4.profile_id
                                              and t1.region_id=t4.region_id
    
       
                    
                               
    
        """

        ch = ClickHouseHandler(host=self.host, port=self.port, user=self.user, password=self.password,
                               database=self.database, prefix=self.target_table)

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
                    "dws_profileid_staytime","dws_visitation_analytics_and_casino_entrances"]
    date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]

    vase = dws_visitation_analytics_and_casino_entrances(host=host, port=port, user=user, password=password, database=database,
                                target_table=target_table[4], date_list=date_list)
    vase.main()






