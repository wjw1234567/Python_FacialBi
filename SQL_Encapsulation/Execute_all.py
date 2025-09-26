from dwd_user_capture_detail_sql import dwd_user_capture_detail
from dws_profileid_groupid_sql  import dws_profileid_group
from dws_profileid_NewOrReturn_visitor_sql import dws_profileid_NewOrReturn_visitor
from dws_profileid_staytime_sql import dws_profileid_staytime
from dws_visitation_analytics_and_casino_entrances_sql import dws_visitation_analytics_and_casino_entrances
from dws_visitation_demographics_sql import dws_visitation_demographics
from dws_visitor_path_track_heatmap_sql import dws_visitor_path_track_heatmap
from dws_visitor_path_track_heatmap import TrackHeatmap
import schedule
import time


def run_jobs():
    host=['localhost','localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database=['Facial', 'Facial']
    target_table=["dwd_user_capture_detail"
                 ,"dws_profileid_group"
                 ,"dws_profileid_staytime"
                 ,"dws_profileid_NewOrReturn_visitor"
                 ,"dws_visitation_analytics_and_casino_entrances"
                 ,"dws_visitation_demographics"
                 ,"dws_visitor_path_track_heatmap"
                  ]
    date_list=["2025-08-25","2025-08-26","2025-08-27"]


    cd=dwd_user_capture_detail(host=host, port=port, user=user, password=password, database=database,target_table=target_table[0],date_list=date_list)
    cd.main()

    pg=dws_profileid_group(host=host, port=port, user=user, password=password, database=database,target_table=target_table[1],date_list=date_list)
    pg.main()


    ps = dws_profileid_staytime(host=host, port=port, user=user, password=password, database=database,
                                target_table=target_table[2], date_list=date_list)
    ps.main()



    pnv = dws_profileid_NewOrReturn_visitor(host=host, port=port, user=user, password=password, database=database,
                             target_table=target_table[3], date_list=date_list)
    pnv.main()



    vase = dws_visitation_analytics_and_casino_entrances(host=host, port=port, user=user, password=password,
                                                         database=database,target_table=target_table[4], date_list=date_list)
    vase.main()



    vd = dws_visitation_demographics(host=host, port=port, user=user, password=password, database=database,
                                     target_table=target_table[5], date_list=date_list)
    vd.main()



    '''
        热力图有2种实现方法SQL和Pandas
    '''


    # pth = dws_visitor_path_track_heatmap(host=host, port=port, user=user, password=password, database=database,
    #                                      target_table=target_table[6], date_list=date_list)
    # pth.main()


    trackheatmap = TrackHeatmap(host=host, port=port, user=user, password=password, database=database, prefix=target_table[6])
    trackheatmap.process_main(target_table[6], date_list)



def main():
    # 每4小时执行一次 run_jobs 函数
    schedule.every(4).hours.do(run_jobs)

    # 循环监听定时任务
    while True:
        schedule.run_pending()  # 运行所有待执行的任务
        time.sleep(60)  # 每60秒检查一次是否有任务需要执行



if __name__ == "__main__":
    main()




