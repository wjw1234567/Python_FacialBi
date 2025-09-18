from clickhouse_driver import Client
from Logger import Logger



class ClickHouseHandler:
    def __init__(self, host=['localhost','localhost'], port=[9000,9000], user=['default','default'], password=['',''], database=['default','default'],prefix=None):

        self.host=host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.prefix = prefix


        self.read_client = Client(host=host[0], port=port[0], user=user[0], password=password[0],database=database[0])
        self.wd_client=Client(host=host[1], port=port[1], user=user[1], password=password[1],database=database[1])
        self.logger = Logger(log_dir="./logs", prefix=self.prefix)




    def stream_query_insert(self, source_sql: str, target_table: str,condict:dict, batch_size: int = 10000):
        """
        安全版流式查询 ClickHouse，边生成边写入目标表
        自动使用查询列名作为 INSERT 的列
        使用独立连接，避免 PartiallyConsumedQueryError
        """
        # 建立独立连接，避免与其他操作冲突，读取操作
        with Client(host=self.host[0], port=self.port[0], user=self.user[0], password=self.password[0],database=self.database[0]) as client:

            # 获取查询列名
            column_names = self._get_query_columns(source_sql, client,condict)
            print(f"使用列名: {column_names}")

            batch = []
            for row in client.execute_iter(source_sql,params=condict):
                batch.append(row)
                # print("batch=",batch[1])
                if len(batch) >= batch_size:
                    self._insert_batch(target_table, column_names, batch)
                    batch.clear()

            # 写入剩余数据
            if batch:
                self._insert_batch(target_table, column_names, batch)

    def _get_query_columns(self, sql: str, client: Client,condict:dict):
        """
        获取 SQL 查询列名
        """
        # LIMIT 0 获取列信息，不拉取数据
        res = self.read_client.execute(f"SELECT * FROM ({sql}) LIMIT 0", with_column_types=True,params=condict)
        columns = [col[0] for col in res[1]]
        return columns




    def delete_partition(self, delete_sql: str, table_name: str,condict:dict):
        """
        删除目标表分区数据
        """
        try:
            with self.wd_client as client:
                print(f"执行删除: {delete_sql}")
                client.execute(delete_sql,params=condict)
                print(f"{condict.get('date','')} 已执行完成删除 {table_name}  的数据")
                self.logger.log(f"{condict.get('date','')} 已执行完成删除 {table_name}  的数据")


        except Exception as e:

            self.logger.error(f"执行delete语句出错: {type(e).__name__}: {str(e)}")



    # 配合stream_query_insert流式查询，需要加工的采取这种方式，可支配的
    def _insert_batch(self, table_name: str, columns: list, data: list):
        """
        批量写入 ClickHouse
        """

        try:

            if not data:
                return
            col_str = ', '.join(columns)
            with self.wd_client as client:


                client.execute(f"INSERT INTO {table_name} ({col_str}) VALUES", data)
            print(f"已写入 {len(data)} 行到 {table_name}")
            self.logger.log(f"已写入 {len(data)} 行到 {table_name}")

        except Exception as e:

            self.logger.error(f"执行_insert_batch出错: {type(e).__name__}: {str(e)}")







# ===================== 使用示例 =====================
if __name__ == "__main__":


    date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]
    target_table = "dws_visitation_analytics_and_casino_entrances"

    ch = ClickHouseHandler(host=['localhost','localhost'], port=[9000,9000], user=['default','default'], password=['ck_test','ck_test'], database=['Facial','Facial'])

    delete_sql=f"alter table {target_table} delete where date=%(date)s"

    source_sql = f"""
                select            
                      formatDateTime(capture_time,'%%Y-%%m-%%d %%H:00:00') date
                        ,formatDateTime(capture_time,'%%H:00') date_hour
                        ,formatDateTime(capture_time+21600,'%%Y-%%m-%%d %%H:00:00') date_casino
                        ,formatDateTime(capture_time+21600,'%%H:00') date_casino_hour
                      ,region_id
                      ,region_name
                      ,region_type
                      , count(distinct person_id) visitors_num
                      , now() batch_time
                 from dwd_user_capture_detail
                where toDate(capture_time) = %(date)s
                group by date,region_id,region_name,region_type,date_hour,date_casino,date_casino_hour

    """


    for date in date_list:

        ch.delete_partition(delete_sql, target_table,{"date":date})
        ch.stream_query_insert(source_sql, target_table,{"date":date})
        # ch.stream_query_insert(source_sql, target_table,{"date":date},1000)

