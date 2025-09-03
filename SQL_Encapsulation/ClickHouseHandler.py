from clickhouse_driver import Client

class ClickHouseHandler:
    def __init__(self, host='localhost', port=9000, user='default', password='', database='default'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def stream_query_insert(self, source_sql: str, target_table: str,condict:dict, batch_size: int = 10000):
        """
        安全版流式查询 ClickHouse，边生成边写入目标表
        自动使用查询列名作为 INSERT 的列
        使用独立连接，避免 PartiallyConsumedQueryError
        """
        # 建立独立连接，避免与其他操作冲突
        with Client(host=self.host, port=self.port, user=self.user,
                    password=self.password, database=self.database) as client:

            # 获取查询列名
            column_names = self._get_query_columns(source_sql, client,condict)
            print(f"使用列名: {column_names}")

            batch = []
            for row in client.execute_iter(source_sql,params=condict):
                batch.append(row)
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
        res = client.execute(f"SELECT * FROM ({sql}) LIMIT 0", with_column_types=True,params=condict)
        columns = [col[0] for col in res[1]]
        return columns




    def delete_partition(self, delete_sql: str, table_name: str,condict:dict):
        """
        删除目标表分区数据
        """
        with Client(host=self.host, port=self.port, user=self.user,
                    password=self.password, database=self.database) as client:
            print(f"执行删除: {delete_sql}")
            client.execute(delete_sql,params=condict)
            print(f"{condict.get('date','')} 已执行完成删除 {table_name}  的数据")


    # 配合stream_query_insert流式查询，需要加工的采取这种方式，可支配的
    def _insert_batch(self, table_name: str, columns: list, data: list):
        """
        批量写入 ClickHouse
        """
        if not data:
            return
        col_str = ', '.join(columns)
        with Client(host=self.host, port=self.port, user=self.user,
                    password=self.password, database=self.database) as client:
            client.execute(f"INSERT INTO {table_name} ({col_str}) VALUES", data)
        print(f"已写入 {len(data)} 行到 {table_name}")


    # 直接采取insert into select 的机制
    def _insert_into_select(self, source_sql: str, target_table: str,condit:dict):
        """
        ClickHouse 原生执行 INSERT INTO ... SELECT
        """

        with Client(host=self.host, port=self.port, user=self.user,
                    password=self.password, database=self.database) as client:

            column_names = self._get_query_columns(source_sql, client,condit)
            col_str = ', '.join(column_names)

            client.execute(f"INSERT INTO {target_table} ({col_str}) {source_sql}",params=condit)
        print(f"{condit.get('date','')}已写入 到  {target_table}")



# ===================== 使用示例 =====================
if __name__ == "__main__":
    ch = ClickHouseHandler(host='localhost', port=9000, user='default', password='ck_test', database='Facial')

    delete_sql="alter table dwd_user_capture_heatmap delete where 1=1"
    source_sql = "select profile_id,region_id,region_name,capture_time,age,member_tier,gender,batch_time from dwd_user_capture_original "
    target_table = "dwd_user_capture_heatmap"
    ch.delete_partition(delete_sql,target_table,{})
    # ch.stream_query_insert(source_sql, target_table,{}, batch_size=50000)
    ch._insert_into_select(source_sql,target_table,{})
