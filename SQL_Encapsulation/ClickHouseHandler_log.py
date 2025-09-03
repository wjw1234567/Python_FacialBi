from clickhouse_driver import Client
import os
from datetime import datetime

class ClickHouseHandler:
    def __init__(self, host='localhost', port=9000, user='default', password='', database='default',log_dir='clickhouse_logs'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

        self.log_dir = log_dir
        # 确保日志目录存在
        os.makedirs(self.log_dir, exist_ok=True)
        # 记录当前日志月份（用于判断是否需要切换文件）
        self.current_log_month = None
        # 当前日志文件句柄
        self.log_file_handle = None



    def _get_log_file_path(self):
        """获取当前月份的日志文件路径"""
        current_month = datetime.now().strftime('%Y-%m')
        return os.path.join(self.log_dir, f"{current_month}_sql_exec.log")

    def _write_log(self, content, is_error=False):
        """写入日志内容，自动处理月份切换"""
        current_month = datetime.now().strftime('%Y-%m')
        # 切换日志文件逻辑
        if current_month != self.current_log_month or not self.log_file_handle:
            if self.log_file_handle:
                self.log_file_handle.close()
            self.current_log_month = current_month
            log_path = self._get_log_file_path()
            self.log_file_handle = open(log_path, 'a', encoding='utf-8')

        # 日志前缀（区分错误和普通日志）
        log_type = "[ERROR]" if is_error else "[INFO]"
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.log_file_handle.write(f"{timestamp} {log_type} {content}\n")
        self.log_file_handle.flush()




    def _log_sql(self, sql: str, params: dict = None):
        """记录SQL执行日志"""
        if params:
            self._write_log(f"执行SQL成功: {sql}\n\t参数: {params},执行时间：{datetime.now()}")
        else:
            self._write_log(f"执行SQL成功: {sql} 执行时间：{datetime.now()}")

    def _log_error(self, error, sql: str = None, params: dict = None):
        """记录错误信息"""
        error_msg = f"错误类型: {type(error).__name__}\n错误信息: {str(error)}"
        if sql:
            error_msg += f"\n关联SQL: {sql}"
        if params:
            error_msg += f"\n关联参数: {params}"
        self._write_log(error_msg, is_error=True)





    def stream_query_insert(self, source_sql: str, target_table: str,condict:dict, batch_size: int = 10000):
        """
        安全版流式查询 ClickHouse，边生成边写入目标表
        自动使用查询列名作为 INSERT 的列
        使用独立连接，避免 PartiallyConsumedQueryError
        """
        # 建立独立连接，避免与其他操作冲突
        try:
            with Client(host=self.host, port=self.port, user=self.user,
                        password=self.password, database=self.database) as client:

                column_names = self._get_query_columns(source_sql, client, condict)
                print(f"使用列名: {column_names}")

                batch = []
                for row in client.execute_iter(source_sql, params=condict):
                    batch.append(row)
                    if len(batch) >= batch_size:
                        self._insert_batch(target_table, column_names, batch)
                        batch.clear()

                if batch:
                    self._insert_batch(target_table, column_names, batch)
        except Exception as e:
            self._log_error(e, source_sql, condict)
            raise  # 重新抛出异常，不影响原有程序流




    def _get_query_columns(self, sql: str, client: Client,condict:dict):
        """
        获取 SQL 查询列名
        """
        # LIMIT 0 获取列信息，不拉取数据

        try:
            res = client.execute(f"SELECT * FROM ({sql}) LIMIT 0", with_column_types=True, params=condict)
            self._log_sql(f"获取列名SQL: SELECT * FROM ({sql}) LIMIT 0", condict)
            return [col[0] for col in res[1]]
        except Exception as e:
            self._log_error(e, f"SELECT * FROM ({sql}) LIMIT 0", condict)
            raise







    def delete_partition(self, delete_sql: str, table_name: str,condict:dict):
        """
        删除目标表分区数据
        """

        try:
            with Client(host=self.host, port=self.port, user=self.user,
                        password=self.password, database=self.database) as client:
                print(f"执行删除: {delete_sql}")
                client.execute(delete_sql, params=condict)
                self._log_sql(delete_sql, condict)
                print(f"{condict.get('date', '')} 已执行完成删除 {table_name} 的数据")
        except Exception as e:
            self._log_error(e, delete_sql, condict)
            raise


    # 配合stream_query_insert流式查询，需要加工的采取这种方式，可支配的
    def _insert_batch(self, table_name: str, columns: list, data: list):
        """
        批量写入 ClickHouse
        """
        if not data:
            return
        try:
            col_str = ', '.join(columns)
            sql = f"INSERT INTO {table_name} ({col_str}) VALUES"
            with Client(host=self.host, port=self.port, user=self.user,
                        password=self.password, database=self.database) as client:
                client.execute(sql, data)
                self._log_sql(sql, {"数据量": len(data)})
            print(f"已写入 {len(data)} 行到 {table_name}")
        except Exception as e:
            self._log_error(e, sql, {"数据量": len(data)})
            raise




    # 直接采取insert into select 的机制
    def _insert_into_select(self, source_sql: str, target_table: str,condit:dict):
        """
        ClickHouse 原生执行 INSERT INTO ... SELECT
        """
        try:
            with Client(host=self.host, port=self.port, user=self.user,
                        password=self.password, database=self.database) as client:

                column_names = self._get_query_columns(source_sql, client, condit)
                col_str = ', '.join(column_names)
                sql = f"INSERT INTO {target_table} ({col_str}) {source_sql}"
                client.execute(sql, params=condit)
                self._log_sql(sql, condit)
            print(f"{condit.get('date', '')}已写入 到  {target_table}")
        except Exception as e:
            self._log_error(e, sql, condit)
            raise



    def __del__(self):
        """对象销毁时关闭日志文件句柄"""
        if self.log_file_handle:
            self.log_file_handle.close()



# ===================== 使用示例 =====================
if __name__ == "__main__":
    ch = ClickHouseHandler(host='localhost', port=9000, user='default', password='ck_test', database='Facial', log_dir='./clickhouse_exec_logs')

    delete_sql="alter table dwd_user_capture_heatmap delete where 1=1"
    source_sql = "select profile_id,region_id,region_name,capture_time,age,member_tier,gender,batch_time from dwd_user_capture_original "
    target_table = "dwd_user_capture_heatmap"
    ch.delete_partition(delete_sql,target_table,{})
    # ch.stream_query_insert(source_sql, target_table,{}, batch_size=50000)
    ch._insert_into_select(source_sql,target_table,{})
