from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException
import logging
from contextlib import contextmanager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@contextmanager
def clickhouse_connection(host, port, user, password, database):
    """上下文管理器：自动处理ClickHouse连接的创建和关闭"""
    client = None
    try:
        client = Client(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            settings={"use_numpy": False}  # 禁用numpy，返回原生Python类型
        )
        logger.info("成功连接到ClickHouse")
        yield client
    except ServerException as e:
        logger.error(f"ClickHouse连接/操作失败: {str(e)}")
        raise
    finally:
        if client:
            client.disconnect()
            logger.info("ClickHouse连接已关闭")


def read_data_in_chunks(client, query, chunk_size=10000):
    """
    分块读取数据
    :param client: ClickHouse客户端实例
    :param query: 查询SQL语句
    :param chunk_size: 每块数据的行数
    :return: 生成器，每次返回(列名, 数据块)
    """
    try:
        # 创建游标并执行查询
        cursor = client.execute_iter(query, with_column_types=True)

        # 获取列名（第一行返回列信息）
        column_types = next(cursor)
        columns = [col[0] for col in column_types]  # 提取列名

        chunk = []
        for row in cursor:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield (columns, chunk)
                chunk = []

        # 返回剩余数据
        if chunk:
            yield (columns, chunk)

        logger.info("数据读取完成")

    except ServerException as e:
        logger.error(f"读取数据失败: {str(e)}")
        raise


def insert_chunk(client, table_name, columns, data_chunk):
    """
    批量插入数据块到目标表
    :param client: ClickHouse客户端实例
    :param table_name: 目标表名
    :param columns: 列名列表
    :param data_chunk: 待插入的数据块（元组列表）
    """
    try:
        # 构建插入SQL（包含列名，确保顺序匹配）
        columns_str = ", ".join(columns)
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES"

        # 执行批量插入
        client.execute(insert_sql, data_chunk)
        logger.info(f"成功插入 {len(data_chunk)} 条记录到 {table_name}")

    except ServerException as e:
        logger.error(f"插入数据失败: {str(e)}")
        raise


def execute_sql(client, sql, params=None):
        """执行SQL语句"""
        try:
            logging.info(f"执行SQL: {sql}")
            result=client.execute(sql, params)
            logging.info(f"SQL删除语句执行成功，影响行数：{len(result) if result is not None else 0}")
        except Exception as e:
            logging.error(f"SQL执行失败: {str(e)}", exc_info=True)
            raise


def main():
    # 配置连接参数（请根据实际环境修改）
    config = {
        "host": "localhost",  # 例如：localhost
        "port": 9000,  # clickhouse-driver默认使用9000端口（TCP）
        "user": "default",
        "password": "ck_test",
        "database": "Facial"
    }

    # 替换为你的查询SQL
    target_table = "dws_user_visitation_demographics"  # 替换为目标表名


    delete_query=f"""
    alter table  {target_table}  delete  where batch_time>=toDate(formatDateTime(now(),'%Y-%m-%d'))
    
    """



    # 源查询和目标表配置
    source_query = """
        select date date
      ,region_id
      ,region_name
      ,'' region_type
      ,gender
      ,Age_range
      ,profile_type
      , count(distinct profile_id) visitors_num
      , now() batch_time
 from Facial.dws_profileid_aggregation
group by date,region_id,region_name,gender,Age_range,profile_type
        
    """


    chunk_size = 5000  # 每块数据大小（根据内存调整）

    try:
        # 使用上下文管理器创建连接
        with clickhouse_connection(**config) as client:

            # 执行删除语句
            execute_sql(client,delete_query)
            # 分块读取并插入
            chunk_count = 0
            for columns, chunk in read_data_in_chunks(client, source_query, chunk_size):
                chunk_count += 1
                logger.info(f"处理第 {chunk_count} 块，共 {len(chunk)} 条记录")
                insert_chunk(client, target_table, columns, chunk)

        logger.info("所有数据处理完成")

    except Exception as e:
        logger.error(f"执行过程出错: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()
