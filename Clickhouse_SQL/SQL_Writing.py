import time
import logging
from clickhouse_driver import Client
import schedule

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('clickhouse_scheduler.log'),
        logging.StreamHandler()
    ]
)


class ClickHouseScheduler:
    def __init__(self, host='localhost', port=9000, user='default', password='', database='default'):
        """初始化ClickHouse连接"""
        self.client = Client(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        logging.info(f"成功连接到ClickHouse: {host}:{port}/{database}")

    def execute_sql(self, sql, params=None):
        """执行SQL语句"""
        try:
            logging.info(f"执行SQL: {sql}")
            result = self.client.execute(sql, params)
            logging.info(f"SQL执行成功，影响行数: {len(result) if result is not None else 0}")
            return result
        except Exception as e:
            logging.error(f"SQL执行失败: {str(e)}", exc_info=True)
            raise

    def sample_task(self):
        """示例任务：查询并处理数据"""
        try:
            # 示例1：创建表（如果不存在）
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS user_stats (
                date Date,
                user_id UInt64,
                page_views UInt32,
                clicks UInt32
            ) ENGINE = MergeTree()
            ORDER BY (date, user_id)
            """
            self.execute_sql(create_table_sql)

            # 示例2：插入数据
            insert_sql = """
            INSERT INTO user_stats (date, user_id, page_views, clicks)
            VALUES (%(date)s, %(user_id)s, %(page_views)s, %(clicks)s)
            """
            data = {
                'date': '2023-10-01',
                'user_id': 12345,
                'page_views': 10,
                'clicks': 3
            }
            self.execute_sql(insert_sql, data)

            # 示例3：查询数据
            query_sql = """
            SELECT date, COUNT(DISTINCT user_id) as total_users, 
                   SUM(page_views) as total_views, SUM(clicks) as total_clicks
            FROM user_stats
            WHERE date = %(date)s
            GROUP BY date
            """
            result = self.execute_sql(query_sql, {'date': '2023-10-01'})

            if result:
                date, total_users, total_views, total_clicks = result[0]
                logging.info(f"统计结果 - 日期: {date}, 用户数: {total_users}, "
                             f"浏览量: {total_views}, 点击量: {total_clicks}")

        except Exception as e:
            logging.error(f"任务执行失败: {str(e)}", exc_info=True)

    def setup_schedule(self):
        """设置任务调度"""
        # 每分钟执行一次示例任务
        schedule.every(1).minutes.do(self.sample_task)
        logging.info("调度任务已设置：每分钟执行一次示例任务")

        # 可以添加更多任务
        # schedule.every().day.at("03:00").do(self.another_task)  # 每天凌晨3点执行

    def run_scheduler(self):
        """运行调度器"""
        logging.info("调度器开始运行...")
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    # 初始化ClickHouse调度器
    scheduler = ClickHouseScheduler(
        host='localhost',  # 替换为你的ClickHouse主机
        port=9000,  # 替换为你的ClickHouse端口
        user='default',  # 替换为你的用户名
        password='ck_test',  # 替换为你的密码
        database='Facial_1'  # 替换为你的数据库名
    )

    # 设置并运行调度
    scheduler.setup_schedule()
    try:
        scheduler.run_scheduler()
    except KeyboardInterrupt:
        logging.info("调度器已手动停止")
    except Exception as e:
        logging.error(f"调度器运行出错: {str(e)}", exc_info=True)
