import pandas as pd
import numpy as np
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException
import logging
from datetime import datetime
import os

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('excel_to_clickhouse.log'),
        logging.StreamHandler()
    ]
)


class ExcelToClickHouse:
    def __init__(self, clickhouse_host, clickhouse_port, user, password, database, table_name):
        """初始化连接参数"""
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        self.user = user
        self.password = password
        self.database = database
        self.table_name = table_name
        self.client = None
        self.type_mapping = {
            'int64': 'Int64',
            'float64': 'String', #clickhouse有严格数据插入，在pandas里面读取数据如果有空值，会自动将int64识别成float64，float64会兼容nan。因此需要将float64在clickhouse里面创建成String类型
            'object': 'String',
            'bool': 'UInt8',
            'datetime64[ns]': 'DateTime',
            'timedelta64[ns]': 'String'  # ClickHouse没有直接对应的timedelta类型
        }

    def connect(self):
        """连接到ClickHouse"""
        try:
            self.client = Client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            logging.info("成功连接到ClickHouse")
            return True
        except Exception as e:
            logging.error(f"连接ClickHouse失败: {str(e)}")
            return False

    # 在clickhouse创建表
    def create_table_if_not_exists(self, df_sample):
        """根据样本数据创建表（如果不存在）"""
        if not self.client:
            logging.error("未连接到ClickHouse，请先调用connect()")
            return False

        # 生成CREATE TABLE语句
        columns_def = []
        for col, dtype in df_sample.dtypes.items():
            # 处理列名中的特殊字符
            clean_col = col.replace(' ', '_').replace('-', '_').replace('.', '_')
            # 映射数据类型
            ch_type = self.type_mapping.get(str(dtype), 'String')
            columns_def.append(f"`{clean_col}` {ch_type}")

            # print('columns_def:',columns_def)

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.table_name}` (
            {', '.join(columns_def)}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """

        try:
            self.client.execute(create_sql)
            logging.info(f"表 {self.table_name} 创建或已存在")
            return True
        except ServerException as e:
            logging.error(f"创建表失败: {str(e)}")
            return False

    def convert_data_types(self, df):
        """将DataFrame数据类型转换为ClickHouse兼容类型"""
        # 复制DataFrame避免修改原始数据
        converted_df = df.copy()

        # 处理列名
        converted_df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in
                                converted_df.columns]

        # 处理每列的数据类型
        for col, dtype in converted_df.dtypes.items():
            dtype_str = str(dtype)

            # 处理日期时间类型
            if dtype_str.startswith('datetime64'):
                # 转换为ClickHouse的DateTime格式
                converted_df[col] = converted_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                converted_df[col] = converted_df[col].replace('NaT', None)

            # 处理布尔类型
            elif dtype_str == 'bool':
                # 转换为0和1
                converted_df[col] = converted_df[col].astype(int)

            # 处理缺失值
            elif converted_df[col].isnull().any():
                # 对于数值类型，将NaN转换为None
                if dtype_str == 'int64':
                    converted_df[col] = converted_df[col].replace({np.nan: None})
                elif dtype_str == 'float64':
                    converted_df[col] = converted_df[col].replace({np.nan: None}).astype(str)
                # 对于字符串类型，将NaN转换为空字符串
                elif dtype_str == 'object':
                    converted_df[col] = converted_df[col].fillna('')

        return converted_df

    def batch_insert(self, df):
        """批量插入数据到ClickHouse"""
        if not self.client:
            logging.error("未连接到ClickHouse，请先调用connect()")
            return False

        if df.empty:
            logging.warning("空DataFrame，跳过插入")
            return True

        try:
            # 转换数据类型
            converted_df = self.convert_data_types(df)

            # 获取列名和数据
            columns = converted_df.columns.tolist()

            '''
            to_dict('records') 是 pandas 的内置方法，'records' 表示按行转换，每行数据的字典格式为 {列名1: 值1, 列名2: 值2, ...}
            '''
            data = converted_df.to_dict('records')

            # 执行插入
            self.client.execute(
                f"INSERT INTO `{self.table_name}` ({', '.join(columns)}) VALUES",
                data
            )

            logging.info(f"成功插入 {len(df)} 条记录")
            return True
        except Exception as e:
            logging.error(f"插入数据失败: {str(e)}")
            return False

    def process_excel_in_chunks(self, excel_path, chunk_size=10000):
        """分块处理Excel文件"""
        try:
            # 获取Excel文件信息
            file_name = os.path.basename(excel_path)
            logging.info(f"开始处理Excel文件: {file_name}, 分块大小: {chunk_size}")

            # 读取第一个chunk获取表结构样本
            first_chunk = next(pd.read_csv(excel_path,  chunksize=chunk_size))
            # 创建表
            self.create_table_if_not_exists(first_chunk)
            # 插入第一个chunk
            self.batch_insert(first_chunk)

            # 处理剩余的chunk
            chunk_iter = pd.read_csv(excel_path, chunksize=chunk_size)
            # 跳过第一个已经处理的chunk
            next(chunk_iter)

            total_rows = len(first_chunk)
            chunk_num = 1

            for chunk in chunk_iter:
                chunk_num += 1
                total_rows += len(chunk)
                logging.info(f"处理第 {chunk_num} 块, 累计处理 {total_rows} 行")

                if not self.batch_insert(chunk):
                    logging.warning(f"第 {chunk_num} 块插入失败，继续处理下一块")

            logging.info(f"Excel文件处理完成，共插入 {total_rows} 行数据到表 {self.table_name}")
            return total_rows

        except Exception as e:
            logging.error(f"处理Excel文件时出错: {str(e)}")
            return 0


if __name__ == "__main__":
    # 配置参数
    CLICKHOUSE_HOST = 'localhost'  # 替换为你的ClickHouse主机
    CLICKHOUSE_PORT = 9000  # ClickHouse TCP端口
    USER = 'default'  # 替换为你的用户名
    PASSWORD = 'ck_test'  # 替换为你的密码
    DATABASE = 'Facial_1'  # 替换为你的数据库名
    TABLE_NAME = 'profile'  # 目标表名

    EXCEL_PATH = r"C:\Users\13106\Desktop\tmp\csv\profile.csv"  # Excel文件路径
    CHUNK_SIZE = 10000  # 每块处理的行数

    # 创建实例并执行
    etl = ExcelToClickHouse(
        clickhouse_host=CLICKHOUSE_HOST,
        clickhouse_port=CLICKHOUSE_PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        table_name=TABLE_NAME
    )

    if etl.connect():
        etl.process_excel_in_chunks(
            excel_path=EXCEL_PATH,
            chunk_size=CHUNK_SIZE
        )
