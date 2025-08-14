import os
from pathlib import Path
import  ExcleToClickhouse_integrate as ec

def get_filenames_with_os_listdir(folder_path):
    # 获取所有条目名称
    all_entries = os.listdir(folder_path)
    # 过滤出文件（排除子文件夹）
    file_names = [entry for entry in all_entries if os.path.isfile(os.path.join(folder_path, entry))]
    return file_names


# 使用示例
if __name__ == "__main__":

    CLICKHOUSE_HOST = 'localhost'  # 替换为你的ClickHouse主机
    CLICKHOUSE_PORT = 9000  # ClickHouse TCP端口
    USER = 'default'  # 替换为你的用户名
    PASSWORD = 'ck_test'  # 替换为你的密码
    DATABASE = 'Facial'  # 替换为你的数据库名
     # = 'capture1'  # 目标表名

      # Excel文件路径
    CHUNK_SIZE = 10000  # 每块处理的行数

    # csv文件的存放路径
    folder_path=r"C:\Users\13106\Desktop\tmp\csv"


    file_names=get_filenames_with_os_listdir(folder_path)


    for file_name in file_names:
       TABLE_NAME= file_name.replace('.csv','')
       EXCEL_PATH = os.path.join(folder_path,file_name)
       # print(EXCEL_PATH)

       etl = ec.ExcelToClickHouse(
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





