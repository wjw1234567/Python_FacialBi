import pandas as pd
import os


def excel_to_csvs(excel_path, output_dir):
    """
    将Excel文件中的所有工作表转换为CSV文件

    参数:
    excel_path: Excel文件的路径
    output_dir: 输出CSV文件的目录
    """
    # 确保输出目录存在，如果不存在则创建
    os.makedirs(output_dir, exist_ok=True)

    # 读取Excel文件中的所有工作表
    excel_file = pd.ExcelFile(excel_path)

    # 获取所有工作表的名称
    sheet_names = excel_file.sheet_names

    # 检查工作表数量是否为10个
    if len(sheet_names) != 10:
        print(f"警告: 该Excel文件包含 {len(sheet_names)} 个工作表")

    # 遍历每个工作表并转换为CSV
    for sheet_name in sheet_names:
        # 读取当前工作表
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        # 生成CSV文件名（使用工作表名）
        csv_filename = f"{sheet_name}.csv"
        csv_path = os.path.join(output_dir, csv_filename)

        # 保存为CSV文件
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"已生成: {csv_path}")

    print(f"转换完成，共生成 {len(sheet_names)} 个CSV文件")


# 使用示例
if __name__ == "__main__":
    # 替换为你的Excel文件路径
    excel_file_path = r"C:\Users\13106\Desktop\tmp\Facial_tab.xlsx"

    # 替换为你想要保存CSV文件的目录
    output_directory = r"C:\Users\13106\Desktop\tmp\csv"

    # 调用函数执行转换
    excel_to_csvs(excel_file_path, output_directory)
