import os

def rename_py_to_txt(folder_path):
    """
    将指定文件夹内所有 .py 文件重命名为 .txt 文件
    :param folder_path: 文件夹路径（绝对路径或相对路径）
    """
    # 检查文件夹是否存在
    if not os.path.exists(folder_path):
        print(f"错误：文件夹 '{folder_path}' 不存在！")
        return
    if not os.path.isdir(folder_path):
        print(f"错误：'{folder_path}' 不是一个文件夹！")
        return

    # 遍历文件夹内所有文件
    for filename in os.listdir(folder_path):
        # 只处理 .py 结尾的文件
        if filename.endswith(".py"):
            # 构建旧文件完整路径
            old_path = os.path.join(folder_path, filename)
            # 确保是文件（不是子文件夹）
            if os.path.isfile(old_path):
                # 生成新文件名（替换 .py 为 .txt）
                new_filename = filename.replace(".py", ".txt")
                new_path = os.path.join(folder_path, new_filename)
                # 重命名
                os.rename(old_path, new_path)
                print(f"已重命名：{filename} -> {new_filename}")

    print("批量重命名完成！")

# 示例：修改当前脚本所在文件夹内的 .py 文件
if __name__ == "__main__":
    # 1. 若要处理当前文件夹，直接传 "."（表示当前目录）
    # target_folder = "."

    # 2. 若要处理指定文件夹，传入绝对路径或相对路径
    # 例如：Windows 路径
    # target_folder = "C:/Users/你的用户名/Documents/Python脚本"
    # 例如：Linux/Mac 路径
    # target_folder = "/home/你的用户名/scripts"

    # 这里以当前文件夹为例
    target_folder = r"C:\Users\13106\Desktop\tmp\SQL_Encapsulation"
    rename_py_to_txt(target_folder)