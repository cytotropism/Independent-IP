import csv

def convert_tbl_to_csv(tbl_file_path, csv_file_path):
    with open(tbl_file_path, 'r') as tbl_file:
        # 读取 .tbl 文件的每一行
        lines = tbl_file.readlines()

    with open(csv_file_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)

        for line in lines:
            # 使用制表符（或其他分隔符）分割行
            row = line.strip().split('|')  # 假设 .tbl 文件使用 '|' 作为分隔符
            writer.writerow(row)

# 指定文件路径
tbl_file = '/content/orders.tbl'  # 输入的 .tbl 文件路径
csv_file = '/content/orders.csv'    # 输出的 .csv 文件路径

convert_tbl_to_csv(tbl_file, csv_file)
print(f'Converted {tbl_file} to {csv_file}')