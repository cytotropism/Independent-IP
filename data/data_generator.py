# Load packages
import os
import csv
import random

# Reproducibility
random.seed(0)

# Dataset files and filtered tables
dataset_files = ["lineitem.tbl", "orders.tbl", "supplier.tbl", "customer.tbl", "nation.tbl"]
FILTERED_TBL_SET = {"nation.tbl", "supplier.tbl", "customer.tbl"}
csv_file_path = "source_data.csv"

def data_generator():
    dataset_firstHalf = []
    dataset_lastHalf = {table: [] for table in dataset_files}
    final_dataset = []
    add_count = delete_count = total_size = 0

    # Load data
    for file_name in dataset_files:
        with open(file_name, "r") as file:
            file_data = file.readlines()

        n = len(file_data)
        total_size += n
        print(f"{file_name} size is {n}")

        if file_name not in FILTERED_TBL_SET:
            mid = n // 2
            dataset_firstHalf.extend(f'+|{file_name}|{line.strip().replace(",", ".")}' for line in file_data[:mid])
            dataset_lastHalf[file_name].extend(f'+|{file_name}|{line.strip().replace(",", ".")}' for line in file_data[mid:])
            final_dataset.extend(dataset_firstHalf[-mid:])
            add_count += mid
        else:
            final_dataset.extend(f'+|{file_name}|{line.strip().replace(",", ".")}' for line in file_data)
            add_count += n

    # Randomize data
    random.shuffle(dataset_firstHalf)
    while dataset_firstHalf:
        deleted_tuple = dataset_firstHalf.pop()
        table_name = deleted_tuple.split("|")[1]

        while table_name in FILTERED_TBL_SET or not dataset_lastHalf[table_name]:
            deleted_tuple = dataset_firstHalf.pop()
            table_name = deleted_tuple.split("|")[1]

        final_dataset.append(f'-{deleted_tuple[1:]}')
        delete_count += 1
        inserted_tuple = dataset_lastHalf[table_name].pop()
        final_dataset.append(inserted_tuple)

    # Add remaining tuples
    for table_data in dataset_lastHalf.values():
        final_dataset.extend(table_data)
        add_count += len(table_data)

    print(f'add count: {add_count}')
    print(f'delete count: {delete_count}')
    print(f'total size: {total_size}')

    # Dump to CSV
    with open(csv_file_path, 'w') as f:
        f.writelines("\n".join(final_dataset))

    print('CSV file created and data written successfully.')

if __name__ == '__main__':
    data_generator()