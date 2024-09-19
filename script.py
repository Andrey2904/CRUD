import pandas as pd
import os
from datetime import datetime, timedelta
import sys

logi_dir = "logi"
output_dir = "output"

def process_logs(target_date):
    target_date = datetime.strptime(target_date,"%Y-%m-%d")

    start_day = target_date - timedelta(days=7)
    end_day = target_date-timedelta(days = 1)

    all_data = []

    for i in range(1,8):
        log_date = start_day+timedelta(days=i)
        log_filename = f"{log_date.strftime('%Y-%m-%d')}.csv"
        log_filepath = os.path.join(logi_dir,log_filename)
        if os.path.exists((log_filepath)):

            df = pd.read_csv(log_filepath,header = None)
            df.columns = ['email','action','dt']
            all_data.append(df)
            #print(df['email'], df['action'], sep="/n")

        else:
            print(f"Файл {log_filename} не найден. Пропускаем этот день.")

    if all_data:
        all_data_df = pd.concat(all_data)

        aggregated_df = all_data_df.groupby(['email','action']).size().unstack(fill_value=0).reset_index()

        aggregated_df.columns = ['email', 'create_count', 'delete_count', 'read_count', 'update_count']

        output_filename = f"{target_date.strftime('%Y-%m-%d')}.csv"
        output_filepath = os.path.join(output_dir, output_filename)
        aggregated_df.to_csv(output_filepath, index=False)
        print(f"Файл {output_filename} успешно создан в директории {output_dir}")
    else:
        print("Нет данных для обработки за указанный период.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Использование: python script.py <YYYY-mm-dd>")
        sys.exit(1)

    # Получаем дату из аргументов командной строки
    target_date = sys.argv[1]
    process_logs(target_date)