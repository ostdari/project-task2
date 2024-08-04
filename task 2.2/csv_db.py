from pathlib import Path
import psycopg2
import csv
from datetime import datetime
import time

# Константы для подключения к базе
HOST = 'localhost'
DATABASE = 'dwh'
USER = 'postgres'
PASSWORD = '123'


# Подсчет количества строк в csv файле без заголовка
def count_rows(csv_file):
    with open(csv_file, 'r', encoding='unicode_escape') as f:
        reader = csv.reader(f)
        return sum(1 for row in reader) - 1  # -1 для исключения заголовка


# Поиск колонок с датами
def find_date_columns(header):
    return [i for i, col in enumerate(header) if 'date' in col.lower()]


# Функция для преобразования даты в стандартный формат
def convert_to_standard_format(date_str):
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    return date_obj


def import_csv_to_db_coding(cur, csv_file, table_name):
    cur.execute(
        f"""
            DELETE FROM {table_name}
            """
    )
    
    with open(csv_file, 'r', encoding='cp1251', errors='replace') as f:
        reader = csv.reader(f, delimiter=',')
        header = next(reader)  # Чтение заголовка
        date_columns = find_date_columns(header)  # Поиск колонок с датой

        for row in reader:
            # Замена пустых строк на None для обработки NULL значений
            row = [None if val == '' else val for val in row]
            # Преобразование строк даты в формат datetime
            for idx in date_columns:
                date_str = row[idx]
                if date_str:
                    # Приводим дату к формату yyyy-MM-dd
                    row[idx] = convert_to_standard_format(date_str)

            cur.execute(
                f"""
                INSERT INTO {table_name} ({', '.join(header)}) 
                VALUES ({', '.join(['%s'] * len(row))})
                """,
                row
            )


def basic_flow():
    # Подключения к базе данных
    conn = psycopg2.connect(
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD
    )

    # Пути к CSV файлам
    csv_files = [r'C:\Users\Dasha\Desktop\project\материалы\data\loan_holiday_info\deal_info.csv',
                 r'C:\Users\Dasha\Desktop\project\материалы\data\loan_holiday_info\product_info.csv']

    # Таблицы для импорта данных
    data_tables = ['rd2.deal_info', 'rd2.product']

    cur = conn.cursor()
    for data_table, csv_file in zip(data_tables, csv_files):
        try:
            # Подсчет количества строк в CSV файле
            row_count = count_rows(csv_file)

            # Время начала загрузки
            start_time = datetime.now()

            # Импорт данных из CSV файла в базу данных

            import_csv_to_db_coding(cur, csv_file, data_table)

            #
            # Подтверждение всех изменений в базе данных
            conn.commit()

            print(
                f"Данные из файла {Path(csv_file).name} успешно импортированы в базу данных PostgreSQL в таблицу {data_table}")
        except Exception as e:
            print(f"Произошла ошибка: {e}")
            conn.rollback()

    # Закрытие курсора и соединения
    cur.close()
    conn.close()


if __name__ == "__main__":
    basic_flow()
