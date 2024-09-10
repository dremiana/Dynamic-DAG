import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.datasets import Dataset
import json
import csv

# Директории файлов
base_dir = os.path.dirname(os.path.abspath(__file__))
files_dir = os.path.join(base_dir, 'files')

colors_file = Dataset(os.path.join(files_dir, 'colors.json'))
report_file_path = os.path.join(files_dir, 'report.csv')

ALL_COLORS = ["Yellow", "Black", "Blue", "Brown", "Green", "Grey", "Orange", "Pink", "Purple", "Red", "White"]

# Функция которая проверяет доступен ли файл colors и был ли он обновлен за последнюю минуту
def wait_for_file(**kwargs):
    return os.path.exists(colors_file.uri) and os.path.getmtime(colors_file.uri) > (datetime.now().timestamp() - 60)

# Функция для обработки файлов
def process_color_files(color, multiplier, **kwargs):
    # Читаем число из файла с цветом
    file_path = os.path.join(files_dir, f"{color}.txt")
    with open(file_path, "r") as f:
        number = int(f.read().strip())

    # Умножаем число из файла на multiplier и создаем соответствующее количество файлов
    total_tasks = number * multiplier
    for i in range(total_tasks):
        with open(os.path.join(files_dir, f"{color}_{i:02d}.txt"), "w") as f:
            pass

    return total_tasks

# Функция для обновления файла отчёта
def update_report(**kwargs):
    # Получаем контекст задачи для извлечения XCom
    ti = kwargs['ti']
    # Читаем файл colors
    with open(colors_file.uri, "r") as f:
        data = json.load(f)
    processed_colors = data['colors'] # Цвета
    multiplier = data['multiplier'] # Множитель

    # Получаем результаты обработки цветов и добавляем их в контекст
    results = {color: ti.xcom_pull(task_ids=f"process_{color}") for color in processed_colors}

    # Если файл отчета не существует создаем его
    if not os.path.exists(report_file_path):
        with open(report_file_path, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["id", "finish_time"] + ALL_COLORS)

    # Получаем последний id в отчете
    with open(report_file_path, "r") as f:
        reader = csv.reader(f)
        rows = list(reader)
        last_id = len(rows) - 1

    # Новая строка для отчета
    new_row = [last_id + 1, datetime.now()]  # Новый id и время
    for color in ALL_COLORS:
        # Если цвет был обработан добавляем его результат, если нет - 0
        if color in results:
            new_row.append(results[color])
        else:
            new_row.append(0)

    # Добавляем новую строку
    with open(report_file_path, "a", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(new_row)

with DAG(
        "dag_2",
        start_date=datetime(2024, 1, 1),
        schedule=[colors_file],  # Запустится после обновления colors.json
        catchup=False,
        default_args={
            'retries': 3,  # Количество попыток для задач
            'retry_delay': timedelta(minutes=5),  # Задержка перед новой попыткой
        }
) as dag:

    # PythonSensor для ожидания, пока файл colors.json обновится
    wait_for_file_task = PythonSensor(
        task_id='wait_for_colors_file',
        python_callable=wait_for_file,
        timeout=300,  # Максимум 5 минут ожидания
        poke_interval=30,  # Проверять каждые 30 секунд
        mode='poke',  # Режим poke для сенсора
    )


    # Получаем список цветов и множитель
    def read_colors_json():
        with open(colors_file.uri, "r") as f:
            data = json.load(f)
        return data['colors'], data['multiplier']

    colors, multiplier = read_colors_json()

    # Создаем задачи для обработки каждого цвета
    process_color_tasks = []
    for color in colors:
        task = PythonOperator(
            task_id=f"process_{color}",  # Имя задачи
            python_callable=process_color_files,  # Функция для обработки файлов
            op_kwargs={"color": color, "multiplier": multiplier}  # Параметры функции
        )
        process_color_tasks.append(task)  # Добавляем задачу в список

    # Задача для обновления отчета
    update_report_task = PythonOperator(
        task_id="update_report",
        python_callable=update_report,
    )

    # Зависимости
    wait_for_file_task >> process_color_tasks >> update_report_task
