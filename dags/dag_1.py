from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.models.param import ParamsDict
from airflow.utils.dates import days_ago
from airflow.datasets import Dataset
import os
import random
import json

base_dir = os.path.dirname(os.path.abspath(__file__))
color_files_dir = os.path.join(base_dir, 'files')

# Объявляем Dataset для colors.json
colors_file = Dataset(os.path.join(color_files_dir, 'colors.json'))

os.makedirs(color_files_dir, exist_ok=True)

with DAG(
        dag_id='dag_1',
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False,
        params={
            "colors": ["Yellow", "Black", "Blue", "Brown", "Green", "Grey", "Orange", "Pink", "Purple", "Red", "White"],
            "multiplier": 3,
        }
) as dag:
    @task
    def cleanup():
        for file in os.listdir(color_files_dir):
            if file not in ['colors.json', 'report.csv']:
                os.remove(os.path.join(color_files_dir, file))
        print('Cleanup completed')

    # Функция для создания задач
    def generate_color_file_task(color):

        @task(task_id=color)
        def generate_color_file(**kwargs):
            params: ParamsDict = kwargs["params"]
            colors_to_process = params["colors"]
            multiplier = params["multiplier"]

            if color in colors_to_process:
                random_num = random.randint(1, 10)
                file_path = os.path.join(color_files_dir, f'{color}.txt')
                with open(file_path, 'w') as f:
                    f.write(str(random_num))
                print(f'Generated {color}.txt file with number {random_num}')
            else:
                print(f'Skipped {color}')

        return generate_color_file()


    @task(outlets=[colors_file])  # Указываем, что эта задача обновляет Dataset
    def save_parameters(**kwargs):
        params: ParamsDict = kwargs["params"]
        colors = params["colors"]
        multiplier = params["multiplier"]

        params_to_save = {'colors': colors, 'multiplier': multiplier}
        with open(os.path.join(color_files_dir, 'colors.json'), 'w') as f:
            json.dump(params_to_save, f)
        print('Parameters saved')


    # Стартовая задача
    start = cleanup()

    # Task Group для группировки всех цветовых задач
    with TaskGroup(group_id='color_tasks') as color_tasks_group:
        # Определяем все цвета
        all_colors = ["Yellow", "Black", "Blue", "Brown", "Green", "Grey", "Orange", "Pink", "Purple", "Red", "White"]

        # Создаем задачи для каждого цвета в группе
        for color in all_colors:
            generate_color_file_task(color=color)

    end = save_parameters()

    # Установка зависимостей
    start >> color_tasks_group >> end
