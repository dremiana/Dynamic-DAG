1. Клонируйте себе на ПК репозиторий: 
```
https://github.com/dremiana/Dynamic-DAG.git
```

2. Инициализируйте Airflow:
    Запустите команду:
    ```bash
    docker-compose up airflow-init
    ```
    
3. Запустите сервисы:
    ```bash
    docker-compose up
    ```

4. Доступ к Airflow:
    - Откройте браузер и перейдите по адресу [http://localhost:8080](http://localhost:8080).
    - Войдите с помощью следующих учетных данных:
        - Имя пользователя: `airflow`
        - Пароль: `airflow`

5. Запустите dag_1, dag_2 запустится автоматически после обновления файла colors.json

Схема первого dag:

![image](https://github.com/user-attachments/assets/7a34c934-c2ef-4729-add4-48708ab3c694)

При запуске на вход задается параметр colors в виде массива из ключей, 
представляющих из себя название цветов, например:
[“red”, ”grey”, ”green”].
Вторым параметром задается переменной multiplier число в диапазоне от 2 до 10

Схема второго dag:

![image](https://github.com/user-attachments/assets/2a43f2cb-876f-4cb7-8095-8c0e1a44cb34)


