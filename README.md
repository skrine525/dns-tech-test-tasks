# Тестовые задания для DNS Технологии
Тестовые задания для DNS Технологии
## Установка зависимостей
Для запуска скриптов необходимо установить зависимости:
```shell
pip install -r requirements.txt
```
## Настройка подключения к БД
Для запуска первого скрипта необходимо настроить подключения к БД в файле **task1.py**:
```python
# Параметры подключения
db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'secret',
    'host': 'localhost',
    'port': '5432'
}
```