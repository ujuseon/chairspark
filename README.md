# Обработка и анализ данных о зданиях с использованием PySpark, Airflow и ClickHouse
### Описание проекта  
Этот проект выполнен в рамках итогового задания курса "Data Engineer с нуля до junior".  
Цель проекта — разработать ETL-пайплайн для обработки и анализа данных о зданиях с использованием PySpark, Airflow и ClickHouse. 
### Задание
Необходимо:  
1. Развернуть среду с Airflow и ClickHouse (в docker-compose).  
2. Загрузить CSV-файл в PySpark, проверить корректность данных, преобразовать поля в соответствующие типы данных.  
3. Выполнить анализ данных:  
   - Найти средний и медианный год постройки зданий;
   - Определить топ-10 областей и городов с наибольшим количеством объектов;
   - Найти минимальную и максимальную площадь зданий в каждом регионе;
   - Определить количество зданий по десятилетиям;
   - Визуализировать результаты с помощью Matplotlib.
4. Создать таблицу в ClickHouse и загрузить туда обработанные данные.  
5. Настроить соединение с ClickHouse и выполнить SQL-запрос:  
   - Вывести топ-25 зданий с площадью более 60 кв.м.
### Структура проекта
```
chairspark/
|-- docker-compose.yml          # Конфигурация для развертывания сервисов
|-- Dockerfile                  # Dockerfile для сборки образа
|-- dags/                       # Папка с DAG-файлами для Airflow
|   |-- main_dag.py             # Основной DAG для выполнения задания
|-- requirements.txt            # Зависимости проекта
|-- README.md                   # Описание проекта
|-- shared_volume/              # Папка для данных и графиков
```
### Исходные данные
- **Источник**: [Ссылка на файл](https://disk.yandex.ru/d/bhf2M8C557AFVw)
- **Размер**: 300 МБ (590 708 строк)
- **Структура данных**:

|ID объекта|  Широта | Долгота |Количество жителей| Площадь |     Регион    |Населенный пункт|        Адрес        |               Полный адрес              | ID коммунальной службы |                                                  Описание объекта                                                     |Год постройки| 
|----------|---------|---------|------------------|---------|---------------|----------------|---------------------|-----------------------------------------|------------------------|-----------------------------------------------------------------------------------------------------------------------|-------------|
|  528953  |59.914344|30.450112|       1987       |187955.11|Санкт-Петербург| Санкт-Петербург|ул. Коллонтай, д. 5/1|г. Санкт-Петербург, ул. Коллонтай, д. 5/1|          29159         |Жилой дом в Санкт-Петербурге, по адресу ул. Коллонтай, д. 5/1, 2012 года постройки, под управлением УК «Аврора-Восток».|    2012     |


### Как запустить проект?

1. Собрать Dockerfile.
```
docker build -t airflow-with-java .
```

2. Запустить контейнеры.
```
docker compose up -d --build
```

3. Открыть Airflow (http://localhost:8080), запустить DAG main_dag и дождаться его выполнения.
После выполнения DAG данные будут загружены в таблицу ClickHouse, графики будут загружены в папку shared_volume/, результаты анализа отобразятся в логах задач Airflow.

Можно подключиться к ClickHouse для проверки данных вручную:
```
docker exec -it <clickhouse-container-id> clickhouse-client
SELECT * FROM real_estate_db.buildings LIMIT 10;
```

4. Остановить контейнеры
```
docker compose down -v
```
