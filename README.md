#  Olist E-Commerce: End-to-End Data Pipeline

Полноценный data pipeline на основе реального датасета бразильского маркетплейса Olist (126MB, 9 таблиц, ~100k заказов). Проект охватывает весь путь данных — от сырых CSV до аналитического дашборда с бизнес-инсайтами, включая промежуточное хранение в S3-совместимом хранилище.

##  Архитектура

```
CSV (9 файлов, 126MB)
        │
        ▼
┌─────────────────────┐
│    Apache Airflow   │  ← Оркестрация
│9 параллельных тасков│
└──────────┬──────────┘
           │
    ┌──────┴──────┐
    ▼             ▼
Parquet       MinIO (S3)     ← Data Lake
конвертация   хранение
    └──────┬──────┘
           │
           ▼
┌─────────────────────┐
│    PostgreSQL 16    │  ← raw schema
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│        dbt          │  ← Трансформации
│  staging → marts    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Apache Superset   │  ← Дашборд
└─────────────────────┘
```

Вся инфраструктура развёрнута в **Docker Compose**.

##  Стек технологий

| Компонент | Технология |
|---|---|
| Оркестрация | Apache Airflow 3.3.0 |
| Data Lake | MinIO (S3-совместимое хранилище) |
| Формат хранения | Apache Parquet |
| DWH | PostgreSQL 16 |
| Трансформации | dbt-postgres 1.10.2 |
| Визуализация | Apache Superset |
| Язык | Python 3.13 |
| Библиотеки | pandas, SQLAlchemy, psycopg2, boto3 |
| Инфраструктура | Docker, Docker Compose |

##  Структура проекта

```
airflow-course/
├── dags/
│   ├── extract.py          # DAG v1 (CSV → PostgreSQL)
│   └── extract_v2.py       # DAG v2 (CSV → Parquet → MinIO → PostgreSQL)
├── olist_dbt/
│   ├── models/
│   │   ├── staging/        # 9 staging моделей + schema.yml (dbt tests)
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_order_payments.sql
│   │   │   ├── stg_order_reviews.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_sellers.sql
│   │   │   ├── stg_geolocation.sql
│   │   │   ├── stg_category_translations.sql
│   │   │   └── schema.yml
│   │   └── marts/          # 3 mart модели + schema.yml
│   │       ├── mart_revenue.sql
│   │       ├── mart_top_categories.sql
│   │       ├── mart_geography.sql
│   │       └── schema.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── dbt_project.yml
└── docker-compose.yml
```

##  Запуск

### 1. Клонировать репозиторий
```bash
git clone https://github.com/kopar89/olist-pipeline
cd olist-pipeline
```

### 2. Запустить инфраструктуру
```bash
docker compose up -d
```

### 3. Создать контейнеры для данных
```bash
# PostgreSQL
docker run -d \
  --name postgres-olist \
  --network airflow-course_default \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_USER=olist_user \
  -e POSTGRES_DB=olist \
  -p 5433:5432 \
  postgres:16

# MinIO (S3)
docker run -d \
  --name minio \
  --network airflow-course_default \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=admin \
  -e MINIO_ROOT_PASSWORD=password123 \
  minio/minio server /data --console-address ":9001"
```

### 4. Создать bucket в MinIO
Открыть `http://localhost:9001` → Buckets → Create Bucket → `olist-data`

### 5. Скачать датасет
Скачать [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) с Kaggle и положить CSV файлы в папку `dags/`.

### 6. Запустить DAG
Открыть Airflow: `http://localhost:8080` и запустить DAG `ET4_minio`.

### 7. Просмотр дашборда
Открыть Superset: `http://localhost:8088` (admin/admin).

## Граф зависимостей DAG

![зависимости]('ET4_minio-graph.png')

Каждая таблица: сначала `upload` (CSV → Parquet → MinIO), потом `load` (MinIO → PostgreSQL). Все пары работают **параллельно**.

##  Слои данных (dbt)

### raw → staging
Очистка и стандартизация:
- Удаление NULL в критичных полях
- Приведение типов (строки → timestamp)
- Нормализация строк (`LOWER(TRIM(...))`)
- Фильтрация аномалий (отрицательные цены, невалидные координаты)
- `COALESCE` для пропущенных значений

### staging → marts
- `mart_revenue` — выручка по месяцам (24 месяца)
- `mart_top_categories` — топ-20 категорий по выручке
- `mart_geography` — 4300 городов с разбивкой по штатам

### dbt tests (контроль качества)
- `not_null` — критичные поля не пустые
- `unique` — первичные ключи уникальны
- `accepted_values` — статусы заказов и оценки отзывов в допустимом диапазоне

##  Ключевые бизнес-инсайты

### География
- **São Paulo (SP)** — 42% всех заказов, выручка 1.9M BRL
- Топ-5 штатов = 78% заказов → потенциал роста в Nordeste/Norte

  ![штаты]('заказы-по-штатам.jpg')

### Динамика выручки
- Старт: сентябрь 2016 (3 заказа)
- **Ноябрь 2017** — первый миллионный месяц (+63% благодаря Black Friday)
- С февраля 2018 — плато 865–996 тыс. BRL/месяц

  ![выручка]('выручка-по-месяцам.jpg')


### Топ категорий
| Категория | Выручка | Средний чек |
|---|---|---|
| Health & Beauty | 1,258,681 BRL | 130 BRL |
| Watches & Gifts | 1,205,005 BRL | 201 BRL |
| Bed, Bath & Table | 1,036,039 BRL | 93 BRL |

  ![категории]('топ-категорий.jpg')


## Технические решения

- **Data Lake паттерн** — промежуточное хранение в MinIO (S3) перед загрузкой в DWH
- **Parquet** — сжатый колоночный формат вместо CSV (5-10x меньше места)
- **boto3** — работа с S3 API для чтения/записи объектов
- **Параллельная загрузка** — 9 пар тасков работают одновременно
- **Чанковая обработка** (`chunksize=10000`) — защита от OOM
- **CASCADE DROP** — корректное обновление данных без конфликтов с dbt view
- **`{{ ref() }}` в dbt** — управление зависимостями между моделями
