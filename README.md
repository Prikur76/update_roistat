#### Проект: Обработка и загрузка данных в PostgreSQL из CSV-файлов и S3

---

### Описание проекта

Данный проект предназначен для автоматической обработки данных из CSV-файлов, загруженных в S3 хранилище, их преобразования и последующей записи в базу данных PostgreSQL. Проект включает в себя модульную структуру, обеспечивающую гибкость и удобство поддержки.

---

### Структура проекта

```tree
update_roistat/
├── app_logger.py                # Модуль для логирования
├── data_processing/
│   ├── db_updater.py            # Обновление данных в БД
│   ├── processors.py            # Преобразование данных
│   ├── s3_utils.py              # Работа с S3
│   ├── validators.py            # Валидация данных
│   └── __init__.py
├── db_schema.py                 # Создание таблиц в БД
├── decorators.py                # Декораторы для функций
├── main.py                      # Основной скрипт
├── README.md                    # Документация проекта
├── requirements.txt             # Требуемые библиотеки
├── settings.py                  # Конфигурации
├── tools.py                     # Утилиты (get_snakecase_row, clean_phone, split_fio)
└── __init__.py
```

---

### Функциональность

1. **Загрузка данных**:
   - Поддерживается загрузка файлов напрямую из Amazon S3.
   - Файлы автоматически преобразуются в pandas DataFrame.

2. **Обработка данных**:
   - Применяются функции для преобразования столбцов (например, преобразование в snake_case).
   - Данные очищаются от ненужных значений, выполняется валидация.

3. **Валидация**:
   - Проверка наличия обязательных столбцов.
   - Логирование ошибок при несоответствии данных ожидаемой структуре.

4. **Запись в PostgreSQL**:
   - Используется `ON CONFLICT` для обновления существующих записей.
   - Поддержка нескольких сред (test, prod).

5. **Логирование**:
   - Все операции логируются для анализа ошибок и отслеживания прогресса.

---

### Требования

- Python 3.9+
- Библиотеки:
  ```bash
  pip install -r requirements.txt
  ```
- Настройки AWS:
  - Необходимо настроить доступ к S3 через переменные окружения или конфигурационный файл.
- PostgreSQL:
  - База данных должна быть доступна для подключения.

---

### Настройка

1. **Установка зависимостей**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Настройка параметров**:
   - Отредактируйте файл `settings.py`, указав параметры подключения к S3 и PostgreSQL:
     ```python
     S3_CONFIG = {
         'aws_access_key_id': 'YOUR_ACCESS_KEY',
         'aws_secret_access_key': 'YOUR_SECRET_KEY',
         'region_name': 'YOUR_REGION'
     }

     DB_PARAMS = {
         'test': {
             'dbname': 'test_db',
             'user': 'db_user',
             'password': 'db_password',
             'host': 'localhost'
         },
         'prod': {
             'dbname': 'prod_db',
             'user': 'db_user',
             'password': 'db_password',
             'host': 'prod_host'
         }
     }
     ```

3. **Создание таблиц**:
   - Выполните создание таблиц в PostgreSQL:
     ```bash
     python db_schema.py
     ```

---

### Запуск

1. **Основной скрипт**:
   ```bash
   python main.py
   ```

2. **Логирование**:
   - Логи будут сохраняться в файл или выводиться в консоль в зависимости от настройки `app_logger`.

---

### Модули

#### 1. `data_processing/s3_utils.py`
- Работа с Amazon S3:
  - `get_latest_object_key`: Получает путь к последнему файлу.
  - `get_dataframe`: Читает файл из S3 в DataFrame.

#### 2. `data_processing/processors.py`
- Преобразование данных:
  - `process_contracts`: Обработка контрактов.
  - `process_payments`: Обработка платежей.
  - `process_cars`: Обработка автомобилей.
  - `process_companies`: Обработка компаний.
  - `process_drivers`: Обработка водителей.

#### 3. `data_processing/validators.py`
- Валидация данных:
  - Проверка наличия обязательных столбцов.
  - Логирование ошибок.

#### 4. `data_processing/db_updater.py`
- Обновление данных в PostgreSQL:
  - `update_table`: Записывает данные в таблицу с использованием `ON CONFLICT`.

#### 5. `tools.py`
- Утилиты для обработки данных:
  - `get_snakecase_row`: Преобразует имена столбцов в snake_case.
  - `clean_phone`: Очищает номер телефона.
  - `split_fio`: Разделяет ФИО на составляющие.

#### 6. `db_schema.py`
- Создание таблиц в PostgreSQL.

#### 7. `main.py`
- Основной скрипт для выполнения задач:
  - Загрузка данных из S3.
  - Обработка и валидация данных.
  - Запись в PostgreSQL.

---

### Возможные проблемы и решения

1. **Ошибка `InvalidColumnReference`**:
   - Причина: Неверно указан уникальный столбец в запросе `ON CONFLICT`.
   - Решение: Убедитесь, что столбцы имеют ограничения UNIQUE или PRIMARY KEY.

2. **Ошибка при чтении CSV**:
   - Причина: Несоответствие формата файла или отсутствие необходимых столбцов.
   - Решение: Проверьте структуру файла и добавьте соответствующие проверки.

3. **Проблемы с типами данных**:
   - Причина: Несоответствие типов данных в DataFrame.
   - Решение: Используйте метод `.astype()` для явного определения типов.

---

### TODO

- [ ] Добавить тесты для каждого модуля.
- [ ] Реализовать механизм ретраев при временных ошибках S3.
- [ ] Добавить возможность работы с несколькими префиксами в S3.

---