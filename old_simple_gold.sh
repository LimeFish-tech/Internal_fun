#!/bin/bash

# Параметры подключения к БД (можно вынести в переменные окружения)
DB_NAME="your_db"
DB_HOST="your_host"
DB_PORT="5432"
DB_USER="your_user"
export PGPASSWORD="your_password"

# Количество параллельных процессов
PARALLEL_JOBS=8

# Временный файл со списком OID таблиц
TABLE_LIST="tables.txt"
RESULT_TABLE="public.table_segment_sizes"

# 1. Получить список OID пользовательских таблиц (исключая системные схемы)
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -Atc "
    SELECT c.oid
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname NOT LIKE 'pg\_%'
      AND n.nspname != 'information_schema'
      AND n.nspname NOT LIKE 'gp\_%'
      AND n.nspname != 'toast'
      AND c.relkind = 'r';   -- только обычные таблицы; при необходимости добавить 'p' для секционированных
" > "$TABLE_LIST"

if [[ ! -s "$TABLE_LIST" ]]; then
    echo "Нет таблиц для обработки или ошибка подключения."
    exit 1
fi

echo "Найдено $(wc -l < "$TABLE_LIST") таблиц. Запуск $PARALLEL_JOBS параллельных процессов..."

# 2. Функция обработки одной таблицы (по OID)
process_table() {
    local oid="$1"
    # Вставка размеров по сегментам
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "
        INSERT INTO $RESULT_TABLE (segment_id, relnamespace, relname, byte_size)
        SELECT
            gp_execution_segment(),
            pc.relnamespace,
            pc.relname,
            pg_total_relation_size(pc.oid) AS byte_size
        FROM gp_dist_random('pg_class') pc
        WHERE pc.relnamespace = $oid
        ORDER BY byte_size DESC;
    " > /dev/null 2>&1

    if [[ $? -eq 0 ]]; then
        echo "Таблица $oid обработана успешно"
    else
        echo "Ошибка при обработке таблицы $oid" >&2
    fi
}

export -f process_table
export DB_NAME DB_HOST DB_PORT DB_USER PGPASSWORD RESULT_TABLE

# 3. Параллельная обработка
cat "$TABLE_LIST" | xargs -n 1 -P "$PARALLEL_JOBS" -I {} bash -c 'process_table "$@"' _ {}

# 4. Очистка
rm -f "$TABLE_LIST"
echo "Готово."
