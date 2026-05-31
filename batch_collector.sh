#!/bin/bash
set -euo pipefail

# === Настройки ===
export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGDATABASE="${PGDATABASE:-postgres}"
export PGUSER="${PGUSER:-gpadmin}"
export PGPASSWORD="${PGPASSWORD:-}"
PARALLEL="${PARALLEL:-4}"                     # число параллельных сессий
BATCH_SIZE="${BATCH_SIZE:-10}"               # сколько объектов передавать в одну сессию
WORKDIR="${WORKDIR:-/tmp/table_sizing}"
LOGFILE="${LOGFILE:-$WORKDIR/collection.log}"

mkdir -p "$WORKDIR"

# === 1. Список OID обычных таблиц ===
psql -X -A -q -t -o "$WORKDIR/regular_oids.txt" <<'SQL'
SELECT pc.oid
FROM pg_class pc
JOIN pg_namespace pn ON pn.oid = pc.relnamespace
WHERE pc.relkind = 'r'
  AND pc.relhassubclass = false
  AND pn.nspname NOT LIKE 'pg_%'
  AND pn.nspname NOT LIKE 'gp_%'
  AND pn.nspname != 'information_schema'
  AND pn.nspname NOT LIKE '%toast%'
ORDER BY 1;
SQL

# === 2. Список партиций с родительской информацией ===
psql -X -A -q -t -F '|' -o "$WORKDIR/partition_oids.txt" <<'SQL'
SELECT c.oid, p.partitionschemaname, p.partitiontablename,
       p.schemaname AS parent_schema, p.tablename AS parent_table
FROM pg_partitions p
JOIN pg_class c ON c.relname = p.partitiontablename
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = p.partitionschemaname
WHERE p.partitionschemaname NOT LIKE 'pg_%'
  AND p.partitionschemaname NOT LIKE 'gp_%'
  AND p.partitionschemaname != 'information_schema'
  AND p.partitionschemaname NOT LIKE '%toast%'
ORDER BY 1;
SQL

# === 3. Функция пакетной обработки обычных таблиц ===
collect_regular_batch() {
    # Аргументы – список OID
    if [ $# -eq 0 ]; then return; fi
    local oid_list
    oid_list=$(IFS=','; echo "$*")
    local err_log
    err_log=$(mktemp)

    psql -X -A -q -t 2>"$err_log" -v oid_list="$oid_list" <<'SQL'
SET application_name = 'table_size_collector';

-- Создаём временную таблицу один раз на сессию
CREATE TEMP TABLE _tmp_ins (
    table_oid       oid,
    schema_name     text,
    table_name      text,
    segment_id      integer,
    total_size_bytes bigint,
    collected_date  timestamptz
) DISTRIBUTED BY (table_oid);

-- Обрабатываем каждый OID из переданного списка
DO $$
DECLARE
    oid_arr oid[] := string_to_array(:'oid_list', ',')::oid[];
    cur_oid oid;
BEGIN
    FOREACH cur_oid IN ARRAY oid_arr LOOP
        -- Наполняем временную таблицу данными для текущего OID
        INSERT INTO _tmp_ins
        SELECT pc.oid,
               pn.nspname,
               pc.relname,
               gp_execution_segment(),
               pg_total_relation_size(pc.oid),
               now()
        FROM gp_dist_random('pg_class') pc
        JOIN pg_namespace pn ON pn.oid = pc.relnamespace
        WHERE pc.oid = cur_oid;

        -- Вставка в историю (без дублей)
        INSERT INTO dbatools.regular_sizes_history
            (table_oid, schema_name, table_name,
             segment_id, total_size_bytes, collected_date)
        SELECT t.table_oid, t.schema_name, t.table_name,
               t.segment_id, t.total_size_bytes, t.collected_date
        FROM _tmp_ins t
        WHERE t.table_oid = cur_oid
          AND NOT EXISTS (
              SELECT 1 FROM dbatools.regular_sizes_history h
              WHERE h.table_oid = t.table_oid
                AND h.segment_id = t.segment_id
                AND h.collected_date = t.collected_date
          );

        -- Вставка дельт
        INSERT INTO dbatools.regular_sizes_delta
            (table_oid, schema_name, table_name,
             segment_id, total_size_bytes, collected_date,
             prev_size_bytes, size_delta, delta_period)
        SELECT t.table_oid, t.schema_name, t.table_name,
               t.segment_id, t.total_size_bytes, t.collected_date,
               prev.total_size_bytes,
               t.total_size_bytes - prev.total_size_bytes,
               t.collected_date - prev.collected_date
        FROM _tmp_ins t
        LEFT JOIN LATERAL (
            SELECT total_size_bytes, collected_date
            FROM dbatools.regular_sizes_history
            WHERE table_oid = t.table_oid
              AND segment_id = t.segment_id
              AND collected_date < t.collected_date
            ORDER BY collected_date DESC
            LIMIT 1
        ) prev ON true
        WHERE t.table_oid = cur_oid
          AND NOT EXISTS (
              SELECT 1 FROM dbatools.regular_sizes_delta d
              WHERE d.table_oid = t.table_oid
                AND d.segment_id = t.segment_id
                AND d.collected_date = t.collected_date
          );

        -- Удаляем данные из временной таблицы для следующего OID
        DELETE FROM _tmp_ins WHERE table_oid = cur_oid;
    END LOOP;
END $$;

-- Удаляем временную таблицу в конце сессии
DROP TABLE IF EXISTS _tmp_ins;
SQL

    if [ -s "$err_log" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') [ERROR] Пакетная ошибка: $(cat "$err_log")" >> "$LOGFILE"
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Обработана группа OID: ${oid_list}" >> "$LOGFILE"
    fi
    rm -f "$err_log"
}

# === 4. Функция пакетной обработки партиций ===
collect_partition_batch() {
    # Получаем строки вида oid|schema|part|parent_schema|parent через xargs
    local data="$*"
    if [ -z "$data" ]; then return; fi
    local err_log
    err_log=$(mktemp)

    # Передаём как массив строк
    psql -X -A -q -t 2>"$err_log" -v part_data="$data" <<'SQL'
SET application_name = 'table_size_collector';

CREATE TEMP TABLE _tmp_ins_part (
    partition_oid   oid,
    schema_name     text,
    partition_name  text,
    parent_schema   text,
    parent_table    text,
    segment_id      integer,
    total_size_bytes bigint,
    collected_date  timestamptz
) DISTRIBUTED BY (partition_oid);

DO $$
DECLARE
    lines text[] := string_to_array(:'part_data', E'\n');
    line text;
    fields text[];
    cur_oid oid;
    cur_schema text;
    cur_part text;
    cur_parent_schema text;
    cur_parent_table text;
BEGIN
    FOREACH line IN ARRAY lines LOOP
        IF line = '' THEN CONTINUE; END IF;
        fields := string_to_array(line, '|');
        IF array_length(fields,1) <> 5 THEN
            RAISE WARNING 'Неверная строка партиции: %', line;
            CONTINUE;
        END IF;

        cur_oid           := fields[1]::oid;
        cur_schema        := fields[2];
        cur_part          := fields[3];
        cur_parent_schema := fields[4];
        cur_parent_table  := fields[5];

        INSERT INTO _tmp_ins_part
        SELECT cur_oid,
               cur_schema,
               cur_part,
               cur_parent_schema,
               cur_parent_table,
               gp_execution_segment(),
               pg_total_relation_size(pc.oid),
               now()
        FROM gp_dist_random('pg_class') pc
        WHERE pc.oid = cur_oid;

        INSERT INTO dbatools.partition_sizes_history
            (partition_oid, schema_name, partition_name,
             parent_schema, parent_table, segment_id,
             total_size_bytes, collected_date)
        SELECT t.partition_oid, t.schema_name, t.partition_name,
               t.parent_schema, t.parent_table, t.segment_id,
               t.total_size_bytes, t.collected_date
        FROM _tmp_ins_part t
        WHERE t.partition_oid = cur_oid
          AND NOT EXISTS (
              SELECT 1 FROM dbatools.partition_sizes_history h
              WHERE h.partition_oid = t.partition_oid
                AND h.segment_id = t.segment_id
                AND h.collected_date = t.collected_date
          );

        INSERT INTO dbatools.partition_sizes_delta
            (partition_oid, schema_name, partition_name,
             parent_schema, parent_table, segment_id,
             total_size_bytes, collected_date,
             prev_size_bytes, size_delta, delta_period)
        SELECT t.partition_oid, t.schema_name, t.partition_name,
               t.parent_schema, t.parent_table, t.segment_id,
               t.total_size_bytes, t.collected_date,
               prev.total_size_bytes,
               t.total_size_bytes - prev.total_size_bytes,
               t.collected_date - prev.collected_date
        FROM _tmp_ins_part t
        LEFT JOIN LATERAL (
            SELECT total_size_bytes, collected_date
            FROM dbatools.partition_sizes_history
            WHERE partition_oid = t.partition_oid
              AND segment_id = t.segment_id
              AND collected_date < t.collected_date
            ORDER BY collected_date DESC
            LIMIT 1
        ) prev ON true
        WHERE t.partition_oid = cur_oid
          AND NOT EXISTS (
              SELECT 1 FROM dbatools.partition_sizes_delta d
              WHERE d.partition_oid = t.partition_oid
                AND d.segment_id = t.segment_id
                AND d.collected_date = t.collected_date
          );

        DELETE FROM _tmp_ins_part WHERE partition_oid = cur_oid;
    END LOOP;
END $$;

DROP TABLE IF EXISTS _tmp_ins_part;
SQL

    if [ -s "$err_log" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') [ERROR] Пакетная ошибка партиций: $(cat "$err_log")" >> "$LOGFILE"
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S') [INFO] Обработана группа партиций" >> "$LOGFILE"
    fi
    rm -f "$err_log"
}

export -f collect_regular_batch
export -f collect_partition_batch
export LOGFILE

# === 5. Запуск сбора обычных таблиц ===
echo "$(date '+%Y-%m-%d %H:%M:%S') [START] Начало сбора обычных таблиц (пакеты по $BATCH_SIZE, параллельно $PARALLEL)" >> "$LOGFILE"
xargs -P "$PARALLEL" -n "$BATCH_SIZE" bash -c 'collect_regular_batch "$@"' _ < "$WORKDIR/regular_oids.txt"
echo "$(date '+%Y-%m-%d %H:%M:%S') [FINISH] Обычные таблицы обработаны" >> "$LOGFILE"

# === 6. Запуск сбора партиций ===
# Для партиций каждая строка – это полный набор полей, поэтому используем -L (по числу строк, а не аргументов)
echo "$(date '+%Y-%m-%d %H:%M:%S') [START] Начало сбора партиций (пакеты по $BATCH_SIZE, параллельно $PARALLEL)" >> "$LOGFILE"
xargs -P "$PARALLEL" -L "$BATCH_SIZE" bash -c 'collect_partition_batch "$@"' _ < "$WORKDIR/partition_oids.txt"
echo "$(date '+%Y-%m-%d %H:%M:%S') [FINISH] Партиции обработаны" >> "$LOGFILE"

echo "$(date '+%Y-%m-%d %H:%M:%S') [END] Скрипт завершён" >> "$LOGFILE"
