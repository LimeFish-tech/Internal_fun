#!/bin/bash
set -euo pipefail

# === Настройки ===
export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGDATABASE="${PGDATABASE:-postgres}"
export PGUSER="${PGUSER:-gpadmin}"
export PGPASSWORD="${PGPASSWORD:-}"
export PGAPPNAME="${PGAPPNAME:-table_size_collector}"
PARALLEL="${PARALLEL:-4}"               # число параллельных процессов
WORKDIR="${WORKDIR:-/tmp/table_sizing}"
LOGFILE="${LOGFILE:-$WORKDIR/collection.log}"

mkdir -p "$WORKDIR"

# === Функция логирования ===
log_msg() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$1] $2" >> "$LOGFILE"
}

# === 1. Получить OID обычных таблиц ===
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

# === 2. Получить данные партиций (oid + имена) ===
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

# === 3. Обработчик одной обычной таблицы ===
collect_regular_one() {
    local oid="$1"
    local err_log
    err_log=$(mktemp)   # временный файл для захвата ошибок psql

    psql -X -A -q -t --application_name="$PGAPPNAME" 2>"$err_log" <<SQL
CREATE TEMP TABLE IF NOT EXISTS _tmp_ins (
    table_oid       oid,
    schema_name     text,
    table_name      text,
    segment_id      integer,
    total_size_bytes bigint,
    collected_date  timestamptz
);

TRUNCATE _tmp_ins;

INSERT INTO _tmp_ins
SELECT pc.oid,
       pn.nspname,
       pc.relname,
       gp_execution_segment(),
       pg_total_relation_size(pc.oid),
       now()
FROM gp_dist_random('pg_class') pc
JOIN pg_namespace pn ON pn.oid = pc.relnamespace
WHERE pc.oid = ${oid};

INSERT INTO dbatools.regular_sizes_history
    (table_oid, schema_name, table_name,
     segment_id, total_size_bytes, collected_date)
SELECT table_oid, schema_name, table_name,
       segment_id, total_size_bytes, collected_date
FROM _tmp_ins;

INSERT INTO dbatools.regular_sizes_delta
    (table_oid, schema_name, table_name,
     segment_id, total_size_bytes, collected_date,
     prev_size_bytes, size_delta, delta_period)
SELECT i.table_oid, i.schema_name, i.table_name,
       i.segment_id, i.total_size_bytes, i.collected_date,
       prev.total_size_bytes,
       i.total_size_bytes - prev.total_size_bytes,
       i.collected_date - prev.collected_date
FROM _tmp_ins i
LEFT JOIN LATERAL (
    SELECT total_size_bytes, collected_date
    FROM dbatools.regular_sizes_history
    WHERE table_oid = i.table_oid
      AND segment_id = i.segment_id
      AND collected_date < i.collected_date
    ORDER BY collected_date DESC
    LIMIT 1
) prev ON true;
SQL

    if [ -s "$err_log" ]; then
        log_msg "ERROR" "Ошибка обработки OID ${oid}: $(cat "$err_log")"
    else
        log_msg "INFO" "Обработан OID ${oid}"
    fi
    rm -f "$err_log"
}

# === 4. Обработчик одной партиции (вход: строка oid|schema|part|parent_schema|parent) ===
collect_partition_one() {
    local line="$1"
    IFS='|' read -r oid schema part_name parent_schema parent_table <<< "$line"
    local err_log
    err_log=$(mktemp)

    psql -X -A -q -t --application_name="$PGAPPNAME" 2>"$err_log" <<SQL
CREATE TEMP TABLE IF NOT EXISTS _tmp_ins_part (
    partition_oid   oid,
    schema_name     text,
    partition_name  text,
    parent_schema   text,
    parent_table    text,
    segment_id      integer,
    total_size_bytes bigint,
    collected_date  timestamptz
);

TRUNCATE _tmp_ins_part;

INSERT INTO _tmp_ins_part
SELECT ${oid}::oid,
       '${schema}',
       '${part_name}',
       '${parent_schema}',
       '${parent_table}',
       gp_execution_segment(),
       pg_total_relation_size(pc.oid),
       now()
FROM gp_dist_random('pg_class') pc
WHERE pc.oid = ${oid};

INSERT INTO dbatools.partition_sizes_history
    (partition_oid, schema_name, partition_name,
     parent_schema, parent_table, segment_id,
     total_size_bytes, collected_date)
SELECT partition_oid, schema_name, partition_name,
       parent_schema, parent_table, segment_id,
       total_size_bytes, collected_date
FROM _tmp_ins_part;

INSERT INTO dbatools.partition_sizes_delta
    (partition_oid, schema_name, partition_name,
     parent_schema, parent_table, segment_id,
     total_size_bytes, collected_date,
     prev_size_bytes, size_delta, delta_period)
SELECT i.partition_oid, i.schema_name, i.partition_name,
       i.parent_schema, i.parent_table, i.segment_id,
       i.total_size_bytes, i.collected_date,
       prev.total_size_bytes,
       i.total_size_bytes - prev.total_size_bytes,
       i.collected_date - prev.collected_date
FROM _tmp_ins_part i
LEFT JOIN LATERAL (
    SELECT total_size_bytes, collected_date
    FROM dbatools.partition_sizes_history
    WHERE partition_oid = i.partition_oid
      AND segment_id = i.segment_id
      AND collected_date < i.collected_date
    ORDER BY collected_date DESC
    LIMIT 1
) prev ON true;
SQL

    if [ -s "$err_log" ]; then
        log_msg "ERROR" "Ошибка обработки партиции OID ${oid}: $(cat "$err_log")"
    else
        log_msg "INFO" "Обработана партиция OID ${oid} (${schema}.${part_name})"
    fi
    rm -f "$err_log"
}

export -f collect_regular_one
export -f collect_partition_one
export LOGFILE PGAPPNAME   # передаём в дочерние bash

# === 5. Параллельный запуск: обычные таблицы ===
log_msg "START" "Начало сбора обычных таблиц (параллельно: $PARALLEL)"
xargs -P "$PARALLEL" -n 1 bash -c 'collect_regular_one "$@"' _ < "$WORKDIR/regular_oids.txt"
log_msg "FINISH" "Обычные таблицы обработаны"

# === 6. Параллельный запуск: партиции ===
log_msg "START" "Начало сбора партиций (параллельно: $PARALLEL)"
xargs -P "$PARALLEL" -n 1 bash -c 'collect_partition_one "$@"' _ < "$WORKDIR/partition_oids.txt"
log_msg "FINISH" "Партиции обработаны"

log_msg "END" "Скрипт завершён"
