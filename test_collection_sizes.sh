#!/bin/bash
set -euo pipefail

# === Настройки подключения ===
export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGDATABASE="${PGDATABASE:-postgres}"
export PGUSER="${PGUSER:-gpadmin}"
export PGPASSWORD="${PGPASSWORD:-}"
export PGAPPNAME="${PGAPPNAME:-table_size_collector}"
PARALLEL="${PARALLEL:-4}"
WORKDIR="${WORKDIR:-/tmp/table_sizing}"
mkdir -p "$WORKDIR"

# === 1. Получить OID обычных таблиц ===
psql -X -A -t -o "$WORKDIR/regular_oids.txt" <<'SQL'
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

# === 2. Получить OID партиций + родительскую информацию ===
psql -X -A -t -F '|' -o "$WORKDIR/partition_oids.txt" <<'SQL'
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

# === 3. Функция сбора для одной обычной таблицы ===
collect_regular() {
    local oid="$1"
    psql -X -A -t --application_name="$PGAPPNAME" <<SQL
BEGIN;

-- Временная таблица с данными текущего замера
CREATE TEMP TABLE _tmp_ins AS
SELECT pc.oid AS table_oid,
       pn.nspname AS schema_name,
       pc.relname AS table_name,
       gp_execution_segment() AS segment_id,
       pg_total_relation_size(pc.oid) AS total_size_bytes,
       now() AS collected_date
FROM gp_dist_random('pg_class') pc
JOIN pg_namespace pn ON pn.oid = pc.relnamespace
WHERE pc.oid = ${oid}
RETURNING *;

-- Вставка в историю
INSERT INTO dbatools.regular_sizes_history
    (table_oid, schema_name, table_name, segment_id, total_size_bytes, collected_date)
SELECT table_oid, schema_name, table_name, segment_id, total_size_bytes, collected_date
FROM _tmp_ins;

-- Вставка дельты, используя предыдущую запись для того же (oid, segment_id)
INSERT INTO dbatools.regular_sizes_delta
    (table_oid, schema_name, table_name, segment_id, total_size_bytes,
     collected_date, prev_size_bytes, size_delta, delta_period)
SELECT i.table_oid,
       i.schema_name,
       i.table_name,
       i.segment_id,
       i.total_size_bytes,
       i.collected_date,
       prev.total_size_bytes AS prev_size_bytes,
       i.total_size_bytes - prev.total_size_bytes AS size_delta,
       i.collected_date - prev.collected_date AS delta_period
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

DROP TABLE _tmp_ins;
COMMIT;
SQL
}

# === 4. Функция сбора для одной партиции ===
collect_partition() {
    local oid="$1"
    local schema="$2"
    local part_name="$3"
    local parent_schema="$4"
    local parent_table="$5"
    psql -X -A -t --application_name="$PGAPPNAME" <<SQL
BEGIN;

CREATE TEMP TABLE _tmp_ins AS
SELECT ${oid} AS partition_oid,
       '${schema}' AS schema_name,
       '${part_name}' AS partition_name,
       '${parent_schema}' AS parent_schema,
       '${parent_table}' AS parent_table,
       gp_execution_segment() AS segment_id,
       pg_total_relation_size(pc.oid) AS total_size_bytes,
       now() AS collected_date
FROM gp_dist_random('pg_class') pc
WHERE pc.oid = ${oid}
RETURNING *;

INSERT INTO dbatools.partition_sizes_history
    (partition_oid, schema_name, partition_name,
     parent_schema, parent_table, segment_id, total_size_bytes, collected_date)
SELECT partition_oid, schema_name, partition_name,
       parent_schema, parent_table, segment_id, total_size_bytes, collected_date
FROM _tmp_ins;

INSERT INTO dbatools.partition_sizes_delta
    (partition_oid, schema_name, partition_name,
     parent_schema, parent_table, segment_id, total_size_bytes,
     collected_date, prev_size_bytes, size_delta, delta_period)
SELECT i.partition_oid,
       i.schema_name,
       i.partition_name,
       i.parent_schema,
       i.parent_table,
       i.segment_id,
       i.total_size_bytes,
       i.collected_date,
       prev.total_size_bytes AS prev_size_bytes,
       i.total_size_bytes - prev.total_size_bytes AS size_delta,
       i.collected_date - prev.collected_date AS delta_period
FROM _tmp_ins i
LEFT JOIN LATERAL (
    SELECT total_size_bytes, collected_date
    FROM dbatools.partition_sizes_history
    WHERE partition_oid = i.partition_oid
      AND segment_id = i.segment_id
      AND collected_date < i.collected_date
    ORDER BY collected_date DESC
    LIMIT 1
) prev ON true;

DROP TABLE _tmp_ins;
COMMIT;
SQL
}

export -f collect_regular
export -f collect_partition

# === 5. Параллельный сбор обычных таблиц ===
echo "Сбор обычных таблиц..."
xargs -P "$PARALLEL" -I {} bash -c 'collect_regular "$@"' _ {} < "$WORKDIR/regular_oids.txt"

# === 6. Параллельный сбор партиций ===
echo "Сбор партиций..."
while IFS='|' read -r oid schema part_name parent_schema parent_table; do
    echo "$oid|$schema|$part_name|$parent_schema|$parent_table"
done < "$WORKDIR/partition_oids.txt" | \
xargs -P "$PARALLEL" -I {} bash -c '
    IFS="|" read -r oid schema part_name parent_schema parent_table <<<"$@"
    collect_partition "$oid" "$schema" "$part_name" "$parent_schema" "$parent_table"
' _ {}

echo "Сбор завершён."
