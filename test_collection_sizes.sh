#!/bin/bash
set -euo pipefail

# === Настройки ===
export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGDATABASE="${PGDATABASE:-postgres}"
export PGUSER="${PGUSER:-gpadmin}"
export PGPASSWORD="${PGPASSWORD:-}"
export PGAPPNAME="${PGAPPNAME:-table_size_collector}"
PARALLEL="${PARALLEL:-4}"
WORKDIR="${WORKDIR:-/tmp/table_sizing}"
mkdir -p "$WORKDIR"

# === 1. OID обычных таблиц ===
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

# === 2. OID партиций + родительская информация ===
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

# === 3. Сбор для одной обычной таблицы ===
collect_regular() {
    local oid="$1"
    psql -X -A -t --application_name="$PGAPPNAME" <<SQL
BEGIN;
WITH ins AS (
    INSERT INTO dbatools.regular_sizes_history
        (table_oid, schema_name, table_name, segment_id, total_size_bytes)
    SELECT pc.oid, pn.nspname, pc.relname,
           gp_execution_segment(),
           pg_total_relation_size(pc.oid)
    FROM gp_dist_random('pg_class') pc
    JOIN pg_namespace pn ON pn.oid = pc.relnamespace
    WHERE pc.oid = ${oid}
    RETURNING *
),
prev AS (
    SELECT total_size_bytes AS prev_size, collected_date AS prev_date
    FROM dbatools.regular_sizes_history
    WHERE table_oid = ${oid}
      AND segment_id = (SELECT segment_id FROM ins)
      AND id < (SELECT max(id) FROM ins)
    ORDER BY collected_date DESC
    LIMIT 1
)
INSERT INTO dbatools.regular_sizes_delta
    (table_oid, schema_name, table_name, segment_id, total_size_bytes,
     collected_date, prev_size_bytes, size_delta, delta_period)
SELECT i.table_oid, i.schema_name, i.table_name, i.segment_id,
       i.total_size_bytes, i.collected_date,
       p.prev_size,
       i.total_size_bytes - p.prev_size,
       i.collected_date - p.prev_date
FROM ins i
LEFT JOIN prev p ON true;
COMMIT;
SQL
}

# === 4. Сбор для одной партиции ===
collect_partition() {
    local oid="$1"
    local schema="$2"
    local part_name="$3"
    local parent_schema="$4"
    local parent_table="$5"
    psql -X -A -t --application_name="$PGAPPNAME" <<SQL
BEGIN;
WITH ins AS (
    INSERT INTO dbatools.partition_sizes_history
        (partition_oid, schema_name, partition_name,
         parent_schema, parent_table, segment_id, total_size_bytes)
    SELECT ${oid}, '${schema}', '${part_name}',
           '${parent_schema}', '${parent_table}',
           gp_execution_segment(),
           pg_total_relation_size(pc.oid)
    FROM gp_dist_random('pg_class') pc
    WHERE pc.oid = ${oid}
    RETURNING *
),
prev AS (
    SELECT total_size_bytes AS prev_size, collected_date AS prev_date
    FROM dbatools.partition_sizes_history
    WHERE partition_oid = ${oid}
      AND segment_id = (SELECT segment_id FROM ins)
      AND id < (SELECT max(id) FROM ins)
    ORDER BY collected_date DESC
    LIMIT 1
)
INSERT INTO dbatools.partition_sizes_delta
    (partition_oid, schema_name, partition_name,
     parent_schema, parent_table, segment_id, total_size_bytes,
     collected_date, prev_size_bytes, size_delta, delta_period)
SELECT i.partition_oid, i.schema_name, i.partition_name,
       i.parent_schema, i.parent_table, i.segment_id,
       i.total_size_bytes, i.collected_date,
       p.prev_size,
       i.total_size_bytes - p.prev_size,
       i.collected_date - p.prev_date
FROM ins i
LEFT JOIN prev p ON true;
COMMIT;
SQL
}

export -f collect_regular
export -f collect_partition

# === 5. Параллельный запуск: обычные таблицы ===
echo "Сбор обычных таблиц..."
xargs -P "$PARALLEL" -I {} bash -c 'collect_regular "$@"' _ {} < "$WORKDIR/regular_oids.txt"

# === 6. Параллельный запуск: партиции ===
echo "Сбор партиций..."
while IFS='|' read -r oid schema part_name parent_schema parent_table; do
    echo "$oid|$schema|$part_name|$parent_schema|$parent_table"
done < "$WORKDIR/partition_oids.txt" | \
xargs -P "$PARALLEL" -I {} bash -c '
    IFS="|" read -r oid schema part_name parent_schema parent_table <<<"$@"
    collect_partition "$oid" "$schema" "$part_name" "$parent_schema" "$parent_table"
' _ {}

echo "Сбор завершён."
