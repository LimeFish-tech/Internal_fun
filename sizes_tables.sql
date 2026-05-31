-- Обычные таблицы: история
CREATE TABLE dbatools.regular_sizes_history (
    id              bigserial PRIMARY KEY,
    table_oid       oid NOT NULL,
    schema_name     text NOT NULL,
    table_name      text NOT NULL,
    segment_id      integer NOT NULL,
    total_size_bytes bigint NOT NULL,
    collected_date  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON dbatools.regular_sizes_history (table_oid, segment_id, collected_date DESC);

-- Обычные таблицы: дельты
CREATE TABLE dbatools.regular_sizes_delta (
    id              bigserial PRIMARY KEY,
    table_oid       oid NOT NULL,
    schema_name     text NOT NULL,
    table_name      text NOT NULL,
    segment_id      integer NOT NULL,
    total_size_bytes bigint NOT NULL,
    collected_date  timestamptz NOT NULL,
    prev_size_bytes bigint,
    size_delta      bigint,
    delta_period    interval
);
CREATE INDEX ON dbatools.regular_sizes_delta (table_oid, segment_id, collected_date DESC);

-- Партиции: история
CREATE TABLE dbatools.partition_sizes_history (
    id              bigserial PRIMARY KEY,
    partition_oid   oid NOT NULL,
    schema_name     text NOT NULL,
    partition_name  text NOT NULL,
    parent_schema   text NOT NULL,
    parent_table    text NOT NULL,
    segment_id      integer NOT NULL,
    total_size_bytes bigint NOT NULL,
    collected_date  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON dbatools.partition_sizes_history (partition_oid, segment_id, collected_date DESC);

-- Партиции: дельты
CREATE TABLE dbatools.partition_sizes_delta (
    id              bigserial PRIMARY KEY,
    partition_oid   oid NOT NULL,
    schema_name     text NOT NULL,
    partition_name  text NOT NULL,
    parent_schema   text NOT NULL,
    parent_table    text NOT NULL,
    segment_id      integer NOT NULL,
    total_size_bytes bigint NOT NULL,
    collected_date  timestamptz NOT NULL,
    prev_size_bytes bigint,
    size_delta      bigint,
    delta_period    interval
);
CREATE INDEX ON dbatools.partition_sizes_delta (partition_oid, segment_id, collected_date DESC);
