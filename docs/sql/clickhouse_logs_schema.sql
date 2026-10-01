-- ClickHouse tables for c66_logger: logs.log_entry and logs.log_run.
--
-- Same column names and meaning as the Postgres tables in logs_schema.sql, so
-- code and queries move between the two. Differences, because ClickHouse is
-- append-only and has no foreign keys:
--
--   * ids are not checked against app.* (no FKs in ClickHouse);
--   * CHECK constraints on level / status are kept (enforced on INSERT);
--   * log_entry is a ReplacingMergeTree keyed on log_id, and c66_logger sends an
--     insert_deduplication_token per batch, so a retried batch isn't duplicated;
--   * log_run is a ReplacingMergeTree(updated_at): starting and ending a run are
--     two row versions. Read the current state with FINAL:
--         SELECT * FROM logs.log_run FINAL WHERE tenant_id = '...' ORDER BY started_at DESC
--   * metadata_json is a String holding JSON: JSONExtractFloat(metadata_json, 'total_cost_usd').
--
-- Generated from c66_logger.sinks.clickhouse.clickhouse_ddl(); a test keeps them in sync.
-- c66_logger runs these itself only with ClickHouseSink(create_tables=True).

CREATE DATABASE IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.log_entry
(
    log_id              UUID,
    tenant_id           UUID,
    environment_id      UUID,
    run_id              Nullable(UUID),
    accelerator_id      Nullable(UUID),
    use_case_id         Nullable(UUID),
    tenant_user_id      Nullable(UUID),
    correlation_id      Nullable(UUID),
    logger_name         LowCardinality(String),
    level               LowCardinality(String),
    level_no            Int16,
    message             String,
    module              Nullable(String),
    func_name           Nullable(String),
    pathname            Nullable(String),
    line_no             Nullable(Int32),
    process_id          Nullable(Int32),
    process_name        Nullable(String),
    thread_id           Nullable(Int64),
    thread_name         Nullable(String),
    exception_type      Nullable(String),
    exception_message   Nullable(String),
    exception_traceback Nullable(String),
    metadata_json       String DEFAULT '{}',
    logged_at           DateTime64(6, 'UTC'),
    created_at          DateTime64(6, 'UTC') DEFAULT now64(6),
    INDEX ix_run_id run_id TYPE bloom_filter GRANULARITY 4,
    INDEX ix_correlation_id correlation_id TYPE bloom_filter GRANULARITY 4,
    CONSTRAINT chk_log_entry_level CHECK level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(logged_at)
ORDER BY (tenant_id, environment_id, logged_at, log_id)
SETTINGS non_replicated_deduplication_window = 1000;

CREATE TABLE IF NOT EXISTS logs.log_run
(
    run_id          UUID,
    tenant_id       UUID,
    environment_id  UUID,
    accelerator_id  Nullable(UUID),
    use_case_id     Nullable(UUID),
    tenant_user_id  Nullable(UUID),
    run_type        Nullable(String),
    status          LowCardinality(String),
    started_at      DateTime64(6, 'UTC'),
    ended_at        Nullable(DateTime64(6, 'UTC')),
    duration_ms     Nullable(Int32),
    error_summary   Nullable(String),
    metadata_json   String DEFAULT '{}',
    created_at      DateTime64(6, 'UTC') DEFAULT now64(6),
    updated_at      DateTime64(6, 'UTC'),
    CONSTRAINT chk_log_run_status CHECK status IN ('Running', 'Completed', 'Failed', 'Cancelled')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(started_at)
ORDER BY (tenant_id, environment_id, run_id);
