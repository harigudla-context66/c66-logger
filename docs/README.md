# c66_logger docs

| Document | What it covers |
| --- | --- |
| [implementation-guide.md](implementation-guide.md) | Step-by-step setup for the `salesforce` tenant: tables, install, create the logger (connection objects, `bind()`), runs, entries, ids, embedding, c66-chatbot, verify, troubleshoot |
| [architecture.md](architecture.md) | Decisions, what changed in v0.4, component map, sink contract, connections, status and open items |
| [chatbot-log-mapping.md](chatbot-log-mapping.md) | Every c66-chatbot logging call → `log_run` / `log_entry`, field by field; the `app_common.py` switch; dashboard queries; open points for Pal |
| [sql/logs_schema.sql](sql/logs_schema.sql) | Postgres DDL for `logs.log_run` and `logs.log_entry` (the target tables, as provided) |
| [sql/clickhouse_logs_schema.sql](sql/clickhouse_logs_schema.sql) | ClickHouse DDL for the same two tables (generated from `clickhouse_ddl()`) |
| [architecture-diagram.svg](architecture-diagram.svg) | Data-flow diagram, embeddable in Markdown |
| [architecture-diagram.html](architecture-diagram.html) | The same diagram as a standalone page with a component table; follows light/dark mode |
| [../examples/](../examples/README.md) | 18 runnable examples, one per use case |
