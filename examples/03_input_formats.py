"""
Case 3 — every input log() accepts, and how items map onto logs.log_entry.

Each item becomes one row. Keys that match a column set it:
    message, level, logger_name, logged_at, log_id,
    run_id, correlation_id, tenant_user_id, use_case_id, accelerator_id
Every other key goes into metadata_json. `message` is required (the column
is NOT NULL): put it in the item, or pass message= as the default for the call.

Runs with no database (memory target).

    python examples/03_input_formats.py
"""

from _settings import TENANT

from c66_logger import AuditLogger, InvalidLogInputError


def main() -> None:
    audit = AuditLogger(**TENANT, target_type="memory", mode="sync", logger_name="formats_demo")

    audit.info("a plain one-line string is the message")                         # 1 row
    audit.info(message="or pass message= with no items")                          # 1 row
    audit.info({"message": "one dict", "order_id": 1})                            # 1 row, order_id -> metadata
    audit.info({"message": "several"}, {"message": "items"}, {"message": "at once"})  # 3 rows
    audit.info([{"message": "a list"}, {"message": "of dicts"}])                  # 2 rows
    audit.info('{"message": "a JSON object", "sku": "A1"}')                       # 1 row
    audit.info('[{"message": "a JSON array"}, {"message": "of objects"}]')        # 2 rows
    audit.info('{"message": "JSON Lines"}\n{"message": "one per line"}')          # 2 rows

    # CSV with a header row: one row per line; values arrive as strings
    audit.info("message,order_id,status\norder created,3001,new\norder paid,3001,paid")
    # CSV without a header: name the columns
    audit.info("refund requested,3001,damaged", fmt="csv", csv_fieldnames=["message", "order_id", "reason"])

    # message= fills in for items that don't have their own
    audit.info({"order_id": 4001}, {"order_id": 4002}, message="bulk update")

    # items can set columns: level, logged_at (historical import), logger_name
    audit.log("message,level,logged_at,logger_name\n"
              "legacy error,ERROR,2026-09-01T10:00:00Z,legacy_system\n"
              "legacy info,INFO,2026-09-01T10:05:00Z,legacy_system")

    # levels are limited to what chk_log_entry_level allows
    for bad in ({"message": "x", "level": "AUDIT"}, {"order_id": 5}, 12345):
        try:
            audit.info({"message": "fine"}, bad)
        except InvalidLogInputError as exc:
            print(f"rejected before writing anything: {exc}")

    print(f"\n{len(audit.sink.entries)} log_entry rows:")
    for e in audit.sink.entries:
        print(f"  {e.level:<7} {e.logger_name:<14} {e.message:<40} {e.metadata_json}")
    audit.close()


if __name__ == "__main__":
    main()
