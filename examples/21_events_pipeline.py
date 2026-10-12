"""
21 — Data pipeline events with EventLogger: observability, audit and UI telemetry.

The data pipeline service reports its whole lifecycle — created, configured,
run (with steps), failed — and the UI's clicks arrive through an HTTP endpoint.
Every event is one generic, OpenTelemetry-shaped record with key/value
attributes; the policy decides criticality (P1 also goes to the alerts topic)
and how many days ClickHouse keeps it.

    python examples/21_events_pipeline.py                    # in memory, prints the events
    python examples/21_events_pipeline.py --kafka localhost:29092
        # with deploy/docker-compose.events.yml running: Kafka -> ClickHouse logs.events
"""

import argparse
import json

from _settings import ENVIRONMENT_ID, TENANT_ID

from c66_logger import EventLogger, continue_trace

# Normally loaded from the app's config file (YAML / JSON). Everything not set uses the defaults.
POLICY = {
    "rules": {
        "pipeline.run.failed": "P1",       # page someone
        "pipeline.step.error": "P2",
        "pipeline.config.*": "P3",
        "ui.*": "P4",
    },
}


def run_pipeline(events: EventLogger, pipeline_id: str, fail: bool) -> None:
    with events.span("pipeline.run", pipeline_id=pipeline_id, trigger="schedule") as run:
        for step, rows in (("extract", 1200), ("transform", 1180), ("load", 1180)):
            if fail and step == "transform":
                raise ValueError("column 'amount' is not numeric in row 412")
            events.observe("pipeline.step.completed", step=step, rows=rows)
        run.set_attributes(rows_loaded=1180, output="s3://lake/orders/2026-10-12/")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka", metavar="BOOTSTRAP", help="publish to Kafka instead of memory")
    args = parser.parse_args()
    target = (dict(target_type="kafka", connection={"bootstrap_servers": args.kafka})
              if args.kafka else dict(target_type="memory"))

    with EventLogger("pipeline-service", TENANT_ID, ENVIRONMENT_ID, service_version="1.4.0",
                     policy=POLICY, **target) as events:
        # 1. creation and configuration (observability + audit)
        events.observe("pipeline.created", pipeline_id="p-42", source={"type": "s3", "bucket": "raw"})
        events.audit("pipeline.config.updated", actor="user-17", pipeline_id="p-42",
                     field="schedule", old="daily", new="hourly")

        # 2. a run that succeeds, and one that fails (span: started -> completed / failed)
        run_pipeline(events, "p-42", fail=False)
        try:
            run_pipeline(events, "p-42", fail=True)
        except ValueError:
            pass  # the service's own error handling; the failure is already recorded

        # 3. a click in the UI, posted to the backend with the browser's traceparent;
        #    the backend continues the same trace for what the click triggers
        ui_payload = {"event_name": "ui.pipeline.run_clicked", "attributes": {"pipeline_id": "p-42"},
                      "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
        events.track_client_event(ui_payload, user_id="user-17", session_id="sess-9")
        with continue_trace(ui_payload["traceparent"]):
            events.observe("pipeline.run.requested", pipeline_id="p-42")

        events.flush()
        if not args.kafka:
            for e in events.sink.events:
                attrs = {k: v for k, v in e.attributes.items()
                         if not k.startswith(("code.", "thread.", "exception.stacktrace"))}
                print(f"{e.category:13} {e.event_name:26} {e.severity_text:7} {e.criticality} "
                      f"{e.retention_days:>3}d trace={e.trace_id[:8] or '-':8} {json.dumps(attrs)}")
        else:
            print("sent; query ClickHouse: SELECT EventName, Criticality, LogAttributes FROM logs.events")


if __name__ == "__main__":
    main()
