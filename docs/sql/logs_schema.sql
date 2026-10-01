-- Target tables for c66_logger, exactly as provided (2026-09-23).
-- c66_logger never creates or alters these; the package only inserts into
-- logs.log_entry and inserts/updates logs.log_run.
-- Requires the app.* tables referenced by the foreign keys.

CREATE SCHEMA IF NOT EXISTS logs;

-- logs.log_run definition (created first: log_entry.run_id references it)

CREATE TABLE logs.log_run (
	run_id uuid DEFAULT gen_random_uuid() NOT NULL,
	tenant_id uuid NOT NULL,
	environment_id uuid NOT NULL,
	accelerator_id uuid NULL,
	use_case_id uuid NULL,
	tenant_user_id uuid NULL,
	run_type varchar(50) NULL,
	status varchar(20) DEFAULT 'Running'::character varying NOT NULL,
	started_at timestamptz DEFAULT now() NOT NULL,
	ended_at timestamptz NULL,
	duration_ms int4 NULL,
	error_summary text NULL,
	metadata_json jsonb DEFAULT '{}'::jsonb NOT NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT chk_log_run_status CHECK (((status)::text = ANY (ARRAY[('Running'::character varying)::text, ('Completed'::character varying)::text, ('Failed'::character varying)::text, ('Cancelled'::character varying)::text]))),
	CONSTRAINT log_run_pkey PRIMARY KEY (run_id)
);
CREATE INDEX ix_log_run_accelerator_id ON logs.log_run USING btree (accelerator_id);
CREATE INDEX ix_log_run_status ON logs.log_run USING btree (status) WHERE ((status)::text = 'Running'::text);
CREATE INDEX ix_log_run_tenant_env_started ON logs.log_run USING btree (tenant_id, environment_id, started_at DESC);
CREATE INDEX ix_log_run_tenant_user_id ON logs.log_run USING btree (tenant_user_id);
CREATE INDEX ix_log_run_use_case_id ON logs.log_run USING btree (use_case_id);

ALTER TABLE logs.log_run ADD CONSTRAINT fk_log_run_accelerator FOREIGN KEY (accelerator_id) REFERENCES app.accelerator(accelerator_id) ON DELETE SET NULL;
ALTER TABLE logs.log_run ADD CONSTRAINT fk_log_run_environment FOREIGN KEY (environment_id) REFERENCES app.tenant_environment(environment_id) ON DELETE CASCADE;
ALTER TABLE logs.log_run ADD CONSTRAINT fk_log_run_tenant FOREIGN KEY (tenant_id) REFERENCES app.tenant(tenant_id) ON DELETE CASCADE;
ALTER TABLE logs.log_run ADD CONSTRAINT fk_log_run_tenant_user FOREIGN KEY (tenant_user_id) REFERENCES app.tenant_user(tenant_user_id) ON DELETE SET NULL;
ALTER TABLE logs.log_run ADD CONSTRAINT fk_log_run_use_case FOREIGN KEY (use_case_id) REFERENCES app.business_use_case(use_case_id) ON DELETE SET NULL;

-- logs.log_entry definition

CREATE TABLE logs.log_entry (
	log_id uuid DEFAULT gen_random_uuid() NOT NULL,
	tenant_id uuid NOT NULL,
	environment_id uuid NOT NULL,
	run_id uuid NULL,
	accelerator_id uuid NULL,
	use_case_id uuid NULL,
	tenant_user_id uuid NULL,
	correlation_id uuid NULL,
	logger_name varchar(200) NOT NULL,
	"level" varchar(10) NOT NULL,
	level_no int2 NOT NULL,
	message text NOT NULL,
	"module" varchar(200) NULL,
	func_name varchar(200) NULL,
	pathname varchar(500) NULL,
	line_no int4 NULL,
	process_id int4 NULL,
	process_name varchar(100) NULL,
	thread_id int8 NULL,
	thread_name varchar(100) NULL,
	exception_type varchar(200) NULL,
	exception_message text NULL,
	exception_traceback text NULL,
	metadata_json jsonb DEFAULT '{}'::jsonb NOT NULL,
	logged_at timestamptz NOT NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT chk_log_entry_level CHECK (((level)::text = ANY (ARRAY[('DEBUG'::character varying)::text, ('INFO'::character varying)::text, ('WARNING'::character varying)::text, ('ERROR'::character varying)::text, ('CRITICAL'::character varying)::text]))),
	CONSTRAINT log_entry_pkey PRIMARY KEY (log_id)
);
CREATE INDEX ix_log_entry_accelerator_id ON logs.log_entry USING btree (accelerator_id);
CREATE INDEX ix_log_entry_correlation_id ON logs.log_entry USING btree (correlation_id) WHERE (correlation_id IS NOT NULL);
CREATE INDEX ix_log_entry_errors ON logs.log_entry USING btree (tenant_id, logged_at DESC) WHERE (level_no >= 40);
CREATE INDEX ix_log_entry_run_id ON logs.log_entry USING btree (run_id);
CREATE INDEX ix_log_entry_tenant_env_logged ON logs.log_entry USING btree (tenant_id, environment_id, logged_at DESC);
CREATE INDEX ix_log_entry_tenant_level_logged ON logs.log_entry USING btree (tenant_id, level_no, logged_at DESC);
CREATE INDEX ix_log_entry_tenant_user_id ON logs.log_entry USING btree (tenant_user_id);
CREATE INDEX ix_log_entry_use_case_id ON logs.log_entry USING btree (use_case_id);

ALTER TABLE logs.log_entry ADD CONSTRAINT fk_log_entry_accelerator FOREIGN KEY (accelerator_id) REFERENCES app.accelerator(accelerator_id) ON DELETE SET NULL;
ALTER TABLE logs.log_entry ADD CONSTRAINT fk_log_entry_environment FOREIGN KEY (environment_id) REFERENCES app.tenant_environment(environment_id) ON DELETE CASCADE;
ALTER TABLE logs.log_entry ADD CONSTRAINT fk_log_entry_run FOREIGN KEY (run_id) REFERENCES logs.log_run(run_id) ON DELETE SET NULL;
ALTER TABLE logs.log_entry ADD CONSTRAINT fk_log_entry_tenant FOREIGN KEY (tenant_id) REFERENCES app.tenant(tenant_id) ON DELETE CASCADE;
ALTER TABLE logs.log_entry ADD CONSTRAINT fk_log_entry_tenant_user FOREIGN KEY (tenant_user_id) REFERENCES app.tenant_user(tenant_user_id) ON DELETE SET NULL;
ALTER TABLE logs.log_entry ADD CONSTRAINT fk_log_entry_use_case FOREIGN KEY (use_case_id) REFERENCES app.business_use_case(use_case_id) ON DELETE SET NULL;
