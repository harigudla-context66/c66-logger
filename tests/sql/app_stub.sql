-- TEST / LOCAL DEV ONLY. Minimal stand-ins for the app.* tables that the
-- logs.* foreign keys reference, plus demo rows the tests and examples use.
-- Your real app schema has more columns; don't run this against it.

CREATE SCHEMA IF NOT EXISTS app;
CREATE TABLE app.tenant (tenant_id uuid PRIMARY KEY, name text);
CREATE TABLE app.tenant_environment (environment_id uuid PRIMARY KEY, tenant_id uuid REFERENCES app.tenant, name text);
CREATE TABLE app.accelerator (accelerator_id uuid PRIMARY KEY, name text);
CREATE TABLE app.business_use_case (use_case_id uuid PRIMARY KEY, name text);
CREATE TABLE app.tenant_user (tenant_user_id uuid PRIMARY KEY, tenant_id uuid REFERENCES app.tenant, email text);

-- demo ids (also used by examples/_settings.py)
INSERT INTO app.tenant VALUES ('5a1e5f0c-0000-4000-8000-000000000001', 'salesforce');
INSERT INTO app.tenant_environment VALUES ('e0000000-0000-4000-8000-000000000001', '5a1e5f0c-0000-4000-8000-000000000001', 'prod');
INSERT INTO app.accelerator VALUES ('acce1e7a-0000-4000-8000-000000000001', 'lead-scoring');
INSERT INTO app.business_use_case VALUES ('0c0c0c0c-0000-4000-8000-000000000001', 'nightly-crm-sync');
INSERT INTO app.tenant_user VALUES ('00000000-0000-4000-8000-00000000a11c', '5a1e5f0c-0000-4000-8000-000000000001', 'alice@salesforce.example');

-- two more demo tenants for the multi-tenant example
INSERT INTO app.tenant VALUES ('4b0b5907-0000-4000-8000-000000000002', 'hubspot'), ('2e4de5c0-0000-4000-8000-000000000003', 'zendesk');
INSERT INTO app.tenant_environment VALUES
  ('e0000000-0000-4000-8000-000000000002', '4b0b5907-0000-4000-8000-000000000002', 'prod'),
  ('e0000000-0000-4000-8000-000000000003', '2e4de5c0-0000-4000-8000-000000000003', 'prod');
