BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SELECT pg_advisory_xact_lock(hashtextextended('worker_uplift_fetcher:state-contract:v1', 0));

CREATE SCHEMA IF NOT EXISTS worker_uplift_fetcher;

CREATE TABLE IF NOT EXISTS worker_uplift_fetcher.inbox (
  id bigserial PRIMARY KEY,
  message_id text NOT NULL UNIQUE,
  pipeline_run_id text NOT NULL,
  stage_execution_id text NOT NULL,
  source_stage text NOT NULL,
  source_message_id text NOT NULL,
  entity_kind text NOT NULL,
  entity_id text NOT NULL,
  schema_version integer NOT NULL CHECK (schema_version > 0),
  operation_version integer NOT NULL CHECK (operation_version > 0),
  idempotency_key text NOT NULL UNIQUE,
  payload_ref text NOT NULL,
  payload_digest text NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  processed_at timestamptz,
  status text NOT NULL DEFAULT 'received' CHECK (status IN ('received', 'processing', 'processed', 'duplicate', 'failed', 'parked')),
  diagnostic_metadata jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(diagnostic_metadata) = 'object'),
  sanitized_error_code text,
  sanitized_error_message text CHECK (sanitized_error_message IS NULL OR length(sanitized_error_message) <= 512),
  redact_after timestamptz NOT NULL DEFAULT (now() + interval '30 days'),
  claim_owner_message_id text,
  claim_expires_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (pipeline_run_id, stage_execution_id, source_message_id)
);

CREATE TABLE IF NOT EXISTS worker_uplift_fetcher.outbox (
  id bigserial PRIMARY KEY,
  outbox_message_id text NOT NULL UNIQUE,
  pipeline_run_id text NOT NULL,
  stage_execution_id text NOT NULL,
  destination_stage text NOT NULL,
  routing_key text NOT NULL,
  entity_kind text NOT NULL,
  entity_id text NOT NULL,
  schema_version integer NOT NULL CHECK (schema_version > 0),
  operation_version integer NOT NULL CHECK (operation_version > 0),
  idempotency_key text NOT NULL UNIQUE,
  payload_ref text NOT NULL,
  payload_digest text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  confirmed_at timestamptz,
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'published', 'confirmed', 'retrying', 'dead_lettered', 'cancelled')),
  diagnostic_metadata jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(diagnostic_metadata) = 'object'),
  sanitized_error_code text,
  sanitized_error_message text CHECK (sanitized_error_message IS NULL OR length(sanitized_error_message) <= 512),
  redact_after timestamptz NOT NULL DEFAULT (now() + interval '30 days'),
  claim_owner_key text,
  claim_expires_at timestamptz,
  publication_command jsonb CHECK (publication_command IS NULL OR jsonb_typeof(publication_command) = 'object'),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (pipeline_run_id, stage_execution_id, destination_stage, entity_id, operation_version)
);

CREATE TABLE IF NOT EXISTS worker_uplift_fetcher.fetch_versions (
  id bigserial PRIMARY KEY,
  feed_url text NOT NULL,
  feed_id text,
  fetch_version integer NOT NULL CHECK (fetch_version > 0),
  source_etag text,
  source_last_modified text,
  content_fingerprint text,
  http_status integer CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
  status text NOT NULL CHECK (status IN ('fetched', 'not_modified', 'failed', 'parse_failed')),
  payload_ref text,
  payload_digest text NOT NULL,
  fetched_at timestamptz NOT NULL DEFAULT now(),
  sanitized_error_code text,
  sanitized_error_message text CHECK (sanitized_error_message IS NULL OR length(sanitized_error_message) <= 512),
  diagnostic_metadata jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(diagnostic_metadata) = 'object'),
  redact_after timestamptz NOT NULL DEFAULT (now() + interval '30 days'),
  UNIQUE (feed_url, fetch_version),
  UNIQUE (feed_url, payload_digest)
);

CREATE TABLE IF NOT EXISTS worker_uplift_fetcher.fetch_outcomes (
  id bigserial PRIMARY KEY,
  feed_id text NOT NULL,
  feed_url text NOT NULL,
  fetch_status text NOT NULL CHECK (fetch_status IN ('success', 'unchanged', 'transient_failure', 'permanent_failure')),
  fetched_at timestamptz NOT NULL,
  http_status integer CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
  body_bytes integer NOT NULL CHECK (body_bytes >= 0),
  item_count integer NOT NULL CHECK (item_count >= 0),
  duration_ms integer NOT NULL CHECK (duration_ms >= 0),
  failure_class text,
  failure_code text,
  retryable boolean,
  diagnostic_metadata jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(diagnostic_metadata) = 'object'),
  redact_after timestamptz NOT NULL DEFAULT (now() + interval '30 days'),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS worker_uplift_fetcher.feed_health_projections (
  id bigserial PRIMARY KEY,
  feed_url text NOT NULL,
  status text NOT NULL CHECK (status IN ('healthy', 'warning', 'failed', 'paused')),
  last_success_at timestamptz,
  last_failure_at timestamptz,
  consecutive_failures integer NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
  projection_version integer NOT NULL CHECK (projection_version > 0),
  sanitized_error_code text,
  sanitized_error_message text CHECK (sanitized_error_message IS NULL OR length(sanitized_error_message) <= 512),
  diagnostic_metadata jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(diagnostic_metadata) = 'object'),
  redact_after timestamptz NOT NULL DEFAULT (now() + interval '30 days'),
  UNIQUE (feed_url, projection_version)
);

CREATE TABLE IF NOT EXISTS worker_uplift_fetcher.state_contract (
  component text PRIMARY KEY,
  contract_version integer NOT NULL CHECK (contract_version > 0),
  migrated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO worker_uplift_fetcher.state_contract (component, contract_version)
VALUES ('fetcher_state_store', 1)
ON CONFLICT (component)
DO UPDATE SET contract_version = greatest(worker_uplift_fetcher.state_contract.contract_version, EXCLUDED.contract_version),
              migrated_at = now();

CREATE INDEX IF NOT EXISTS worker_uplift_fetcher_inbox_status_received_idx
  ON worker_uplift_fetcher.inbox (status, received_at);
CREATE INDEX IF NOT EXISTS worker_uplift_fetcher_outbox_status_created_idx
  ON worker_uplift_fetcher.outbox (status, created_at);
CREATE INDEX IF NOT EXISTS worker_uplift_fetcher_outbox_candidate_idx
  ON worker_uplift_fetcher.outbox (entity_id, created_at DESC)
  WHERE entity_kind = 'candidate';
CREATE INDEX IF NOT EXISTS worker_uplift_fetcher_fetch_state_idx
  ON worker_uplift_fetcher.fetch_versions (feed_id, fetched_at DESC, id DESC)
  WHERE feed_id IS NOT NULL AND status IN ('fetched', 'not_modified');
CREATE INDEX IF NOT EXISTS worker_uplift_fetcher_fetch_outcomes_feed_idx
  ON worker_uplift_fetcher.fetch_outcomes (feed_id, fetched_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS worker_uplift_fetcher_fetch_outcomes_redact_idx
  ON worker_uplift_fetcher.fetch_outcomes (redact_after, id);
CREATE INDEX IF NOT EXISTS worker_uplift_fetcher_feed_health_latest_idx
  ON worker_uplift_fetcher.feed_health_projections (feed_url, projection_version DESC);

COMMIT;
