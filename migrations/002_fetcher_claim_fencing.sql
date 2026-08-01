BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SELECT pg_advisory_xact_lock(hashtextextended('worker_uplift_fetcher:state-contract:v2', 0));

ALTER TABLE worker_uplift_fetcher.inbox
  ADD COLUMN IF NOT EXISTS claim_token text;

ALTER TABLE worker_uplift_fetcher.inbox
  ADD COLUMN IF NOT EXISTS claim_acquired_at timestamptz;

ALTER TABLE worker_uplift_fetcher.outbox
  ADD COLUMN IF NOT EXISTS claim_token text;

ALTER TABLE worker_uplift_fetcher.outbox
  ADD COLUMN IF NOT EXISTS claim_acquired_at timestamptz;

-- A v1 lease has no opaque compare-and-set token and therefore cannot be
-- safely inherited by a v2 process. Make it immediately reclaimable; the v2
-- claim operation assigns a fresh token while holding the row lock.
UPDATE worker_uplift_fetcher.inbox
SET claim_expires_at = NULL,
    claim_acquired_at = NULL,
    updated_at = now()
WHERE claim_token IS NULL
  AND (claim_expires_at IS NOT NULL OR claim_acquired_at IS NOT NULL);

UPDATE worker_uplift_fetcher.outbox
SET claim_expires_at = NULL,
    claim_acquired_at = NULL,
    updated_at = now()
WHERE claim_token IS NULL
  AND (claim_expires_at IS NOT NULL OR claim_acquired_at IS NOT NULL);

DO $indexes$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_class index_relation
    JOIN pg_namespace index_namespace
      ON index_namespace.oid = index_relation.relnamespace
    WHERE index_namespace.nspname = 'worker_uplift_fetcher'
      AND index_relation.relname = 'worker_uplift_fetcher_inbox_claim_token_idx'
  ) AND NOT EXISTS (
    SELECT 1
    FROM pg_class index_relation
    JOIN pg_namespace index_namespace
      ON index_namespace.oid = index_relation.relnamespace
    JOIN pg_index index_row
      ON index_row.indexrelid = index_relation.oid
    JOIN pg_attribute attribute_row
      ON attribute_row.attrelid = index_row.indrelid
     AND attribute_row.attnum = index_row.indkey[0]
    WHERE index_namespace.nspname = 'worker_uplift_fetcher'
      AND index_relation.relname = 'worker_uplift_fetcher_inbox_claim_token_idx'
      AND index_row.indrelid = 'worker_uplift_fetcher.inbox'::regclass
      AND index_row.indisunique
      AND index_row.indisvalid
      AND index_row.indisready
      AND index_row.indnkeyatts = 1
      AND index_row.indnatts = 1
      AND attribute_row.attname = 'claim_token'
      AND pg_get_expr(index_row.indpred, index_row.indrelid)
        IN ('claim_token IS NOT NULL', '(claim_token IS NOT NULL)')
  ) THEN
    DROP INDEX worker_uplift_fetcher.worker_uplift_fetcher_inbox_claim_token_idx;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_class index_relation
    JOIN pg_namespace index_namespace
      ON index_namespace.oid = index_relation.relnamespace
    WHERE index_namespace.nspname = 'worker_uplift_fetcher'
      AND index_relation.relname = 'worker_uplift_fetcher_outbox_claim_token_idx'
  ) AND NOT EXISTS (
    SELECT 1
    FROM pg_class index_relation
    JOIN pg_namespace index_namespace
      ON index_namespace.oid = index_relation.relnamespace
    JOIN pg_index index_row
      ON index_row.indexrelid = index_relation.oid
    JOIN pg_attribute attribute_row
      ON attribute_row.attrelid = index_row.indrelid
     AND attribute_row.attnum = index_row.indkey[0]
    WHERE index_namespace.nspname = 'worker_uplift_fetcher'
      AND index_relation.relname = 'worker_uplift_fetcher_outbox_claim_token_idx'
      AND index_row.indrelid = 'worker_uplift_fetcher.outbox'::regclass
      AND index_row.indisunique
      AND index_row.indisvalid
      AND index_row.indisready
      AND index_row.indnkeyatts = 1
      AND index_row.indnatts = 1
      AND attribute_row.attname = 'claim_token'
      AND pg_get_expr(index_row.indpred, index_row.indrelid)
        IN ('claim_token IS NOT NULL', '(claim_token IS NOT NULL)')
  ) THEN
    DROP INDEX worker_uplift_fetcher.worker_uplift_fetcher_outbox_claim_token_idx;
  END IF;
END
$indexes$;

CREATE UNIQUE INDEX IF NOT EXISTS worker_uplift_fetcher_inbox_claim_token_idx
  ON worker_uplift_fetcher.inbox (claim_token)
  WHERE claim_token IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS worker_uplift_fetcher_outbox_claim_token_idx
  ON worker_uplift_fetcher.outbox (claim_token)
  WHERE claim_token IS NOT NULL;

DO $migration$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'worker_uplift_fetcher.inbox'::regclass
      AND conname = 'worker_uplift_fetcher_inbox_claim_lease_check'
      AND NOT (
        contype = 'c'
        AND convalidated
        AND pg_get_constraintdef(oid) LIKE '%claim_token IS NULL%'
        AND pg_get_constraintdef(oid) LIKE '%claim_acquired_at IS NULL%'
        AND pg_get_constraintdef(oid) LIKE '%claim_expires_at IS NULL%'
        AND pg_get_constraintdef(oid) LIKE '%claim_token IS NOT NULL%'
        AND pg_get_constraintdef(oid) LIKE '%char_length(claim_token) >= 8%'
        AND pg_get_constraintdef(oid) LIKE '%char_length(claim_token) <= 160%'
        AND pg_get_constraintdef(oid) LIKE '%COLLATE "C"%'
        AND strpos(pg_get_constraintdef(oid), '^[A-Za-z0-9][A-Za-z0-9._:-]{7,159}$') > 0
        AND pg_get_constraintdef(oid) LIKE '%claim_acquired_at IS NOT NULL%'
        AND pg_get_constraintdef(oid) LIKE '%claim_expires_at IS NOT NULL%'
        AND pg_get_constraintdef(oid) LIKE '%claim_expires_at > claim_acquired_at%'
        AND (
          pg_get_constraintdef(oid) LIKE '%claim_expires_at <=%claim_acquired_at +%00:05:00%'
          OR pg_get_constraintdef(oid) LIKE '%claim_expires_at <=%claim_acquired_at +%5 minutes%'
        )
      )
  ) THEN
    ALTER TABLE worker_uplift_fetcher.inbox
      DROP CONSTRAINT worker_uplift_fetcher_inbox_claim_lease_check;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'worker_uplift_fetcher.inbox'::regclass
      AND conname = 'worker_uplift_fetcher_inbox_claim_lease_check'
  ) THEN
    ALTER TABLE worker_uplift_fetcher.inbox
      ADD CONSTRAINT worker_uplift_fetcher_inbox_claim_lease_check CHECK (
        (claim_token IS NULL AND claim_acquired_at IS NULL AND claim_expires_at IS NULL)
        OR (
          claim_token IS NOT NULL
          AND char_length(claim_token) >= 8
          AND char_length(claim_token) <= 160
          AND (claim_token COLLATE "C") ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{7,159}$'
          AND claim_acquired_at IS NOT NULL
          AND claim_expires_at IS NOT NULL
          AND claim_expires_at > claim_acquired_at
          AND claim_expires_at <= claim_acquired_at + interval '5 minutes'
        )
      );
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'worker_uplift_fetcher.outbox'::regclass
      AND conname = 'worker_uplift_fetcher_outbox_claim_lease_check'
      AND NOT (
        contype = 'c'
        AND convalidated
        AND pg_get_constraintdef(oid) LIKE '%claim_token IS NULL%'
        AND pg_get_constraintdef(oid) LIKE '%claim_acquired_at IS NULL%'
        AND pg_get_constraintdef(oid) LIKE '%claim_expires_at IS NULL%'
        AND pg_get_constraintdef(oid) LIKE '%claim_token IS NOT NULL%'
        AND pg_get_constraintdef(oid) LIKE '%char_length(claim_token) >= 8%'
        AND pg_get_constraintdef(oid) LIKE '%char_length(claim_token) <= 160%'
        AND pg_get_constraintdef(oid) LIKE '%COLLATE "C"%'
        AND strpos(pg_get_constraintdef(oid), '^[A-Za-z0-9][A-Za-z0-9._:-]{7,159}$') > 0
        AND pg_get_constraintdef(oid) LIKE '%claim_acquired_at IS NOT NULL%'
        AND pg_get_constraintdef(oid) LIKE '%claim_expires_at IS NOT NULL%'
        AND pg_get_constraintdef(oid) LIKE '%claim_expires_at > claim_acquired_at%'
        AND (
          pg_get_constraintdef(oid) LIKE '%claim_expires_at <=%claim_acquired_at +%00:05:00%'
          OR pg_get_constraintdef(oid) LIKE '%claim_expires_at <=%claim_acquired_at +%5 minutes%'
        )
      )
  ) THEN
    ALTER TABLE worker_uplift_fetcher.outbox
      DROP CONSTRAINT worker_uplift_fetcher_outbox_claim_lease_check;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'worker_uplift_fetcher.outbox'::regclass
      AND conname = 'worker_uplift_fetcher_outbox_claim_lease_check'
  ) THEN
    ALTER TABLE worker_uplift_fetcher.outbox
      ADD CONSTRAINT worker_uplift_fetcher_outbox_claim_lease_check CHECK (
        (claim_token IS NULL AND claim_acquired_at IS NULL AND claim_expires_at IS NULL)
        OR (
          claim_token IS NOT NULL
          AND char_length(claim_token) >= 8
          AND char_length(claim_token) <= 160
          AND (claim_token COLLATE "C") ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{7,159}$'
          AND claim_acquired_at IS NOT NULL
          AND claim_expires_at IS NOT NULL
          AND claim_expires_at > claim_acquired_at
          AND claim_expires_at <= claim_acquired_at + interval '5 minutes'
        )
      );
  END IF;
END
$migration$;

INSERT INTO worker_uplift_fetcher.state_contract (component, contract_version)
VALUES ('fetcher_state_store', 2)
ON CONFLICT (component)
DO UPDATE SET contract_version = EXCLUDED.contract_version,
              migrated_at = now()
WHERE worker_uplift_fetcher.state_contract.contract_version < EXCLUDED.contract_version;

COMMIT;
