import { createHash, randomUUID } from "node:crypto";

import {
  assertWorkerEnvelope,
  getWorkerRoute,
  validateStagePayload
} from "@ramideltoro/nutsnews-worker-contracts";
import type {
  RuntimeClock,
  RuntimeIdempotencyClaimContext,
  RuntimeIdempotencyClaimReleaseResult,
  RuntimeIdempotencyClaimResult,
  RuntimeIdempotencyCompletion,
  RuntimeIdempotencyFailure
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  Pool,
  type PoolClient,
  type QueryResultRow
} from "pg";

import type {
  FetcherCandidateClaim,
  FetcherCandidateClaimResult,
  FetcherCandidatePublication,
  FetcherCandidatePublicationFailure,
  FetcherClaimedPendingCandidatePublication,
  FetcherDependencyProbe,
  FetcherDurableStateStore,
  FetcherFeedMetadata,
  FetcherFetchOutcome,
  FetcherPendingCandidatePublication,
  FetcherPendingPublicationQuery
} from "./dependencies.js";
import { FETCHER_MAX_CLAIM_LEASE_MS } from "./dependencies.js";

export const FETCHER_POSTGRES_SCHEMA = "worker_uplift_fetcher" as const;
export const FETCHER_POSTGRES_STATE_CONTRACT_VERSION = 2 as const;
export const FETCHER_FETCH_OUTCOME_RETENTION_DAYS = 30 as const;

function uniqueConstraintExpression(
  table: "inbox" | "outbox" | "fetch_versions",
  columns: readonly string[]
): string {
  const expectedColumns = columns
    .map((column) => `'${column.replaceAll("'", "''")}'`)
    .join(", ");

  return `EXISTS (
    SELECT 1
    FROM pg_constraint constraint_row
    WHERE constraint_row.conrelid = '${FETCHER_POSTGRES_SCHEMA}.${table}'::regclass
      AND constraint_row.contype = 'u'
      AND ARRAY(
        SELECT attribute.attname::text
        FROM unnest(constraint_row.conkey) WITH ORDINALITY AS constraint_key(attnum, position)
        JOIN pg_attribute attribute
          ON attribute.attrelid = constraint_row.conrelid
         AND attribute.attnum = constraint_key.attnum
        ORDER BY constraint_key.position
      ) = ARRAY[${expectedColumns}]::text[]
  )`;
}

function claimTokenIndexExpression(
  table: "inbox" | "outbox",
  indexName: string
): string {
  return `EXISTS (
    SELECT 1
    FROM pg_class index_relation
    JOIN pg_namespace index_namespace
      ON index_namespace.oid = index_relation.relnamespace
    JOIN pg_index index_row
      ON index_row.indexrelid = index_relation.oid
    JOIN pg_attribute attribute_row
      ON attribute_row.attrelid = index_row.indrelid
     AND attribute_row.attnum = index_row.indkey[0]
    WHERE index_namespace.nspname = '${FETCHER_POSTGRES_SCHEMA}'
      AND index_relation.relname = '${indexName}'
      AND index_row.indrelid = '${FETCHER_POSTGRES_SCHEMA}.${table}'::regclass
      AND index_row.indisunique
      AND index_row.indisvalid
      AND index_row.indisready
      AND index_row.indnkeyatts = 1
      AND index_row.indnatts = 1
      AND attribute_row.attname = 'claim_token'
      AND pg_get_expr(index_row.indpred, index_row.indrelid)
        IN ('claim_token IS NOT NULL', '(claim_token IS NOT NULL)')
  )`;
}

export interface PostgresFetcherPoolOptions {
  readonly databaseUrl: string;
  readonly applicationName: string;
  readonly maxConnections: number;
  readonly timeoutMs: number;
}

export interface PostgresFetcherStateStoreOptions {
  readonly pool: Pool;
  readonly clock: RuntimeClock;
  readonly leaseMs: number;
  readonly ownsPool?: boolean;
  readonly claimTokenFactory?: () => string;
}

export class FetcherStateOwnershipError extends Error {
  constructor(operation: string) {
    super(`Fetcher PostgreSQL state ownership was lost during ${operation}.`);
    this.name = "FetcherStateOwnershipError";
  }
}

export class FetcherStateContractError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FetcherStateContractError";
  }
}

export function createPostgresFetcherPool(options: PostgresFetcherPoolOptions): Pool {
  const pool = new Pool({
    connectionString: options.databaseUrl,
    application_name: options.applicationName,
    max: options.maxConnections,
    connectionTimeoutMillis: options.timeoutMs,
    idleTimeoutMillis: 30_000,
    query_timeout: options.timeoutMs,
    statement_timeout: options.timeoutMs,
    allowExitOnIdle: true
  });

  // An idle connection failure must be reflected by the next bounded probe;
  // it must not become an unhandled EventEmitter error that kills diagnostics.
  pool.on("error", () => undefined);
  return pool;
}

export class PostgresFetcherStateStore implements FetcherDurableStateStore {
  readonly name = "postgresql-fetcher-state-store";
  readonly mode = "postgresql" as const;
  readonly adapter = "backend-postgresql";
  readonly durable = true;
  readonly #pool: Pool;
  readonly #leaseMs: number;
  readonly #ownsPool: boolean;
  readonly #claimTokenFactory: () => string;

  constructor(options: PostgresFetcherStateStoreOptions) {
    this.#pool = options.pool;
    if (!Number.isInteger(options.leaseMs)
      || options.leaseMs <= 0
      || options.leaseMs > FETCHER_MAX_CLAIM_LEASE_MS) {
      throw new RangeError(`Fetcher claim lease must be an integer from 1 to ${String(FETCHER_MAX_CLAIM_LEASE_MS)} milliseconds.`);
    }

    this.#leaseMs = options.leaseMs;
    this.#ownsPool = options.ownsPool ?? false;
    this.#claimTokenFactory = options.claimTokenFactory ?? randomUUID;
  }

  async probe(): Promise<FetcherDependencyProbe> {
    try {
      const result = await this.#pool.query<StateContractRow>(
        `WITH
           inbox_shape AS (
             SELECT id, message_id, pipeline_run_id, stage_execution_id, source_stage,
                    source_message_id, entity_kind, entity_id, schema_version,
                    operation_version, idempotency_key, payload_ref, payload_digest,
                    received_at, processed_at, status, diagnostic_metadata,
                    sanitized_error_code, sanitized_error_message,
                    claim_owner_message_id, claim_token, claim_acquired_at,
                    claim_expires_at, updated_at
             FROM ${FETCHER_POSTGRES_SCHEMA}.inbox
             WHERE false
           ),
           outbox_shape AS (
             SELECT id, outbox_message_id, pipeline_run_id, stage_execution_id,
                    destination_stage, routing_key, entity_kind, entity_id,
                    schema_version, operation_version, idempotency_key, payload_ref,
                    payload_digest, created_at, published_at, confirmed_at, status,
                    diagnostic_metadata, sanitized_error_code, sanitized_error_message,
                    claim_owner_key, claim_token, claim_acquired_at,
                    claim_expires_at, publication_command, updated_at
             FROM ${FETCHER_POSTGRES_SCHEMA}.outbox
             WHERE false
           ),
           fetch_versions_shape AS (
             SELECT id, feed_url, feed_id, fetch_version, source_etag,
                    source_last_modified, content_fingerprint, http_status, status,
                    payload_ref, payload_digest, fetched_at, diagnostic_metadata
             FROM ${FETCHER_POSTGRES_SCHEMA}.fetch_versions
             WHERE false
           ),
           fetch_outcomes_shape AS (
             SELECT id, feed_id, feed_url, fetch_status, fetched_at, http_status,
                    body_bytes, item_count, duration_ms, failure_class, failure_code,
                    retryable, diagnostic_metadata, redact_after, created_at
             FROM ${FETCHER_POSTGRES_SCHEMA}.fetch_outcomes
             WHERE false
           ),
           feed_health_shape AS (
             SELECT id, feed_url, status, last_success_at, last_failure_at,
                    consecutive_failures, projection_version, sanitized_error_code,
                    sanitized_error_message, diagnostic_metadata
             FROM ${FETCHER_POSTGRES_SCHEMA}.feed_health_projections
             WHERE false
           )
         SELECT state_contract.contract_version,
                (SELECT count(*) = 0 FROM inbox_shape) AS inbox_shape_ready,
                (SELECT count(*) = 0 FROM outbox_shape) AS outbox_shape_ready,
                (SELECT count(*) = 0 FROM fetch_versions_shape) AS fetch_versions_shape_ready,
                (SELECT count(*) = 0 FROM fetch_outcomes_shape) AS fetch_outcomes_shape_ready,
                (SELECT count(*) = 0 FROM feed_health_shape) AS feed_health_shape_ready,
                has_schema_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}', 'USAGE') AS schema_usage_ready,
                has_table_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.inbox', 'SELECT')
                  AND has_table_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.inbox', 'INSERT')
                  AND has_table_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.inbox', 'UPDATE') AS inbox_privileges_ready,
                has_table_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.outbox', 'SELECT')
                  AND has_table_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.outbox', 'INSERT')
                  AND has_table_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.outbox', 'UPDATE') AS outbox_privileges_ready,
                has_table_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.fetch_versions', 'SELECT')
                  AND has_table_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.fetch_versions', 'INSERT')
                  AND has_table_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.fetch_versions', 'UPDATE') AS fetch_versions_privileges_ready,
                has_table_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.fetch_outcomes', 'INSERT') AS fetch_outcomes_privileges_ready,
                has_table_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.feed_health_projections', 'SELECT')
                  AND has_table_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.feed_health_projections', 'INSERT') AS feed_health_privileges_ready,
                has_sequence_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.inbox_id_seq', 'USAGE')
                  AND has_sequence_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.outbox_id_seq', 'USAGE')
                  AND has_sequence_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.fetch_versions_id_seq', 'USAGE')
                  AND has_sequence_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.fetch_outcomes_id_seq', 'USAGE')
                  AND has_sequence_privilege(current_user, '${FETCHER_POSTGRES_SCHEMA}.feed_health_projections_id_seq', 'USAGE') AS sequence_privileges_ready,
                (
                  SELECT count(*) = 15
                  FROM information_schema.columns
                  WHERE table_schema = '${FETCHER_POSTGRES_SCHEMA}'
                    AND (
                      (table_name = 'inbox' AND column_name = 'claim_owner_message_id' AND udt_name = 'text' AND is_nullable = 'YES')
                      OR (table_name = 'inbox' AND column_name = 'claim_token' AND udt_name = 'text' AND is_nullable = 'YES')
                      OR (table_name = 'inbox' AND column_name = 'claim_acquired_at' AND udt_name = 'timestamptz' AND is_nullable = 'YES')
                      OR (table_name = 'inbox' AND column_name = 'claim_expires_at' AND udt_name = 'timestamptz' AND is_nullable = 'YES')
                      OR (table_name = 'inbox' AND column_name = 'diagnostic_metadata' AND udt_name = 'jsonb' AND is_nullable = 'NO')
                      OR (table_name = 'outbox' AND column_name = 'claim_owner_key' AND udt_name = 'text' AND is_nullable = 'YES')
                      OR (table_name = 'outbox' AND column_name = 'claim_token' AND udt_name = 'text' AND is_nullable = 'YES')
                      OR (table_name = 'outbox' AND column_name = 'claim_acquired_at' AND udt_name = 'timestamptz' AND is_nullable = 'YES')
                      OR (table_name = 'outbox' AND column_name = 'claim_expires_at' AND udt_name = 'timestamptz' AND is_nullable = 'YES')
                      OR (table_name = 'outbox' AND column_name = 'publication_command' AND udt_name = 'jsonb' AND is_nullable = 'YES')
                      OR (table_name = 'fetch_versions' AND column_name = 'content_fingerprint' AND udt_name = 'text' AND is_nullable = 'YES')
                      OR (table_name = 'fetch_outcomes' AND column_name = 'duration_ms' AND udt_name = 'int4' AND is_nullable = 'NO')
                      OR (table_name = 'fetch_outcomes' AND column_name = 'redact_after' AND udt_name = 'timestamptz' AND is_nullable = 'NO')
                      OR (table_name = 'feed_health_projections' AND column_name = 'diagnostic_metadata' AND udt_name = 'jsonb' AND is_nullable = 'NO')
                      OR (table_name = 'state_contract' AND column_name = 'contract_version' AND udt_name = 'int4' AND is_nullable = 'NO')
                    )
                ) AS column_contract_ready,
                ${uniqueConstraintExpression("inbox", ["idempotency_key"])}
                  AND ${uniqueConstraintExpression("outbox", ["idempotency_key"])}
                  AND ${uniqueConstraintExpression("fetch_versions", ["feed_url", "payload_digest"])}
                  AND EXISTS (
                    SELECT 1
                    FROM pg_constraint constraint_row
                    WHERE constraint_row.conrelid = '${FETCHER_POSTGRES_SCHEMA}.outbox'::regclass
                      AND constraint_row.contype = 'c'
                      AND pg_get_constraintdef(constraint_row.oid) LIKE '%publication_command%'
                      AND pg_get_constraintdef(constraint_row.oid) LIKE '%jsonb_typeof%'
                  ) AS constraint_contract_ready,
                ${claimTokenIndexExpression("inbox", "worker_uplift_fetcher_inbox_claim_token_idx")}
                  AND ${claimTokenIndexExpression("outbox", "worker_uplift_fetcher_outbox_claim_token_idx")}
                  AND EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conrelid = '${FETCHER_POSTGRES_SCHEMA}.inbox'::regclass
                      AND conname = 'worker_uplift_fetcher_inbox_claim_lease_check'
                      AND contype = 'c'
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
                  AND EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conrelid = '${FETCHER_POSTGRES_SCHEMA}.outbox'::regclass
                      AND conname = 'worker_uplift_fetcher_outbox_claim_lease_check'
                      AND contype = 'c'
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
                  ) AS claim_fencing_ready
         FROM ${FETCHER_POSTGRES_SCHEMA}.state_contract AS state_contract
         WHERE state_contract.component = 'fetcher_state_store'
         LIMIT 1`
      );
      const row = result.rows[0];
      const ready = row !== undefined
        && row.contract_version >= FETCHER_POSTGRES_STATE_CONTRACT_VERSION
        && row.inbox_shape_ready
        && row.outbox_shape_ready
        && row.fetch_versions_shape_ready
        && row.fetch_outcomes_shape_ready
        && row.feed_health_shape_ready
        && row.schema_usage_ready
        && row.inbox_privileges_ready
        && row.outbox_privileges_ready
        && row.fetch_versions_privileges_ready
        && row.fetch_outcomes_privileges_ready
        && row.feed_health_privileges_ready
        && row.sequence_privileges_ready
        && row.column_contract_ready
        && row.constraint_contract_ready
        && row.claim_fencing_ready;

      return {
        status: ready ? "ok" : "unhealthy",
        summary: ready
          ? `backend PostgreSQL fetcher state contract v${String(row.contract_version)} ready`
          : "backend PostgreSQL fetcher state contract is missing or incomplete"
      };
    } catch {
      return {
        status: "unhealthy",
        summary: "backend PostgreSQL fetcher state probe failed"
      };
    }
  }

  async claim(
    idempotencyKey: string,
    context: RuntimeIdempotencyClaimContext
  ): Promise<RuntimeIdempotencyClaimResult> {
    return withTransaction(this.#pool, async (client) => {
      const claimToken = this.nextClaimToken();
      const inserted = await client.query<{ readonly received_at: Date }>(
        `INSERT INTO ${FETCHER_POSTGRES_SCHEMA}.inbox (
           message_id, pipeline_run_id, stage_execution_id, source_stage, source_message_id,
           entity_kind, entity_id, schema_version, operation_version, idempotency_key,
           payload_ref, payload_digest, received_at, status, diagnostic_metadata,
           claim_owner_message_id, claim_token, claim_acquired_at, claim_expires_at, updated_at
         ) VALUES (
           $1, $2, $3, $4, $5,
           $6, $7, $8, $9, $10,
           $11, $12, $13::timestamptz, 'processing', $14::jsonb,
           $1, $15, statement_timestamp(),
           statement_timestamp() + ($16::integer * interval '1 millisecond'), statement_timestamp()
         )
         ON CONFLICT (idempotency_key) DO NOTHING
         RETURNING received_at`,
        [
          context.envelope.messageId,
          context.envelope.correlationId,
          context.envelope.messageId,
          context.envelope.producer.name,
          context.envelope.causationId,
          context.envelope.aggregate.type,
          context.envelope.aggregate.id,
          context.envelope.schemaVersion,
          Math.max(1, context.envelope.aggregate.version),
          idempotencyKey,
          context.envelope.payloadRef.uri,
          context.envelope.payloadRef.digest ?? sha256Json(context.envelope.payloadRef),
          context.receivedAt,
          JSON.stringify({
            route: context.envelope.route,
            attempt: context.envelope.attempt
          }),
          claimToken,
          this.#leaseMs
        ]
      );

      if ((inserted.rowCount ?? 0) > 0) {
        return {
          status: "claimed",
          firstSeenAt: context.receivedAt,
          replay: false,
          claimToken
        };
      }

      const existing = await client.query<InboxClaimRow>(
        `SELECT status, received_at, processed_at, claim_owner_message_id, claim_token,
                claim_expires_at,
                (claim_expires_at IS NULL OR claim_expires_at <= statement_timestamp()) AS claim_expired
         FROM ${FETCHER_POSTGRES_SCHEMA}.inbox
         WHERE idempotency_key = $1
         FOR UPDATE`,
        [idempotencyKey]
      );
      const row = existing.rows[0];

      if (row === undefined) {
        return {
          status: "in-progress",
          firstSeenAt: context.receivedAt
        };
      }

      const firstSeenAt = isoString(row.received_at);

      if (row.status === "processed" || row.status === "duplicate") {
        return {
          status: "already-completed",
          firstSeenAt,
          completedAt: isoString(row.processed_at ?? row.received_at)
        };
      }

      const reclaimable = row.status === "failed"
        || row.status === "parked"
        || row.status === "received"
        || row.claim_expired;

      if (!reclaimable) {
        return {
          status: "in-progress",
          firstSeenAt
        };
      }

      await client.query(
        `UPDATE ${FETCHER_POSTGRES_SCHEMA}.inbox
         SET status = 'processing',
             processed_at = NULL,
             sanitized_error_code = NULL,
             sanitized_error_message = NULL,
             claim_owner_message_id = $2,
             claim_token = $3,
             claim_acquired_at = statement_timestamp(),
             claim_expires_at = statement_timestamp() + ($4::integer * interval '1 millisecond'),
             updated_at = statement_timestamp(),
             diagnostic_metadata = diagnostic_metadata || $5::jsonb
         WHERE idempotency_key = $1`,
        [
          idempotencyKey,
          context.envelope.messageId,
          claimToken,
          this.#leaseMs,
          JSON.stringify({
            replayedAt: context.receivedAt,
            replayMessageId: context.envelope.messageId,
            attempt: context.envelope.attempt
          })
        ]
      );

      return {
        status: "claimed",
        firstSeenAt,
        replay: true,
        claimToken
      };
    });
  }

  async markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    const result = await this.#pool.query(
      `UPDATE ${FETCHER_POSTGRES_SCHEMA}.inbox
       SET status = 'processed',
           processed_at = $4::timestamptz,
           claim_token = NULL,
           claim_acquired_at = NULL,
           claim_expires_at = NULL,
           updated_at = now(),
           diagnostic_metadata = diagnostic_metadata || $5::jsonb
       WHERE idempotency_key = $1
         AND claim_token = $2
         AND claim_owner_message_id = $3
         AND status = 'processing'
         AND claim_expires_at > statement_timestamp()
       RETURNING id`,
      [
        idempotencyKey,
        completion.claimToken,
        completion.messageId,
        completion.completedAt,
        JSON.stringify({
          completedMessageId: completion.messageId,
          completedStage: completion.stage
        })
      ]
    );

    assertOwned(result.rowCount, "markCompleted");
  }

  async markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    const result = await this.#pool.query(
      `UPDATE ${FETCHER_POSTGRES_SCHEMA}.inbox
       SET status = 'failed',
           sanitized_error_code = $4,
           sanitized_error_message = $5,
           claim_token = NULL,
           claim_acquired_at = NULL,
           claim_expires_at = NULL,
           updated_at = now(),
           diagnostic_metadata = diagnostic_metadata || $6::jsonb
       WHERE idempotency_key = $1
         AND claim_token = $2
         AND claim_owner_message_id = $3
         AND status = 'processing'
         AND claim_expires_at > statement_timestamp()
       RETURNING id`,
      [
        idempotencyKey,
        failure.claimToken,
        failure.messageId,
        sanitizeCode(failure.reason),
        sanitizeMessage(failure.reason),
        JSON.stringify({
          failedAt: failure.failedAt,
          failedMessageId: failure.messageId,
          failedStage: failure.stage,
          retryable: failure.retryable
        })
      ]
    );

    assertOwned(result.rowCount, "markFailed");
  }

  async releaseClaim(
    idempotencyKey: string,
    failure: RuntimeIdempotencyFailure
  ): Promise<RuntimeIdempotencyClaimReleaseResult> {
    return withTransaction(this.#pool, async (client) => {
      const released = await client.query(
        `UPDATE ${FETCHER_POSTGRES_SCHEMA}.inbox
         SET status = 'failed',
             sanitized_error_code = $4,
             sanitized_error_message = $5,
             claim_token = NULL,
             claim_acquired_at = NULL,
             claim_expires_at = NULL,
             updated_at = now(),
             diagnostic_metadata = diagnostic_metadata || $6::jsonb
         WHERE idempotency_key = $1
           AND claim_token = $2
           AND claim_owner_message_id = $3
           AND status = 'processing'
           AND claim_expires_at > statement_timestamp()
         RETURNING id`,
        [
          idempotencyKey,
          failure.claimToken,
          failure.messageId,
          sanitizeCode(failure.reason),
          sanitizeMessage(failure.reason),
          JSON.stringify({
            releasedAt: failure.failedAt,
            releasedMessageId: failure.messageId,
            releasedStage: failure.stage,
            retryable: failure.retryable
          })
        ]
      );

      if ((released.rowCount ?? 0) > 0) {
        return {
          status: "released"
        };
      }

      const current = await client.query<{ readonly status: string }>(
        `SELECT status
         FROM ${FETCHER_POSTGRES_SCHEMA}.inbox
         WHERE idempotency_key = $1
         FOR UPDATE`,
        [idempotencyKey]
      );

      return current.rows[0]?.status === "processed" || current.rows[0]?.status === "duplicate"
        ? {
            status: "preserved-completed"
          }
        : {
            status: "not-owned"
          };
    });
  }

  async getFeedMetadata(feedId: string): Promise<FetcherFeedMetadata | undefined> {
    const result = await this.#pool.query<FeedMetadataRow>(
      `SELECT feed_id, source_etag, source_last_modified, content_fingerprint, fetched_at
       FROM ${FETCHER_POSTGRES_SCHEMA}.fetch_versions
       WHERE feed_id = $1
         AND status IN ('fetched', 'not_modified')
       ORDER BY fetched_at DESC, id DESC
       LIMIT 1`,
      [feedId]
    );
    const row = result.rows[0];

    if (row === undefined) {
      return undefined;
    }

    return {
      feedId: row.feed_id,
      ...(row.source_etag === null ? {} : {
        etag: row.source_etag
      }),
      ...(row.source_last_modified === null ? {} : {
        lastModified: row.source_last_modified
      }),
      ...(row.content_fingerprint === null ? {} : {
        contentFingerprint: row.content_fingerprint
      }),
      fetchedAt: isoString(row.fetched_at)
    };
  }

  async recordFetchOutcome(outcome: FetcherFetchOutcome): Promise<void> {
    await withTransaction(this.#pool, async (client) => {
      await client.query(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        [JSON.stringify([
          outcome.feedId,
          outcome.feedUrl
        ])]
      );

      await client.query(
        `INSERT INTO ${FETCHER_POSTGRES_SCHEMA}.fetch_outcomes (
           feed_id, feed_url, fetch_status, fetched_at, http_status,
           body_bytes, item_count, duration_ms, failure_class, failure_code,
           retryable, diagnostic_metadata, redact_after
         ) VALUES (
           $1, $2, $3, $4::timestamptz, $5,
           $6, $7, $8, $9, $10,
           $11, $12::jsonb, now() + ($13::integer * interval '1 day')
         )`,
        [
          outcome.feedId,
          outcome.feedUrl,
          outcome.fetchStatus,
          outcome.fetchedAt,
          outcome.httpStatus ?? null,
          boundedInteger(outcome.bodyBytes),
          boundedInteger(outcome.itemCount),
          boundedInteger(outcome.durationMs),
          outcome.failure?.failureClass ?? null,
          outcome.failure?.code ?? null,
          outcome.failure?.retryable ?? null,
          JSON.stringify({
            diagnosticSample: boundedText(outcome.diagnosticSample, 512),
            failureAction: outcome.failure?.action,
            candidateCount: outcome.itemRefs?.length ?? 0
          }),
          FETCHER_FETCH_OUTCOME_RETENTION_DAYS
        ]
      );

      if (outcome.fetchStatus === "success" || outcome.fetchStatus === "unchanged") {
        await recordFetchVersion(client, outcome);
      }

      await recordFeedHealthProjection(client, outcome);
    });
  }

  async claimCandidate(
    candidateId: string,
    claim: FetcherCandidateClaim
  ): Promise<FetcherCandidateClaimResult> {
    return withTransaction(this.#pool, async (client) => {
      const command = claim.command;
      const route = getWorkerRoute(command.envelope.route);
      const claimToken = this.nextClaimToken();
      const publicationCommand = JSON.stringify(command);
      const inserted = await client.query(
        `INSERT INTO ${FETCHER_POSTGRES_SCHEMA}.outbox (
           outbox_message_id, pipeline_run_id, stage_execution_id, destination_stage,
           routing_key, entity_kind, entity_id, schema_version, operation_version,
           idempotency_key, payload_ref, payload_digest, created_at, status,
           diagnostic_metadata, claim_owner_key, claim_token, claim_acquired_at, claim_expires_at,
           publication_command, updated_at
         ) VALUES (
           $1, $2, $3, $4,
           $5, $6, $7, $8, $9,
           $10, $11, $12, $13::timestamptz, 'pending',
           $14::jsonb, $15, $16, statement_timestamp(),
           statement_timestamp() + ($17::integer * interval '1 millisecond'),
           $18::jsonb, statement_timestamp()
         )
         ON CONFLICT (idempotency_key) DO NOTHING
         RETURNING id`,
        [
          command.envelope.messageId,
          stringPayloadField(command.payload, "pipelineRunId") ?? command.envelope.correlationId,
          stringPayloadField(command.payload, "stageExecutionId") ?? command.envelope.messageId,
          command.envelope.route,
          route.routingKey,
          command.envelope.aggregate.type,
          candidateId,
          command.envelope.schemaVersion,
          Math.max(1, command.envelope.aggregate.version),
          command.envelope.idempotencyKey,
          command.envelope.payloadRef.uri,
          command.envelope.payloadRef.digest ?? sha256Json(command.payload),
          claim.firstSeenAt,
          JSON.stringify({
            feedId: claim.feedId,
            sourceItemId: claim.sourceItemId,
            contentFingerprint: claim.contentFingerprint
          }),
          command.envelope.messageId,
          claimToken,
          this.#leaseMs,
          publicationCommand
        ]
      );

      if ((inserted.rowCount ?? 0) > 0) {
        return {
          status: "claimed",
          command,
          claimToken
        };
      }

      const existing = await client.query<OutboxClaimRow>(
        `SELECT entity_id, status, outbox_message_id, created_at, confirmed_at,
                claim_owner_key, claim_token, claim_expires_at, publication_command,
                (claim_expires_at IS NULL OR claim_expires_at <= statement_timestamp()) AS claim_expired,
                greatest(1000, least(60000, coalesce(
                  extract(epoch FROM (claim_expires_at - statement_timestamp())) * 1000,
                  1000
                )))::integer AS retry_after_ms
         FROM ${FETCHER_POSTGRES_SCHEMA}.outbox
         WHERE idempotency_key = $1
         FOR UPDATE`,
        [command.envelope.idempotencyKey]
      );
      const row = existing.rows[0];

      if (row?.entity_id !== candidateId) {
        throw new FetcherStateContractError("Candidate outbox idempotency key resolved to an incompatible record.");
      }

      if (row.status === "confirmed") {
        return {
          status: "already-published",
          publishedAt: isoString(row.confirmed_at ?? row.created_at),
          messageId: row.outbox_message_id
        };
      }

      const storedCommand = publicationCommandFromJson(row.publication_command);
      const reclaimable = row.status === "retrying" || row.claim_expired;

      if (!reclaimable) {
        return {
          status: "in-progress",
          retryAfterMs: row.retry_after_ms
        };
      }

      await client.query(
        `UPDATE ${FETCHER_POSTGRES_SCHEMA}.outbox
         SET status = 'pending',
             claim_owner_key = $2,
             claim_token = $3,
             claim_acquired_at = statement_timestamp(),
             claim_expires_at = statement_timestamp() + ($4::integer * interval '1 millisecond'),
             sanitized_error_code = NULL,
             sanitized_error_message = NULL,
             updated_at = now()
         WHERE idempotency_key = $1`,
        [
          command.envelope.idempotencyKey,
          command.envelope.messageId,
          claimToken,
          this.#leaseMs
        ]
      );

      return {
        status: "claimed",
        command: storedCommand,
        claimToken
      };
    });
  }

  async markCandidatePublished(
    candidateId: string,
    publication: FetcherCandidatePublication
  ): Promise<void> {
    const updated = await this.#pool.query(
      `UPDATE ${FETCHER_POSTGRES_SCHEMA}.outbox
       SET status = 'confirmed',
           published_at = $4::timestamptz,
           confirmed_at = $4::timestamptz,
           claim_token = NULL,
           claim_acquired_at = NULL,
           claim_expires_at = NULL,
           updated_at = now(),
           diagnostic_metadata = diagnostic_metadata || $5::jsonb
       WHERE entity_id = $1
         AND idempotency_key = $2
         AND claim_token = $3
         AND outbox_message_id = $6
         AND status IN ('pending', 'published', 'retrying', 'confirmed')
         AND claim_expires_at > statement_timestamp()
       RETURNING id`,
      [
        candidateId,
        publication.idempotencyKey,
        publication.claimToken,
        publication.publishedAt,
        JSON.stringify({
          confirmedMessageId: publication.messageId
        }),
        publication.messageId
      ]
    );

    assertOwned(updated.rowCount, "markCandidatePublished");
  }

  async markCandidatePublishFailed(
    candidateId: string,
    failure: FetcherCandidatePublicationFailure
  ): Promise<void> {
    const updated = await this.#pool.query(
      `UPDATE ${FETCHER_POSTGRES_SCHEMA}.outbox
       SET status = 'retrying',
           claim_token = NULL,
           claim_acquired_at = NULL,
           claim_expires_at = NULL,
           sanitized_error_code = $4,
           sanitized_error_message = $5,
           updated_at = now(),
           diagnostic_metadata = diagnostic_metadata || $6::jsonb
       WHERE entity_id = $1
         AND idempotency_key = $2
         AND claim_token = $3
         AND status IN ('pending', 'published', 'retrying')
         AND claim_expires_at > statement_timestamp()
       RETURNING id`,
      [
        candidateId,
        failure.idempotencyKey,
        failure.claimToken,
        sanitizeCode(failure.reason),
        sanitizeMessage(failure.reason),
        JSON.stringify({
          lastPublishFailureAt: failure.failedAt
        })
      ]
    );

    assertOwned(updated.rowCount, "markCandidatePublishFailed");
  }

  async listPendingCandidatePublications(
    query: FetcherPendingPublicationQuery
  ): Promise<readonly FetcherPendingCandidatePublication[]> {
    const result = await this.#pool.query<PendingOutboxRow>(
      `SELECT id, entity_id, publication_command, created_at
       FROM ${FETCHER_POSTGRES_SCHEMA}.outbox
       WHERE entity_kind = 'candidate'
         AND status IN ('pending', 'published', 'retrying')
         AND publication_command IS NOT NULL
         AND (claim_expires_at IS NULL OR claim_expires_at <= statement_timestamp())
         AND created_at <= now() - ($2::integer * interval '1 second')
       ORDER BY created_at, id
       LIMIT $1`,
      [
        Math.max(1, Math.min(100, query.maxItems)),
        Math.max(0, Math.min(86_400, query.minAgeSeconds))
      ]
    );

    return result.rows.map((row) => {
      return {
        candidateId: row.entity_id,
        command: publicationCommandFromJson(row.publication_command),
        createdAt: isoString(row.created_at)
      };
    });
  }

  async claimPendingCandidatePublications(
    query: FetcherPendingPublicationQuery
  ): Promise<readonly FetcherClaimedPendingCandidatePublication[]> {
    return withTransaction(this.#pool, async (client) => {
      const result = await client.query<PendingOutboxRow>(
        `SELECT id, entity_id, publication_command, created_at
         FROM ${FETCHER_POSTGRES_SCHEMA}.outbox
         WHERE entity_kind = 'candidate'
           AND status IN ('pending', 'published', 'retrying')
           AND publication_command IS NOT NULL
           AND (claim_expires_at IS NULL OR claim_expires_at <= statement_timestamp())
           AND created_at <= now() - ($1::integer * interval '1 second')
         ORDER BY created_at, id
         LIMIT 1
         FOR UPDATE SKIP LOCKED`,
        [
          Math.max(0, Math.min(86_400, query.minAgeSeconds))
        ]
      );
      const claimed: FetcherClaimedPendingCandidatePublication[] = [];

      for (const row of result.rows) {
        const claimToken = this.nextClaimToken();

        await client.query(
          `UPDATE ${FETCHER_POSTGRES_SCHEMA}.outbox
           SET status = 'pending',
               claim_token = $2,
               claim_acquired_at = statement_timestamp(),
               claim_expires_at = statement_timestamp() + ($3::integer * interval '1 millisecond'),
               updated_at = statement_timestamp()
           WHERE id = $1`,
          [
            row.id,
            claimToken,
            this.#leaseMs
          ]
        );
        claimed.push({
          candidateId: row.entity_id,
          claimToken,
          command: publicationCommandFromJson(row.publication_command),
          createdAt: isoString(row.created_at)
        });
      }

      return claimed;
    });
  }

  async close(): Promise<void> {
    if (this.#ownsPool) {
      await this.#pool.end();
    }
  }

  private nextClaimToken(): string {
    const claimToken = this.#claimTokenFactory();

    if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{7,159}$/u.test(claimToken)) {
      throw new FetcherStateContractError("Claim token factory returned an invalid bounded token.");
    }

    return claimToken;
  }
}

interface StateContractRow extends QueryResultRow {
  readonly contract_version: number;
  readonly inbox_shape_ready: boolean;
  readonly outbox_shape_ready: boolean;
  readonly fetch_versions_shape_ready: boolean;
  readonly fetch_outcomes_shape_ready: boolean;
  readonly feed_health_shape_ready: boolean;
  readonly schema_usage_ready: boolean;
  readonly inbox_privileges_ready: boolean;
  readonly outbox_privileges_ready: boolean;
  readonly fetch_versions_privileges_ready: boolean;
  readonly fetch_outcomes_privileges_ready: boolean;
  readonly feed_health_privileges_ready: boolean;
  readonly sequence_privileges_ready: boolean;
  readonly column_contract_ready: boolean;
  readonly constraint_contract_ready: boolean;
  readonly claim_fencing_ready: boolean;
}

interface InboxClaimRow extends QueryResultRow {
  readonly status: string;
  readonly received_at: Date | string;
  readonly processed_at: Date | string | null;
  readonly claim_owner_message_id: string | null;
  readonly claim_token: string | null;
  readonly claim_expires_at: Date | string | null;
  readonly claim_expired: boolean;
}

interface FeedMetadataRow extends QueryResultRow {
  readonly feed_id: string;
  readonly source_etag: string | null;
  readonly source_last_modified: string | null;
  readonly content_fingerprint: string | null;
  readonly fetched_at: Date | string;
}

interface OutboxClaimRow extends QueryResultRow {
  readonly entity_id: string;
  readonly status: string;
  readonly outbox_message_id: string;
  readonly created_at: Date | string;
  readonly confirmed_at: Date | string | null;
  readonly claim_owner_key: string | null;
  readonly claim_token: string | null;
  readonly claim_expires_at: Date | string | null;
  readonly claim_expired: boolean;
  readonly retry_after_ms: number;
  readonly publication_command: unknown;
}

interface PendingOutboxRow extends QueryResultRow {
  readonly id: string;
  readonly entity_id: string;
  readonly publication_command: unknown;
  readonly created_at: Date | string;
}

async function recordFetchVersion(client: PoolClient, outcome: FetcherFetchOutcome): Promise<void> {
  const versionResult = await client.query<{ readonly next_version: string }>(
    `SELECT (coalesce(max(fetch_version), 0) + 1)::text AS next_version
     FROM ${FETCHER_POSTGRES_SCHEMA}.fetch_versions
     WHERE feed_url = $1`,
    [outcome.feedUrl]
  );
  const nextVersion = Number(versionResult.rows[0]?.next_version ?? "1");
  const fingerprint = outcome.contentFingerprint ?? sha256Json({
    feedId: outcome.feedId,
    fetchedAt: outcome.fetchedAt,
    fetchStatus: outcome.fetchStatus
  });
  const status = outcome.fetchStatus === "unchanged" ? "not_modified" : "fetched";

  await client.query(
    `INSERT INTO ${FETCHER_POSTGRES_SCHEMA}.fetch_versions (
       feed_url, feed_id, fetch_version, source_etag, source_last_modified,
       content_fingerprint, http_status, status, payload_ref, payload_digest,
       fetched_at, diagnostic_metadata
     ) VALUES (
       $1, $2, $3, $4, $5,
       $6, $7, $8, $9, $6,
       $10::timestamptz, $11::jsonb
     )
     ON CONFLICT (feed_url, payload_digest)
     DO UPDATE SET feed_id = EXCLUDED.feed_id,
                   source_etag = EXCLUDED.source_etag,
                   source_last_modified = EXCLUDED.source_last_modified,
                   content_fingerprint = EXCLUDED.content_fingerprint,
                   http_status = EXCLUDED.http_status,
                   status = EXCLUDED.status,
                   fetched_at = EXCLUDED.fetched_at,
                   diagnostic_metadata = EXCLUDED.diagnostic_metadata`,
    [
      outcome.feedUrl,
      outcome.feedId,
      nextVersion,
      outcome.etag ?? null,
      outcome.lastModified ?? null,
      fingerprint,
      outcome.httpStatus ?? null,
      status,
      `backend://worker-uplift/feed-fetcher/${encodeURIComponent(outcome.feedId)}/fetch-version/${String(nextVersion)}`,
      outcome.fetchedAt,
      JSON.stringify({
        bodyBytes: outcome.bodyBytes,
        itemCount: outcome.itemCount,
        durationMs: outcome.durationMs,
        fetchStatus: outcome.fetchStatus
      })
    ]
  );
}

async function recordFeedHealthProjection(client: PoolClient, outcome: FetcherFetchOutcome): Promise<void> {
  const latest = await client.query<{
    readonly projection_version: number;
    readonly consecutive_failures: number;
    readonly last_success_at: Date | string | null;
  }>(
    `SELECT projection_version, consecutive_failures, last_success_at
     FROM ${FETCHER_POSTGRES_SCHEMA}.feed_health_projections
     WHERE feed_url = $1
     ORDER BY projection_version DESC
     LIMIT 1`,
    [outcome.feedUrl]
  );
  const previous = latest.rows[0];
  const succeeded = outcome.fetchStatus === "success" || outcome.fetchStatus === "unchanged";
  const status = succeeded
    ? "healthy"
    : outcome.fetchStatus === "permanent_failure"
      ? "failed"
      : "warning";
  const consecutiveFailures = succeeded ? 0 : (previous?.consecutive_failures ?? 0) + 1;

  await client.query(
    `INSERT INTO ${FETCHER_POSTGRES_SCHEMA}.feed_health_projections (
       feed_url, status, last_success_at, last_failure_at, consecutive_failures,
       projection_version, sanitized_error_code, sanitized_error_message,
       diagnostic_metadata
     ) VALUES (
       $1, $2, $3::timestamptz, $4::timestamptz, $5,
       $6, $7, $8, $9::jsonb
     )`,
    [
      outcome.feedUrl,
      status,
      succeeded ? outcome.fetchedAt : previous?.last_success_at ?? null,
      succeeded ? null : outcome.fetchedAt,
      consecutiveFailures,
      (previous?.projection_version ?? 0) + 1,
      outcome.failure === undefined ? null : sanitizeCode(outcome.failure.code),
      outcome.failure === undefined ? null : sanitizeMessage(outcome.failure.diagnosticSample),
      JSON.stringify({
        feedId: outcome.feedId,
        fetchStatus: outcome.fetchStatus,
        httpStatus: outcome.httpStatus
      })
    ]
  );
}

async function withTransaction<T>(pool: Pool, operation: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    const value = await operation(client);
    await client.query("COMMIT");
    return value;
  } catch (error: unknown) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

function publicationCommandFromJson(value: unknown): FetcherCandidateClaim["command"] {
  if (!isRecord(value) || !isRecord(value.envelope) || !isRecord(value.payload)) {
    throw new FetcherStateContractError("Pending candidate outbox command is missing or malformed.");
  }

  const envelope = assertWorkerEnvelope(value.envelope);
  const payload = validateStagePayload(value.payload);

  if (envelope.route !== "canonicalization" || !payload.ok || payload.definition.stage !== "canonicalization") {
    throw new FetcherStateContractError("Pending candidate outbox command does not satisfy the canonicalization contract.");
  }

  return {
    envelope,
    payload: payload.value
  };
}

function assertOwned(rowCount: number | null, operation: string): void {
  if ((rowCount ?? 0) === 0) {
    throw new FetcherStateOwnershipError(operation);
  }
}

function isoString(value: Date | string): string {
  return value instanceof Date ? value.toISOString() : new Date(value).toISOString();
}

function sha256Json(value: unknown): string {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

function sanitizeCode(value: string): string {
  const code = value.trim().toLowerCase().replace(/[^a-z0-9_.:-]+/gu, "-").slice(0, 128);

  return code.length > 0 ? code : "unknown";
}

function sanitizeMessage(value: string): string {
  return value.replace(/[\r\n\t]+/gu, " ").trim().slice(0, 512);
}

function boundedText(value: string | undefined, maxLength: number): string | undefined {
  if (value === undefined) {
    return undefined;
  }

  const bounded = value.replace(/[\r\n\t]+/gu, " ").trim().slice(0, maxLength);

  return bounded.length > 0 ? bounded : undefined;
}

function boundedInteger(value: number): number {
  return Math.max(0, Math.min(2_147_483_647, Math.round(Number.isFinite(value) ? value : 0)));
}

function stringPayloadField(payload: Readonly<Record<string, unknown>>, key: string): string | undefined {
  const value = payload[key];

  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
