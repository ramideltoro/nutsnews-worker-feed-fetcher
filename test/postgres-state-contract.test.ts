import { readFile } from "node:fs/promises";

import type { Pool } from "pg";

import {
  describe,
  expect,
  it
} from "vitest";

import {
  FETCHER_FETCH_OUTCOME_RETENTION_DAYS,
  FETCHER_POSTGRES_STATE_CONTRACT_VERSION,
  PostgresFetcherStateStore
} from "../src/postgres-state-store.js";
import { ManualFetcherClock } from "../src/test-doubles.js";

describe("fetcher PostgreSQL state contract", () => {
  it("keeps outcome retention bounded and cleanup-indexed in the reference schema", async () => {
    const [baseMigration, fencingMigration] = await Promise.all([
      readFile(new URL("../migrations/001_worker_uplift_fetcher_state.sql", import.meta.url), "utf8"),
      readFile(new URL("../migrations/002_fetcher_claim_fencing.sql", import.meta.url), "utf8")
    ]);

    expect(FETCHER_POSTGRES_STATE_CONTRACT_VERSION).toBe(2);
    expect(FETCHER_FETCH_OUTCOME_RETENTION_DAYS).toBe(30);
    expect(baseMigration).toContain("redact_after timestamptz NOT NULL DEFAULT (now() + interval '30 days')");
    expect(baseMigration).toContain("worker_uplift_fetcher_fetch_outcomes_redact_idx");
    expect(baseMigration).toContain("ON worker_uplift_fetcher.fetch_outcomes (redact_after, id)");
    expect(fencingMigration).toContain("claim_token text");
    expect(fencingMigration).toContain("claim_acquired_at timestamptz");
    expect(fencingMigration).toContain("worker_uplift_fetcher_inbox_claim_token_idx");
    expect(fencingMigration).toContain("worker_uplift_fetcher_outbox_claim_token_idx");
    expect(fencingMigration).toContain("AND (claim_expires_at IS NOT NULL OR claim_acquired_at IS NOT NULL)");
    expect(fencingMigration).toContain("char_length(claim_token) >= 8");
    expect(fencingMigration).toContain("char_length(claim_token) <= 160");
    expect(fencingMigration).toContain("(claim_token COLLATE \"C\") ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{7,159}$'");
    expect(fencingMigration).toContain("claim_expires_at > claim_acquired_at");
    expect(fencingMigration).toContain("claim_expires_at <= claim_acquired_at + interval '5 minutes'");
    expect(fencingMigration).toContain("pg_get_constraintdef(oid) LIKE '%claim_expires_at <=%claim_acquired_at +%00:05:00%'");
    expect(fencingMigration).toContain("VALUES ('fetcher_state_store', 2)");
    expect(fencingMigration).toContain("WHERE worker_uplift_fetcher.state_contract.contract_version < EXCLUDED.contract_version");
  });

  it("fails readiness when any required production privilege or schema contract is absent", async () => {
    let probeSql = "";
    const readyRow = completeProbeRow();
    const readyStore = new PostgresFetcherStateStore({
      pool: queryOnlyPool((sql) => {
        probeSql = sql;
        return readyRow;
      }),
      clock: new ManualFetcherClock(),
      leaseMs: 60_000
    });

    await expect(readyStore.probe()).resolves.toMatchObject({
      status: "ok"
    });
    for (const requiredSql of [
      "claim_owner_message_id",
      "claim_token",
      "claim_acquired_at",
      "publication_command",
      "has_schema_privilege",
      "has_table_privilege",
      "inbox_id_seq",
      "outbox_id_seq",
      "fetch_versions_id_seq",
      "fetch_outcomes_id_seq",
      "feed_health_projections_id_seq",
      "constraint_row.contype = 'u'",
      "jsonb_typeof",
      "index_row.indisunique",
      "index_row.indisvalid",
      "index_row.indisready",
      "attribute_row.attname = 'claim_token'",
      "pg_get_expr(index_row.indpred, index_row.indrelid)",
      "char_length(claim_token)",
      "COLLATE \"C\"",
      "{7,159}",
      "claim_expires_at > claim_acquired_at",
      "claim_expires_at <=%claim_acquired_at +%00:05:00",
      "claim_fencing_ready"
    ]) {
      expect(probeSql).toContain(requiredSql);
    }

    const driftedStore = new PostgresFetcherStateStore({
      pool: queryOnlyPool(() => ({
        ...readyRow,
        outbox_privileges_ready: false
      })),
      clock: new ManualFetcherClock(),
      leaseMs: 60_000
    });

    await expect(driftedStore.probe()).resolves.toMatchObject({
      status: "unhealthy",
      summary: "backend PostgreSQL fetcher state contract is missing or incomplete"
    });
  });
});

function completeProbeRow(): Readonly<Record<string, boolean | number>> {
  return {
    contract_version: 2,
    inbox_shape_ready: true,
    outbox_shape_ready: true,
    fetch_versions_shape_ready: true,
    fetch_outcomes_shape_ready: true,
    feed_health_shape_ready: true,
    schema_usage_ready: true,
    inbox_privileges_ready: true,
    outbox_privileges_ready: true,
    fetch_versions_privileges_ready: true,
    fetch_outcomes_privileges_ready: true,
    feed_health_privileges_ready: true,
    sequence_privileges_ready: true,
    column_contract_ready: true,
    constraint_contract_ready: true,
    claim_fencing_ready: true
  };
}

function queryOnlyPool(
  row: (sql: string) => Readonly<Record<string, boolean | number>>
): Pool {
  return {
    query: (sql: string) => Promise.resolve({
      rows: [row(sql)]
    })
  } as unknown as Pool;
}
