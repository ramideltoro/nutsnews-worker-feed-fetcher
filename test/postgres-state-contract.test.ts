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
    const migration = await readFile(
      new URL("../migrations/001_worker_uplift_fetcher_state.sql", import.meta.url),
      "utf8"
    );

    expect(FETCHER_POSTGRES_STATE_CONTRACT_VERSION).toBe(1);
    expect(FETCHER_FETCH_OUTCOME_RETENTION_DAYS).toBe(30);
    expect(migration).toContain("redact_after timestamptz NOT NULL DEFAULT (now() + interval '30 days')");
    expect(migration).toContain("worker_uplift_fetcher_fetch_outcomes_redact_idx");
    expect(migration).toContain("ON worker_uplift_fetcher.fetch_outcomes (redact_after, id)");
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
      "publication_command",
      "has_schema_privilege",
      "has_table_privilege",
      "inbox_id_seq",
      "outbox_id_seq",
      "fetch_versions_id_seq",
      "fetch_outcomes_id_seq",
      "feed_health_projections_id_seq",
      "constraint_row.contype = 'u'",
      "jsonb_typeof"
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
    contract_version: 1,
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
    constraint_contract_ready: true
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
