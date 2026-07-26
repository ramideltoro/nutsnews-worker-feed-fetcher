import {
  describe,
  expect,
  it
} from "vitest";

import { createFetcherFailClosedReconciler } from "../src/reconciliation.js";
import { ManualFetcherClock } from "../src/test-doubles.js";

describe("fetcher reconciliation", () => {
  it("reports a bounded no-op dry-run when no service-owned replay candidates exist", async () => {
    const reconciler = createFetcherFailClosedReconciler(new ManualFetcherClock());

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-20260723"
    });

    expect(report).toMatchObject({
      service: "fetcher",
      status: "dry_run",
      selectedCount: 0,
      replayedCount: 0,
      writesPerformed: false,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false
    });
    expect(report.errors).toEqual([]);
  });
});
