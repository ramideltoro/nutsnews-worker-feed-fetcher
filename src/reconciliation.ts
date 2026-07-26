import {
  runtimeNow,
  type RuntimeClock
} from "@ramideltoro/nutsnews-worker-runtime";

export type FetcherReconciliationMode = "dry-run" | "apply";
export type FetcherReconciliationStatus = "dry_run" | "applied" | "failed_closed" | "not_configured" | "unauthorized" | "kill_switch_active";

export interface FetcherReconciliationRequest {
  readonly mode: FetcherReconciliationMode;
  readonly runId?: string;
  readonly reason?: string;
  readonly maxItems?: number;
  readonly minAgeSeconds?: number;
  readonly protectedConfirmation?: string;
}

export interface FetcherReconciliationReport {
  readonly service: "fetcher";
  readonly mode: FetcherReconciliationMode;
  readonly status: FetcherReconciliationStatus;
  readonly requestedAt: string;
  readonly runId?: string;
  readonly reason?: string;
  readonly maxItems: number;
  readonly minAgeSeconds: number;
  readonly selectedCount: 0;
  readonly replayedCount: 0;
  readonly failedClosedCount: 0;
  readonly skippedCount: 0;
  readonly writesPerformed: false;
  readonly dryRun: boolean;
  readonly productionVisibilityEnabled: false;
  readonly legacyRuntimeRequired: false;
  readonly protectedApplyRequired: true;
  readonly candidates: readonly [];
  readonly errors: readonly string[];
  readonly metrics: {
    readonly candidateCount: 0;
    readonly replayedCount: 0;
    readonly failedClosedCount: 0;
    readonly skippedCount: 0;
  };
}

export interface FetcherReconciler {
  readonly name: string;
  reconcile(request: FetcherReconciliationRequest): Promise<FetcherReconciliationReport>;
}

export const FETCHER_RECONCILIATION_PATH = "/reconcile/outbox";
export const FETCHER_RECONCILIATION_CONFIRMATION = "fetcher:fail-closed:v1";

export function createFetcherFailClosedReconciler(
  clock: RuntimeClock,
  env: NodeJS.ProcessEnv = process.env
): FetcherReconciler {
  return {
    name: "fetcher-fail-closed-reconciler",
    reconcile: (request) => {
      const mode = request.mode === "apply" ? "apply" : "dry-run";
      const requestedAt = runtimeNow(clock);
      const maxItems = boundedInteger(request.maxItems, 100, 1, 100);
      const minAgeSeconds = boundedInteger(request.minAgeSeconds, 900, 0, 86_400);
      const runId = safeRunId(request.runId);
      const reason = safeReason(request.reason);

      if (flagEnabled(env.NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_STOP) || flagEnabled(env.NUTSNEWS_FETCHER_RECONCILIATION_STOP)) {
        return Promise.resolve(report({
          mode,
          requestedAt,
          runId,
          reason,
          maxItems,
          minAgeSeconds,
          status: "kill_switch_active",
          errors: [
            "fetcher reconciliation stop switch is active"
          ]
        }));
      }

      if (mode === "apply" && request.protectedConfirmation !== FETCHER_RECONCILIATION_CONFIRMATION) {
        return Promise.resolve(report({
          mode,
          requestedAt,
          runId,
          reason,
          maxItems,
          minAgeSeconds,
          status: "failed_closed",
          errors: [
            `protectedConfirmation must be ${FETCHER_RECONCILIATION_CONFIRMATION}`
          ]
        }));
      }

      return Promise.resolve(report({
        mode,
        requestedAt,
        runId,
        reason,
        maxItems,
        minAgeSeconds,
        status: mode === "apply" ? "applied" : "dry_run",
        errors: []
      }));
    }
  };
}

function report(input: {
  readonly mode: FetcherReconciliationMode;
  readonly requestedAt: string;
  readonly runId?: string | undefined;
  readonly reason?: string | undefined;
  readonly maxItems: number;
  readonly minAgeSeconds: number;
  readonly status: FetcherReconciliationStatus;
  readonly errors: readonly string[];
}): FetcherReconciliationReport {
  return {
    service: "fetcher",
    mode: input.mode,
    status: input.status,
    requestedAt: input.requestedAt,
    ...(input.runId === undefined ? {} : {
      runId: input.runId
    }),
    ...(input.reason === undefined ? {} : {
      reason: input.reason
    }),
    maxItems: input.maxItems,
    minAgeSeconds: input.minAgeSeconds,
    selectedCount: 0,
    replayedCount: 0,
    failedClosedCount: 0,
    skippedCount: 0,
    writesPerformed: false,
    dryRun: input.mode === "dry-run",
    productionVisibilityEnabled: false,
    legacyRuntimeRequired: false,
    protectedApplyRequired: true,
    candidates: [],
    errors: input.errors,
    metrics: {
      candidateCount: 0,
      replayedCount: 0,
      failedClosedCount: 0,
      skippedCount: 0
    }
  };
}

function boundedInteger(value: unknown, defaultValue: number, min: number, max: number): number {
  if (typeof value !== "number" || !Number.isInteger(value)) {
    return defaultValue;
  }

  return Math.max(min, Math.min(max, value));
}

function safeRunId(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();

  return /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/u.test(trimmed) ? trimmed : undefined;
}

function safeReason(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.replace(/[\r\n\t]+/gu, " ").trim();

  return trimmed.length === 0 ? undefined : trimmed.slice(0, 160);
}

function flagEnabled(value: string | undefined): boolean {
  return value?.trim().toLowerCase() === "true";
}
