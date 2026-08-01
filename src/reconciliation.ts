import {
  runtimeNow,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeClock
} from "@ramideltoro/nutsnews-worker-runtime";

import type { FetcherDurableStateStore } from "./dependencies.js";

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
  readonly selectedCount: number;
  readonly replayedCount: number;
  readonly failedClosedCount: number;
  readonly skippedCount: number;
  readonly writesPerformed: boolean;
  readonly dryRun: boolean;
  readonly productionVisibilityEnabled: false;
  readonly legacyRuntimeRequired: false;
  readonly protectedApplyRequired: true;
  readonly candidates: readonly FetcherReconciliationCandidate[];
  readonly errors: readonly string[];
  readonly metrics: {
    readonly candidateCount: number;
    readonly replayedCount: number;
    readonly failedClosedCount: number;
    readonly skippedCount: number;
  };
}

export interface FetcherReconciliationCandidate {
  readonly candidateId: string;
  readonly messageId: string;
  readonly createdAt: string;
  readonly outcome: "candidate" | "replayed" | "failed-closed";
}

export interface FetcherReconciler {
  readonly name: string;
  reconcile(request: FetcherReconciliationRequest): Promise<FetcherReconciliationReport>;
}

export const FETCHER_RECONCILIATION_PATH = "/reconcile/outbox";
export const FETCHER_RECONCILIATION_CONFIRMATION = "fetcher:fail-closed:v1";

export function createFetcherOutboxReconciler(options: {
  readonly clock: RuntimeClock;
  readonly stateStore: FetcherDurableStateStore;
  readonly publish: (command: BrokerPublishCommand) => Promise<BrokerPublishReceipt>;
  readonly env?: NodeJS.ProcessEnv;
}): FetcherReconciler {
  const env = options.env ?? process.env;

  return {
    name: "fetcher-postgresql-outbox-reconciler",
    async reconcile(request): Promise<FetcherReconciliationReport> {
      const input = reconciliationInput(request, options.clock);

      if (reconciliationStopped(env)) {
        return report({
          ...input,
          status: "kill_switch_active",
          errors: [
            "fetcher reconciliation stop switch is active"
          ]
        });
      }

      if (input.mode === "apply" && request.protectedConfirmation !== FETCHER_RECONCILIATION_CONFIRMATION) {
        return report({
          ...input,
          status: "failed_closed",
          errors: [
            `protectedConfirmation must be ${FETCHER_RECONCILIATION_CONFIRMATION}`
          ]
        });
      }

      let pending: Awaited<ReturnType<FetcherDurableStateStore["listPendingCandidatePublications"]>>;

      try {
        pending = await options.stateStore.listPendingCandidatePublications({
          maxItems: input.maxItems,
          minAgeSeconds: input.minAgeSeconds
        });
      } catch (error: unknown) {
        return report({
          ...input,
          status: "failed_closed",
          failedClosedCount: 1,
          errors: [
            safeErrorName(error)
          ]
        });
      }

      if (input.mode === "dry-run") {
        return report({
          ...input,
          status: "dry_run",
          selectedCount: pending.length,
          candidates: pending.map((candidate) => ({
            candidateId: candidate.candidateId,
            messageId: candidate.command.envelope.messageId,
            createdAt: candidate.createdAt,
            outcome: "candidate"
          })),
          errors: []
        });
      }

      const candidates: FetcherReconciliationCandidate[] = [];
      const errors: string[] = [];
      let replayedCount = 0;
      let failedClosedCount = 0;

      for (const candidate of pending) {
        try {
          const receipt = await options.publish(candidate.command);

          await options.stateStore.markCandidatePublished(candidate.candidateId, {
            publishedAt: receipt.confirmedAt,
            messageId: receipt.messageId,
            idempotencyKey: candidate.command.envelope.idempotencyKey,
            claimOwnerKey: candidate.claimOwnerKey
          });
          replayedCount += 1;
          candidates.push({
            candidateId: candidate.candidateId,
            messageId: receipt.messageId,
            createdAt: candidate.createdAt,
            outcome: "replayed"
          });
        } catch (error: unknown) {
          failedClosedCount += 1;
          errors.push(safeErrorName(error));
          candidates.push({
            candidateId: candidate.candidateId,
            messageId: candidate.command.envelope.messageId,
            createdAt: candidate.createdAt,
            outcome: "failed-closed"
          });
        }
      }

      return report({
        ...input,
        status: failedClosedCount === 0 ? "applied" : "failed_closed",
        selectedCount: pending.length,
        replayedCount,
        failedClosedCount,
        writesPerformed: replayedCount > 0,
        candidates,
        errors
      });
    }
  };
}

export function createFetcherFailClosedReconciler(
  clock: RuntimeClock,
  env: NodeJS.ProcessEnv = process.env
): FetcherReconciler {
  return {
    name: "fetcher-fail-closed-reconciler",
    reconcile: (request) => {
      const input = reconciliationInput(request, clock);

      if (reconciliationStopped(env)) {
        return Promise.resolve(report({
          ...input,
          status: "kill_switch_active",
          errors: [
            "fetcher reconciliation stop switch is active"
          ]
        }));
      }

      if (input.mode === "apply" && request.protectedConfirmation !== FETCHER_RECONCILIATION_CONFIRMATION) {
        return Promise.resolve(report({
          ...input,
          status: "failed_closed",
          errors: [
            `protectedConfirmation must be ${FETCHER_RECONCILIATION_CONFIRMATION}`
          ]
        }));
      }

      return Promise.resolve(report({
        ...input,
        status: input.mode === "apply" ? "applied" : "dry_run",
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
  readonly selectedCount?: number;
  readonly replayedCount?: number;
  readonly failedClosedCount?: number;
  readonly skippedCount?: number;
  readonly writesPerformed?: boolean;
  readonly candidates?: readonly FetcherReconciliationCandidate[];
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
    selectedCount: input.selectedCount ?? 0,
    replayedCount: input.replayedCount ?? 0,
    failedClosedCount: input.failedClosedCount ?? 0,
    skippedCount: input.skippedCount ?? 0,
    writesPerformed: input.writesPerformed ?? false,
    dryRun: input.mode === "dry-run",
    productionVisibilityEnabled: false,
    legacyRuntimeRequired: false,
    protectedApplyRequired: true,
    candidates: input.candidates ?? [],
    errors: input.errors,
    metrics: {
      candidateCount: input.selectedCount ?? 0,
      replayedCount: input.replayedCount ?? 0,
      failedClosedCount: input.failedClosedCount ?? 0,
      skippedCount: input.skippedCount ?? 0
    }
  };
}

function reconciliationInput(
  request: FetcherReconciliationRequest,
  clock: RuntimeClock
): {
  readonly mode: FetcherReconciliationMode;
  readonly requestedAt: string;
  readonly runId?: string;
  readonly reason?: string;
  readonly maxItems: number;
  readonly minAgeSeconds: number;
} {
  const runId = safeRunId(request.runId);
  const reason = safeReason(request.reason);

  return {
    mode: request.mode === "apply" ? "apply" : "dry-run",
    requestedAt: runtimeNow(clock),
    ...(runId === undefined ? {} : {
      runId
    }),
    ...(reason === undefined ? {} : {
      reason
    }),
    maxItems: boundedInteger(request.maxItems, 100, 1, 100),
    minAgeSeconds: boundedInteger(request.minAgeSeconds, 900, 0, 86_400)
  };
}

function reconciliationStopped(env: NodeJS.ProcessEnv): boolean {
  return flagEnabled(env.NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_STOP)
    || flagEnabled(env.NUTSNEWS_FETCHER_RECONCILIATION_STOP);
}

function safeErrorName(error: unknown): string {
  return error instanceof Error && /^[A-Za-z][A-Za-z0-9]{0,127}$/u.test(error.name)
    ? error.name
    : "FetcherReconciliationError";
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
