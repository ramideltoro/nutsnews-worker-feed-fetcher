import {
  SYSTEM_RUNTIME_CLOCK,
  createInMemoryIdempotencyStore,
  type RuntimeClock,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure
} from "@ramideltoro/nutsnews-worker-runtime";

import type { FetcherConfig } from "./config.js";
import type {
  FetcherCandidateClaim,
  FetcherCandidateClaimResult,
  FetcherCandidatePublication,
  FetcherCandidatePublicationFailure,
  FetcherDependencyProbe,
  FetcherDurableStateStore,
  FetcherFeedMetadata,
  FetcherFetchOutcome,
  FetcherPendingCandidatePublication,
  FetcherPendingPublicationQuery,
  FetcherStateStoreMode
} from "./dependencies.js";
import {
  FetcherStateOwnershipError,
  PostgresFetcherStateStore,
  createPostgresFetcherPool,
  type PostgresFetcherStateStoreOptions
} from "./postgres-state-store.js";

export const FETCHER_PRODUCTION_STATE_STORE_MODE = "postgresql" as const;

export class FetcherStateStoreUnavailableError extends Error {
  readonly operation: string;

  constructor(operation: string) {
    super(`Production fetcher state store is unavailable for ${operation}.`);
    this.name = "FetcherStateStoreUnavailableError";
    this.operation = operation;
  }
}

export class InMemoryFetcherStateStore implements FetcherDurableStateStore {
  readonly name = "local-memory-state-store";
  readonly mode = "local-memory" as const;
  readonly adapter = "runtime-in-memory";
  readonly durable = false;
  status: FetcherDependencyProbe["status"] = "ok";
  readonly outcomes: FetcherFetchOutcome[] = [];
  private readonly feedMetadata = new Map<string, FetcherFeedMetadata>();
  private readonly candidates = new Map<string, FetcherCandidatePublication>();
  private readonly candidateClaims = new Map<string, FetcherCandidateClaim>();
  private readonly retryableCandidates = new Set<string>();
  private readonly store;
  private readonly clock: RuntimeClock;

  constructor(clock: RuntimeClock = SYSTEM_RUNTIME_CLOCK) {
    this.clock = clock;
    this.store = createInMemoryIdempotencyStore(clock);
  }

  probe(): FetcherDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local in-memory state ready" : "local in-memory state unavailable"
    };
  }

  claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
    return this.store.claim(idempotencyKey, context);
  }

  markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    return this.store.markCompleted(idempotencyKey, completion);
  }

  markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    return this.store.markFailed(idempotencyKey, failure);
  }

  getFeedMetadata(feedId: string): Promise<FetcherFeedMetadata | undefined> {
    return Promise.resolve(this.feedMetadata.get(feedId));
  }

  recordFetchOutcome(outcome: FetcherFetchOutcome): Promise<void> {
    this.outcomes.push(outcome);

    if (outcome.fetchStatus === "success" || outcome.fetchStatus === "unchanged") {
      this.feedMetadata.set(outcome.feedId, {
        feedId: outcome.feedId,
        ...(outcome.etag === undefined ? {} : {
          etag: outcome.etag
        }),
        ...(outcome.lastModified === undefined ? {} : {
          lastModified: outcome.lastModified
        }),
        ...(outcome.contentFingerprint === undefined ? {} : {
          contentFingerprint: outcome.contentFingerprint
        }),
        fetchedAt: outcome.fetchedAt
      });
    }

    return Promise.resolve();
  }

  claimCandidate(candidateId: string, claim: FetcherCandidateClaim): Promise<FetcherCandidateClaimResult> {
    const existing = this.candidates.get(candidateId);

    if (existing !== undefined) {
      return Promise.resolve({
        status: "already-published",
        publishedAt: existing.publishedAt,
        messageId: existing.messageId
      });
    }

    const pending = this.candidateClaims.get(candidateId);

    if (pending !== undefined
      && pending.claimOwnerKey !== claim.claimOwnerKey
      && !this.retryableCandidates.has(candidateId)) {
      return Promise.resolve({
        status: "in-progress",
        retryAfterMs: 1_000
      });
    }

    if (pending === undefined) {
      this.candidateClaims.set(candidateId, claim);
    } else if (pending.claimOwnerKey !== claim.claimOwnerKey) {
      this.candidateClaims.set(candidateId, {
        ...pending,
        claimOwnerKey: claim.claimOwnerKey
      });
    }
    this.retryableCandidates.delete(candidateId);

    return Promise.resolve({
      status: "claimed",
      command: pending?.command ?? claim.command
    });
  }

  markCandidatePublished(candidateId: string, publication: FetcherCandidatePublication): Promise<void> {
    const claim = this.candidateClaims.get(candidateId);

    if (!publicationOwnedByClaim(claim, publication)) {
      return Promise.reject(new FetcherStateOwnershipError("markCandidatePublished"));
    }

    this.candidates.set(candidateId, publication);
    this.retryableCandidates.delete(candidateId);
    return Promise.resolve();
  }

  markCandidatePublishFailed(candidateId: string, failure: FetcherCandidatePublicationFailure): Promise<void> {
    const claim = this.candidateClaims.get(candidateId);

    if (!failureOwnedByClaim(claim, failure)) {
      return Promise.reject(new FetcherStateOwnershipError("markCandidatePublishFailed"));
    }

    this.retryableCandidates.add(candidateId);
    return Promise.resolve();
  }

  listPendingCandidatePublications(query: FetcherPendingPublicationQuery): Promise<readonly FetcherPendingCandidatePublication[]> {
    const oldestAllowed = this.clock.now().getTime() - (Math.max(0, query.minAgeSeconds) * 1_000);

    return Promise.resolve([...this.candidateClaims.entries()]
      .filter(([candidateId, claim]) => !this.candidates.has(candidateId)
        && this.retryableCandidates.has(candidateId)
        && Date.parse(claim.firstSeenAt) <= oldestAllowed)
      .slice(0, query.maxItems)
      .map(([candidateId, claim]) => ({
        candidateId,
        claimOwnerKey: claim.claimOwnerKey,
        command: claim.command,
        createdAt: claim.firstSeenAt
      })));
  }
}

export class UnsupportedProductionFetcherStateStore implements FetcherDurableStateStore {
  readonly name = "unsupported-production-state-store";
  readonly mode = "unsupported" as const;
  readonly adapter = "none";
  readonly durable = false;

  probe(): FetcherDependencyProbe {
    return {
      status: "unhealthy",
      summary: "PostgreSQL state adapter was not configured; production consumption is disabled"
    };
  }

  claim(): Promise<RuntimeIdempotencyClaimResult> {
    return unavailable("claim");
  }

  markCompleted(): Promise<void> {
    return unavailable("markCompleted");
  }

  markFailed(): Promise<void> {
    return unavailable("markFailed");
  }

  getFeedMetadata(): Promise<FetcherFeedMetadata | undefined> {
    return unavailable("getFeedMetadata");
  }

  recordFetchOutcome(): Promise<void> {
    return unavailable("recordFetchOutcome");
  }

  claimCandidate(): Promise<FetcherCandidateClaimResult> {
    return unavailable("claimCandidate");
  }

  markCandidatePublished(): Promise<void> {
    return unavailable("markCandidatePublished");
  }

  markCandidatePublishFailed(): Promise<void> {
    return unavailable("markCandidatePublishFailed");
  }

  listPendingCandidatePublications(): Promise<readonly FetcherPendingCandidatePublication[]> {
    return unavailable("listPendingCandidatePublications");
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}

export function expectedFetcherStateStoreMode(config: FetcherConfig): FetcherStateStoreMode {
  return config.dependencyMode === "production" ? FETCHER_PRODUCTION_STATE_STORE_MODE : "local-memory";
}

export interface FetcherStateStoreFactoryOptions {
  readonly databaseUrl?: string;
  readonly postgres?: Omit<PostgresFetcherStateStoreOptions, "clock" | "leaseMs">;
}

export function createFetcherStateStore(
  config: FetcherConfig,
  clock: RuntimeClock,
  options: FetcherStateStoreFactoryOptions = {}
): FetcherDurableStateStore {
  if (config.dependencyMode === "production") {
    const databaseUrl = options.databaseUrl?.trim();

    if (databaseUrl === undefined || databaseUrl.length === 0) {
      return new UnsupportedProductionFetcherStateStore();
    }

    const pool = options.postgres?.pool ?? createPostgresFetcherPool({
      databaseUrl,
      applicationName: config.serviceName,
      maxConnections: config.database.poolMax,
      timeoutMs: config.database.timeoutMs
    });

    return new PostgresFetcherStateStore({
      pool,
      clock,
      leaseMs: config.database.idempotencyLeaseMs,
      ownsPool: options.postgres?.pool === undefined
    });
  }

  return new InMemoryFetcherStateStore(clock);
}

function unavailable<T>(operation: string): Promise<T> {
  return Promise.reject(new FetcherStateStoreUnavailableError(operation));
}

function publicationOwnedByClaim(
  claim: FetcherCandidateClaim | undefined,
  publication: FetcherCandidatePublication
): boolean {
  if (claim === undefined) {
    return false;
  }

  return claim.claimOwnerKey === publication.claimOwnerKey
    && claim.command.envelope.idempotencyKey === publication.idempotencyKey
    && claim.command.envelope.messageId === publication.messageId;
}

function failureOwnedByClaim(
  claim: FetcherCandidateClaim | undefined,
  failure: FetcherCandidatePublicationFailure
): boolean {
  if (claim === undefined) {
    return false;
  }

  return claim.claimOwnerKey === failure.claimOwnerKey
    && claim.command.envelope.idempotencyKey === failure.idempotencyKey;
}
