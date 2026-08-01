import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadFetcherConfig } from "../src/config.js";
import {
  createFetcherApplication
} from "../src/index.js";
import type {
  FetcherDependencyProbe,
  FetcherDurableStateStore
} from "../src/dependencies.js";
import { InMemoryFetcherStateStore } from "../src/state-store.js";
import { LocalBrokerTransport } from "../src/test-doubles.js";

describe("fetcher application lifecycle", () => {
  it("binds diagnostics before a blocked state-store startup probe settles", async () => {
    const config = loadFetcherConfig({
      NUTSNEWS_FETCHER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_FETCHER_HTTP_PORT: "0",
      NUTSNEWS_FETCHER_STARTUP_TIMEOUT_MS: "10000",
      NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
    });
    const probeGate = deferredSignal();
    const stateStore = createGatedStateStore(probeGate.promise);
    const application = createFetcherApplication(config, {
      stateStore
    });
    const startup = application.start();
    const liveUrl = await waitForUrl(application, "/live");

    expect((await fetch(liveUrl)).status).toBe(200);
    expect((await fetch(application.url("/startupz"))).status).toBe(503);
    expect((await fetch(application.url("/metrics"))).status).toBe(200);

    probeGate.resolve();
    await startup;
    expect((await fetch(application.url("/readyz"))).status).toBe(200);

    await application.stop();
  });

  it("closes durable state even when bounded service shutdown reports a consumer-cancel timeout", async () => {
    vi.useFakeTimers();

    try {
      const config = loadFetcherConfig({
        NUTSNEWS_FETCHER_HTTP_HOST: "127.0.0.1",
        NUTSNEWS_FETCHER_HTTP_PORT: "0",
        NUTSNEWS_FETCHER_SHUTDOWN_TIMEOUT_MS: "1000",
        NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
      });
      const stateStore = new InMemoryFetcherStateStore() as InMemoryFetcherStateStore & {
        close(): Promise<void>;
      };
      const closeStateStore = vi.fn(() => Promise.resolve());
      const broker = new LocalBrokerTransport();

      stateStore.close = closeStateStore;
      const application = createFetcherApplication(config, {
        stateStore,
        brokerTransport: broker
      });

      await application.start();
      broker.cancelGate = new Promise<void>(() => undefined);
      const stopping = application.stop();
      const stopOutcome = stopping.then(
        () => undefined,
        (reason: unknown) => reason
      );

      await vi.advanceTimersByTimeAsync(1_000);
      const error = await stopOutcome;

      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toContain("broker-consumer-cancel exceeded 200ms");
      expect(closeStateStore).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("cancels broker intake while an active readiness request is still probing state", async () => {
    const config = loadFetcherConfig({
      NUTSNEWS_FETCHER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_FETCHER_HTTP_PORT: "0",
      NUTSNEWS_FETCHER_STARTUP_TIMEOUT_MS: "1000",
      NUTSNEWS_FETCHER_SHUTDOWN_TIMEOUT_MS: "1000",
      NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
    });
    const delegate = new InMemoryFetcherStateStore();
    const readinessEntered = deferredSignal();
    const readinessGate = deferredSignal();
    let blockReadiness = false;
    const stateStore: FetcherDurableStateStore = {
      name: "shutdown-order-state-store",
      mode: "local-memory",
      adapter: "runtime-in-memory",
      durable: false,
      probe: async (): Promise<FetcherDependencyProbe> => {
        if (blockReadiness) {
          readinessEntered.resolve();
          await readinessGate.promise;
        }

        return delegate.probe();
      },
      claim: delegate.claim.bind(delegate),
      markCompleted: delegate.markCompleted.bind(delegate),
      markFailed: delegate.markFailed.bind(delegate),
      releaseClaim: delegate.releaseClaim.bind(delegate),
      getFeedMetadata: delegate.getFeedMetadata.bind(delegate),
      recordFetchOutcome: delegate.recordFetchOutcome.bind(delegate),
      claimCandidate: delegate.claimCandidate.bind(delegate),
      markCandidatePublished: delegate.markCandidatePublished.bind(delegate),
      markCandidatePublishFailed: delegate.markCandidatePublishFailed.bind(delegate),
      listPendingCandidatePublications: delegate.listPendingCandidatePublications.bind(delegate),
      claimPendingCandidatePublications: delegate.claimPendingCandidatePublications.bind(delegate)
    };
    const broker = new LocalBrokerTransport();
    const application = createFetcherApplication(config, {
      stateStore,
      brokerTransport: broker
    });

    await application.start();
    blockReadiness = true;
    const readinessRequest = fetch(application.url("/readyz"), {
      headers: {
        connection: "close"
      }
    });

    await readinessEntered.promise;
    const stopping = application.stop();

    await waitForBrokerCancel(broker);
    expect(broker.cancelCalls).toBe(1);

    readinessGate.resolve();
    const readinessResponse = await readinessRequest;

    expect([
      200,
      503
    ]).toContain(readinessResponse.status);
    await expect(stopping).resolves.toBeUndefined();
  });
});

function createGatedStateStore(gate: Promise<void>): FetcherDurableStateStore {
  const delegate = new InMemoryFetcherStateStore();

  return {
    name: "gated-local-state-store",
    mode: "local-memory",
    adapter: "runtime-in-memory",
    durable: false,
    probe: async (): Promise<FetcherDependencyProbe> => {
      await gate;
      return delegate.probe();
    },
    claim: delegate.claim.bind(delegate),
    markCompleted: delegate.markCompleted.bind(delegate),
    markFailed: delegate.markFailed.bind(delegate),
    releaseClaim: delegate.releaseClaim.bind(delegate),
    getFeedMetadata: delegate.getFeedMetadata.bind(delegate),
    recordFetchOutcome: delegate.recordFetchOutcome.bind(delegate),
    claimCandidate: delegate.claimCandidate.bind(delegate),
    markCandidatePublished: delegate.markCandidatePublished.bind(delegate),
    markCandidatePublishFailed: delegate.markCandidatePublishFailed.bind(delegate),
    listPendingCandidatePublications: delegate.listPendingCandidatePublications.bind(delegate),
    claimPendingCandidatePublications: delegate.claimPendingCandidatePublications.bind(delegate)
  };
}

async function waitForUrl(
  application: ReturnType<typeof createFetcherApplication>,
  path: string
): Promise<string> {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      return application.url(path);
    } catch {
      await new Promise<void>((resolve) => {
        setImmediate(resolve);
      });
    }
  }

  throw new Error("fetcher HTTP server did not bind in time");
}

async function waitForBrokerCancel(broker: LocalBrokerTransport): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (broker.cancelCalls > 0) {
      return;
    }

    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
  }

  throw new Error("fetcher broker intake was not cancelled in time");
}

function deferredSignal() {
  let resolve!: () => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<void>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {
    promise,
    resolve,
    reject
  };
}
