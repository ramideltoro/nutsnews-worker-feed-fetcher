import {
  describe,
  expect,
  it
} from "vitest";

import { loadFetcherConfig } from "../src/config.js";
import { createFetcherPrometheusMetricsSink } from "../src/metrics.js";
import { InMemoryFetcherStateStore } from "../src/state-store.js";
import {
  bestEffortFetcherMetricsSink,
  combineBestEffortTelemetrySinks
} from "../src/telemetry-safety.js";

describe("fetcher telemetry safety", () => {
  it("isolates configured sinks so one rejection does not starve another", async () => {
    let observed = 0;
    const telemetry = combineBestEffortTelemetrySinks(
      {
        emit: () => Promise.reject(new Error("log unavailable"))
      },
      {
        emit: () => {
          observed += 1;
        }
      }
    );

    await expect(telemetry?.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-07-23T00:00:00.000Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      outcome: "success"
    })).resolves.toBeUndefined();
    expect(observed).toBe(1);
  });

  it("does not expose the removed direct dependency-latency mutation path", () => {
    const config = loadFetcherConfig();
    const sink = createFetcherPrometheusMetricsSink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      },
      config,
      stateStore: new InMemoryFetcherStateStore()
    });
    const wrapped = bestEffortFetcherMetricsSink(sink);

    expect("recordDependencyLatency" in sink).toBe(false);
    expect("recordDependencyLatency" in wrapped).toBe(false);
  });

  it("returns an explicit collection failure gauge when the underlying collector throws", () => {
    const config = loadFetcherConfig();
    const sink = createFetcherPrometheusMetricsSink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      },
      config,
      stateStore: new InMemoryFetcherStateStore()
    });
    const wrapped = bestEffortFetcherMetricsSink({
      ...sink,
      collect: () => {
        throw new Error("collector unavailable");
      }
    });
    const output = wrapped.collect();

    expect(output).not.toBe("");
    expect(output).toContain('nutsnews_worker_metrics_enabled{environment="local",service="nutsnews-worker-feed-fetcher"} 1');
    expect(output).toContain('nutsnews_worker_telemetry_collection_ready{environment="local",service="nutsnews-worker-feed-fetcher"} 0');
  });
});
