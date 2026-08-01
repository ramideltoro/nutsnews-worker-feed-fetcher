import {
  describe,
  expect,
  it
} from "vitest";

import { loadFetcherConfig } from "../src/config.js";
import {
  FETCHER_DURATION_HISTOGRAM_BUCKETS_SECONDS,
  FETCHER_METRIC_DEPENDENCIES,
  FETCHER_METRIC_LABELS,
  FETCHER_STAGE_LATENCY_BUCKETS_SECONDS,
  FETCHER_STAGE_METRIC_OUTCOMES,
  createFetcherPrometheusMetricsSink
} from "../src/metrics.js";
import { InMemoryFetcherStateStore } from "../src/state-store.js";
import { ManualFetcherClock } from "../src/test-doubles.js";

describe("fetcher Prometheus metrics", () => {
  it("exports the fixed canonical stage family at zero before traffic without changing cardinality", async () => {
    const clock = new ManualFetcherClock();
    const config = loadFetcherConfig({
      HOSTNAME: "fetcher-host"
    });
    const metrics = createFetcherPrometheusMetricsSink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      },
      config,
      stateStore: new InMemoryFetcherStateStore(clock)
    });
    const initialOutput = metrics.collect();
    const initialSeries = canonicalStageSeriesKeys(initialOutput);

    expect(initialSeries).toHaveLength(22);

    for (const outcome of FETCHER_STAGE_METRIC_OUTCOMES) {
      expect(initialOutput).toContain(`nutsnews_worker_uplift_stage_events_total{environment="local",service="fetch",outcome="${outcome}"} 0`);
    }

    for (const boundary of FETCHER_STAGE_LATENCY_BUCKETS_SECONDS) {
      expect(initialOutput).toContain(`nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="fetch",le="${String(boundary)}"} 0`);
    }

    expect(initialOutput).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="fetch",le="+Inf"} 0');
    expect(initialOutput).toContain('nutsnews_worker_uplift_stage_latency_seconds_sum{environment="local",service="fetch"} 0');
    expect(initialOutput).toContain('nutsnews_worker_uplift_stage_latency_seconds_count{environment="local",service="fetch"} 0');

    await metrics.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-07-23T00:05:00.000Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      outcome: "success",
      durationMs: 5
    });

    expect(canonicalStageSeriesKeys(metrics.collect())).toEqual(initialSeries);
  });

  it("exports bounded build, mode, readiness, ownership, and last-success signals", async () => {
    const clock = new ManualFetcherClock();
    const config = loadFetcherConfig({
      HOSTNAME: "fetcher-host",
      NUTSNEWS_FETCHER_BUILD_REVISION: "501ededcad48924b632b0547679f4dcb54ed4a90"
    });
    const stateStore = new InMemoryFetcherStateStore(clock);
    const metrics = createFetcherPrometheusMetricsSink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      },
      config,
      stateStore
    });

    metrics.setStateStoreReady(true);
    await metrics.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-07-23T00:05:00.000Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      outcome: "success"
    });
    await metrics.emit({
      name: "runtime.message.duplicate",
      level: "info",
      at: "2026-07-23T00:06:00.000Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      outcome: "duplicate"
    });
    await metrics.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-07-23T00:04:00.000Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      outcome: "success"
    });

    const output = metrics.collect();

    expect(output).toContain('nutsnews_worker_build_info{environment="local",revision="501ededcad48924b632b0547679f4dcb54ed4a90",service="nutsnews-worker-feed-fetcher",version="0.1.0"} 1');
    expect(output).toContain('nutsnews_worker_deployment_info{adapter="in_memory",deployment="shadow",environment="local",service="nutsnews-worker-feed-fetcher"} 1');
    expect(output).toContain('nutsnews_worker_expected_active{environment="local",service="nutsnews-worker-feed-fetcher"} 0');
    expect(output).toMatch(/nutsnews_worker_state_store_ready\{[^\n]+outcome="local-memory"[^\n]+\} 1/u);
    expect(output).toMatch(new RegExp(`nutsnews_worker_last_success_timestamp_seconds\\{[^\\n]+\\} ${String(Date.parse("2026-07-23T00:06:00.000Z") / 1_000)}`, "u"));
    expect(output).toContain('nutsnews_worker_health_probe{environment="local",service="fetch",probe="liveness",outcome="ok"} 1');
    expect(output).toContain('nutsnews_worker_health_probe{environment="local",service="fetch",probe="startup",outcome="unhealthy"} 1');
    expect(output).toContain('nutsnews_worker_health_probe{environment="local",service="fetch",probe="readiness",outcome="unhealthy"} 1');
    expect(output).toContain('nutsnews_worker_metrics_enabled{environment="local",service="nutsnews-worker-feed-fetcher"} 1');
    expect(output).toContain('nutsnews_worker_telemetry_collection_ready{environment="local",service="nutsnews-worker-feed-fetcher"} 1');

    const customSeries = output.split("\n").filter((line) =>
      /^(nutsnews_worker_build_info|nutsnews_worker_deployment_info|nutsnews_worker_expected_active|nutsnews_worker_state_store_ready|nutsnews_worker_last_success_timestamp_seconds|nutsnews_worker_health_probe|nutsnews_worker_metrics_enabled|nutsnews_worker_telemetry_collection_ready)\{/u.test(line)
    );

    expect(customSeries).toHaveLength(16);

    expect(metrics.allowedLabels).toEqual(FETCHER_METRIC_LABELS);
    const allowedLabels = new Set<string>(metrics.allowedLabels);

    for (const line of output.split("\n").filter((entry) => entry.startsWith("nutsnews_worker_") && entry.includes("{"))) {
      const labels = Array.from(line.matchAll(/([a-z_]+)="/gu), (match) => match[1]);

      expect(labels.every((label) => label !== undefined && allowedLabels.has(label))).toBe(true);
    }

    expect(output).not.toMatch(/feed_id|message_id|idempotency|correlation|trace|url=/u);
  });

  it("replaces Runtime 0.5 summaries with fixed-bucket seconds histograms", async () => {
    const clock = new ManualFetcherClock();
    const config = loadFetcherConfig({
      HOSTNAME: "fetcher-host"
    });
    const metrics = createFetcherPrometheusMetricsSink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      },
      config,
      stateStore: new InMemoryFetcherStateStore(clock)
    });

    await metrics.emit({
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-07-23T00:00:00.000Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      durationMs: 37,
      outcome: "success",
      attributes: {
        dependency: "feed-fetch"
      }
    });

    const output = metrics.collect();

    expect(FETCHER_DURATION_HISTOGRAM_BUCKETS_SECONDS).toContain(0.025);
    expect(output).toContain('# TYPE nutsnews_worker_dependency_duration_seconds histogram');
    expect(output).toMatch(/nutsnews_worker_dependency_duration_seconds_bucket\{[^\n]+outcome="success",dependency="feed-fetch",le="0.025"\} 0/u);
    expect(output).toMatch(/nutsnews_worker_dependency_duration_seconds_bucket\{[^\n]+outcome="success",dependency="feed-fetch",le="0.05"\} 1/u);
    expect(output).toMatch(/nutsnews_worker_dependency_duration_seconds_bucket\{[^\n]+outcome="success",dependency="feed-fetch",le="\+Inf"\} 1/u);
    expect(output).toMatch(/nutsnews_worker_dependency_duration_seconds_sum\{[^\n]+outcome="success",dependency="feed-fetch"\} 0\.037/u);
    expect(output).toMatch(/nutsnews_worker_dependency_duration_seconds_count\{[^\n]+outcome="success",dependency="feed-fetch"\} 1/u);
    expect(output).not.toContain("_duration_ms");
    expect(output).not.toMatch(/^# TYPE .* summary$/mu);
  });

  it("does not manufacture a zero-latency sample for a duration-less dependency event", async () => {
    const clock = new ManualFetcherClock();
    const config = loadFetcherConfig({
      HOSTNAME: "fetcher-host"
    });
    const metrics = createFetcherPrometheusMetricsSink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      },
      config,
      stateStore: new InMemoryFetcherStateStore(clock)
    });

    await metrics.emit({
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-07-23T00:00:00.000Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      outcome: "success",
      attributes: {
        dependency: "fetcher-shell"
      }
    });

    const output = metrics.collect();

    expect(output).not.toContain("nutsnews_worker_dependency_duration_seconds");
    expect(output).not.toContain("_duration_ms");
  });

  it("folds arbitrary dependency names into one finite other series", async () => {
    const clock = new ManualFetcherClock();
    const config = loadFetcherConfig({
      HOSTNAME: "fetcher-host"
    });
    const metrics = createFetcherPrometheusMetricsSink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      },
      config,
      stateStore: new InMemoryFetcherStateStore(clock)
    });

    for (const dependency of ["feed-123", "https://private.example.test/path"]) {
      await metrics.emit({
        name: "runtime.dependency.observed",
        level: "warn",
        at: "2026-07-23T00:00:00.000Z",
        stage: "fetch",
        queue: "nutsnews.worker.fetch.v1",
        durationMs: 10,
        outcome: "failure",
        attributes: {
          dependency
        }
      });
    }

    const output = metrics.collect();
    const dependencyCounts = output.split("\n").filter((line) =>
      line.startsWith("nutsnews_worker_dependency_duration_seconds_count{")
    );

    expect(FETCHER_METRIC_DEPENDENCIES).toEqual([
      "feed-fetch",
      "broker-settlement",
      "other"
    ]);
    expect(dependencyCounts).toHaveLength(1);
    expect(dependencyCounts[0]).toContain('dependency="other"');
    expect(dependencyCounts[0]).toContain('outcome="failure"');
    expect(dependencyCounts[0]?.endsWith(" 2")).toBe(true);
    expect(output).not.toContain("feed-123");
    expect(output).not.toContain("private.example.test");
  });

  it("keeps one seeded health family while consuming health events locally", async () => {
    const clock = new ManualFetcherClock();
    const config = loadFetcherConfig({
      HOSTNAME: "fetcher-host"
    });
    const metrics = createFetcherPrometheusMetricsSink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      },
      config,
      stateStore: new InMemoryFetcherStateStore(clock)
    });

    await metrics.emit({
      name: "runtime.health.evaluated",
      level: "info",
      at: "2026-07-23T00:00:00.000Z",
      stage: "fetch",
      outcome: "ok",
      attributes: {
        probe: "readiness"
      }
    });

    const output = metrics.collect();

    expect(output.split("\n").filter((line) => line === "# HELP nutsnews_worker_health_probe Worker liveness, startup, and readiness state by bounded probe and outcome.")).toHaveLength(1);
    expect(output.split("\n").filter((line) => line.startsWith("nutsnews_worker_health_probe{"))).toHaveLength(9);
    expect(output).toContain('nutsnews_worker_health_probe{environment="local",service="fetch",probe="readiness",outcome="ok"} 1');
    expect(output).not.toContain("nutsnews_worker_health{");
  });

  it("exports one bounded canonical outcome and one fixed-bucket latency observation per completion event", async () => {
    const clock = new ManualFetcherClock();
    const config = loadFetcherConfig({
      HOSTNAME: "fetcher-host"
    });
    const metrics = createFetcherPrometheusMetricsSink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      },
      config,
      stateStore: new InMemoryFetcherStateStore(clock)
    });
    const completions = [
      ["runtime.message.accepted", "success", 5],
      ["runtime.message.duplicate", "duplicate", 50],
      ["runtime.message.invalid", "failure", 250],
      ["runtime.message.retry", "retry", 10_000],
      ["runtime.message.dlq", "dlq", 30_001],
      ["runtime.message.accepted", "failure", 40_000]
    ] as const;

    for (const [name, outcome, durationMs] of completions) {
      await metrics.emit({
        name,
        level: outcome === "success" || outcome === "duplicate" ? "info" : "warn",
        at: "2026-07-23T00:10:00.000Z",
        stage: "fetch",
        queue: "nutsnews.worker.fetch.v1",
        outcome,
        durationMs
      });
    }

    await metrics.emit({
      name: "runtime.message.dlq",
      level: "error",
      at: "2026-07-23T00:11:00.000Z",
      stage: "fetch",
      queue: "nutsnews.worker.fetch.v1",
      outcome: "dlq"
    });
    await metrics.emit({
      name: "runtime.message.accepted",
      level: "info",
      at: "2026-07-23T00:12:00.000Z",
      stage: "fetch",
      queue: "wrong.queue",
      outcome: "success",
      durationMs: 1
    });

    const output = metrics.collect();

    expect(FETCHER_STAGE_METRIC_OUTCOMES).toEqual([
      "success",
      "duplicate",
      "invalid",
      "retry",
      "dlq",
      "failure"
    ]);
    expect(FETCHER_STAGE_LATENCY_BUCKETS_SECONDS).toContain(30);
    expect(output).toContain('nutsnews_worker_uplift_stage_events_total{environment="local",service="fetch",outcome="success"} 1');
    expect(output).toContain('nutsnews_worker_uplift_stage_events_total{environment="local",service="fetch",outcome="duplicate"} 1');
    expect(output).toContain('nutsnews_worker_uplift_stage_events_total{environment="local",service="fetch",outcome="invalid"} 1');
    expect(output).toContain('nutsnews_worker_uplift_stage_events_total{environment="local",service="fetch",outcome="retry"} 1');
    expect(output).toContain('nutsnews_worker_uplift_stage_events_total{environment="local",service="fetch",outcome="dlq"} 2');
    expect(output).toContain('nutsnews_worker_uplift_stage_events_total{environment="local",service="fetch",outcome="failure"} 1');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="fetch",le="0.01"} 1');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="fetch",le="0.05"} 2');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="fetch",le="0.25"} 3');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="fetch",le="10"} 4');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="fetch",le="30"} 4');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="fetch",le="60"} 6');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="fetch",le="+Inf"} 6');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_sum{environment="local",service="fetch"} 80.306');
    expect(output).toContain('nutsnews_worker_uplift_stage_latency_seconds_count{environment="local",service="fetch"} 6');
    expect(output).toContain('# TYPE nutsnews_worker_processing_duration_seconds histogram');
    expect(output).toMatch(/nutsnews_worker_processing_duration_seconds_count\{[^\n]+outcome="success"\} 1/u);
    expect(output).toMatch(/nutsnews_worker_processing_duration_seconds_sum\{[^\n]+outcome="dlq"\} 30\.001/u);
    expect(output).toMatch(/nutsnews_worker_processing_duration_seconds_sum\{[^\n]+outcome="failure"\} 40/u);
    expect(output).not.toContain("_duration_ms");
    expect(output).not.toMatch(/^# TYPE .* summary$/mu);

    const lifecycleSeries = output.split("\n").filter((line) =>
      line.startsWith("nutsnews_worker_uplift_stage_events_total{")
    );

    expect(lifecycleSeries).toHaveLength(6);
  });
});

function canonicalStageSeriesKeys(output: string): readonly string[] {
  return output
    .split("\n")
    .filter((line) => line.startsWith("nutsnews_worker_uplift_stage_events_total{")
      || line.startsWith("nutsnews_worker_uplift_stage_latency_seconds_"))
    .map((line) => line.slice(0, line.lastIndexOf(" ")))
    .sort();
}
