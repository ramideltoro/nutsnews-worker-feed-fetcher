import { getWorkerRoute } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createPrometheusRuntimeTelemetrySink,
  type PrometheusRuntimeTelemetrySinkOptions,
  type RuntimeServiceIdentity,
  type RuntimeTelemetryEvent,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { FetcherConfig } from "./config.js";
import type { FetcherDurableStateStore } from "./dependencies.js";

export const FETCHER_METRIC_LABELS = [
  "environment",
  "host",
  "service",
  "version",
  "revision",
  "deployment",
  "adapter",
  "stage",
  "queue",
  "outcome",
  "dependency",
  "probe",
  "check",
  "le"
] as const;

export const FETCHER_METRIC_DEPENDENCIES = [
  "feed-fetch",
  "broker-settlement",
  "state-store"
] as const;

export const FETCHER_HEALTH_CHECKS = [
  "process",
  "service-started",
  "not-stopping",
  "broker-lifecycle",
  "rabbitmq-consumer",
  "http-client",
  "dns-policy",
  "production-adapters",
  "durable-state"
] as const;

export const FETCHER_STAGE_LATENCY_BUCKETS_SECONDS = [
  0.005,
  0.01,
  0.025,
  0.05,
  0.1,
  0.25,
  0.5,
  1,
  2.5,
  5,
  10,
  30,
  60,
  120,
  300
] as const;

export const FETCHER_STAGE_METRIC_OUTCOMES = [
  "success",
  "duplicate",
  "invalid",
  "retry",
  "dlq",
  "failure"
] as const;

export type FetcherStageMetricOutcome = (typeof FETCHER_STAGE_METRIC_OUTCOMES)[number];
export type FetcherMetricAdapter = "in_memory" | "mixed" | "production" | "unknown";

export interface FetcherBaseMetricsSink extends RuntimeTelemetrySink {
  readonly allowedLabels: readonly string[];
  collect(): string;
  collectStatus(collectionReady: boolean): string;
  setInFlight(queue: string, value: number): void;
  setShutdownDraining(draining: boolean): void;
}

export interface FetcherPrometheusMetricsSink extends FetcherBaseMetricsSink {
  readonly allowedLabels: typeof FETCHER_METRIC_LABELS;
  setStateStoreReady(ready: boolean): void;
}

export interface FetcherMetricIdentity extends RuntimeServiceIdentity {
  readonly revision?: string;
  readonly deployment?: FetcherConfig["deploymentMode"];
  readonly adapter?: FetcherMetricAdapter;
}

export interface FetcherPrometheusMetricsSinkOptions extends Omit<
  PrometheusRuntimeTelemetrySinkOptions,
  "cardinality" | "defaultQueue" | "expectedActive" | "identity"
> {
  readonly identity: FetcherMetricIdentity;
  readonly config: FetcherConfig;
  readonly stateStore: FetcherDurableStateStore;
}

type MetricLabels = Readonly<Record<string, string>>;

interface FetcherStageHistogram {
  readonly buckets: number[];
  count: number;
  sum: number;
}

const FETCHER_STAGE_METRIC_SERVICE = "fetch";

export function createFetcherPrometheusMetricsSink(
  options: FetcherPrometheusMetricsSinkOptions
): FetcherPrometheusMetricsSink {
  const metricIdentity: FetcherMetricIdentity & Required<Pick<FetcherMetricIdentity, "revision" | "deployment" | "adapter">> = {
    ...options.identity,
    revision: options.config.buildRevision,
    deployment: options.config.deploymentMode,
    adapter: options.identity.adapter ?? fetcherMetricAdapter(options.stateStore)
  };
  const queue = getWorkerRoute("fetch").mainQueue.name;
  const runtimeMetrics = createPrometheusRuntimeTelemetrySink({
    identity: metricIdentity,
    defaultQueue: queue,
    expectedActive: options.config.expectedActive,
    cardinality: {
      dependencies: FETCHER_METRIC_DEPENDENCIES,
      healthChecks: FETCHER_HEALTH_CHECKS
    }
  });
  runtimeMetrics.setLastSuccessTimestamp(0);
  const identity = {
    environment: metricIdentity.environment,
    host: metricIdentity.host ?? "unknown",
    service: metricIdentity.service,
    version: metricIdentity.version
  };
  const stageEventCounts = new Map<FetcherStageMetricOutcome, number>();
  const stageHistogram: FetcherStageHistogram = {
    buckets: FETCHER_STAGE_LATENCY_BUCKETS_SECONDS.map(() => 0),
    count: 0,
    sum: 0
  };
  let stateStoreReady = false;
  let lastSuccessTimestampSeconds = 0;

  return {
    allowedLabels: FETCHER_METRIC_LABELS,
    async emit(event: RuntimeTelemetryEvent): Promise<void> {
      await runtimeMetrics.emit(event);
      const stageOutcome = fetcherStageOutcome(event, queue);

      if (stageOutcome === undefined) {
        return;
      }

      stageEventCounts.set(stageOutcome, (stageEventCounts.get(stageOutcome) ?? 0) + 1);
      const durationSeconds = durationSecondsFrom(measuredDuration(event));

      if (durationSeconds !== undefined) {
        observeStageHistogram(stageHistogram, durationSeconds);
      }

      if (stageOutcome === "success" || stageOutcome === "duplicate") {
        const timestampMs = Date.parse(event.at);

        if (Number.isFinite(timestampMs)) {
          lastSuccessTimestampSeconds = Math.max(lastSuccessTimestampSeconds, timestampMs / 1_000);
          runtimeMetrics.setLastSuccessTimestamp(lastSuccessTimestampSeconds);
        }
      }
    },
    collect(): string {
      const baseLabels = {
        environment: boundedLabel(identity.environment),
        host: boundedLabel(identity.host),
        service: boundedLabel(identity.service),
        version: boundedLabel(identity.version)
      };
      const lines = [
        "# HELP nutsnews_worker_state_store_ready Whether the configured state store is healthy and safe for the selected dependency mode.",
        "# TYPE nutsnews_worker_state_store_ready gauge",
        metricLine("nutsnews_worker_state_store_ready", {
          ...baseLabels,
          dependency: "state-store",
          outcome: options.stateStore.mode,
          queue
        }, stateStoreReady ? 1 : 0)
      ];
      const stageMetrics = collectStageMetrics(
        boundedLabel(options.identity.environment),
        stageEventCounts,
        stageHistogram
      );

      const runtimeOutput = runtimeMetrics.collect().trimEnd();

      return `${[
        runtimeOutput,
        lines.join("\n"),
        collectFetcherTelemetryStatusMetrics(metricIdentity, true, true),
        stageMetrics.trimEnd()
      ].filter((output) => output.length > 0).join("\n")}\n`;
    },
    collectStatus(collectionReady): string {
      return collectFetcherTelemetryStatusMetrics(metricIdentity, true, collectionReady);
    },
    setInFlight(queueName, value): void {
      runtimeMetrics.setInFlight(queueName, value);
    },
    setShutdownDraining(draining): void {
      runtimeMetrics.setShutdownDraining(draining);
    },
    setStateStoreReady(ready): void {
      stateStoreReady = ready;
    }
  };
}

export function collectFetcherTelemetryStatusMetrics(
  identity: Pick<FetcherMetricIdentity, "environment" | "service">,
  metricsEnabled: boolean,
  collectionReady: boolean
): string {
  const labels = {
    environment: boundedLabel(identity.environment),
    service: boundedLabel(identity.service)
  };

  return [
    "# HELP nutsnews_worker_metrics_enabled Whether bounded Prometheus application metrics are enabled.",
    "# TYPE nutsnews_worker_metrics_enabled gauge",
    metricLine("nutsnews_worker_metrics_enabled", labels, metricsEnabled ? 1 : 0),
    "# HELP nutsnews_worker_telemetry_collection_ready Whether the application metrics collector completed the current scrape.",
    "# TYPE nutsnews_worker_telemetry_collection_ready gauge",
    metricLine("nutsnews_worker_telemetry_collection_ready", labels, collectionReady ? 1 : 0)
  ].join("\n");
}

export function fetcherMetricAdapter(stateStore: FetcherDurableStateStore): FetcherMetricAdapter {
  switch (stateStore.mode) {
    case "local-memory":
      return "in_memory";
    case "postgresql":
      return "production";
    case "unsupported":
      return "unknown";
  }
}

function fetcherStageOutcome(
  event: RuntimeTelemetryEvent,
  queue: string
): FetcherStageMetricOutcome | undefined {
  if (event.stage !== "fetch" || event.queue !== queue) {
    return undefined;
  }

  switch (event.name) {
    case "runtime.message.accepted":
      // Accepted is the success event in both supported Runtime generations,
      // but retain an explicitly reported terminal failure as the shared
      // lifecycle contract's bounded fallback rather than misclassifying it.
      return event.outcome === "failure" ? "failure" : "success";
    case "runtime.message.duplicate":
      return "duplicate";
    case "runtime.message.invalid":
      return "invalid";
    case "runtime.message.retry":
      return "retry";
    case "runtime.message.dlq":
      return "dlq";
    case "runtime.broker.consumer_state_changed":
    case "runtime.broker.state_changed":
    case "runtime.broker.topology_asserted":
    case "runtime.dependency.observed":
    case "runtime.health.evaluated":
    case "runtime.message.started":
    case "runtime.shutdown.completed":
    case "runtime.shutdown.failed":
    case "runtime.shutdown.started":
      return undefined;
  }
}

function observeStageHistogram(histogram: FetcherStageHistogram, durationSeconds: number): void {
  histogram.count += 1;
  histogram.sum += durationSeconds;

  for (const [index, boundary] of FETCHER_STAGE_LATENCY_BUCKETS_SECONDS.entries()) {
    if (durationSeconds <= boundary) {
      histogram.buckets[index] = (histogram.buckets[index] ?? 0) + 1;
    }
  }
}

function collectStageMetrics(
  environment: string,
  eventCounts: ReadonlyMap<FetcherStageMetricOutcome, number>,
  histogram: FetcherStageHistogram
): string {
  const lines = [
    "# HELP nutsnews_worker_uplift_stage_events_total Completed worker-uplift stage delivery outcomes.",
    "# TYPE nutsnews_worker_uplift_stage_events_total counter"
  ];

  for (const outcome of FETCHER_STAGE_METRIC_OUTCOMES) {
    lines.push(orderedMetricLine("nutsnews_worker_uplift_stage_events_total", [
      ["environment", environment],
      ["service", FETCHER_STAGE_METRIC_SERVICE],
      ["outcome", outcome]
    ], eventCounts.get(outcome) ?? 0));
  }

  lines.push(
    "# HELP nutsnews_worker_uplift_stage_latency_seconds Worker-uplift stage delivery completion latency in seconds.",
    "# TYPE nutsnews_worker_uplift_stage_latency_seconds histogram"
  );

  for (const [index, boundary] of FETCHER_STAGE_LATENCY_BUCKETS_SECONDS.entries()) {
    lines.push(orderedMetricLine("nutsnews_worker_uplift_stage_latency_seconds_bucket", [
      ["environment", environment],
      ["service", FETCHER_STAGE_METRIC_SERVICE],
      ["le", String(boundary)]
    ], histogram.buckets[index] ?? 0));
  }

  lines.push(
    orderedMetricLine("nutsnews_worker_uplift_stage_latency_seconds_bucket", [
      ["environment", environment],
      ["service", FETCHER_STAGE_METRIC_SERVICE],
      ["le", "+Inf"]
    ], histogram.count),
    orderedMetricLine("nutsnews_worker_uplift_stage_latency_seconds_sum", [
      ["environment", environment],
      ["service", FETCHER_STAGE_METRIC_SERVICE]
    ], histogram.sum),
    orderedMetricLine("nutsnews_worker_uplift_stage_latency_seconds_count", [
      ["environment", environment],
      ["service", FETCHER_STAGE_METRIC_SERVICE]
    ], histogram.count)
  );

  return `${lines.join("\n")}\n`;
}

function durationSecondsFrom(value: number | undefined): number | undefined {
  return value !== undefined && Number.isFinite(value) ? Math.max(0, value) / 1_000 : undefined;
}

function measuredDuration(event: RuntimeTelemetryEvent): number | undefined {
  if (event.durationMs !== undefined && Number.isFinite(event.durationMs)) {
    return event.durationMs;
  }

  const durationMs = event.attributes?.durationMs;

  return typeof durationMs === "number" && Number.isFinite(durationMs) ? durationMs : undefined;
}

function metricLine(metric: string, labels: MetricLabels, value: number): string {
  const renderedLabels = Object.entries(labels)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([name, labelValue]) => `${name}="${escapeLabelValue(boundedLabel(labelValue))}"`)
    .join(",");

  return `${metric}{${renderedLabels}} ${formatMetricNumber(value)}`;
}

function orderedMetricLine(
  metric: string,
  labels: readonly (readonly [string, string])[],
  value: number
): string {
  const renderedLabels = labels
    .map(([name, labelValue]) => `${name}="${escapeLabelValue(boundedLabel(labelValue))}"`)
    .join(",");

  return `${metric}{${renderedLabels}} ${formatMetricNumber(value)}`;
}

function boundedLabel(value: string): string {
  const bounded = value
    .trim()
    .replace(/[^a-zA-Z0-9_.:+-]+/gu, "_")
    .slice(0, 128);

  return bounded.length > 0 ? bounded : "unknown";
}

function escapeLabelValue(value: string): string {
  return value
    .replace(/\\/gu, "\\\\")
    .replace(/\n/gu, "\\n")
    .replace(/"/gu, "\\\"");
}

function formatMetricNumber(value: number): string {
  if (!Number.isFinite(value)) {
    return "0";
  }

  return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(6)));
}
