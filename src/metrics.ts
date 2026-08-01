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
  "le"
] as const;

export const FETCHER_DURATION_HISTOGRAM_BUCKETS_SECONDS = [
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

export const FETCHER_METRIC_DEPENDENCIES = [
  "feed-fetch",
  "broker-settlement",
  "other"
] as const;

export const FETCHER_STAGE_LATENCY_BUCKETS_SECONDS = [
  0.01,
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
  "dlq"
] as const;

export type FetcherStageMetricOutcome = (typeof FETCHER_STAGE_METRIC_OUTCOMES)[number];
export type FetcherHealthProbe = "liveness" | "startup" | "readiness";
export type FetcherHealthOutcome = "ok" | "degraded" | "unhealthy";
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
  setHealthProbe(probe: FetcherHealthProbe, outcome: FetcherHealthOutcome): void;
}

export interface FetcherMetricIdentity extends RuntimeServiceIdentity {
  readonly revision?: string;
  readonly deployment?: FetcherConfig["deploymentMode"];
  readonly adapter?: FetcherMetricAdapter;
}

export interface FetcherPrometheusMetricsSinkOptions extends Omit<PrometheusRuntimeTelemetrySinkOptions, "identity"> {
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

type FetcherMetricDependency = (typeof FETCHER_METRIC_DEPENDENCIES)[number];

interface FetcherDurationHistogram {
  readonly buckets: number[];
  count: number;
  sum: number;
}

interface FetcherDependencyHistogramKey {
  readonly dependency: FetcherMetricDependency;
  readonly outcome: "success" | "retry" | "failure" | "other";
}

const FETCHER_STAGE_METRIC_SERVICE = "fetch";
const FETCHER_HEALTH_PROBES = [
  "liveness",
  "startup",
  "readiness"
] as const satisfies readonly FetcherHealthProbe[];
const FETCHER_HEALTH_OUTCOMES = [
  "ok",
  "degraded",
  "unhealthy"
] as const satisfies readonly FetcherHealthOutcome[];

export function createFetcherPrometheusMetricsSink(
  options: FetcherPrometheusMetricsSinkOptions
): FetcherPrometheusMetricsSink {
  const metricIdentity: FetcherMetricIdentity & Required<Pick<FetcherMetricIdentity, "revision" | "deployment" | "adapter">> = {
    ...options.identity,
    revision: options.config.buildRevision,
    deployment: options.config.deploymentMode,
    adapter: options.identity.adapter ?? fetcherMetricAdapter(options.stateStore)
  };
  const runtimeMetrics = createPrometheusRuntimeTelemetrySink({
    identity: metricIdentity,
    ...(options.defaultQueue === undefined ? {} : {
      defaultQueue: options.defaultQueue
    }),
    expectedActive: options.config.expectedActive
  } as PrometheusRuntimeTelemetrySinkOptions);
  const queue = getWorkerRoute("fetch").mainQueue.name;
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
  const processingHistograms = new Map<FetcherStageMetricOutcome, FetcherDurationHistogram>();
  const dependencyHistograms = new Map<string, {
    readonly key: FetcherDependencyHistogramKey;
    readonly histogram: FetcherDurationHistogram;
  }>();
  let stateStoreReady = false;
  let lastSuccessTimestampSeconds = 0;
  const health = new Map<FetcherHealthProbe, FetcherHealthOutcome>([
    ["liveness", "ok"],
    ["startup", "unhealthy"],
    ["readiness", "unhealthy"]
  ]);

  return {
    allowedLabels: FETCHER_METRIC_LABELS,
    async emit(event: RuntimeTelemetryEvent): Promise<void> {
      // Runtime health events remain available to the independent structured
      // log sink. This service owns the seeded health family, so forwarding
      // the same event to Runtime 1 metrics would create a duplicate family.
      const isServiceOwnedHealthEvent = event.name === "runtime.health.evaluated";
      // Runtime 0.5 treats missing dependency duration as zero. Preserve the
      // event for logs/custom state while refusing to fabricate latency.
      const isDependencyEvent = event.name === "runtime.dependency.observed";

      // Runtime 0.5 only exposes dependency latency as a millisecond summary.
      // This service records that event into a bounded seconds histogram below
      // and does not retain the obsolete summary in the compatibility sink.
      if (!isServiceOwnedHealthEvent && !isDependencyEvent) {
        await runtimeMetrics.emit(withoutDurationForRuntimeMetrics(event));
      }

      recordHealthEvent(health, event);
      recordDependencyHistogram(dependencyHistograms, event, queue);
      const stageOutcome = fetcherStageOutcome(event, queue);

      if (stageOutcome === undefined) {
        return;
      }

      stageEventCounts.set(stageOutcome, (stageEventCounts.get(stageOutcome) ?? 0) + 1);
      const durationSeconds = durationSecondsFrom(measuredDuration(event));

      if (durationSeconds !== undefined) {
        observeStageHistogram(stageHistogram, durationSeconds);
        observeDurationHistogram(
          histogramFor(processingHistograms, stageOutcome),
          durationSeconds
        );
      }

      if (stageOutcome === "success" || stageOutcome === "duplicate") {
        const timestampMs = Date.parse(event.at);

        if (Number.isFinite(timestampMs)) {
          lastSuccessTimestampSeconds = Math.max(lastSuccessTimestampSeconds, timestampMs / 1_000);
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
        }, stateStoreReady ? 1 : 0),
        ...collectHealthMetrics(
          boundedLabel(options.identity.environment),
          health
        )
      ];
      const stageMetrics = collectStageMetrics(
        boundedLabel(options.identity.environment),
        stageEventCounts,
        stageHistogram
      );

      const runtimeOutput = runtimeMetrics.collect().trimEnd();
      const compatibility = collectCompatibilityRuntimeMetrics(
        options,
        metricIdentity,
        runtimeOutput,
        lastSuccessTimestampSeconds
      );

      return `${[
        runtimeOutput,
        compatibility,
        lines.join("\n"),
        collectFetcherTelemetryStatusMetrics(metricIdentity, true, true),
        collectDurationMetrics(
          metricIdentity,
          queue,
          processingHistograms,
          dependencyHistograms
        ),
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
    },
    setHealthProbe(probe, outcome): void {
      health.set(probe, outcome);
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

function collectCompatibilityRuntimeMetrics(
  options: FetcherPrometheusMetricsSinkOptions,
  identity: Required<Pick<FetcherMetricIdentity, "revision" | "deployment" | "adapter">> & FetcherMetricIdentity,
  runtimeOutput: string,
  lastSuccessTimestampSeconds: number
): string {
  const identityLabels = {
    environment: boundedLabel(identity.environment),
    service: boundedLabel(identity.service)
  };
  const lines: string[] = [];

  if (!hasMetricFamily(runtimeOutput, "nutsnews_worker_build_info")) {
    lines.push(
      "# HELP nutsnews_worker_build_info Immutable worker build identity.",
      "# TYPE nutsnews_worker_build_info gauge",
      metricLine("nutsnews_worker_build_info", {
        ...identityLabels,
        version: boundedLabel(identity.version),
        revision: boundedLabel(identity.revision)
      }, 1)
    );
  }

  if (!hasMetricFamily(runtimeOutput, "nutsnews_worker_deployment_info")) {
    lines.push(
      "# HELP nutsnews_worker_deployment_info Worker deployment ownership and dependency adapter identity.",
      "# TYPE nutsnews_worker_deployment_info gauge",
      metricLine("nutsnews_worker_deployment_info", {
        ...identityLabels,
        deployment: boundedLabel(identity.deployment),
        adapter: boundedLabel(identity.adapter)
      }, 1)
    );
  }

  if (!hasMetricFamily(runtimeOutput, "nutsnews_worker_expected_active")) {
    lines.push(
      "# HELP nutsnews_worker_expected_active Whether this deployment is expected to own active production work.",
      "# TYPE nutsnews_worker_expected_active gauge",
      metricLine("nutsnews_worker_expected_active", identityLabels, options.config.expectedActive ? 1 : 0)
    );
  }

  if (!hasMetricFamily(runtimeOutput, "nutsnews_worker_last_success_timestamp_seconds")) {
    lines.push(
      "# HELP nutsnews_worker_last_success_timestamp_seconds Unix timestamp of the latest service-owned successful work cycle.",
      "# TYPE nutsnews_worker_last_success_timestamp_seconds gauge",
      metricLine("nutsnews_worker_last_success_timestamp_seconds", identityLabels, lastSuccessTimestampSeconds)
    );
  }

  return lines.join("\n");
}

function hasMetricFamily(output: string, metric: string): boolean {
  return output.split("\n").some((line) => line.startsWith(`# HELP ${metric} `)
    || line.startsWith(`${metric}{`)
    || line.startsWith(`${metric} `));
}

function withoutDurationForRuntimeMetrics(event: RuntimeTelemetryEvent): RuntimeTelemetryEvent {
  const runtimeEvent = {
    ...event
  };

  // Runtime 0.5 couples message counters to millisecond summaries. Forward a
  // duration-free clone for counters and own all duration observations in the
  // fixed-bucket seconds histograms below.
  Reflect.deleteProperty(runtimeEvent, "durationMs");
  return runtimeEvent;
}

function recordDependencyHistogram(
  histograms: Map<string, {
    readonly key: FetcherDependencyHistogramKey;
    readonly histogram: FetcherDurationHistogram;
  }>,
  event: RuntimeTelemetryEvent,
  queue: string
): void {
  if (event.name !== "runtime.dependency.observed"
    || event.stage !== "fetch"
    || event.queue !== queue) {
    return;
  }

  const durationSeconds = durationSecondsFrom(measuredDuration(event));

  if (durationSeconds === undefined) {
    return;
  }

  const key = {
    dependency: boundedDependency(event.attributes?.dependency),
    outcome: boundedDependencyOutcome(event.outcome ?? event.attributes?.outcome)
  } satisfies FetcherDependencyHistogramKey;
  const mapKey = `${key.dependency}\u0000${key.outcome}`;
  let entry = histograms.get(mapKey);

  if (entry === undefined) {
    entry = {
      key,
      histogram: createDurationHistogram()
    };
    histograms.set(mapKey, entry);
  }

  observeDurationHistogram(entry.histogram, durationSeconds);
}

function collectDurationMetrics(
  identity: FetcherMetricIdentity,
  queue: string,
  processingHistograms: ReadonlyMap<FetcherStageMetricOutcome, FetcherDurationHistogram>,
  dependencyHistograms: ReadonlyMap<string, {
    readonly key: FetcherDependencyHistogramKey;
    readonly histogram: FetcherDurationHistogram;
  }>
): string {
  const baseLabels = [
    ["environment", boundedLabel(identity.environment)],
    ["host", boundedLabel(identity.host ?? "unknown")],
    ["service", boundedLabel(identity.service)],
    ["version", boundedLabel(identity.version)],
    ["stage", FETCHER_STAGE_METRIC_SERVICE],
    ["queue", queue]
  ] as const;
  const lines: string[] = [];

  if (processingHistograms.size > 0) {
    lines.push(
      "# HELP nutsnews_worker_processing_duration_seconds Worker processing latency in seconds.",
      "# TYPE nutsnews_worker_processing_duration_seconds histogram"
    );

    for (const outcome of FETCHER_STAGE_METRIC_OUTCOMES) {
      const histogram = processingHistograms.get(outcome);

      if (histogram !== undefined) {
        collectDurationHistogram(lines, "nutsnews_worker_processing_duration_seconds", [
          ...baseLabels,
          ["outcome", outcome]
        ], histogram);
      }
    }
  }

  if (dependencyHistograms.size > 0) {
    lines.push(
      "# HELP nutsnews_worker_dependency_duration_seconds Worker dependency latency in seconds.",
      "# TYPE nutsnews_worker_dependency_duration_seconds histogram"
    );

    const sortedEntries = Array.from(dependencyHistograms.values()).sort((left, right) => {
      const dependencyOrder = FETCHER_METRIC_DEPENDENCIES.indexOf(left.key.dependency)
        - FETCHER_METRIC_DEPENDENCIES.indexOf(right.key.dependency);

      return dependencyOrder !== 0
        ? dependencyOrder
        : left.key.outcome.localeCompare(right.key.outcome);
    });

    for (const entry of sortedEntries) {
      collectDurationHistogram(lines, "nutsnews_worker_dependency_duration_seconds", [
        ...baseLabels,
        ["outcome", entry.key.outcome],
        ["dependency", entry.key.dependency]
      ], entry.histogram);
    }
  }

  return lines.join("\n");
}

function collectDurationHistogram(
  lines: string[],
  metric: string,
  labels: readonly (readonly [string, string])[],
  histogram: FetcherDurationHistogram
): void {
  for (const [index, boundary] of FETCHER_DURATION_HISTOGRAM_BUCKETS_SECONDS.entries()) {
    lines.push(orderedMetricLine(`${metric}_bucket`, [
      ...labels,
      ["le", String(boundary)]
    ], histogram.buckets[index] ?? 0));
  }

  lines.push(
    orderedMetricLine(`${metric}_bucket`, [
      ...labels,
      ["le", "+Inf"]
    ], histogram.count),
    orderedMetricLine(`${metric}_sum`, labels, histogram.sum),
    orderedMetricLine(`${metric}_count`, labels, histogram.count)
  );
}

function histogramFor<K>(
  histograms: Map<K, FetcherDurationHistogram>,
  key: K
): FetcherDurationHistogram {
  let histogram = histograms.get(key);

  if (histogram === undefined) {
    histogram = createDurationHistogram();
    histograms.set(key, histogram);
  }

  return histogram;
}

function createDurationHistogram(): FetcherDurationHistogram {
  return {
    buckets: FETCHER_DURATION_HISTOGRAM_BUCKETS_SECONDS.map(() => 0),
    count: 0,
    sum: 0
  };
}

function observeDurationHistogram(histogram: FetcherDurationHistogram, durationSeconds: number): void {
  histogram.count += 1;
  histogram.sum += durationSeconds;

  for (const [index, boundary] of FETCHER_DURATION_HISTOGRAM_BUCKETS_SECONDS.entries()) {
    if (durationSeconds <= boundary) {
      histogram.buckets[index] = (histogram.buckets[index] ?? 0) + 1;
    }
  }
}

function boundedDependency(value: unknown): FetcherMetricDependency {
  return typeof value === "string"
    && FETCHER_METRIC_DEPENDENCIES.some((dependency) => dependency !== "other" && dependency === value)
    ? value as FetcherMetricDependency
    : "other";
}

function boundedDependencyOutcome(value: unknown): FetcherDependencyHistogramKey["outcome"] {
  switch (value) {
    case "success":
    case "retry":
    case "failure":
      return value;
    default:
      return "other";
  }
}

function recordHealthEvent(
  health: Map<FetcherHealthProbe, FetcherHealthOutcome>,
  event: RuntimeTelemetryEvent
): void {
  if (event.name !== "runtime.health.evaluated") {
    return;
  }

  const probe = event.attributes?.probe;
  const outcome = event.outcome ?? event.attributes?.status;

  if (isHealthProbe(probe) && isHealthOutcome(outcome)) {
    health.set(probe, outcome);
  }
}

function collectHealthMetrics(
  environment: string,
  health: ReadonlyMap<FetcherHealthProbe, FetcherHealthOutcome>
): string[] {
  const lines = [
    "# HELP nutsnews_worker_health_probe Worker liveness, startup, and readiness state by bounded probe and outcome.",
    "# TYPE nutsnews_worker_health_probe gauge"
  ];

  for (const probe of FETCHER_HEALTH_PROBES) {
    const current = health.get(probe) ?? "unhealthy";

    for (const outcome of FETCHER_HEALTH_OUTCOMES) {
      lines.push(orderedMetricLine("nutsnews_worker_health_probe", [
        ["environment", environment],
        ["service", FETCHER_STAGE_METRIC_SERVICE],
        ["probe", probe],
        ["outcome", outcome]
      ], current === outcome ? 1 : 0));
    }
  }

  return lines;
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
      return "success";
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

function isHealthProbe(value: unknown): value is FetcherHealthProbe {
  return typeof value === "string" && FETCHER_HEALTH_PROBES.some((probe) => probe === value);
}

function isHealthOutcome(value: unknown): value is FetcherHealthOutcome {
  return typeof value === "string" && FETCHER_HEALTH_OUTCOMES.some((outcome) => outcome === value);
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
