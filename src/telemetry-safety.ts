import type {
  RuntimeTelemetryEvent,
  RuntimeTelemetryFlusher,
  RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type {
  FetcherBaseMetricsSink,
  FetcherHealthOutcome,
  FetcherHealthProbe,
  FetcherPrometheusMetricsSink
} from "./metrics.js";

export function bestEffortTelemetrySink(
  sink: RuntimeTelemetrySink | undefined
): RuntimeTelemetrySink | undefined {
  if (sink === undefined) {
    return undefined;
  }

  return {
    emit: async (event) => {
      try {
        await sink.emit(event);
      } catch {
        // Observability is never part of acknowledgement or retry state.
      }
    }
  };
}

export function combineBestEffortTelemetrySinks(
  ...sinks: readonly (RuntimeTelemetrySink | undefined)[]
): RuntimeTelemetrySink | undefined {
  const configured = sinks
    .map((sink) => bestEffortTelemetrySink(sink))
    .filter((sink): sink is RuntimeTelemetrySink => sink !== undefined);

  if (configured.length === 0) {
    return undefined;
  }

  return {
    emit: async (event) => {
      for (const sink of configured) {
        await sink.emit(event);
      }
    }
  };
}

export function bestEffortTelemetryFlusher(
  sink: (RuntimeTelemetrySink & RuntimeTelemetryFlusher) | undefined
): (RuntimeTelemetrySink & RuntimeTelemetryFlusher) | undefined {
  if (sink === undefined) {
    return undefined;
  }

  return {
    emit: async (event) => {
      try {
        await sink.emit(event);
      } catch {
        // A failed structured-log write cannot alter fetch processing.
      }
    },
    flush: async () => {
      try {
        await sink.flush();
      } catch {
        // A failed log flush cannot block graceful shutdown.
      }
    }
  };
}

export function bestEffortFetcherMetricsSink(
  sink: FetcherPrometheusMetricsSink
): FetcherPrometheusMetricsSink;
export function bestEffortFetcherMetricsSink(
  sink: FetcherBaseMetricsSink
): FetcherBaseMetricsSink;
export function bestEffortFetcherMetricsSink(
  sink: FetcherBaseMetricsSink | FetcherPrometheusMetricsSink | undefined
): FetcherBaseMetricsSink | FetcherPrometheusMetricsSink | undefined;
export function bestEffortFetcherMetricsSink(
  sink: FetcherBaseMetricsSink | FetcherPrometheusMetricsSink | undefined
): FetcherBaseMetricsSink | FetcherPrometheusMetricsSink | undefined {
  if (sink === undefined) {
    return undefined;
  }

  const common = {
    allowedLabels: sink.allowedLabels,
    emit: async (event: RuntimeTelemetryEvent): Promise<void> => {
      try {
        await sink.emit(event);
      } catch {
        // Metric rejection cannot alter ack, retry, DLQ, or idempotency state.
      }
    },
    collect: (): string => {
      try {
        return sink.collect();
      } catch {
        return safelyCollectStatus(sink, false);
      }
    },
    collectStatus: (collectionReady: boolean): string => safelyCollectStatus(sink, collectionReady),
    setInFlight: (queue: string, value: number): void => {
      safely(() => sink.setInFlight(queue, value));
    },
    setShutdownDraining: (draining: boolean): void => {
      safely(() => sink.setShutdownDraining(draining));
    }
  };

  if (!isFetcherMetrics(sink)) {
    return common;
  }

  return {
    ...common,
    allowedLabels: sink.allowedLabels,
    setStateStoreReady: (ready: boolean): void => {
      safely(() => sink.setStateStoreReady(ready));
    },
    setHealthProbe: (probe: FetcherHealthProbe, outcome: FetcherHealthOutcome): void => {
      safely(() => sink.setHealthProbe(probe, outcome));
    }
  };
}

function safelyCollectStatus(sink: FetcherBaseMetricsSink, collectionReady: boolean): string {
  try {
    return sink.collectStatus(collectionReady);
  } catch {
    return [
      "# HELP nutsnews_worker_metrics_enabled Whether bounded Prometheus application metrics are enabled.",
      "# TYPE nutsnews_worker_metrics_enabled gauge",
      "nutsnews_worker_metrics_enabled 1",
      "# HELP nutsnews_worker_telemetry_collection_ready Whether the application metrics collector completed the current scrape.",
      "# TYPE nutsnews_worker_telemetry_collection_ready gauge",
      `nutsnews_worker_telemetry_collection_ready ${collectionReady ? "1" : "0"}`,
      ""
    ].join("\n");
  }
}

function isFetcherMetrics(
  sink: FetcherBaseMetricsSink | FetcherPrometheusMetricsSink
): sink is FetcherPrometheusMetricsSink {
  return "setStateStoreReady" in sink
    && typeof sink.setStateStoreReady === "function"
    && "setHealthProbe" in sink
    && typeof sink.setHealthProbe === "function";
}

function safely(operation: () => void): void {
  try {
    operation();
  } catch {
    // Metric mutation is best effort and cannot alter service behavior.
  }
}
