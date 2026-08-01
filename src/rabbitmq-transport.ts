import {
  WORKER_DELIVERY_BEHAVIOR,
  getRetryDestination,
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  computeRetryJitterMs,
  createBrokerConsumerMonitor,
  createRetryEnvelope,
  emitRuntimeTelemetry,
  assertRabbitMqTopology,
  randomUuidMessageIdFactory,
  runtimeNow,
  runtimeTraceHeadersFromEnvelope,
  type BrokerConsumerHandle,
  type BrokerConsumerMonitor,
  type BrokerConsumerStatus,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RabbitMqConfirmChannel,
  type RuntimeClock,
  type RuntimeMessageProcessingResult,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  connect as amqpConnect,
  type ChannelModel,
  type ConfirmChannel,
  type ConsumeMessage,
  type Options
} from "amqplib";

import {
  FetcherDefinitePublishError,
  type FetcherBrokerTransport
} from "./dependencies.js";
import { bestEffortTelemetrySink } from "./telemetry-safety.js";

const DEFAULT_CONFIRM_TIMEOUT_MS = WORKER_DELIVERY_BEHAVIOR.confirmTimeoutMs;
const DEFAULT_DRAIN_TIMEOUT_MS = 30_000;

interface PayloadCarrier {
  readonly envelope: WorkerMessageEnvelope;
  readonly payload: Readonly<Record<string, unknown>>;
}

interface PayloadConsumerRegistration {
  readonly handler: BrokerDeliveryHandler;
  readonly monitor: BrokerConsumerMonitor;
  consumerTag: string | undefined;
}

type RabbitMqConnect = (url: string) => Promise<ChannelModel>;

export class PayloadRabbitMqTransport implements FetcherBrokerTransport {
  readonly adapterMode = "production" as const;
  readonly name = "rabbitmq-payload-transport";

  private readonly url: string;
  private readonly prefetchCount: number;
  private readonly clock: RuntimeClock;
  private readonly telemetry: RuntimeTelemetrySink | undefined;
  private readonly connectToBroker: RabbitMqConnect;
  private readonly consumers = new Map<WorkerStage, PayloadConsumerRegistration>();
  private readonly consumerStateListeners = new Set<() => void>();
  private readonly inFlight = new Set<Promise<void>>();
  private connection: ChannelModel | undefined;
  private channel: ConfirmChannel | undefined;
  private channelOperation: Promise<ConfirmChannel> | undefined;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private reconnecting = false;
  private routes: readonly WorkerRoute[] = [];
  private closing = false;

  constructor(options: {
    readonly url: string;
    readonly prefetch: number;
    readonly clock: RuntimeClock;
    readonly telemetry?: RuntimeTelemetrySink;
    readonly connect?: RabbitMqConnect;
  }) {
    this.url = options.url;
    this.prefetchCount = options.prefetch;
    this.clock = options.clock;
    this.telemetry = bestEffortTelemetrySink(options.telemetry);
    this.connectToBroker = options.connect ?? amqpConnect;
  }

  get inFlightDeliveryCount(): number {
    return this.inFlight.size;
  }

  consumerStatus(stage: WorkerStage): BrokerConsumerStatus | undefined {
    return this.consumers.get(stage)?.monitor.status;
  }

  onConsumerStateChange(listener: () => void): () => void {
    this.consumerStateListeners.add(listener);

    return () => {
      this.consumerStateListeners.delete(listener);
    };
  }

  async connect(): Promise<void> {
    this.closing = false;
    await this.ensureChannel();
  }

  async assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    const channelExists = this.channel !== undefined;

    this.routes = routes;
    const channel = await this.ensureChannel();

    if (channelExists) {
      await assertPayloadRabbitMqTopology(channel, routes);
    }
  }

  async publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    const route = getWorkerRoute(command.envelope.route);
    let channel: ConfirmChannel;

    try {
      channel = await this.ensureChannel();
    } catch (error: unknown) {
      // The carrier has not been handed to amqplib yet, so this is one of the
      // few failures for which an immediate retry is provably safe.
      throw new FetcherDefinitePublishError("RabbitMQ channel was unavailable before publish.", error);
    }

    await publishCarrierWithConfirm(channel, {
      carrier: {
        envelope: command.envelope,
        payload: command.payload
      },
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmTimeoutMs: DEFAULT_CONFIRM_TIMEOUT_MS
    });

    return {
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: runtimeNow(this.clock)
    };
  }

  async consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    const channel = await this.ensureChannel();
    const existing = this.consumers.get(stage);

    if (existing?.consumerTag !== undefined) {
      existing.monitor.markClosed("consumer-replaced");
      this.notifyConsumerStateChange();
      await channel.cancel(existing.consumerTag).catch(() => undefined);
    }

    const registration: PayloadConsumerRegistration = {
      handler,
      consumerTag: undefined,
      monitor: createBrokerConsumerMonitor({
        stage,
        clock: this.clock,
        ...(this.telemetry === undefined ? {} : {
          telemetry: this.telemetry
        })
      })
    };
    this.consumers.set(stage, registration);
    await this.activateConsumer(stage, registration, channel);

    return {
      stage,
      cancel: async (): Promise<void> => {
        const registered = this.consumers.get(stage);
        this.consumers.delete(stage);
        registered?.monitor.markClosed("consumer-handle-cancelled");
        this.notifyConsumerStateChange();

        if (registered?.consumerTag !== undefined && this.channel !== undefined) {
          await this.channel.cancel(registered.consumerTag).catch(() => undefined);
        }
      }
    };
  }

  async drain(timeoutMs = DEFAULT_DRAIN_TIMEOUT_MS): Promise<void> {
    if (this.inFlight.size === 0) {
      return;
    }

    let timeout: ReturnType<typeof setTimeout> | undefined;

    try {
      await Promise.race([
        Promise.all([...this.inFlight]).then(() => undefined),
        new Promise<void>((_, reject) => {
          timeout = setTimeout(() => {
            reject(new Error("Timed out waiting for RabbitMQ payload deliveries to drain."));
          }, timeoutMs);
        })
      ]);
    } finally {
      if (timeout !== undefined) {
        clearTimeout(timeout);
      }
    }
  }

  async close(): Promise<void> {
    this.closing = true;
    this.clearReconnectTimer();
    const channel = this.channel;
    const connection = this.connection;

    this.channel = undefined;
    this.connection = undefined;

    if (channel !== undefined) {
      for (const registration of this.consumers.values()) {
        if (registration.consumerTag !== undefined) {
          // A broker RPC can hang after a network partition. Cancellation is
          // best effort here because the connection close below is the hard
          // intake boundary and must always be initiated.
          void channel.cancel(registration.consumerTag).catch(() => undefined);
        }
        registration.monitor.markClosed("transport-closing");
        this.notifyConsumerStateChange();
      }
    }

    this.consumers.clear();
    // Managed lifecycle shutdown drains before close. A direct close is the
    // bounded force-close path after that graceful drain times out, so it must
    // not repeat the same wait and strand the connection for another window.
    this.inFlight.clear();

    // Initiate both closes concurrently. In particular, do not wait for a
    // channel RPC before starting the connection close that tears down the
    // underlying socket. The service-level wrapper bounds this whole call.
    await Promise.all([
      channel?.close().catch(() => undefined),
      connection?.close().catch(() => undefined)
    ]);
  }

  private async ensureChannel(): Promise<ConfirmChannel> {
    if (this.closingRequested()) {
      throw new Error("RabbitMQ payload transport is closing.");
    }

    if (this.channelOperation !== undefined) {
      return this.channelOperation;
    }

    if (this.channel !== undefined) {
      return this.channel;
    }

    const operation = this.openChannel();

    this.channelOperation = operation;

    try {
      return await operation;
    } finally {
      if (this.channelOperation === operation) {
        this.channelOperation = undefined;
      }
    }
  }

  private async openChannel(): Promise<ConfirmChannel> {
    const connection = await this.connectToBroker(this.url);

    if (this.closing) {
      await connection.close().catch(() => undefined);
      throw new Error("RabbitMQ payload transport closed while connecting.");
    }

    let channel: ConfirmChannel;

    try {
      channel = await connection.createConfirmChannel();
    } catch (error: unknown) {
      await connection.close().catch(() => undefined);
      throw error;
    }

    if (this.closingRequested()) {
      await Promise.all([
        channel.close().catch(() => undefined),
        connection.close().catch(() => undefined)
      ]);
      throw new Error("RabbitMQ payload transport closed while opening a channel.");
    }

    this.connection = connection;
    this.channel = channel;

    connection.on("close", () => {
      this.markChannelClosed(channel, "connection-closed");
      this.scheduleConsumerReconnect();
    });
    connection.on("error", () => {
      this.markChannelClosed(channel, "connection-error");
      this.scheduleConsumerReconnect();
    });
    channel.on("close", () => {
      this.markChannelClosed(channel, "channel-closed");
      this.scheduleConsumerReconnect();
    });
    channel.on("error", () => {
      this.markChannelClosed(channel, "channel-error");
      this.scheduleConsumerReconnect();
    });

    try {
      if (this.routes.length > 0) {
        await assertPayloadRabbitMqTopology(channel, this.routes);
      }
      await this.restoreConsumers(channel);
    } catch (error: unknown) {
      if (this.channel === channel) {
        this.channel = undefined;
      }
      if (this.connection === connection) {
        this.connection = undefined;
      }
      await Promise.all([
        channel.close().catch(() => undefined),
        connection.close().catch(() => undefined)
      ]);
      throw error;
    }

    if (this.closingRequested() || this.channel !== channel || this.connection !== connection) {
      await Promise.all([
        channel.close().catch(() => undefined),
        connection.close().catch(() => undefined)
      ]);
      throw new Error("RabbitMQ channel changed while consumers were being restored.");
    }

    return channel;
  }

  private closingRequested(): boolean {
    return this.closing;
  }

  private async restoreConsumers(channel: ConfirmChannel): Promise<void> {
    for (const [stage, registration] of this.consumers) {
      await this.activateConsumer(stage, registration, channel);
    }
  }

  private async activateConsumer(
    stage: WorkerStage,
    registration: PayloadConsumerRegistration,
    channel: ConfirmChannel
  ): Promise<void> {
    if (registration.consumerTag !== undefined) {
      return;
    }

    const route = getWorkerRoute(stage);
    registration.monitor.markRecovering("consumer-activation");
    this.notifyConsumerStateChange();
    await channel.prefetch(this.prefetchCount);
    const reply = await channel.consume(route.mainQueue.name, (message) => {
      if (message === null) {
        registration.consumerTag = undefined;
        registration.monitor.markCancelled("broker-cancelled-consumer");
        this.notifyConsumerStateChange();
        this.recoverCancelledConsumer(stage, registration, channel);
        return;
      }

      const tracked = this.handleDelivery(stage, registration.handler, message);
      this.inFlight.add(tracked);
      void tracked.then(
        () => {
          this.inFlight.delete(tracked);
        },
        () => {
          this.inFlight.delete(tracked);
        }
      );
    }, {
      noAck: false
    });

    registration.consumerTag = reply.consumerTag;
    registration.monitor.markActive("consumer-registered");
    this.notifyConsumerStateChange();
  }

  private markChannelClosed(channel: ConfirmChannel, reason: string): void {
    if (this.channel !== channel) {
      return;
    }

    this.channel = undefined;
    const connection = this.connection;

    this.connection = undefined;
    for (const registration of this.consumers.values()) {
      registration.consumerTag = undefined;
      registration.monitor.markChannelDropped(reason);
      this.notifyConsumerStateChange();
    }

    if (!this.closing && connection !== undefined) {
      void connection.close().catch(() => undefined);
    }
  }

  private notifyConsumerStateChange(): void {
    for (const listener of this.consumerStateListeners) {
      try {
        listener();
      } catch {
        // A health refresh listener is observability-only and must not alter
        // broker recovery, cancellation, or delivery semantics.
      }
    }
  }

  private recoverCancelledConsumer(
    stage: WorkerStage,
    registration: PayloadConsumerRegistration,
    channel: ConfirmChannel
  ): void {
    if (this.closing || this.channel !== channel || this.consumers.get(stage) !== registration) {
      return;
    }

    void this.activateConsumer(stage, registration, channel).catch(() => {
      this.markChannelClosed(channel, "consumer-reactivation-failed");
      this.scheduleConsumerReconnect();
    });
  }

  private scheduleConsumerReconnect(): void {
    if (this.closing || this.consumers.size === 0 || this.reconnecting || this.reconnectTimer !== undefined) {
      return;
    }

    this.reconnectTimer = setTimeout(() => {
      void this.reconnectConsumers();
    }, 1_000);
    this.reconnectTimer.unref();
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer !== undefined) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
  }

  private async reconnectConsumers(): Promise<void> {
    this.reconnectTimer = undefined;

    if (this.closing || this.consumers.size === 0 || this.channel !== undefined) {
      return;
    }

    this.reconnecting = true;
    let retry = false;
    try {
      await this.ensureChannel();
    } catch {
      retry = true;
    } finally {
      this.reconnecting = false;
    }

    if (retry) {
      this.scheduleConsumerReconnect();
    }
  }

  private async handleDelivery(
    stage: WorkerStage,
    handler: BrokerDeliveryHandler,
    message: ConsumeMessage
  ): Promise<void> {
    const channel = this.channel;

    if (channel === undefined) {
      return;
    }

    let carrier: PayloadCarrier;

    try {
      carrier = decodeCarrier(message);
    } catch (error: unknown) {
      channel.nack(message, false, false);
      await emitRuntimeTelemetry(this.telemetry, {
        name: "runtime.message.invalid",
        level: "warn",
        at: runtimeNow(this.clock),
        stage,
        queue: getWorkerRoute(stage).mainQueue.name,
        outcome: "failure",
        attributes: {
          issueCode: "invalid-payload-carrier",
          error: classifyTransportError(error)
        }
      });
      return;
    }

    const startedAtMs = this.clock.now().getTime();
    let result: RuntimeMessageProcessingResult;

    try {
      result = await handler({
        envelope: carrier.envelope,
        payload: carrier.payload,
        receivedAt: runtimeNow(this.clock)
      });
    } catch (error: unknown) {
      try {
        await this.settleProcessorFailure(channel, message, carrier, error, elapsedMs(this.clock, startedAtMs));
      } catch (settlementError: unknown) {
        channel.nack(message, false, true);
        await emitProcessorFailureDisposition(
          this.telemetry,
          this.clock,
          carrier.envelope,
          "retry",
          "broker-requeue",
          error,
          elapsedMs(this.clock, startedAtMs),
          classifyTransportError(settlementError)
        );
      }
      return;
    }

    try {
      await this.settleDelivery(channel, message, carrier, result);
    } catch (error: unknown) {
      // A failed or ambiguous transfer confirmation must retain the original
      // delivery. Redelivery may duplicate a confirmed-but-unobserved publish,
      // which downstream idempotency handles; dropping the source is unsafe.
      channel.nack(message, false, true);
      await emitRuntimeTelemetry(this.telemetry, {
        name: "runtime.dependency.observed",
        level: "error",
        at: runtimeNow(this.clock),
        stage: carrier.envelope.route,
        queue: getWorkerRoute(carrier.envelope.route).mainQueue.name,
        durationMs: elapsedMs(this.clock, startedAtMs),
        outcome: "failure",
        attributes: {
          dependency: "broker-settlement",
          error: classifyTransportError(error)
        }
      });
    }
  }

  private async settleProcessorFailure(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    carrier: PayloadCarrier,
    error: unknown,
    durationMs: number
  ): Promise<void> {
    const envelope = carrier.envelope;
    const route = getWorkerRoute(envelope.route);
    const destination = getRetryDestination(envelope.route, envelope.attempt.count);

    if ("ttlMs" in destination) {
      const retryEnvelope = createRetryEnvelope(envelope, {
        now: runtimeNow(this.clock),
        messageIdFactory: randomUuidMessageIdFactory
      });
      const retryJitterMs = computeRetryJitterMs(destination.ttlMs, 0.1);

      await publishCarrierWithConfirm(channel, {
        carrier: {
          envelope: retryEnvelope,
          payload: carrier.payload
        },
        exchange: route.retryExchange,
        routingKey: destination.routingKey,
        confirmTimeoutMs: DEFAULT_CONFIRM_TIMEOUT_MS,
        retryJitterMs
      });
      await emitProcessorFailureDisposition(
        this.telemetry,
        this.clock,
        envelope,
        "retry",
        destination.name,
        error,
        durationMs
      );
      channel.ack(message);
      return;
    }

    await publishCarrierWithConfirm(channel, {
      carrier,
      exchange: route.dlqExchange,
      routingKey: destination.routingKey,
      confirmTimeoutMs: DEFAULT_CONFIRM_TIMEOUT_MS
    });
    await emitProcessorFailureDisposition(
      this.telemetry,
      this.clock,
      envelope,
      "dlq",
      destination.name,
      error,
      durationMs
    );
    channel.ack(message);
  }

  private async settleDelivery(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    carrier: PayloadCarrier,
    result: RuntimeMessageProcessingResult
  ): Promise<void> {
    if (result.action === "ack") {
      channel.ack(message);
      return;
    }

    if (result.action === "retry") {
      const retryEnvelope = createRetryEnvelope(result.envelope, {
        now: runtimeNow(this.clock),
        messageIdFactory: randomUuidMessageIdFactory
      });
      const retryJitterMs = computeRetryJitterMs(result.destination.ttlMs, 0.1);
      await publishCarrierWithConfirm(channel, {
        carrier: {
          envelope: retryEnvelope,
          payload: carrier.payload
        },
        exchange: getWorkerRoute(result.envelope.route).retryExchange,
        routingKey: result.destination.routingKey,
        confirmTimeoutMs: DEFAULT_CONFIRM_TIMEOUT_MS,
        retryJitterMs
      });
      channel.ack(message);
      return;
    }

    if (result.envelope !== undefined && result.destination !== undefined) {
      await publishCarrierWithConfirm(channel, {
        carrier: {
          envelope: result.envelope,
          payload: carrier.payload
        },
        exchange: getWorkerRoute(result.envelope.route).dlqExchange,
        routingKey: result.destination.routingKey,
        confirmTimeoutMs: DEFAULT_CONFIRM_TIMEOUT_MS
      });
      channel.ack(message);
      return;
    }

    channel.nack(message, false, false);
  }
}

async function emitProcessorFailureDisposition(
  telemetry: RuntimeTelemetrySink | undefined,
  clock: RuntimeClock,
  envelope: WorkerMessageEnvelope,
  disposition: "retry" | "dlq",
  destination: string,
  error: unknown,
  durationMs: number,
  settlementError?: string
): Promise<void> {
  await emitRuntimeTelemetry(telemetry, {
    name: disposition === "retry" ? "runtime.message.retry" : "runtime.message.dlq",
    level: disposition === "retry" ? "warn" : "error",
    at: runtimeNow(clock),
    stage: envelope.route,
    queue: getWorkerRoute(envelope.route).mainQueue.name,
    messageId: envelope.messageId,
    idempotencyKey: envelope.idempotencyKey,
    correlationId: envelope.correlationId,
    causationId: envelope.causationId,
    traceparent: envelope.traceparent,
    ...(envelope.tracestate === undefined ? {} : {
      tracestate: envelope.tracestate
    }),
    attempt: envelope.attempt.count,
    durationMs,
    outcome: disposition,
    attributes: {
      reason: "processor-exception",
      destination,
      error: classifyTransportError(error),
      ...(settlementError === undefined ? {} : {
        settlementError
      })
    }
  });
}

function elapsedMs(clock: RuntimeClock, startedAtMs: number): number {
  return Math.max(0, clock.now().getTime() - startedAtMs);
}

function classifyTransportError(error: unknown): string {
  if (error instanceof Error && error.name.length > 0) {
    return error.name;
  }

  return "unknown-transport-error";
}

async function publishCarrierWithConfirm(
  channel: ConfirmChannel,
  options: {
    readonly carrier: PayloadCarrier;
    readonly exchange: string;
    readonly routingKey: string;
    readonly confirmTimeoutMs: number;
    readonly retryJitterMs?: number;
  }
): Promise<void> {
  const content = Buffer.from(JSON.stringify(options.carrier), "utf8");
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let settled = false;

  await new Promise<void>((resolve, reject) => {
    const cleanup = (): void => {
      if (timeout !== undefined) {
        clearTimeout(timeout);
      }

      channel.off("return", onReturn);
      channel.off("close", onClose);
      channel.off("error", onChannelError);
      settled = true;
    };
    const fail = (error: Error): void => {
      if (settled) {
        return;
      }

      cleanup();
      reject(error);
    };
    const onReturn = (returned: unknown): void => {
      if (returnedMessageId(returned) === options.carrier.envelope.messageId) {
        fail(new FetcherDefinitePublishError(
          `RabbitMQ publish was returned for ${options.exchange}:${options.routingKey}.`
        ));
      }
    };
    const onClose = (): void => {
      fail(new Error("RabbitMQ channel closed during publish."));
    };
    const onChannelError = (): void => {
      fail(new Error("RabbitMQ channel errored during publish."));
    };

    timeout = setTimeout(() => {
      fail(new Error("RabbitMQ publish confirm timed out."));
    }, options.confirmTimeoutMs);

    channel.on("return", onReturn);
    channel.on("close", onClose);
    channel.on("error", onChannelError);
    try {
      channel.publish(
        options.exchange,
        options.routingKey,
        content,
        publishOptions(options.carrier.envelope, options.retryJitterMs),
        (error: unknown) => {
          if (error !== null && error !== undefined) {
            // amqplib can surface a channel-close error through this same
            // callback while a publish is unconfirmed. Treat it as ambiguous.
            fail(error instanceof Error ? error : new Error("RabbitMQ publish confirm failed."));
            return;
          }

          if (!settled) {
            cleanup();
            resolve();
          }
        }
      );
    } catch (error: unknown) {
      // Once channel.publish has been entered, a synchronous failure can still
      // follow partial frame writes. Conservatively retain the outbox lease.
      fail(error instanceof Error ? error : new Error("RabbitMQ publish failed ambiguously."));
    }
  });
}

function assertPayloadRabbitMqTopology(
  channel: ConfirmChannel,
  routes: readonly WorkerRoute[]
): Promise<void> {
  // The runtime's narrow channel interface and amqplib's channel expose the
  // same topology methods. Their consume-message header typings differ, but
  // topology assertion does not use consumer messages.
  return assertRabbitMqTopology(channel as unknown as RabbitMqConfirmChannel, routes);
}

function decodeCarrier(message: ConsumeMessage): PayloadCarrier {
  const parsed = JSON.parse(message.content.toString("utf8")) as unknown;

  if (isRecord(parsed) && isRecord(parsed.envelope)) {
    return {
      envelope: parsed.envelope as unknown as WorkerMessageEnvelope,
      payload: isRecord(parsed.payload) ? parsed.payload : {}
    };
  }

  if (!isRecord(parsed)) {
    throw new Error("RabbitMQ message body must be a JSON object.");
  }

  return {
    envelope: parsed as unknown as WorkerMessageEnvelope,
    payload: {}
  };
}

function publishOptions(envelope: WorkerMessageEnvelope, retryJitterMs: number | undefined): Options.Publish {
  return {
    persistent: true,
    mandatory: true,
    contentType: WORKER_DELIVERY_BEHAVIOR.contentType,
    contentEncoding: WORKER_DELIVERY_BEHAVIOR.contentEncoding,
    messageId: envelope.messageId,
    correlationId: envelope.correlationId,
    timestamp: Math.floor(Date.parse(envelope.occurredAt) / 1_000),
    headers: {
      schemaId: envelope.schemaId,
      schemaVersion: envelope.schemaVersion,
      route: envelope.route,
      attemptCount: envelope.attempt.count,
      idempotencyKey: envelope.idempotencyKey,
      payloadCarrier: "envelope-plus-payload",
      ...runtimeTraceHeadersFromEnvelope(envelope),
      ...(retryJitterMs === undefined ? {} : {
        retryJitterMs
      })
    }
  };
}

function returnedMessageId(returned: unknown): string | undefined {
  if (!isRecord(returned) || !isRecord(returned.properties)) {
    return undefined;
  }

  const messageId = returned.properties.messageId;

  return typeof messageId === "string" ? messageId : undefined;
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
