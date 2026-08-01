import type {
  ChannelModel,
  ConfirmChannel,
  ConsumeMessage
} from "amqplib";
import {
  afterEach,
  describe,
  expect,
  it,
  vi
} from "vitest";
import {
  createBufferedRuntimeTelemetrySink,
  type RuntimeMessageProcessingResult
} from "@ramideltoro/nutsnews-worker-runtime";
import { getWorkerRoute } from "@ramideltoro/nutsnews-worker-contracts";

import { isFetcherDefinitePublishError } from "../src/dependencies.js";
import { PayloadRabbitMqTransport } from "../src/rabbitmq-transport.js";
import {
  createMinimalCanonicalizationCommand,
  createMinimalFetchDelivery,
  createMinimalFetchEnvelope
} from "../src/test-doubles.js";

type CloseHandler = () => void;

interface FakeBroker {
  readonly connections: FakeConnection[];
  readonly connect: (url: string) => Promise<ChannelModel>;
}

interface FakeBrokerOptions {
  readonly assertQueue?: (queue: string) => Promise<void>;
  readonly cancel?: (consumerTag: string) => Promise<void>;
  readonly publishBehavior?: "backpressure" | "callback-error" | "channel-close" | "channel-error" | "never-confirm" | "returned" | "sync-error";
}

interface FakeConnection {
  readonly channel: FakeChannel;
  readonly closeCalls: string[];
  emitClose(): void;
  toChannelModel(): ChannelModel;
}

interface FakeChannel {
  readonly assertedExchanges: string[];
  readonly assertedQueues: string[];
  readonly bindings: {
    readonly queue: string;
    readonly exchange: string;
    readonly routingKey: string;
  }[];
  readonly cancelCalls: string[];
  readonly closeCalls: string[];
  readonly consumeQueues: string[];
  readonly prefetchCalls: number[];
  readonly publishes: {
    readonly exchange: string;
    readonly routingKey: string;
    readonly content: Buffer;
  }[];
  readonly acknowledgements: ConsumeMessage[];
  readonly negativeAcknowledgements: {
    readonly message: ConsumeMessage;
    readonly requeue: boolean | undefined;
  }[];
  deliver(queue: string, carrier: unknown): ConsumeMessage;
  toConfirmChannel(): ConfirmChannel;
}

const clock = {
  now: () => new Date("2026-07-26T00:00:00.000Z")
};

describe("RabbitMQ payload transport", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("automatically restores registered fetch consumers after broker close", async () => {
    vi.useFakeTimers();
    const broker = createFakeBroker();
    const telemetry = createBufferedRuntimeTelemetrySink();
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://fetcher:test@example.invalid:5672",
      prefetch: 4,
      clock,
      connect: broker.connect,
      telemetry
    });

    await transport.assertTopology([
      getWorkerRoute("fetch")
    ]);
    await transport.consume("fetch", () => Promise.resolve({
      action: "dlq",
      reason: "not-used"
    }));

    expect(broker.connections).toHaveLength(1);
    expect(broker.connections[0]?.channel.consumeQueues).toEqual([
      "nutsnews.worker.fetch.v1"
    ]);

    broker.connections[0]?.emitClose();
    expect(transport.consumerStatus("fetch")).toMatchObject({
      state: "channel-dropped",
      activeConsumers: 0
    });
    expect(telemetry.events).toContainEqual(expect.objectContaining({
      name: "runtime.broker.consumer_state_changed",
      outcome: "channel-dropped",
      stage: "fetch"
    }));
    await vi.advanceTimersByTimeAsync(1_000);

    expect(broker.connections).toHaveLength(2);
    expect(broker.connections[1]?.channel.consumeQueues).toEqual([
      "nutsnews.worker.fetch.v1"
    ]);
    expect(broker.connections[1]?.channel.prefetchCalls).toEqual([
      4
    ]);
    expect(broker.connections[1]?.channel.assertedQueues).toContain("nutsnews.worker.fetch.v1");
    expect(transport.consumerStatus("fetch")).toMatchObject({
      state: "active",
      activeConsumers: 1
    });

    await transport.close();
  });

  it("coalesces concurrent reconnect paths and restores one tracked consumer", async () => {
    const broker = createFakeBroker();
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://fetcher:test@example.invalid:5672",
      prefetch: 4,
      clock,
      connect: broker.connect
    });

    await transport.assertTopology([
      getWorkerRoute("fetch"),
      getWorkerRoute("canonicalization")
    ]);
    await transport.consume("fetch", () => Promise.resolve({
      action: "dlq",
      reason: "not-used"
    }));
    broker.connections[0]?.emitClose();

    await Promise.all([
      transport.publish(createMinimalCanonicalizationCommand()),
      transport.publish(createMinimalCanonicalizationCommand({
        candidateId: "candidate-world-two",
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3632",
        idempotencyKey: "fetcher:canonicalization:candidate-world-two:fingerprint-v1"
      }))
    ]);

    expect(broker.connections).toHaveLength(2);
    expect(broker.connections[1]?.channel.consumeQueues).toEqual([
      "nutsnews.worker.fetch.v1"
    ]);
    expect(broker.connections[1]?.channel.publishes).toHaveLength(2);
    expect(transport.consumerStatus("fetch")).toMatchObject({
      state: "active",
      activeConsumers: 1
    });

    await transport.close();
  });

  it("asserts exchanges, queues, and bindings for input and output routes", async () => {
    const broker = createFakeBroker();
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://fetcher:test@example.invalid:5672",
      prefetch: 4,
      clock,
      connect: broker.connect
    });
    const fetchRoute = getWorkerRoute("fetch");
    const canonicalizationRoute = getWorkerRoute("canonicalization");

    await transport.assertTopology([
      fetchRoute,
      canonicalizationRoute
    ]);

    const channel = broker.connections[0]?.channel;

    expect(channel?.assertedExchanges).toEqual([
      fetchRoute.exchange,
      fetchRoute.retryExchange,
      fetchRoute.dlqExchange
    ]);
    expect(channel?.assertedQueues).toEqual(expect.arrayContaining([
      fetchRoute.mainQueue.name,
      ...fetchRoute.retryQueues.map((queue) => queue.name),
      fetchRoute.terminalDlq.name,
      canonicalizationRoute.mainQueue.name,
      ...canonicalizationRoute.retryQueues.map((queue) => queue.name),
      canonicalizationRoute.terminalDlq.name
    ]));
    expect(channel?.bindings).toContainEqual({
      queue: canonicalizationRoute.mainQueue.name,
      exchange: canonicalizationRoute.exchange,
      routingKey: canonicalizationRoute.routingKey
    });

    await transport.close();
  });

  it("does not publish through a channel before topology initialization completes", async () => {
    let releaseTopology!: () => void;
    let markTopologyEntered!: () => void;
    const topologyGate = new Promise<void>((resolve) => {
      releaseTopology = resolve;
    });
    const topologyEntered = new Promise<void>((resolve) => {
      markTopologyEntered = resolve;
    });
    const broker = createFakeBroker({
      assertQueue: async () => {
        markTopologyEntered();
        await topologyGate;
      }
    });
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://fetcher:test@example.invalid:5672",
      prefetch: 4,
      clock,
      connect: broker.connect
    });
    const topology = transport.assertTopology([
      getWorkerRoute("canonicalization")
    ]);

    await topologyEntered;
    const publication = transport.publish(createMinimalCanonicalizationCommand());

    await Promise.resolve();
    expect(broker.connections[0]?.channel.publishes).toHaveLength(0);

    releaseTopology();
    await Promise.all([
      topology,
      publication
    ]);
    expect(broker.connections[0]?.channel.publishes).toHaveLength(1);

    await transport.close();
  });

  it("starts socket teardown even when consumer cancellation never settles", async () => {
    const broker = createFakeBroker({
      cancel: () => new Promise<void>(() => undefined)
    });
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://fetcher:test@example.invalid:5672",
      prefetch: 4,
      clock,
      connect: broker.connect
    });

    const envelope = createMinimalFetchEnvelope();

    await transport.consume("fetch", () => Promise.resolve({
      action: "ack",
      reason: "handled",
      envelope
    }));
    await expect(transport.close()).resolves.toBeUndefined();

    const connection = broker.connections[0];

    expect(connection?.channel.cancelCalls).toEqual([
      "consumer-1"
    ]);
    expect(connection?.channel.closeCalls).toEqual([
      "close"
    ]);
    expect(connection?.closeCalls).toEqual([
      "close"
    ]);
  });

  it("clears the drain deadline after in-flight delivery settles", async () => {
    vi.useFakeTimers();
    const broker = createFakeBroker();
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://fetcher:test@example.invalid:5672",
      prefetch: 4,
      clock,
      connect: broker.connect
    });
    let settle: ((result: RuntimeMessageProcessingResult) => void) | undefined;
    const handled = new Promise<RuntimeMessageProcessingResult>((resolve) => {
      settle = resolve;
    });
    const delivery = createMinimalFetchDelivery();
    const envelope = createMinimalFetchEnvelope();

    await transport.consume("fetch", () => handled);
    broker.connections[0]?.channel.deliver("nutsnews.worker.fetch.v1", delivery);
    expect(transport.inFlightDeliveryCount).toBe(1);

    const drain = transport.drain(30_000);

    settle?.({
      action: "ack",
      reason: "handled",
      envelope
    });
    await drain;

    expect(transport.inFlightDeliveryCount).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
    await transport.close();
  });

  it("marks setup failure and mandatory return as definitely unpublished", async () => {
    const command = createMinimalCanonicalizationCommand();
    const setupFailure = new PayloadRabbitMqTransport({
      url: "amqp://fetcher:test@example.invalid:5672",
      prefetch: 4,
      clock,
      connect: () => Promise.reject(new Error("setup unavailable"))
    });
    const returnedBroker = createFakeBroker({
      publishBehavior: "returned"
    });
    const returned = new PayloadRabbitMqTransport({
      url: "amqp://fetcher:test@example.invalid:5672",
      prefetch: 4,
      clock,
      connect: returnedBroker.connect
    });

    expect(isFetcherDefinitePublishError(await rejectedValue(setupFailure.publish(command)))).toBe(true);
    expect(isFetcherDefinitePublishError(await rejectedValue(returned.publish(command)))).toBe(true);

    await returned.close();
  });

  it.each([
    "callback-error",
    "channel-close",
    "channel-error",
    "sync-error"
  ] as const)("keeps %s publish failures ambiguous", async (publishBehavior) => {
    const broker = createFakeBroker({ publishBehavior });
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://fetcher:test@example.invalid:5672",
      prefetch: 4,
      clock,
      connect: broker.connect
    });

    expect(isFetcherDefinitePublishError(await rejectedValue(
      transport.publish(createMinimalCanonicalizationCommand())
    ))).toBe(false);

    await transport.close();
  });

  it("keeps confirm timeout ambiguous and treats false return as backpressure", async () => {
    vi.useFakeTimers();
    const timeoutBroker = createFakeBroker({
      publishBehavior: "never-confirm"
    });
    const timeoutTransport = new PayloadRabbitMqTransport({
      url: "amqp://fetcher:test@example.invalid:5672",
      prefetch: 4,
      clock,
      connect: timeoutBroker.connect
    });
    const timedOut = rejectedValue(timeoutTransport.publish(createMinimalCanonicalizationCommand()));

    await vi.runAllTimersAsync();
    expect(isFetcherDefinitePublishError(await timedOut)).toBe(false);
    await timeoutTransport.close();

    const backpressureBroker = createFakeBroker({
      publishBehavior: "backpressure"
    });
    const backpressureTransport = new PayloadRabbitMqTransport({
      url: "amqp://fetcher:test@example.invalid:5672",
      prefetch: 4,
      clock,
      connect: backpressureBroker.connect
    });

    await expect(backpressureTransport.publish(createMinimalCanonicalizationCommand())).resolves.toMatchObject({
      confirmed: true
    });
    await backpressureTransport.close();
  });

  it.each([
    {
      attempt: 1,
      disposition: "retry",
      exchange: getWorkerRoute("fetch").retryExchange,
      routingKey: getWorkerRoute("fetch").retryQueues[0]?.routingKey
    },
    {
      attempt: 4,
      disposition: "dlq",
      exchange: getWorkerRoute("fetch").dlqExchange,
      routingKey: getWorkerRoute("fetch").terminalDlq.routingKey
    }
  ])("confirms a $disposition transfer for a processor exception and emits exactly one lifecycle disposition", async ({
    attempt,
    disposition,
    exchange,
    routingKey
  }) => {
    if (routingKey === undefined) {
      throw new Error("fetch retry route is missing");
    }

    const broker = createFakeBroker();
    const telemetry = createBufferedRuntimeTelemetrySink();
    const transport = new PayloadRabbitMqTransport({
      url: "amqp://fetcher:test@example.invalid:5672",
      prefetch: 4,
      clock,
      connect: broker.connect,
      telemetry
    });

    await transport.consume("fetch", () => Promise.reject(new Error("state store unavailable")));
    const delivery = createMinimalFetchDelivery(attempt === 1 ? {} : {
      envelope: {
        attempt: {
          count: attempt,
          max: 4,
          firstAttemptAt: "2026-07-23T00:00:00.000Z",
          lastAttemptAt: "2026-07-23T00:20:00.000Z"
        }
      }
    });
    const channel = broker.connections[0]?.channel;

    if (channel === undefined) {
      throw new Error("expected a connected fake channel");
    }

    const message = channel.deliver("nutsnews.worker.fetch.v1", delivery);

    await waitForSettlement(channel);
    expect(channel.publishes).toHaveLength(1);
    expect(channel.publishes[0]).toMatchObject({
      exchange,
      routingKey
    });
    expect(channel.acknowledgements).toEqual([
      message
    ]);
    expect(channel.negativeAcknowledgements).toHaveLength(0);

    const lifecycle = telemetry.events.filter((event) =>
      event.name === "runtime.message.retry" || event.name === "runtime.message.dlq"
    );

    expect(lifecycle).toHaveLength(1);
    expect(lifecycle[0]).toMatchObject({
      name: `runtime.message.${disposition}`,
      stage: "fetch",
      outcome: disposition,
      attributes: {
        reason: "processor-exception",
        error: "Error"
      }
    });

    const published = JSON.parse(channel.publishes[0]?.content.toString("utf8") ?? "{}") as {
      readonly envelope?: {
        readonly attempt?: {
          readonly count?: number;
        };
      };
    };

    expect(published.envelope?.attempt?.count).toBe(disposition === "retry" ? 2 : 4);

    await transport.close();
  });
});

function createFakeBroker(options: FakeBrokerOptions = {}): FakeBroker {
  const connections: FakeConnection[] = [];

  return {
    connections,
    connect: (url: string): Promise<ChannelModel> => {
      expect(url).toBe("amqp://fetcher:test@example.invalid:5672");
      const connection = createFakeConnection(options);
      connections.push(connection);
      return Promise.resolve(connection.toChannelModel());
    }
  };
}

function createFakeConnection(options: FakeBrokerOptions): FakeConnection {
  const channel = createFakeChannel(options);
  const closeHandlers: CloseHandler[] = [];
  const closeCalls: string[] = [];
  const connection = {
    createConfirmChannel(): Promise<ConfirmChannel> {
      return Promise.resolve(channel.toConfirmChannel());
    },
    close(): Promise<void> {
      closeCalls.push("close");
      for (const handler of closeHandlers) {
        handler();
      }

      return Promise.resolve();
    },
    on(event: string, handler: unknown): unknown {
      if (event === "close" && isCloseHandler(handler)) {
        closeHandlers.push(handler);
      }

      return connection;
    }
  };

  return {
    channel,
    closeCalls,
    emitClose(): void {
      for (const handler of closeHandlers) {
        handler();
      }
    },
    toChannelModel(): ChannelModel {
      return connection as unknown as ChannelModel;
    }
  };
}

function createFakeChannel(options: FakeBrokerOptions): FakeChannel {
  const assertedExchanges: string[] = [];
  const assertedQueues: string[] = [];
  const bindings: FakeChannel["bindings"] = [];
  const cancelCalls: string[] = [];
  const closeCalls: string[] = [];
  const consumeQueues: string[] = [];
  const prefetchCalls: number[] = [];
  const publishes: FakeChannel["publishes"] = [];
  const acknowledgements: ConsumeMessage[] = [];
  const negativeAcknowledgements: FakeChannel["negativeAcknowledgements"] = [];
  const consumers = new Map<string, (message: ConsumeMessage | null) => void>();
  const eventHandlers = new Map<string, Set<(...args: unknown[]) => void>>();
  const channel = {
    assertExchange(exchange: string): Promise<void> {
      assertedExchanges.push(exchange);
      return Promise.resolve();
    },
    async assertQueue(queue: string): Promise<void> {
      assertedQueues.push(queue);
      await options.assertQueue?.(queue);
    },
    bindQueue(queue: string, exchange: string, routingKey: string): Promise<void> {
      bindings.push({
        queue,
        exchange,
        routingKey
      });
      return Promise.resolve();
    },
    prefetch(count: number): Promise<void> {
      prefetchCalls.push(count);
      return Promise.resolve();
    },
    consume(queue: string, onMessage: (message: ConsumeMessage | null) => void): Promise<{ readonly consumerTag: string }> {
      consumeQueues.push(queue);
      consumers.set(queue, onMessage);
      return Promise.resolve({
        consumerTag: `consumer-${String(consumeQueues.length)}`
      });
    },
    cancel(consumerTag: string): Promise<void> {
      cancelCalls.push(consumerTag);
      return options.cancel?.(consumerTag) ?? Promise.resolve();
    },
    close(): Promise<void> {
      closeCalls.push("close");
      emitChannelEvent(eventHandlers, "close");

      return Promise.resolve();
    },
    on(event: string, handler: unknown): unknown {
      if (typeof handler === "function") {
        const handlers = eventHandlers.get(event) ?? new Set<(...args: unknown[]) => void>();

        handlers.add(handler as (...args: unknown[]) => void);
        eventHandlers.set(event, handlers);
      }

      return channel;
    },
    off(event: string, handler: unknown): unknown {
      if (typeof handler === "function") {
        eventHandlers.get(event)?.delete(handler as (...args: unknown[]) => void);
      }

      return channel;
    },
    publish(
      exchange: string,
      routingKey: string,
      content: Buffer,
      publishOptions: unknown,
      callback: (error: unknown) => void
    ): boolean {
      publishes.push({
        exchange,
        routingKey,
        content
      });

      if (options.publishBehavior === "sync-error") {
        throw new Error("simulated synchronous publish failure");
      }
      if (options.publishBehavior === "returned") {
        const messageId = isRecordWithMessageId(publishOptions)
          ? publishOptions.messageId
          : undefined;

        queueMicrotask(() => {
          emitChannelEvent(eventHandlers, "return", {
            properties: {
              messageId
            }
          });
        });
        return true;
      }
      if (options.publishBehavior === "callback-error") {
        queueMicrotask(() => {
          callback(new Error("simulated confirm callback failure"));
        });
        return true;
      }
      if (options.publishBehavior === "channel-close") {
        queueMicrotask(() => {
          emitChannelEvent(eventHandlers, "close");
        });
        return true;
      }
      if (options.publishBehavior === "channel-error") {
        queueMicrotask(() => {
          emitChannelEvent(eventHandlers, "error");
        });
        return true;
      }
      if (options.publishBehavior === "never-confirm") {
        return true;
      }
      queueMicrotask(() => {
        callback(null);
      });
      return options.publishBehavior !== "backpressure";
    },
    ack(message: ConsumeMessage): void {
      acknowledgements.push(message);
    },
    nack(message: ConsumeMessage, _allUpTo?: boolean, requeue?: boolean): void {
      negativeAcknowledgements.push({
        message,
        requeue
      });
    }
  };

  return {
    assertedExchanges,
    assertedQueues,
    bindings,
    cancelCalls,
    closeCalls,
    consumeQueues,
    prefetchCalls,
    publishes,
    acknowledgements,
    negativeAcknowledgements,
    deliver(queue: string, carrier: unknown): ConsumeMessage {
      const consumer = consumers.get(queue);

      if (consumer === undefined) {
        throw new Error(`No fake consumer for ${queue}.`);
      }

      const message = {
        content: Buffer.from(JSON.stringify(carrier), "utf8")
      } as ConsumeMessage;

      consumer(message);
      return message;
    },
    toConfirmChannel(): ConfirmChannel {
      return channel as unknown as ConfirmChannel;
    }
  };
}

function emitChannelEvent(
  handlers: ReadonlyMap<string, ReadonlySet<(...args: unknown[]) => void>>,
  event: string,
  ...args: unknown[]
): void {
  for (const handler of handlers.get(event) ?? []) {
    handler(...args);
  }
}

function isRecordWithMessageId(value: unknown): value is { readonly messageId: string } {
  return typeof value === "object"
    && value !== null
    && "messageId" in value
    && typeof value.messageId === "string";
}

async function rejectedValue(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error: unknown) {
    return error;
  }

  throw new Error("Expected operation to reject.");
}

async function waitForSettlement(channel: FakeChannel): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (channel.acknowledgements.length + channel.negativeAcknowledgements.length > 0) {
      return;
    }

    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
  }

  throw new Error("fake RabbitMQ delivery did not settle");
}

function isCloseHandler(handler: unknown): handler is CloseHandler {
  return typeof handler === "function";
}
