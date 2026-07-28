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
import { createBufferedRuntimeTelemetrySink } from "@ramideltoro/nutsnews-worker-runtime";

import { PayloadRabbitMqTransport } from "../src/rabbitmq-transport.js";

type CloseHandler = () => void;

interface FakeBroker {
  readonly connections: FakeConnection[];
  readonly connect: (url: string) => Promise<ChannelModel>;
}

interface FakeConnection {
  readonly channel: FakeChannel;
  emitClose(): void;
  toChannelModel(): ChannelModel;
}

interface FakeChannel {
  readonly consumeQueues: string[];
  readonly prefetchCalls: number[];
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
    expect(transport.consumerStatus("fetch")).toMatchObject({
      state: "active",
      activeConsumers: 1
    });

    await transport.close();
  });
});

function createFakeBroker(): FakeBroker {
  const connections: FakeConnection[] = [];

  return {
    connections,
    connect: (url: string): Promise<ChannelModel> => {
      expect(url).toBe("amqp://fetcher:test@example.invalid:5672");
      const connection = createFakeConnection();
      connections.push(connection);
      return Promise.resolve(connection.toChannelModel());
    }
  };
}

function createFakeConnection(): FakeConnection {
  const channel = createFakeChannel();
  const closeHandlers: CloseHandler[] = [];
  const connection = {
    createConfirmChannel(): Promise<ConfirmChannel> {
      return Promise.resolve(channel.toConfirmChannel());
    },
    close(): Promise<void> {
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

function createFakeChannel(): FakeChannel {
  const consumeQueues: string[] = [];
  const prefetchCalls: number[] = [];
  const closeHandlers: CloseHandler[] = [];
  const channel = {
    prefetch(count: number): Promise<void> {
      prefetchCalls.push(count);
      return Promise.resolve();
    },
    consume(queue: string, onMessage: (message: ConsumeMessage | null) => void): Promise<{ readonly consumerTag: string }> {
      void onMessage;
      consumeQueues.push(queue);
      return Promise.resolve({
        consumerTag: `consumer-${String(consumeQueues.length)}`
      });
    },
    cancel(): Promise<void> {
      return Promise.resolve();
    },
    close(): Promise<void> {
      for (const handler of closeHandlers) {
        handler();
      }

      return Promise.resolve();
    },
    on(event: string, handler: unknown): unknown {
      if (event === "close" && isCloseHandler(handler)) {
        closeHandlers.push(handler);
      }

      return channel;
    }
  };

  return {
    consumeQueues,
    prefetchCalls,
    toConfirmChannel(): ConfirmChannel {
      return channel as unknown as ConfirmChannel;
    }
  };
}

function isCloseHandler(handler: unknown): handler is CloseHandler {
  return typeof handler === "function";
}
