import {
  describe,
  expect,
  it
} from "vitest";

import {
  LocalBrokerTransport,
  LocalDnsPolicy,
  createMinimalFetchDelivery
} from "../src/test-doubles.js";

describe("fetcher test doubles", () => {
  it("blocks localhost URLs through the local DNS policy", () => {
    const policy = new LocalDnsPolicy();

    expect(policy.evaluate(new URL("http://localhost/feed.xml"))).toEqual({
      allowed: false,
      reason: "blocked-localhost"
    });
    expect(policy.evaluate(new URL("https://feeds.example.test/feed.xml"))).toEqual({
      allowed: true,
      reason: "allowed"
    });
  });

  it("requires a registered local broker consumer before delivery", async () => {
    const broker = new LocalBrokerTransport();

    await broker.connect();

    await expect(broker.deliverFetch(createMinimalFetchDelivery())).rejects.toThrow("No local consumer is registered for fetch.");
  });
});
