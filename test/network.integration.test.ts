import http from "node:http";
import type { AddressInfo } from "node:net";
import {
  afterEach,
  describe,
  expect,
  it
} from "vitest";

import {
  DefaultFetcherDnsPolicy,
  FetcherHttpError,
  NodeFetcherHttpClient
} from "../src/network.js";

let activeServer: http.Server | undefined;

afterEach(async () => {
  if (activeServer !== undefined) {
    await closeServer(activeServer);
    activeServer = undefined;
  }
});

describe("NodeFetcherHttpClient", () => {
  it("sends user agent and conditional headers while following bounded redirects", async () => {
    const seenHeaders: http.IncomingHttpHeaders[] = [];

    activeServer = await listen((request, response) => {
      seenHeaders.push(request.headers);

      if (request.url === "/feed.xml") {
        response.writeHead(302, {
          location: "/actual.xml"
        });
        response.end();
        return;
      }

      response.writeHead(200, {
        "content-type": "application/rss+xml"
      });
      response.end("<rss><channel><title>ok</title></channel></rss>");
    });

    const client = new NodeFetcherHttpClient();
    const response = await client.request({
      url: new URL(`${serverUrl(activeServer)}/feed.xml`),
      headers: {
        "if-none-match": "\"etag\""
      },
      userAgent: "test-fetcher",
      connectTimeoutMs: 250,
      readTimeoutMs: 1_000,
      totalTimeoutMs: 5_000,
      maxRedirects: 3,
      maxResponseBytes: 1_024
    });

    expect(response).toMatchObject({
      statusCode: 200,
      finalUrl: `${serverUrl(activeServer)}/actual.xml`
    });
    expect(seenHeaders[0]).toMatchObject({
      "user-agent": "test-fetcher",
      "if-none-match": "\"etag\""
    });
  });

  it("rejects responses larger than the configured byte limit", async () => {
    activeServer = await listen((_request, response) => {
      response.writeHead(200, {
        "content-type": "application/rss+xml"
      });
      response.end("x".repeat(64));
    });

    const client = new NodeFetcherHttpClient();

    await expect(client.request({
      url: new URL(`${serverUrl(activeServer)}/feed.xml`),
      userAgent: "test-fetcher",
      connectTimeoutMs: 250,
      readTimeoutMs: 1_000,
      totalTimeoutMs: 5_000,
      maxRedirects: 0,
      maxResponseBytes: 8
    })).rejects.toMatchObject({
      name: "ResponseTooLargeError"
    } satisfies Partial<FetcherHttpError>);
  });

  it("fails slow response bodies with a read timeout", async () => {
    activeServer = await listen((_request, response) => {
      response.writeHead(200, {
        "content-type": "application/rss+xml"
      });
      response.flushHeaders();
      setTimeout(() => {
        response.end("<rss><channel><title>late</title></channel></rss>");
      }, 200);
    });

    const client = new NodeFetcherHttpClient();

    await expect(client.request({
      url: new URL(`${serverUrl(activeServer)}/feed.xml`),
      userAgent: "test-fetcher",
      connectTimeoutMs: 250,
      readTimeoutMs: 25,
      totalTimeoutMs: 5_000,
      maxRedirects: 0,
      maxResponseBytes: 1_024
    })).rejects.toMatchObject({
      name: "ReadTimeoutError"
    } satisfies Partial<FetcherHttpError>);
  });

  it("does not follow redirects to blocked destinations", async () => {
    let protectedHits = 0;
    const protectedServer = await listen((_request, response) => {
      protectedHits += 1;
      response.writeHead(200, {
        "content-type": "application/rss+xml"
      });
      response.end("<rss><channel><title>protected</title></channel></rss>");
    });

    try {
      activeServer = await listen((_request, response) => {
        response.writeHead(302, {
          location: `${serverUrl(protectedServer)}/metadata.xml`
        });
        response.end();
      });

      const client = new NodeFetcherHttpClient();

      await expect(client.request({
        url: new URL(`${serverUrl(activeServer)}/feed.xml`),
        redirectPolicy: {
          evaluate: (url) => url.origin === serverUrl(protectedServer)
            ? {
                allowed: false,
                reason: "blocked-loopback-address"
              }
            : {
                allowed: true,
                reason: "allowed"
              }
        },
        userAgent: "test-fetcher",
        connectTimeoutMs: 250,
        readTimeoutMs: 1_000,
        totalTimeoutMs: 5_000,
        maxRedirects: 3,
        maxResponseBytes: 1_024
      })).rejects.toMatchObject({
        name: "RedirectBlockedError"
      } satisfies Partial<FetcherHttpError>);

      expect(protectedHits).toBe(0);
    } finally {
      await closeServer(protectedServer);
    }
  });
});

describe("DefaultFetcherDnsPolicy", () => {
  it("classifies protected literal destinations before DNS lookup", async () => {
    const policy = new DefaultFetcherDnsPolicy();

    await expect(policy.evaluate(new URL("ftp://feeds.example.test/feed.xml"))).resolves.toMatchObject({
      allowed: false,
      reason: "blocked-unsupported-protocol"
    });
    await expect(policy.evaluate(new URL("http://localhost/feed.xml"))).resolves.toMatchObject({
      allowed: false,
      reason: "blocked-localhost"
    });
    await expect(policy.evaluate(new URL("http://127.0.0.1/feed.xml"))).resolves.toMatchObject({
      allowed: false,
      reason: "blocked-loopback-address"
    });
    await expect(policy.evaluate(new URL("http://169.254.169.254/latest/meta-data"))).resolves.toMatchObject({
      allowed: false,
      reason: "blocked-metadata-address"
    });
    await expect(policy.evaluate(new URL("http://192.168.1.1/feed.xml"))).resolves.toMatchObject({
      allowed: false,
      reason: "blocked-private-address"
    });
    await expect(policy.evaluate(new URL("http://[fe80::1]/feed.xml"))).resolves.toMatchObject({
      allowed: false,
      reason: "blocked-link-local-address"
    });
  });
});

function listen(handler: http.RequestListener): Promise<http.Server> {
  const server = http.createServer(handler);

  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve(server);
    });
  });
}

function serverUrl(server: http.Server): string {
  const address = server.address();

  if (!isAddressInfo(address)) {
    throw new Error("server is not listening on a TCP address");
  }

  return `http://127.0.0.1:${String(address.port)}`;
}

function isAddressInfo(address: string | AddressInfo | null): address is AddressInfo {
  return typeof address === "object" && address !== null;
}

function closeServer(server: http.Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error !== undefined) {
        reject(error);
        return;
      }

      resolve();
    });
  });
}
