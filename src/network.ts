import dns from "node:dns/promises";
import net from "node:net";
import type { LookupFunction } from "node:net";

import {
  Agent,
  fetch
} from "undici";

import type {
  FetcherDependencyProbe,
  FetcherDnsPolicy,
  FetcherDnsPolicyDecision,
  FetcherDnsPolicyReason,
  FetcherHttpClient,
  FetcherHttpRequest,
  FetcherHttpResponse,
  FetcherResolvedAddress
} from "./dependencies.js";

export class FetcherHttpError extends Error {
  readonly diagnostics: Readonly<Record<string, string | number | boolean>>;

  constructor(
    name: string,
    message: string,
    diagnostics: Readonly<Record<string, string | number | boolean>> = {}
  ) {
    super(message);
    this.name = name;
    this.diagnostics = diagnostics;
  }
}

export class NodeFetcherHttpClient implements FetcherHttpClient {
  readonly name: string = "node-fetch-http-client";
  readonly adapterMode = "production" as const;

  probe(): FetcherDependencyProbe {
    return {
      status: "ok",
      summary: "node fetch HTTP client ready"
    };
  }

  async request(request: FetcherHttpRequest): Promise<FetcherHttpResponse> {
    const startedAt = Date.now();
    const deadlineAt = startedAt + request.totalTimeoutMs;
    let currentUrl = request.url;
    let currentDnsDecision = request.initialDnsDecision;
    let redirectCount = 0;

    for (;;) {
      const remainingTotalTimeoutMs = deadlineAt - Date.now();

      if (remainingTotalTimeoutMs <= 0) {
        throw new FetcherHttpError("TotalTimeoutError", "Feed fetch total timeout exceeded.");
      }

      const dnsDecision = await decisionForRequest(request, currentUrl, currentDnsDecision);
      const dispatcher = dnsDecision === undefined
        ? undefined
        : createPinnedDispatcher(dnsDecision, request.connectTimeoutMs);
      const controller = new AbortController();
      let abortReason = "TotalTimeoutError";
      const totalTimeout = setTimeout(() => {
        abortReason = "TotalTimeoutError";
        controller.abort();
      }, remainingTotalTimeoutMs);
      const connectTimeout = setTimeout(() => {
        abortReason = "ConnectTimeoutError";
        controller.abort();
      }, Math.min(request.connectTimeoutMs, remainingTotalTimeoutMs));
      try {
        const response = await fetch(currentUrl, {
          ...(dispatcher === undefined ? {} : {
            dispatcher
          }),
          headers: {
            "accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, text/rss+xml;q=0.9, */*;q=0.1",
            "user-agent": request.userAgent,
            ...request.headers
          },
          redirect: "manual",
          signal: controller.signal
        });

        clearTimeout(connectTimeout);

        if (isRedirect(response.status)) {
          const location = response.headers.get("location");

          if (location === null) {
            throw new FetcherHttpError("RedirectLocationError", "Feed redirect response did not include a Location header.");
          }

          if (redirectCount >= request.maxRedirects) {
            throw new FetcherHttpError("RedirectLimitError", "Feed redirect limit exceeded.", {
              maxRedirects: request.maxRedirects
            });
          }

          const nextUrl = new URL(location, currentUrl);
          currentDnsDecision = await redirectDecision(request, nextUrl);
          await response.body?.cancel("redirect-policy-evaluated");
          currentUrl = nextUrl;
          redirectCount += 1;
          continue;
        }

        const body = await readResponseBody(response, request.maxResponseBytes, Math.min(request.readTimeoutMs, deadlineAt - Date.now()));

        return {
          statusCode: response.status,
          headers: responseHeaders(response.headers),
          body,
          bodyBytes: body.byteLength,
          finalUrl: currentUrl.toString(),
          durationMs: Math.max(0, Date.now() - startedAt)
        };
      } catch (error: unknown) {
        if (error instanceof Error && error.name === "AbortError") {
          throw new FetcherHttpError(abortReason, abortReason === "ConnectTimeoutError"
            ? "Feed fetch connection timeout exceeded."
            : "Feed fetch total timeout exceeded.");
        }

        throw error;
      } finally {
        clearTimeout(connectTimeout);
        clearTimeout(totalTimeout);
        await dispatcher?.close();
      }
    }
  }
}

export class DefaultFetcherDnsPolicy implements FetcherDnsPolicy {
  readonly name: string = "default-dns-policy";
  readonly adapterMode = "production" as const;

  probe(): FetcherDependencyProbe {
    return {
      status: "ok",
      summary: "default DNS policy ready"
    };
  }

  async evaluate(url: URL): Promise<FetcherDnsPolicyDecision> {
    if (url.protocol !== "https:" && url.protocol !== "http:") {
      return {
        allowed: false,
        reason: "blocked-unsupported-protocol"
      };
    }

    const hostname = normalizeHostname(url.hostname);
    const lowerHostname = hostname.toLowerCase();

    if (lowerHostname === "localhost" || lowerHostname.endsWith(".localhost")) {
      return {
        allowed: false,
        reason: "blocked-localhost"
      };
    }

    if (lowerHostname === "metadata.google.internal" || lowerHostname.endsWith(".metadata.google.internal")) {
      return {
        allowed: false,
        reason: "blocked-metadata-address"
      };
    }

    const literalReason = protectedAddressReason(hostname);

    if (literalReason !== undefined) {
      return {
        allowed: false,
        reason: literalReason
      };
    }

    const addresses = await dns.lookup(hostname, {
      all: true,
      verbatim: true
    });
    const blockedReason = addresses.map((address) => protectedAddressReason(address.address)).find((reason) => reason !== undefined);

    if (blockedReason !== undefined) {
      return {
        allowed: false,
        reason: blockedReason
      };
    }

    return {
      allowed: true,
      reason: "allowed",
      resolvedAddresses: addresses.map((address) => ({
        address: address.address,
        family: address.family === 6 ? 6 : 4
      }))
    };
  }
}

function isRedirect(statusCode: number): boolean {
  return statusCode === 301 || statusCode === 302 || statusCode === 303 || statusCode === 307 || statusCode === 308;
}

async function decisionForRequest(
  request: FetcherHttpRequest,
  currentUrl: URL,
  currentDecision: FetcherDnsPolicyDecision | undefined
): Promise<FetcherDnsPolicyDecision | undefined> {
  if (currentDecision !== undefined) {
    assertAllowedDecision(currentDecision);
    return currentDecision;
  }

  if (request.redirectPolicy === undefined) {
    return undefined;
  }

  const decision = await request.redirectPolicy.evaluate(currentUrl);
  assertAllowedDecision(decision);
  return decision;
}

async function redirectDecision(
  request: FetcherHttpRequest,
  nextUrl: URL
): Promise<FetcherDnsPolicyDecision | undefined> {
  if (request.redirectPolicy === undefined) {
    return undefined;
  }

  const decision = await request.redirectPolicy.evaluate(nextUrl);

  if (!decision.allowed) {
    throw new FetcherHttpError("RedirectBlockedError", "Feed redirect target failed network policy.", {
      policyReason: decision.reason
    });
  }

  assertAllowedDecision(decision);
  return decision;
}

function assertAllowedDecision(decision: FetcherDnsPolicyDecision): void {
  if (!decision.allowed) {
    throw new FetcherHttpError("DnsPolicyBlockedError", "Feed target failed network policy.", {
      policyReason: decision.reason
    });
  }

  if (decision.resolvedAddresses === undefined || decision.resolvedAddresses.length === 0) {
    throw new FetcherHttpError("DnsBindingError", "Allowed DNS policy decision did not include a validated address set.");
  }

  for (const resolved of decision.resolvedAddresses) {
    if (net.isIP(resolved.address) !== resolved.family) {
      throw new FetcherHttpError("DnsBindingError", "Allowed DNS policy decision included an invalid address family.");
    }
  }
}

function createPinnedDispatcher(decision: FetcherDnsPolicyDecision, connectTimeoutMs: number): Agent {
  const addresses = decision.resolvedAddresses;

  if (addresses === undefined || addresses.length === 0) {
    throw new FetcherHttpError("DnsBindingError", "Allowed DNS policy decision did not include a validated address set.");
  }

  return new Agent({
    connectTimeout: connectTimeoutMs,
    connect: {
      lookup: pinnedLookup(addresses)
    }
  });
}

function pinnedLookup(addresses: readonly FetcherResolvedAddress[]): LookupFunction {
  return (_hostname, options, callback) => {
    const family = options.family;
    const candidates = family === 4 || family === 6
      ? addresses.filter((address) => address.family === family)
      : addresses;

    if (candidates.length === 0) {
      const error = new Error("Validated DNS address set has no address for the requested family.") as NodeJS.ErrnoException;
      error.code = "ENOTFOUND";
      callback(error, "", 0);
      return;
    }

    if (options.all === true) {
      callback(null, candidates.map((address) => ({
        address: address.address,
        family: address.family
      })));
      return;
    }

    const selected = candidates[0];

    if (selected === undefined) {
      const error = new Error("Validated DNS address set was empty.") as NodeJS.ErrnoException;
      error.code = "ENOTFOUND";
      callback(error, "", 0);
      return;
    }

    callback(null, selected.address, selected.family);
  };
}

async function readResponseBody(response: Response, maxBytes: number, readTimeoutMs: number): Promise<Uint8Array> {
  const contentLength = parseContentLength(response.headers.get("content-length"));

  if (contentLength !== undefined && contentLength > maxBytes) {
    throw new FetcherHttpError("ResponseTooLargeError", "Feed response declared a body larger than the configured byte limit.", {
      contentLength,
      maxBytes
    });
  }

  if (response.body === null) {
    const body = new Uint8Array(await withReadTimeout(response.arrayBuffer(), readTimeoutMs));

    if (body.byteLength > maxBytes) {
      throw new FetcherHttpError("ResponseTooLargeError", "Feed response exceeded the configured byte limit.", {
        bodyBytes: body.byteLength,
        maxBytes
      });
    }

    return body;
  }

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let bodyBytes = 0;

  try {
    for (;;) {
      const result = await withReadTimeout(reader.read(), readTimeoutMs);

      if (result.done) {
        break;
      }

      const chunk = result.value as unknown;

      if (!(chunk instanceof Uint8Array)) {
        continue;
      }

      bodyBytes += chunk.byteLength;

      if (bodyBytes > maxBytes) {
        await reader.cancel("response-too-large");
        throw new FetcherHttpError("ResponseTooLargeError", "Feed response exceeded the configured byte limit.", {
          bodyBytes,
          maxBytes
        });
      }

      chunks.push(chunk);
    }
  } catch (error: unknown) {
    await cancelReader(reader);
    throw error;
  } finally {
    reader.releaseLock();
  }

  return concatenate(chunks, bodyBytes);
}

async function cancelReader(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<void> {
  try {
    await reader.cancel("feed-response-read-failed");
  } catch {
    // The read path is already failing; cancellation is best-effort cleanup.
  }
}

async function withReadTimeout<T>(operation: Promise<T>, readTimeoutMs: number): Promise<T> {
  if (readTimeoutMs <= 0) {
    throw new FetcherHttpError("ReadTimeoutError", "Feed response read timeout exceeded.");
  }

  let timeout: ReturnType<typeof setTimeout> | undefined;

  try {
    return await Promise.race([
      operation,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => {
          reject(new FetcherHttpError("ReadTimeoutError", "Feed response read timeout exceeded."));
        }, readTimeoutMs);
      })
    ]);
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
}

function parseContentLength(value: string | null): number | undefined {
  if (value === null || value.trim().length === 0) {
    return undefined;
  }

  const parsed = Number(value);

  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

function concatenate(chunks: readonly Uint8Array[], bodyBytes: number): Uint8Array {
  const body = new Uint8Array(bodyBytes);
  let offset = 0;

  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return body;
}

function responseHeaders(headers: Headers): Readonly<Record<string, string>> {
  const output: Record<string, string> = {};

  for (const [key, value] of headers.entries()) {
    output[key.toLowerCase()] = value;
  }

  return output;
}

function normalizeHostname(hostname: string): string {
  if (hostname.startsWith("[") && hostname.endsWith("]")) {
    return hostname.slice(1, -1);
  }

  return hostname;
}

function protectedAddressReason(address: string): FetcherDnsPolicyReason | undefined {
  if (net.isIPv4(address)) {
    return protectedIpv4Reason(address);
  }

  if (net.isIPv6(address)) {
    return protectedIpv6Reason(address);
  }

  return undefined;
}

function protectedIpv4Reason(address: string): FetcherDnsPolicyReason | undefined {
  const octets = address.split(".").map((part) => Number(part));
  const [first, second, third, fourth] = octets;

  if (first === undefined || second === undefined || third === undefined || fourth === undefined) {
    return "blocked-private-address";
  }

  if (first === 127) {
    return "blocked-loopback-address";
  }

  if (first === 169 && second === 254 && third === 169 && fourth === 254) {
    return "blocked-metadata-address";
  }

  if (first === 169 && second === 254) {
    return "blocked-link-local-address";
  }

  if (first === 10 ||
    (first === 172 && second >= 16 && second <= 31) ||
    (first === 192 && second === 168) ||
    (first === 100 && second >= 64 && second <= 127) ||
    (first === 198 && (second === 18 || second === 19)) ||
    first === 0) {
    return "blocked-private-address";
  }

  return undefined;
}

function protectedIpv6Reason(address: string): FetcherDnsPolicyReason | undefined {
  const normalized = address.toLowerCase();

  if (normalized === "::1") {
    return "blocked-loopback-address";
  }

  if (normalized === "fd00:ec2::254") {
    return "blocked-metadata-address";
  }

  if (normalized.startsWith("fe80:")) {
    return "blocked-link-local-address";
  }

  if (normalized.startsWith("fc") || normalized.startsWith("fd")) {
    return "blocked-private-address";
  }

  return undefined;
}
