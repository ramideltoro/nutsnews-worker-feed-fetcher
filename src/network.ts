import dns from "node:dns/promises";
import net from "node:net";

import type {
  FetcherDependencyProbe,
  FetcherDnsPolicy,
  FetcherDnsPolicyDecision,
  FetcherHttpClient,
  FetcherHttpRequest,
  FetcherHttpResponse
} from "./dependencies.js";

export class FetcherHttpError extends Error {
  constructor(name: string, message: string) {
    super(message);
    this.name = name;
  }
}

export class NodeFetcherHttpClient implements FetcherHttpClient {
  readonly name: string = "node-fetch-http-client";

  probe(): FetcherDependencyProbe {
    return {
      status: "ok",
      summary: "node fetch HTTP client ready"
    };
  }

  async request(request: FetcherHttpRequest): Promise<FetcherHttpResponse> {
    const startedAt = Date.now();
    let currentUrl = request.url;
    let redirectCount = 0;

    for (;;) {
      const controller = new AbortController();
      let abortReason = "TotalTimeoutError";
      const totalTimeout = setTimeout(() => {
        abortReason = "TotalTimeoutError";
        controller.abort();
      }, request.totalTimeoutMs);
      const connectTimeout = setTimeout(() => {
        abortReason = "ConnectTimeoutError";
        controller.abort();
      }, Math.min(request.connectTimeoutMs, request.totalTimeoutMs));

      try {
        const response = await fetch(currentUrl, {
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
            throw new FetcherHttpError("RedirectLimitError", "Feed redirect limit exceeded.");
          }

          currentUrl = new URL(location, currentUrl);
          redirectCount += 1;
          continue;
        }

        const body = await readResponseBody(response, request.maxResponseBytes, request.readTimeoutMs);

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
      }
    }
  }
}

export class DefaultFetcherDnsPolicy implements FetcherDnsPolicy {
  readonly name: string = "default-dns-policy";

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

    if (url.hostname === "localhost") {
      return {
        allowed: false,
        reason: "blocked-localhost"
      };
    }

    const addresses = await dns.lookup(url.hostname, {
      all: true,
      verbatim: true
    });

    if (addresses.some((address) => isPrivateAddress(address.address))) {
      return {
        allowed: false,
        reason: "blocked-private-address"
      };
    }

    return {
      allowed: true,
      reason: "allowed"
    };
  }
}

function isRedirect(statusCode: number): boolean {
  return statusCode === 301 || statusCode === 302 || statusCode === 303 || statusCode === 307 || statusCode === 308;
}

async function readResponseBody(response: Response, maxBytes: number, readTimeoutMs: number): Promise<Uint8Array> {
  const body = new Uint8Array(await withReadTimeout(response.arrayBuffer(), readTimeoutMs));

  if (body.byteLength > maxBytes) {
    throw new FetcherHttpError("ResponseTooLargeError", "Feed response exceeded the configured byte limit.");
  }

  return body;
}

async function withReadTimeout(operation: Promise<ArrayBuffer>, readTimeoutMs: number): Promise<ArrayBuffer> {
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

function responseHeaders(headers: Headers): Readonly<Record<string, string>> {
  const output: Record<string, string> = {};

  for (const [key, value] of headers.entries()) {
    output[key.toLowerCase()] = value;
  }

  return output;
}

function isPrivateAddress(address: string): boolean {
  if (net.isIPv4(address)) {
    return isPrivateIpv4(address);
  }

  if (net.isIPv6(address)) {
    return isPrivateIpv6(address);
  }

  return true;
}

function isPrivateIpv4(address: string): boolean {
  const octets = address.split(".").map((part) => Number(part));
  const [first, second] = octets;

  if (first === undefined || second === undefined) {
    return true;
  }

  return first === 10 ||
    first === 127 ||
    (first === 172 && second >= 16 && second <= 31) ||
    (first === 192 && second === 168) ||
    (first === 169 && second === 254) ||
    first === 0;
}

function isPrivateIpv6(address: string): boolean {
  const normalized = address.toLowerCase();

  return normalized === "::1" ||
    normalized.startsWith("fc") ||
    normalized.startsWith("fd") ||
    normalized.startsWith("fe80:");
}
