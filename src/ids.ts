import { createHash, randomUUID } from "node:crypto";

export interface FetcherIdFactory {
  uuid(): string;
}

export class SequenceFetcherIdFactory implements FetcherIdFactory {
  private index = 0;

  constructor(private readonly values: readonly string[]) {}

  uuid(): string {
    const value = this.values[this.index];

    if (value === undefined) {
      throw new Error("SequenceFetcherIdFactory exhausted.");
    }

    this.index += 1;
    return value;
  }
}

export function createCryptoFetcherIdFactory(): FetcherIdFactory {
  return {
    uuid: () => randomUUID()
  };
}

export function stableCandidateId(parts: readonly string[]): string {
  const digest = createHash("sha256")
    .update(parts.join("\u001f"))
    .digest("hex")
    .slice(0, 32);

  return `cand_${digest}`;
}

export function sha256Hex(value: string | Uint8Array): string {
  return createHash("sha256")
    .update(value)
    .digest("hex");
}

export function sha256Base64Url(value: string | Uint8Array): string {
  return createHash("sha256")
    .update(value)
    .digest("base64url");
}
