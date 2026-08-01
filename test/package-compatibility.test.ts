import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import { getRuntimePackageMetadata } from "@ramideltoro/nutsnews-worker-runtime";
import { describe, expect, it } from "vitest";

import {
  SUPPORTED_CONTRACTS_PACKAGE_VERSION,
  SUPPORTED_RUNTIME_PACKAGE_VERSION
} from "../src/index.js";

describe("package compatibility", () => {
  it("accepts the installed worker runtime release", () => {
    expect(getContractPackageMetadata().packageVersion).toBe(SUPPORTED_CONTRACTS_PACKAGE_VERSION);
    expect(getRuntimePackageMetadata().packageVersion).toBe(SUPPORTED_RUNTIME_PACKAGE_VERSION);
    expect(SUPPORTED_CONTRACTS_PACKAGE_VERSION).toBe("1.0.0");
    expect(SUPPORTED_RUNTIME_PACKAGE_VERSION).toBe("1.0.0");
  });
});
