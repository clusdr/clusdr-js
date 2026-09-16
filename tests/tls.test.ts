import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { ClusdrError } from "../src/error.ts";
import { clientCredentials, cnFromPem } from "../src/tls.ts";

const NODE_A_CERT = Buffer.from(`-----BEGIN CERTIFICATE-----
MIIBmTCCAT+gAwIBAgIUQcxWr82x7fO3L8Jl+Gdnhg2XsrQwCgYIKoZIzj0EAwIw
IjEPMA0GA1UECgwGY2x1c2RyMQ8wDQYDVQQDDAZub2RlLWEwHhcNMjYwOTEyMDk0
MjE0WhcNMjYwOTEzMDk0MjE0WjAiMQ8wDQYDVQQKDAZjbHVzZHIxDzANBgNVBAMM
Bm5vZGUtYTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABPT6vDCA4Rz1uoJqDDRn
Oek6f1d/DUKHL6EphIpdG7Nt5G8BflHU0EorNZp5mU0T+NHkF88RjUOQdXO9N58d
Q5yjUzBRMB0GA1UdDgQWBBRRcJ28BTllqHsVyBLoyIiEIhvrgTAfBgNVHSMEGDAW
gBRRcJ28BTllqHsVyBLoyIiEIhvrgTAPBgNVHRMBAf8EBTADAQH/MAoGCCqGSM49
BAMCA0gAMEUCIBSuV03b6SI3b0La8HerkVb4jsDiKG711hB8jhV/X6v1AiEAsMIB
70nN7/saTVsIuZDacHOF+kM+mYqPN33lUdIukXk=
-----END CERTIFICATE-----
`);

test("cn from pem", () => {
  assert.equal(cnFromPem(NODE_A_CERT), "node-a");
});

test("missing certs error", () => {
  const dir = mkdtempSync(join(tmpdir(), "clusdr-js-"));
  assert.throws(
    () => clientCredentials({ addr: "127.0.0.1:1", dataDir: dir, insecure: false, holder: "", requestTimeout: 10, readyTimeout: 10, serverName: "" }),
    ClusdrError,
  );
});
