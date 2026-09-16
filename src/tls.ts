import { credentials, type ChannelCredentials, type ChannelOptions } from "@grpc/grpc-js";
import { X509Certificate } from "node:crypto";
import { readFileSync, statSync } from "node:fs";
import { join } from "node:path";

import { ClusdrError } from "./error.js";
import { CA_FILE, CERT_FILE, KEY_FILE, envDataDir, envInsecure, envServerName, type Options } from "./options.js";

export interface TlsChannel {
  credentials: ChannelCredentials;
  options: ChannelOptions;
}

export function channelSetup(opts: Options): TlsChannel {
  if (opts.insecure || (!opts.insecure && envInsecure() && !opts.dataDir)) {
    return { credentials: credentials.createInsecure(), options: {} };
  }
  const [creds, serverName] = clientCredentials(opts);
  const options: ChannelOptions = {};
  if (serverName) {
    options["grpc.ssl_target_name_override"] = serverName;
    options["grpc.default_authority"] = serverName;
  }
  return { credentials: creds, options };
}

export function clientCredentials(opts: Options): [ChannelCredentials, string] {
  const directory = opts.dataDir || envDataDir();
  const files = loadFiles(directory);
  if (!files) {
    throw new ClusdrError(
      `clusdr: TLS enabled but ${CA_FILE}/${CERT_FILE}/${KEY_FILE} missing in ${directory}; set CLUSDR_TLS=disabled or pass insecure: true`,
    );
  }
  const [ca, cert, key] = files;
  const creds = credentials.createSsl(ca, key, cert);
  const name = opts.serverName || envServerName() || cnFromPem(cert);
  if (!name) {
    throw new ClusdrError(
      "clusdr: TLS hostname unknown; set CLUSDR_TLS_SERVER_NAME or serverName to the peer node id",
    );
  }
  return [creds, name];
}

export function cnFromPem(pem: Buffer): string {
  try {
    const x509 = new X509Certificate(pem);
    const match = /(?:^|[\n,])\s*CN=([^,\n]+)/.exec(x509.subject);
    return match?.[1]?.trim() ?? "";
  } catch {
    return "";
  }
}

function loadFiles(directory: string): [Buffer, Buffer, Buffer] | undefined {
  const ca = join(directory, CA_FILE);
  const cert = join(directory, CERT_FILE);
  const key = join(directory, KEY_FILE);
  if (!isFile(ca) || !isFile(cert) || !isFile(key)) {
    return undefined;
  }
  return [readFileSync(ca), readFileSync(cert), readFileSync(key)];
}

function isFile(path: string): boolean {
  try {
    return statSync(path).isFile();
  } catch {
    return false;
  }
}
