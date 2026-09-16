import { loadPackageDefinition, type GrpcObject, type ServiceClientConstructor } from "@grpc/grpc-js";
import { loadSync, type Options as LoaderOptions } from "@grpc/proto-loader";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const PROTO_DIR = join(dirname(fileURLToPath(import.meta.url)), "..", "proto");

const FILES = [
  "clusdr/v1alpha1/health.proto",
  "clusdr/v1alpha1/membership.proto",
  "clusdr/v1alpha1/watch.proto",
  "clusdr/v1alpha1/events.proto",
  "clusdr/v1alpha1/locks.proto",
  "clusdr/v1alpha1/leases.proto",
].map((f) => join(PROTO_DIR, f));

export const PROTO_LOADER_OPTIONS: LoaderOptions = {
  keepCase: false,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [PROTO_DIR],
};

export interface MemberMsg {
  id: string;
  address: string;
  status: string;
  leader: boolean;
  role: string;
}

export interface ListMembersResponse {
  members: MemberMsg[];
}

export interface GetLeaderResponse {
  leaderId: string;
  address: string;
}

export interface HealthResponse {
  nodeId: string;
  clusterId: string;
  role: string;
  healthy: boolean;
}

export interface PublishEventRequest {
  topic: string;
  payload: Buffer;
  eventId?: string;
  source?: string;
  relay?: boolean;
}

export interface PublishEventResponse {
  accepted: boolean;
  message: string;
  eventId: string;
  type: string;
}

export interface WatchRequest {
  lastSeq: number;
  topics: string[];
  eventTypes: string[];
}

export interface WatchResponse {
  type: string;
  source: string;
  payload: Buffer;
  timestampUnixMs: number;
  seq: number;
}

export interface LockRequest {
  name: string;
  holder: string;
  ttlMs: number;
}

export interface LockResponse {
  acquired: boolean;
  message: string;
  fencingToken: number;
  holder: string;
  deadlineUnixMs: number;
}

export interface UnlockRequest {
  name: string;
  holder: string;
  fencingToken: number;
}

export interface UnlockResponse {
  released: boolean;
  message: string;
}

export interface RenewLockRequest {
  name: string;
  holder: string;
  fencingToken: number;
  ttlMs: number;
}

export interface RenewLockResponse {
  renewed: boolean;
  message: string;
  fencingToken: number;
  deadlineUnixMs: number;
}

export interface GrantLeaseRequest {
  name: string;
  owner: string;
  ttlMs: number;
}

export interface GrantLeaseResponse {
  granted: boolean;
  message: string;
  fencingToken: number;
  owner: string;
  deadlineUnixMs: number;
}

export interface RenewLeaseRequest {
  name: string;
  owner: string;
  fencingToken: number;
  ttlMs: number;
}

export interface RenewLeaseResponse {
  renewed: boolean;
  message: string;
  fencingToken: number;
  deadlineUnixMs: number;
}

export interface RevokeLeaseRequest {
  name: string;
  owner: string;
  fencingToken: number;
}

export interface RevokeLeaseResponse {
  revoked: boolean;
  message: string;
}

export interface UnaryCall<Req, Res> {
  (req: Req, metadata: unknown, options: { deadline: Date }, callback: (err: Error | null, res?: Res) => void): void;
  (req: Req, options: { deadline: Date }, callback: (err: Error | null, res?: Res) => void): void;
}

export interface ClientReadableStream<T> {
  on(event: "data", listener: (chunk: T) => void): this;
  on(event: "error", listener: (err: Error) => void): this;
  on(event: "end", listener: () => void): this;
  on(event: "status", listener: (status: unknown) => void): this;
  cancel(): void;
}

export interface HealthClient {
  health: UnaryCall<Record<string, never>, HealthResponse>;
  close(): void;
}

export interface MembershipClient {
  listMembers: UnaryCall<Record<string, never>, ListMembersResponse>;
  getLeader: UnaryCall<Record<string, never>, GetLeaderResponse>;
  close(): void;
}

export interface EventClient {
  publishEvent: UnaryCall<PublishEventRequest, PublishEventResponse>;
  close(): void;
}

export interface WatchClient {
  watch(req: WatchRequest): ClientReadableStream<WatchResponse>;
  close(): void;
}

export interface LockClient {
  lock: UnaryCall<LockRequest, LockResponse>;
  tryLock: UnaryCall<LockRequest, LockResponse>;
  unlock: UnaryCall<UnlockRequest, UnlockResponse>;
  renew: UnaryCall<RenewLockRequest, RenewLockResponse>;
  close(): void;
}

export interface LeaseClient {
  grant: UnaryCall<GrantLeaseRequest, GrantLeaseResponse>;
  renew: UnaryCall<RenewLeaseRequest, RenewLeaseResponse>;
  revoke: UnaryCall<RevokeLeaseRequest, RevokeLeaseResponse>;
  close(): void;
}

export interface V1Alpha1Pkg {
  HealthService: ServiceClientConstructor;
  MembershipService: ServiceClientConstructor;
  EventService: ServiceClientConstructor;
  WatchService: ServiceClientConstructor;
  LockService: ServiceClientConstructor;
  LeaseService: ServiceClientConstructor;
}

let cached: V1Alpha1Pkg | undefined;

export function protoRoot(): string {
  return PROTO_DIR;
}

export function loadV1(): V1Alpha1Pkg {
  if (!cached) {
    const def = loadSync(FILES, PROTO_LOADER_OPTIONS);
    const obj = loadPackageDefinition(def) as GrpcObject;
    const ns = obj.clusdr as GrpcObject;
    cached = (ns.v1alpha1 as unknown) as V1Alpha1Pkg;
  }
  return cached;
}

export function unary<Req, Res>(
  fn: UnaryCall<Req, Res>,
  req: Req,
  deadlineMs: number,
): Promise<Res> {
  return new Promise((resolve, reject) => {
    fn(req, { deadline: new Date(Date.now() + deadlineMs) }, (err, res) => {
      if (err) {
        reject(err);
        return;
      }
      if (res === undefined) {
        reject(new Error("clusdr: empty response"));
        return;
      }
      resolve(res);
    });
  });
}
