import { ClusdrError } from "./error.js";
import { MAX_WATCH_TOPIC, type WatchFilter } from "./options.js";

export function normalizeWatchFilter(filter: WatchFilter | undefined): [string[], string[]] {
  return [normalizeTopics(filter?.topics), normalizeTypes(filter?.eventTypes)];
}

function normalizeTopics(topics: readonly string[] | undefined): string[] {
  if (!topics || topics.length === 0) {
    return [];
  }
  const out: string[] = [];
  const seen = new Set<string>();
  for (const raw of topics) {
    let t = raw.trim();
    if (t.startsWith("custom.")) {
      t = t.slice(7);
    }
    if (!t) {
      continue;
    }
    if (t.length > MAX_WATCH_TOPIC || ![...t].every(validTopicChar)) {
      throw new ClusdrError(`clusdr: watch topic ${JSON.stringify(raw)} is invalid`);
    }
    if (seen.has(t)) {
      continue;
    }
    seen.add(t);
    out.push(t);
  }
  return out;
}

function normalizeTypes(types: readonly string[] | undefined): string[] {
  if (!types || types.length === 0) {
    return [];
  }
  const out: string[] = [];
  const seen = new Set<string>();
  for (const raw of types) {
    const t = raw.trim();
    if (!t || seen.has(t)) {
      continue;
    }
    seen.add(t);
    out.push(t);
  }
  return out;
}

function validTopicChar(ch: string): boolean {
  return /[A-Za-z0-9._-]/.test(ch);
}
