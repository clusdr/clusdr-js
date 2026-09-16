/** A Clusdr daemon or SDK failure. */
export class ClusdrError extends Error {
  override readonly name = "ClusdrError";

  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
  }
}

export function wrap(prefix: string, err: unknown): ClusdrError {
  if (err instanceof ClusdrError) {
    return err;
  }
  const msg = err instanceof Error ? err.message : String(err);
  return new ClusdrError(`${prefix}: ${msg}`, { cause: err });
}
