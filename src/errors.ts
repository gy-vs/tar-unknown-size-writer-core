/**
 * Error hierarchy for the unknown-size TAR writer / reader.
 *
 * Every write-side failure carries `bytesWritten`: the archive boundary,
 * i.e. the number of bytes that had been successfully flushed to the sink
 * before the failure. A failed stream never receives end-of-archive blocks,
 * so this boundary always points at a truncated (invalid) archive.
 */

export interface TarErrorOptions {
  cause?: unknown;
}

export class TarWriteError extends Error {
  /** Archive boundary: bytes successfully handed to the output sink. */
  readonly bytesWritten: number;

  constructor(message: string, bytesWritten: number, options?: TarErrorOptions) {
    super(message, options);
    this.name = new.target.name;
    this.bytesWritten = bytesWritten;
  }
}

/** The async data source rejected while an entry was being produced. */
export class SourceFailedError extends TarWriteError {
  constructor(bytesWritten: number, cause: unknown, path: string) {
    super(
      `source for entry ${JSON.stringify(path)} failed before the entry could be completed`,
      bytesWritten,
      { cause },
    );
    this.path = path;
  }
  readonly path: string;
}

/** In-memory threshold was exceeded and the on-disk spill budget was too small. */
export class DiskBudgetExceededError extends TarWriteError {
  constructor(
    bytesWritten: number,
    details: { path: string; requiredBytes: number; diskBudget: number },
  ) {
    super(
      `temporary storage budget of ${details.diskBudget} bytes exceeded while prescanning ${JSON.stringify(details.path)} (needed at least ${details.requiredBytes} bytes)`,
      bytesWritten,
    );
    this.path = details.path;
    this.requiredBytes = details.requiredBytes;
    this.diskBudget = details.diskBudget;
  }
  readonly path: string;
  readonly requiredBytes: number;
  readonly diskBudget: number;
}

/** Operation was aborted through an AbortSignal. */
export class TarAbortedError extends TarWriteError {
  constructor(bytesWritten: number, path?: string) {
    super(
      path
        ? `aborted while writing entry ${JSON.stringify(path)}`
        : 'archive operation was aborted',
      bytesWritten,
    );
  }
}

/** The output sink rejected a write (e.g. network stream closed). */
export class SinkWriteError extends TarWriteError {
  constructor(bytesWritten: number, cause: unknown) {
    super('output sink rejected a write', bytesWritten, { cause });
  }
}

/**
 * An entry previously failed (or was aborted): the TAR framing is damaged,
 * no further entries may be added and no valid end-of-archive can be emitted.
 */
export class TarStreamBrokenError extends TarWriteError {
  constructor(bytesWritten: number) {
    super(
      'archive stream is broken because an earlier entry failed or was aborted; no end-of-archive marker was written',
      bytesWritten,
    );
  }
}

/** A TAR byte stream cannot be parsed. */
export class TarParseError extends Error {
  constructor(message: string, options?: TarErrorOptions) {
    super(message, options);
    this.name = new.target.name;
  }
}

/** A chunked-extension final record disagrees with the bytes that were read. */
export class DigestMismatchError extends TarParseError {
  constructor(path: string, expected: string, actual: string) {
    super(
      `digest mismatch for ${JSON.stringify(path)}: final record declares ${expected}, stream hashes to ${actual}`,
    );
    this.path = path;
    this.expected = expected;
    this.actual = actual;
  }
  readonly path: string;
  readonly expected: string;
  readonly actual: string;
}
