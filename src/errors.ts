/**
 * Error hierarchy for the unknown-size TAR writer and its reader.
 *
 * Every writer error carries `boundary`: the number of archive bytes that had
 * already been handed to the sink when the failure occurred. Callers can use
 * it to report the written prefix. A failed or aborted writer never emits the
 * two trailing zero blocks, so `boundary` always points at an unterminated
 * archive.
 */
export class TarError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'TarError';
  }
}

export class TarWriterError extends TarError {
  boundary: number;

  constructor(message: string, options: { boundary?: number; cause?: unknown } = {}) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = 'TarWriterError';
    this.boundary = options.boundary ?? 0;
  }
}

/** The async data source threw (or yielded an invalid chunk). */
export class SourceError extends TarWriterError {
  constructor(cause?: unknown, boundary = 0) {
    super('tar entry source failed', { boundary, cause });
    this.name = 'SourceError';
  }
}

/** Standard strategy: the spooled part of the source exceeded `tempBudget`. */
export class TempBudgetExceededError extends TarWriterError {
  readonly limit: number;
  readonly required: number;

  constructor(limit: number, required: number, boundary = 0) {
    super(
      `temp budget of ${limit} byte(s) exceeded; at least ${required} byte(s) of spool space needed`,
      { boundary },
    );
    this.name = 'TempBudgetExceededError';
    this.limit = limit;
    this.required = required;
  }
}

/** Standard strategy with `compatibility: 'ustar'` needed a PAX extended header. */
export class CompatibilityError extends TarWriterError {
  constructor(reason: string, boundary = 0) {
    super(`entry cannot be written in plain ustar form: ${reason}`, { boundary });
    this.name = 'CompatibilityError';
  }
}

/** The entry's AbortSignal fired before the entry was fully written. */
export class AbortedError extends TarWriterError {
  constructor(boundary = 0) {
    super('tar entry aborted', { boundary });
    this.name = 'AbortedError';
  }
}

/** Writing into the sink rejected (including slow-output backpressure failure). */
export class SinkError extends TarWriterError {
  constructor(cause?: unknown, boundary = 0) {
    super('tar sink write failed', { boundary, cause });
    this.name = 'SinkError';
  }
}

/** API misuse: writer already terminated/stopped, bad arguments, etc. */
export class WriterStateError extends TarWriterError {
  constructor(message: string, boundary = 0) {
    super(message, { boundary });
    this.name = 'WriterStateError';
  }
}

/** Malformed or truncated archive input. */
export class TarParseError extends TarError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'TarParseError';
  }
}

export class TarUnexpectedEofError extends TarParseError {
  constructor(public readonly expected: number, public readonly found: number) {
    super(`unexpected end of archive: needed ${expected} more byte(s), found ${found}`);
    this.name = 'TarUnexpectedEofError';
  }
}

/** Chunked extension: a stream entry is missing chunks or size/digest mismatch. */
export class TarIntegrityError extends TarError {
  constructor(message: string) {
    super(message);
    this.name = 'TarIntegrityError';
  }
}
