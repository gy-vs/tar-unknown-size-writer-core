/**
 * TAR writer that accepts asynchronous data sources of UNKNOWN length and
 * streams to a sink that may be non-seekable (e.g. a network connection).
 *
 * Two strategies are offered; the caller chooses explicitly:
 *
 *   'standard'  - prescan the source into bounded storage (RAM up to
 *                 memoryBudget, excess into a temp directory up to
 *                 tempBudget), then emit an ordinary ustar/PAX entry. The
 *                 resulting archive is accepted by common TAR readers
 *                 (GNU tar, bsdtar, ...). `compatibility: 'ustar'` forbids
 *                 PAX extended headers entirely.
 *
 *   'chunked'   - stream data immediately as library-defined chunk records
 *                 (typeflag 'C') followed by a summary record carrying the
 *                 final size and SHA-256 digest (typeflag 'S'). No temp
 *                 storage is needed. Only round-trips through THIS library.
 *
 * On cancellation or source failure the writer stops emitting, never writes
 * the terminating zero blocks (so it never claims a valid end-of-archive),
 * and reports the number of archive bytes already handed to the sink.
 */
import { createHash, randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { mkdir, open, rm } from 'node:fs/promises';
import type { FileHandle } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import * as path from 'node:path';
import type { Writable } from 'node:stream';
import {
  BLOCK_SIZE,
  buildHeader,
  buildUstarHeader,
  encodePaxRecords,
  encodeText,
  paddedSize,
  type RawHeaderFields,
} from './blocks.js';
import {
  CHUNK_TYPEFLAG,
  SUMMARY_KEYS,
  SUMMARY_TYPEFLAG,
  chunkName,
  summaryName,
} from './extension.js';
import {
  AbortedError,
  CompatibilityError,
  SinkError,
  SourceError,
  TarWriterError,
  TempBudgetExceededError,
  WriterStateError,
} from './errors.js';

/** Anything that consumes byte chunks and can apply backpressure. */
export type TarSink = (chunk: Uint8Array) => Promise<void> | void;

/** A byte source of unknown length. */
export type ByteSource = AsyncIterable<Uint8Array> | Iterable<Uint8Array>;

export type StandardCompatibility = 'ustar' | 'pax';

export interface TarWriterCommonOptions {
  sink: TarSink;
  /** Directory used for spool files; defaults to os.tmpdir(). */
  tempDir?: string;
}

export interface StandardWriterOptions extends TarWriterCommonOptions {
  strategy: 'standard';
  /** Bytes kept in RAM per entry before spilling into a temp file. */
  memoryBudget: number;
  /** Maximum bytes of temp storage allowed per entry. */
  tempBudget: number;
  /**
   * 'ustar' - plain POSIX ustar only; entries needing a PAX extended header
   *           (very large size or long/UTF-8 names) fail with
   *           CompatibilityError.
   * 'pax'   - emit PAX extended headers when required (portable, accepted by
   *           all modern readers).
   */
  compatibility: StandardCompatibility;
}

export interface ChunkedWriterOptions extends TarWriterCommonOptions {
  strategy: 'chunked';
  /** Size of each chunk payload; defaults to 64 KiB. */
  chunkSize?: number;
}

export type TarWriterOptions = StandardWriterOptions | ChunkedWriterOptions;

export interface AddFileOptions {
  path: string;
  source: ByteSource;
  mode?: number;
  mtime?: number;
  signal?: AbortSignal;
}

export interface AddFileSuccess {
  ok: true;
  path: string;
  size: number;
  sha256: string;
  boundary: number;
}

export interface AddFileFailure {
  ok: false;
  path: string;
  error: TarWriterError;
  /** Archive bytes already handed to the sink; the archive is unterminated. */
  boundary: number;
  canceled: boolean;
}

export type AddFileResult = AddFileSuccess | AddFileFailure;

export interface EndSuccess {
  ok: true;
  /** Size of the complete, valid archive including terminator. */
  size: number;
}

export interface EndFailure {
  ok: false;
  error: TarWriterError;
  boundary: number;
  canceled: boolean;
}

export type EndResult = EndSuccess | EndFailure;

interface EntryMeta {
  mode: number;
  mtime: number;
}

/**
 * Produce a byte sequence backed by a fresh, independent ArrayBuffer.
 * This is required because Node's Buffer overrides slice()/subarray-derived
 * copies in surprising ways (Buffer.prototype.slice aliases the pool), so a
 * plain `.slice()` on an unknown Uint8Array is not a guaranteed copy.
 */
function cloneBytes(bytes: Uint8Array, start = 0, end?: number): Uint8Array {
  const e = end ?? bytes.length;
  const copy = new Uint8Array(e - start);
  copy.set(bytes.subarray(start, e));
  return copy;
}

/**
 * Split a source into fixed-size payload chunks (last chunk may be short).
 * Every yielded chunk is an independent buffer: source iterators (including
 * transpiled async generators) are allowed to reuse the backing storage of
 * a previously yielded chunk after the next `next()` call, so every piece
 * that outlives the iteration step must be copied.
 */
async function* rechunk(source: ByteSource, chunkSize: number, signal?: AbortSignal): AsyncGenerator<Uint8Array> {
  let carry: Uint8Array = new Uint8Array(0);
  for await (const raw0 of asAsync(source, signal)) {
    const raw = raw0 instanceof Uint8Array ? raw0 : new Uint8Array(0);
    let chunk: Uint8Array;
    if (carry.length > 0) {
      chunk = new Uint8Array(carry.length + raw.length);
      chunk.set(carry, 0);
      chunk.set(raw, carry.length);
      carry = new Uint8Array(0);
    } else {
      chunk = raw;
    }
    let offset = 0;
    while (chunk.length - offset >= chunkSize) {
      // Copy: may be a view into the reusable source buffer.
      yield cloneBytes(chunk, offset, offset + chunkSize);
      offset += chunkSize;
    }
    if (offset < chunk.length) carry = cloneBytes(chunk, offset);
  }
  if (carry.length > 0) yield carry;
}

async function* asAsync(source: ByteSource, signal?: AbortSignal): AsyncGenerator<Uint8Array> {
  if (Symbol.asyncIterator in source) {
    for await (const chunk of source as AsyncIterable<Uint8Array>) {
      signal?.throwIfAborted();
      yield chunk;
    }
  } else {
    for (const chunk of source as Iterable<Uint8Array>) {
      signal?.throwIfAborted();
      yield chunk;
    }
  }
}

/* ----------------------------- spooling ----------------------------- */

/**
 * Bounded prescan buffer: bytes stay in RAM up to memoryBudget; anything
 * beyond is appended to a temp file. The temp file must never grow past
 * tempBudget.
 */
class Spool implements AsyncIterable<Uint8Array> {
  readonly ram: Uint8Array[] = [];
  #ramBytes = 0;
  #fh: FileHandle | null = null;
  #fileBytes = 0;
  #filePath: string | null = null;

  constructor(
    private readonly tempDir: string,
    private readonly entryId: string,
    private readonly memoryBudget: number,
    private readonly tempBudget: number,
  ) {}

  get size(): number {
    return this.#ramBytes + this.#fileBytes;
  }

  get filePath(): string | null {
    return this.#filePath;
  }

  async append(chunk: Uint8Array): Promise<void> {
    let remaining = chunk;
    if (this.#ramBytes < this.memoryBudget) {
      const room = this.memoryBudget - this.#ramBytes;
      if (remaining.length <= room) {
        this.ram.push(cloneBytes(remaining));
        this.#ramBytes += remaining.length;
        return;
      }
      this.ram.push(cloneBytes(remaining, 0, room));
      this.#ramBytes += room;
      remaining = remaining.subarray(room);
    }
    // The rest spills to disk. The budget covers the final file size.
    if (this.#fileBytes + remaining.length > this.tempBudget) {
      throw new TempBudgetExceededError(this.tempBudget, this.#fileBytes + remaining.length);
    }
    if (!this.#fh) {
      await mkdir(this.tempDir, { recursive: true });
      this.#filePath = path.join(this.tempDir, `spool-${this.entryId}.tarpart`);
      this.#fh = await open(this.#filePath, 'wx+', 0o600);
    }
    // Always pass the absolute file position explicitly; relying on the
    // shared handle's implicit offset loses bytes across separate writes.
    let written = 0;
    while (written < remaining.length) {
      const res = await this.#fh.write(
        remaining,
        written,
        remaining.length - written,
        this.#fileBytes + written,
      );
      if (res.bytesWritten <= 0) throw new Error('temp file write made no progress');
      written += res.bytesWritten;
    }
    this.#fileBytes += written;
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<Uint8Array> {
    for (const chunk of this.ram) yield chunk;
    if (this.#filePath && this.#fileBytes > 0) {
      // A fresh read stream avoids sharing the append handle's position and
      // any pooled-buffer aliasing.
      const stream = createReadStream(this.#filePath, { highWaterMark: 64 * 1024 });
      let seen = 0;
      for await (const part of stream) {
        seen += part.length;
        if (seen > this.#fileBytes) {
          yield Uint8Array.from(part.subarray(0, part.length - (seen - this.#fileBytes)));
        } else {
          yield Uint8Array.from(part);
        }
      }
    }
  }

  async cleanup(): Promise<void> {
    const fh = this.#fh;
    this.#fh = null;
    if (fh) {
      await fh.close().catch(() => {});
    }
    if (this.#filePath) {
      await rm(this.#filePath, { force: true }).catch(() => {});
      this.#filePath = null;
    }
    this.ram.length = 0;
    this.#ramBytes = 0;
    this.#fileBytes = 0;
  }
}

/* ------------------------------ writer ------------------------------ */

let writerCounter = 0;

export class TarWriter {
  readonly strategy: TarWriterOptions['strategy'];
  readonly #sink: TarSink;
  readonly #tempDir: string;
  readonly #memoryBudget: number;
  readonly #tempBudget: number;
  readonly #chunkSize: number;
  readonly #compatibility: StandardCompatibility;

  #bytesWritten = 0;
  #ended = false;
  #stopped = false;
  #tail: Promise<unknown> = Promise.resolve();

  constructor(options: TarWriterOptions) {
    if (!options || typeof (options as { sink?: unknown }).sink !== 'function') {
      throw new WriterStateError('TarWriter requires a sink function');
    }
    this.strategy = options.strategy;
    if (this.strategy !== 'standard' && this.strategy !== 'chunked') {
      throw new WriterStateError(`unknown strategy: ${String((options as { strategy?: unknown }).strategy)}`);
    }
    this.#sink = options.sink;
    this.#tempDir = (options as TarWriterCommonOptions).tempDir ?? defaultTempDir();
    if (this.strategy === 'standard') {
      validateNonNegativeInteger((options as StandardWriterOptions).memoryBudget, 'memoryBudget');
      validateNonNegativeInteger((options as StandardWriterOptions).tempBudget, 'tempBudget');
      const compat = (options as StandardWriterOptions).compatibility;
      if (compat !== 'ustar' && compat !== 'pax') {
        throw new WriterStateError(`compatibility must be 'ustar' or 'pax', got ${String(compat)}`);
      }
      this.#memoryBudget = (options as StandardWriterOptions).memoryBudget;
      this.#tempBudget = (options as StandardWriterOptions).tempBudget;
      this.#compatibility = compat;
      this.#chunkSize = 0;
    } else {
      this.#memoryBudget = 0;
      this.#tempBudget = 0;
      this.#compatibility = 'pax';
      this.#chunkSize = (options as ChunkedWriterOptions).chunkSize ?? 64 * 1024;
      validatePositiveInteger(this.#chunkSize, 'chunkSize');
      if (this.#chunkSize > 0o77777777777) {
        throw new WriterStateError('chunkSize must fit in a TAR octal size field');
      }
    }
  }

  /** Total bytes successfully handed to the sink so far. */
  get bytesWritten(): number {
    return this.#bytesWritten;
  }

  /** True once an entry failed/aborted or end() ran. */
  get closed(): boolean {
    return this.#ended;
  }

  /** Serialize every operation so sink writes are never interleaved. */
  #enqueue<T>(task: () => Promise<T>): Promise<T> {
    const run = this.#tail.then(task, task);
    // Keep the chain alive regardless of a rejected task.
    this.#tail = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  addFile(options: AddFileOptions): Promise<AddFileResult> {
    return this.#enqueue(() => this.#addFile(options));
  }

  end(signal?: AbortSignal): Promise<EndResult> {
    return this.#enqueue(() => this.#end(signal));
  }

  async #addFile(options: AddFileOptions): Promise<AddFileResult> {
    const entryPath = options.path;
    try {
      if (typeof entryPath !== 'string' || entryPath.length === 0) {
        throw new WriterStateError('entry path must be a non-empty string', this.#bytesWritten);
      }
      if (this.#ended) {
        throw new WriterStateError('writer is already terminated', this.#bytesWritten);
      }
      if (this.#stopped) {
        throw new WriterStateError('writer stopped after a previous failure', this.#bytesWritten);
      }
      options.signal?.throwIfAborted();
      const source = options.source as { [Symbol.asyncIterator]?: unknown; [Symbol.iterator]?: unknown } | null | undefined;
      if (source == null || (typeof source[Symbol.asyncIterator] !== 'function' &&
        typeof source[Symbol.iterator] !== 'function')) {
        throw new WriterStateError('source must be an async or sync iterable of Uint8Array', this.#bytesWritten);
      }

      const mode = options.mode ?? 0o644;
      const mtime = options.mtime ?? 0;
      if (!Number.isInteger(mode) || mode < 0 || mode > 0o7777) {
        throw new WriterStateError(`mode must be an integer in [0, 0o7777], got ${mode}`, this.#bytesWritten);
      }
      if (!Number.isInteger(mtime) || mtime < 0 || mtime > 0o77777777777) {
        throw new WriterStateError(`mtime must be a non-negative integer fitting the TAR field, got ${mtime}`, this.#bytesWritten);
      }
      const meta: EntryMeta = { mode, mtime };

      if (this.strategy === 'standard') {
        return await this.#writeStandard(entryPath, options.source, meta, options.signal);
      }
      return await this.#writeChunked(entryPath, options.source, meta, options.signal);
    } catch (error) {
      return this.#failure(entryPath, error, options.signal);
    }
  }

  #failure(entryPath: string, error: unknown, signal?: AbortSignal): AddFileFailure {
    // Any failure permanently stops the archive; it cannot be terminated,
    // because emitting more entries or zero blocks would claim validity for
    // a stream that may be truncated.
    this.#stopped = true;
    const canceled = !!signal?.aborted || error instanceof AbortedError;
    const tarError = toWriterError(error, this.#bytesWritten);
    return {
      ok: false,
      path: entryPath,
      error: tarError,
      boundary: this.#bytesWritten,
      canceled,
    };
  }

  async #writeStandard(
    entryPath: string,
    source: ByteSource,
    meta: EntryMeta,
    signal: AbortSignal | undefined,
  ): Promise<AddFileSuccess> {
    // Prescan into bounded storage; nothing archive-related is emitted yet,
    // so a prescan failure leaves the archive exactly at its prior boundary.
    const entryId = `${process.pid.toString(36)}-${(writerCounter++).toString(36)}-${randomUUID().slice(0, 8)}`;
    const spool = new Spool(this.#tempDir, entryId, this.#memoryBudget, this.#tempBudget);
    const hash = createHash('sha256');
    let size = 0;
    try {
      for await (const chunk of asAsync(source, signal)) {
        if (!(chunk instanceof Uint8Array)) {
          throw new SourceError(new TypeError('source yielded a non-Uint8Array value'), this.#bytesWritten);
        }
        await spool.append(chunk);
        hash.update(chunk);
        size += chunk.length;
      }
      signal?.throwIfAborted();

      const { headerBlock, paxBlock } = buildStandardBlocks(entryPath, size, meta, this.#compatibility);

      // A PAX extended header must precede the entry header it describes.
      if (paxBlock) await this.#emit(paxBlock, signal);
      await this.#emit(headerBlock, signal);

      const payload = this.#payloadWriter(signal);
      for await (const chunk of spool) {
        signal?.throwIfAborted();
        await payload.write(chunk);
      }
      await payload.flush();
      signal?.throwIfAborted();
    } finally {
      await spool.cleanup();
    }

    return { ok: true, path: entryPath, size, sha256: hash.digest('hex'), boundary: this.#bytesWritten };
  }

  /**
   * Create a per-payload block packer. State must NOT be shared between
   * entries/chunks: a residue from one payload would corrupt the next one.
   */
  #payloadWriter(signal?: AbortSignal): {
    write(chunk: Uint8Array): Promise<void>;
    flush(): Promise<void>;
  } {
    let carry: Uint8Array = new Uint8Array(0);
    const emit = (block: Uint8Array) => this.#emit(block, signal);
    return {
      async write(chunk: Uint8Array): Promise<void> {
        let data = chunk;
        if (carry.length > 0) {
          const merged = new Uint8Array(carry.length + data.length);
          merged.set(carry, 0);
          merged.set(data, carry.length);
          data = merged;
          carry = new Uint8Array(0);
        }
        const aligned = Math.floor(data.length / BLOCK_SIZE) * BLOCK_SIZE;
        if (aligned > 0) await emit(data.subarray(0, aligned));
        // Copy the residue: `data` itself may be a pooled/reused source view.
        if (aligned < data.length) carry = cloneBytes(data, aligned);
      },
      async flush(): Promise<void> {
        if (carry.length === 0) return;
        const block = new Uint8Array(BLOCK_SIZE);
        block.set(carry, 0);
        carry = new Uint8Array(0);
        await emit(block);
      },
    };
  }

  async #writeChunked(
    entryPath: string,
    source: ByteSource,
    meta: EntryMeta,
    signal: AbortSignal | undefined,
  ): Promise<AddFileSuccess> {
    const id = randomUUID().replace(/-/g, '').slice(0, 16);
    const hash = createHash('sha256');
    let size = 0;
    let seq = 0;

    try {
      const payload = this.#payloadWriter(signal);
      for await (const chunk of rechunk(source, this.#chunkSize, signal)) {
        if (!(chunk instanceof Uint8Array)) {
          throw new SourceError(new TypeError('source yielded a non-Uint8Array value'), this.#bytesWritten);
        }
        const headerBlock = buildHeader({
          name: chunkName(id, seq),
          size: chunk.length,
          mode: 0o600,
          mtime: 0,
          typeflag: CHUNK_TYPEFLAG,
        });
        await this.#emit(headerBlock, signal);
        await payload.write(chunk);
        await payload.flush();
        hash.update(chunk);
        size += chunk.length;
        seq++;
      }
      signal?.throwIfAborted();
    } catch (error) {
      // Commit marker is intentionally NOT written: dangling chunks mark the
      // boundary but can never be read back as a complete entry.
      throw toWriterError(error, this.#bytesWritten);
    }

    const sha256 = hash.digest('hex');
    const summaryRecords: Record<string, string> = {
      [SUMMARY_KEYS.path]: entryPath,
      [SUMMARY_KEYS.size]: String(size),
      [SUMMARY_KEYS.sha256]: sha256,
      [SUMMARY_KEYS.mode]: meta.mode.toString(8),
      [SUMMARY_KEYS.mtime]: String(meta.mtime),
    };
    const payload = encodePaxRecords(summaryRecords);
    const summaryHeader = buildHeader({
      name: summaryName(id),
      size: payload.length,
      mode: 0o600,
      mtime: 0,
      typeflag: SUMMARY_TYPEFLAG,
    });
    await this.#emit(summaryHeader, signal);
    const summaryPayload = this.#payloadWriter(signal);
    await summaryPayload.write(payload);
    await summaryPayload.flush();

    return { ok: true, path: entryPath, size, sha256, boundary: this.#bytesWritten };
  }

  async #end(signal?: AbortSignal): Promise<EndResult> {
    try {
      if (this.#ended) throw new WriterStateError('writer is already terminated', this.#bytesWritten);
      if (this.#stopped) {
        // Refuse to claim a valid end-of-archive for a broken stream.
        throw new WriterStateError(
          'cannot terminate an archive that stopped after a failure; use the reported boundary',
          this.#bytesWritten,
        );
      }
      signal?.throwIfAborted();
      await this.#emit(new Uint8Array(BLOCK_SIZE), signal);
      await this.#emit(new Uint8Array(BLOCK_SIZE), signal);
      this.#ended = true;
      return { ok: true, size: this.#bytesWritten };
    } catch (error) {
      this.#stopped = true;
      const tarError = toWriterError(error, this.#bytesWritten);
      return {
        ok: false,
        error: tarError,
        boundary: this.#bytesWritten,
        canceled: !!signal?.aborted || error instanceof AbortedError,
      };
    }
  }

  /**
   * One sink write with backpressure; only advances the boundary on success.
   * The emitted chunk is copied first. Sources and filesystem streams are
   * allowed to reuse/pool the backing ArrayBuffer of yielded chunks, so
   * handing a view directly to a sink that stores it (memory collector) or
   * forwards it asynchronously (network) would let later bytes overwrite
   * earlier archive blocks.
   */
  async #emit(chunk: Uint8Array, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    const owned = cloneBytes(chunk);
    try {
      await this.#sink(owned);
    } catch (error) {
      throw new SinkError(error, this.#bytesWritten);
    }
    if (signal?.aborted) throw new AbortedError(this.#bytesWritten);
    this.#bytesWritten += owned.length;
  }
}

/* ------------------------- standard entry layout ------------------------- */

function buildStandardBlocks(
  entryPath: string,
  size: number,
  meta: EntryMeta,
  compatibility: StandardCompatibility,
): { headerBlock: Uint8Array; paxBlock: Uint8Array | null } {
  const pathBytes = encodeText(entryPath);
  const needsPaxForPath = pathBytes.length > 100;
  const needsPaxForSize = size > 0o77777777777;
  const needsPax = needsPaxForPath || needsPaxForSize;

  if (needsPax && compatibility === 'ustar') {
    if (needsPaxForSize) throw new CompatibilityError(`file size ${size} exceeds the ustar 8 GiB limit`);
    throw new CompatibilityError(`path is ${pathBytes.length} bytes (>100) and PAX extended headers were forbidden`);
  }

  let paxBlock: Uint8Array | null = null;
  let visibleName = entryPath;
  if (needsPax) {
    const records: Record<string, string> = {};
    if (needsPaxForPath) records.path = entryPath;
    if (needsPaxForSize) records.size = String(size);
    const paxPayload = encodePaxRecords(records);
    paxBlock = buildHeader({
      name: `PaxHeaders.${process.pid}/${Math.random().toString(36).slice(2, 10)}`,
      size: paxPayload.length,
      mode: 0o644,
      mtime: 0,
      typeflag: 'x',
    });
    const paxPadded = new Uint8Array(paddedSize(paxPayload.length));
    paxPadded.set(paxPayload, 0);
    const combined = new Uint8Array(BLOCK_SIZE + paxPadded.length);
    combined.set(paxBlock, 0);
    combined.set(paxPadded, BLOCK_SIZE);
    paxBlock = combined;

    // Visible fallback name: must be ASCII (re-encoding to <=100 bytes is
    // guaranteed); the true name is in the PAX path record.
    visibleName = `pax-${Math.random().toString(36).slice(2, 10)}-${writerCounter.toString(36)}`;
  }

  const fields: RawHeaderFields = {
    name: visibleName,
    size: needsPaxForSize ? 0 : size,
    mode: meta.mode,
    mtime: meta.mtime,
    typeflag: '0',
  };
  const headerBlock = needsPax ? buildHeader(fields) : buildUstarHeader(fields);
  return { headerBlock, paxBlock };
}

/* ------------------------------ helpers ------------------------------ */

let cachedTmpDir: string | null = null;
function defaultTempDir(): string {
  if (cachedTmpDir === null) cachedTmpDir = tmpdir();
  return cachedTmpDir;
}

function validateNonNegativeInteger(value: number, name: string): void {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) {
    throw new WriterStateError(`${name} must be a non-negative integer`);
  }
}

function validatePositiveInteger(value: number, name: string): void {
  if (typeof value !== 'number' || !Number.isInteger(value) || value <= 0) {
    throw new WriterStateError(`${name} must be a positive integer`);
  }
}

function toWriterError(error: unknown, boundary: number): TarWriterError {
  if (error instanceof TarWriterError) {
    if (error.boundary === 0 && boundary !== 0) error.boundary = boundary;
    return error;
  }
  // DOM/Node AbortError from signal-throwing iteration or sink awaits.
  if (
    error instanceof Error &&
    ((error as Error).name === 'AbortError' || (error as { code?: string }).code === 'ABORT_ERR')
  ) {
    return new AbortedError(boundary);
  }
  return new SourceError(error, boundary);
}

/**
 * Adapt a Node Writable (network socket, file stream, ...) into a TarSink.
 * Backpressure is applied by awaiting each `write()` callback, and an
 * 'error' event becomes a rejected write.
 */
export function sinkFromWritable(writable: Writable): TarSink {
  return (chunk: Uint8Array) =>
    new Promise<void>((resolve, reject) => {
      const onError = (err: Error) => {
        writable.removeListener('error', onError);
        reject(err);
      };
      writable.once('error', onError);
      writable.write(chunk, (err) => {
        writable.removeListener('error', onError);
        if (err) reject(err);
        else resolve();
      });
    });
}
