/**
 * TAR writer supporting data sources whose final size is unknown.
 *
 * Two caller-selected strategies:
 *
 * - 'standard': prescan each source into bounded temporary storage
 *   (memory up to memoryThreshold, overflow into a temp file capped by
 *   diskBudget) to determine size + sha256, then emit a regular ustar
 *   entry. Output is accepted by stock TAR readers (GNU tar, bsdtar, ...).
 *
 * - 'chunked': emit the library-defined chunked extension straight to the
 *   sink: INITIAL block, data blocks as the source produces them, FINAL
 *   block carrying size + sha256. No seeking, no full-source buffering;
 *   write() awaits naturally give slow sinks backpressure.
 *
 * On cancellation or source failure output stops immediately; the sink
 * never receives end-of-archive zero blocks. The thrown error exposes
 * `bytesWritten`, the exact archive boundary that was successfully
 * flushed. Such an archive is truncated and must not be treated as valid.
 */
import { createHash } from 'node:crypto';
import {
  SinkWriteError,
  SourceFailedError,
  TarAbortedError,
  TarStreamBrokenError,
  TarWriteError,
} from './errors.js';
import { BLOCK_SIZE, entryHeaderBlocks, zeroPadding } from './header.js';
import { SpillBuffer } from './spill-buffer.js';
import {
  CHUNKED_MAGIC,
  DATA_PAYLOAD_SIZE,
  buildFinalBlock,
  buildInitialBlock,
  buildDataBlock,
  type ChunkedFinal,
} from './chunked.js';

export type ByteSource = AsyncIterable<Buffer> | Iterable<Buffer>;

export interface EntryMeta {
  path: string;
  mode?: number;
  mtime?: number;
}

export interface StandardStrategy {
  strategy: 'standard';
  /** Bytes kept in RAM per entry before spilling to a temp file. */
  memoryThreshold: number;
  /** Max bytes allowed in the temp file; Infinity means unbounded. */
  diskBudget: number;
  tmpDir?: string;
}

export interface ChunkedStrategy {
  strategy: 'chunked';
}

export type UnknownSizeStrategy = StandardStrategy | ChunkedStrategy;

export interface AddResult {
  path: string;
  size: number;
  sha256: string;
  /** Bytes the sink had accepted by the end of this entry. */
  bytesWritten: number;
  /** Standard strategy only: where overflow went, if anywhere. */
  spilled?: boolean;
}

export type ByteSink = (chunk: Buffer) => Promise<void> | void;

export interface WriterOptions {
  sink: ByteSink;
  signal?: AbortSignal;
}

const EOA = Buffer.alloc(BLOCK_SIZE * 2);

export class TarUnknownSizeWriter {
  readonly #sink: ByteSink;
  readonly #signal?: AbortSignal;
  #bytesWritten = 0;
  #broken = false;
  #closed = false;
  #entryCount = 0;
  #mode: 'empty' | 'ustar' | 'chunked' = 'empty';
  #chunkedMagicWritten = false;

  constructor(options: WriterOptions) {
    this.#sink = options.sink;
    this.#signal = options.signal;
  }

  /** Archive boundary: bytes successfully flushed to the sink so far. */
  get bytesWritten(): number {
    return this.#bytesWritten;
  }

  get closed(): boolean {
    return this.#closed;
  }

  get broken(): boolean {
    return this.#broken;
  }

  #checkAlive(path?: string): void {
    if (this.#broken) throw new TarStreamBrokenError(this.#bytesWritten);
    if (this.#closed) throw new TarWriteError('writer is already closed', this.#bytesWritten);
    if (this.#signal?.aborted) {
      this.#broken = true;
      throw new TarAbortedError(this.#bytesWritten, path);
    }
  }

  async #write(data: Buffer): Promise<void> {
    try {
      await this.#sink(data);
    } catch (cause) {
      this.#broken = true;
      throw new SinkWriteError(this.#bytesWritten, cause);
    }
    this.#bytesWritten += data.length;
  }

  /**
   * Emit one regular (size-known) entry.
   *
   * In an empty or ustar archive it is a plain ustar entry. After any
   * chunked entry has been started it is encoded with the chunked
   * framing (known size is still verified), so an archive never mixes
   * byte-level formats.
   */
  async addEntry(
    meta: EntryMeta,
    source: ByteSource,
    size: number,
  ): Promise<AddResult> {
    this.#checkAlive(meta.path);
    if (this.#mode === 'chunked') return this.#addChunked(meta, source, size);
    this.#mode = 'ustar';
    try {
      const blocks = entryHeaderBlocks(meta.path, size, {
        mode: meta.mode,
        mtime: meta.mtime,
        paxSerial: this.#entryCount,
      });
      for (const block of blocks) await this.#write(block);

      const hash = createHash('sha256');
      let received = 0;
      for await (const chunk of normalize(source)) {
        this.#checkAlive(meta.path);
        hash.update(chunk);
        received += chunk.length;
        if (received > size) {
          this.#broken = true;
          throw new TarWriteError(
            `entry ${JSON.stringify(meta.path)} source provided more bytes than declared (${received} > ${size})`,
            this.#bytesWritten,
          );
        }
        // Stream to sink in block-aligned fashion, holding back < 512
        // bytes of tail so every write is fully framed.
        await this.#writeBlockAligned(chunk);
      }
      if (received !== size) {
        this.#broken = true;
        throw new TarWriteError(
          `entry ${JSON.stringify(meta.path)} source ended early: expected ${size}, got ${received}`,
          this.#bytesWritten,
        );
      }
      await this.#flushTrailing(size);

      this.#entryCount++;
      return {
        path: meta.path,
        size,
        sha256: hash.digest('hex'),
        bytesWritten: this.#bytesWritten,
      };
    } catch (error) {
      return this.#fail(error, meta.path);
    }
  }

  /** Add an entry whose source length is not known in advance. */
  async addUnknownSize(meta: EntryMeta, source: ByteSource, strategy: UnknownSizeStrategy): Promise<AddResult> {
    this.#checkAlive(meta.path);
    if (strategy.strategy === 'chunked') {
      if (this.#mode === 'ustar') {
        // ustar bytes (possibly only a header) already went out; starting
        // the chunked magic now would produce an unreadable hybrid.
        this.#broken = true;
        throw new TarWriteError(
          'cannot add a chunked entry after plain ustar bytes have been written; use the same strategy for all entries',
          this.#bytesWritten,
        );
      }
      this.#mode = 'chunked';
      return this.#addChunked(meta, source);
    }
    if (this.#mode === 'chunked') {
      this.#broken = true;
      throw new TarWriteError(
        'cannot add a standard ustar entry after chunked-extension bytes have been written',
        this.#bytesWritten,
      );
    }
    this.#mode = 'ustar';
    return this.#addStandard(meta, source, strategy);
  }

  async #addStandard(meta: EntryMeta, source: ByteSource, options: StandardStrategy): Promise<AddResult> {
    let spill: SpillBuffer | null = null;
    try {
      spill = new SpillBuffer({
        memoryThreshold: options.memoryThreshold,
        diskBudget: options.diskBudget,
        tmpDir: options.tmpDir,
      });

      // Phase 1: prescan — buffer to RAM/temp, compute size + digest.
      const hash = createHash('sha256');
      for await (const chunk of normalize(source)) {
        this.#checkAlive(meta.path);
        hash.update(chunk);
        await spill.append(chunk, this.#bytesWritten, meta.path);
      }
      const size = spill.size;
      const sha256 = hash.digest('hex');

      // Phase 2: emit a plain ustar entry from the replay.
      const blocks = entryHeaderBlocks(meta.path, size, {
        mode: meta.mode,
        mtime: meta.mtime,
        paxSerial: this.#entryCount,
      });
      for (const block of blocks) await this.#write(block);

      let written = 0;
      for await (const part of spill.replay()) {
        this.#checkAlive(meta.path);
        await this.#writeBlockAligned(part);
        written += part.length;
      }
      if (written !== size) {
        this.#broken = true;
        throw new TarWriteError(
          `internal error replaying ${JSON.stringify(meta.path)}: expected ${size}, got ${written}`,
          this.#bytesWritten,
        );
      }
      await this.#flushTrailing(size);

      const spilledNow = spill.spilled;
      this.#entryCount++;
      return {
        path: meta.path,
        size,
        sha256,
        bytesWritten: this.#bytesWritten,
        spilled: spilledNow,
      };
    } catch (error) {
      return this.#fail(error, meta.path);
    } finally {
      if (spill) await spill.cleanup();
    }
  }

  /** Leftover alignment state for the straight-to-sink paths. */
  #pending: Buffer = Buffer.alloc(0) as Buffer;

  async #writeBlockAligned(chunk: Buffer): Promise<void> {
    this.#pending = this.#pending.length ? Buffer.concat([this.#pending, chunk]) : chunk;
    if (this.#pending.length >= BLOCK_SIZE) {
      const aligned = Math.floor(this.#pending.length / BLOCK_SIZE) * BLOCK_SIZE;
      await this.#write(this.#pending.subarray(0, aligned));
      this.#pending = this.#pending.subarray(aligned);
    }
  }

  async #flushTrailing(size: number): Promise<void> {
    if (this.#pending.length) {
      const pad = zeroPadding(size);
      await this.#write(pad.length ? Buffer.concat([this.#pending, pad]) : this.#pending);
      this.#pending = Buffer.alloc(0);
    } else {
      const pad = zeroPadding(size);
      if (pad.length) await this.#write(pad);
    }
  }

  async #addChunked(meta: EntryMeta, source: ByteSource, knownSize?: number): Promise<AddResult> {
    try {
      this.#mode = 'chunked';
      if (!this.#chunkedMagicWritten) {
        await this.#write(CHUNKED_MAGIC);
        this.#chunkedMagicWritten = true;
      }
      await this.#write(buildInitialBlock(meta.path));

      const hash = createHash('sha256');
      let size = 0;
      // Pack source bytes into at most 507-byte DATA payloads. Only one
      // partial payload is held at a time; each full record is flushed
      // (awaited) before pulling more from the source -> sink backpressure.
      let carry: Buffer = Buffer.alloc(0) as Buffer;
      for await (const chunk of normalize(source)) {
        this.#checkAlive(meta.path);
        hash.update(chunk);
        size += chunk.length;
        if (knownSize !== undefined && size > knownSize) {
          this.#broken = true;
          throw new TarWriteError(
            `entry ${JSON.stringify(meta.path)} source provided more bytes than declared (${size} > ${knownSize})`,
            this.#bytesWritten,
          );
        }
        carry = carry.length ? Buffer.concat([carry, chunk]) : chunk;
        while (carry.length >= DATA_PAYLOAD_SIZE) {
          await this.#write(buildDataBlock(carry.subarray(0, DATA_PAYLOAD_SIZE)));
          carry = carry.subarray(DATA_PAYLOAD_SIZE);
        }
      }
      if (knownSize !== undefined && size !== knownSize) {
        this.#broken = true;
        throw new TarWriteError(
          `entry ${JSON.stringify(meta.path)} source ended early: expected ${knownSize}, got ${size}`,
          this.#bytesWritten,
        );
      }
      if (carry.length) await this.#write(buildDataBlock(carry));

      const final: ChunkedFinal = { size, sha256: hash.digest('hex') };
      await this.#write(buildFinalBlock(final));

      this.#entryCount++;
      return {
        path: meta.path,
        size: final.size,
        sha256: final.sha256,
        bytesWritten: this.#bytesWritten,
      };
    } catch (error) {
      return this.#fail(error, meta.path);
    }
  }

  /**
   * Finish the archive. Only a clean (non-broken) writer emits the two
   * end-of-archive zero blocks. After a failure or abort this rethrows the
   * terminal TarStreamBrokenError carrying the boundary; the already
   * written bytes remain truncated on the sink.
   */
  async close(): Promise<{ bytesWritten: number; entries: number }> {
    if (this.#closed) return { bytesWritten: this.#bytesWritten, entries: this.#entryCount };
    if (this.#broken) throw new TarStreamBrokenError(this.#bytesWritten);
    if (this.#signal?.aborted) {
      this.#broken = true;
      throw new TarAbortedError(this.#bytesWritten);
    }
    try {
      await this.#write(EOA);
      this.#closed = true;
      return { bytesWritten: this.#bytesWritten, entries: this.#entryCount };
    } catch (error) {
      if (error instanceof TarWriteError) throw error;
      this.#broken = true;
      throw new SinkWriteError(this.#bytesWritten, error);
    }
  }

  /**
   * Mark the stream failed: no EOA will ever be written. Errors we
   * produced already carry the boundary; source rejections are wrapped.
   */
  #fail(error: unknown, path?: string): never {
    this.#broken = true;
    if (this.#signal?.aborted && !(error instanceof TarAbortedError)) {
      throw new TarAbortedError(this.#bytesWritten, path);
    }
    if (error instanceof TarWriteError) throw error;
    throw new SourceFailedError(this.#bytesWritten, error, path ?? '');
  }
}

async function* normalize(source: ByteSource): AsyncIterableIterator<Buffer> {
  if (Symbol.asyncIterator in source) {
    for await (const chunk of source as AsyncIterable<Buffer>) yield toBuffer(chunk);
  } else {
    for (const chunk of source as Iterable<Buffer>) yield toBuffer(chunk);
  }
}

function toBuffer(chunk: unknown): Buffer {
  if (Buffer.isBuffer(chunk)) return chunk;
  if (chunk instanceof Uint8Array) return Buffer.from(chunk);
  if (typeof chunk === 'string') return Buffer.from(chunk);
  throw new TypeError('source must yield Buffer/Uint8Array chunks');
}
