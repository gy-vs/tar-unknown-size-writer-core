/**
 * Streaming TAR reader.
 *
 * Reads ordinary ustar (POSIX) archives and PAX extended archives, including
 * GNU long-name ('L') records produced by GNU tar. It also reassembles the
 * library-defined chunked extension from ./extension.ts, verifying that the
 * final size and SHA-256 digest in the summary record match the chunks.
 *
 * Payload bytes are streamed (never fully buffered) for ordinary entries.
 * Chunked entries are assembled after their summary record arrives and are
 * therefore necessarily materialized until verified.
 */
import { createHash } from 'node:crypto';
import {
  BLOCK_SIZE,
  joinPrefixName,
  isZeroBlock,
  parseHeader,
  parsePaxRecords,
  type PaxRecordMap,
  type ParsedHeader,
} from './blocks.js';
import {
  SUMMARY_TYPEFLAG,
  CHUNK_TYPEFLAG,
  parseChunkName,
  parseSummaryName,
  parseSummaryPayload,
  type SummaryInfo,
} from './extension.js';
import { TarIntegrityError, TarParseError, TarUnexpectedEofError } from './errors.js';

export type ByteInput = AsyncIterable<Uint8Array> | Iterable<Uint8Array>;

export type TarEntryType = 'file' | 'directory' | 'link' | 'unknown';

export interface TarEntryMeta {
  path: string;
  size: number;
  mode: number;
  mtime: number;
  type: TarEntryType;
  typeflag: string;
  linkname?: string;
  checksumOk: boolean;
}

export interface FileEntry extends TarEntryMeta {
  type: 'file';
  /** True for entries reassembled from the chunked extension. */
  chunked: boolean;
  /** SHA-256 hex digest for verified chunked entries. */
  sha256?: string;
  /**
   * Consume the payload. May be called at most once; payload not consumed
   * before requesting the next entry is discarded automatically.
   */
  data(): AsyncIterable<Uint8Array>;
}

export interface SimpleEntry extends TarEntryMeta {
  type: 'directory' | 'link' | 'unknown';
}

export type TarEntry = FileEntry | SimpleEntry;

/* ------------------------------ feeder ------------------------------ */

class Feeder {
  #chunks: Uint8Array[] = [];
  #buffered = 0;
  #done = false;
  #ready: Promise<void>;
  #signalReady!: () => void;
  #streamError: unknown = null;

  constructor(input: ByteInput) {
    this.#ready = this.#makeReady();
    this.#pump(input);
  }

  #makeReady(): Promise<void> {
    return new Promise((resolve) => {
      this.#signalReady = resolve;
    });
  }

  #pump(input: ByteInput): void {
    const run = (async () => {
      try {
        if (Symbol.asyncIterator in input) {
          for await (const chunk of input as AsyncIterable<Uint8Array>) this.#push(chunk);
        } else {
          for (const chunk of input as Iterable<Uint8Array>) this.#push(chunk);
        }
      } catch (error) {
        this.#streamError = error;
      } finally {
        this.#done = true;
        this.#signalReady();
      }
    })();
    void run;
  }

  /** Wake one waiter and re-arm before any new data can arrive. */
  #notify(): void {
    const signal = this.#signalReady;
    this.#ready = this.#makeReady();
    signal();
  }

  #push(chunk: Uint8Array): void {
    if (!(chunk instanceof Uint8Array)) {
      this.#streamError = new TypeError('input yielded a non-Uint8Array value');
      this.#notify();
      return;
    }
    if (chunk.length > 0) {
      this.#chunks.push(chunk);
      this.#buffered += chunk.length;
    }
    this.#notify();
  }

  /** Wait until data may be available (spurious wakeups are handled by callers). */
  async #waitForData(): Promise<void> {
    const ready = this.#ready;
    await ready;
  }

  #throwIfErrored(): void {
    if (this.#streamError !== null) {
      const err = this.#streamError;
      this.#streamError = null;
      throw err;
    }
  }

  /** Read exactly `n` bytes; returns null only when nothing is buffered at EOF. */
  async readExact(n: number): Promise<Uint8Array | null> {
    const out = new Uint8Array(n);
    let offset = 0;
    while (offset < n) {
      if (this.#chunks.length === 0) {
        if (offset > 0) {
          if (this.#done) {
            this.#throwIfErrored();
            throw new TarUnexpectedEofError(n - offset, 0);
          }
        } else if (this.#done) {
          this.#throwIfErrored();
          return null;
        }
        await this.#waitForData();
        this.#throwIfErrored();
        continue;
      }
      const head = this.#chunks[0];
      const need = n - offset;
      if (head.length <= need) {
        out.set(head, offset);
        offset += head.length;
        this.#buffered -= head.length;
        this.#chunks.shift();
      } else {
        out.set(head.subarray(0, need), offset);
        this.#chunks[0] = head.subarray(need);
        this.#buffered -= need;
        offset = n;
      }
    }
    return out;
  }

  /** Take up to `max` bytes without blocking (at least 1 byte when available). */
  #takeSome(max: number): Uint8Array | null {
    if (this.#chunks.length === 0) return null;
    const head = this.#chunks[0];
    if (head.length <= max) {
      this.#buffered -= head.length;
      this.#chunks.shift();
      return head;
    }
    const part = head.subarray(0, max).slice();
    this.#chunks[0] = head.subarray(max);
    this.#buffered -= max;
    return part;
  }

  /**
   * Stream exactly `size` payload bytes. Padding is NOT consumed here; the
   * main loop calls skipPayloadTail() afterwards. `delivered.count` tracks
   * bytes actually yielded even if the consumer abandons the generator, so
   * the tail skip never relies on resuming a half-used async generator.
   */
  streamPayload(size: number): {
    stream: AsyncIterable<Uint8Array>;
    delivered: { count: number };
  } {
    const feeder = this;
    const delivered = { count: 0 };

    async function* generate(): AsyncGenerator<Uint8Array> {
      let remaining = size;
      while (remaining > 0) {
        let piece = feeder.#takeSome(remaining);
        if (piece === null) {
          if (feeder.#done) {
            feeder.#throwIfErrored();
            throw new TarUnexpectedEofError(remaining, 0);
          }
          await feeder.#waitForData();
          feeder.#throwIfErrored();
          piece = feeder.#takeSome(remaining);
          if (piece === null) continue;
        }
        remaining -= piece.length;
        delivered.count += piece.length;
        yield piece;
      }
    }
    return { stream: { [Symbol.asyncIterator]: generate }, delivered };
  }

  /** Consume the unread remainder of a payload plus its padding. */
  async skipPayloadTail(size: number, deliveredCount: number): Promise<void> {
    const pad = (BLOCK_SIZE - (size % BLOCK_SIZE)) % BLOCK_SIZE;
    await this.skip(size - deliveredCount + pad);
  }

  /** Discard exactly `n` bytes, waiting for the input as needed. */
  async skip(n: number): Promise<void> {
    let remaining = n;
    while (remaining > 0) {
      const piece = this.#takeSome(remaining);
      if (piece === null) {
        if (this.#done) {
          this.#throwIfErrored();
          throw new TarUnexpectedEofError(remaining, 0);
        }
        await this.#waitForData();
        this.#throwIfErrored();
        continue;
      }
      remaining -= piece.length;
    }
  }

  /** Read a full TAR payload of `size` bytes and consume its padding. */
  async readPaddedPayload(size: number): Promise<Uint8Array> {
    const data = await this.readExact(size);
    if (data === null) throw new TarUnexpectedEofError(size, 0);
    const rem = size % BLOCK_SIZE;
    if (rem !== 0) await this.skip(BLOCK_SIZE - rem);
    return data;
  }
}

/* ------------------------------ parsing ------------------------------ */

interface PendingChunkStream {
  chunks: Uint8Array[];
  size: number;
  receivedSeqs: Set<number>;
}

function classify(typeflag: string): TarEntryType {
  if (typeflag === '0' || typeflag === ' ' || typeflag === '7') return 'file';
  if (typeflag === '5') return 'directory';
  if (typeflag === '1' || typeflag === '2') return 'link';
  return 'unknown';
}

function mergeMaps(base: PaxRecordMap | null, overlay: PaxRecordMap): PaxRecordMap {
  return new Map([...(base ?? []) as PaxRecordMap, ...overlay]);
}

/**
 * Iterate logical entries of a TAR archive. Throws TarParseError /
 * TarIntegrityError / TarUnexpectedEofError on malformed input.
 */
export async function* readTar(input: ByteInput): AsyncGenerator<TarEntry> {
  // A bare Uint8Array iterates as a sequence of numbers; treat it as one
  // binary chunk to make the common readTar(bytes) call work correctly.
  const normalized: ByteInput = input instanceof Uint8Array ? [input] : input;
  const feeder = new Feeder(normalized);
  let globalPax: PaxRecordMap = new Map();
  const pending = new Map<string, PendingChunkStream>();

  let activePayload: {
    stream: AsyncIterable<Uint8Array>;
    size: number;
    delivered: { count: number };
  } | null = null;

  for (;;) {
    // Skip the remainder of the previous entry plus its padding, whether or
    // not the caller consumed its payload, so the next header is aligned.
    if (activePayload !== null) {
      await feeder.skipPayloadTail(activePayload.size, activePayload.delivered.count);
      activePayload = null;
    }

    const block = await feeder.readExact(BLOCK_SIZE);
    if (block === null) break; // clean EOF, any number of trailing zero blocks
    if (isZeroBlock(block)) {
      // One zero block means end of archive; the second is conventional.
      await consumeTrailer(feeder);
      break;
    }

    const header = parseHeader(block);
    if (!header.checksumOk) {
      throw new TarParseError(`bad header checksum for ${JSON.stringify(header.name)}`);
    }

    let localPax: PaxRecordMap | null = null;
    let gnuLongName: string | null = null;
    let gnuLongLink: string | null = null;

    // Metadata headers carry a payload and precede the entry they describe.
    while (header.typeflag === 'x' || header.typeflag === 'g' || header.typeflag === 'L' || header.typeflag === 'K') {
      const payload = await feeder.readPaddedPayload(header.size);

      if (header.typeflag === 'x' || header.typeflag === 'g') {
        const records = parsePaxRecords(payload);
        if (header.typeflag === 'g') {
          globalPax = mergeMaps(globalPax, records);
        } else {
          localPax = mergeMaps(localPax, records);
        }
      } else if (header.typeflag === 'L') {
        gnuLongName = stripTrailingNull(payload);
      } else {
        gnuLongLink = stripTrailingNull(payload);
      }

      const next = await feeder.readExact(BLOCK_SIZE);
      if (next === null) throw new TarUnexpectedEofError(BLOCK_SIZE, 0);
      if (isZeroBlock(next)) {
        await consumeTrailer(feeder);
        return;
      }
      const following = parseHeader(next);
      if (!following.checksumOk) throw new TarParseError('bad header checksum after metadata record');
      Object.assign(header, following);
    }

    // --- library-defined chunked extension ---
    const chunkRef = parseChunkName(header.name);
    if (header.typeflag === CHUNK_TYPEFLAG && chunkRef) {
      const data = await feeder.readPaddedPayload(header.size);

      let stream = pending.get(chunkRef.id);
      if (!stream) {
        stream = { chunks: [], size: 0, receivedSeqs: new Set() };
        pending.set(chunkRef.id, stream);
      }
      if (stream.receivedSeqs.has(chunkRef.seq)) {
        throw new TarIntegrityError(`duplicate chunk sequence ${chunkRef.seq} in stream ${chunkRef.id}`);
      }
      stream.receivedSeqs.add(chunkRef.seq);
      stream.chunks[chunkRef.seq] = data;
      stream.size += data.length;
      continue;
    }

    const summaryRef = parseSummaryName(header.name);
    if (header.typeflag === SUMMARY_TYPEFLAG && summaryRef) {
      const payload = await feeder.readPaddedPayload(header.size);

      const info = parseSummaryPayload(payload);
      // An empty source has no chunk records, so register an empty stream.
      if (!pending.has(summaryRef.id) && info.size === 0) {
        pending.set(summaryRef.id, { chunks: [], size: 0, receivedSeqs: new Set() });
      }
      yield* resolveSummary(summaryRef.id, info, pending);
      pending.delete(summaryRef.id);
      continue;
    }

    // --- ordinary entry ---
    const meta = resolveMeta(header, globalPax, localPax, gnuLongName, gnuLongLink);

    if (meta.type !== 'file') {
      await feeder.skip(padded(header.size));
      yield meta as SimpleEntry;
      continue;
    }

    const { stream: payload, delivered } = feeder.streamPayload(header.size);
    activePayload = { stream: payload, size: header.size, delivered };
    const entryDataConsumed = new WeakSet<FileEntry>();
    const entry: FileEntry = {
      ...meta,
      type: 'file' as const,
      chunked: false,
      data(): AsyncIterable<Uint8Array> {
        if (entryDataConsumed.has(entry)) {
          throw new TarParseError('entry data() can only be called once');
        }
        entryDataConsumed.add(entry);
        return payload;
      },
    };
    yield entry;
  }

  // A complete archive must not leave chunks without summary records.
  if (pending.size > 0) {
    throw new TarIntegrityError(
      `archive ended with ${pending.size} unfinished chunked stream(s) missing summary records`,
    );
  }
}

async function* resolveSummary(
  id: string,
  info: SummaryInfo,
  pending: Map<string, PendingChunkStream>,
): AsyncGenerator<FileEntry> {
  const stream = pending.get(id);
  if (!stream) {
    throw new TarIntegrityError(`summary record ${id} has no preceding chunks (unknown stream id)`);
  }
  if (stream.chunks.length !== stream.receivedSeqs.size) {
    throw new TarIntegrityError(`chunked stream ${id} has gaps in its sequence numbers`);
  }
  for (let i = 0; i < stream.chunks.length; i++) {
    if (stream.chunks[i] === undefined) {
      throw new TarIntegrityError(`chunked stream ${id} is missing sequence ${i}`);
    }
  }
  if (stream.size !== info.size) {
    throw new TarIntegrityError(
      `chunked stream ${id} size mismatch: chunks total ${stream.size} byte(s), summary says ${info.size}`,
    );
  }
  const hash = createHash('sha256');
  for (const chunk of stream.chunks) hash.update(chunk);
  const actual = hash.digest('hex');
  if (actual !== info.sha256) {
    throw new TarIntegrityError(`chunked stream ${id} digest mismatch: expected ${info.sha256}, got ${actual}`);
  }

  let dataHandedOut = false;
  const chunks = stream.chunks;
  const entry: FileEntry = {
    path: info.path,
    size: info.size,
    mode: info.mode,
    mtime: info.mtime,
    type: 'file' as const,
    typeflag: '0',
    checksumOk: true,
    chunked: true,
    sha256: actual,
    data(): AsyncIterable<Uint8Array> {
      if (dataHandedOut) throw new TarParseError('entry data() can only be called once');
      dataHandedOut = true;
      return (async function* () {
        for (const chunk of chunks) yield chunk;
      })();
    },
  };
  yield entry;
}

function resolveMeta(
  header: ParsedHeader,
  globalPax: PaxRecordMap,
  localPax: PaxRecordMap | null,
  gnuLongName: string | null,
  gnuLongLink: string | null,
): TarEntryMeta {
  const paxPath = localPax?.get('path') ?? globalPax.get('path');
  const paxSize = localPax?.get('size') ?? globalPax.get('size');
  const paxLink = localPax?.get('linkpath') ?? globalPax.get('linkpath');
  const paxMtime = localPax?.get('mtime') ?? globalPax.get('mtime');
  const paxMode = localPax?.get('mode') ?? globalPax.get('mode');

  let name = paxPath ?? gnuLongName ?? header.name;
  if (!paxPath && !gnuLongName && header.prefix) {
    name = joinPrefixName(header.prefix, header.name);
  }

  const size = paxSize !== undefined ? Number(paxSize) : header.size;
  const mtime = paxMtime !== undefined ? Math.trunc(Number(paxMtime)) : header.mtime;
  const mode = paxMode !== undefined ? parseInt(paxMode, 8) : header.mode;
  const linkname = paxLink ?? gnuLongLink ?? (header.linkname || undefined);
  const type = classify(header.typeflag);
  let path = name;
  if (type === 'directory' && path.length > 0 && !path.endsWith('/')) path += '/';
  return {
    path,
    size: Number.isFinite(size) ? size : 0,
    mode,
    mtime,
    type,
    typeflag: header.typeflag,
    linkname,
    checksumOk: header.checksumOk,
  };
}

function padded(size: number): number {
  return Math.ceil(size / BLOCK_SIZE) * BLOCK_SIZE;
}

function stripTrailingNull(bytes: Uint8Array): string {
  let end = bytes.length;
  while (end > 0 && bytes[end - 1] === 0) end--;
  return new TextDecoder().decode(bytes.subarray(0, end));
}

/** Consume the conventional second zero block if present; tolerate EOF. */
async function consumeTrailer(feeder: Feeder): Promise<void> {
  const second = await feeder.readExact(BLOCK_SIZE).catch(() => null);
  if (second !== null && !isZeroBlock(second)) {
    throw new TarParseError('expected second zero block at end of archive');
  }
}

/** Consume the conventional second zero block if present; tolerate EOF. */
