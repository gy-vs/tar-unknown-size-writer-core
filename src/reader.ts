/**
 * Reader for the library-defined chunked extension.
 *
 * Parses archives produced by TarUnknownSizeWriter chunked entries and
 * re-presents each entry's data as an async iterable. The trailing FINAL
 * record (size + sha256) is verified against the bytes actually streamed.
 * An entry whose source failed or whose archive was aborted reaches EOF
 * before FINAL: body iteration raises ChunkTruncatedError carrying the
 * number of data bytes seen.
 */
import { createHash } from 'node:crypto';
import { BLOCK_SIZE } from './header.js';
import {
  CHUNKED_MAGIC,
  dataPayload,
  parseDataBlock,
  parseFinalBlock,
  parseInitialBlock,
  type ChunkedFinal,
} from './chunked.js';
import { DigestMismatchError, TarParseError } from './errors.js';

export interface ChunkedEntry extends AsyncIterable<Buffer> {
  readonly path: string;
  /** Size/digest from FINAL; undefined until the body is fully consumed. */
  readonly final?: ChunkedFinal;
}

export class ChunkTruncatedError extends TarParseError {
  constructor(
    public readonly path: string,
    public readonly bytesRead: number,
  ) {
    super(
      `chunked entry ${JSON.stringify(path)} is truncated: no FINAL record (source failed or archive was aborted after ${bytesRead} data bytes)`,
    );
  }
}

class RecordReader {
  #iterator: AsyncIterator<Buffer>;
  #buffered: Buffer = Buffer.alloc(0) as Buffer;
  #done = false;

  constructor(source: AsyncIterable<Buffer>) {
    this.#iterator = source[Symbol.asyncIterator]();
  }

  /** Return exactly `length` bytes, or null when the source ends first. */
  async read(length: number): Promise<Buffer | null> {
    while (this.#buffered.length < length) {
      if (this.#done) return null; // EOF (possibly a short tail) -> truncation
      const { value, done } = await this.#iterator.next();
      if (done) {
        this.#done = true;
      } else if (value && value.length) {
        this.#buffered = this.#buffered.length
          ? Buffer.concat([this.#buffered, value])
          : value;
      }
    }
    const out = Buffer.from(this.#buffered.subarray(0, length));
    this.#buffered = this.#buffered.subarray(length);
    return out;
  }
}

/**
 * Visit every chunked entry. `onEntry` must fully consume (or abort
 * iteration over) `entry` before the next entry is visited; verification
 * of FINAL runs as the last step of body consumption.
 */
export async function readChunkedArchive(
  source: AsyncIterable<Buffer>,
  onEntry: (entry: ChunkedEntry) => Promise<void> | void,
): Promise<void> {
  const reader = new RecordReader(source);

  const magic = await reader.read(CHUNKED_MAGIC.length);
  if (!magic || !magic.equals(CHUNKED_MAGIC)) {
    throw new TarParseError('not a chunked archive: magic prefix missing');
  }

  for (;;) {
    const block = await reader.read(BLOCK_SIZE);
    if (!block) {
      throw new TarParseError('chunked archive ended unexpectedly before the next entry or EOA');
    }
    if (block.equals(Buffer.alloc(BLOCK_SIZE))) {
      // End of archive: ustar uses two zero blocks; tolerate one.
      const second = await reader.read(BLOCK_SIZE);
      if (second && !second.equals(Buffer.alloc(BLOCK_SIZE))) {
        throw new TarParseError('unexpected non-zero bytes after end-of-archive');
      }
      return;
    }

    const path = parseInitialBlock(block);
    let finalRecord: ChunkedFinal | undefined;
    let consumed = false;

    const entry: ChunkedEntry = {
      path,
      get final() {
        return finalRecord;
      },
      async *[Symbol.asyncIterator]() {
        if (consumed) throw new TarParseError('entry body can only be consumed once');
        consumed = true;
        const hash = createHash('sha256');
        let received = 0;
        for (;;) {
          const rec = await reader.read(BLOCK_SIZE);
          if (!rec) throw new ChunkTruncatedError(path, received);
          if (rec[0] === 0x02) {
            finalRecord = parseFinalBlock(rec);
            break;
          }
          const length = parseDataBlock(rec);
          if (length === 0) throw new TarParseError('zero-length DATA record is not permitted');
          const payload = dataPayload(rec, length);
          hash.update(payload);
          received += length;
          yield payload;
        }
        if (received !== finalRecord.size) {
          throw new TarParseError(
            `entry ${JSON.stringify(path)}: FINAL declares size ${finalRecord.size} but ${received} data bytes were present`,
          );
        }
        const actual = hash.digest('hex');
        if (actual !== finalRecord.sha256) {
          throw new DigestMismatchError(path, finalRecord.sha256, actual);
        }
      },
    };

    await onEntry(entry);
  }
}
