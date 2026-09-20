/**
 * tar-unknown-size-writer-core
 *
 * Write TAR archives from data sources whose final length is not known
 * ahead of time, onto sinks that may be non-seekable (network streams).
 *
 * Two caller-selected strategies (see writer.ts):
 *   - 'standard': bounded prescan (memory -> temp file with disk budget),
 *     then a plain ustar entry readable by stock TAR tools.
 *   - 'chunked' : library-defined streaming extension with a trailing
 *     size + sha256 record, round-tripped by readChunkedArchive().
 */
export type { TarHeader } from './core-misc.js';
export { mergeMetadata, ArchiveIndex } from './core-misc.js';

export {
  TarUnknownSizeWriter,
  type ByteSource,
  type ByteSink,
  type EntryMeta,
  type WriterOptions,
  type AddResult,
  type UnknownSizeStrategy,
  type StandardStrategy,
  type ChunkedStrategy,
} from './writer.js';

export { readChunkedArchive, ChunkTruncatedError, type ChunkedEntry } from './reader.js';

export {
  buildUstarHeader,
  entryHeaderBlocks,
  encodePaxRecords,
  BLOCK_SIZE,
  USTAR_OCTAL_SIZE_LIMIT,
  type HeaderFields,
} from './header.js';

export { SpillBuffer, type SpillOptions } from './spill-buffer.js';

export {
  CHUNKED_MAGIC,
  DATA_PAYLOAD_SIZE,
  buildInitialBlock,
  buildDataBlock,
  buildFinalBlock,
  parseInitialBlock,
  parseFinalBlock,
  type ChunkedFinal,
} from './chunked.js';

export {
  TarWriteError,
  TarAbortedError,
  SourceFailedError,
  SinkWriteError,
  DiskBudgetExceededError,
  TarStreamBrokenError,
  TarParseError,
  DigestMismatchError,
} from './errors.js';

import type { Writable } from 'node:stream';
import type { ByteSink } from './writer.js';

/** Adapt a Node Writable (e.g. a network socket) into a back-pressured sink. */
export function sinkFromWritable(writable: Writable): ByteSink {
  return (chunk: Buffer) =>
    new Promise<void>((resolve, reject) => {
      if (writable.destroyed || writable.writableEnded) {
        reject(new Error('writable stream is no longer accepting writes'));
        return;
      }
      const cleanup = () => writable.off('error', onError);
      const onError = (cause: unknown) => {
        cleanup();
        reject(cause instanceof Error ? cause : new Error(String(cause)));
      };
      const onDone = () => {
        cleanup();
        resolve();
      };
      writable.once('error', onError);
      // write() returns false when the internal buffer is full; wait for
      // drain before resolving so the writer cannot race ahead of slow sinks.
      if (writable.write(chunk)) {
        onDone();
      } else {
        writable.once('drain', onDone);
      }
    });
}
