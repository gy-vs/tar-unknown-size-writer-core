# TAR unknown-size writer core

Write TAR archives from **asynchronous data sources whose final length is not
known ahead of time**, onto sinks that may be **non-seekable** (network
streams, pipes). Nothing is cached wholesale in memory, and headers are never
rewritten after the fact.

## Strategies (caller-selected)

| Strategy | When the size is determined | Interoperability | Disk usage |
| --- | --- | --- | --- |
| `'standard'` | prescan into bounded temp storage, then write a plain ustar entry | read by every common TAR reader (GNU tar, bsdtar, …) | bounded by `diskBudget` |
| `'chunked'` | never — a trailing library record carries final size + sha256 | this library only (`readChunkedArchive`) | none |

### Standard strategy

Each source is prescanned into a `SpillBuffer`: the first `memoryThreshold`
bytes stay in RAM, anything beyond is appended to a temp file whose size may
never exceed `diskBudget`. Once the source ends, the entry is emitted as a
regular **ustar** entry (with a PAX local header only when the path is longer
than 100 bytes or the file is ≥ 8 GiB). The temp file is replayed straight
into the sink and deleted afterwards.

If prescanning would exceed `diskBudget`, nothing for that entry is emitted
and `DiskBudgetExceededError` is thrown.

### Chunked extension

A self-describing record stream (magic `TARCHNK1`):

```
magic  "TARCHNK1"
entry: INITIAL(path)  DATA*(507-byte payloads)  FINAL{ size, sha256 }   …
EOA:   two zero blocks (only after a clean close)
```

DATA records carry an explicit length, so payloads may contain any byte
values. The writer holds at most one partial DATA payload and awaits each
sink write before pulling the next chunk, so a slow sink applies natural
backpressure with bounded memory.

## Failure semantics

On **source failure**, **abort** (`AbortSignal`), or **sink failure**:

1. output stops immediately;
2. end-of-archive zero blocks are **never** written, so the output can never
   be mistaken for a completed archive;
3. the writer becomes *broken* (further `add*`/`close` calls throw
   `TarStreamBrokenError`);
4. the rejection carries `bytesWritten` — the exact archive boundary that was
   successfully flushed before the failure.

## Usage

```ts
import { TarUnknownSizeWriter, sinkFromWritable } from './dist/index.js';

const writer = new TarUnknownSizeWriter({
  sink: sinkFromWritable(networkStream), // respects drain => backpressure
  signal: abortController.signal,
});

// Standard, interoperable output with an explicit memory/disk budget.
await writer.addUnknownSize(
  { path: 'data/one.bin' },
  asyncDataSource,
  { strategy: 'standard', memoryThreshold: 1 << 20, diskBudget: 1 << 30 },
);

// Or fully streaming with no temp storage:
await writer.addUnknownSize({ path: 'data/two.bin' }, anotherSource, {
  strategy: 'chunked',
});

const { bytesWritten, entries } = await writer.close();
```

Round-trip the chunked extension:

```ts
import { readChunkedArchive } from './dist/index.js';

await readChunkedArchive(networkInputStream, async (entry) => {
  for await (const chunk of entry) await uploadChunk(chunk);
  console.log(entry.path, entry.final!.size, entry.final!.sha256); // verified
});
```

A truncated entry (source died, aborted, socket closed) reaches EOF before its
FINAL record and fails body iteration with `ChunkTruncatedError`; a corrupted
payload fails with `DigestMismatchError`.

## Development

```sh
npm install
npm test      # vitest, including GNU tar interop tests
npm run build
```
