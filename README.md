# TAR unknown-size writer

TypeScript library for writing TAR archives from **asynchronous data sources
whose length is not known up front**, to sinks that may be **non-seekable**
(network sockets, pipes). It also provides a matching streaming reader.

Run `npm install`, then `npm test` and `npm run build`.

## Strategies

The strategy — and all resource/compatibility limits — are chosen explicitly
by the caller:

| Option | `standard` | `chunked` |
| --- | --- | --- |
| Output | ordinary ustar / PAX entry | library-defined extension records |
| Temp storage | prescan: RAM up to `memoryBudget`, then temp files up to `tempBudget` | none |
| Start latency | only after the whole source is read | immediately |
| Portable | yes (GNU tar, bsdtar, …) | no: round-trips only through this library |

`compatibility` (standard only):

- `'pax'` (recommended) — emit a PAX extended header when an entry needs one
  (path > 100 bytes, size > 8 GiB).
- `'ustar'` — plain POSIX ustar only; an entry that would need PAX fails with
  `CompatibilityError` instead of silently becoming less portable.

### Standard

The source is fully consumed into bounded storage first, so a real TAR header
with the final size is written afterwards. Nothing is buffered in memory
beyond `memoryBudget` per entry; the remainder spills to `tempDir`, and a
source larger than `tempBudget` fails with `TempBudgetExceededError` before
any archive byte is emitted.

```ts
import { TarWriter } from './dist/index.js';

const writer = new TarWriter({
  strategy: 'standard',
  sink: (chunk) => socketWrite(chunk), // awaited; provides backpressure
  memoryBudget: 1 << 20,               // 1 MiB in RAM per entry
  tempBudget: 1 << 30,                 // up to 1 GiB of temp files
  compatibility: 'pax',
  tempDir: '/var/tmp',                 // defaults to os.tmpdir()
});

const result = await writer.addFile({
  path: 'logs/2026-09-20.log',
  source: asyncIterableOfBytes,
  signal: abortController.signal,
});
if (!result.ok) {
  // result.boundary = archive bytes already on the wire; the archive is
  // NOT terminated and must not be treated as a valid TAR.
}
await writer.end(); // writes the two trailing zero blocks
```

### Chunked extension

Data is streamed straight to the sink as ordered **chunk records**
(typeflag `C`, names `@ux/<id>/<seq>`); once the source finishes, a single
**summary record** (typeflag `S`, name `@ux/<id>`) commits the entry. The
summary carries PAX-framed records with the final path, size, mode, mtime and
a SHA-256 digest (`uxlib.path`, `uxlib.size`, `uxlib.sha256`, …).

An empty source produces just a summary record. A stream without its summary
is explicitly invalid: the reader refuses archives that end with dangling
chunks, and verifies size and digest on assembly.

```ts
const writer = new TarWriter({
  strategy: 'chunked',
  sink: (chunk) => socketWrite(chunk),
  chunkSize: 64 * 1024,
});
```

Standard TAR readers will list these vendor records as unknown-type entries;
use `standard` whenever portability matters.

## Failure and cancellation semantics

- Every sink write is awaited, so a slow sink applies natural backpressure; a
  rejected sink becomes a `SinkError`.
- On source failure, sink failure, or `AbortSignal`, the writer stops
  immediately, never writes the terminating zero blocks, and reports
  `boundary` — the number of archive bytes already accepted by the sink.
  That prefix is unterminated; nothing claims a valid end-of-archive.
- After any failed entry the writer is stopped; `end()` returns a failure
  rather than appending a fake terminator.
- Standard prescan failures (oversize source) leave the output at exactly its
  previous boundary and remove any temp files.

## Reading

`readTar(input)` streams ordinary ustar/PAX archives (including GNU long-name
records), transparently reassembles chunked entries, and verifies their
summary. Payloads are provided via `entry.data()`; unconsumed payloads are
skipped automatically.

```ts
for await (const entry of readTar(input)) {
  if (entry.type !== 'file') continue;
  for await (const part of entry.data()) {
    // entry.chunked === true for extension entries; entry.sha256 is verified
  }
}
```

Errors: `SourceError`, `SinkError`, `TempBudgetExceededError`,
`CompatibilityError`, `AbortedError`, `WriterStateError`, `TarParseError`,
`TarUnexpectedEofError`, `TarIntegrityError`.
