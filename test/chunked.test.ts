import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import {
  CHUNKED_MAGIC,
  ChunkTruncatedError,
  DigestMismatchError,
  TarUnknownSizeWriter,
  TarParseError,
  readChunkedArchive,
  type ByteSource,
} from '../src/index.js';

function sha256(buf: Buffer): string {
  return createHash('sha256').update(buf).digest('hex');
}

function sourceFromChunks(chunks: Buffer[]): ByteSource {
  return {
    async *[Symbol.asyncIterator]() {
      for (const c of chunks) yield c;
    },
  };
}

async function writeChunked(
  entries: Array<{ path: string; chunks: Buffer[] }>,
  signal?: AbortSignal,
): Promise<Buffer> {
  const out: Buffer[] = [];
  const w = new TarUnknownSizeWriter({ sink: (c) => out.push(c), signal });
  for (const e of entries) {
    await w.addUnknownSize({ path: e.path }, sourceFromChunks(e.chunks), {
      strategy: 'chunked',
    });
  }
  await w.close();
  return Buffer.concat(out);
}

async function readAll(source: AsyncIterable<Buffer>): Promise<Buffer> {
  const out: Buffer[] = [];
  for await (const c of source) out.push(c);
  return Buffer.concat(out);
}

describe('chunked extension — round trip', () => {
  it('round-trips an empty source with FINAL size 0', async () => {
    const archive = await writeChunked([{ path: 'empty', chunks: [] }]);
    const seen: Array<{ path: string; data: Buffer }> = [];
    await readChunkedArchive(sourceFromChunks([archive]), async (entry) => {
      const data = await readAll(entry);
      seen.push({ path: entry.path, data });
      expect(entry.final?.size).toBe(0);
      expect(entry.final?.sha256).toBe(sha256(Buffer.alloc(0)));
    });
    expect(seen).toEqual([{ path: 'empty', data: Buffer.alloc(0) }]);
  });

  it('round-trips multiple unknown-size entries with awkward boundaries', () =>
    runRoundTrip([
      {
        path: 'tiny',
        chunks: [Buffer.from('a')],
      },
      {
        path: 'split-across-records',
        chunks: [Buffer.alloc(506, 1), Buffer.alloc(3, 2), Buffer.alloc(507 * 3 + 11, 3)],
      },
      {
        path: 'exact-multiple',
        chunks: [Buffer.alloc(507, 4), Buffer.alloc(507, 5)],
      },
      {
        path: 'one-byte-at-a-time',
        chunks: Array.from({ length: 2000 }, (_, i) => Buffer.from([i % 256])),
      },
      {
        path: 'unicode/名前/файл 🦦.bin',
        chunks: [Buffer.from('payload-✓-финал')],
      },
      { path: 'again-empty', chunks: [] },
    ]));

  it('handles byte patterns indistinguishable from record flags', () =>
    runRoundTrip([
      // Payloads whose bytes begin with 0x00/0x01/0x02 must parse as DATA.
      { path: 'flags', chunks: [Buffer.from([0, 1, 2, 0, 0xff, 2, 1, 0])] },
      {
        path: 'all-flags-507',
        chunks: [Buffer.from(Array.from({ length: 507 }, (_, i) => i % 3))],
      },
    ]));

  it('parses an archive delivered to the reader in tiny byte-sized chunks', async () => {
    const data = Buffer.alloc(5000, 0x7f);
    const archive = await writeChunked([{ path: 'x', chunks: [data] }]);
    // Break everything into 1..7 byte reads; RecordReader must reassemble.
    const fragmented: AsyncIterable<Buffer> = {
      async *[Symbol.asyncIterator]() {
        for (let i = 0; i < archive.length; ) {
          const n = Math.min(1 + ((i * 13) % 7), archive.length - i);
          yield archive.subarray(i, i + n);
          i += n;
        }
      },
    };
    await readChunkedArchive(fragmented, async (entry) => {
      expect(entry.path).toBe('x');
      const body = await readAll(entry);
      expect(body.equals(data)).toBe(true);
      expect(entry.final?.sha256).toBe(sha256(data));
    });
  });

  async function runRoundTrip(entries: Array<{ path: string; chunks: Buffer[] }>): Promise<void> {
    const expected = entries.map((e) => ({ path: e.path, data: Buffer.concat(e.chunks) }));
    const archive = await writeChunked(entries);
    expect(archive.subarray(0, CHUNKED_MAGIC.length).equals(CHUNKED_MAGIC)).toBe(true);
    const seen: Array<{ path: string; data: Buffer }> = [];
    await readChunkedArchive(sourceFromChunks([archive]), async (entry) => {
      const data = await readAll(entry);
      seen.push({ path: entry.path, data });
      expect(entry.final?.size).toBe(data.length);
      expect(entry.final?.sha256).toBe(sha256(data));
    });
    expect(seen.map((s) => s.path)).toEqual(expected.map((e) => e.path));
    for (let i = 0; i < expected.length; i++) {
      expect(seen[i].data.equals(expected[i].data)).toBe(true);
    }
  }
});

describe('chunked extension — failures and archive boundary', () => {
  it('a source failure mid-entry leaves no FINAL and the reader rejects it', async () => {
    const out: Buffer[] = [];
    const w = new TarUnknownSizeWriter({ sink: (c) => out.push(c) });
    await w.addUnknownSize(
      { path: 'first-ok' },
      sourceFromChunks([Buffer.from('complete')]),
      { strategy: 'chunked' },
    );
    const firstBoundary = w.bytesWritten;
    const broken: ByteSource = {
      async *[Symbol.asyncIterator]() {
        yield Buffer.alloc(1000, 0xee);
        yield Buffer.alloc(1000, 0xef);
        throw new Error('network source vanished');
      },
    };
    await expect(
      w.addUnknownSize({ path: 'second-dies' }, broken, { strategy: 'chunked' }),
    ).rejects.toMatchObject({ name: 'SourceFailedError' });
    // Boundary covers whatever DATA records were flushed before the throw;
    // FINAL and EOA are never written. It must contain the second entry's
    // INITIAL block and a number of DATA records between 1 and ceil(2000/507).
    const maxDataRecords = Math.ceil(2000 / 507);
    const boundary = w.bytesWritten;
    const secondStart = firstBoundary + 512;
    const flushedDataBlocks = (boundary - secondStart) / 512;
    expect(Number.isInteger(flushedDataBlocks)).toBe(true);
    expect(flushedDataBlocks).toBeGreaterThanOrEqual(1);
    expect(flushedDataBlocks).toBeLessThanOrEqual(maxDataRecords);
    await expect(w.close()).rejects.toMatchObject({ name: 'TarStreamBrokenError' });

    const archive = Buffer.concat(out);
    expect(archive.length).toBe(boundary);
    // The second entry's INITIAL block starts with flag 0x00 and its name.
    expect(archive[firstBoundary]).toBe(0x00);
    expect(archive.toString('utf8', firstBoundary + 1, firstBoundary + 12)).toBe('second-dies');
    // Tail must not be zero EOA blocks.
    expect(archive.subarray(archive.length - 1024).equals(Buffer.alloc(1024))).toBe(false);
    // Reader successfully yields the first entry, then fails on the second.
    const results: string[] = [];
    await expect(
      readChunkedArchive(sourceFromChunks([archive]), async (entry) => {
        await readAll(entry);
        results.push(entry.path);
      }),
    ).rejects.toBeInstanceOf(ChunkTruncatedError);
    expect(results).toEqual(['first-ok']);
  });

  it('abort after N entries: bytes stop at boundary, no EOA', async () => {
    const ac = new AbortController();
    const out: Buffer[] = [];
    const w = new TarUnknownSizeWriter({ sink: (c) => out.push(c), signal: ac.signal });
    await w.addUnknownSize({ path: 'a' }, sourceFromChunks([Buffer.from('aaaa')]), {
      strategy: 'chunked',
    });
    const boundary = w.bytesWritten;
    ac.abort();
    await expect(
      w.addUnknownSize({ path: 'b' }, sourceFromChunks([Buffer.from('bbbb')]), {
        strategy: 'chunked',
      }),
    ).rejects.toMatchObject({ name: 'TarAbortedError', bytesWritten: boundary });
    const archive = Buffer.concat(out);
    expect(archive.length).toBe(boundary);
    // No zero EOA blocks at the tail.
    expect(archive.subarray(archive.length - 1024).equals(Buffer.alloc(1024))).toBe(false);
  });

  it('detects a tampered digest', async () => {
    const archive = await writeChunked([{ path: 'x', chunks: [Buffer.from('hello world')] }]);
    // Flip one payload byte inside the first DATA record (magic 8 + initial 512 + data offset 5).
    const tampered = Buffer.from(archive);
    tampered[8 + 512 + 5 + 1] ^= 0xff;
    await expect(
      readChunkedArchive(sourceFromChunks([tampered]), async (entry) => {
        await readAll(entry);
      }),
    ).rejects.toBeInstanceOf(DigestMismatchError);
  });

  it('rejects data that is not a chunked archive (e.g. plain ustar)', async () => {
    const ustar = Buffer.alloc(1024);
    ustar.write('ustar', 257, 'ascii');
    await expect(
      readChunkedArchive(sourceFromChunks([ustar]), async () => {}),
    ).rejects.toBeInstanceOf(TarParseError);
  });

  it('rejects a truncated FINAL record', async () => {
    const archive = await writeChunked([{ path: 'x', chunks: [Buffer.from('abc')] }]);
    // Drop the final 10 bytes (part of FINAL block) — cleanly cut, no EOA.
    const cut = archive.subarray(0, archive.length - 10 - 1024);
    await expect(
      readChunkedArchive(sourceFromChunks([cut]), async (entry) => {
        await readAll(entry);
      }),
    ).rejects.toBeInstanceOf(ChunkTruncatedError);
  });

  it('encodes known-size addEntry calls in chunked framing once chunked mode starts', async () => {
    const out: Buffer[] = [];
    const w = new TarUnknownSizeWriter({ sink: (c) => out.push(c) });
    await w.addUnknownSize({ path: 'u' }, sourceFromChunks([Buffer.from('unknown-len')]), {
      strategy: 'chunked',
    });
    const known = Buffer.from('i know my length');
    await w.addEntry({ path: 'k' }, sourceFromChunks([known]), known.length);
    await w.close();

    const seen: Array<{ path: string; data: Buffer }> = [];
    await readChunkedArchive(sourceFromChunks([Buffer.concat(out)]), async (entry) => {
      seen.push({ path: entry.path, data: await readAll(entry) });
    });
    expect(seen).toEqual([
      { path: 'u', data: Buffer.from('unknown-len') },
      { path: 'k', data: known },
    ]);
  });

  it('refuses to switch to chunked after plain ustar bytes were written', async () => {
    const out: Buffer[] = [];
    const w = new TarUnknownSizeWriter({ sink: (c) => out.push(c) });
    const known = Buffer.from('plain');
    await w.addEntry({ path: 'plain.txt' }, sourceFromChunks([known]), known.length);
    await expect(
      w.addUnknownSize({ path: 'late' }, sourceFromChunks([Buffer.from('x')]), {
        strategy: 'chunked',
      }),
    ).rejects.toThrow(/same strategy/);
    expect(w.broken).toBe(true);
  });
});
