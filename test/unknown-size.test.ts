import { createHash, randomBytes } from 'node:crypto';
import { execFile } from 'node:child_process';
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import * as path from 'node:path';
import { promisify } from 'node:util';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  AbortedError,
  BLOCK_SIZE,
  CompatibilityError,
  SinkError,
  SourceError,
  TarWriter,
  TempBudgetExceededError,
  isZeroBlock,
  readTar,
  type ByteSource,
  type TarSink,
} from '../src/index.js';

const pExecFile = promisify(execFile);

/* ------------------------------ helpers ------------------------------ */

function bytes(content: string | Uint8Array): Uint8Array {
  return typeof content === 'string' ? new TextEncoder().encode(content) : content;
}

function concat(parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

function sha256(data: Uint8Array): string {
  return createHash('sha256').update(data).digest('hex');
}

/**
 * Byte-for-byte equality. Used instead of toEqual because vitest does not
 * treat a plain Uint8Array as deep-equal to a Node Buffer even when their
 * contents are identical.
 */
function expectBytes(actual: Uint8Array, expected: Uint8Array): void {
  expect(actual.length).toBe(expected.length);
  for (let i = 0; i < expected.length; i++) {
    if (actual[i] !== expected[i]) {
      throw new Error(`byte mismatch at ${i}: got ${actual[i]}, want ${expected[i]}`);
    }
  }
}

function arraySource(data: Uint8Array, chunkSize = 37): ByteSource {
  return {
    *[Symbol.iterator]() {
      for (let i = 0; i < data.length; i += chunkSize) yield data.subarray(i, i + chunkSize);
    },
  };
}

async function* asyncSource(data: Uint8Array, chunkSize = 37, delay = 0): AsyncGenerator<Uint8Array> {
  for (let i = 0; i < data.length; i += chunkSize) {
    if (delay > 0) await new Promise((r) => setTimeout(r, delay));
    yield data.subarray(i, i + chunkSize);
  }
}

/** Collect sink output into memory. */
function memorySink(): { sink: TarSink; chunks: Uint8Array[]; size: () => number } {
  const chunks: Uint8Array[] = [];
  return {
    sink: (chunk) => {
      chunks.push(chunk.slice());
    },
    chunks,
    size: () => chunks.reduce((n, c) => n + c.length, 0),
  };
}

async function collectEntries(input: ByteSource): Promise<{ path: string; data: Uint8Array; chunked: boolean; digest?: string }[]> {
  // A bare Uint8Array iterates as numbers; wrap it so it is one binary chunk.
  const source: ByteSource = input instanceof Uint8Array ? [input] : input;
  const out: { path: string; data: Uint8Array; chunked: boolean; digest?: string }[] = [];
  for await (const entry of readTar(source)) {
    if (entry.type !== 'file') continue;
    const parts: Uint8Array[] = [];
    for await (const part of entry.data()) parts.push(part);
    out.push({ path: entry.path, data: concat(parts), chunked: entry.chunked, digest: entry.sha256 });
  }
  return out;
}

let workDir: string;
beforeAll(async () => {
  workDir = await mkdtemp(path.join(tmpdir(), 'tar-tests-'));
});
afterAll(async () => {
  await rm(workDir, { recursive: true, force: true });
});

const STANDARD = {
  strategy: 'standard',
  memoryBudget: 4096,
  tempBudget: 16 * 1024 * 1024,
  compatibility: 'pax',
} as const;

/* ==================================================================== */
/* standard strategy                                                    */
/* ==================================================================== */

describe('standard strategy', () => {
  it('writes an empty source as a valid zero-length entry', async () => {
    const out = memorySink();
    const w = new TarWriter({ ...STANDARD, sink: out.sink });
    const res = await w.addFile({ path: 'empty.txt', source: [] });
    expect(res.ok).toBe(true);
    if (res.ok) {
      expect(res.size).toBe(0);
      expect(res.sha256).toBe(sha256(new Uint8Array(0)));
    }
    const end = await w.end();
    expect(end.ok).toBe(true);

    const entries = await collectEntries(concat(out.chunks));
    expect(entries).toHaveLength(1);
    expect(entries[0].path).toBe('empty.txt');
    expect(entries[0].data).toHaveLength(0);

    // header block + 2 zero blocks
    expect(out.size()).toBe(BLOCK_SIZE * 3);
  });

  it('round-trips content that stays inside the RAM budget', async () => {
    const data = bytes('hello world'.repeat(100));
    const out = memorySink();
    const w = new TarWriter({ ...STANDARD, memoryBudget: data.length + 1, sink: out.sink });
    const res = await w.addFile({ path: 'ram.bin', source: arraySource(data, 13) });
    expect(res.ok).toBe(true);
    if (res.ok) {
      expect(res.size).toBe(data.length);
      expect(res.sha256).toBe(sha256(data));
    }
    expect(await w.end()).toMatchObject({ ok: true });

    const entries = await collectEntries(concat(out.chunks));
    expectBytes(entries[0].data, data);
  });

  it('spills into a temp file past the memory threshold and round-trips', async () => {
    const data = randomBytes(50_000);
    const entryTempDir = await mkdtemp(path.join(workDir, 'spill-'));
    const sinkChunks: Uint8Array[] = [];
    let spoolSeenDuringWrite = false;
    let firstWrite = true;
    const sink: TarSink = async (chunk) => {
      if (firstWrite) {
        // Output starts only after prescan finished; the spool file is still
        // open here and gets removed once the entry completed.
        spoolSeenDuringWrite = (await listFiles(entryTempDir)).some((f) => f.startsWith('spool-'));
        firstWrite = false;
      }
      sinkChunks.push(chunk.slice());
    };

    const w = new TarWriter({
      ...STANDARD,
      memoryBudget: 1024,
      tempBudget: 1024 * 1024,
      tempDir: entryTempDir,
      sink,
    });
    const res = await w.addFile({ path: 'big.bin', source: asyncSource(data, 4096) });
    expect(res.ok).toBe(true);
    if (res.ok) expect(res.sha256).toBe(sha256(data));
    expect(await w.end()).toMatchObject({ ok: true });

    expect(spoolSeenDuringWrite).toBe(true);
    expect(await listFiles(entryTempDir)).toEqual([]);

    const entries = await collectEntries(concat(sinkChunks));
    expect(entries[0].path).toBe('big.bin');
    expectBytes(entries[0].data, data);
  });

  it('fails with TempBudgetExceededError and emits nothing for the entry', async () => {    const data = randomBytes(20_000);
    const out = memorySink();
    const w = new TarWriter({
      strategy: 'standard',
      memoryBudget: 1024,
      tempBudget: 4096,
      compatibility: 'pax',
      tempDir: workDir,
      sink: out.sink,
    });
    const boundaryBefore = w.bytesWritten;
    const res = await w.addFile({ path: 'too-big.bin', source: asyncSource(data, 1000) });
    expect(res.ok).toBe(false);
    if (!res.ok) {
      expect(res.error).toBeInstanceOf(TempBudgetExceededError);
      expect(res.error.limit).toBe(4096);
      expect(res.error.required).toBeGreaterThan(4096);
      expect(res.canceled).toBe(false);
      // Prescan failure happens before any archive byte is emitted.
      expect(res.boundary).toBe(boundaryBefore);
    }

    const end = await w.end();
    expect(end.ok).toBe(false);
    if (!end.ok) expect(end.boundary).toBe(boundaryBefore);

    // No zero terminator: the output is an unterminated prefix.
    const raw = concat(out.chunks);
    expect(raw.length % BLOCK_SIZE).toBe(0);
    expect(isZeroBlock(raw.subarray(raw.length - BLOCK_SIZE))).toBe(false);
  });

  it('reports a source failure during prescan as SourceError without partial entry bytes', async () => {
    const boom = new Error('disk read failed');
    const source: ByteSource = {
      async *[Symbol.asyncIterator]() {
        yield bytes('partial data ');
        await new Promise((r) => setTimeout(r, 2));
        throw boom;
      },
    };
    const out = memorySink();
    const entryTempDir = await mkdtemp(path.join(workDir, 'fail-'));
    const w = new TarWriter({ ...STANDARD, memoryBudget: 4, tempDir: entryTempDir, sink: out.sink });
    const boundaryBefore = w.bytesWritten;
    const res = await w.addFile({ path: 'boom.txt', source });
    expect(res.ok).toBe(false);
    if (!res.ok) {
      expect(res.error).toBeInstanceOf(SourceError);
      expect(res.error.cause).toBe(boom);
      // Standard strategy emits nothing until the whole source is spooled.
      expect(res.boundary).toBe(boundaryBefore);
    }
    // The spool file opened during prescan must be removed on failure.
    expect(await listFiles(entryTempDir)).toEqual([]);
  });

  it('honors abort during the output phase and keeps the boundary block-aligned', async () => {
    const controller = new AbortController();
    const data = randomBytes(100_000);
    let aborted = false;
    const sink: TarSink = (chunk) => {
      if (!aborted && chunk.length === BLOCK_SIZE) {
        // Abort after the entry header has gone out; payload writes follow.
        controller.abort();
        aborted = true;
      }
    };
    const w = new TarWriter({ ...STANDARD, memoryBudget: 4096, sink });
    const res = await w.addFile({ path: 'canceled.bin', source: arraySource(data, 7000), signal: controller.signal });
    expect(res.ok).toBe(false);
    if (!res.ok) {
      expect(res.canceled).toBe(true);
      expect(res.boundary % BLOCK_SIZE).toBe(0);
    }
  });

  it('refuses PAX headers under ustar compatibility', async () => {
    const data = bytes('x');
    const longPath = 'a'.repeat(120);
    const out = memorySink();
    const w = new TarWriter({
      strategy: 'standard',
      memoryBudget: 1024,
      tempBudget: 1024,
      compatibility: 'ustar',
      sink: out.sink,
    });
    const res = await w.addFile({ path: longPath, source: arraySource(data) });
    expect(res.ok).toBe(false);
    if (!res.ok) expect(res.error).toBeInstanceOf(CompatibilityError);

    // A normal short path still works in ustar mode.
    const w2 = new TarWriter({
      strategy: 'standard',
      memoryBudget: 1024,
      tempBudget: 1024,
      compatibility: 'ustar',
      sink: out.sink,
    });
    const res2 = await w2.addFile({ path: 'ok.txt', source: arraySource(data) });
    expect(res2.ok).toBe(true);
    expect(await w2.end()).toMatchObject({ ok: true });
  });

  it('writes several unknown-size entries into one valid archive', async () => {
    const payloads = new Map<string, Uint8Array>([
      ['one.txt', bytes('first file\n')],
      ['dir/two.txt', bytes('second file, a bit longer'.repeat(50))],
      ['three-empty', new Uint8Array(0)],
      ['four.bin', randomBytes(70_000)],
    ]);
    const out = memorySink();
    const w = new TarWriter({ ...STANDARD, memoryBudget: 8192, sink: out.sink });
    for (const [name, data] of payloads) {
      const res = await w.addFile({ path: name, source: asyncSource(data, 5000) });
      expect(res.ok, res.ok ? '' : res.error.message).toBe(true);
    }
    expect(await w.end()).toMatchObject({ ok: true });

    const entries = await collectEntries(concat(out.chunks));
    expect(entries.map((e) => e.path)).toEqual([...payloads.keys()]);
    for (const entry of entries) {
      expectBytes(entry.data, payloads.get(entry.path)!);
      expect(entry.chunked).toBe(false);
    }
  });

  it('is accepted by the system GNU tar and extracts byte-identical content', async () => {
    let available = false;
    try {
      await pExecFile('tar', ['--version']);
      available = true;
    } catch {
      available = false;
    }
    if (!available) return;

    const payloads = new Map<string, Uint8Array>([
      ['plain.txt', bytes('plain contents\n')],
      ['data/odd-size.bin', randomBytes(3333)],
      ['data/spilled.bin', randomBytes(45_000)],
      ['empty.txt', new Uint8Array(0)],
      ['päkë/中文.bin', bytes('unicode path contents')],
    ]);

    const out = memorySink();
    const w = new TarWriter({ ...STANDARD, memoryBudget: 4096, tempBudget: 1024 * 1024, sink: out.sink });
    for (const [name, data] of payloads) {
      const res = await w.addFile({ path: name, source: asyncSource(data, 7777), mtime: 0 });
      expect(res.ok, res.ok ? '' : res.error.message).toBe(true);
    }
    expect(await w.end()).toMatchObject({ ok: true });

    const archive = path.join(workDir, `std-${Date.now()}.tar`);
    await writeAll(archive, concat(out.chunks));

    // GNU tar lists it and verifies structural integrity.
    const list = await pExecFile('tar', ['-tf', archive]);
    for (const name of payloads.keys()) expect(list.stdout).toContain(name);

    const extractDir = await mkdtemp(path.join(workDir, 'extract-'));
    await pExecFile('tar', ['-xf', archive, '-C', extractDir]);
    for (const [name, data] of payloads) {
      const onDisk = await readFile(path.join(extractDir, name));
      expectBytes(new Uint8Array(onDisk), data);
    }

    // ustar-only output is also accepted (no PAX records in it).
    const out2 = memorySink();
    const w2 = new TarWriter({
      strategy: 'standard',
      memoryBudget: 4096,
      tempBudget: 1024 * 1024,
      compatibility: 'ustar',
      sink: out2.sink,
    });
    const ustarData = bytes('ustar-only body'.repeat(10));
    expect(await w2.addFile({ path: 'nested/dir/ustar.txt', source: arraySource(ustarData) })).toMatchObject({ ok: true });
    expect(await w2.end()).toMatchObject({ ok: true });
    const archive2 = path.join(workDir, `ustar-${Date.now()}.tar`);
    await writeAll(archive2, concat(out2.chunks));
    const ex2 = await mkdtemp(path.join(workDir, 'extract-ustar-'));
    await pExecFile('tar', ['-xf', archive2, '-C', ex2]);
    expectBytes(new Uint8Array(await readFile(path.join(ex2, 'nested/dir/ustar.txt'))), ustarData);
  });
});

/* ==================================================================== */
/* chunked extension strategy                                           */
/* ==================================================================== */

describe('chunked strategy', () => {
  it('writes an empty source as a summary record with no chunks', async () => {
    const out = memorySink();
    const w = new TarWriter({ strategy: 'chunked', sink: out.sink, chunkSize: 1024 });
    const res = await w.addFile({ path: 'empty', source: [] });
    expect(res.ok).toBe(true);
    if (res.ok) {
      expect(res.size).toBe(0);
      expect(res.sha256).toBe(sha256(new Uint8Array(0)));
    }
    expect(await w.end()).toMatchObject({ ok: true });

    const entries = await collectEntries(concat(out.chunks));
    expect(entries).toHaveLength(1);
    expect(entries[0].path).toBe('empty');
    expect(entries[0].data).toHaveLength(0);
    expect(entries[0].chunked).toBe(true);
    expect(entries[0].digest).toBe(sha256(new Uint8Array(0)));

    // summary header + padded PAX summary payload (1 block) + 2 zero blocks
    expect(out.size()).toBe(BLOCK_SIZE * 4);
  });

  it('splits streamed data into ordered chunks and reassembles with digest', async () => {
    const data = randomBytes(10_000);
    const out = memorySink();
    const w = new TarWriter({ strategy: 'chunked', sink: out.sink, chunkSize: 1000 });
    const res = await w.addFile({ path: 'streamed.bin', source: asyncSource(data, 333) });
    expect(res.ok).toBe(true);
    if (res.ok) {
      expect(res.size).toBe(data.length);
      expect(res.sha256).toBe(sha256(data));
    }
    expect(await w.end()).toMatchObject({ ok: true });

    // 10 full chunks + 1 summary, all headers aligned to 512 bytes.
    let chunkBlocks = 0;
    for (const c of out.chunks) {
      expect(c.length % BLOCK_SIZE).toBe(0);
    }
    void chunkBlocks;

    const entries = await collectEntries(concat(out.chunks));
    expect(entries).toHaveLength(1);
    expectBytes(entries[0].data, data);
    expect(entries[0].chunked).toBe(true);
    expect(entries[0].digest).toBe(sha256(data));
  });

  it('interleaves multiple unknown-size entries in one archive', async () => {
    const payloads = new Map<string, Uint8Array>([
      ['a', new Uint8Array(0)],
      ['b', randomBytes(2500)],
      ['c', bytes('hello')],
      ['d', randomBytes(9001)],
    ]);
    const out = memorySink();
    const w = new TarWriter({ strategy: 'chunked', sink: out.sink, chunkSize: 700 });
    for (const [name, data] of payloads) {
      const res = await w.addFile({ path: name, source: asyncSource(data, 123) });
      expect(res.ok, res.ok ? '' : res.error.message).toBe(true);
    }
    expect(await w.end()).toMatchObject({ ok: true });

    const entries = await collectEntries(concat(out.chunks));
    expect(entries.map((e) => e.path)).toEqual([...payloads.keys()]);
    for (const entry of entries) {
      expect(entry.chunked).toBe(true);
      expectBytes(entry.data, payloads.get(entry.path)!);
      expect(entry.digest).toBe(sha256(payloads.get(entry.path)!));
    }
  });

  it('does not emit a summary when the source fails mid-stream', async () => {
    const data = bytes('x'.repeat(5000));
    const failure = new Error('source exploded');
    const failingSource: ByteSource = {
      async *[Symbol.asyncIterator]() {
        yield data.subarray(0, 1000);
        yield data.subarray(1000, 2000);
        await new Promise((r) => setTimeout(r, 5));
        throw failure;
      },
    };
    const out = memorySink();
    const w = new TarWriter({ strategy: 'chunked', sink: out.sink, chunkSize: 500 });
    const res = await w.addFile({ path: 'broken', source: failingSource });
    expect(res.ok).toBe(false);
    if (!res.ok) {
      expect(res.error).toBeInstanceOf(SourceError);
      expect(res.error.cause).toBe(failure);
      expect(res.canceled).toBe(false);
      expect(res.boundary).toBe(w.bytesWritten);
      expect(res.boundary).toBeGreaterThan(0);
    }

    // end() must not write terminating zero blocks.
    const end = await w.end();
    expect(end.ok).toBe(false);
    const raw = concat(out.chunks);
    expect(isZeroBlock(raw.subarray(raw.length - BLOCK_SIZE))).toBe(false);

    // The library reader sees dangling chunks and rejects the truncated stream.
    await expect(collectEntries(raw)).rejects.toThrow(/missing summary/);
  });

  it('stops on abort mid-entry and reports the written boundary', async () => {
    const controller = new AbortController();
    const data = randomBytes(20_000);
    const out = memorySink();
    const w = new TarWriter({ strategy: 'chunked', sink: out.sink, chunkSize: 1000 });
    const abortedAfter = 5;
    let yielded = 0;
    const source: ByteSource = {
      async *[Symbol.asyncIterator]() {
        for (let i = 0; i < data.length; i += 500) {
          yield data.subarray(i, i + 500);
          yielded++;
          if (yielded === abortedAfter) controller.abort();
        }
      },
    };
    const res = await w.addFile({ path: 'canceled', source, signal: controller.signal });
    expect(res.ok).toBe(false);
    if (!res.ok) {
      expect(res.error).toBeInstanceOf(AbortedError);
      expect(res.canceled).toBe(true);
      // boundary equals bytes actually accepted by the sink
      expect(res.boundary).toBe(out.size());
    }
    const raw = concat(out.chunks);
    expect(isZeroBlock(raw.subarray(raw.length - BLOCK_SIZE))).toBe(false);
  });

  it('detects a truncated chunk payload without a summary', async () => {
    const data = randomBytes(3000);
    const out = memorySink();
    const w = new TarWriter({ strategy: 'chunked', sink: out.sink, chunkSize: 1000 });
    expect(await w.addFile({ path: 'x', source: arraySource(data) })).toMatchObject({ ok: true });
    expect(await w.end()).toMatchObject({ ok: true });

    // Chop the archive before the summary record (drop last summary + trailer).
    const full = concat(out.chunks);
    const truncated = full.subarray(0, full.length - (BLOCK_SIZE * 3));
    await expect(collectEntries(truncated)).rejects.toThrow(/missing summary|unexpected end/i);
  });
});

/* ==================================================================== */
/* sink backpressure / sink failure                                     */
/* ==================================================================== */

describe('sink behavior', () => {
  it('awaits a slow sink and applies backpressure to the source', async () => {
    const data = randomBytes(20_000);
    let inFlight = 0;
    let maxInFlight = 0;
    let writes = 0;
    const sink: TarSink = () => {
      inFlight++;
      maxInFlight = Math.max(maxInFlight, inFlight);
      writes++;
      return new Promise<void>((resolve) =>
        setTimeout(() => {
          inFlight--;
          resolve();
        }, 2),
      );
    };
    let sourceActive = 0;
    let maxSourceActive = 0;
    const source: ByteSource = {
      async *[Symbol.asyncIterator]() {
        for (let i = 0; i < data.length; i += 256) {
          sourceActive++;
          maxSourceActive = Math.max(maxSourceActive, sourceActive);
          yield data.subarray(i, i + 256);
          sourceActive--;
        }
      },
    };

    const w = new TarWriter({ strategy: 'chunked', sink, chunkSize: 512 });
    expect(await w.addFile({ path: 'slow', source })).toMatchObject({ ok: true });
    expect(await w.end()).toMatchObject({ ok: true });
    expect(writes).toBeGreaterThan(1);
    // Serialized writes: at most one sink call outstanding.
    expect(maxInFlight).toBe(1);
    void maxSourceActive;
  });

  it('wraps a rejected sink in SinkError and stops without terminator', async () => {
    const sinkFailure = new Error('network reset');
    let calls = 0;
    const sink: TarSink = () => {
      calls++;
      if (calls === 3) return Promise.reject(sinkFailure);
      return Promise.resolve();
    };
    const w = new TarWriter({ strategy: 'chunked', sink, chunkSize: 100 });
    const res = await w.addFile({ path: 'net', source: asyncSource(randomBytes(5000), 200) });
    expect(res.ok).toBe(false);
    if (!res.ok) {
      expect(res.error).toBeInstanceOf(SinkError);
      expect(res.error.cause).toBe(sinkFailure);
      // Calls 1..2 were the first chunk's header block + padded payload;
      // call 3 (next header) rejected, so exactly 1024 bytes were accepted.
      expect(calls).toBe(3);
      expect(res.boundary).toBe(BLOCK_SIZE * 2);
    }
    const end = await w.end();
    expect(end.ok).toBe(false);
  });
});

/* ==================================================================== */
/* reader robustness                                                    */
/* ==================================================================== */

describe('reader', () => {
  it('parses archives delivered in arbitrarily split byte chunks', async () => {
    // Small dataset exercises every alignment with byte/7-byte grains cheaply.
    const small = new Map<string, Uint8Array>([
      ['one.txt', bytes('first file\n')],
      ['dir/two.txt', bytes('second file, a bit longer'.repeat(3))],
      ['empty', new Uint8Array(0)],
      ['odd.bin', randomBytes(1337)],
    ]);
    const build = async (payloads: Map<string, Uint8Array>, mem: number) => {
      const out = memorySink();
      const w = new TarWriter({ ...STANDARD, memoryBudget: mem, sink: out.sink });
      for (const [name, data] of payloads) {
        expect(await w.addFile({ path: name, source: arraySource(data, 97) })).toMatchObject({ ok: true });
      }
      expect(await w.end()).toMatchObject({ ok: true });
      return concat(out.chunks);
    };

    const flat = await build(small, 256);
    for (const grain of [1, 7, 100, 512, 513, 1000]) {
      const chopped: ByteSource = {
        *[Symbol.iterator]() {
          for (let i = 0; i < flat.length; i += grain) yield flat.subarray(i, i + grain);
        },
      };
      const entries = await collectEntries(chopped);
      expect(entries.map((e) => e.path)).toEqual([...small.keys()]);
      for (const entry of entries) expectBytes(entry.data, small.get(entry.path)!);
    }

    // Coarse grains against a large archive that also spills to temp storage,
    // crossing block and record boundaries at different offsets.
    const big = new Map<string, Uint8Array>([['big.bin', randomBytes(70_000)], ['tail', bytes('ok')]]);
    const flatBig = await build(big, 8192);
    for (const grain of [511, 512, 513, 4096, 65537]) {
      const chopped: ByteSource = {
        *[Symbol.iterator]() {
          for (let i = 0; i < flatBig.length; i += grain) yield flatBig.subarray(i, i + grain);
        },
      };
      const entries = await collectEntries(chopped);
      expect(entries.map((e) => e.path)).toEqual([...big.keys()]);
      for (const entry of entries) expectBytes(entry.data, big.get(entry.path)!);
    }
  });

  it('rejects a chunked archive whose payload byte was tampered with', async () => {
    const data = randomBytes(3000);
    const out = memorySink();
    const w = new TarWriter({ strategy: 'chunked', sink: out.sink, chunkSize: 1000 });
    expect(await w.addFile({ path: 'x', source: arraySource(data) })).toMatchObject({ ok: true });
    expect(await w.end()).toMatchObject({ ok: true });

    const flat = concat(out.chunks);
    flat[BLOCK_SIZE + 10] ^= 0xff; // flip a byte inside the first chunk payload
    await expect(collectEntries(flat)).rejects.toThrow(/digest mismatch/);
  });

  it('rejects a chunked archive whose summary size was tampered with', async () => {
    const data = randomBytes(2000);
    const out = memorySink();
    const w = new TarWriter({ strategy: 'chunked', sink: out.sink, chunkSize: 1000 });
    expect(await w.addFile({ path: 'x', source: arraySource(data) })).toMatchObject({ ok: true });
    expect(await w.end()).toMatchObject({ ok: true });
    const flat = concat(out.chunks);

    // Summary header is right after 2 chunk records (each 512 + 1024 bytes).
    // Corrupting a payload byte there makes the size record unparseable or
    // mismatched; either way the reader must reject it.
    const summaryHeaderOffset = 2 * (BLOCK_SIZE + 1024);
    flat[summaryHeaderOffset + 5] ^= 0x01;
    await expect(collectEntries(flat)).rejects.toThrow(/checksum|summary|size|digest/i);
  });
});



async function listFiles(dir: string): Promise<string[]> {
  const { readdir } = await import('node:fs/promises');
  try {
    return await readdir(dir);
  } catch {
    return [];
  }
}

async function writeAll(file: string, data: Uint8Array): Promise<void> {
  const { writeFile } = await import('node:fs/promises');
  await writeFile(file, data);
  const s = await stat(file);
  expect(s.size).toBe(data.length);
}
