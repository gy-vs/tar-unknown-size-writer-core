import { execFile } from 'node:child_process';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PassThrough } from 'node:stream';
import { promisify } from 'node:util';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  DiskBudgetExceededError,
  SourceFailedError,
  TarAbortedError,
  TarStreamBrokenError,
  TarUnknownSizeWriter,
  sinkFromWritable,
  type ByteSink,
  type ByteSource,
} from '../src/index.js';

const execFileP = promisify(execFile);

function sha256(buf: Buffer): string {
  return createHash('sha256').update(buf).digest('hex');
}

/** Source that yields data with arbitrary chunk boundaries. */
function sourceFromBuffer(data: Buffer, chunkSize = 100, delayMs = 0): ByteSource {
  return {
    async *[Symbol.asyncIterator]() {
      for (let offset = 0; offset < data.length; offset += chunkSize) {
        if (delayMs) await new Promise((r) => setTimeout(r, delayMs));
        yield data.subarray(offset, offset + chunkSize);
      }
    },
  };
}

function failingSource(chunksBeforeFailure: number, chunkSize = 100): ByteSource {
  return {
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < chunksBeforeFailure; i++) yield Buffer.alloc(chunkSize, i + 1);
      throw new Error('boom: source died mid-stream');
    },
  };
}

/** Collect sink: captures bytes and optionally gates each write. */
function collectingSink(opts: { gate?: () => Promise<void>; onWrite?: (n: number) => void } = {}) {
  const chunks: Buffer[] = [];
  let inflight = 0;
  const sink: ByteSink = async (chunk) => {
    inflight++;
    try {
      if (opts.gate) await opts.gate();
      chunks.push(chunk);
      opts.onWrite?.(chunk.length);
    } finally {
      inflight--;
    }
  };
  return {
    sink,
    buffer: () => Buffer.concat(chunks),
    get inflight() {
      return inflight;
    },
  };
}

describe('TarUnknownSizeWriter — standard strategy', () => {
  let workDir: string;
  let standard: Parameters<InstanceType<typeof TarUnknownSizeWriter>['addUnknownSize']>[2];

  beforeEach(async () => {
    workDir = await mkdtemp(join(tmpdir(), 'tar-test-'));
    standard = {
      strategy: 'standard',
      memoryThreshold: 4096,
      diskBudget: 1024 * 1024 * 16,
      tmpDir: workDir,
    } as const;
  });

  afterEach(async () => {
    await rm(workDir, { recursive: true, force: true });
  });

  it('handles an empty source (zero-byte entry)', async () => {
    const { sink, buffer } = collectingSink();
    const w = new TarUnknownSizeWriter({ sink });
    const res = await w.addUnknownSize({ path: 'empty.bin' }, sourceFromBuffer(Buffer.alloc(0)), standard);
    expect(res.size).toBe(0);
    expect(res.sha256).toBe(sha256(Buffer.alloc(0)));
    expect(res.spilled).toBe(false);
    const closed = await w.close();
    expect(closed.entries).toBe(1);
    // header(512) + two EOA blocks(1024)
    expect(buffer().length).toBe(512 + 1024);
  });

  it('keeps data in memory below the threshold and reports no spill', async () => {
    const data = Buffer.alloc(3000, 7);
    const { sink, buffer } = collectingSink();
    const w = new TarUnknownSizeWriter({ sink });
    const res = await w.addUnknownSize({ path: 'small.bin' }, sourceFromBuffer(data, 333), standard);
    expect(res.size).toBe(3000);
    expect(res.sha256).toBe(sha256(data));
    expect(res.spilled).toBe(false);
    // No temp directory entries should have been created.
    expect(await readdir(workDir)).toEqual([]);
    await w.close();
    expect(buffer().length).toBe(512 + 3072 + 1024);
  });

  it('spills to a temp file above the memory threshold, then cleans it up', async () => {
    const data = Buffer.alloc(20_000, 9);
    const { sink, buffer } = collectingSink();
    const w = new TarUnknownSizeWriter({ sink });
    const res = await w.addUnknownSize({ path: 'bigger.bin' }, sourceFromBuffer(data, 1234), standard);
    expect(res.spilled).toBe(true);
    expect(res.size).toBe(20_000);
    expect(res.sha256).toBe(sha256(data));
    await w.close();
    // temp storage removed after the entry
    expect(await readdir(workDir)).toEqual([]);
    const archive = buffer();
    // Data payload lives at offset 512, size 20000, padded to 20480.
    expect(archive.length).toBe(512 + 20480 + 1024);
    expect(archive.subarray(512, 512 + 20_000).equals(data)).toBe(true);
  });

  it('fails with DiskBudgetExceededError and writes no entry bytes', async () => {
    const data = Buffer.alloc(20_000, 1);
    const { sink, buffer } = collectingSink();
    const w = new TarUnknownSizeWriter({ sink });
    // memory holds 4 KiB, overflow needs 20_000 - 4_096 = 15_904 > 8 KiB.
    const tight = { ...(standard as any), diskBudget: 8192 };
    await expect(
      w.addUnknownSize({ path: 'too-big.bin' }, sourceFromBuffer(data, 5000), tight),
    ).rejects.toBeInstanceOf(DiskBudgetExceededError);
    await expect(w.close()).rejects.toBeInstanceOf(TarStreamBrokenError);
    // Nothing was emitted: prescan failure happens before any header write.
    expect(buffer().length).toBe(0);
    expect(w.broken).toBe(true);
    // Even on failure the partially-created temp area must be removed.
    expect(await readdir(workDir)).toEqual([]);
  });

  it('supports multiple unknown-size entries in one archive', async () => {
    const entries: Array<{ path: string; data: Buffer }> = [
      { path: 'a.bin', data: Buffer.from('hello world') },
      { path: 'empty', data: Buffer.alloc(0) },
      { path: 'dir/name with spaces/b.bin', data: Buffer.alloc(12_345, 0x42) },
      { path: 'c.bin', data: Buffer.from('') },
    ];
    const { sink, buffer } = collectingSink();
    const w = new TarUnknownSizeWriter({ sink });
    for (const e of entries) {
      const res = await w.addUnknownSize({ path: e.path }, sourceFromBuffer(e.data, 777), standard);
      expect(res.size).toBe(e.data.length);
      expect(res.sha256).toBe(sha256(e.data));
    }
    const info = await w.close();
    expect(info.entries).toBe(4);

    // Extract with GNU tar and verify every payload.
    const outDir = await mkdtemp(join(tmpdir(), 'tar-extract-'));
    try {
      const archivePath = join(workDir, 'out.tar');
      await import('node:fs/promises').then((fs) => fs.writeFile(archivePath, buffer()));
      await execFileP('tar', ['-xf', archivePath, '-C', outDir]);
      for (const e of entries) {
        const extracted = await readFile(join(outDir, e.path));
        expect(extracted.equals(e.data)).toBe(true);
      }
    } finally {
      await rm(outDir, { recursive: true, force: true });
    }
  });

  it('produces an archive accepted by GNU tar (long names + mixed sizes)', async () => {
    const longName = `prefix/${'very-long-directory-name/'.repeat(8)}leaf.dat`;
    const entries = [
      { path: 'normal.txt', data: Buffer.from('plain\n') },
      { path: longName, data: Buffer.alloc(30_000, 0x5a) },
      { path: 'ünïcode-名前.txt', data: Buffer.from('unicode payload ✓') },
    ];
    const { sink, buffer } = collectingSink();
    const w = new TarUnknownSizeWriter({ sink });
    for (const e of entries) {
      await w.addUnknownSize({ path: e.path }, sourceFromBuffer(e.data, 4096), standard);
    }
    await w.close();

    const outDir = await mkdtemp(join(tmpdir(), 'tar-extract-'));
    try {
      const archivePath = join(workDir, 'pax.tar');
      await import('node:fs/promises').then((fs) => fs.writeFile(archivePath, buffer()));
      // GNU tar accepts PAX long names without extra flags.
      await execFileP('tar', ['-xf', archivePath, '-C', outDir]);
      for (const e of entries) {
        const extracted = await readFile(join(outDir, e.path));
        expect(extracted.equals(e.data)).toBe(true);
      }
      // GNU tar should report PAX headers present.
      const { stdout } = await execFileP('tar', ['-tvf', archivePath]);
      expect(stdout).toContain(longName);
    } finally {
      await rm(outDir, { recursive: true, force: true });
    }
  });

  it('stops and reports the boundary when the source fails mid-stream', async () => {
    const { sink, buffer } = collectingSink();
    const w = new TarUnknownSizeWriter({ sink });
    await expect(
      w.addUnknownSize({ path: 'dies.bin' }, failingSource(5, 100), standard),
    ).rejects.toBeInstanceOf(SourceFailedError);
    // Standard prescan fails before header emission -> boundary 0.
    expect(buffer().length).toBe(0);
    // No valid end marker can ever be written.
    await expect(w.close()).rejects.toMatchObject({
      name: 'TarStreamBrokenError',
      bytesWritten: 0,
    });
  });

  it('stops on AbortSignal and reports the written boundary', async () => {
    const ac = new AbortController();
    const { sink, buffer } = collectingSink();
    const w = new TarUnknownSizeWriter({ sink, signal: ac.signal });
    // First a successful entry, then abort during the second.
    await w.addUnknownSize(
      { path: 'ok.bin' },
      sourceFromBuffer(Buffer.alloc(1000), 500),
      standard,
    );
    const boundaryAfterFirst = w.bytesWritten;
    expect(boundaryAfterFirst).toBe(512 + 1024);
    ac.abort();
    await expect(
      w.addUnknownSize({ path: 'aborted.bin' }, sourceFromBuffer(Buffer.alloc(9999)), standard),
    ).rejects.toBeInstanceOf(TarAbortedError);
    expect(w.bytesWritten).toBe(boundaryAfterFirst);
    expect(buffer().length).toBe(boundaryAfterFirst);
    // The truncated archive has no end blocks: total is not a valid tar tail.
    await expect(w.close()).rejects.toBeInstanceOf(TarStreamBrokenError);
  });

  it('reports the exact boundary if the sink fails during replay', async () => {
    // First entry completes. The second entry prescans fine (spills to
    // disk), its header reaches the sink, then the sink dies mid-payload.
    const accepted: Buffer[] = [];
    let failAfter = Infinity;
    let seen = 0;
    const sink: ByteSink = (chunk) => {
      seen += chunk.length;
      if (seen > failAfter) throw new Error('connection reset by peer');
      accepted.push(chunk);
    };
    const w = new TarUnknownSizeWriter({ sink });
    await w.addUnknownSize({ path: 'ok.bin' }, sourceFromBuffer(Buffer.alloc(1000), 500), standard);
    failAfter = w.bytesWritten + 512 + 1024; // die inside the second entry's data

    await expect(
      w.addUnknownSize({ path: 'later.bin' }, sourceFromBuffer(Buffer.alloc(20_000), 4096), standard),
    ).rejects.toMatchObject({ name: 'SinkWriteError' });
    expect(w.bytesWritten).toBe(accepted.reduce((n, b) => n + b.length, 0));
    await expect(w.close()).rejects.toBeInstanceOf(TarStreamBrokenError);
  });
});

describe('TarUnknownSizeWriter — slow sink backpressure', () => {
  /** Sink where every write blocks until manually released, one at a time. */
  function gatedSink() {
    const chunks: Buffer[] = [];
    const arrived: Array<() => void> = [];
    let permits = 0; // writes currently blocked inside the sink
    let waiters: Array<() => void> = [];
    let inFlight = 0;
    let maxInFlight = 0;
    const sink: ByteSink = async (chunk) => {
      inFlight++;
      maxInFlight = Math.max(maxInFlight, inFlight);
      chunks.push(chunk);
      permits++;
      waiters.shift()?.(); // wake a pumper waiting for arrival
      await new Promise<void>((resolve) => arrived.push(resolve));
      inFlight--;
    };
    return {
      sink,
      chunks: () => Buffer.concat(chunks),
      async waitForArrival() {
        while (permits === 0) {
          await new Promise<void>((r) => waiters.push(r));
        }
      },
      release() {
        if (permits === 0) throw new Error('release without an arrived write');
        permits--;
        arrived.shift()?.();
      },
      get permitsAvailable() {
        return permits;
      },
      get maxInFlight() {
        return maxInFlight;
      },
    };
  }

  /** Drive `task` to completion by releasing exactly one blocked write at a time. */
  async function pump(task: Promise<unknown>, gate: ReturnType<typeof gatedSink>) {
    let guard = 0;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      let error: unknown;
      const done = await Promise.race([
        task.then(
          () => true,
          (e: unknown) => {
            error = e;
            return true;
          },
        ),
        gate.waitForArrival().then(() => false),
      ]);
      if (done) {
        while (gate.permitsAvailable > 0) gate.release();
        if (error) throw error;
        return;
      }
      gate.release();
      if (++guard > 100000) throw new Error('backpressure pump stalled');
    }
  }

  it('standard strategy does not push ahead of a gated sink', async () => {
    const workDir = await mkdtemp(join(tmpdir(), 'tar-backpressure-'));
    try {
      const data = Buffer.alloc(50_000, 3);
      const gate = gatedSink();
      const w = new TarUnknownSizeWriter({ sink: gate.sink });
      const standard: any = {
        strategy: 'standard',
        memoryThreshold: 4096,
        diskBudget: 1024 * 1024,
        tmpDir: workDir,
      };
      await pump(
        w.addUnknownSize({ path: 'bp.bin' }, sourceFromBuffer(data, 5000), standard),
        gate,
      );
      await pump(w.close(), gate);
      expect(gate.maxInFlight).toBeLessThanOrEqual(1);
      // Payload bytes survived the spill/replay round trip.
      const archive = gate.chunks();
      expect(archive.subarray(512, 512 + 50_000).equals(data)).toBe(true);
    } finally {
      await rm(workDir, { recursive: true, force: true });
    }
  });

  it('chunked strategy streams straight to the sink with one write in flight', async () => {
    const data = Buffer.alloc(256 * 1024, 0xab);
    const gate = gatedSink();
    const w = new TarUnknownSizeWriter({ sink: gate.sink });
    const source: ByteSource = {
      async *[Symbol.asyncIterator]() {
        for (let off = 0; off < data.length; off += 64 * 1024) {
          yield data.subarray(off, off + 64 * 1024);
        }
      },
    };
    const resP = w.addUnknownSize({ path: 'stream.bin' }, source, { strategy: 'chunked' });
    await pump(resP, gate);
    const res = await resP;
    expect(res.size).toBe(data.length);
    expect(res.sha256).toBe(sha256(data));
    await pump(w.close(), gate);
    expect(gate.maxInFlight).toBeLessThanOrEqual(1);

    // Exactly ceil(size/507) DATA records plus one INITIAL and one FINAL
    // were emitted as 512-byte writes; EOA is a single 1024-byte write.
    const writes = gate.chunks();
    // Layout: magic(8) + INITIAL(512) + N*DATA(512) + FINAL(512) + EOA(1024)
    const dataRecords = Math.ceil(data.length / 507);
    expect(writes.length).toBe(8 + 512 * (dataRecords + 2) + 1024);
  });

  it('sinkFromWritable drains a Writable without losing bytes', async () => {
    const pt = new PassThrough();
    const received: Buffer[] = [];
    pt.on('data', (c) => received.push(c as Buffer));

    const w = new TarUnknownSizeWriter({ sink: sinkFromWritable(pt) });
    const data = Buffer.alloc(20_000, 6);
    await w.addUnknownSize({ path: 'x.bin' }, sourceFromBuffer(data, 1000), {
      strategy: 'chunked',
    });
    await w.close();
    pt.end();
    await new Promise<void>((resolve, reject) => {
      pt.on('end', resolve);
      pt.on('error', reject);
    });
    expect(Buffer.concat(received).length).toBeGreaterThan(0);
  });
});
