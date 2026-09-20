import { mkdtemp, readdir, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import {
  BLOCK_SIZE,
  SpillBuffer,
  TarUnknownSizeWriter,
  USTAR_OCTAL_SIZE_LIMIT,
  buildUstarHeader,
  encodePaxRecords,
  entryHeaderBlocks,
  type ByteSource,
} from '../src/index.js';

describe('ustar header', () => {
  it('writes a checksummed header that GNU tar recognizes structurally', () => {
    const h = buildUstarHeader({ name: 'a/b.txt', size: 5, typeflag: '0', mode: 0o644 });
    expect(h.length).toBe(512);
    expect(h.toString('ascii', 257, 262)).toBe('ustar');
    // Verify checksum independently (checksum field counts as spaces).
    let chk = 0;
    for (let i = 0; i < 512; i++) chk += i >= 148 && i < 156 ? 0x20 : h[i];
    expect(h.toString('ascii', 148, 154)).toBe(chk.toString(8).padStart(6, '0'));
    expect(h.readUIntLE(124, 4)).toBeGreaterThan(0); // size octal non-empty
  });

  it('rejects sizes above the ustar octal field limit for the raw header', () => {
    expect(() =>
      buildUstarHeader({ name: 'x', size: USTAR_OCTAL_SIZE_LIMIT + 1, typeflag: '0' }),
    ).toThrow(/octal/);
  });

  it('emits PAX records only when required (long path or >8GiB)', () => {
    const short = entryHeaderBlocks('short', 10, { paxSerial: 0 });
    expect(short.length).toBe(1);

    // Path of 101 bytes -> PAX record body exceeds 512 bytes
    // ("<len> path=xxx...\n" ~115 bytes padded to one block):
    // blocks are [pax header][pax body block][ustar header] = 3.
    const long = entryHeaderBlocks('x'.repeat(101), 10, { paxSerial: 1 });
    expect(long.length).toBe(3); // pax header + 1 padded pax body block + ustar header
    expect(long[0][156]).toBe('x'.charCodeAt(0)); // typeflag 'x'
    expect(long[0].toString('ascii', 257, 262)).toBe('ustar');
    // PAX body carries the real path.
    expect(long[1].toString('utf8')).toContain(`path=${'x'.repeat(101)}\n`);

    const huge = entryHeaderBlocks('ok', USTAR_OCTAL_SIZE_LIMIT + 1, { paxSerial: 2 });
    expect(huge.length).toBe(3);
    expect(huge[1].toString('utf8')).toContain('size=');
  });
});

describe('PAX record encoding', () => {
  it('computes self-referential lengths with fixed point', () => {
    const body = encodePaxRecords([['path', 'a'.repeat(200)]]);
    // Parse: "<len> path=aaa...\n"
    const space = body.indexOf(0x20);
    const len = Number(body.toString('ascii', 0, space));
    expect(len).toBe(body.length);
    expect(body.toString('utf8', space + 1)).toBe(`path=${'a'.repeat(200)}\n`);
  });

  it('lengths stay correct when digit count crosses powers of ten', () => {
    for (const n of [8, 9, 98, 99, 997, 998, 999, 1000]) {
      const body = encodePaxRecords([['path', 'v'.repeat(n)]]);
      const space = body.indexOf(0x20);
      expect(Number(body.toString('ascii', 0, space))).toBe(body.length);
    }
  });
});

describe('SpillBuffer', () => {
  it('keeps bytes in memory up to the threshold', async () => {
    const b = new SpillBuffer({ memoryThreshold: 100, diskBudget: 1000 });
    await b.append(Buffer.alloc(60));
    await b.append(Buffer.alloc(40));
    expect(b.spilled).toBe(false);
    expect(b.size).toBe(100);
    const out: Buffer[] = [];
    for await (const c of b.replay()) out.push(c);
    expect(Buffer.concat(out).length).toBe(100);
  });

  it('splits a chunk at the threshold and spills only the overflow', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'spill-unit-'));
    try {
      const b = new SpillBuffer({ memoryThreshold: 10, diskBudget: 100, tmpDir: dir });
      await b.append(Buffer.from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]));
      expect(b.spilled).toBe(true);
      expect(b.diskBytes).toBe(2);
      expect(b.size).toBe(12);
      const out: Buffer[] = [];
      for await (const c of b.replay()) out.push(c);
      expect(Buffer.concat(out)).toEqual(Buffer.from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]));
      await b.cleanup();
      expect(await readdir(dir)).toEqual([]);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('rejects overflow beyond disk budget before writing the excess', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'spill-budget-'));
    try {
      const b = new SpillBuffer({ memoryThreshold: 4, diskBudget: 2, tmpDir: dir });
      await b.append(Buffer.from([1, 2, 3, 4]));
      await expect(b.append(Buffer.from([5, 6, 7, 8]), 123, 'entry/path')).rejects.toMatchObject({
        name: 'DiskBudgetExceededError',
        bytesWritten: 123,
        path: 'entry/path',
        requiredBytes: 4,
        diskBudget: 2,
      });
      await b.cleanup();
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});

describe('TarUnknownSizeWriter — known-size entries', () => {
  it('writes a plain ustar entry with matching declared length', async () => {
    const out: Buffer[] = [];
    const w = new TarUnknownSizeWriter({ sink: (c) => out.push(c) });
    const data = Buffer.from('known!');
    const src: ByteSource = {
      async *[Symbol.asyncIterator]() {
        yield data;
      },
    };
    const res = await w.addEntry({ path: 'k.txt' }, src, data.length);
    expect(res.size).toBe(6);
    await w.close();
    const archive = Buffer.concat(out);
    expect(archive.length).toBe(512 + 512 + 1024);
    expect(archive.subarray(512, 518).toString()).toBe('known!');
  });

  it('refuses a source that overruns its declared size without emitting EOA', async () => {
    const out: Buffer[] = [];
    const w = new TarUnknownSizeWriter({ sink: (c) => out.push(c) });
    const src: ByteSource = {
      async *[Symbol.asyncIterator]() {
        yield Buffer.alloc(10);
      },
    };
    await expect(w.addEntry({ path: 'k' }, src, 5)).rejects.toThrow(/more bytes/);
    expect(w.broken).toBe(true);
    await expect(w.close()).rejects.toThrow(/broken/);
  });
});
