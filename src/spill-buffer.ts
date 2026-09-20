/**
 * SpillBuffer: prescan staging area for the standard strategy.
 *
 * The first `memoryThreshold` bytes are kept in memory. Any overflow is
 * streamed into a temp file, so disk usage is exactly
 *   max(0, totalSize - memoryThreshold)
 * and is bounded by `diskBudget`. Replay yields the in-memory prefix first,
 * followed by a streaming read of the overflow file — the full source is
 * never held in memory at once.
 */
import { createReadStream } from 'node:fs';
import { mkdtemp, open, rm, rmdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DiskBudgetExceededError } from './errors.js';

export interface SpillOptions {
  /** Bytes kept in RAM; bytes beyond this go to disk. */
  memoryThreshold: number;
  /** Maximum bytes allowed in the temp file (overflow size). */
  diskBudget: number;
  /** Override temp directory (tests). */
  tmpDir?: string;
}

export class SpillBuffer {
  readonly memoryThreshold: number;
  readonly diskBudget: number;
  #memory: Buffer[] = [];
  #memoryLength = 0;
  #filePath: string | null = null;
  #dirPath: string | null = null;
  #fd: Awaited<ReturnType<typeof open>> | null = null;
  #diskUsed = 0;
  #totalSize = 0;
  #cleaned = false;

  constructor(options: SpillOptions) {
    if (!Number.isSafeInteger(options.memoryThreshold) || options.memoryThreshold < 0) {
      throw new RangeError('memoryThreshold must be a non-negative safe integer');
    }
    if (
      options.diskBudget !== Infinity &&
      (!Number.isSafeInteger(options.diskBudget) || options.diskBudget < 0)
    ) {
      throw new RangeError('diskBudget must be a non-negative safe integer or Infinity');
    }
    this.memoryThreshold = options.memoryThreshold;
    this.diskBudget = options.diskBudget;
  }

  get size(): number {
    return this.#totalSize;
  }

  get diskBytes(): number {
    return this.#diskUsed;
  }

  get spilled(): boolean {
    return this.#fd !== null;
  }

  get filePath(): string | null {
    return this.#filePath;
  }

  async #ensureFile(): Promise<void> {
    if (this.#fd) return;
    const base = this.#dirPath ?? (await mkdtemp(join(tmpdir(), 'tar-spill-')));
    this.#dirPath = base;
    const filePath = join(base, 'spill.tmp');
    this.#fd = await open(filePath, 'wx');
    this.#filePath = filePath;
  }

  /**
   * Append a prescanned chunk. Throws DiskBudgetExceededError (before the
   * over-budget bytes hit disk) when the overflow file would grow past
   * diskBudget.
   *
   * @param archiveBoundary bytes already written to the archive (for the
   *   DiskBudgetExceededError boundary field).
   */
  async append(chunk: Buffer, archiveBoundary = 0, entryPath = ''): Promise<void> {
    if (this.#cleaned) throw new Error('SpillBuffer has already been cleaned up');
    if (chunk.length === 0) return;
    this.#totalSize += chunk.length;

    const roomInMemory = this.memoryThreshold - this.#memoryLength;
    if (!this.spilled && chunk.length <= roomInMemory) {
      this.#memory.push(chunk);
      this.#memoryLength += chunk.length;
      return;
    }

    let overflow: Buffer;
    if (!this.spilled && roomInMemory > 0) {
      // Split: keep the prefix in memory, spill only the overflow.
      this.#memory.push(chunk.subarray(0, roomInMemory));
      this.#memoryLength += roomInMemory;
      overflow = chunk.subarray(roomInMemory);
    } else {
      overflow = chunk;
    }

    const required = this.#diskUsed + overflow.length;
    if (required > this.diskBudget) {
      // Roll the total back: this chunk never entered the buffer. Callers
      // treat this as fatal anyway, but keep counters coherent.
      this.#totalSize -= chunk.length;
      throw new DiskBudgetExceededError(archiveBoundary, {
        path: entryPath,
        requiredBytes: required,
        diskBudget: this.diskBudget,
      });
    }

    await this.#ensureFile();
    await this.#fd!.write(overflow);
    this.#diskUsed += overflow.length;
  }

  /**
   * Replay all buffered bytes exactly once: in-memory prefix then the
   * overflow file streamed from disk.
   */
  async *replay(): AsyncIterableIterator<Buffer> {
    if (this.#cleaned) throw new Error('SpillBuffer has already been cleaned up');
    if (this.#memoryLength > 0) {
      yield Buffer.concat(this.#memory);
    }
    if (this.#filePath) {
      const stream = createReadStream(this.#filePath);
      try {
        for await (const part of stream) yield part as Buffer;
      } finally {
        stream.destroy();
      }
    }
  }

  async cleanup(): Promise<void> {
    if (this.#cleaned) return;
    this.#cleaned = true;
    this.#memory = [];
    if (this.#fd) {
      try {
        await this.#fd.close();
      } catch {
        // best effort
      }
      this.#fd = null;
    }
    if (this.#dirPath) {
      await rm(this.#dirPath, { recursive: true, force: true }).catch(() => {});
      try {
        await rmdir(this.#dirPath);
      } catch {
        // already removed with the recursive rm
      }
      this.#dirPath = null;
      this.#filePath = null;
    }
  }
}
