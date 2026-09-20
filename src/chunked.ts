/**
 * Library-defined chunked extension for sources of unknown final size.
 *
 * Archive layout (chunked streams start with an explicit magic so they
 * cannot be mistaken for a truncated ustar stream):
 *
 *   magic      8 bytes  "TARCHNK1"
 *   entry*               one or more entries
 *   eoa        2 zero blocks (1024 bytes, only after a successful close)
 *
 * Per entry every record is exactly one 512-byte block:
 *
 *   INITIAL  [0]=0x00  [1..511] entry path, UTF-8 NUL-padded
 *   DATA     [0]=0x01  [1..4]   uint32 BE logical payload length (<=507)
 *                     [5..511] payload bytes, NUL-padded
 *   FINAL    [0]=0x02  [1..511] JSON {"size":<bytes>,"sha256":"<hex>"}
 *
 * Records are self-describing, so a DATA payload may contain any bytes
 * (a payload starting with 0x02 is not mistaken for FINAL). The writer
 * only buffers one DATA payload (507 bytes) ahead of the sink, so slow
 * sinks exert direct backpressure and memory stays bounded. A stream that
 * ends before FINAL (source failure, abort, sink disconnect) is
 * structurally incomplete and must be rejected.
 */
import { createHash } from 'node:crypto';
import { BLOCK_SIZE } from './header.js';
import { TarParseError } from './errors.js';

export const CHUNKED_MAGIC = Buffer.from('TARCHNK1', 'ascii');
export const FLAG_INITIAL = 0x00;
export const FLAG_DATA = 0x01;
export const FLAG_FINAL = 0x02;

/** Bytes available for payload inside a DATA record. */
export const DATA_PAYLOAD_SIZE = BLOCK_SIZE - 5; // 507

export interface ChunkedFinal {
  size: number;
  sha256: string;
}

/** INITIAL block: flag byte followed by the UTF-8 path. */
export function buildInitialBlock(path: string): Buffer {
  const name = Buffer.from(path, 'utf8');
  if (name.length > BLOCK_SIZE - 1) {
    throw new TarParseError(`chunked entry path exceeds ${BLOCK_SIZE - 1} bytes`);
  }
  const block = Buffer.alloc(BLOCK_SIZE);
  block[0] = FLAG_INITIAL;
  name.copy(block, 1);
  return block;
}

/** DATA record: flag + uint32 length + payload (already <= 507 bytes). */
export function buildDataBlock(payload: Buffer, length = payload.length): Buffer {
  if (length > DATA_PAYLOAD_SIZE) {
    throw new TarParseError(`DATA payload of ${length} bytes exceeds ${DATA_PAYLOAD_SIZE}`);
  }
  const block = Buffer.alloc(BLOCK_SIZE);
  block[0] = FLAG_DATA;
  block.writeUInt32BE(length, 1);
  payload.copy(block, 5, 0, length);
  return block;
}

/** FINAL block: flag byte followed by JSON payload. */
export function buildFinalBlock(final: ChunkedFinal): Buffer {
  const json = Buffer.from(JSON.stringify(final), 'utf8');
  if (json.length > BLOCK_SIZE - 1) {
    throw new TarParseError('chunked final record exceeds one block');
  }
  const block = Buffer.alloc(BLOCK_SIZE);
  block[0] = FLAG_FINAL;
  json.copy(block, 1);
  return block;
}

export function parseInitialBlock(block: Buffer): string {
  if (block.length !== BLOCK_SIZE || block[0] !== FLAG_INITIAL) {
    throw new TarParseError('expected a chunked INITIAL block');
  }
  let end = 1;
  while (end < BLOCK_SIZE && block[end] !== 0) end++;
  return block.toString('utf8', 1, end);
}

/** Parse a DATA record header; returns the logical payload length. */
export function parseDataBlock(block: Buffer): number {
  if (block.length !== BLOCK_SIZE || block[0] !== FLAG_DATA) {
    throw new TarParseError('expected a chunked DATA record');
  }
  const length = block.readUInt32BE(1);
  if (length > DATA_PAYLOAD_SIZE) {
    throw new TarParseError(`DATA record declares invalid payload length ${length}`);
  }
  return length;
}

/** Extract the payload slice of a DATA record (no copy; caller finishes before reuse). */
export function dataPayload(block: Buffer, length: number): Buffer {
  return block.subarray(5, 5 + length);
}

export function parseFinalBlock(block: Buffer): ChunkedFinal {
  if (block.length !== BLOCK_SIZE || block[0] !== FLAG_FINAL) {
    throw new TarParseError('expected a chunked FINAL block');
  }
  let end = 1;
  while (end < BLOCK_SIZE && block[end] !== 0) end++;
  let parsed: unknown;
  try {
    parsed = JSON.parse(block.toString('utf8', 1, end));
  } catch (cause) {
    throw new TarParseError('chunked FINAL block does not contain valid JSON', { cause });
  }
  const rec = parsed as Partial<ChunkedFinal>;
  if (
    typeof rec !== 'object' ||
    rec === null ||
    !Number.isSafeInteger(rec.size) ||
    (rec.size as number) < 0 ||
    typeof rec.sha256 !== 'string' ||
    !/^[0-9a-f]{64}$/.test(rec.sha256)
  ) {
    throw new TarParseError('chunked FINAL block has invalid size/sha256 fields');
  }
  return { size: rec.size as number, sha256: rec.sha256 };
}

export function createEntryHash(): import('node:crypto').Hash {
  return createHash('sha256');
}
