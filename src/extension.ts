/**
 * Library-defined chunked extension for entries whose size is unknown until
 * the source finishes and whose output sink is non-seekable.
 *
 * Wire layout (every piece is an ordinary TAR header block + padded payload):
 *
 *   1. Zero or more data chunks, in order:
 *        typeflag 'C'  (vendor chunk), name "@ux/<id>/<seq>"
 *        payload: raw bytes of the entry
 *   2. Exactly one summary record:
 *        typeflag 'S'  (vendor summary), name "@ux/<id>"
 *        payload: PAX-framed records binding the logical entry together
 *
 * An empty source produces just the summary record (no chunks).
 *
 * The summary is the commit marker. Its records give the logical path, final
 * size and SHA-256 digest, so any truncation, dropped chunk or corruption is
 * detectable. Without a matching summary, the chunks are NOT a valid entry:
 * a reader that reaches end-of-archive with dangling chunks rejects them.
 *
 * These typeflags live in the vendor range; standard readers will list the
 * pieces as unknown-type entries, so interoperability is intentionally
 * limited to this library. Use the 'standard' strategy for portable output.
 */
import { PaxRecordMap, parsePaxRecords } from './blocks.js';

export const CHUNK_TYPEFLAG = 'C';
export const SUMMARY_TYPEFLAG = 'S';

const CHUNK_PREFIX = '@ux/';

/** Vendor PAX keywords used inside summary records. */
export const SUMMARY_KEYS = {
  path: 'uxlib.path',
  size: 'uxlib.size',
  sha256: 'uxlib.sha256',
  mode: 'uxlib.mode',
  mtime: 'uxlib.mtime',
} as const;

export function chunkName(id: string, seq: number): string {
  return `${CHUNK_PREFIX}${id}/${seq}`;
}

export function summaryName(id: string): string {
  return `${CHUNK_PREFIX}${id}`;
}

const CHUNK_NAME_RE = /^@ux\/([0-9a-f-]{8,64})\/(\d+)$/;
const SUMMARY_NAME_RE = /^@ux\/([0-9a-f-]{8,64})$/;

export function parseChunkName(name: string): { id: string; seq: number } | null {
  const match = CHUNK_NAME_RE.exec(name);
  if (!match) return null;
  return { id: match[1], seq: Number(match[2]) };
}

export function parseSummaryName(name: string): { id: string } | null {
  const match = SUMMARY_NAME_RE.exec(name);
  if (!match) return null;
  return { id: match[1] };
}

export interface SummaryInfo {
  path: string;
  size: number;
  sha256: string;
  mode: number;
  mtime: number;
}

export function parseSummaryPayload(payload: Uint8Array): SummaryInfo {
  const records: PaxRecordMap = parsePaxRecords(payload);
  const path = records.get(SUMMARY_KEYS.path);
  const sizeText = records.get(SUMMARY_KEYS.size);
  const sha256 = records.get(SUMMARY_KEYS.sha256);
  if (path === undefined || sizeText === undefined || sha256 === undefined) {
    throw new Error('chunked summary is missing path/size/sha256 records');
  }
  const size = Number(sizeText);
  if (!Number.isInteger(size) || size < 0) throw new Error('chunked summary has invalid size');
  return {
    path,
    size,
    sha256,
    mode: records.has(SUMMARY_KEYS.mode) ? parseInt(records.get(SUMMARY_KEYS.mode)!, 8) : 0o644,
    mtime: records.has(SUMMARY_KEYS.mtime) ? Number(records.get(SUMMARY_KEYS.mtime)!) : 0,
  };
}
