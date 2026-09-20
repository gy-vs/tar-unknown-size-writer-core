/**
 * Low level TAR block codec: 512-byte ustar headers, PAX extended records
 * and GNU long-name records. Nothing in here performs I/O.
 */
export const BLOCK_SIZE = 512;

const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8');

export function encodeText(text: string): Uint8Array {
  return encoder.encode(text);
}

export function decodeText(bytes: Uint8Array): string {
  return decoder.decode(bytes);
}

/** Rounded number of 512-byte blocks occupied by a payload of `size` bytes. */
export function paddedBlocks(size: number): number {
  return Math.ceil(size / BLOCK_SIZE);
}

export function paddedSize(size: number): number {
  return paddedBlocks(size) * BLOCK_SIZE;
}

/**
 * Parse a TAR octal field. TAR writes octal numbers NUL-terminated (and often
 * NUL-padded); some readers space-pad them. A fully empty field means zero.
 */
export function parseOctal(field: Uint8Array): number {
  let start = 0;
  let end = field.length;
  while (start < end && (field[start] === 0x20 || field[start] === 0x00)) start++;
  while (end > start && (field[end - 1] === 0x20 || field[end - 1] === 0x00)) end--;
  if (start === end) return 0;
  let value = 0;
  for (let i = start; i < end; i++) {
    const c = field[i];
    if (c < 0x30 || c > 0x37) throw new Error(`invalid octal digit 0o${c.toString(8)}`);
    value = value * 8 + (c - 0x30);
  }
  return value;
}

/** Fixed-width octal field: zero padded, terminated with NUL. */
export function formatOctal(value: number, width: number): Uint8Array {
  const out = new Uint8Array(width);
  let text = value.toString(8);
  if (text.length > width - 1) throw new Error(`octal value ${value} does not fit in ${width} bytes`);
  text = text.padStart(width - 1, '0');
  for (let i = 0; i < width - 1; i++) out[i] = text.charCodeAt(i);
  out[width - 1] = 0x00;
  return out;
}

export interface RawHeaderFields {
  name: string;
  /** Header payload size, octal-capable: this module never emits base-256. */
  size: number;
  mode: number;
  uid?: number;
  gid?: number;
  mtime: number;
  typeflag: string;
  linkname?: string;
  uname?: string;
  gname?: string;
  devmajor?: number;
  devminor?: number;
}

function putField(block: Uint8Array, offset: number, value: Uint8Array): void {
  block.set(value, offset);
}

function copyCString(block: Uint8Array, offset: number, length: number, text: string): void {
  const bytes = encodeText(text);
  if (bytes.length > length) throw new Error(`field of ${length} bytes cannot hold ${bytes.length} bytes`);
  block.set(bytes, offset);
}

export function buildHeader(fields: RawHeaderFields): Uint8Array {
  const block = new Uint8Array(BLOCK_SIZE);

  copyCString(block, 0, 100, fields.name);
  putField(block, 100, formatOctal(fields.mode, 8));
  putField(block, 108, formatOctal(fields.uid ?? 0, 8));
  putField(block, 116, formatOctal(fields.gid ?? 0, 8));
  putField(block, 124, formatOctal(fields.size, 12));
  putField(block, 136, formatOctal(fields.mtime, 12));
  // Checksum field is spaces while the real checksum is computed.
  block.fill(0x20, 148, 156);
  block[156] = fields.typeflag.charCodeAt(0);
  if (fields.linkname) copyCString(block, 157, 100, fields.linkname);
  copyCString(block, 257, 6, 'ustar');
  block[263] = 0x00; // '00' version
  block[264] = 0x00;
  copyCString(block, 265, 32, fields.uname ?? '');
  copyCString(block, 297, 32, fields.gname ?? '');
  putField(block, 329, formatOctal(fields.devmajor ?? 0, 8));
  putField(block, 337, formatOctal(fields.devminor ?? 0, 8));

  let checksum = 0;
  for (const byte of block) checksum += byte;
  const checksumText = checksum.toString(8).padStart(6, '0');
  for (let i = 0; i < 6; i++) block[148 + i] = checksumText.charCodeAt(i);
  block[154] = 0x00;
  block[155] = 0x20;
  return block;
}

export interface ParsedHeader {
  name: string;
  prefix: string;
  size: number;
  mode: number;
  uid: number;
  gid: number;
  mtime: number;
  typeflag: string;
  linkname: string;
  magic: string;
  uname: string;
  gname: string;
  checksumOk: boolean;
}

function sliceString(block: Uint8Array, start: number, end: number): string {
  let n = start;
  while (n < end && block[n] !== 0) n++;
  return decodeText(block.subarray(start, n));
}

export function isZeroBlock(block: Uint8Array): boolean {
  for (let i = 0; i < BLOCK_SIZE; i++) if (block[i] !== 0) return false;
  return true;
}

export function parseHeader(block: Uint8Array, verifyChecksum = true): ParsedHeader {
  const magic = decodeText(block.subarray(257, 263));
  let checksumOk = true;
  if (verifyChecksum) {
    const stored = parseOctal(block.subarray(148, 156));
    let actual = 0;
    for (let i = 0; i < BLOCK_SIZE; i++) {
      if (i >= 148 && i < 156) actual += 0x20;
      else actual += block[i];
    }
    checksumOk = stored === actual;
  }

  // Non-ustar (v7) archives store size in a 12-byte field the same way.
  let size = 0;
  try {
    size = parseOctal(block.subarray(124, 136));
  } catch {
    size = 0;
  }

  return {
    name: sliceString(block, 0, 100),
    prefix: sliceString(block, 345, 500),
    size,
    mode: safeOctal(block, 100, 108),
    uid: safeOctal(block, 108, 116),
    gid: safeOctal(block, 116, 124),
    mtime: safeOctal(block, 136, 148),
    typeflag: String.fromCharCode(block[156] ?? 0),
    linkname: sliceString(block, 157, 257),
    magic,
    uname: sliceString(block, 265, 297),
    gname: sliceString(block, 297, 329),
    checksumOk,
  };
}

function safeOctal(block: Uint8Array, start: number, end: number): number {
  try {
    return parseOctal(block.subarray(start, end));
  } catch {
    return 0;
  }
}

/** Combine a ustar prefix field with the name field. */
export function joinPrefixName(prefix: string, name: string): string {
  if (!prefix) return name;
  return `${prefix}/${name}`;
}

/**
 * Split a path for ustar name(100)/prefix(155) fields. Returns null when no
 * legal split exists; callers must then use PAX (or fail in strict ustar).
 */
export function splitUstarName(pathBytes: Uint8Array): { prefix: Uint8Array; name: Uint8Array } | null {
  if (pathBytes.length <= 100) return { prefix: new Uint8Array(0), name: pathBytes };
  // The split point must be a slash, both halves NUL-terminated fields.
  const minSplit = pathBytes.length - 100;
  for (let i = Math.min(pathBytes.length - 1, 155 + 1 - 1); i >= minSplit; i--) {
    if (pathBytes[i] === 0x2f /* '/' */) {
      const prefix = pathBytes.subarray(0, i);
      const name = pathBytes.subarray(i + 1);
      if (prefix.length <= 155 && name.length > 0 && name.length <= 100) {
        return { prefix, name };
      }
    }
  }
  return null;
}

/* ------------------------------------------------------------------ */
/* PAX extended records (typeflag 'x'/'g') and GNU longname ('L'/'K')  */
/* ------------------------------------------------------------------ */

export type PaxRecordMap = Map<string, string>;

function decimalLength(n: number): number {
  return String(n).length;
}

/**
 * Encode one PAX record: "%d %s=%s\n" where the decimal length covers the
 * whole record (digits, space, keyword, equals, value, newline) in bytes.
 */
export function encodePaxRecord(keyword: string, value: string): Uint8Array {
  const tail = ` ${keyword}=${value}\n`;
  const tailBytes = encodeText(tail);
  let length = tailBytes.length;
  // Adjust for the growing decimal length of the length field itself.
  let digitCount = decimalLength(length);
  for (;;) {
    const next = tailBytes.length + digitCount;
    if (next === length && String(next).length === digitCount) break;
    length = next;
    const newDigitCount = decimalLength(length);
    if (newDigitCount === digitCount) break;
    digitCount = newDigitCount;
  }
  const out = new Uint8Array(length);
  const digits = String(length);
  for (let i = 0; i < digits.length; i++) out[i] = digits.charCodeAt(i);
  out.set(tailBytes, digits.length);
  return out;
}

export function encodePaxRecords(records: Record<string, string> | PaxRecordMap): Uint8Array {
  const chunks: Uint8Array[] = [];
  let total = 0;
  const entries = records instanceof Map ? records.entries() : Object.entries(records);
  for (const [keyword, value] of entries) {
    const chunk = encodePaxRecord(keyword, value);
    chunks.push(chunk);
    total += chunk.length;
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

/** Parse a PAX payload into key/value pairs. Throws on malformed framing. */
export function parsePaxRecords(payload: Uint8Array): PaxRecordMap {
  const map: PaxRecordMap = new Map();
  let offset = 0;
  while (offset < payload.length) {
    const spaceAt = indexOfByte(payload, 0x20, offset);
    if (spaceAt < 0) throw new Error('malformed PAX record: missing length separator');
    const recordLength = Number(decodeText(payload.subarray(offset, spaceAt)));
    if (!Number.isInteger(recordLength) || recordLength <= 0 || offset + recordLength > payload.length) {
      throw new Error('malformed PAX record: bad record length');
    }
    const recordEnd = offset + recordLength;
    if (payload[recordEnd - 1] !== 0x0a /* '\n' */) {
      throw new Error('malformed PAX record: not newline terminated');
    }
    const equalsAt = indexOfByte(payload, 0x3d /* '=' */, spaceAt + 1);
    if (equalsAt < 0 || equalsAt >= recordEnd) throw new Error('malformed PAX record: missing "="');
    const keyword = decodeText(payload.subarray(spaceAt + 1, equalsAt));
    const value = decodeText(payload.subarray(equalsAt + 1, recordEnd - 1));
    map.set(keyword, value);
    offset = recordEnd;
  }
  return map;
}

function indexOfByte(bytes: Uint8Array, target: number, from: number): number {
  for (let i = from; i < bytes.length; i++) if (bytes[i] === target) return i;
  return -1;
}

/** Build a ustar header and prefix-split long paths when possible. */
export function buildUstarHeader(fields: RawHeaderFields): Uint8Array {
  const nameBytes = encodeText(fields.name);
  const split = splitUstarName(nameBytes);
  if (!split) throw new Error(`path ${fields.name} cannot be represented in ustar fields`);
  const block = buildHeader({
    ...fields,
    name: decodeText(split.name),
  });
  if (split.prefix.length > 0) block.set(split.prefix, 345);
  return block;
}
