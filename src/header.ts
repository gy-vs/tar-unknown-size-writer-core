/**
 * ustar header and PAX local extended-header encoding.
 *
 * The standard strategy only ever emits constructs accepted by stock TAR
 * readers: regular ustar headers (typeflag `0`), directory headers (`5`)
 * and, only when required, PAX local extended headers (`x`) carrying
 * `path` (long / non-ASCII names) or `size` (>= 8 GiB).
 */

export const BLOCK_SIZE = 512;

/** Largest size representable in a ustar 11-byte octal size field. */
export const USTAR_OCTAL_SIZE_LIMIT = 0o77777777777; // 2^33 - 1 = 8 GiB - 1

export type UstarTypeFlag = '0' | '5' | 'x';

export interface HeaderFields {
  name: string;
  size: number;
  typeflag: UstarTypeFlag;
  mode?: number;
  mtime?: number;
  linkname?: string;
}

const MAGIC = Buffer.from('ustar\0', 'ascii');
const VERSION = Buffer.from('00', 'ascii');

function writeOctal(buf: Buffer, offset: number, width: number, value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`invalid octal field value: ${value}`);
  }
  // `width - 1` octal digits followed by NUL (classic ustar form).
  const digits = value.toString(8);
  if (digits.length > width - 1) {
    throw new RangeError(`value ${value} does not fit in ${width}-byte octal field`);
  }
  buf.write(digits.padStart(width - 1, '0'), offset, width - 1, 'ascii');
  buf[offset + width - 1] = 0;
}

function writeStringField(buf: Buffer, offset: number, width: number, value: string): void {
  const encoded = Buffer.from(value, 'utf8');
  if (encoded.length > width) {
    throw new RangeError(`field value exceeds ${width} bytes`);
  }
  encoded.copy(buf, offset);
}

/** Build one 512-byte ustar header block (checksum computed). */
export function buildUstarHeader(fields: HeaderFields): Buffer {
  const block = Buffer.alloc(BLOCK_SIZE);
  writeStringField(block, 0, 100, fields.name);
  writeOctal(block, 100, 8, fields.mode ?? (fields.typeflag === '5' ? 0o755 : 0o644));
  writeOctal(block, 108, 8, 0); // uid
  writeOctal(block, 116, 8, 0); // gid
  writeOctal(block, 124, 12, fields.size);
  writeOctal(block, 136, 12, Math.floor(fields.mtime ?? 0));
  // checksum placeholder: eight spaces
  block.write('        ', 148, 8, 'ascii');
  block[156] = fields.typeflag.charCodeAt(0);
  writeStringField(block, 157, 100, fields.linkname ?? '');
  MAGIC.copy(block, 257);
  VERSION.copy(block, 263);
  block.write('root', 265, 32, 'ascii'); // uname
  block.write('root', 297, 32, 'ascii'); // gname
  writeOctal(block, 329, 8, 0); // devmajor
  writeOctal(block, 337, 8, 0); // devminor

  let checksum = 0;
  for (let i = 0; i < BLOCK_SIZE; i++) checksum += block[i];
  block.write(checksum.toString(8).padStart(6, '0'), 148, 6, 'ascii');
  block[154] = 0;
  block[155] = 0x20;
  return block;
}

/** Number of zero padding bytes following `size` data bytes. */
export function paddingSize(size: number): number {
  return (BLOCK_SIZE - (size % BLOCK_SIZE)) % BLOCK_SIZE;
}

export function zeroPadding(size: number): Buffer {
  return Buffer.alloc(paddingSize(size));
}

/**
 * Encode PAX extended-header records.
 * Format per record: "<length> <keyword>=<value>\n" where <length> is the
 * total record length in bytes including the length field itself.
 */
export function encodePaxRecords(records: ReadonlyArray<readonly [string, string]>): Buffer {
  const parts: Buffer[] = [];
  for (const [keyword, value] of records) {
    const tail = Buffer.from(` ${keyword}=${value}\n`, 'utf8');
    // Resolve the self-referential length field by fixed point:
    // len = digits(len) + tail.length. Grow until stable.
    let length = tail.length + 1;
    let digits = String(length).length;
    let next = tail.length + digits;
    while (next !== length) {
      length = next;
      const nextDigits = String(length).length;
      next = tail.length + nextDigits;
      if (nextDigits !== digits) digits = nextDigits;
      else length = next;
    }
    parts.push(Buffer.from(String(length), 'ascii'), tail);
  }
  return Buffer.concat(parts);
}

/**
 * Header blocks that must precede a regular entry: an optional PAX local
 * extended header (when the name does not fit or the size is too large),
 * followed by the ustar header.
 */
export function entryHeaderBlocks(
  name: string,
  size: number,
  options: { mode?: number; mtime?: number; paxSerial: number | string } = {
    paxSerial: 0,
  },
): Buffer[] {
  const nameBytes = Buffer.byteLength(name, 'utf8');
  const paxRecords: Array<readonly [string, string]> = [];
  if (nameBytes > 100) paxRecords.push(['path', name]);
  if (size > USTAR_OCTAL_SIZE_LIMIT) paxRecords.push(['size', String(size)]);

  const blocks: Buffer[] = [];
  if (paxRecords.length > 0) {
    const body = encodePaxRecords(paxRecords);
    const paxName = `PaxHeaders/${options.paxSerial}`;
    blocks.push(buildUstarHeader({ name: paxName, size: body.length, typeflag: 'x', mode: 0o644 }));
    // Emit body + zero padding as whole physical blocks.
    const pad = zeroPadding(body.length);
    blocks.push(pad.length ? Buffer.concat([body, pad]) : body);
  }

  // ustar name field: when a PAX path record carries the real name, the
  // 100-byte field only needs a placeholder (readers ignore it).
  const ustarName = nameBytes > 100 ? `pax-${options.paxSerial}` : name;
  blocks.push(
    buildUstarHeader({
      name: ustarName,
      // When PAX carries the true size, the legacy field cannot hold it;
      // write the all-7s sentinel (PAX size overrides; matches GNU tar).
      size: size > USTAR_OCTAL_SIZE_LIMIT ? USTAR_OCTAL_SIZE_LIMIT : size,
      typeflag: '0',
      mode: options.mode,
      mtime: options.mtime,
    }),
  );
  return blocks;
}
