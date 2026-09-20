export type { TarHeader } from './legacy.js';
export { mergeMetadata, ArchiveIndex } from './legacy.js';

export {
  TarWriter,
  sinkFromWritable,
  type TarSink,
  type ByteSource,
  type TarWriterOptions,
  type StandardWriterOptions,
  type ChunkedWriterOptions,
  type StandardCompatibility,
  type AddFileOptions,
  type AddFileResult,
  type AddFileSuccess,
  type AddFileFailure,
  type EndResult,
  type EndSuccess,
  type EndFailure,
} from './writer.js';

export { readTar, type ByteInput, type TarEntry, type FileEntry, type SimpleEntry, type TarEntryMeta } from './reader.js';

export {
  TarError,
  TarWriterError,
  SourceError,
  TempBudgetExceededError,
  CompatibilityError,
  AbortedError,
  SinkError,
  WriterStateError,
  TarParseError,
  TarUnexpectedEofError,
  TarIntegrityError,
} from './errors.js';

export {
  CHUNK_TYPEFLAG,
  SUMMARY_TYPEFLAG,
  SUMMARY_KEYS,
  chunkName,
  summaryName,
  parseChunkName,
  parseSummaryName,
  parseSummaryPayload,
  type SummaryInfo,
} from './extension.js';

export {
  BLOCK_SIZE,
  buildHeader,
  buildUstarHeader,
  parseHeader,
  parsePaxRecords,
  encodePaxRecords,
  encodePaxRecord,
  isZeroBlock,
  paddedBlocks,
  paddedSize,
  parseOctal,
  formatOctal,
  splitUstarName,
  joinPrefixName,
  type ParsedHeader,
  type PaxRecordMap,
  type RawHeaderFields,
} from './blocks.js';
