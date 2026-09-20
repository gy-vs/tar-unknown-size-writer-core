/** Original indexing helpers kept for backwards compatibility. */
export type TarHeader = { path: string; size: number; type: 'file' | 'directory' | 'link' };

export function mergeMetadata(
  header: TarHeader,
  globalPax: Record<string, string>,
  localPax: Record<string, string>,
  longname?: string,
): TarHeader {
  return {
    ...header,
    ...localPax,
    ...globalPax,
    path: globalPax.path ?? localPax.path ?? longname ?? header.path,
    size: Number(localPax.size ?? globalPax.size ?? header.size),
  };
}

export class ArchiveIndex {
  #entries: TarHeader[] = [];

  add(entry: TarHeader): void {
    this.#entries.push(entry);
  }

  list(): TarHeader[] {
    return this.#entries.slice();
  }

  find(path: string): TarHeader | undefined {
    return this.#entries.find((entry) => entry.path === path);
  }
}
