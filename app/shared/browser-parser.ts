import { unzip, gunzipSync } from "fflate";
import type { ParsedLine, SessionData, WorkspaceProgress } from "./types";
import { isGzipFile, isTextFile, isZipFile, parseLinesWithoutTokens as parseLines } from "./parser/primitives";

export { extractLogTimestamp, isGzipFile, isTextFile, isZipFile } from "./parser/primitives";
export { parseLinesWithoutTokens as parseLines } from "./parser/primitives";

const MAX_ZIP_DEPTH = 4;
const utf8Decoder = new TextDecoder("utf-8");

async function decompressGzipBlob(blob: Blob): Promise<string> {
  const decompression = new DecompressionStream("gzip");
  const stream = blob.stream().pipeThrough(decompression);
  const reader = stream.getReader();
  const decoder = new TextDecoder("utf-8");
  let out = "";
  while (true) {
    const { value, done } = await reader.read();
    if (value && value.length) {
      out += decoder.decode(value, { stream: true });
    }
    if (done) break;
  }
  out += decoder.decode();
  return out;
}

function decompressGzipBytes(bytes: Uint8Array): string {
  return utf8Decoder.decode(gunzipSync(bytes));
}

function unzipBytes(bytes: Uint8Array): Promise<Record<string, Uint8Array>> {
  return new Promise((resolvePromise, reject) => {
    unzip(bytes, (err, data) => {
      if (err) {
        reject(err);
        return;
      }
      resolvePromise(data);
    });
  });
}

async function readZipBytesIntoLines(bytes: Uint8Array, sourcePath: string, depth: number): Promise<ParsedLine[]> {
  if (depth > MAX_ZIP_DEPTH) {
    return [];
  }
  const entries = await unzipBytes(bytes);
  const out: ParsedLine[] = [];
  for (const [entryName, entryBytes] of Object.entries(entries)) {
    if (!entryBytes || entryName.endsWith("/")) continue;
    const nestedSource = `${sourcePath}!${entryName}`;
    if (isZipFile(entryName)) {
      const nested = await readZipBytesIntoLines(entryBytes, nestedSource, depth + 1);
      for (const line of nested) out.push(line);
      continue;
    }
    if (isGzipFile(entryName)) {
      try {
        const text = decompressGzipBytes(entryBytes);
        for (const line of parseLines(text, nestedSource)) out.push(line);
      } catch {
        // Skip unreadable inner gzip entry
      }
      continue;
    }
    if (isTextFile(entryName)) {
      const text = utf8Decoder.decode(entryBytes);
      for (const line of parseLines(text, nestedSource)) out.push(line);
    }
  }
  return out;
}

async function readZipBlobIntoLines(blob: Blob, sourcePath: string): Promise<ParsedLine[]> {
  const bytes = new Uint8Array(await blob.arrayBuffer());
  return readZipBytesIntoLines(bytes, sourcePath, 0);
}

async function readFileAsText(file: File): Promise<string> {
  return file.text();
}

export type LocalIngestProgress = (progress: WorkspaceProgress) => void;

export type LocalIngestEntry = {
  file: File;
  relativePath: string;
};

export type LocalIngestResult = {
  session: SessionData;
  skipped: { name: string; reason: string }[];
};

export async function ingestBrowserFilesLocally(
  entries: LocalIngestEntry[],
  onProgress?: LocalIngestProgress,
): Promise<LocalIngestResult> {
  const skipped: { name: string; reason: string }[] = [];
  const out: ParsedLine[] = [];
  const total = Math.max(entries.length, 1);

  onProgress?.({
    phase: "prepare",
    message: `parsing ${entries.length} file${entries.length === 1 ? "" : "s"} locally`,
    percent: 4,
    completed: 0,
    total: entries.length,
  });

  for (let index = 0; index < entries.length; index += 1) {
    const entry = entries[index];
    const relativePath = entry.relativePath || entry.file.name;
    onProgress?.({
      phase: "read",
      message: `reading ${index + 1}/${entries.length}`,
      percent: Math.round((index / total) * 90) + 4,
      completed: index,
      total: entries.length,
      currentPath: relativePath,
    });
    try {
      if (isTextFile(relativePath)) {
        const text = await readFileAsText(entry.file);
        for (const line of parseLines(text, relativePath)) out.push(line);
      } else if (isGzipFile(relativePath)) {
        const text = await decompressGzipBlob(entry.file);
        for (const line of parseLines(text, relativePath)) out.push(line);
      } else if (isZipFile(relativePath)) {
        const lines = await readZipBlobIntoLines(entry.file, relativePath);
        for (const line of lines) out.push(line);
      }
    } catch (error) {
      skipped.push({ name: relativePath, reason: error instanceof Error ? error.message : String(error) });
    }
  }

  onProgress?.({
    phase: "build",
    message: "building local session",
    percent: 96,
    completed: entries.length,
    total: entries.length,
  });

  const session: SessionData = {
    sessionId: `local-${Date.now()}`,
    lines: out,
    detail: null,
    lineDetails: {},
  };

  onProgress?.({
    phase: "complete",
    message: "local session ready",
    percent: 100,
    completed: entries.length,
    total: entries.length,
  });

  return { session, skipped };
}
