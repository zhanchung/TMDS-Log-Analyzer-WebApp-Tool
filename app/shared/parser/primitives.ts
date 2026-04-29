import type { ParsedLine } from "../types";

const logTimestampPattern = /^(?:(\d{2})-(\d{2})-(\d{4})|(\d{4})-(\d{2})-(\d{2})) (\d{2}):(\d{2}):(\d{2})\.(\d{3,4})/;
const embeddedSlashTimestampPattern = /^([A-Z#]{3,5}\s*:)\s*(\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}\.\d{3,4}):(.*)$/;

const TEXT_EXTENSIONS = new Set([".txt", ".log", ".csv", ".json", ".xml", ".md", ".tsv", ".ini", ".cfg", ".out", ".trace", ".evt", ".dat"]);

function getLowerExt(name: string): string {
  const lower = name.toLowerCase();
  const dot = lower.lastIndexOf(".");
  return dot >= 0 ? lower.slice(dot) : "";
}

export function extractLogTimestamp(raw: string): string | undefined {
  const direct = raw.match(logTimestampPattern)?.[0];
  if (direct) {
    return direct;
  }
  const embedded = embeddedSlashTimestampPattern.exec(raw);
  return embedded?.[2];
}

export function stripLeadingLogTimestamp(raw: string): string {
  const direct = raw.replace(logTimestampPattern, "").trim();
  if (direct !== raw.trim()) {
    return direct;
  }
  const embedded = embeddedSlashTimestampPattern.exec(raw);
  if (embedded) {
    return `${embedded[1]} ${embedded[3].trim()}`.trim();
  }
  return raw.trim();
}

export function isTextFile(name: string): boolean {
  const lower = name.toLowerCase();
  const ext = getLowerExt(name);
  if (TEXT_EXTENSIONS.has(ext)) {
    return true;
  }
  const fileName = lower.split(/[\\/]/).pop() ?? "";
  return !ext && /(log|event|trace|socket|code|control|genisys|cad)/.test(fileName);
}

export function isGzipFile(name: string): boolean {
  const lower = name.toLowerCase();
  return lower.endsWith(".gz") || lower.endsWith(".log.gz");
}

export function isZipFile(name: string): boolean {
  return getLowerExt(name) === ".zip";
}

export function parseLines(raw: string, sourcePath: string): ParsedLine[] {
  return raw
    .split(/\r?\n/)
    .map((line, idx) => ({
      id: `${sourcePath}:${idx + 1}`,
      lineNumber: idx + 1,
      timestamp: extractLogTimestamp(line),
      source: sourcePath,
      raw: line,
      tokens: line.split(/\s+/).filter(Boolean),
    }))
    .filter((row) => row.raw.length > 0);
}

export function parseLinesWithoutTokens(raw: string, sourcePath: string): ParsedLine[] {
  return raw
    .split(/\r?\n/)
    .map((line, idx) => ({
      id: `${sourcePath}:${idx + 1}`,
      lineNumber: idx + 1,
      timestamp: extractLogTimestamp(line),
      source: sourcePath,
      raw: line,
      tokens: [] as string[],
    }))
    .filter((row) => row.raw.length > 0);
}
