import { unzip, gunzipSync } from "fflate";
import assignmentRows from "../../exports/normalized/code_station_assignment_map.json";
import type { DetailModel, ParsedLine, SessionData, WorkspaceProgress } from "./types";
import { isGzipFile, isTextFile, isZipFile, parseLinesWithoutTokens as parseLines } from "./parser/primitives";

export { extractLogTimestamp, isGzipFile, isTextFile, isZipFile } from "./parser/primitives";
export { parseLinesWithoutTokens as parseLines } from "./parser/primitives";

const MAX_ZIP_DEPTH = 4;
const utf8Decoder = new TextDecoder("utf-8");

type AssignmentEntry = {
  bit_position: string;
  mnemonic: string;
  long_name: string;
  word_type?: string;
};

type AssignmentRow = {
  code_line_name?: string;
  station_name?: string;
  control_point_name?: string;
  control_address?: string;
  indication_address?: string;
  control_assignments?: AssignmentEntry[];
  indication_assignments?: AssignmentEntry[];
};

const staticAssignmentRows = assignmentRows as AssignmentRow[];
const assignmentByKey = new Map<string, AssignmentRow>();

function normalizeKey(value: string | undefined): string {
  return String(value ?? "").trim().toUpperCase().replace(/\s+/g, " ");
}

function addAssignmentKey(key: string | undefined, row: AssignmentRow) {
  const normalized = normalizeKey(key);
  if (normalized && !assignmentByKey.has(normalized)) {
    assignmentByKey.set(normalized, row);
  }
}

for (const row of staticAssignmentRows) {
  addAssignmentKey(row.station_name, row);
  addAssignmentKey(row.control_point_name, row);
  addAssignmentKey(row.control_address, row);
  addAssignmentKey(row.indication_address, row);
}

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

function assignmentLabel(entry: AssignmentEntry | undefined): string {
  if (!entry) return "unmapped";
  const mnemonic = entry.mnemonic || "BLANK";
  const longName = entry.long_name || "";
  return longName && normalizeKey(longName) !== normalizeKey(mnemonic) ? `${mnemonic} - ${longName}` : mnemonic;
}

function positionMap(entries: AssignmentEntry[] | undefined): Map<number, AssignmentEntry> {
  const map = new Map<number, AssignmentEntry>();
  for (const entry of entries ?? []) {
    const position = Number(entry.bit_position);
    if (Number.isFinite(position)) {
      map.set(position, entry);
    }
  }
  return map;
}

function assertedPositions(bits: string): number[] {
  const out: number[] = [];
  for (let index = 0; index < bits.length; index += 1) {
    if (bits[index] === "1") out.push(index + 1);
  }
  return out;
}

function extractStationFromRaw(raw: string): string {
  return (
    /\(([A-Z0-9 _-]+(?:TC)?)\)/i.exec(raw)?.[1] ??
    /(?:SendCommand|QueueTheCommand):([A-Z0-9 _-]+?):/i.exec(raw)?.[1] ??
    /ProcessSendQueue-?([A-Z0-9 _-]+?)(?:CONTROL|RECALL|$)/i.exec(raw)?.[1] ??
    /(?:CONTROL UPDATED|PROCESS IND):\s*([0-9A-Z]+)(?:\(([^)]+)\))?/i.exec(raw)?.[2] ??
    ""
  ).trim();
}

function parseMnemonicStates(raw: string): Array<{ position: number; mnemonic: string; value: string }> {
  const states: Array<{ position: number; mnemonic: string; value: string }> = [];
  const pattern = /\((\d+)\)\s*([A-Z0-9_/-]+)=([A-Za-z0-9_/-]+)/gi;
  let match = pattern.exec(raw);
  while (match) {
    states.push({ position: Number(match[1]), mnemonic: match[2], value: match[3] });
    match = pattern.exec(raw);
  }
  return states;
}

function findAssignmentRow(station: string): AssignmentRow | null {
  return assignmentByKey.get(normalizeKey(station)) ?? null;
}

function findNearbyStation(lines: ParsedLine[], index: number): string {
  const source = lines[index]?.source;
  for (let offset = 0; offset <= 16; offset += 1) {
    for (const candidateIndex of [index - offset, index + offset]) {
      if (candidateIndex < 0 || candidateIndex >= lines.length) continue;
      const candidate = lines[candidateIndex];
      if (candidate.source !== source) continue;
      const station = extractStationFromRaw(candidate.raw);
      if (station) return station;
    }
  }
  return "";
}

function makeStaticDetail(line: ParsedLine, lines: ParsedLine[], index: number): DetailModel {
  const raw = line.raw;
  const station = findNearbyStation(lines, index);
  const row = findAssignmentRow(station);
  const isControlMnemonic = /\bCTL MNEM:/i.test(raw);
  const isIndicationMnemonic = /\bIND MNEM:/i.test(raw);
  const controlPayload = /\b(SendControl|ProcessControlBegin)(\d*)?:\s*([01]+)/i.exec(raw);
  const controlUpdated = /\bCONTROL UPDATED:\s*([0-9A-Z]+)(?:\(([^)]+)\))?\s+\[([01]+)\]/i.exec(raw);
  const processInd = /\bPROCESS IND:\s*([0-9A-Z]+)\s*\(([^)]+)\)\s*\(([01]+)\)/i.exec(raw);
  const payloadBits = controlPayload?.[3] ?? controlUpdated?.[3] ?? processInd?.[3] ?? "";
  const assignmentKind = isIndicationMnemonic || processInd ? "indication" : "control";
  const assignments = assignmentKind === "indication" ? row?.indication_assignments : row?.control_assignments;
  const byPosition = positionMap(assignments);
  const activePositions = payloadBits ? assertedPositions(payloadBits) : [];
  const mnemonicStates = parseMnemonicStates(raw);
  const mappedMnemonicStates = mnemonicStates.map((state) => {
    const assignment = byPosition.get(state.position);
    return `${state.position}. ${state.mnemonic}=${state.value}${assignment ? `; assignment=${assignmentLabel(assignment)}` : ""}`;
  });
  const activeAssignments = activePositions.map((position) => `${position}. ${assignmentLabel(byPosition.get(position))}`);
  const payloadContext = [
    payloadBits ? `Payload bits: ${payloadBits}` : "",
    payloadBits ? `Payload width: ${payloadBits.length} bits` : "",
    payloadBits ? `Asserted positions: ${activePositions.length ? activePositions.join(", ") : "none"}` : "",
    ...activeAssignments.map((entry) => `Active assignment: ${entry}`),
    ...mappedMnemonicStates.map((entry) => `Mnemonic state: ${entry}`),
  ].filter(Boolean);
  const databaseContext = [
    row ? `Station: ${row.station_name}` : station ? `Station context unresolved: ${station}` : "Station context unresolved in local static mode.",
    row?.control_point_name ? `Control point: ${row.control_point_name}` : "",
    row?.code_line_name ? `Code line: ${row.code_line_name}` : "",
    row ? `Control assignments: ${row.control_assignments?.length ?? 0}` : "",
    row ? `Indication assignments: ${row.indication_assignments?.length ?? 0}` : "",
  ].filter(Boolean);
  const summary = payloadBits
    ? `${assignmentKind === "indication" ? "Indication" : "Control"} payload; ${activePositions.length} asserted bit${activePositions.length === 1 ? "" : "s"}.`
    : mnemonicStates.length
      ? `${assignmentKind === "indication" ? "Indication" : "Control"} mnemonic snapshot; ${mnemonicStates.length} observed state${mnemonicStates.length === 1 ? "" : "s"}.`
      : "Static browser detail generated from the selected raw line.";

  return {
    lineId: line.id,
    lineNumber: line.lineNumber,
    timestamp: line.timestamp,
    raw,
    translation: {
      original: raw,
      structured: [
        line.source ? `Source: ${line.source}` : "",
        line.timestamp ? `Timestamp: ${line.timestamp}` : "",
        station ? `Nearby station: ${station}` : "",
        ...databaseContext,
      ].filter(Boolean),
      english: [summary],
      unresolved: row ? [] : ["No static assignment row was resolved for this line's nearby station context."],
    },
    workflow: {
      summary,
      currentStep: controlPayload?.[1] ?? (controlUpdated ? "CONTROL UPDATED" : processInd ? "PROCESS IND" : isControlMnemonic ? "CTL MNEM" : isIndicationMnemonic ? "IND MNEM" : ""),
      systems: ["Code line"],
      objects: station ? [station] : [],
      knownState: activePositions.length ? "One or more payload bits asserted" : payloadBits ? "All logged payload bits clear" : "",
      unresolved: row ? [] : ["Static assignment context unavailable for this selected line."],
    },
    genisysContext: [],
    icdContext: [],
    databaseContext,
    workflowContext: [],
    payloadContext,
    sourceReferences: row ? [{
      id: "code_station_assignment_map",
      kind: "generated",
      title: "Normalized code station assignment map",
      path: "exports/normalized/code_station_assignment_map.json",
      notes: "Bundled into the browser build for static GitHub Pages detail generation.",
    }] : [],
  };
}

function buildStaticLineDetails(lines: ParsedLine[]): Record<string, DetailModel> {
  const details: Record<string, DetailModel> = {};
  for (let index = 0; index < lines.length; index += 1) {
    const raw = lines[index].raw;
    if (/\b(IND MNEM|CTL MNEM|SendControl|ProcessControlBegin|CONTROL UPDATED|PROCESS IND)\b/i.test(raw)) {
      details[lines[index].id] = makeStaticDetail(lines[index], lines, index);
    }
  }
  return details;
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
    lineDetails: buildStaticLineDetails(out),
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
