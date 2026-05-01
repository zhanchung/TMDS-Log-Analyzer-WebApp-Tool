import { unzip, gunzipSync } from "fflate";
import genisysProtocolReference from "../../exports/mappings/genisys_protocol_reference.json";
import icdMessageCatalog from "../../exports/mappings/icd_message_catalog.json";
import assignmentRows from "../../exports/normalized/code_station_assignment_map.json";
import stationFoundationRows from "../../exports/normalized/station_foundation_summary.json";
import componentLookupRows from "../../exports/raw/sql_foundation/tmdsDatabaseStatic.component_lookup.json";
import genisysSampleLog from "../../sample_logs/curated/genisys_sample.log?raw";
import socketTraceSampleLog from "../../sample_logs/curated/sockettrace_sample.log?raw";
import workflowSampleLog from "../../sample_logs/curated/workflow_sample.log?raw";
import type { DetailModel, ParsedLine, SessionData, WorkspaceProgress } from "./types";
import { decodeGenisysSocketFrame, extractBracketedHexBytes, formatHexByte } from "./genisys";
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
  code_line_number?: string;
  station_name?: string;
  code_station_number?: string;
  control_point_number?: string;
  control_point_name?: string;
  subdivision_name?: string;
  signal_count?: string | number;
  track_count?: string | number;
  switch_count?: string | number;
  route_count?: string | number;
  control_address?: string;
  indication_address?: string;
  control_assignments?: AssignmentEntry[];
  indication_assignments?: AssignmentEntry[];
};

type StationFoundationRow = {
  code_line_number?: string;
  code_line_name?: string;
  code_station_number?: string;
  station_name?: string;
  control_point_number?: string;
  control_point_name?: string;
  subdivision_name?: string;
  signal_count?: string | number;
  track_count?: string | number;
  switch_count?: string | number;
  route_count?: string | number;
  number_of_controls?: string | number;
  number_of_indications?: string | number;
};

type ComponentLookupRow = {
  component_family?: string;
  component_uid?: string | number;
  parent_control_point_uid?: string | number;
  component_name?: string;
  component_secondary_name?: string;
  component_detail_name?: string;
  component_codeline?: string | number;
  territory_assignment?: string | number;
  subdivision?: string | number;
};

type IcdCatalogRow = {
  document_title?: string;
  release?: string;
  section?: string;
  message_id?: string;
  message_name?: string;
  message_version?: number;
  page?: number;
  direction?: string;
};

const staticAssignmentRows = assignmentRows as AssignmentRow[];
const staticStationRows = stationFoundationRows as StationFoundationRow[];
const staticComponentRows = componentLookupRows as ComponentLookupRow[];
const staticIcdRows = icdMessageCatalog as IcdCatalogRow[];
const staticGenisysReference = genisysProtocolReference as {
  office_headers?: Array<{ byte?: string; meaning?: string }>;
  field_headers?: Array<{ byte?: string; meaning?: string }>;
  mode_bit_definitions?: Array<{ bit?: number; meaning?: string }>;
};
const assignmentByKey = new Map<string, AssignmentRow>();
const stationByKey = new Map<string, StationFoundationRow>();
const componentByUid = new Map<string, ComponentLookupRow>();
const icdByMessageId = new Map<string, IcdCatalogRow>();

function getFileLabel(source?: string): string {
  if (!source) return "";
  const normalized = source.replace(/\\/g, "/");
  const parts = normalized.split("/");
  return parts[parts.length - 1] || source;
}

function normalizeKey(value: string | undefined): string {
  return String(value ?? "").trim().toUpperCase().replace(/^CP\s+/, "").replace(/\s+/g, " ");
}

function addAssignmentKey(key: string | undefined, row: AssignmentRow) {
  const normalized = normalizeKey(key);
  if (normalized && !assignmentByKey.has(normalized)) {
    assignmentByKey.set(normalized, row);
  }
}

function addStationKey(key: string | undefined, row: StationFoundationRow) {
  const normalized = normalizeKey(key);
  if (normalized && !stationByKey.has(normalized)) {
    stationByKey.set(normalized, row);
  }
}

for (const row of staticAssignmentRows) {
  addAssignmentKey(row.station_name, row);
  addAssignmentKey(row.control_point_name, row);
  addAssignmentKey(row.control_point_number, row);
  addAssignmentKey(row.code_station_number, row);
  addAssignmentKey(row.control_address, row);
  addAssignmentKey(row.indication_address, row);
}

for (const row of staticStationRows) {
  addStationKey(row.station_name, row);
  addStationKey(row.control_point_name, row);
  addStationKey(row.control_point_number, row);
  addStationKey(row.code_station_number, row);
}
for (const row of staticComponentRows) {
  const uid = normalizeKey(String(row.component_uid ?? ""));
  if (uid && !componentByUid.has(uid)) {
    componentByUid.set(uid, row);
  }
}

function normalizeMessageId(value: string | number | undefined): string {
  const digits = String(value ?? "").replace(/\D/g, "");
  return digits ? String(Number(digits)) : "";
}

for (const row of staticIcdRows) {
  const messageId = normalizeMessageId(row.message_id);
  if (messageId && !icdByMessageId.has(messageId)) {
    icdByMessageId.set(messageId, row);
  }
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

function displayMessageId(value: string | number | undefined): string {
  const normalized = normalizeMessageId(value);
  return normalized ? normalized.padStart(normalized.length <= 4 ? 4 : 5, "0") : "";
}

function parseAngleFields(raw: string): string[] {
  const fields: string[] = [];
  const pattern = /<([^<>]+)>/g;
  let match = pattern.exec(raw);
  while (match) {
    const value = match[1].trim();
    if (value) fields.push(value);
    match = pattern.exec(raw);
  }
  return fields;
}

function parseAngleFieldMap(raw: string): Map<string, string> {
  const fields = new Map<string, string>();
  for (const field of parseAngleFields(raw)) {
    const separator = field.indexOf("=");
    if (separator <= 0) continue;
    fields.set(field.slice(0, separator).trim(), field.slice(separator + 1).trim());
  }
  return fields;
}

function extractJsonPayload(raw: string): unknown | null {
  const start = raw.indexOf("{");
  const end = raw.lastIndexOf("}");
  if (start < 0 || end <= start) return null;
  try {
    return JSON.parse(raw.slice(start, end + 1)) as unknown;
  } catch {
    return null;
  }
}

function valueAtPath(value: unknown, path: string[]): unknown {
  let current = value;
  for (const part of path) {
    if (!current || typeof current !== "object" || !(part in current)) return undefined;
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}

function stringifyScalar(value: unknown): string {
  if (value === undefined || value === null || value === "") return "";
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") return String(value);
  return "";
}

function formatBytes(value: string | undefined): string {
  const bytes = Number(value);
  if (!Number.isFinite(bytes)) return value ?? "";
  return `${bytes} bytes (${(bytes / 1024 / 1024).toFixed(1)} MiB)`;
}

function sourceReference(id: string, title: string, path: string, notes: string) {
  return {
    id,
    kind: "generated" as const,
    title,
    path,
    notes,
  };
}

function resolvedLongName(entry: AssignmentEntry): string {
  const mnemonic = String(entry.mnemonic || "BLANK").trim().toUpperCase();
  const longName = String(entry.long_name || "").trim();
  if (longName && longName.toUpperCase() !== mnemonic) {
    return longName;
  }
  return inferDescriptionFromMnemonic(mnemonic) || longName || mnemonic;
}

function inferDescriptionFromMnemonic(mnemonic: string): string {
  const m = mnemonic.toUpperCase();
  if (m === "TIMERST") return "TIMER RESET";
  if (m === "EGNS") return "TRAIN READY";
  if (m.endsWith("LOK")) return "LIGHT OUT INDICATION";
  if (m.endsWith("POK")) return "POWER OFF INDICATION";
  if (m.endsWith("DOK")) return "ILLEGAL ENTRY INDICATION";
  if (m.endsWith("MOK")) return "MANUAL OPERATE INDICATION";
  if (m.endsWith("NWK")) return "NORMAL SWITCH INDICATION";
  if (m.endsWith("RWK")) return "REVERSE SWITCH INDICATION";
  if (m.endsWith("NWS")) return "NORMAL SWITCH CONTROL";
  if (m.endsWith("RWS")) return "REVERSE SWITCH CONTROL";
  if (m.endsWith("EGK") || m.endsWith("WGK") || m.endsWith("TEK")) return "SIGNAL INDICATION";
  if (m.endsWith("EGS") || m.endsWith("WGS") || m.endsWith("TES")) return "SIGNAL CONTROL";
  if (m.endsWith("BCAN")) return "BYPASS CANCEL";
  if (m.endsWith("RQR") || m.endsWith("RQK")) return "REQUEST INDICATION";
  if (m.endsWith("ATKR")) return "TRACK INDICATION";
  if (m.endsWith("STK") || m.endsWith("ATK")) return "TRACK INDICATION";
  if (m.endsWith("AK")) return "TRACK INDICATION";
  if (m.endsWith("BLK")) return "BLOCK INDICATION";
  if (m.endsWith("BK")) return "BLOCK OCCUPANCY INDICATION";
  if (m.endsWith("TK")) return "TRACK INDICATION";
  if (m.endsWith("RTE")) return "ROUTE INDICATION";
  return "";
}

function assignmentLabel(entry: AssignmentEntry | undefined): string {
  if (!entry) return "unmapped";
  const mnemonic = entry.mnemonic || "BLANK";
  const longName = entry.long_name || "";
  if (longName && normalizeKey(longName) !== normalizeKey(mnemonic)) {
    return `${mnemonic} - ${longName}`;
  }
  const inferred = inferDescriptionFromMnemonic(mnemonic);
  return inferred ? `${mnemonic} - ${inferred}` : mnemonic;
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
  const embeddedProtocolStation = /\b(?:INDICATION|CONTROL);(\d+):(\d+):(\d+):[01]+/i.exec(raw)?.[2];
  if (embeddedProtocolStation) {
    return embeddedProtocolStation.trim();
  }
  const namedParen = /\((CP\s+[A-Z0-9 _-]+|[A-Z][A-Z0-9 _-]*(?:TC|JCT))\)/i.exec(raw)?.[1];
  return (
    namedParen ??
    /FOR CODESTATION:\s*([A-Z0-9 _-]+)/i.exec(raw)?.[1] ??
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

function findStationRow(station: string): StationFoundationRow | null {
  return stationByKey.get(normalizeKey(station)) ?? null;
}

function findComponentRow(uid: string | number | undefined): ComponentLookupRow | null {
  return componentByUid.get(normalizeKey(String(uid ?? ""))) ?? null;
}

function getComponentStationRow(component: ComponentLookupRow | null): StationFoundationRow | null {
  if (!component) return null;
  return findStationRow(String(component.parent_control_point_uid ?? ""));
}

function getComponentAssignmentRow(component: ComponentLookupRow | null): AssignmentRow | null {
  if (!component) return null;
  return findAssignmentRow(String(component.parent_control_point_uid ?? ""))
    ?? findAssignmentRow(getComponentStationRow(component)?.control_point_name ?? "");
}

function describeComponent(component: ComponentLookupRow | null): string {
  if (!component) return "";
  const uid = component.component_uid ?? "";
  const family = component.component_family ?? "component";
  const name = component.component_name ?? component.component_secondary_name ?? "";
  const stationRow = getComponentStationRow(component);
  const stationText = stationRow?.station_name || stationRow?.control_point_name
    ? ` at ${stationRow.station_name ?? stationRow.control_point_name}`
    : component.parent_control_point_uid
      ? ` at CP ${component.parent_control_point_uid}`
      : "";
  const codeLineText = component.component_codeline ? ` on code line ${component.component_codeline}` : "";
  return `${family} ${uid}${name ? ` ${name}` : ""}${stationText}${codeLineText}`.trim();
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
  const stationRow = findStationRow(station);
  const row = findAssignmentRow(station)
    ?? findAssignmentRow(stationRow?.control_point_number ?? "")
    ?? findAssignmentRow(stationRow?.control_point_name ?? "");
  const isControlMnemonic = /\bCTL MNEM:/i.test(raw);
  const isIndicationMnemonic = /\bIND MNEM:/i.test(raw);
  const controlPayload = /\b(SendControl|ProcessControlBegin)(\d*)?:\s*([01]+)/i.exec(raw);
  const controlUpdated = /\bCONTROL UPDATED:\s*([0-9A-Z]+)(?:\(([^)]+)\))?\s+\[([01]+)\]/i.exec(raw);
  const processInd = /\bPROCESS IND:\s*([0-9A-Z]+)\s*\(([^)]+)\)\s*\(([01]+)\)/i.exec(raw);
  const directIndication = /\bINDICATION;(\d+):(\d+):(\d+):([01]+)\s+FOR CODESTATION:\s*([A-Z0-9 _-]+)/i.exec(raw);
  const embeddedIndication = /\bINDICATION;(\d+):(\d+):(\d+):([01]+)/i.exec(raw);
  const embeddedControl = /\bCONTROL;(\d+):(\d+):(\d+):([01]+)/i.exec(raw);
  const codeServerControl = /\bCONTROL(?:\s+UPDATE\s+ONLY)?:([0-9.]+):(\d+):(\d+):(\d+):([01]+)/i.exec(raw);
  const controlSent = /\b<<CONTROL SENT:\s*(\d+)\s+\(([^)]+)\)\s*-\s*\(([01]+)\)/i.exec(raw);
  const payloadBits = controlPayload?.[3] ?? controlUpdated?.[3] ?? processInd?.[3] ?? directIndication?.[4] ?? embeddedIndication?.[4] ?? embeddedControl?.[4] ?? codeServerControl?.[5] ?? controlSent?.[3] ?? "";
  const assignmentKind = isIndicationMnemonic || processInd || directIndication || embeddedIndication ? "indication" : "control";
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
  const nonBlankControlEntries = nonBlankAssignments(row?.control_assignments);
  const nonBlankIndicationEntries = nonBlankAssignments(row?.indication_assignments);
  const controlBitLines: string[] = nonBlankControlEntries.length
    ? [
        "Control bits:",
        ...nonBlankControlEntries.map((entry) => `${entry.bit_position}. ${entry.mnemonic} = ${resolvedLongName(entry)}`),
      ]
    : [];
  const indicationBitLines: string[] = nonBlankIndicationEntries.length
    ? [
        "Indication bits:",
        ...nonBlankIndicationEntries.map((entry) => `${entry.bit_position}. ${entry.mnemonic} = ${resolvedLongName(entry)}`),
      ]
    : [];
  const databaseContext = [
    row || stationRow ? `Station: ${stationRow?.station_name ?? row?.station_name}` : station ? `Station context unresolved: ${station}` : "Station context unresolved in local static mode.",
    row?.control_point_number || stationRow?.control_point_number ? `Control point: ${stationRow?.control_point_number ?? row?.control_point_number} (${stationRow?.control_point_name ?? row?.control_point_name ?? ""})`.replace(/\s+\(\)$/, "") : "",
    stationRow?.subdivision_name || row?.subdivision_name ? `Subdivision: ${stationRow?.subdivision_name ?? row?.subdivision_name}` : "",
    row?.code_line_name || stationRow?.code_line_name ? `Code line ${stationRow?.code_line_number ?? row?.code_line_number ?? ""}: ${stationRow?.code_line_name ?? row?.code_line_name}`.replace(/^Code line :/, "Code line:") : "",
    stationRow ? `Station inventory: signals=${stationRow.signal_count ?? 0}, tracks=${stationRow.track_count ?? 0}, switches=${stationRow.switch_count ?? 0}, routes=${stationRow.route_count ?? 0}` : "",
    ...controlBitLines,
    ...indicationBitLines,
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
        line.source ? `File: ${getFileLabel(line.source)}` : "",
        line.timestamp ? `Timestamp: ${line.timestamp}` : "",
        ...databaseContext,
      ].filter(Boolean),
      english: [summary],
      unresolved: row ? [] : ["No bundled assignment row was resolved for this line's nearby station context."],
    },
    workflow: {
      summary,
      currentStep: controlPayload?.[1] ?? (controlUpdated ? "CONTROL UPDATED" : processInd ? "PROCESS IND" : directIndication || embeddedIndication ? "INDICATION" : embeddedControl || codeServerControl ? "CONTROL" : controlSent ? "CONTROL SENT" : isControlMnemonic ? "CTL MNEM" : isIndicationMnemonic ? "IND MNEM" : ""),
      systems: ["Code line"],
      objects: (stationRow?.station_name ?? station) ? [stationRow?.station_name ?? station] : [],
      knownState: activePositions.length ? "One or more payload bits asserted" : payloadBits ? "All logged payload bits clear" : "",
      unresolved: row ? [] : ["Bundled assignment context unavailable for this selected line."],
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

function isBlankAssignment(entry: AssignmentEntry): boolean {
  const mnemonic = String(entry.mnemonic ?? "").trim().toUpperCase();
  const longName = String(entry.long_name ?? "").trim().toUpperCase();
  return mnemonic === "BLANK" || longName === "BLANK" || longName === "BLANK CONTROL" || longName === "BLANK INDICATION";
}

function nonBlankAssignments(entries: AssignmentEntry[] | undefined): AssignmentEntry[] {
  return (entries ?? []).filter((entry) => !isBlankAssignment(entry));
}

// Decodes Genisys word/data payload pairs into named bit states.
// Each pair is (word_number, byte_value); bits are LSB-first (bit 0 of the byte = lowest
// numbered bit in that word). 0xE0 is the special mode byte and is decoded as flags.
function decodeFrameWordPairs(
  payloadPairs: Array<{ address: number; data: number }>,
  kind: "indication" | "control",
  row: AssignmentRow | null,
): string[] {
  if (!payloadPairs.length) return [];
  const assignments = kind === "indication" ? row?.indication_assignments : row?.control_assignments;
  const byPosition = positionMap(assignments);
  const kindLabel = kind === "indication" ? "Indication" : "Control";
  const lines: string[] = [];
  for (const { address, data } of payloadPairs) {
    if (address === 0xe0) {
      const flags: string[] = [];
      if (data & 0x01) flags.push("Database Complete");
      if (data & 0x02) flags.push("Checkback Control Enabled");
      if (data & 0x04) flags.push("Secure Poll");
      if (data & 0x08) flags.push("Common Command Enabled");
      lines.push(`Mode byte (0xE0 = 0x${formatHexByte(data)}): ${flags.length ? flags.join(", ") : "no flags set"}`);
      continue;
    }
    const wordNum = address;
    const baseBit = wordNum * 8 + 1;
    const asserted: string[] = [];
    for (let bitIndex = 0; bitIndex < 8; bitIndex += 1) {
      if ((data >> bitIndex) & 1) {
        const bitPos = baseBit + bitIndex;
        const entry = byPosition.get(bitPos);
        const label = entry && !isBlankAssignment(entry) ? assignmentLabel(entry) : `bit ${bitPos}`;
        asserted.push(label);
      }
    }
    const wordRange = `${kindLabel} bits ${baseBit}–${baseBit + 7}`;
    lines.push(
      asserted.length
        ? `Word ${wordNum} (${wordRange}): ${asserted.join(", ")}`
        : `Word ${wordNum} (${wordRange}): all clear`,
    );
  }
  return lines;
}

function makeSocketRawFrameDetail(line: ParsedLine): DetailModel | null {
  const match = /(?:^|\s)([<>-]{3})\s+(XMT|RCV):([^:]+):(.+)$/i.exec(line.raw);
  if (!match) return null;
  const directionGlyph = match[1];
  const socketAction = match[2].toUpperCase();
  const stationToken = match[3].trim();
  const payloadBytes = extractBracketedHexBytes(match[4]);
  if (!payloadBytes.length) return null;
  const decoded = decodeGenisysSocketFrame(payloadBytes);
  const row = findAssignmentRow(stationToken);
  const stationRow = findStationRow(stationToken);
  const stationName = stationRow?.station_name ?? row?.station_name ?? stationToken;
  const controlPoint = stationRow?.control_point_number || row?.control_point_number
    ? `${stationRow?.control_point_number ?? row?.control_point_number} (${stationRow?.control_point_name ?? row?.control_point_name ?? stationName})`
    : "";
  const payloadPairs = decoded.payloadPairs.length
    ? decoded.payloadPairs.map(({ address, data }, pairIndex) => `${pairIndex + 1}. 0x${formatHexByte(address)} = 0x${formatHexByte(data)} (${data})`)
    : ["none"];
  const summary = `${socketAction === "XMT" ? "Office transmitted" : "Field returned"} ${decoded.headerLabel} for ${stationName}.`;
  const structured = [
    line.source ? `File: ${getFileLabel(line.source)}` : "",
    line.timestamp ? `Timestamp: ${line.timestamp}` : "",
    `Direction marker: ${directionGlyph} ${socketAction}`,
    `Station: ${stationName}`,
    controlPoint ? `Control point: ${controlPoint}` : "",
    stationRow?.subdivision_name ? `Subdivision: ${stationRow.subdivision_name}` : "",
    stationRow?.code_line_number || row?.code_line_number ? `Code line ${stationRow?.code_line_number ?? row?.code_line_number}: ${stationRow?.code_line_name ?? row?.code_line_name ?? ""}`.replace(/\s+$/, "") : "",
  ].filter(Boolean);
  const nonBlankControls = nonBlankAssignments(row?.control_assignments);
  const nonBlankIndications = nonBlankAssignments(row?.indication_assignments);
  const controlBitLines: string[] = nonBlankControls.length
    ? ["Control bits:", ...nonBlankControls.map((entry) => `${entry.bit_position}. ${entry.mnemonic} = ${resolvedLongName(entry)}`)]
    : [];
  const indicationBitLines: string[] = nonBlankIndications.length
    ? ["Indication bits:", ...nonBlankIndications.map((entry) => `${entry.bit_position}. ${entry.mnemonic} = ${resolvedLongName(entry)}`)]
    : [];
  const databaseContext = [...structured, ...controlBitLines, ...indicationBitLines];
  // Determine payload kind for word/bit decoding
  const framePayloadKind: "indication" | "control" | null =
    decoded.headerCode === 0xf2 ? "indication"
    : decoded.headerCode === 0xf3 || decoded.headerCode === 0xfc || decoded.headerCode === 0xf9 ? "control"
    : null;
  const decodedWordLines = framePayloadKind
    ? decodeFrameWordPairs(decoded.payloadPairs, framePayloadKind, row)
    : [];
  const payloadContext = [
    "Decoded Genisys frame:",
    `Header = ${decoded.headerLabel}${decoded.headerCode === null ? "" : ` (0x${formatHexByte(decoded.headerCode)})`}`,
    `Role = ${decoded.protocolDirection}`,
    decoded.serverAddress !== null ? `Server address = 0x${formatHexByte(decoded.serverAddress)} (${decoded.serverAddress} decimal)` : "",
    decoded.crcHex ? `CRC = ${decoded.crcHex}` : "",
    `Payload bytes = ${payloadBytes.join(" ")}`,
    decoded.payloadPairs.length ? "Raw word/data pairs:" : "",
    ...payloadPairs,
    decodedWordLines.length ? "Decoded bit states:" : "",
    ...decodedWordLines,
    ...decoded.issues.map((issue) => `Decode note: ${issue}`),
  ].filter(Boolean);

  return {
    lineId: line.id,
    lineNumber: line.lineNumber,
    timestamp: line.timestamp,
    raw: line.raw,
    translation: {
      original: line.raw,
      structured,
      english: [summary],
      unresolved: [],
    },
    workflow: {
      summary,
      currentStep: decoded.headerLabel,
      systems: ["CodeServer", "Genisys"],
      objects: [stationName],
      knownState: decoded.protocolDirection,
      unresolved: [],
    },
    genisysContext: payloadContext,
    icdContext: [],
    databaseContext,
    workflowContext: [
      socketAction === "XMT"
        ? "This socket frame was transmitted from office toward the field endpoint."
        : "This socket frame was received back from the field endpoint.",
    ],
    payloadContext,
    sourceReferences: [
      sourceReference("genisys_shared_decoder", "Shared Genisys frame decoder", "app/shared/genisys.ts", "Used by both Electron/server detail generation and GitHub static mode."),
    ],
  };
}

function makeCadFeedbackProblemDetail(line: ParsedLine): DetailModel | null {
  const problemMatch = /\b(?:FEEDBACK:)?\s*SWITCH ALIGNMENT ERROR:\s*([^(<]+?)\s*\((\d+)\)\s*<([^>]*)>/i.exec(line.raw);
  if (!problemMatch) return null;
  const reportedName = problemMatch[1].trim();
  const signalUid = problemMatch[2].trim();
  const fields = problemMatch[3];
  const switchUid = /Switch\s+GUID\s*:\s*(\d+)/i.exec(fields)?.[1] ?? "";
  const normalState = /NORMAL\s*:\s*([A-Za-z0-9_-]+)/i.exec(fields)?.[1] ?? "";
  const reverseState = /REVERSE\s*:\s*([A-Za-z0-9_-]+)/i.exec(fields)?.[1] ?? "";
  const signalComponent = findComponentRow(signalUid);
  const switchComponent = findComponentRow(switchUid);
  const stationRow = getComponentStationRow(signalComponent) ?? getComponentStationRow(switchComponent);
  const assignmentRow = getComponentAssignmentRow(switchComponent) ?? getComponentAssignmentRow(signalComponent);
  const switchControlAssignments = (assignmentRow?.control_assignments ?? [])
    .filter((assignment) => /(?:NWS|RWS|NORMAL SWITCH|REVERSE SWITCH)/i.test(`${assignment.mnemonic} ${assignment.long_name}`))
    .slice(0, 8)
    .map((assignment) => `Switch control assignment: bit ${assignment.bit_position} ${assignmentLabel(assignment)}`);
  const switchIndicationAssignments = (assignmentRow?.indication_assignments ?? [])
    .filter((assignment) => /(?:NWK|RWK|NORMAL SWITCH|REVERSE SWITCH)/i.test(`${assignment.mnemonic} ${assignment.long_name}`))
    .slice(0, 8)
    .map((assignment) => `Switch indication assignment: bit ${assignment.bit_position} ${assignmentLabel(assignment)}`);
  const structured = [
    line.source ? `File: ${getFileLabel(line.source)}` : "",
    line.timestamp ? `Timestamp: ${line.timestamp}` : "",
    `Problem: switch alignment error`,
    `Reported signal: ${reportedName} (${signalUid})`,
    signalComponent ? `Resolved signal asset: ${describeComponent(signalComponent)}` : `Signal asset unresolved: ${signalUid}`,
    switchUid ? `Reported switch GUID: ${switchUid}` : "",
    switchComponent ? `Resolved switch asset: ${describeComponent(switchComponent)}` : switchUid ? `Switch asset unresolved: ${switchUid}` : "",
    normalState ? `NORMAL flag: ${normalState}` : "",
    reverseState ? `REVERSE flag: ${reverseState}` : "",
    stationRow?.station_name ? `Station: ${stationRow.station_name}` : "",
    stationRow?.control_point_number ? `Control point: ${stationRow.control_point_number} (${stationRow.control_point_name ?? stationRow.station_name ?? ""})`.replace(/\s+\(\)$/, "") : "",
    stationRow?.subdivision_name ? `Subdivision: ${stationRow.subdivision_name}` : "",
    stationRow?.code_line_number ? `Code line ${stationRow.code_line_number}: ${stationRow.code_line_name ?? ""}`.replace(/\s+$/, "") : "",
    ...switchControlAssignments,
    ...switchIndicationAssignments,
  ].filter(Boolean);
  const summary = `CAD logged switch alignment error for ${reportedName} (${signalUid})${switchComponent ? ` involving ${switchComponent.component_name ?? "switch"} (${switchUid})` : switchUid ? ` involving switch ${switchUid}` : ""}.`;

  return {
    lineId: line.id,
    lineNumber: line.lineNumber,
    timestamp: line.timestamp,
    raw: line.raw,
    translation: {
      original: line.raw,
      structured,
      english: [summary],
      unresolved: [
        signalComponent ? "" : `Signal UID ${signalUid} was not found in bundled component lookup.`,
        switchUid && !switchComponent ? `Switch UID ${switchUid} was not found in bundled component lookup.` : "",
      ].filter(Boolean),
    },
    workflow: {
      summary,
      currentStep: "SWITCH ALIGNMENT ERROR",
      systems: ["CAD", "Code Server", "Field indication"],
      objects: [stationRow?.station_name ?? "", reportedName, switchComponent?.component_name ?? ""].filter(Boolean),
      knownState: [normalState ? `NORMAL=${normalState}` : "", reverseState ? `REVERSE=${reverseState}` : ""].filter(Boolean).join("; "),
      unresolved: signalComponent && (!switchUid || switchComponent) ? [] : ["One or more component UIDs did not resolve from bundled static data."],
    },
    genisysContext: [],
    icdContext: [],
    databaseContext: structured,
    workflowContext: [
      "The workstation log reports a switch alignment error from feedback, not a normal received indication.",
      switchComponent ? `Switch GUID ${switchUid} resolves to ${switchComponent.component_name ?? "switch"} under ${stationRow?.station_name ?? "the parent control point"}.` : "",
    ].filter(Boolean),
    payloadContext: structured,
    sourceReferences: [
      sourceReference("component_lookup", "Raw static component lookup", "exports/raw/sql_foundation/tmdsDatabaseStatic.component_lookup.json", "Used to resolve signal and switch UIDs in CAD feedback errors."),
      sourceReference("code_station_assignment_map", "Normalized code station assignment map", "exports/normalized/code_station_assignment_map.json", "Used to show related switch control/indication assignments for the parent control point."),
    ],
  };
}

function makeBocSystemStatsDetail(line: ParsedLine): DetailModel | null {
  if (!/\bBackOfficeControl SystemStats:/i.test(line.raw)) return null;
  const fields = parseAngleFieldMap(line.raw);
  const computerName = fields.get("ComputerName") ?? "";
  const softwareVersion = fields.get("SoftwareVersion") ?? "";
  const summary = computerName
    ? `BackOfficeControl system statistics from ${computerName}.`
    : "BackOfficeControl system statistics.";
  const payloadContext = [
    computerName ? `Computer name: ${computerName}` : "",
    softwareVersion ? `Software version: ${softwareVersion}` : "",
    fields.get("AppStartTime") ? `App start time: ${fields.get("AppStartTime")}` : "",
    fields.get("WorkingMemory") ? `Working memory: ${formatBytes(fields.get("WorkingMemory"))}` : "",
    fields.get("PeakWorkingMemory") ? `Peak working memory: ${formatBytes(fields.get("PeakWorkingMemory"))}` : "",
    fields.get("PagedMemory") ? `Paged memory: ${formatBytes(fields.get("PagedMemory"))}` : "",
    fields.get("PeakPagedMemory") ? `Peak paged memory: ${formatBytes(fields.get("PeakPagedMemory"))}` : "",
    fields.get("ThreadCount") ? `Thread count: ${fields.get("ThreadCount")}` : "",
    fields.get("HandleCount") ? `Handle count: ${fields.get("HandleCount")}` : "",
    fields.get("PrivilegedProcessorTime") ? `Privileged processor time: ${fields.get("PrivilegedProcessorTime")}` : "",
    fields.get("TotalProcessorTime") ? `Total processor time: ${fields.get("TotalProcessorTime")}` : "",
  ].filter(Boolean);

  return {
    lineId: line.id,
    lineNumber: line.lineNumber,
    timestamp: line.timestamp,
    raw: line.raw,
    translation: {
      original: line.raw,
      structured: [line.source ? `File: ${getFileLabel(line.source)}` : "", line.timestamp ? `Timestamp: ${line.timestamp}` : "", ...payloadContext].filter(Boolean),
      english: [summary, "This is a BOC operational health snapshot logged by BackOfficeControl.ControlApp.LogBocStats."],
      unresolved: [],
    },
    workflow: {
      summary,
      currentStep: "BOC system statistics",
      systems: ["BackOfficeControl", computerName].filter(Boolean),
      objects: computerName ? [computerName] : [],
      knownState: softwareVersion ? `Running software ${softwareVersion}` : "",
      unresolved: [],
    },
    genisysContext: [],
    icdContext: [],
    databaseContext: [],
    workflowContext: ["BackOfficeControl logs this line as a periodic application/process health record."],
    payloadContext,
    sourceReferences: [
      sourceReference("music_more_boc", "MORE BOC back-office control logs", "sample_logs/curated/boc_sample.log", "Pattern grounded from local BackOfficeControl EventLog samples."),
      sourceReference("mdm_training_boc", "BackOfficeControl training material", "exports/manuals/training/MDM_Training.txt", "Training material describes BOC as the UI/control application for the service-side system."),
    ],
  };
}

function makeIcdJsonDetail(line: ParsedLine): DetailModel | null {
  const match = /TryDeserialize successfully deserialized to\s+(Msg(\d+)_([A-Za-z0-9_]+)):\s*(\{.*\})\|?$/i.exec(line.raw);
  if (!match) return null;
  const messageType = match[1];
  const messageId = normalizeMessageId(match[2]);
  const messageName = match[3].replace(/_/g, " ");
  const catalogRow = icdByMessageId.get(messageId);
  const payload = extractJsonPayload(line.raw);
  const payloadRecord = payload && typeof payload === "object" ? payload as Record<string, unknown> : null;
  const locomotives = Array.isArray(payloadRecord?.Locomotives) ? payloadRecord.Locomotives as Array<Record<string, unknown>> : [];
  const firstLoco = locomotives[0];
  const locoSummary = firstLoco ? [
    stringifyScalar(firstLoco.LocoID) ? `Loco ID: ${stringifyScalar(firstLoco.LocoID)}` : "",
    stringifyScalar(firstLoco.UpdateType) ? `Update type: ${stringifyScalar(firstLoco.UpdateType)}` : "",
    stringifyScalar(firstLoco.LocoStateSummary) ? `State summary: ${stringifyScalar(firstLoco.LocoStateSummary)}` : "",
    stringifyScalar(firstLoco.LocoState) ? `Loco state: ${stringifyScalar(firstLoco.LocoState)}` : "",
    stringifyScalar(firstLoco.RepositoryTransferStates) ? `Repository transfer: ${stringifyScalar(firstLoco.RepositoryTransferStates)}` : "",
    stringifyScalar(firstLoco.DownloadStatus) ? `Download status: ${stringifyScalar(firstLoco.DownloadStatus)}` : "",
    stringifyScalar(firstLoco.UploadStatus) ? `Upload status: ${stringifyScalar(firstLoco.UploadStatus)}` : "",
    stringifyScalar(firstLoco.CurrentUploadProgress) ? `Current upload progress: ${stringifyScalar(firstLoco.CurrentUploadProgress)}` : "",
  ].filter(Boolean) : [];
  const nestedStatus = [
    stringifyScalar(valueAtPath(payload, ["LocoID"])) ? `Loco ID: ${stringifyScalar(valueAtPath(payload, ["LocoID"]))}` : "",
    stringifyScalar(valueAtPath(payload, ["LocomotiveStatus", "LocoState"])) ? `Loco state: ${stringifyScalar(valueAtPath(payload, ["LocomotiveStatus", "LocoState"]))}` : "",
    stringifyScalar(valueAtPath(payload, ["LocomotiveStatus", "StateSummary"])) ? `State summary: ${stringifyScalar(valueAtPath(payload, ["LocomotiveStatus", "StateSummary"]))}` : "",
    stringifyScalar(valueAtPath(payload, ["LocomotiveStatus", "ReasonForChange"])) ? `Reason for change: ${stringifyScalar(valueAtPath(payload, ["LocomotiveStatus", "ReasonForChange"]))}` : "",
    stringifyScalar(valueAtPath(payload, ["LocomotiveStatus", "LocoPositionInfo", "ReasonForReport"])) ? `Reason for report: ${stringifyScalar(valueAtPath(payload, ["LocomotiveStatus", "LocoPositionInfo", "ReasonForReport"]))}` : "",
  ].filter(Boolean);
  const payloadContext = [
    `Message type: ${messageType}`,
    `Message ID: ${displayMessageId(messageId)}`,
    catalogRow?.message_name ? `Catalog name: ${catalogRow.message_name}` : `Name from log: ${messageName}`,
    catalogRow?.direction ? `Catalog direction: ${catalogRow.direction}` : "",
    catalogRow?.document_title ? `Catalog source: ${catalogRow.document_title}` : "",
    `JSON payload parsed: ${payloadRecord ? "yes" : "no"}`,
    locomotives.length ? `Locomotives in payload: ${locomotives.length}` : "",
    ...locoSummary,
    ...nestedStatus,
  ].filter(Boolean);
  const summary = catalogRow?.message_name
    ? `ICD parser decoded ${displayMessageId(messageId)} ${catalogRow.message_name}.`
    : `ICD parser decoded ${displayMessageId(messageId)} ${messageName}.`;

  return {
    lineId: line.id,
    lineNumber: line.lineNumber,
    timestamp: line.timestamp,
    raw: line.raw,
    translation: {
      original: line.raw,
      structured: [line.source ? `File: ${getFileLabel(line.source)}` : "", line.timestamp ? `Timestamp: ${line.timestamp}` : "", ...payloadContext].filter(Boolean),
      english: [summary, firstLoco ? "Payload contains a BOC/BOS locomotive update record." : "Payload was parsed as an ICD JSON message."],
      unresolved: catalogRow ? [] : ["No bundled ICD catalog row matched this message ID; message identity is grounded from the log line itself."],
    },
    workflow: {
      summary,
      currentStep: "ICD JSON deserialize",
      systems: ["JsonDataIcdBase", "BackOfficeControl"],
      objects: firstLoco && stringifyScalar(firstLoco.LocoID) ? [stringifyScalar(firstLoco.LocoID)] : [],
      knownState: firstLoco && stringifyScalar(firstLoco.LocoState) ? `Loco state ${stringifyScalar(firstLoco.LocoState)}` : "",
      unresolved: catalogRow ? [] : ["Bundled ICD catalog did not contain this message ID."],
    },
    genisysContext: [],
    icdContext: payloadContext,
    databaseContext: [],
    workflowContext: ["The parser accepted the JSON payload and materialized it as the logged message class."],
    payloadContext,
    sourceReferences: [
      sourceReference("icd_message_catalog", "Bundled ICD message catalog", "exports/mappings/icd_message_catalog.json", "Used when a message ID is present in the bundled catalog."),
      sourceReference("music_more_boc", "MORE BOC back-office control logs", "sample_logs/curated/boc_sample.log", "Pattern grounded from local BOC/BOS sample lines."),
    ],
  };
}

function makeBocBosProcessingDetail(line: ParsedLine): DetailModel | null {
  const match = /<<(SEND|RECV)>>\s+(Msg(\d+)_([A-Za-z0-9_]+))\s+Processing Complete:\s+([^:]+?)\s+Message Data:\s+(.+?)\|?$/i.exec(line.raw);
  if (!match) return null;
  const direction = match[1].toUpperCase();
  const messageType = match[2];
  const messageId = normalizeMessageId(match[3]);
  const messageName = match[4].replace(/_/g, " ");
  const route = match[5].trim();
  const data = match[6].trim();
  const catalogRow = icdByMessageId.get(messageId);
  const fields = parseAngleFieldMap(data);
  const fieldList = parseAngleFields(data).slice(0, 48).map((field) => `Message field: ${field}`);
  const locoId = fields.get("LocoID") ?? "";
  const locoState = fields.get("LocoStatus") ?? fields.get("LocoState") ?? "";
  const payloadContext = [
    `Direction marker: ${direction}`,
    `Route: ${route}`,
    `Message type: ${messageType}`,
    `Message ID: ${displayMessageId(messageId)}`,
    catalogRow?.message_name ? `Catalog name: ${catalogRow.message_name}` : `Name from log: ${messageName}`,
    fields.get("Version") ? `Version: ${fields.get("Version")}` : "",
    locoId ? `Loco ID: ${locoId}` : "",
    fields.get("UpdateType") ? `Update type: ${fields.get("UpdateType")}` : "",
    fields.get("LocoStateSummary") ? `Loco state summary: ${fields.get("LocoStateSummary")}` : "",
    locoState ? `Loco status: ${locoState}` : "",
    fields.get("RepositoryTransferStates") ? `Repository transfer: ${fields.get("RepositoryTransferStates")}` : "",
    fields.get("DownloadStatus") ? `Download status: ${fields.get("DownloadStatus")}` : "",
    fields.get("UploadStatus") ? `Upload status: ${fields.get("UploadStatus")}` : "",
    fields.get("CurrentUploadProgress") ? `Current upload progress: ${fields.get("CurrentUploadProgress")}` : "",
    ...fieldList,
  ].filter(Boolean);
  const summary = `${route} ${direction === "RECV" ? "received" : "sent"} ${displayMessageId(messageId)} ${catalogRow?.message_name ?? messageName}; processing completed.`;

  return {
    lineId: line.id,
    lineNumber: line.lineNumber,
    timestamp: line.timestamp,
    raw: line.raw,
    translation: {
      original: line.raw,
      structured: [line.source ? `File: ${getFileLabel(line.source)}` : "", line.timestamp ? `Timestamp: ${line.timestamp}` : "", ...payloadContext].filter(Boolean),
      english: [summary],
      unresolved: catalogRow ? [] : ["No bundled ICD catalog row matched this message ID; message identity is grounded from the log line itself."],
    },
    workflow: {
      summary,
      currentStep: "BOC/BOS message processing complete",
      systems: route.split("->").map((part) => part.trim()).filter(Boolean),
      objects: locoId ? [locoId] : [],
      knownState: locoState ? `Loco status ${locoState}` : "",
      unresolved: catalogRow ? [] : ["Bundled ICD catalog did not contain this message ID."],
    },
    genisysContext: [],
    icdContext: payloadContext,
    databaseContext: [],
    workflowContext: [`${route} processing path completed for ${messageType}.`],
    payloadContext,
    sourceReferences: [
      sourceReference("icd_message_catalog", "Bundled ICD message catalog", "exports/mappings/icd_message_catalog.json", "Used when a message ID is present in the bundled catalog."),
      sourceReference("music_more_boc", "MORE BOC back-office control logs", "sample_logs/curated/boc_sample.log", "Pattern grounded from local BOC/BOS sample lines."),
    ],
  };
}

function makeStaticBrowserDetail(line: ParsedLine, lines: ParsedLine[], index: number): DetailModel | null {
  return (
    makeBocSystemStatsDetail(line) ??
    makeIcdJsonDetail(line) ??
    makeBocBosProcessingDetail(line) ??
    makeCadFeedbackProblemDetail(line) ??
    makeSocketRawFrameDetail(line) ??
    (/\b(IND MNEM|CTL MNEM|SendControl|ProcessControlBegin|CONTROL UPDATED|PROCESS IND|INDICATION;|CONTROL;|CONTROL SENT|CONTROL(?:\s+UPDATE\s+ONLY)?:)\b/i.test(line.raw)
      ? makeStaticDetail(line, lines, index)
      : null)
  );
}

export function buildStaticDetailForLine(lines: ParsedLine[], line: ParsedLine, lineIndex?: number): DetailModel | null {
  const index = lineIndex ?? lines.findIndex((candidate) => candidate.id === line.id);
  return makeStaticBrowserDetail(line, lines, index >= 0 ? index : 0);
}

function buildStaticLineDetails(lines: ParsedLine[]): Record<string, DetailModel> {
  const details: Record<string, DetailModel> = {};
  for (let index = 0; index < lines.length; index += 1) {
    const detail = makeStaticBrowserDetail(lines[index], lines, index);
    if (detail) {
      details[lines[index].id] = detail;
    }
  }
  return details;
}

function makeReferenceDetail(raw: string, summary: string, structured: string[], sourcePath: string): DetailModel {
  return {
    lineId: "",
    lineNumber: 0,
    raw,
    translation: {
      original: raw,
      structured,
      english: [summary],
      unresolved: [],
    },
    workflow: {
      summary,
      currentStep: "Static reference entry",
      systems: [],
      objects: [],
      knownState: "",
      unresolved: [],
    },
    genisysContext: [],
    icdContext: structured,
    databaseContext: structured,
    workflowContext: [],
    payloadContext: [],
    sourceReferences: [
      sourceReference("static_reference_bundle", "Static GitHub reference bundle", sourcePath, "Bundled into the renderer for GitHub Pages local/static mode."),
    ],
  };
}

function makeReferenceLine(id: string, lineNumber: number, source: string, raw: string, detail: DetailModel): { line: ParsedLine; detail: DetailModel } {
  const line: ParsedLine = {
    id,
    lineNumber,
    source,
    raw,
    tokens: raw.split(/\s+/).filter(Boolean),
  };
  return { line, detail: { ...detail, lineId: id, lineNumber, raw } };
}

export function buildStaticReferenceSession(): SessionData {
  const pairs: Array<{ line: ParsedLine; detail: DetailModel }> = [];
  let lineNumber = 1;

  for (const row of staticAssignmentRows.slice(0, 800)) {
    const title = [row.station_name, row.control_point_name, row.code_line_name].filter(Boolean).join(" / ");
    if (!title) continue;
    const raw = `Code station assignment: ${title}`;
    const structured = [
      row.station_name ? `Station: ${row.station_name}` : "",
      row.control_point_name ? `Control point: ${row.control_point_name}` : "",
      row.code_line_name ? `Code line: ${row.code_line_name}` : "",
      row.control_address ? `Control address: ${row.control_address}` : "",
      row.indication_address ? `Indication address: ${row.indication_address}` : "",
      `Control assignments: ${row.control_assignments?.length ?? 0}`,
      `Indication assignments: ${row.indication_assignments?.length ?? 0}`,
      ...(row.control_assignments ?? []).slice(0, 8).map((entry) => `Control bit ${entry.bit_position}: ${assignmentLabel(entry)}`),
      ...(row.indication_assignments ?? []).slice(0, 8).map((entry) => `Indication bit ${entry.bit_position}: ${assignmentLabel(entry)}`),
    ].filter(Boolean);
    pairs.push(makeReferenceLine(`static-ref-code-${lineNumber}`, lineNumber, "reference:code-stations", raw, makeReferenceDetail(raw, `Static code station assignment reference for ${title}.`, structured, "exports/normalized/code_station_assignment_map.json")));
    lineNumber += 1;
  }

  for (const row of staticIcdRows.slice(0, 1200)) {
    const id = displayMessageId(row.message_id);
    const raw = `Train/office ICD message ${id}: ${row.message_name ?? "unnamed"}${row.message_version ? ` v${row.message_version}` : ""}`;
    const structured = [
      row.message_id ? `Message ID: ${id}` : "",
      row.message_name ? `Message name: ${row.message_name}` : "",
      row.message_version ? `Message version: ${row.message_version}` : "",
      row.direction ? `Direction: ${row.direction}` : "",
      row.document_title ? `Document: ${row.document_title}` : "",
      row.release ? `Release: ${row.release}` : "",
      row.section ? `Section: ${row.section}` : "",
      row.page ? `Page: ${row.page}` : "",
    ].filter(Boolean);
    pairs.push(makeReferenceLine(`static-ref-icd-${lineNumber}`, lineNumber, "reference:icd-messages", raw, makeReferenceDetail(raw, `Static ICD catalog entry for ${id} ${row.message_name ?? ""}.`.trim(), structured, "exports/mappings/icd_message_catalog.json")));
    lineNumber += 1;
  }

  for (const header of [...(staticGenisysReference.office_headers ?? []), ...(staticGenisysReference.field_headers ?? [])]) {
    if (!header.byte || !header.meaning) continue;
    const raw = `Genisys header ${header.byte}: ${header.meaning}`;
    pairs.push(makeReferenceLine(`static-ref-genisys-${lineNumber}`, lineNumber, "reference:genisys", raw, makeReferenceDetail(raw, `Genisys protocol header ${header.byte} means ${header.meaning}.`, [`Byte: ${header.byte}`, `Meaning: ${header.meaning}`], "exports/mappings/genisys_protocol_reference.json")));
    lineNumber += 1;
  }

  for (const bit of staticGenisysReference.mode_bit_definitions ?? []) {
    if (bit.bit === undefined || !bit.meaning) continue;
    const raw = `Genisys mode bit ${bit.bit}: ${bit.meaning}`;
    pairs.push(makeReferenceLine(`static-ref-genisys-${lineNumber}`, lineNumber, "reference:genisys", raw, makeReferenceDetail(raw, `Genisys mode bit ${bit.bit} means ${bit.meaning}.`, [`Bit: ${bit.bit}`, `Meaning: ${bit.meaning}`], "exports/mappings/genisys_protocol_reference.json")));
    lineNumber += 1;
  }

  const lines = pairs.map((pair) => pair.line);
  const lineDetails = Object.fromEntries(pairs.map((pair) => [pair.line.id, pair.detail]));
  return {
    sessionId: `static-reference-${Date.now()}`,
    lines,
    detail: lines[0] ? lineDetails[lines[0].id] : null,
    lineDetails,
  };
}

export function buildStaticReviewSampleSession(): SessionData {
  const lines = [
    ...parseLines(genisysSampleLog, "sample_logs/curated/genisys_sample.log"),
    ...parseLines(socketTraceSampleLog, "sample_logs/curated/sockettrace_sample.log"),
    ...parseLines(workflowSampleLog, "sample_logs/curated/workflow_sample.log"),
  ];
  return {
    sessionId: `static-review-sample-${Date.now()}`,
    lines,
    detail: null,
    lineDetails: buildStaticLineDetails(lines),
  };
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
    lineDetails: out.length > 25000 ? {} : buildStaticLineDetails(out),
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
