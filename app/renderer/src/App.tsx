import { Fragment, startTransition, useEffect, useMemo, useRef, useState } from "react";
import type { Dispatch, DragEvent, ReactNode, SetStateAction } from "react";
import type { DetailModel, ParsedLine, ReferenceArtifact, ReferenceChoiceGroup, ReferenceChoiceItem, ReferenceDiagram, SearchConfig, SessionData, WorkflowRelatedDetail, WorkspaceProgress } from "@shared/types";
import type { WorkspaceMenuCommand } from "@shared/native-api";
import { buildStaticReferenceSession, buildStaticReviewSampleSession, ingestBrowserFilesLocally } from "@shared/browser-parser";
import { stripLeadingLogTimestamp } from "@shared/parser/primitives";
import { AccountPanel, AdminPanel, LoginScreen, fetchAuthState, logout } from "./features/auth";
import type { AuthState } from "./features/auth";

const defaultSearch: SearchConfig = {
  query: "",
  caseSensitive: false,
  wholeWord: false,
  regex: false,
  wrapAround: true,
  filterOnlyMatches: false,
};

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function buildMatchExpression(config: SearchConfig): RegExp | null {
  if (!config.query) return null;
  const flags = config.caseSensitive ? "g" : "gi";
  if (config.regex) {
    try {
      return new RegExp(config.query, flags);
    } catch {
      return null;
    }
  }
  const escaped = escapeRegExp(config.query);
  const source = config.wholeWord ? `\\b${escaped}\\b` : escaped;
  return new RegExp(source, flags);
}

function getSearchPatternError(config: SearchConfig): string {
  if (!config.query || !config.regex) {
    return "";
  }
  try {
    new RegExp(config.query, config.caseSensitive ? "g" : "gi");
    return "";
  } catch (error) {
    return error instanceof Error ? error.message : "Invalid regular expression.";
  }
}

function textMatchesPattern(text: string | undefined, pattern: RegExp | null): boolean {
  if (!pattern) return true;
  pattern.lastIndex = 0;
  return pattern.test(text ?? "");
}

function buildDetailSearchText(detail: DetailModel | null): string[] {
  if (!detail) {
    return [];
  }
  return [
    detail.translation.original,
    ...detail.translation.structured,
    ...detail.translation.english,
    ...detail.translation.unresolved,
    detail.workflow.summary,
    detail.workflow.currentStep,
    detail.workflow.priorStep ?? "",
    detail.workflow.nextStep ?? "",
    detail.workflow.knownState,
    ...detail.workflow.systems,
    ...detail.workflow.objects,
    ...detail.workflow.unresolved,
    ...(detail.workflowRelated ?? []).flatMap((entry) => [
      `${entry.lineNumber}`,
      entry.raw,
      entry.deltaLabel,
      entry.relation,
    ]),
    detail.relatedPair?.relationLabel ?? "",
    detail.relatedPair?.raw ?? "",
    detail.relatedPair?.summary ?? "",
    detail.relatedPair?.reason ?? "",
    ...(detail.databaseContext ?? []),
    ...(detail.workflowContext ?? []),
    ...(detail.payloadContext ?? []),
    ...(detail.referenceBadges ?? []),
    ...((detail.referenceChoiceGroups ?? []).flatMap((group) => [
      group.label,
      ...group.items.flatMap((item) => collectReferenceChoiceSearchText(item)),
    ])),
  ].filter(Boolean);
}

function collectReferenceChoiceSearchText(item: ReferenceChoiceItem): string[] {
  return [
    item.label,
    ...item.content,
    ...((item.detailChoiceGroups ?? []).flatMap((group) => [
      group.label,
      ...group.items.flatMap((nestedItem) => collectReferenceChoiceSearchText(nestedItem)),
    ])),
  ];
}

function matchesWithPattern(line: ParsedLine, pattern: RegExp | null, detail?: DetailModel | null): boolean {
  if (!pattern) return true;
  const searchFields = [
    line.raw,
    line.timestamp ?? "",
    line.source ?? "",
    ...buildDetailSearchText(detail ?? null),
  ];
  return searchFields.some((field) => textMatchesPattern(field, pattern));
}

function matches(line: ParsedLine, config: SearchConfig, detail?: DetailModel | null): boolean {
  return matchesWithPattern(line, buildMatchExpression(config), detail);
}

function matchesReferenceChoiceItem(item: ReferenceChoiceItem, config: SearchConfig): boolean {
  const pattern = buildMatchExpression(config);
  if (!pattern) {
    return true;
  }
  return collectReferenceChoiceSearchText(item).some((value) => textMatchesPattern(value, pattern));
}

function hasActiveSearch(config: SearchConfig): boolean {
  return config.query.trim().length > 0;
}

function createSearchConfig(query: string): SearchConfig {
  return {
    ...defaultSearch,
    query,
  };
}

function renderHighlightedText(text: string, config: SearchConfig): ReactNode {
  const pattern = buildMatchExpression(config);
  if (!pattern) return text;

  const output: ReactNode[] = [];
  let lastIndex = 0;
  let match = pattern.exec(text);
  while (match) {
    const start = match.index;
    const matchText = match[0];
    if (start > lastIndex) {
      output.push(text.slice(lastIndex, start));
    }
    output.push(<mark key={`${start}-${matchText}`}>{matchText}</mark>);
    lastIndex = start + matchText.length;
    if (!matchText.length) {
      pattern.lastIndex += 1;
    }
    match = pattern.exec(text);
  }
  if (!output.length) {
    return text;
  }
  if (lastIndex < text.length) {
    output.push(text.slice(lastIndex));
  }
  return output;
}

function waitForNextPaint() {
  return new Promise<void>((resolve) => {
    window.requestAnimationFrame(() => resolve());
  });
}

function isBrowserWebAppMode(): boolean {
  return !window.tmds?.pickInputPaths;
}

function AuthChip({
  authState,
  onLogout,
  onOpenAdmin,
  onOpenAccount,
}: {
  authState: AuthState;
  onLogout?: () => void;
  onOpenAdmin?: () => void;
  onOpenAccount?: () => void;
}) {
  return (
    <div className="auth-chip" role="status">
      <span className="auth-chip-user">
        <strong>{authState.username ?? "Signed in"}</strong>
      </span>
      {authState.role === "Administrator" && onOpenAdmin ? (
        <button type="button" className="ghost compact auth-action-button" onClick={onOpenAdmin}>
          Admin
          {authState.pendingPasswordResetCount ? (
            <span className="auth-alert-badge" aria-label={`${authState.pendingPasswordResetCount} password reset request${authState.pendingPasswordResetCount === 1 ? "" : "s"}`}>
              {authState.pendingPasswordResetCount}
            </span>
          ) : null}
        </button>
      ) : null}
      {authState.role !== "Administrator" && onOpenAccount ? (
        <button type="button" className="ghost compact" onClick={onOpenAccount}>User</button>
      ) : null}
      {onLogout ? (
        <button type="button" className="ghost compact" onClick={onLogout}>Sign out</button>
      ) : null}
    </div>
  );
}

type BrowserUploadFile = {
  file: File;
  relativePath: string;
};

type DroppedBrowserFiles = {
  files: BrowserUploadFile[];
  skipped: string[];
};

type BrowserFileSystemEntry = {
  name: string;
  fullPath?: string;
  isFile: boolean;
  isDirectory: boolean;
};

type BrowserFileSystemFileEntry = BrowserFileSystemEntry & {
  file: (successCallback: (file: File) => void, errorCallback?: (error: DOMException) => void) => void;
};

type BrowserFileSystemDirectoryEntry = BrowserFileSystemEntry & {
  createReader: () => {
    readEntries: (
      successCallback: (entries: BrowserFileSystemEntry[]) => void,
      errorCallback?: (error: DOMException) => void,
    ) => void;
  };
};

type DataTransferItemWithEntry = DataTransferItem & {
  webkitGetAsEntry?: () => BrowserFileSystemEntry | null;
  getAsEntry?: () => BrowserFileSystemEntry | null;
};

function normalizeBrowserRelativePath(path: string): string {
  return path.replace(/\\/g, "/").replace(/^\/+/, "");
}

function getBrowserFileRelativePath(file: File): string {
  return normalizeBrowserRelativePath((file as File & { webkitRelativePath?: string }).webkitRelativePath || file.name);
}

function normalizeBrowserUploadFiles(fileList: FileList | File[]): BrowserUploadFile[] {
  return Array.from(fileList).map((file) => ({
    file,
    relativePath: getBrowserFileRelativePath(file),
  }));
}

function getBrowserUploadRootFolders(files: BrowserUploadFile[]): string[] {
  const roots = new Set<string>();
  for (const entry of files) {
    const relativePath = entry.relativePath || entry.file.name;
    const parts = relativePath.split("/").filter(Boolean);
    if (parts.length > 1) {
      roots.add(parts[0]);
    }
  }
  return Array.from(roots).sort((left, right) => left.localeCompare(right));
}

function summarizeBrowserUpload(files: BrowserUploadFile[]): string {
  const fileCount = files.length;
  const roots = getBrowserUploadRootFolders(files);
  if (roots.length) {
    const folderText = `${roots.length} folder${roots.length === 1 ? "" : "s"}`;
    const itemText = `${fileCount} item${fileCount === 1 ? "" : "s"}`;
    return `${folderText} / ${itemText}`;
  }
  return `${fileCount} file${fileCount === 1 ? "" : "s"}`;
}

function getDroppedEntry(item: DataTransferItem): BrowserFileSystemEntry | null {
  const withEntry = item as DataTransferItemWithEntry;
  return withEntry.webkitGetAsEntry?.() ?? withEntry.getAsEntry?.() ?? null;
}

function readDroppedFileEntry(entry: BrowserFileSystemFileEntry, relativePath: string): Promise<BrowserUploadFile> {
  return new Promise((resolve, reject) => {
    entry.file(
      (file) => resolve({ file, relativePath }),
      (error) => reject(error),
    );
  });
}

function readDroppedDirectoryBatch(entry: BrowserFileSystemDirectoryEntry): Promise<BrowserFileSystemEntry[]> {
  const reader = entry.createReader();
  const batches: BrowserFileSystemEntry[] = [];
  return new Promise((resolve, reject) => {
    const readNext = () => {
      reader.readEntries(
        (entries) => {
          if (!entries.length) {
            resolve(batches);
            return;
          }
          batches.push(...entries);
          readNext();
        },
        (error) => reject(error),
      );
    };
    readNext();
  });
}

async function collectDroppedEntryFiles(
  entry: BrowserFileSystemEntry,
  parentPath: string,
  skipped: string[],
): Promise<BrowserUploadFile[]> {
  const entryPath = normalizeBrowserRelativePath(parentPath ? `${parentPath}/${entry.name}` : entry.name);
  if (entry.isFile) {
    try {
      return [await readDroppedFileEntry(entry as BrowserFileSystemFileEntry, entryPath)];
    } catch (error) {
      const reason = error instanceof DOMException ? error.message : "browser could not open the dropped file";
      skipped.push(`${entryPath}: ${reason}`);
      return [];
    }
  }
  if (!entry.isDirectory) {
    return [];
  }
  let children: BrowserFileSystemEntry[] = [];
  try {
    children = await readDroppedDirectoryBatch(entry as BrowserFileSystemDirectoryEntry);
  } catch (error) {
    const reason = error instanceof DOMException ? error.message : "browser could not open the dropped folder";
    skipped.push(`${entryPath}: ${reason}`);
    return [];
  }
  const nested = await Promise.all(children.map((child) => collectDroppedEntryFiles(child, entryPath, skipped)));
  return nested.flat();
}

async function collectDroppedBrowserFiles(dataTransfer: DataTransfer): Promise<DroppedBrowserFiles> {
  const skipped: string[] = [];
  const entries = Array.from(dataTransfer.items ?? [])
    .map((item) => getDroppedEntry(item))
    .filter((entry): entry is BrowserFileSystemEntry => Boolean(entry));
  if (!entries.length) {
    return { files: normalizeBrowserUploadFiles(dataTransfer.files), skipped };
  }
  const files = (await Promise.all(entries.map((entry) => collectDroppedEntryFiles(entry, "", skipped)))).flat();
  return { files, skipped };
}

async function uploadBrowserFiles(
  files: BrowserUploadFile[],
  onProgress?: (progress: WorkspaceProgress) => void,
): Promise<SessionData> {
  const lastPath = files[files.length - 1]?.relativePath || files[files.length - 1]?.file.name;
  const formData = new FormData();
  const manifest = {
    files: files.map((entry, index) => ({
      fieldName: `f${index}`,
      name: entry.file.name,
      relativePath: entry.relativePath || entry.file.name,
    })),
  };
  formData.append("manifest", JSON.stringify(manifest));
  files.forEach((entry, index) => {
    formData.append(`f${index}`, entry.file, entry.file.name);
  });

  onProgress?.({
    phase: "read",
    message: `Uploading ${files.length} file${files.length === 1 ? "" : "s"} to the TMDS server...`,
    percent: 8,
    completed: files.length,
    total: files.length,
    currentPath: lastPath,
  });

  let response: Response;
  try {
    response = await fetch("/api/ingest-upload", {
      method: "POST",
      body: formData,
    });
  } catch (error) {
    throw normalizeBackendFetchError("/api/ingest-upload", error);
  }

  if (!response.ok || !response.body) {
    let errorMessage = `TMDS upload failed with status ${response.status}`;
    try {
      const text = await response.text();
      const firstLine = text.split("\n").map((line) => line.trim()).filter(Boolean)[0] ?? "";
      if (firstLine.startsWith("{")) {
        const parsed = JSON.parse(firstLine) as { error?: string; message?: string };
        errorMessage = parsed.error || parsed.message || errorMessage;
      } else if (firstLine) {
        errorMessage = firstLine.slice(0, 240);
      }
    } catch {
      // ignore — fall through with default errorMessage
    }
    throw new Error(errorMessage);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8");
  let buffered = "";
  let resultData: SessionData | null = null;
  let streamedResult: SessionData | null = null;
  let streamError: Error | null = null;

  const handleEvent = (rawLine: string): void => {
    if (!rawLine) return;
    let event: {
      type?: string;
      progress?: WorkspaceProgress;
      data?: SessionData;
      lines?: ParsedLine[];
      sessionId?: string;
      detail?: DetailModel | null;
      lineDetails?: Record<string, DetailModel>;
      message?: string;
    };
    try {
      event = JSON.parse(rawLine);
    } catch {
      return;
    }
    if (event.type === "progress" && event.progress) {
      onProgress?.(event.progress);
    } else if (event.type === "result" && event.data) {
      resultData = event.data;
    } else if (event.type === "result-start") {
      streamedResult = {
        sessionId: event.sessionId,
        lines: [],
        detail: event.detail ?? null,
        lineDetails: event.lineDetails ?? {},
      };
    } else if (event.type === "result-lines" && Array.isArray(event.lines)) {
      if (!streamedResult) {
        streamedResult = { lines: [], detail: null, lineDetails: {} };
      }
      streamedResult.lines.push(...event.lines);
    } else if (event.type === "result-end") {
      if (streamedResult) {
        resultData = streamedResult;
      }
    } else if (event.type === "error") {
      streamError = new Error(event.message || "TMDS server reported an error.");
    }
  };

  while (true) {
    const { value, done } = await reader.read();
    if (value && value.length) {
      buffered += decoder.decode(value, { stream: true });
      let newlineIdx = buffered.indexOf("\n");
      while (newlineIdx >= 0) {
        const line = buffered.slice(0, newlineIdx).trim();
        buffered = buffered.slice(newlineIdx + 1);
        handleEvent(line);
        newlineIdx = buffered.indexOf("\n");
      }
    }
    if (done) break;
  }
  buffered += decoder.decode();
  const tail = buffered.trim();
  if (tail) handleEvent(tail);

  if (streamError) throw streamError;
  if (resultData) return resultData;
  throw new Error("TMDS server finished without sending a result.");
}

async function fetchWebSession(path: string): Promise<SessionData> {
  return fetchJson<SessionData>(path);
}

function getBackendFetchMessage(path: string): string {
  const launcherStep = "Start (or restart) the tool with TMDS-Server-Switch.bat, wait for the local build to finish, then open Open-TMDS-Webapp.html or http://127.0.0.1:4173/.";

  try {
    if (window.location.protocol === "file:") {
      return `This TMDS page was opened directly from a local HTML file, so ${path} is unavailable. ${launcherStep}`;
    }

    const origin = window.location.origin || "the local TMDS server";
    return `The TMDS backend request ${path} could not reach ${origin}. ${launcherStep}`;
  } catch {
    return `The TMDS backend request ${path} failed. ${launcherStep}`;
  }
}

function normalizeBackendFetchError(path: string, error: unknown): Error {
  if (error instanceof Error && /Failed to fetch/i.test(error.message)) {
    return new Error(getBackendFetchMessage(path));
  }
  return error instanceof Error ? error : new Error(String(error));
}

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  try {
    const response = await fetch(path, init);
    if (!response.ok) {
      throw new Error(await response.text());
    }
    return response.json() as Promise<T>;
  } catch (error) {
    throw normalizeBackendFetchError(path, error);
  }
}

function buildApiPath(path: string, sessionId?: string | null): string {
  if (!sessionId) {
    return path;
  }
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}sessionId=${encodeURIComponent(sessionId)}`;
}

function getInitialWorkspaceError(): string {
  try {
    if (window.location.protocol === "file:") {
      return "This HTML file was opened directly from disk. Run TMDS-Server-Switch.bat first so the TMDS backend is running, then reopen the tool through Open-TMDS-Webapp.html or http://127.0.0.1:4173/.";
    }
  } catch {
    return "";
  }

  return "";
}

function getSourceLabel(source?: string): string {
  if (!source) return "unknown";
  const normalized = source.replace(/\\/g, "/");
  const parts = normalized.split("/");
  return parts[parts.length - 1] || source;
}

const categoryOrder = ["CODE", "CODELINE", "SOCKET", "CAD"] as const;
const referenceCategoryOrder = ["MESSAGE EXCHANGE", "GENISYS", "TRAIN MESSAGES", "CODELINES & STATIONS"] as const;
type LogCategory = (typeof categoryOrder)[number] | "OTHER" | "NETWORK" | "WORKFLOW" | (typeof referenceCategoryOrder)[number];
const logRowHeight = 28;
const logRowOverscan = 18;
const finderResultRowHeight = 30;
const finderResultOverscan = 24;
const pacificTimeZone = "America/Los_Angeles";
type LogTimeSourceMode = "original" | "utc" | "pacific";
type LogTimeDisplayMode = "source" | "utc" | "pacific";

type ViewerTimestampParts = {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
  millisecond: number;
};

type TimeOnlyParts = {
  hour: number;
  minute: number;
  second: number;
  hasExplicitSecond: boolean;
};

type DateOnlyParts = {
  year: number;
  month: number;
  day: number;
};

const viewerTimestampPattern = /^(?:(\d{2})-(\d{2})-(\d{4})|(\d{4})-(\d{2})-(\d{2})) (\d{2}):(\d{2}):(\d{2})\.(\d{3,4})$/;
const viewerSlashTimestampPattern = /^(\d{4})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{3,4})$/;
const pacificDatePartsFormatter = new Intl.DateTimeFormat("en-CA", {
  timeZone: pacificTimeZone,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});
const pacificZoneNameFormatter = new Intl.DateTimeFormat("en-US", {
  timeZone: pacificTimeZone,
  timeZoneName: "short",
});

function getViewerTimestampParts(timestamp?: string): ViewerTimestampParts | null {
  if (!timestamp) return null;
  const match = viewerTimestampPattern.exec(timestamp);
  const slashMatch = viewerSlashTimestampPattern.exec(timestamp);
  if (!match && !slashMatch) return null;
  return {
    year: Number(match ? (match[3] ?? match[4]) : slashMatch?.[1]),
    month: Number(match ? (match[1] ?? match[5]) : slashMatch?.[2]),
    day: Number(match ? (match[2] ?? match[6]) : slashMatch?.[3]),
    hour: Number(match ? match[7] : slashMatch?.[4]),
    minute: Number(match ? match[8] : slashMatch?.[5]),
    second: Number(match ? match[9] : slashMatch?.[6]),
    millisecond: Number(((match ? match[10] : slashMatch?.[7]) ?? "0").slice(0, 3).padEnd(3, "0")),
  };
}

function parseViewerTimestamp(timestamp?: string): number | null {
  const parts = getViewerTimestampParts(timestamp);
  if (!parts) return null;
  const { year, month, day, hour, minute, second, millisecond } = parts;
  return new Date(
    year,
    month - 1,
    day,
    hour,
    minute,
    second,
    millisecond,
  ).getTime();
}

function parseTimeValue(value: string): TimeOnlyParts | null {
  const match = /^(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(value.trim());
  if (!match) {
    return null;
  }
  return {
    hour: Number(match[1]),
    minute: Number(match[2]),
    second: Number(match[3] ?? "0"),
    hasExplicitSecond: Boolean(match[3]),
  };
}

function parseDateValue(value: string): DateOnlyParts | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value.trim());
  if (!match) {
    return null;
  }
  return {
    year: Number(match[1]),
    month: Number(match[2]),
    day: Number(match[3]),
  };
}

function normalizeTimeInput(value: string): string {
  const digits = value.replace(/\D/g, "").slice(0, 6);
  if (!digits) {
    return "";
  }
  const hourRaw = digits.slice(0, 2);
  const minuteRaw = digits.slice(2, 4);
  const secondRaw = digits.slice(4, 6);
  const hour = hourRaw.length === 2 ? String(Math.min(23, Number(hourRaw))).padStart(2, "0") : hourRaw;
  const minute = minuteRaw.length === 2 ? String(Math.min(59, Number(minuteRaw))).padStart(2, "0") : minuteRaw;
  const second = secondRaw.length === 2 ? String(Math.min(59, Number(secondRaw))).padStart(2, "0") : secondRaw;
  if (digits.length <= 2) {
    return hour;
  }
  if (digits.length <= 4) {
    return `${hour}:${minute}`;
  }
  return `${hour}:${minute}:${second}`;
}

function finalizeTimeInput(value: string): string {
  const normalized = normalizeTimeInput(value);
  if (/^\d{2}:\d{2}$/.test(normalized)) {
    return `${normalized}:00`;
  }
  return normalized;
}

function normalizeDateInput(value: string): string {
  const digits = value.replace(/\D/g, "").slice(0, 8);
  if (!digits) {
    return "";
  }
  const year = digits.slice(0, 4);
  const monthRaw = digits.slice(4, 6);
  const dayRaw = digits.slice(6, 8);
  const month = monthRaw.length === 2 ? String(Math.min(12, Math.max(1, Number(monthRaw)))).padStart(2, "0") : monthRaw;
  const day = dayRaw.length === 2 ? String(Math.min(31, Math.max(1, Number(dayRaw)))).padStart(2, "0") : dayRaw;
  if (digits.length <= 4) {
    return year;
  }
  if (digits.length <= 6) {
    return `${year}-${month}`;
  }
  return `${year}-${month}-${day}`;
}

function getZonedDateParts(epochMs: number, timeZone: string): Omit<ViewerTimestampParts, "millisecond"> {
  const values = new Map<string, string>();
  const formatter = timeZone === pacificTimeZone
    ? pacificDatePartsFormatter
    : new Intl.DateTimeFormat("en-CA", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
  formatter.formatToParts(new Date(epochMs)).forEach((part) => {
    if (part.type !== "literal") {
      values.set(part.type, part.value);
    }
  });
  return {
    year: Number(values.get("year") ?? "0"),
    month: Number(values.get("month") ?? "0"),
    day: Number(values.get("day") ?? "0"),
    hour: Number(values.get("hour") ?? "0"),
    minute: Number(values.get("minute") ?? "0"),
    second: Number(values.get("second") ?? "0"),
  };
}

function convertZonedPartsToEpoch(parts: ViewerTimestampParts, timeZone: string): number | null {
  let guess = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second, parts.millisecond);
  const desiredUtc = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second, 0);
  for (let attempt = 0; attempt < 6; attempt += 1) {
    const zoned = getZonedDateParts(guess, timeZone);
    const zonedUtc = Date.UTC(zoned.year, zoned.month - 1, zoned.day, zoned.hour, zoned.minute, zoned.second, 0);
    const delta = desiredUtc - zonedUtc;
    guess += delta;
    if (delta === 0) {
      return guess;
    }
  }
  const resolved = getZonedDateParts(guess, timeZone);
  if (
    resolved.year === parts.year &&
    resolved.month === parts.month &&
    resolved.day === parts.day &&
    resolved.hour === parts.hour &&
    resolved.minute === parts.minute &&
    resolved.second === parts.second
  ) {
    return guess;
  }
  return null;
}

function getEpochFromViewerParts(parts: ViewerTimestampParts, mode: LogTimeSourceMode | "utc" | "pacific"): number | null {
  if (mode === "utc") {
    return Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second, parts.millisecond);
  }
  if (mode === "pacific") {
    return convertZonedPartsToEpoch(parts, pacificTimeZone);
  }
  return new Date(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second, parts.millisecond).getTime();
}

function getTimeZoneLabel(epochMs: number, mode: Exclude<LogTimeDisplayMode, "source"> | "original"): string {
  if (mode === "utc") {
    return "UTC";
  }
  if (mode === "pacific") {
    const part = pacificZoneNameFormatter.formatToParts(new Date(epochMs)).find((item) => item.type === "timeZoneName")?.value ?? "PT";
    return part.replace("GMT-7", "PDT").replace("GMT-8", "PST");
  }
  return "Local";
}

function formatEpochForDisplay(epochMs: number, mode: Exclude<LogTimeDisplayMode, "source"> | "original"): string {
  const date = new Date(epochMs);
  let year: number;
  let month: number;
  let day: number;
  let hour: number;
  let minute: number;
  let second: number;
  if (mode === "utc") {
    year = date.getUTCFullYear();
    month = date.getUTCMonth() + 1;
    day = date.getUTCDate();
    hour = date.getUTCHours();
    minute = date.getUTCMinutes();
    second = date.getUTCSeconds();
  } else if (mode === "pacific") {
    const zoned = getZonedDateParts(epochMs, pacificTimeZone);
    year = zoned.year;
    month = zoned.month;
    day = zoned.day;
    hour = zoned.hour;
    minute = zoned.minute;
    second = zoned.second;
  } else {
    year = date.getFullYear();
    month = date.getMonth() + 1;
    day = date.getDate();
    hour = date.getHours();
    minute = date.getMinutes();
    second = date.getSeconds();
  }
  const millisecond = date.getMilliseconds();
  const zoneLabel = getTimeZoneLabel(epochMs, mode);
  return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")} ${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:${String(second).padStart(2, "0")}.${String(millisecond).padStart(3, "0")} ${zoneLabel}`;
}

function stripLeadingViewerTimestamp(raw: string): string {
  const direct = raw.replace(/^(?:(?:\d{2}-\d{2}-\d{4})|(?:\d{4}-\d{2}-\d{2})) \d{2}:\d{2}:\d{2}\.\d{3,4}\s*/, "").trim();
  if (direct !== raw.trim()) {
    return direct;
  }
  const embedded = /^([A-Z#]{3,5}\s*:)\s*\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}\.\d{3,4}:(.*)$/.exec(raw);
  if (embedded) {
    return `${embedded[1]} ${embedded[2].trim()}`.trim();
  }
  return raw.trim();
}

function isReferenceLibraryLine(line: ParsedLine): boolean {
  return (line.source ?? "").startsWith("reference:");
}

function isReferenceLibrarySession(lines: ParsedLine[]): boolean {
  return lines.length > 0 && lines.every((line) => isReferenceLibraryLine(line));
}

function getLogCategory(line: ParsedLine): LogCategory {
  const referenceSource = line.source ?? "";
  if (referenceSource === "reference:workflow") return "WORKFLOW";
  if (referenceSource === "reference:genisys") return "GENISYS";
  if (referenceSource === "reference:message-exchange") return "MESSAGE EXCHANGE";
  if (referenceSource === "reference:train-messages") return "TRAIN MESSAGES";
  if (referenceSource === "reference:codelines") return "CODELINES & STATIONS";
  if (referenceSource === "reference:stations") return "CODELINES & STATIONS";
  if (referenceSource === "reference:network") return "NETWORK";

  const source = `${line.source ?? ""} ${line.raw}`.toUpperCase();
  if (source.includes("SOCKETTRACE") || source.includes("CLSCLIENTC")) return "SOCKET";
  if (source.includes("CODESERVER") || source.includes("CODE SERVER")) return "CODE";
  if (source.includes("GENISYS") || source.includes("RECALL SENT") || source.includes("FOR CODESTATION")) return "CODELINE";
  if (/\bCAD\b/.test(source)) return "CAD";
  return "OTHER";
}

function sortLinesForViewer(lines: ParsedLine[]): ParsedLine[] {
  const categorySequence: LogCategory[] = isReferenceLibrarySession(lines)
    ? [...referenceCategoryOrder]
    : ["OTHER"];
  return [...lines].sort((a, b) => {
    const aTime = parseViewerTimestamp(a.timestamp);
    const bTime = parseViewerTimestamp(b.timestamp);
    if (aTime !== null && bTime !== null && aTime !== bTime) {
      return aTime - bTime;
    }
    if (aTime !== null && bTime === null) return -1;
    if (aTime === null && bTime !== null) return 1;

    const aCategory = getLogCategory(a);
    const bCategory = getLogCategory(b);
    const categoryDelta = (categorySequence.indexOf(aCategory) + 1 || 99) - (categorySequence.indexOf(bCategory) + 1 || 99);
    if (categoryDelta !== 0) {
      return categoryDelta;
    }

    const sourceDelta = getSourceLabel(a.source).localeCompare(getSourceLabel(b.source));
    if (sourceDelta !== 0) {
      return sourceDelta;
    }
    return a.lineNumber - b.lineNumber;
  });
}

function makeFallbackDetail(line: ParsedLine): DetailModel {
  const message = stripLeadingLogTimestamp(line.raw);
  const observedFields = extractObservedLineFields(message);
  const bitStates = extractObservedBitStates(message);
  const structured = [
    line.source ? `Source: ${line.source}` : "",
    line.timestamp ? `Timestamp: ${line.timestamp}` : "",
    `Line number: ${line.lineNumber}`,
    message && message !== line.raw ? `Message: ${message}` : "",
    ...observedFields.map((field) => `Observed field: ${field}`),
  ].filter(Boolean);
  const payloadContext = bitStates.length
    ? [
        `Observed bit states: ${bitStates.length}`,
        ...bitStates.map((bit) => `${bit.position} ${bit.name}=${bit.state}`),
      ]
    : [];

  return {
    lineId: line.id,
    lineNumber: line.lineNumber,
    timestamp: line.timestamp,
    raw: line.raw,
    translation: {
      original: line.raw,
      structured,
      english: [
        "Static detail generated in the browser from the selected raw line.",
        message ? `Message text: ${message}` : "",
      ].filter(Boolean),
      unresolved: [],
    },
    workflow: {
      summary: message || line.raw,
      currentStep: "",
      systems: [],
      objects: [],
      knownState: "",
      unresolved: [],
    },
    genisysContext: [],
    icdContext: [],
    databaseContext: [],
    workflowContext: [],
    payloadContext,
    sourceReferences: [],
  };
}

function extractObservedLineFields(message: string): string[] {
  const fields: string[] = [];
  const seen = new Set<string>();
  const patterns = [
    /\b([A-Z][A-Z0-9 _/-]{1,32}):\s*([^<>\r\n]{1,120})/g,
    /\b([A-Za-z][A-Za-z0-9_-]{1,32})=([^<>\s,;]+)/g,
  ];
  for (const pattern of patterns) {
    let match = pattern.exec(message);
    while (match) {
      const key = match[1].replace(/\s+/g, " ").trim();
      const value = match[2].replace(/\s+/g, " ").trim();
      const field = `${key}: ${value}`;
      const normalized = field.toUpperCase();
      if (key && value && !seen.has(normalized)) {
        seen.add(normalized);
        fields.push(field);
      }
      match = pattern.exec(message);
    }
  }
  return fields.slice(0, 40);
}

function extractObservedBitStates(message: string): Array<{ position: string; name: string; state: string }> {
  const out: Array<{ position: string; name: string; state: string }> = [];
  const pattern = /\((\d+)\)\s*([A-Z0-9_/-]+)=([A-Za-z0-9_/-]+)/gi;
  let match = pattern.exec(message);
  while (match) {
    out.push({
      position: match[1],
      name: match[2],
      state: match[3],
    });
    match = pattern.exec(message);
  }
  return out;
}

function describeSessionSelection(session: SessionData): { selected: ParsedLine | null; detail: DetailModel | null } {
  const selected = session.lines[0] ?? null;
  if (!selected) {
    return { selected: null, detail: null };
  }
  return {
    selected,
    detail: session.detail ?? session.lineDetails?.[selected.id] ?? makeFallbackDetail(selected),
  };
}

function buildDetailsText(detail: DetailModel): string {
  const lines: string[] = [];
  lines.push(...detail.translation.english.filter(Boolean));
  const payloadSummary = buildDetailsPayloadSummaryLines(detail);

  if (detail.translation.structured.length) {
    if (lines.length) {
      lines.push("", "Additional fields:", ...detail.translation.structured);
    } else {
      lines.push(...detail.translation.structured);
    }
  }

  if (payloadSummary.length) {
    if (lines.length) {
      lines.push("");
    }
    lines.push(...payloadSummary);
  }

  if (!lines.length && detail.workflow.summary) {
    lines.push(detail.workflow.summary);
  }

  return lines.filter((line, index, all) => line || all[index - 1] !== "").join("\n");
}

function getPayloadContextLines(detail: DetailModel): string[] {
  return detail.payloadContext?.map((line) => line.trim()).filter(Boolean) ?? [];
}

function isDecodedBitPayloadContext(detail: DetailModel): boolean {
  const lines = getPayloadContextLines(detail);
  if (!lines.some((line) => /^Payload bits:/i.test(line))) {
    return false;
  }

  return !lines.some((line) =>
    /^(SQL text|SQL continuation|Raw payload|Raw tracking payload|Raw route-search payload|Raw component payload|Payload preview):/i.test(line),
  );
}

function buildDetailsPayloadSummaryLines(detail: DetailModel): string[] {
  if (!isDecodedBitPayloadContext(detail)) {
    return [];
  }

  const lines = getPayloadContextLines(detail);
  const payloadBits = lines.find((line) => /^Payload bits:/i.test(line));
  const assertedPositions = lines.find((line) => /^Asserted (?:payload )?positions(?: before clear)?:/i.test(line));
  const summaryRows = [payloadBits, assertedPositions].filter((line): line is string => Boolean(line));

  return summaryRows.length ? ["Payload:", ...summaryRows] : [];
}

function shouldHidePayloadTab(detail: DetailModel): boolean {
  return isDecodedBitPayloadContext(detail) && buildDetailsPayloadSummaryLines(detail).length > 0;
}

function buildReferenceTabs(detail: DetailModel): Array<{ key: string; label: string; content: string }> {
  const workflowText = detail.workflowContext?.join("\n") ?? "";
  const referenceText = detail.databaseContext?.join("\n") ?? "";
  const payloadText = detail.payloadContext?.join("\n") ?? "";
  const descriptionText = buildDetailsText(detail);
  const mergedReferenceText = [referenceText, payloadText].filter((value) => value.trim().length > 0).join("\n\n");

  return [
    ...(workflowText.trim().length ? [{ key: "workflow", label: "Workflow", content: workflowText }] : []),
    ...(mergedReferenceText.trim().length ? [{ key: "reference", label: "Details", content: mergedReferenceText }] : []),
    ...(!workflowText.trim().length && !mergedReferenceText.trim().length && descriptionText.trim().length
      ? [{ key: "description", label: "Details", content: descriptionText }]
      : []),
  ];
}

function getReferenceSelectionKey(lineId: string, groupId: string): string {
  return `${lineId}:${groupId}`;
}

function getSelectedReferenceItemIds(
  lineId: string,
  group: ReferenceChoiceGroup,
  selections: Record<string, string[]>,
): string[] {
  const selectionKey = getReferenceSelectionKey(lineId, group.id);
  const storedSelectionIds = selections[selectionKey] ?? [];
  const selectedIds = storedSelectionIds.filter((selectedId) => group.items.some((item) => item.id === selectedId));
  if (group.selectionMode === "multiple") {
    return selectedIds;
  }
  if (selectedIds.length) {
    return [selectedIds[0]];
  }
  return group.items[0] ? [group.items[0].id] : [];
}

function getEffectiveSelectedItems(
  lineId: string,
  group: ReferenceChoiceGroup,
  selections: Record<string, string[]>,
): ReferenceChoiceItem[] {
  const selectedIds = getSelectedReferenceItemIds(lineId, group, selections);
  return group.items.filter((item) => selectedIds.includes(item.id));
}

function getNextReferenceSelectionIds(
  group: ReferenceChoiceGroup,
  currentIds: string[],
  itemId: string,
  additive: boolean,
): string[] {
  if (group.selectionMode !== "multiple") {
    return currentIds.length === 1 && currentIds[0] === itemId ? [] : [itemId];
  }
  if (!additive) {
    return currentIds.length === 1 && currentIds[0] === itemId ? [] : [itemId];
  }
  return currentIds.includes(itemId)
    ? currentIds.filter((selectedId) => selectedId !== itemId)
    : [...currentIds, itemId];
}

function clearNestedReferenceSelections(
  lineId: string,
  items: ReferenceChoiceItem[],
  selections: Record<string, string[]>,
): Record<string, string[]> {
  if (!items.length) {
    return selections;
  }
  const nextSelections = { ...selections };
  const removeItemGroups = (item: ReferenceChoiceItem) => {
    for (const group of item.detailChoiceGroups ?? []) {
      delete nextSelections[getReferenceSelectionKey(lineId, group.id)];
      group.items.forEach(removeItemGroups);
    }
  };
  items.forEach(removeItemGroups);
  return nextSelections;
}

function getVisibleDetailChoiceGroups(item: ReferenceChoiceItem, config: SearchConfig): ReferenceChoiceGroup[] {
  if (!item.detailChoiceGroups?.length || !config.query) {
    return item.detailChoiceGroups ?? [];
  }

  const filteredGroups = item.detailChoiceGroups
    .map((group) => {
      const filteredItems = group.items.filter((nestedItem) => matchesReferenceChoiceItem(nestedItem, config));
      if (!filteredItems.length) {
        return null;
      }
      return {
        ...group,
        items: filteredItems,
      };
    })
    .filter((group): group is NonNullable<typeof group> => Boolean(group));

  return filteredGroups.length ? filteredGroups : item.detailChoiceGroups;
}

function renderNestedReferenceChoiceSections(
  detail: DetailModel,
  item: ReferenceChoiceItem,
  search: SearchConfig,
  selections: Record<string, string[]>,
  setSelections: Dispatch<SetStateAction<Record<string, string[]>>>,
): ReactNode {
  const nestedGroups = getVisibleDetailChoiceGroups(item, search).filter((nestedGroup) => nestedGroup.items.length > 0);
  if (!nestedGroups.length) {
    return null;
  }
  return (
    <div className="reference-choice-panel nested-choice-panel">
      {nestedGroups.map((nestedGroup) => {
        const nestedSelectionKey = getReferenceSelectionKey(detail.lineId, nestedGroup.id);
        const selectedNestedIds = getSelectedReferenceItemIds(detail.lineId, nestedGroup, selections);
        return (
          <div key={nestedGroup.id} className="reference-choice-group">
            {nestedGroup.label.trim().length ? <div className="reference-choice-label">{nestedGroup.label}</div> : null}
            <div className={getReferenceChoiceListClass(nestedGroup.layout)}>
              {nestedGroup.items.map((nestedItem) => {
                const nestedIsActive = selectedNestedIds.includes(nestedItem.id);
                return (
                  <button
                    key={nestedItem.id}
                    type="button"
                    className={nestedIsActive ? "reference-choice active" : "reference-choice"}
                    onClick={(event) => {
                      setSelections((current) => {
                        const currentIds = getSelectedReferenceItemIds(detail.lineId, nestedGroup, current);
                        const nextIds = getNextReferenceSelectionIds(
                          nestedGroup,
                          currentIds,
                          nestedItem.id,
                          event.ctrlKey || event.metaKey || event.shiftKey,
                        );
                        return {
                          ...current,
                          [nestedSelectionKey]: nextIds,
                        };
                      });
                    }}
                  >
                    {renderHighlightedText(nestedItem.label, search)}
                  </button>
                );
              })}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function buildReferenceItemDisplayLines(
  detail: DetailModel,
  item: ReferenceChoiceItem,
  selections: Record<string, string[]>,
  searchConfig: SearchConfig,
): string[] {
  const lines = [...item.content];
  const nestedGroups = getVisibleDetailChoiceGroups(item, searchConfig).filter((group) => group.items.length > 0);
  for (const nestedGroup of nestedGroups) {
    const selectedNestedItems = getEffectiveSelectedItems(detail.lineId, nestedGroup, selections);
    if (!selectedNestedItems.length) {
      continue;
    }
    if (lines.length && lines[lines.length - 1] !== "") {
      lines.push("");
    }
    const includeLabel = nestedGroup.selectionMode === "multiple" || selectedNestedItems.length > 1;
    selectedNestedItems.forEach((nestedItem, index) => {
      if (includeLabel) {
        lines.push(nestedItem.label);
      }
      lines.push(...nestedItem.content);
      if (index < selectedNestedItems.length - 1) {
        lines.push("");
      }
    });
  }
  return lines;
}

function buildReferenceTabDisplayText(
  detail: DetailModel | null,
  activeTab: string,
  baseContent: string,
  selections: Record<string, string[]>,
  searchConfig: SearchConfig,
): string {
  if (!detail || activeTab !== "reference" || !detail.referenceChoiceGroups?.length) {
    return baseContent;
  }

  const selectedSections = getVisibleReferenceChoiceGroups(detail, searchConfig)
    .filter((group) => group.items.length > 0)
    .flatMap((group) => {
      const selectedItems = getEffectiveSelectedItems(detail.lineId, group, selections);
      if (!selectedItems.length) {
        return [];
      }
      const summaryMode = detail.databaseContext?.[0]?.trim();
      const includeItemLabel = group.selectionMode === "multiple" || selectedItems.length > 1;
      if (
        summaryMode === "Packet switch reference:" ||
        summaryMode === "Code line / station reference:" ||
        summaryMode === "Train radio / modem reference:" ||
        summaryMode === "Device / IP reference:"
      ) {
        return selectedItems.flatMap((item, index) => [
          ...(includeItemLabel ? [item.label] : []),
          ...buildReferenceItemDisplayLines(detail, item, selections, searchConfig),
          ...(index < selectedItems.length - 1 ? [""] : []),
        ]);
      }
      return selectedItems.flatMap((item, index) => [
        `${group.label}: ${item.label}`,
        ...buildReferenceItemDisplayLines(detail, item, selections, searchConfig),
        ...(index < selectedItems.length - 1 ? [""] : []),
      ]);
    });

  if (!selectedSections.length) {
    return baseContent;
  }

  const summaryMode = detail.databaseContext?.[0]?.trim();
  if (
    summaryMode === "Packet switch reference:" ||
    summaryMode === "Code line / station reference:" ||
    summaryMode === "Train radio / modem reference:" ||
    summaryMode === "Device / IP reference:"
  ) {
    return selectedSections.join("\n\n");
  }

  return [baseContent, selectedSections.join("\n\n")].filter((value) => value.trim().length > 0).join("\n\n");
}

function getVisibleReferenceChoiceGroups(detail: DetailModel, config: SearchConfig) {
  if (!detail.referenceChoiceGroups?.length || !config.query) {
    return detail.referenceChoiceGroups ?? [];
  }

  const filteredGroups = detail.referenceChoiceGroups
    .map((group) => {
      const filteredItems = group.items.filter((item) => matchesReferenceChoiceItem(item, config));
      if (!filteredItems.length) {
        return null;
      }
      return {
        ...group,
        items: filteredItems,
      };
    })
    .filter((group): group is NonNullable<typeof group> => Boolean(group));

  return filteredGroups.length ? filteredGroups : detail.referenceChoiceGroups;
}

function getReferenceChoiceListClass(layout?: "wrap" | "horizontal" | "column"): string {
  if (layout === "horizontal") {
    return "reference-choice-list horizontal";
  }
  if (layout === "column") {
    return "reference-choice-list column";
  }
  return "reference-choice-list";
}

function normalizeReferenceValue(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

type NetworkDetailRow = {
  label: string;
  value: string;
};

type NetworkDetailCard = {
  title: string;
  rows: NetworkDetailRow[];
};

type StructuredReferenceCard = {
  title?: string;
  rows: NetworkDetailRow[];
  paragraphs: string[];
};

type ActiveStateDisplayItem = {
  raw: string;
  position?: string;
  mnemonic?: string;
  state?: string;
  description?: string;
};

type ActiveStateDisplayGroup = {
  label: string;
  items: ActiveStateDisplayItem[];
};

const activeStateGroupLabels = new Set([
  "signals",
  "switches",
  "tracks",
  "routes",
  "local device",
  "other",
  "blank unassigned",
]);

function simplifyAssignmentDisplayValue(label: string, value: string): string {
  const trimmedValue = value.trim();
  if (!trimmedValue) {
    return trimmedValue;
  }
  const mnemonicMatch = /^\d+\.\s+([A-Z0-9]+)$/.exec(label.trim());
  const mnemonic = mnemonicMatch?.[1]?.trim().toUpperCase() ?? "";
  if (!mnemonic || mnemonic === "BLANK") {
    return trimmedValue;
  }
  const prefixPattern = new RegExp(`^${escapeRegExp(mnemonic)}\\s+`, "i");
  return trimmedValue.replace(prefixPattern, "").trim() || trimmedValue;
}

function parseReferenceRows(lines: string[]): NetworkDetailRow[] {
  return lines
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => {
      const match = /^([^:]+):\s*(.+)$/.exec(line);
      if (!match) {
        return { label: "", value: line };
      }
      return {
        label: match[1].trim(),
        value: match[2].trim(),
      };
    });
}

function shouldOmitNetworkRow(cardTitle: string, row: NetworkDetailRow, contextTitle?: string): boolean {
  const normalizedTitle = normalizeReferenceValue(cardTitle);
  const normalizedValue = normalizeReferenceValue(row.value);
  const normalizedContext = normalizeReferenceValue(contextTitle ?? "");
  const normalizedLabel = normalizeReferenceValue(row.label);
  if (!normalizedValue) {
    return true;
  }
  if ((normalizedLabel === "model" || normalizedLabel === "type") && normalizedTitle) {
    if (normalizedValue === normalizedTitle || normalizedValue.includes(normalizedTitle) || normalizedTitle.includes(normalizedValue)) {
      return true;
    }
  }
  if (normalizedLabel === "type" && normalizedContext && normalizedValue === normalizedContext) {
    return true;
  }
  if (normalizedLabel === "location" && normalizedTitle) {
    if (normalizedValue === normalizedTitle || normalizedTitle.includes(normalizedValue) || normalizedValue.includes(normalizedTitle)) {
      return true;
    }
  }
  return false;
}

function buildNetworkDetailCardsFromTrainItem(item: ReferenceChoiceItem): NetworkDetailCard[] {
  const cards: NetworkDetailCard[] = [];
  let currentTitle = "";
  let currentLines: string[] = [];
  const flush = () => {
    if (!currentTitle) {
      currentLines = [];
      return;
    }
    const rows = parseReferenceRows(currentLines).filter((row) => !shouldOmitNetworkRow(currentTitle, row));
    cards.push({
      title: currentTitle,
      rows,
    });
    currentTitle = "";
    currentLines = [];
  };
  for (const line of item.content) {
    const trimmed = line.trim();
    if (!trimmed) {
      flush();
      continue;
    }
    if (!trimmed.includes(":") && !currentTitle) {
      currentTitle = trimmed;
      continue;
    }
    currentLines.push(trimmed);
  }
  flush();
  return cards;
}

function buildNetworkDetailCard(title: string, lines: string[], contextTitle?: string): NetworkDetailCard {
  return {
    title,
    rows: parseReferenceRows(lines).filter((row) => !shouldOmitNetworkRow(title, row, contextTitle)),
  };
}

function renderNetworkDetailCard(card: NetworkDetailCard, search: SearchConfig, key: string, hideTitle = false): ReactNode {
  return (
    <article key={key} className="network-detail-card">
      {!hideTitle ? <div className="network-detail-card-title">{renderHighlightedText(card.title, search)}</div> : null}
      <dl className="network-detail-grid">
        {card.rows.map((row, index) => (
          <Fragment key={`${card.title}:${row.label}:${row.value}:${index}`}>
            {row.label ? <dt>{renderHighlightedText(row.label, search)}</dt> : null}
            <dd className={row.label ? "" : "full"}>{renderHighlightedText(row.value, search)}</dd>
          </Fragment>
        ))}
      </dl>
    </article>
  );
}

function renderNetworkReferenceDetail(
  detail: DetailModel,
  search: SearchConfig,
  selections: Record<string, string[]>,
): ReactNode | null {
  const summaryMode = detail.databaseContext?.[0]?.trim();
  if (
    summaryMode !== "Packet switch reference:"
    && summaryMode !== "Train radio / modem reference:"
    && summaryMode !== "Device / IP reference:"
  ) {
    return null;
  }

  const visibleGroups = getVisibleReferenceChoiceGroups(detail, search).filter((group) => group.items.length > 0);
  const selectedSections = visibleGroups.flatMap((group) =>
    getEffectiveSelectedItems(detail.lineId, group, selections).map((item) => ({ group, item })),
  );

  if (!selectedSections.length) {
    return <div className="card empty">Select items on the left.</div>;
  }

  return (
    <div className="network-detail-groups">
      {selectedSections.map(({ item }) => {
        let cards: NetworkDetailCard[] = [];
        if (summaryMode === "Train radio / modem reference:") {
          cards = buildNetworkDetailCardsFromTrainItem(item);
        } else if (summaryMode === "Packet switch reference:") {
          cards = [buildNetworkDetailCard(item.label, item.content)];
        } else {
          const nestedItems = getVisibleDetailChoiceGroups(item, search)
            .flatMap((group) => getEffectiveSelectedItems(detail.lineId, group, selections));
          if (!nestedItems.length) {
            return null;
          }
          cards = nestedItems.map((nestedItem) => buildNetworkDetailCard(nestedItem.label, nestedItem.content, item.label));
        }

        if (!cards.length) {
          return null;
        }

        const hideRepeatedCardTitle = selectedSections.length === 1
          && cards.length === 1
          && normalizeReferenceValue(cards[0]?.title ?? "") === normalizeReferenceValue(item.label);

        return (
          <section key={item.id} className="network-detail-group">
            {selectedSections.length > 1 ? (
              <div className="network-detail-group-heading">{renderHighlightedText(item.label, search)}</div>
            ) : null}
            <div className="network-detail-card-grid">
              {cards.map((card, index) =>
                renderNetworkDetailCard(
                  card,
                  search,
                  `${item.id}:${card.title}:${index}`,
                  hideRepeatedCardTitle && index === 0,
                ),
              )}
            </div>
          </section>
        );
      })}
    </div>
  );
}

function buildGenericReferenceCards(lines: string[]): StructuredReferenceCard[] {
  const blocks: string[][] = [];
  let current: string[] = [];
  for (const rawLine of lines) {
    const trimmed = rawLine.trim();
    if (!trimmed) {
      if (current.length) {
        blocks.push(current);
        current = [];
      }
      continue;
    }
    current.push(trimmed);
  }
  if (current.length) {
    blocks.push(current);
  }

  return blocks
    .map((block) => {
      let title = "";
      let working = [...block];
      if (/^\d+\.\s+/.test(working[0])) {
        title = working[0].replace(/^\d+\.\s+/, "").trim();
        working = working.slice(1);
      } else if (/^[^:]+:$/.test(working[0])) {
        title = working[0].slice(0, -1).trim();
        working = working.slice(1);
      } else if (!working[0].includes(":") && working.length > 1) {
        title = working[0];
        working = working.slice(1);
      }

      const rows: NetworkDetailRow[] = [];
      const paragraphs: string[] = [];
      for (const line of working) {
        const match = /^([^:]+):\s*(.+)$/.exec(line);
        if (match) {
          rows.push({
            label: match[1].trim(),
            value: match[2].trim(),
          });
          continue;
        }
        paragraphs.push(line);
      }
      if (!title && !rows.length && paragraphs.length === 1) {
        title = paragraphs[0];
        paragraphs.length = 0;
      }
      return {
        title,
        rows,
        paragraphs,
      };
    })
    .filter((card) => Boolean(card.title) || card.rows.length > 0 || card.paragraphs.length > 0);
}

function buildAssignmentReferenceCards(lines: string[]): StructuredReferenceCard[] {
  const cards: StructuredReferenceCard[] = [];
  const summaryCard: StructuredReferenceCard = {
    title: "",
    rows: [],
    paragraphs: [],
  };
  let currentCard: StructuredReferenceCard | null = null;

  const pushSummaryIfNeeded = () => {
    if (summaryCard.rows.length || summaryCard.paragraphs.length) {
      cards.push({
        title: summaryCard.title,
        rows: [...summaryCard.rows],
        paragraphs: [...summaryCard.paragraphs],
      });
      summaryCard.rows = [];
      summaryCard.paragraphs = [];
    }
  };

  for (const rawLine of lines) {
    const trimmed = rawLine.trim();
    if (!trimmed) {
      continue;
    }
    const sectionMatch = /^(Indication bits|Control bits):$/i.exec(trimmed);
    if (sectionMatch) {
      pushSummaryIfNeeded();
      currentCard = {
        title: sectionMatch[1],
        rows: [],
        paragraphs: [],
      };
      cards.push(currentCard);
      continue;
    }

    const assignmentMapSectionMatch = /^(Station \/ asset map):$/i.exec(trimmed);
    if (assignmentMapSectionMatch) {
      pushSummaryIfNeeded();
      currentCard = {
        title: assignmentMapSectionMatch[1],
        rows: [],
        paragraphs: [],
      };
      cards.push(currentCard);
      continue;
    }

    const assignmentMatch = /^(\d+\.\s+[^=]+)=\s*(.+)$/.exec(trimmed);
    if (currentCard && assignmentMatch) {
      currentCard.rows.push({
        label: assignmentMatch[1].trim(),
        value: simplifyAssignmentDisplayValue(assignmentMatch[1].trim(), assignmentMatch[2].trim()),
      });
      continue;
    }

    const countMatch = /^(\d+)\s+bits total,\s*(\d+)\s+assigned$/i.exec(trimmed);
    if (currentCard && countMatch) {
      currentCard.rows.push(
        { label: "Bits total", value: countMatch[1] },
        { label: "Assigned", value: countMatch[2] },
      );
      continue;
    }

    const keyValueMatch = /^([^:]+):\s*(.+)$/.exec(trimmed);
    if (keyValueMatch) {
      const target = currentCard ?? summaryCard;
      target.rows.push({
        label: keyValueMatch[1].trim(),
        value: keyValueMatch[2].trim(),
      });
      continue;
    }

    const target = currentCard ?? summaryCard;
    target.paragraphs.push(trimmed);
  }

  pushSummaryIfNeeded();
  return cards.filter((card) => card.title || card.rows.length > 0 || card.paragraphs.length > 0);
}

function buildWorkflowReferenceCards(detail: DetailModel, lines: string[]): StructuredReferenceCard[] {
  const cards: StructuredReferenceCard[] = [];
  let inSteps = false;
  let currentCard: StructuredReferenceCard | null = null;

  for (const rawLine of lines) {
    const trimmed = rawLine.trim();
    if (!trimmed) {
      continue;
    }
    if (/^Overview:/i.test(trimmed) || /^Grounded steps:/i.test(trimmed) || /^Flow steps:$/i.test(trimmed)) {
      if (/^Flow steps:$/i.test(trimmed)) {
        inSteps = true;
      }
      continue;
    }
    if (!inSteps) {
      cards.push({
        title: "",
        rows: [],
        paragraphs: [trimmed],
      });
      continue;
    }
    const stepMatch = /^\d+\.\s+(.+)$/.exec(trimmed);
    if (stepMatch) {
      const descriptor = stepMatch[1].trim();
      const descriptorMatch = /^(.+?)\s*=\s*(.+)$/.exec(descriptor);
      currentCard = {
        title: descriptorMatch ? descriptorMatch[1].trim() : descriptor,
        rows: [],
        paragraphs: descriptorMatch ? [descriptorMatch[2].trim()] : [],
      };
      cards.push(currentCard);
      continue;
    }
    const exampleMatch = /^Example\s+(\d+):\s*(.+)$/i.exec(trimmed);
    if (exampleMatch && currentCard) {
      currentCard.rows.push({
        label: `Example ${exampleMatch[1]}`,
        value: exampleMatch[2].trim(),
      });
      continue;
    }
    const rowMatch = /^([^:]+):\s*(.+)$/.exec(trimmed);
    if (currentCard && rowMatch) {
      currentCard.rows.push({
        label: rowMatch[1].trim(),
        value: rowMatch[2].trim(),
      });
      continue;
    }
    if (currentCard) {
      currentCard.paragraphs.push(trimmed);
      continue;
    }
    cards.push({
      title: "",
      rows: [],
      paragraphs: [trimmed],
    });
  }

  return cards.filter((card) => card.title || card.rows.length > 0 || card.paragraphs.length > 0);
}

function buildStructuredReferenceCards(detail: DetailModel, content: string): StructuredReferenceCard[] {
  const lines = content.split(/\r?\n/);
  if (detail.workflow.currentStep === "Reference section" && lines.some((line) => line.trim() === "Flow steps:")) {
    return buildWorkflowReferenceCards(detail, lines);
  }
  if (lines.some((line) => /^Indication bits:$/i.test(line.trim())) || lines.some((line) => /^Control bits:$/i.test(line.trim()))) {
    return buildAssignmentReferenceCards(lines);
  }
  return buildGenericReferenceCards(lines);
}

function buildRuntimeStructuredCards(activeTab: string, content: string): StructuredReferenceCard[] {
  const lines = content.split(/\r?\n/);
  if (activeTab === "tmds" && (lines.some((line) => /^Indication bits:$/i.test(line.trim())) || lines.some((line) => /^Control bits:$/i.test(line.trim())))) {
    return buildAssignmentReferenceCards(lines);
  }

  const cards: StructuredReferenceCard[] = [];
  let currentCard: StructuredReferenceCard | null = null;

  const ensureCard = (): StructuredReferenceCard => {
    if (currentCard) {
      return currentCard;
    }
    currentCard = {
      title: "",
      rows: [],
      paragraphs: [],
    };
    cards.push(currentCard);
    return currentCard;
  };

  for (const rawLine of lines) {
    const trimmed = rawLine.trim();
    if (!trimmed) {
      currentCard = null;
      continue;
    }

    const sectionMatch = /^([^:]+):$/.exec(trimmed);
    if (sectionMatch) {
      currentCard = {
        title: sectionMatch[1].trim(),
        rows: [],
        paragraphs: [],
      };
      cards.push(currentCard);
      continue;
    }

    const booleanWithLongNameMatch = /^(\d+\.\s+[A-Z0-9]+)\s*=\s*(TRUE|FALSE|UNKNOWN)\s*\((.+)\)$/i.exec(trimmed);
    if (booleanWithLongNameMatch) {
      ensureCard().rows.push({
        label: `${booleanWithLongNameMatch[1].trim()} = ${booleanWithLongNameMatch[2].trim()}`,
        value: simplifyAssignmentDisplayValue(booleanWithLongNameMatch[1].trim(), booleanWithLongNameMatch[3].trim()),
      });
      continue;
    }

    const booleanOnlyMatch = /^(\d+\.\s+[A-Z0-9]+)\s*=\s*(TRUE|FALSE|UNKNOWN)$/i.exec(trimmed);
    if (booleanOnlyMatch) {
      ensureCard().rows.push({
        label: `${booleanOnlyMatch[1].trim()} = ${booleanOnlyMatch[2].trim()}`,
        value: "",
      });
      continue;
    }

    if (activeTab === "payload") {
      const orderedHyphenRowMatch = /^(\d+(?:\/\d+)?\.\s+.+?)\s+-\s+(.+)$/.exec(trimmed);
      if (orderedHyphenRowMatch) {
        ensureCard().rows.push({
          label: orderedHyphenRowMatch[1].trim(),
          value: orderedHyphenRowMatch[2].trim(),
        });
        continue;
      }
    }

    const activeLineStatusMatch = /^Active at this line:\s*(.+)$/i.exec(trimmed);
    if (activeLineStatusMatch) {
      currentCard = {
        title: "Active at this line",
        rows: [{ label: "Status", value: activeLineStatusMatch[1].trim() }],
        paragraphs: [],
      };
      cards.push(currentCard);
      continue;
    }

    const activePayloadStatusMatch = /^Active payload positions at this line:\s*(.+)$/i.exec(trimmed);
    if (activePayloadStatusMatch) {
      currentCard = {
        title: "Active at this line",
        rows: [{ label: "Payload positions", value: activePayloadStatusMatch[1].replace(/\.$/, "").trim() }],
        paragraphs: [],
      };
      cards.push(currentCard);
      continue;
    }

    const activeGroupMatch = /^([^:]+):\s*(\d+(?:\/\d+)?\.\s+.+)$/i.exec(trimmed);
    if (activeGroupMatch && isActiveStateGroupLabel(activeGroupMatch[1].trim())) {
      ensureCard().rows.push({
        label: activeGroupMatch[1].trim(),
        value: activeGroupMatch[2].trim(),
      });
      continue;
    }

    const equalsRowMatch = /^([^=]+?)\s+=\s+(.+)$/.exec(trimmed);
    if (equalsRowMatch) {
      ensureCard().rows.push({
        label: equalsRowMatch[1].trim(),
        value: equalsRowMatch[2].trim(),
      });
      continue;
    }

    const keyValueMatch = /^([^:]+):\s*(.+)$/.exec(trimmed);
    if (keyValueMatch) {
      ensureCard().rows.push({
        label: keyValueMatch[1].trim(),
        value: keyValueMatch[2].trim(),
      });
      continue;
    }

    ensureCard().paragraphs.push(trimmed);
  }

  return cards.filter((card) => card.title || card.rows.length > 0 || card.paragraphs.length > 0);
}

function isActiveStateCard(card: StructuredReferenceCard): boolean {
  return normalizeReferenceValue(card.title ?? "") === "active at this line";
}

function isActiveStateGroupLabel(label: string): boolean {
  return activeStateGroupLabels.has(normalizeReferenceValue(label));
}

function rowLooksLikeActiveStateItems(row: NetworkDetailRow): boolean {
  return /\d+(?:\/\d+)?\.\s+[^=]+?\s*=\s*(?:TRUE|FALSE|UNKNOWN)(?:\s*\(|\s*;|$)/i.test(row.value);
}

function isActiveStateGroupRow(row: NetworkDetailRow): boolean {
  return row.value.trim().length > 0 && isActiveStateGroupLabel(row.label) && rowLooksLikeActiveStateItems(row);
}

function parseActiveStateItem(raw: string): ActiveStateDisplayItem {
  const trimmed = raw.trim();
  const match = /^(\d+(?:\/\d+)?)\.\s+([^=]+?)\s*=\s*(TRUE|FALSE|UNKNOWN)(?:\s*\((.+)\))?$/i.exec(trimmed);
  if (!match) {
    return { raw: trimmed, description: trimmed };
  }
  return {
    raw: trimmed,
    position: match[1].trim(),
    mnemonic: match[2].trim(),
    state: match[3].trim().toUpperCase(),
    description: match[4]?.trim(),
  };
}

function buildActiveStateDisplayGroups(card: StructuredReferenceCard): ActiveStateDisplayGroup[] {
  return card.rows
    .filter(isActiveStateGroupRow)
    .map((row) => ({
      label: row.label,
      items: row.value
        .split(";")
        .map((item) => item.trim())
        .filter(Boolean)
        .map(parseActiveStateItem),
    }))
    .filter((group) => group.items.length > 0);
}

function getActiveStateMode(detail: DetailModel): "request" | "report" | "assert" {
  const text = [
    detail.workflow.summary,
    detail.workflow.currentStep,
    detail.workflow.knownState,
    ...detail.translation.english,
  ].join(" ").toLowerCase();
  if (text.includes("command")) {
    return "request";
  }
  if (text.includes("indication")) {
    return "report";
  }
  return "assert";
}

function getActiveStateIntent(detail: DetailModel, groups: ActiveStateDisplayGroup[]): string {
  const mode = getActiveStateMode(detail);
  if (!groups.length) {
    return "No decoded active assignments were found for this selected line.";
  }
  if (mode === "request") {
    return "What this command is asking for:";
  }
  if (mode === "report") {
    return "What this indication is reporting active:";
  }
  return "Decoded meaning of TRUE payload bits:";
}

function getActiveStateGroupTitle(label: string, detail: DetailModel): string {
  const normalized = normalizeReferenceValue(label);
  const mode = getActiveStateMode(detail);
  const suffix = mode === "request" ? "requests" : mode === "report" ? "reports" : "active";
  if (normalized === "switches") return `Switch ${suffix}`;
  if (normalized === "signals") return `Signal ${suffix}`;
  if (normalized === "tracks") return `Track ${suffix}`;
  if (normalized === "routes") return `Route ${suffix}`;
  if (normalized === "local device") return `Local / device ${suffix}`;
  if (normalized === "blank unassigned") return `Blank / unassigned ${suffix}`;
  return `${label} ${suffix}`;
}

function renderActiveStateCard(
  detail: DetailModel,
  activeTab: string,
  card: StructuredReferenceCard,
  index: number,
  search: SearchConfig,
): ReactNode {
  const groups = buildActiveStateDisplayGroups(card);
  const contextRows = card.rows.filter((row) => !isActiveStateGroupRow(row));
  const intent = getActiveStateIntent(detail, groups);

  return (
    <article
      key={`${detail.lineId}:${activeTab}:active-state:${index}`}
      className="reference-structured-card runtime-structured-card active-state-card"
    >
      <div className="active-state-head">
        <div className="reference-structured-title">{renderHighlightedText(card.title ?? "Active at this line", search)}</div>
        <div className="active-state-intent">{renderHighlightedText(intent, search)}</div>
      </div>
      {contextRows.length ? (
        <dl className="reference-structured-grid runtime-structured-grid active-state-context">
          {contextRows.map((row, rowIndex) => (
            <Fragment key={`${detail.lineId}:${activeTab}:active-state-context:${row.label}:${row.value}:${rowIndex}`}>
              {row.label ? <dt>{renderHighlightedText(row.label, search)}</dt> : null}
              <dd className={row.label ? "" : "full"}>{renderHighlightedText(row.value, search)}</dd>
            </Fragment>
          ))}
        </dl>
      ) : null}
      {groups.length ? (
        <div className="active-state-groups">
          {groups.map((group) => (
            <section key={`${detail.lineId}:${activeTab}:active-state:${group.label}`} className="active-state-group">
              <div className="active-state-group-title">
                <span>{renderHighlightedText(getActiveStateGroupTitle(group.label, detail), search)}</span>
                <span className="active-state-count">{group.items.length}</span>
              </div>
              <div className="active-state-list">
                {group.items.map((item, itemIndex) => (
                  <div
                    key={`${detail.lineId}:${activeTab}:active-state:${group.label}:${item.raw}:${itemIndex}`}
                    className={item.position && item.mnemonic && item.state ? "active-state-item" : "active-state-item unparsed"}
                  >
                    {item.position ? <span className="active-state-position">{renderHighlightedText(`Bit ${item.position}`, search)}</span> : null}
                    {item.mnemonic ? <span className="active-state-mnemonic">{renderHighlightedText(item.mnemonic, search)}</span> : null}
                    {item.state ? <span className={`active-state-state ${item.state.toLowerCase()}`}>{renderHighlightedText(item.state, search)}</span> : null}
                    <span className="active-state-description">{renderHighlightedText(item.description ?? item.raw, search)}</span>
                  </div>
                ))}
              </div>
            </section>
          ))}
        </div>
      ) : null}
      {card.paragraphs.length ? (
        <div className="reference-structured-body runtime-structured-body">
          {card.paragraphs.map((paragraph, paragraphIndex) => (
            <p key={`${detail.lineId}:${activeTab}:active-state-note:${paragraphIndex}`}>{renderHighlightedText(paragraph, search)}</p>
          ))}
        </div>
      ) : null}
    </article>
  );
}

function renderStructuredReferenceCards(detail: DetailModel, content: string, search: SearchConfig): ReactNode {
  const cards = buildStructuredReferenceCards(detail, content);
  if (!cards.length) {
    return <pre>{renderHighlightedText(content, search)}</pre>;
  }

  return (
    <div className="reference-structured-cards">
      {cards.map((card, index) => (
        <article
          key={`${detail.lineId}:${card.title ?? "card"}:${index}`}
          className="reference-structured-card"
        >
          {card.title ? <div className="reference-structured-title">{renderHighlightedText(card.title, search)}</div> : null}
          {card.rows.length ? (
            <dl className="reference-structured-grid">
              {card.rows.map((row, rowIndex) => (
                <Fragment key={`${detail.lineId}:${card.title}:${row.label}:${row.value}:${rowIndex}`}>
                  {row.label ? <dt>{renderHighlightedText(row.label, search)}</dt> : null}
                  <dd className={row.label ? "" : "full"}>{renderHighlightedText(row.value, search)}</dd>
                </Fragment>
              ))}
            </dl>
          ) : null}
          {card.paragraphs.length ? (
            <div className="reference-structured-body">
              {card.paragraphs.map((paragraph, paragraphIndex) => (
                <p key={`${detail.lineId}:${card.title}:${paragraphIndex}`}>{renderHighlightedText(paragraph, search)}</p>
              ))}
            </div>
          ) : null}
        </article>
      ))}
    </div>
  );
}

function renderRuntimeStructuredCards(detail: DetailModel, activeTab: string, content: string, search: SearchConfig): ReactNode {
  const cards = buildRuntimeStructuredCards(activeTab, content);
  if (!cards.length) {
    return <pre>{renderHighlightedText(content, search)}</pre>;
  }

  return (
    <div className="reference-structured-cards runtime-structured-cards">
      {cards.map((card, index) => {
        if (isActiveStateCard(card)) {
          return renderActiveStateCard(detail, activeTab, card, index, search);
        }
        return (
          <article
            key={`${detail.lineId}:${activeTab}:${card.title ?? "card"}:${index}`}
            className="reference-structured-card runtime-structured-card"
          >
            {card.title ? <div className="reference-structured-title">{renderHighlightedText(card.title, search)}</div> : null}
            {card.rows.length ? (
              <dl className="reference-structured-grid runtime-structured-grid">
                {card.rows.map((row, rowIndex) => (
                  <Fragment key={`${detail.lineId}:${activeTab}:${card.title}:${row.label}:${row.value}:${rowIndex}`}>
                    {row.label ? <dt>{renderHighlightedText(row.label, search)}</dt> : null}
                    <dd className={row.label ? "" : "full"}>{renderHighlightedText(row.value, search)}</dd>
                  </Fragment>
                ))}
              </dl>
            ) : null}
            {card.paragraphs.length ? (
              <div className="reference-structured-body runtime-structured-body">
                {card.paragraphs.map((paragraph, paragraphIndex) => (
                  <p key={`${detail.lineId}:${activeTab}:${card.title}:${paragraphIndex}`}>{renderHighlightedText(paragraph, search)}</p>
                ))}
              </div>
            ) : null}
          </article>
        );
      })}
    </div>
  );
}

function buildGenisysInlineCardContent(detail: DetailModel): string {
  const sections = [
    detail.databaseContext?.join("\n") ?? "",
    detail.payloadContext?.join("\n") ?? "",
  ].filter((section) => section.trim().length > 0);

  if (sections.length) {
    return sections.join("\n\n");
  }

  return buildDetailsText(detail);
}

function renderGenisysInlineCards(
  lines: ParsedLine[],
  lineDetails: Record<string, DetailModel>,
  search: SearchConfig,
): ReactNode {
  if (!lines.length) {
    return <div className="card empty">No entries match the current search.</div>;
  }

  return (
    <div className="genisys-inline-scroller">
      <div className="genisys-inline-grid" role="list">
        {lines.map((line) => {
          const detail = lineDetails[line.id] ?? makeFallbackDetail(line);
          const bubble = getReferenceBubbleParts(line, detail);
          const content = buildGenisysInlineCardContent(detail);
          return (
            <article key={line.id} className="genisys-inline-card" role="listitem">
              <div className="genisys-inline-head">
                <div className="genisys-inline-code">{renderHighlightedText(bubble.primary, search)}</div>
                <div className="genisys-inline-meta">
                  {detail.referenceBadges?.length ? (
                    <div className="reference-badge-strip">
                      {detail.referenceBadges.map((badge) => (
                        <span key={`${line.id}:${badge}`} className="reference-badge">{badge}</span>
                      ))}
                    </div>
                  ) : null}
                  {bubble.secondary ? (
                    <div className="genisys-inline-subtitle">{renderHighlightedText(bubble.secondary, search)}</div>
                  ) : null}
                </div>
              </div>
              {renderStructuredReferenceCards(detail, content, search)}
            </article>
          );
        })}
      </div>
    </div>
  );
}

function toFileUrl(path: string): string {
  if (!path) {
    return "";
  }
  if (/^[a-z]+:\/\//i.test(path)) {
    return path;
  }
  const normalized = path.replace(/\\/g, "/");
  const withLeadingSlash = /^[a-z]:/i.test(normalized) ? `/${normalized}` : normalized;
  return encodeURI(`file://${withLeadingSlash}`);
}

function renderReferenceArtifact(artifact: ReferenceArtifact, search: SearchConfig): ReactNode {
  const fileUrl = toFileUrl(artifact.path);
  return (
    <section className="reference-artifact-shell">
      <div className="reference-artifact-head">
        <div className="reference-artifact-copy">
          <div className="reference-artifact-title">{renderHighlightedText(artifact.title, search)}</div>
          {artifact.subtitle ? <div className="reference-artifact-subtitle">{renderHighlightedText(artifact.subtitle, search)}</div> : null}
        </div>
        <span className="reference-badge">Embedded {artifact.kind.toUpperCase()}</span>
      </div>
      <div className="reference-artifact-frame-wrap">
        <iframe
          className="reference-artifact-frame"
          title={artifact.title}
          src={fileUrl}
        />
      </div>
      <div className="reference-artifact-path">{renderHighlightedText(artifact.path, search)}</div>
    </section>
  );
}

function renderReferenceDiagram(diagram: ReferenceDiagram, search: SearchConfig): ReactNode {
  const sections = diagram.steps.reduce<Array<{ title: string; steps: ReferenceDiagram["steps"] }>>((output, step) => {
    const existing = output[output.length - 1];
    if (existing && existing.title === step.section) {
      existing.steps.push(step);
      return output;
    }
    output.push({
      title: step.section,
      steps: [step],
    });
    return output;
  }, []);

  return (
    <section className="reference-bounce-shell">
      <div className="reference-bounce-head">
        <div className="reference-bounce-title">{renderHighlightedText(diagram.title, search)}</div>
        {diagram.subtitle ? <div className="reference-bounce-subtitle">{renderHighlightedText(diagram.subtitle, search)}</div> : null}
      </div>
      <div className="reference-bounce-sections">
        {sections.map((section) => (
          <section key={section.title} className="reference-bounce-section">
            <div className="reference-bounce-section-title">{renderHighlightedText(section.title, search)}</div>
            <div className="reference-bounce-rows">
              {section.steps.map((step) => {
                const involvesWsrs = step.fromLane === "WSRS" || step.toLane === "WSRS";
                const leftLane = "Locomotive";
                const rightLane = involvesWsrs ? "WSRS" : "Office";
                const leftLabel = "Loco";
                const rightLabel = involvesWsrs ? "WSRS" : "Office";
                const directionClass = step.fromLane === leftLane ? "to-right" : "to-left";
                const getLaneNodeClass = (lane: string) => {
                  if (lane === "Office") return "office-node";
                  if (lane === "WSRS") return "wsrs-node";
                  return "loco-node";
                };
                const rowClass = [
                  "reference-bounce-row",
                  involvesWsrs ? "wsrs-flow" : "office-flow",
                  directionClass,
                ].join(" ");
                return (
                  <article key={step.id} className={rowClass}>
                    <div className={`reference-bounce-node ${getLaneNodeClass(leftLane)}`}>
                      {renderHighlightedText(leftLabel, search)}
                    </div>
                    <div className="reference-bounce-arrow">
                      <span className="reference-bounce-message">{renderHighlightedText(step.title, search)}</span>
                    </div>
                    <div className={`reference-bounce-node ${getLaneNodeClass(rightLane)}`}>
                      {renderHighlightedText(rightLabel, search)}
                    </div>
                  </article>
                );
              })}
            </div>
          </section>
        ))}
      </div>
    </section>
  );
}

function renderWorkflowRelatedCards(
  detail: DetailModel,
  search: SearchConfig,
  onSelectRelatedLine: (entry: WorkflowRelatedDetail) => void,
): ReactNode {
  const entries = detail.workflowRelated ?? [];
  if (!entries.length) {
    return null;
  }

  return (
    <div className="workflow-related-list">
      {entries.map((entry) => (
        <div
          key={`${detail.lineId}:workflow:${entry.lineId}`}
          className="workflow-related-card"
          onClick={() => onSelectRelatedLine(entry)}
          onKeyDown={(event) => {
            if (event.key === "Enter" || event.key === " ") {
              event.preventDefault();
              onSelectRelatedLine(entry);
            }
          }}
          role="button"
          tabIndex={0}
        >
          <div className="workflow-related-head">
            <span className="workflow-related-line">Line {entry.lineNumber}</span>
            <span className="workflow-related-delta">{entry.deltaLabel}</span>
          </div>
          <div className="workflow-related-raw">{renderHighlightedText(entry.raw, search)}</div>
          <div className="workflow-related-relation">{renderHighlightedText(entry.relation, search)}</div>
        </div>
      ))}
    </div>
  );
}

function renderReferenceDetailContent(
  detail: DetailModel,
  activeTabContent: string,
  search: SearchConfig,
  selections: Record<string, string[]>,
  setSelections: Dispatch<SetStateAction<Record<string, string[]>>>,
): ReactNode {
  if (detail.referenceDiagram) {
    return renderReferenceDiagram(detail.referenceDiagram, search);
  }
  if (detail.referenceArtifact) {
    return renderReferenceArtifact(detail.referenceArtifact, search);
  }
  const networkContent = renderNetworkReferenceDetail(detail, search, selections);
  if (networkContent) {
    return networkContent;
  }
  if (!detail.referenceChoiceGroups?.length) {
    return renderStructuredReferenceCards(detail, activeTabContent, search);
  }

  const visibleGroups = getVisibleReferenceChoiceGroups(detail, search).filter((group) => group.items.length > 0);
  const selectedSections = visibleGroups.flatMap((group) =>
    getEffectiveSelectedItems(detail.lineId, group, selections).map((item) => ({ group, item })),
  );

  if (!selectedSections.length) {
    return <pre>{renderHighlightedText(activeTabContent, search)}</pre>;
  }

  return (
    <div className="reference-detail-sections">
      {selectedSections.map(({ group, item }) => {
        const sectionLines = buildReferenceItemDisplayLines(detail, item, selections, search);
        return (
          <section key={`${group.id}:${item.id}`} className="reference-detail-section">
            {selectedSections.length > 1 ? (
              <div className="reference-detail-heading">{renderHighlightedText(item.label, search)}</div>
            ) : null}
            {renderStructuredReferenceCards(detail, sectionLines.join("\n"), search)}
          </section>
        );
      })}
    </div>
  );
}

function getReferenceBubbleParts(line: ParsedLine, detail?: DetailModel | null): { primary: string; secondary?: string } {
  if ((line.source ?? "") === "reference:codelines" && line.raw.includes(" / ")) {
    const [primary, secondary] = line.raw.split(/\s\/\s(.+)/, 2);
    return { primary, secondary };
  }

  if ((line.source ?? "") === "reference:workflow") {
    const summary = detail?.workflow.summary?.trim();
    return summary ? { primary: line.raw, secondary: summary } : { primary: line.raw };
  }

  const trainMatch = /^Train message (\d{4,5}): (.+)$/.exec(line.raw);
  if (trainMatch) {
    return { primary: formatTrainMessageId(trainMatch[1]), secondary: trainMatch[2] };
  }

  const genisysMatch = /^Genisys (.+?): (.+)$/.exec(line.raw);
  if (genisysMatch) {
    return { primary: genisysMatch[1], secondary: genisysMatch[2] };
  }

  const splitIndex = line.raw.indexOf(": ");
  if (splitIndex > 0) {
    return {
      primary: line.raw.slice(0, splitIndex),
      secondary: line.raw.slice(splitIndex + 2),
    };
  }

  return { primary: line.raw };
}

type TrainMessageFlowBlock = {
  label: string;
  items: string[];
};

type TrainMessageFlowEdge = {
  from: string;
  to: string;
  label: string;
};

type TrainMessageFlowGroup = {
  id: string;
  messages: Array<{ id: string; name: string; flow: string }>;
  edges: TrainMessageFlowEdge[];
};

type WorkspaceSnapshot = {
  lines: ParsedLine[];
  lineDetails: Record<string, DetailModel>;
  selectedLineId: string | null;
  activeSource: string;
  activeTab: string;
  search: SearchConfig;
  referenceSelections: Record<string, string[]>;
};

function parseTrainMessageFlowData(detail: DetailModel): { flow: string; blocks: TrainMessageFlowBlock[]; notes: string[] } {
  const lines = detail.databaseContext ?? [];
  const blocks: TrainMessageFlowBlock[] = [];
  const notes: string[] = [];
  let flow = "";
  let currentBlock: TrainMessageFlowBlock | null = null;

  for (const rawLine of lines) {
    if (!rawLine.trim()) {
      continue;
    }
    if (rawLine.startsWith("Message flow: ")) {
      flow = rawLine.replace(/^Message flow:\s*/, "").trim();
      continue;
    }
    if (!rawLine.startsWith("  ") && rawLine.trim().endsWith(":")) {
      currentBlock = {
        label: rawLine.trim().slice(0, -1),
        items: [],
      };
      blocks.push(currentBlock);
      continue;
    }
    if (rawLine.startsWith("  ") && currentBlock) {
      currentBlock.items.push(rawLine.trim());
      continue;
    }
    notes.push(rawLine.trim());
  }

  return { flow, blocks, notes };
}

function parseTrainMessageFlowItem(value: string): { primary: string; secondary?: string; flow?: string } {
  const match = /^(\d{4,5})\s+(.+?)(?:\s+\(([^)]+)\))?$/.exec(value.trim());
  if (!match) {
    return { primary: value.trim() };
  }
  const [, primary, secondary, flow] = match;
  return {
    primary: formatTrainMessageId(primary),
    secondary: secondary.trim(),
    ...(flow ? { flow: flow.trim() } : {}),
  };
}

function formatTrainMessageId(messageId: string): string {
  const trimmed = String(messageId ?? "").trim();
  return /^\d{1,5}$/.test(trimmed) ? trimmed.padStart(5, "0") : trimmed;
}

function getTrainMessageFlowSectionLabel(label: string): string {
  if (label === "This message is sent after") return "Sent after";
  if (label === "This message confirms receipt of") return "Confirms";
  if (label === "When this message is sent, the other side replies with") return "Reply expected";
  if (label === "After this message is sent, the other side confirms with") return "Confirmation expected";
  return label;
}

function getTrainMessageFlowLabelPriority(label: string): number {
  if (label === "Reply expected") return 4;
  if (label === "Confirmation expected") return 3;
  if (label === "Sent after") return 2;
  if (label === "Confirms") return 1;
  return 0;
}

function isIncomingTrainMessageFlow(label: string): boolean {
  return label === "This message is sent after" || label === "This message confirms receipt of";
}

function getTrainMessageIdentity(line: ParsedLine): { id: string; name: string } | null {
  const match = /^Train message (\d{4,5}): (.+)$/.exec(line.raw);
  if (!match) {
    return null;
  }
  return { id: formatTrainMessageId(match[1]), name: match[2].trim() };
}

function buildTrainMessageFlowGroups(
  lines: ParsedLine[],
  lineDetails: Record<string, DetailModel>,
): TrainMessageFlowGroup[] {
  const nodes = new Map<string, { id: string; name: string; flow: string; order: number; detail: DetailModel }>();

  lines.forEach((line, order) => {
    const identity = getTrainMessageIdentity(line);
    if (!identity) {
      return;
    }
    const detail = lineDetails[line.id] ?? makeFallbackDetail(line);
    const flowData = parseTrainMessageFlowData(detail);
    nodes.set(identity.id, {
      id: identity.id,
      name: identity.name,
      flow: flowData.flow,
      order,
      detail,
    });
  });

  if (!nodes.size) {
    return [];
  }

  const adjacency = new Map<string, Set<string>>();
  for (const messageId of nodes.keys()) {
    adjacency.set(messageId, new Set<string>());
  }

  const edgeMap = new Map<string, TrainMessageFlowEdge>();
  for (const node of nodes.values()) {
    const flowData = parseTrainMessageFlowData(node.detail);
    for (const block of flowData.blocks) {
      const label = getTrainMessageFlowSectionLabel(block.label);
      for (const item of block.items) {
        const relation = parseTrainMessageFlowItem(item);
        if (!nodes.has(relation.primary)) {
          continue;
        }
        const incoming = isIncomingTrainMessageFlow(block.label);
        const from = incoming ? relation.primary : node.id;
        const to = incoming ? node.id : relation.primary;
        if (from === to) {
          continue;
        }
        const edgeKey = `${from}->${to}`;
        const existingEdge = edgeMap.get(edgeKey);
        if (!existingEdge || getTrainMessageFlowLabelPriority(label) > getTrainMessageFlowLabelPriority(existingEdge.label)) {
          edgeMap.set(edgeKey, { from, to, label });
        }
        adjacency.get(from)?.add(to);
        adjacency.get(to)?.add(from);
      }
    }
  }

  const orderedIds = Array.from(nodes.values())
    .sort((left, right) => left.order - right.order || left.id.localeCompare(right.id))
    .map((node) => node.id);

  const visited = new Set<string>();
  const groups: TrainMessageFlowGroup[] = [];

  for (const startId of orderedIds) {
    if (visited.has(startId)) {
      continue;
    }

    const queue = [startId];
    const componentIds: string[] = [];
    visited.add(startId);

    while (queue.length) {
      const currentId = queue.shift();
      if (!currentId) {
        continue;
      }
      componentIds.push(currentId);
      for (const neighbor of adjacency.get(currentId) ?? []) {
        if (visited.has(neighbor)) {
          continue;
        }
        visited.add(neighbor);
        queue.push(neighbor);
      }
    }

    const componentSet = new Set(componentIds);
    const messages = componentIds
      .map((messageId) => nodes.get(messageId))
      .filter((node): node is NonNullable<typeof node> => Boolean(node))
      .sort((left, right) => left.order - right.order || left.id.localeCompare(right.id))
      .map((node) => ({ id: node.id, name: node.name, flow: node.flow }));

    const edges = Array.from(edgeMap.values())
      .filter((edge) => componentSet.has(edge.from) && componentSet.has(edge.to))
      .sort((left, right) => {
        const leftFromOrder = nodes.get(left.from)?.order ?? Number.MAX_SAFE_INTEGER;
        const rightFromOrder = nodes.get(right.from)?.order ?? Number.MAX_SAFE_INTEGER;
        const fromDelta = leftFromOrder - rightFromOrder || left.from.localeCompare(right.from);
        if (fromDelta !== 0) {
          return fromDelta;
        }
        const leftToOrder = nodes.get(left.to)?.order ?? Number.MAX_SAFE_INTEGER;
        const rightToOrder = nodes.get(right.to)?.order ?? Number.MAX_SAFE_INTEGER;
        return leftToOrder - rightToOrder || left.to.localeCompare(right.to) || left.label.localeCompare(right.label);
      });

    groups.push({
      id: messages[0]?.id ?? startId,
      messages,
      edges,
    });
  }

  return groups;
}

function renderTrainMessageReferenceDetail(detail: DetailModel, search: SearchConfig): ReactNode {
  const flowData = parseTrainMessageFlowData(detail);
  const currentId = formatTrainMessageId(/^Train message (\d{4,5}):/.exec(detail.raw)?.[1] ?? "");
  const currentName = /^Train message \d{4,5}: (.+)$/.exec(detail.raw)?.[1]?.trim() ?? "";

  return (
    <div className="train-message-row-detail train-message-inspector-detail">
      {flowData.flow ? <span className="reference-badge train-flow-badge">{flowData.flow}</span> : null}
      {flowData.blocks.map((block) => (
        <div key={`${detail.lineId}:${block.label}`} className="train-message-row-section">
          <div className="train-message-row-section-label">{getTrainMessageFlowSectionLabel(block.label)}</div>
          <div className="train-message-row-flow-list">
            {block.items.map((item) => {
              const relation = parseTrainMessageFlowItem(item);
              const incoming = isIncomingTrainMessageFlow(block.label);
              const currentNode = (
                <span className="train-message-flow-node current">
                  <span className="train-message-related-id">{renderHighlightedText(currentId, search)}</span>
                  {currentName ? <span className="train-message-related-name">{renderHighlightedText(currentName, search)}</span> : null}
                </span>
              );
              const relatedNode = (
                <span className="train-message-flow-node">
                  <span className="train-message-related-id">{renderHighlightedText(relation.primary, search)}</span>
                  {relation.secondary ? <span className="train-message-related-name">{renderHighlightedText(relation.secondary, search)}</span> : null}
                </span>
              );
              return (
                <div key={`${detail.lineId}:${block.label}:${item}`} className="train-message-flow-row">
                  {incoming ? relatedNode : currentNode}
                  <span className="train-flow-arrow" aria-hidden="true">-&gt;</span>
                  {incoming ? currentNode : relatedNode}
                </div>
              );
            })}
          </div>
        </div>
      ))}
      {flowData.notes.map((note) => (
        <div key={`${detail.lineId}:${note}`} className="train-flow-note">{renderHighlightedText(note, search)}</div>
      ))}
      {!flowData.blocks.length && !flowData.notes.length ? (
        <div className="train-flow-note">No grounded request/reply flow was found for this message.</div>
      ) : null}
    </div>
  );
}

function isReferenceChipGridSource(activeSource: string, referenceSession: boolean): boolean {
  return referenceSession && (activeSource === "WORKFLOW" || activeSource === "GENISYS" || activeSource === "MESSAGE EXCHANGE");
}

function isReferenceGroupedSource(activeSource: string, referenceSession: boolean): boolean {
  return referenceSession && (activeSource === "CODELINES & STATIONS" || activeSource === "NETWORK");
}

function getReferenceSectionCategory(activeSource: string, line: ParsedLine): string {
  if (activeSource === "NETWORK") {
    if (/^Train network:/i.test(line.raw) || /: train network$/i.test(line.raw)) {
      return "Train Network";
    }
    if (/^Field \/ device network:/i.test(line.raw) || /: network reference$/i.test(line.raw)) {
      return "Field / Device Network";
    }
    if (/^Packet switches:/i.test(line.raw)) {
      return "Packet Switches";
    }
    return "Packet Switches";
  }
  return "";
}

function getContextTabLabel(detail: DetailModel): string {
  const firstContextLine = detail.databaseContext?.[0]?.trim() ?? "";
  if (detail.workflow.currentStep === "Reference view") {
    return "Reference";
  }
  if (firstContextLine === "Train / runtime context:") {
    return "Train & Runtime Context";
  }
  return "Assignments & Assets";
}

function isBlankAssignmentLine(line: string): boolean {
  const normalized = line.trim().toUpperCase();
  if (!/^\d+\.\s*/.test(normalized)) {
    return false;
  }
  const assignmentText = normalized.replace(/^\d+\.\s*/, "");
  return /^(?:BLANK\s*=\s*(?:BLANK|BLANK INDICATION|BLANK CONTROL|UNASSIGNED|BLANK\/UNASSIGNED)|=\s*(?:BLANK|UNASSIGNED))$/.test(assignmentText);
}

function hasBlankAssignments(detail: DetailModel | null): boolean {
  return (detail?.databaseContext ?? []).some((line) => isBlankAssignmentLine(line));
}

function filterBlankAssignments(content: string): string {
  return content
    .split("\n")
    .filter((line) => !isBlankAssignmentLine(line))
    .join("\n");
}

function getProgressLabel(progress: WorkspaceProgress | null): string {
  if (!progress) {
    return "";
  }
  const counts = progress.total > 0 ? `${Math.min(progress.completed, progress.total)}/${progress.total}` : "";
  return [progress.message, counts ? `(${counts})` : ""].filter(Boolean).join(" ");
}

function getProgressPhaseLabel(progress: WorkspaceProgress | null): string {
  if (!progress) {
    return "";
  }
  switch (progress.phase) {
    case "prepare":
      return "Preparing load";
    case "read":
      return "Reading logs";
    case "detail":
      return "Building details";
    case "package":
      return "Preparing viewer data";
    case "complete":
      return progress.percent >= 100 ? "Load complete" : "Applying viewer";
    default:
      return "Parsing logs";
  }
}

type AppMainProps = {
  authState?: AuthState | null;
  onLogout?: () => void;
  onOpenAdmin?: () => void;
  onOpenAccount?: () => void;
  localOnlyMode?: boolean;
  showLocalModeBanner?: boolean;
  serverReachable?: boolean;
  onReconnect?: () => void;
  updateAvailable?: boolean;
  onApplyUpdate?: () => void;
  onDismissUpdate?: () => void;
};

function AppMain({ authState, onLogout, onOpenAdmin, onOpenAccount, localOnlyMode = false, showLocalModeBanner = true, serverReachable = false, onReconnect, updateAvailable = false, onApplyUpdate, onDismissUpdate }: AppMainProps = {}) {
  const referenceWindowMode = useMemo(() => {
    try {
      return new URLSearchParams(window.location.search).get("mode") === "reference";
    } catch {
      return false;
    }
  }, []);
  const [lines, setLines] = useState<ParsedLine[]>([]);
  const [selected, setSelected] = useState<ParsedLine | null>(null);
  const [detail, setDetail] = useState<DetailModel | null>(null);
  const [lineDetails, setLineDetails] = useState<Record<string, DetailModel>>({});
  const [currentSessionId, setCurrentSessionId] = useState<string | null>(null);
  const [search, setSearch] = useState<SearchConfig>(defaultSearch);
  const [activeTab, setActiveTab] = useState("details");
  const [activeSource, setActiveSource] = useState("all");
  const [showBlankAssignments, setShowBlankAssignments] = useState(false);
  const [referenceSelections, setReferenceSelections] = useState<Record<string, string[]>>({});
  const [referenceDockLines, setReferenceDockLines] = useState<ParsedLine[]>([]);
  const [referenceDockLineDetails, setReferenceDockLineDetails] = useState<Record<string, DetailModel>>({});
  const [referenceDockSelected, setReferenceDockSelected] = useState<ParsedLine | null>(null);
  const [referenceDockDetail, setReferenceDockDetail] = useState<DetailModel | null>(null);
  const [referenceDockSelections, setReferenceDockSelections] = useState<Record<string, string[]>>({});
  const [referenceDockActiveSource, setReferenceDockActiveSource] = useState("MESSAGE EXCHANGE");
  const [referenceDockActiveTab, setReferenceDockActiveTab] = useState("details");
  const [referenceDockSearchQuery, setReferenceDockSearchQuery] = useState("");
  const [dragging, setDragging] = useState(false);
  const [finderOpen, setFinderOpen] = useState(false);
  const [finderShowResults, setFinderShowResults] = useState(false);
  const [finderDraftQuery, setFinderDraftQuery] = useState("");
  const [finderResults, setFinderResults] = useState<ParsedLine[]>([]);
  const [finderError, setFinderError] = useState("");
  const [finderSearchRunning, setFinderSearchRunning] = useState(false);
  const [finderResultsScrollTop, setFinderResultsScrollTop] = useState(0);
  const [finderResultsViewportHeight, setFinderResultsViewportHeight] = useState(240);
  const [timeWindowStartDateInput, setTimeWindowStartDateInput] = useState("");
  const [timeWindowStartInput, setTimeWindowStartInput] = useState("");
  const [timeWindowEndDateInput, setTimeWindowEndDateInput] = useState("");
  const [timeWindowEndInput, setTimeWindowEndInput] = useState("");
  const [workspaceBusy, setWorkspaceBusy] = useState("");
  const [workspaceError, setWorkspaceError] = useState(() => getInitialWorkspaceError());
  const [workspaceProgress, setWorkspaceProgress] = useState<WorkspaceProgress | null>(null);
  const [queuedInputPaths, setQueuedInputPaths] = useState<string[]>([]);
  const [queuedBrowserFiles, setQueuedBrowserFiles] = useState<BrowserUploadFile[]>([]);
  const [queuedBrowserSkipped, setQueuedBrowserSkipped] = useState<string[]>([]);
  const [busyHeartbeatSeconds, setBusyHeartbeatSeconds] = useState(0);
  const [loadingLineDetailId, setLoadingLineDetailId] = useState<string | null>(null);
  const [logListScrollTop, setLogListScrollTop] = useState(0);
  const [logListViewportHeight, setLogListViewportHeight] = useState(720);
  const [workflowAnchor, setWorkflowAnchor] = useState<{ lineId: string; detail: DetailModel } | null>(null);
  const previousNonReferenceWorkspaceRef = useRef<WorkspaceSnapshot | null>(null);
  const selectedLineIdRef = useRef<string | null>(null);
  const logListRef = useRef<HTMLDivElement | null>(null);
  const finderResultsListRef = useRef<HTMLDivElement | null>(null);
  const finderInputRef = useRef<HTMLInputElement | null>(null);
  const autoParseInputsRef = useRef(true);
  const dragDepthRef = useRef(0);
  const browserFileInputRef = useRef<HTMLInputElement | null>(null);
  const browserFolderInputRef = useRef<HTMLInputElement | null>(null);
  const finderSearchRunIdRef = useRef(0);
  const lineTimestampCacheRef = useRef(new Map<string, Partial<Record<LogTimeSourceMode, number | null>>>());
  const pacificOffsetMinutesByHourRef = useRef(new Map<string, number | null>());
  const previousTimeWindowDefaultsRef = useRef<{ start: string; end: string }>({ start: "", end: "" });
  const webAppMode = isBrowserWebAppMode();

  useEffect(() => {
    if (!window.tmds?.onWorkspaceMenuCommand) {
      return;
    }
    return window.tmds.onWorkspaceMenuCommand((command, payload) => {
      void handleWorkspaceMenuCommand(command, payload);
    });
  }, []);

  useEffect(() => {
    if (!workspaceBusy) {
      setBusyHeartbeatSeconds(0);
      return;
    }
    setBusyHeartbeatSeconds(0);
    const startedAt = Date.now();
    const id = window.setInterval(() => {
      setBusyHeartbeatSeconds(Math.floor((Date.now() - startedAt) / 1000));
    }, 1000);
    return () => window.clearInterval(id);
  }, [workspaceBusy]);

  useEffect(() => {
    if (!window.tmds?.onWorkspaceProgress) {
      return;
    }
    return window.tmds.onWorkspaceProgress((progress) => {
      setWorkspaceProgress(progress);
    });
  }, []);

  useEffect(() => {
    if (!referenceWindowMode) {
      return;
    }
    void loadFoundationSession();
  }, [referenceWindowMode]);

  useEffect(() => {
    if (!finderOpen) {
      return;
    }
    const focusHandle = window.requestAnimationFrame(() => {
      finderInputRef.current?.focus();
      finderInputRef.current?.select();
    });
    return () => window.cancelAnimationFrame(focusHandle);
  }, [finderOpen]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && !event.altKey && event.key.toLowerCase() === "f") {
        event.preventDefault();
        openFinder(true);
        return;
      }
      if (event.key === "Escape" && finderOpen) {
        event.preventDefault();
        closeFinder();
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [finderOpen]);

  useEffect(() => {
    if (search.query.trim()) {
      return;
    }
    setFinderShowResults(false);
    setFinderResults([]);
    setFinderError("");
    setFinderSearchRunning(false);
  }, [search.query]);

  useEffect(() => {
    if (finderDraftQuery === search.query) {
      return;
    }
    finderSearchRunIdRef.current += 1;
    setFinderShowResults(false);
    setFinderResults([]);
    setFinderError("");
    setFinderSearchRunning(false);
  }, [finderDraftQuery, search.query]);

  useEffect(() => {
    selectedLineIdRef.current = selected?.id ?? null;
  }, [selected]);

  useEffect(() => {
    lineTimestampCacheRef.current.clear();
  }, [lines]);

  const referenceSession = isReferenceLibrarySession(lines);
  const workflowViewerMode = !referenceSession && activeTab === "workflow" && !!detail;
  const workflowAnchorDetail = workflowViewerMode ? workflowAnchor?.detail ?? detail : null;
  const referenceDockOpen = referenceDockLines.length > 0;
  const referenceDockSearch = useMemo(() => createSearchConfig(referenceDockSearchQuery), [referenceDockSearchQuery]);
  const referenceDockSearchPattern = useMemo(
    () => buildMatchExpression(referenceDockSearch),
    [referenceDockSearch.caseSensitive, referenceDockSearch.query, referenceDockSearch.regex, referenceDockSearch.wholeWord],
  );
  const activeSearch = search;
  const activeSearchPattern = useMemo(
    () => buildMatchExpression(activeSearch),
    [activeSearch.caseSensitive, activeSearch.query, activeSearch.regex, activeSearch.wholeWord],
  );
  const logTimeSourceMode: LogTimeSourceMode = "original";
  const logTimeDisplayMode: LogTimeDisplayMode = "source";
  const effectiveLogTimeDisplayMode: Exclude<LogTimeDisplayMode, "source"> | "original" = "original";
  function getPacificOffsetMinutes(parts: ViewerTimestampParts): number | null {
    const key = `${parts.year}-${String(parts.month).padStart(2, "0")}-${String(parts.day).padStart(2, "0")} ${String(parts.hour).padStart(2, "0")}`;
    const cached = pacificOffsetMinutesByHourRef.current.get(key);
    if (cached !== undefined) {
      return cached;
    }
    const baseParts = { ...parts, minute: 0, second: 0, millisecond: 0 };
    const epoch = convertZonedPartsToEpoch(baseParts, pacificTimeZone);
    if (epoch === null) {
      pacificOffsetMinutesByHourRef.current.set(key, null);
      return null;
    }
    const utcMs = Date.UTC(baseParts.year, baseParts.month - 1, baseParts.day, baseParts.hour, baseParts.minute, baseParts.second, 0);
    const offsetMinutes = Math.round((utcMs - epoch) / 60000);
    pacificOffsetMinutesByHourRef.current.set(key, offsetMinutes);
    return offsetMinutes;
  }

  function resolveEpochFromParts(parts: ViewerTimestampParts, mode: LogTimeSourceMode | "utc" | "pacific"): number | null {
    if (mode === "original") {
      return new Date(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second, parts.millisecond).getTime();
    }
    if (mode === "utc") {
      return Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second, parts.millisecond);
    }
    const offsetMinutes = getPacificOffsetMinutes(parts);
    if (offsetMinutes === null) {
      return null;
    }
    return Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second, parts.millisecond) - (offsetMinutes * 60000);
  }

  function getLineTimestampMs(line: ParsedLine, mode: LogTimeSourceMode): number | null {
    const cache = lineTimestampCacheRef.current.get(line.id) ?? {};
    const cached = cache[mode];
    if (cached !== undefined) {
      return cached;
    }
    const parts = getViewerTimestampParts(line.timestamp);
    const value = parts ? resolveEpochFromParts(parts, mode) : null;
    lineTimestampCacheRef.current.set(line.id, { ...cache, [mode]: value });
    return value;
  }

  const finderHasQuery = finderDraftQuery.trim().length > 0;
  const workflowFocusedLines = useMemo(() => {
    if (!workflowAnchorDetail) {
      return [] as ParsedLine[];
    }
    const focusedIds = new Set<string>([
      workflowAnchorDetail.lineId,
      ...(workflowAnchorDetail.workflowRelated ?? []).map((entry) => entry.lineId),
    ]);
    return lines.filter((line) => focusedIds.has(line.id));
  }, [lines, workflowAnchorDetail]);

  useEffect(() => {
    if (referenceSession || !selected || !detail) {
      setWorkflowAnchor(null);
      return;
    }
    if (!workflowViewerMode) {
      if (workflowAnchor?.lineId !== selected.id || workflowAnchor.detail !== detail) {
        setWorkflowAnchor({ lineId: selected.id, detail });
      }
      return;
    }
    if (!workflowAnchor) {
      setWorkflowAnchor({ lineId: selected.id, detail });
    }
  }, [detail, referenceSession, selected, workflowAnchor, workflowViewerMode]);

  const referenceDockSourceTabs = useMemo(() => {
    const categorySequence: LogCategory[] = [...referenceCategoryOrder];
    const counts = new Map<LogCategory, number>();
    for (const line of referenceDockLines) {
      const category = getLogCategory(line);
      counts.set(category, (counts.get(category) ?? 0) + 1);
    }
    return [
      { key: "all", label: "All", count: referenceDockLines.length },
      ...categorySequence
        .filter((category) => (counts.get(category) ?? 0) > 0)
        .map((category) => ({
          key: category,
          label: category,
          count: counts.get(category) ?? 0,
        })),
    ];
  }, [referenceDockLines]);

  const referenceDockScopedLines = useMemo(() => {
    const scoped = referenceDockActiveSource === "all"
      ? referenceDockLines
      : referenceDockLines.filter((line) => getLogCategory(line) === (referenceDockActiveSource as LogCategory));
    return sortLinesForViewer(scoped);
  }, [referenceDockActiveSource, referenceDockLines]);

  const referenceDockVisible = useMemo(() => {
    const searchActive = hasActiveSearch(referenceDockSearch);
    if (!searchActive) {
      return referenceDockScopedLines;
    }
    const filtered = referenceDockScopedLines.filter((line) => matchesWithPattern(line, referenceDockSearchPattern, referenceDockLineDetails[line.id] ?? null));
    return filtered;
  }, [referenceDockLineDetails, referenceDockScopedLines, referenceDockSearch, referenceDockSearchPattern]);

  const referenceDockTabs = useMemo(() => {
    if (!referenceDockDetail) {
      return [];
    }
    return buildReferenceTabs(referenceDockDetail);
  }, [referenceDockDetail]);

  const referenceDockActiveTabContent = buildReferenceTabDisplayText(
    referenceDockDetail,
    referenceDockActiveTab,
    referenceDockTabs.find((tab) => tab.key === referenceDockActiveTab)?.content ?? "",
    referenceDockSelections,
    referenceDockSearch,
  );

  const sourceTabs = useMemo(() => {
    if (!referenceSession) {
      return [] as Array<{ key: string; label: string; count: number }>;
    }
    const categorySequence: LogCategory[] = referenceSession
      ? [...referenceCategoryOrder]
      : [...categoryOrder, "OTHER"];
    const counts = new Map<LogCategory, number>();
    for (const line of lines) {
      const category = getLogCategory(line);
      counts.set(category, (counts.get(category) ?? 0) + 1);
    }

    const tabs: Array<{ key: string; label: string; count: number }> = [
      ...(referenceSession ? [] : [{ key: "all", label: "All", count: lines.length }]),
      ...categorySequence
        .filter((category) => category !== "OTHER" || (counts.get("OTHER") ?? 0) > 0)
        .map((category) => ({
        key: category,
        label: category,
        count: counts.get(category) ?? 0,
      })),
    ];

    return tabs;
  }, [lines]);

  const sourceScopedLines = useMemo(() => {
    if (workflowViewerMode) {
      return workflowFocusedLines;
    }
    if (!referenceSession) {
      return lines;
    }
    const scoped = activeSource === "all"
      ? lines
      : lines.filter((line) => getLogCategory(line) === (activeSource as LogCategory));
    return sortLinesForViewer(scoped);
  }, [activeSource, lines, workflowFocusedLines, workflowViewerMode]);
  const sessionTimeBounds = useMemo(() => {
    let min: number | null = null;
    let max: number | null = null;
    for (const line of sourceScopedLines) {
      const value = getLineTimestampMs(line, logTimeSourceMode);
      if (value === null) {
        continue;
      }
      if (min === null || value < min) {
        min = value;
      }
      if (max === null || value > max) {
        max = value;
      }
    }
    return { min, max };
  }, [logTimeSourceMode, sourceScopedLines]);
  const sessionStartEpochMs = sessionTimeBounds.min;
  const sessionEndEpochMs = sessionTimeBounds.max;
  const sessionStartDateLabel = useMemo(() => {
    if (sessionStartEpochMs === null) {
      return "No session date";
    }
    const formatted = formatEpochForDisplay(sessionStartEpochMs, effectiveLogTimeDisplayMode);
    return formatted.split(" ")[0] ?? formatted;
  }, [effectiveLogTimeDisplayMode, sessionStartEpochMs]);
  const sessionEndDateLabel = useMemo(() => {
    if (sessionEndEpochMs === null) {
      return "No session date";
    }
    const formatted = formatEpochForDisplay(sessionEndEpochMs, effectiveLogTimeDisplayMode);
    return formatted.split(" ")[0] ?? formatted;
  }, [effectiveLogTimeDisplayMode, sessionEndEpochMs]);
  const defaultTimeWindowStartDate = sessionStartDateLabel === "No session date" ? "" : sessionStartDateLabel;
  const defaultTimeWindowEndDate = sessionEndDateLabel === "No session date" ? "" : sessionEndDateLabel;
  useEffect(() => {
    if (referenceSession) {
      return;
    }
    const previousDefaults = previousTimeWindowDefaultsRef.current;
    if (!timeWindowStartDateInput || timeWindowStartDateInput === previousDefaults.start) {
      setTimeWindowStartDateInput(defaultTimeWindowStartDate);
    }
    if (!timeWindowEndDateInput || timeWindowEndDateInput === previousDefaults.end) {
      setTimeWindowEndDateInput(defaultTimeWindowEndDate);
    }
    previousTimeWindowDefaultsRef.current = {
      start: defaultTimeWindowStartDate,
      end: defaultTimeWindowEndDate,
    };
  }, [defaultTimeWindowEndDate, defaultTimeWindowStartDate, referenceSession, timeWindowEndDateInput, timeWindowStartDateInput]);
  const timeWindowStartMs = useMemo(() => {
    const dateParts = parseDateValue(timeWindowStartDateInput || defaultTimeWindowStartDate);
    const timeParts = parseTimeValue(timeWindowStartInput);
    if (!dateParts) {
      return null;
    }
    return resolveEpochFromParts({
      year: dateParts.year,
      month: dateParts.month,
      day: dateParts.day,
      hour: timeParts?.hour ?? 0,
      minute: timeParts?.minute ?? 0,
      second: timeParts?.second ?? 0,
      millisecond: 0,
    }, effectiveLogTimeDisplayMode);
  }, [defaultTimeWindowStartDate, effectiveLogTimeDisplayMode, timeWindowStartDateInput, timeWindowStartInput]);
  const timeWindowEndMs = useMemo(() => {
    const dateParts = parseDateValue(timeWindowEndDateInput || defaultTimeWindowEndDate);
    const timeParts = parseTimeValue(timeWindowEndInput);
    if (!dateParts) {
      return null;
    }
    return resolveEpochFromParts({
      year: dateParts.year,
      month: dateParts.month,
      day: dateParts.day,
      hour: timeParts?.hour ?? 23,
      minute: timeParts?.minute ?? 59,
      second: timeParts ? (timeParts.hasExplicitSecond ? timeParts.second : 59) : 59,
      millisecond: 999,
    }, effectiveLogTimeDisplayMode);
  }, [defaultTimeWindowEndDate, effectiveLogTimeDisplayMode, timeWindowEndDateInput, timeWindowEndInput]);
  const hasTimeWindowDraft = Boolean(
    timeWindowStartInput
    || timeWindowEndInput
    || ((timeWindowStartDateInput || defaultTimeWindowStartDate) && (timeWindowStartDateInput || defaultTimeWindowStartDate) !== defaultTimeWindowStartDate)
    || ((timeWindowEndDateInput || defaultTimeWindowEndDate) && (timeWindowEndDateInput || defaultTimeWindowEndDate) !== defaultTimeWindowEndDate),
  );
  const hasAppliedTimeWindow = timeWindowStartMs !== null || timeWindowEndMs !== null;
  const timeScopedLines = useMemo(() => {
    if (referenceSession || !hasAppliedTimeWindow) {
      return sourceScopedLines;
    }
    return sourceScopedLines.filter((line) => {
      const timestampMs = getLineTimestampMs(line, logTimeSourceMode);
      if (timestampMs === null) {
        return false;
      }
      if (timeWindowStartMs !== null && timestampMs < timeWindowStartMs) {
        return false;
      }
      if (timeWindowEndMs !== null && timestampMs > timeWindowEndMs) {
        return false;
      }
      return true;
    });
  }, [hasAppliedTimeWindow, logTimeSourceMode, referenceSession, sourceScopedLines, timeWindowEndMs, timeWindowStartMs]);

  const searchActive = hasActiveSearch(activeSearch);
  const runtimeVisible = useMemo(() => {
    if (!searchActive) {
      return timeScopedLines;
    }
    const filtered = timeScopedLines.filter((line) => matchesWithPattern(line, activeSearchPattern, null));
    return activeSearch.filterOnlyMatches ? filtered : timeScopedLines;
  }, [activeSearch, activeSearchPattern, searchActive, timeScopedLines]);

  const referenceVisible = useMemo(() => {
    if (!searchActive) {
      return timeScopedLines;
    }
    const filtered = timeScopedLines.filter((line) => matchesWithPattern(line, activeSearchPattern, lineDetails[line.id] ?? null));
    return activeSearch.filterOnlyMatches ? filtered : timeScopedLines;
  }, [activeSearch, activeSearchPattern, lineDetails, searchActive, timeScopedLines]);

  const visible = referenceSession ? referenceVisible : runtimeVisible;
  const virtualLogWindow = useMemo(() => {
    if (referenceSession) {
      return {
        start: 0,
        end: visible.length,
        topPadding: 0,
        bottomPadding: 0,
        lines: visible,
      };
    }
    const viewportRows = Math.max(1, Math.ceil(logListViewportHeight / logRowHeight));
    const start = Math.max(0, Math.floor(logListScrollTop / logRowHeight) - logRowOverscan);
    const end = Math.min(visible.length, start + viewportRows + (logRowOverscan * 2));
    return {
      start,
      end,
      topPadding: start * logRowHeight,
      bottomPadding: Math.max(0, (visible.length - end) * logRowHeight),
      lines: visible.slice(start, end),
    };
  }, [logListScrollTop, logListViewportHeight, referenceSession, visible]);

  useEffect(() => {
    const node = logListRef.current;
    if (!node || referenceSession) {
      return;
    }
    const syncViewport = () => {
      setLogListViewportHeight(node.clientHeight || 720);
      setLogListScrollTop(node.scrollTop || 0);
    };
    syncViewport();
    const observer = typeof ResizeObserver === "undefined"
      ? null
      : new ResizeObserver(() => syncViewport());
    observer?.observe(node);
    return () => observer?.disconnect();
  }, [referenceSession, visible.length]);

  useEffect(() => {
    const node = finderResultsListRef.current;
    if (!node || !finderShowResults) {
      return;
    }
    const syncViewport = () => {
      setFinderResultsViewportHeight(node.clientHeight || 240);
      setFinderResultsScrollTop(node.scrollTop || 0);
    };
    syncViewport();
    const observer = typeof ResizeObserver === "undefined"
      ? null
      : new ResizeObserver(() => syncViewport());
    observer?.observe(node);
    return () => observer?.disconnect();
  }, [finderResults.length, finderShowResults]);

  useEffect(() => {
    if (referenceSession || !lines.length) {
      return;
    }
    previousNonReferenceWorkspaceRef.current = {
      lines,
      lineDetails,
      selectedLineId: selected?.id ?? null,
      activeSource,
      activeTab,
      search,
      referenceSelections,
    };
  }, [referenceSession, lines, lineDetails, selected, activeSource, activeTab, search, referenceSelections]);

  const finderScopeLabel = referenceSession
    ? `Reference library${activeSource && activeSource !== "all" ? ` · ${activeSource}` : ""}`
    : `Parsed logs${activeSource !== "all" ? ` · ${activeSource}` : ""}`;

  const finderScopeSummary = referenceSession
    ? `Reference library${activeSource && activeSource !== "all" ? ` - ${activeSource}` : ""}`
    : `Parsed logs${activeSource !== "all" ? ` - ${activeSource}` : ""}`;
  const finderPanelSubtitle = `${finderScopeSummary}${referenceDockOpen && !referenceSession ? " - docked reference results stay separate" : ""}`;
  const timeFilterSummary = hasAppliedTimeWindow
    ? `${timeWindowStartDateInput || defaultTimeWindowStartDate || "..."} ${timeWindowStartInput || "00:00:00"} to ${timeWindowEndDateInput || defaultTimeWindowEndDate || "..."} ${timeWindowEndInput || "23:59:59"}`
    : "No time window applied";
  const virtualFinderResults = useMemo(() => {
    const viewportRows = Math.max(1, Math.ceil(finderResultsViewportHeight / finderResultRowHeight));
    const start = Math.max(0, Math.floor(finderResultsScrollTop / finderResultRowHeight) - finderResultOverscan);
    const end = Math.min(finderResults.length, start + viewportRows + (finderResultOverscan * 2));
    return {
      start,
      end,
      topPadding: start * finderResultRowHeight,
      bottomPadding: Math.max(0, (finderResults.length - end) * finderResultRowHeight),
      lines: finderResults.slice(start, end),
    };
  }, [finderResults, finderResultsScrollTop, finderResultsViewportHeight]);


  const tabs = useMemo(() => {
    if (!detail) {
      return [];
    }

    if (referenceSession) {
      return buildReferenceTabs(detail);
    }

    const workflowTabSourceDetail = workflowViewerMode ? workflowAnchorDetail ?? detail : detail;

    return [
      {
        key: "details",
        label: "Details",
        content: buildDetailsText(detail),
      },
      {
        key: "workflow",
        label: "Workflow",
        content: workflowTabSourceDetail.workflowContext?.join("\n") ?? "",
      },
      {
        key: "payload",
        label: "Payload",
        content: detail.payloadContext?.join("\n") ?? "",
        hidden: shouldHidePayloadTab(detail),
      },
      {
        key: "tmds",
        label: getContextTabLabel(detail),
        content: detail.databaseContext?.join("\n") ?? "",
      },
    ].filter((tab) => !tab.hidden && (tab.key === "details" || tab.content.trim().length > 0));
  }, [detail, referenceSession, workflowAnchorDetail, workflowViewerMode]);

  useEffect(() => {
    if (tabs.length && !tabs.some((tab) => tab.key === activeTab)) {
      setActiveTab(tabs[0].key);
    }
  }, [activeTab, tabs]);

  useEffect(() => {
    if (referenceDockTabs.length && !referenceDockTabs.some((tab) => tab.key === referenceDockActiveTab)) {
      setReferenceDockActiveTab(referenceDockTabs[0].key);
    }
  }, [referenceDockActiveTab, referenceDockTabs]);

  useEffect(() => {
    if (!detail?.referenceChoiceGroups?.length) {
      return;
    }

    const choiceGroups = detail.referenceChoiceGroups;
    setReferenceSelections((current) => {
      let changed = false;
      const next = { ...current };
      for (const group of choiceGroups) {
        if (!group.items.length) {
          continue;
        }
        const key = getReferenceSelectionKey(detail.lineId, group.id);
        if (!next[key] && group.selectionMode !== "multiple") {
          next[key] = [group.items[0].id];
          changed = true;
        }
      }
      return changed ? next : current;
    });
  }, [detail]);

  useEffect(() => {
    if (!referenceDockDetail?.referenceChoiceGroups?.length) {
      return;
    }

    const choiceGroups = referenceDockDetail.referenceChoiceGroups;
    setReferenceDockSelections((current) => {
      let changed = false;
      const next = { ...current };
      for (const group of choiceGroups) {
        if (!group.items.length) {
          continue;
        }
        const key = getReferenceSelectionKey(referenceDockDetail.lineId, group.id);
        if (!next[key] && group.selectionMode !== "multiple") {
          next[key] = [group.items[0].id];
          changed = true;
        }
      }
      return changed ? next : current;
    });
  }, [referenceDockDetail]);

  async function handleWorkspaceMenuCommand(command: WorkspaceMenuCommand, payload?: string[]) {
    if (command === "open-inputs" && payload?.length) {
      await parseInputs(payload, false);
      return;
    }
    if (command === "open-finder") {
      openFinder(true);
      return;
    }
    if (command === "load-foundation") {
      await loadFoundationSession();
      return;
    }
    if (command === "load-review-sample") {
      await loadReviewSampleSession();
      return;
    }
  }

  function applySession(session: SessionData, options?: { preserveEmptyReferenceSelection?: boolean }) {
    const nextLineDetails = session.lineDetails ?? {};
    const nextSelection = describeSessionSelection({ ...session, lineDetails: nextLineDetails });
    const referenceSession = isReferenceLibrarySession(session.lines);
    const preserveEmptyReferenceSelection = Boolean(options?.preserveEmptyReferenceSelection && referenceSession);
    const nextActiveSource = referenceSession
      ? (preserveEmptyReferenceSelection ? "" : (Array.from(new Set(session.lines.map((line) => getLogCategory(line))))[0] ?? "all"))
      : "all";
    const resetLogViewport = () => {
      const node = logListRef.current;
      if (node) {
        node.scrollTop = 0;
      }
      setLogListScrollTop(0);
    };
    cancelFinderSearchRun();
    resetLogViewport();
    startTransition(() => {
      setCurrentSessionId(session.sessionId ?? null);
      setLines(session.lines);
      setSelected(preserveEmptyReferenceSelection ? null : nextSelection.selected);
      setDetail(preserveEmptyReferenceSelection ? null : nextSelection.detail);
      setLineDetails(nextLineDetails);
      setFinderDraftQuery("");
      setSearch(defaultSearch);
      setTimeWindowStartDateInput("");
      setTimeWindowStartInput("");
      setTimeWindowEndDateInput("");
      setTimeWindowEndInput("");
      setActiveSource(nextActiveSource);
      setActiveTab("details");
    });
    requestAnimationFrame(resetLogViewport);
  }

  function requestWarmLineDetails(targetLines: ParsedLine[]) {
    if (!targetLines.length || referenceSession || localOnlyMode) {
      return;
    }
    const uniqueIds = Array.from(new Set(targetLines.map((line) => line.id).filter(Boolean)));
    if (!uniqueIds.length) {
      return;
    }
    if (window.tmds?.warmLineDetails) {
      void window.tmds.warmLineDetails(uniqueIds, currentSessionId ?? undefined);
    } else {
      void fetch("/api/warm-line-details", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ lineIds: uniqueIds, sessionId: currentSessionId }),
      }).catch(() => undefined);
    }
  }

  async function applySessionWithProgress(
    session: SessionData,
    mode: "logs" | "reference",
    options?: { preserveEmptyReferenceSelection?: boolean },
  ) {
    const total = Math.max(session.lines.length, 1);
    const buildViewerMessage = (percent: number): string => {
      if (mode === "reference") {
        if (percent >= 99) {
          return `verifying ${session.lines.length} reference entr${session.lines.length === 1 ? "y" : "ies"} in the viewer`;
        }
        if (percent >= 98) {
          return `painting ${session.lines.length} reference entr${session.lines.length === 1 ? "y" : "ies"} in the viewer`;
        }
        return `applying ${session.lines.length} reference entr${session.lines.length === 1 ? "y" : "ies"} to the viewer`;
      }
      if (percent >= 99) {
        return `verifying ${session.lines.length} parsed line${session.lines.length === 1 ? "" : "s"} in the viewer`;
      }
      if (percent >= 98) {
        return `painting ${session.lines.length} parsed line${session.lines.length === 1 ? "" : "s"} in the viewer`;
      }
      return `applying ${session.lines.length} parsed line${session.lines.length === 1 ? "" : "s"} to the viewer`;
    };
    const setViewerProgress = (percent: number) => {
      setWorkspaceProgress((current) => ({
        phase: "complete",
        message: buildViewerMessage(percent),
        percent,
        completed: session.lines.length,
        total,
        currentPath: current?.currentPath,
      }));
    };
    setWorkspaceBusy(mode === "reference" ? "Rendering reference library..." : "Rendering parsed logs...");
    setViewerProgress(97);
    await new Promise<void>((resolve) => {
      requestAnimationFrame(() => resolve());
    });
    applySession(session, options);
    await new Promise<void>((resolve) => {
      requestAnimationFrame(() => resolve());
    });
    setViewerProgress(98);
    await new Promise<void>((resolve) => {
      requestAnimationFrame(() => resolve());
    });
    if (mode === "logs") {
      requestWarmLineDetails(session.lines.slice(0, 400));
    }
    setViewerProgress(99);
    await new Promise<void>((resolve) => {
      requestAnimationFrame(() => resolve());
    });
    setWorkspaceProgress((current) => ({
      phase: "complete",
      message: mode === "reference"
        ? `ready: ${session.lines.length} reference entr${session.lines.length === 1 ? "y" : "ies"} visible`
        : `ready: ${session.lines.length} parsed line${session.lines.length === 1 ? "" : "s"} visible`,
      percent: 100,
      completed: session.lines.length,
      total,
      currentPath: current?.currentPath,
    }));
    await new Promise<void>((resolve) => {
      requestAnimationFrame(() => resolve());
    });
  }

  async function loadFoundationSession() {
    if (localOnlyMode) {
      setWorkspaceError("");
      setWorkspaceBusy("Loading reference library...");
      setWorkspaceProgress(null);
      try {
        let session: SessionData | null = null;
        try {
          const response = await fetch("./data/reference-session.json", { cache: "no-store" });
          if (response.ok) {
            session = (await response.json()) as SessionData;
          }
        } catch {
          // network/parse failure — fall through to in-browser stub builder
        }
        if (!session) {
          session = buildStaticReferenceSession();
          setWorkspaceError("Loaded a partial in-browser reference library because no pre-baked data/reference-session.json was shipped with this build.");
        }
        await applySessionWithProgress(session, "reference", { preserveEmptyReferenceSelection: true });
      } catch (error) {
        setWorkspaceError(error instanceof Error ? error.message : "Reference library load failed.");
      } finally {
        setWorkspaceBusy("");
        setWorkspaceProgress(null);
      }
      return;
    }
    if (!referenceWindowMode && window.tmds?.openReferenceLibraryWindow) {
      setWorkspaceError("");
      await window.tmds.openReferenceLibraryWindow();
      return;
    }
    setWorkspaceError("");
    setWorkspaceBusy("Loading reference library...");
    setWorkspaceProgress(null);
    try {
      const session = window.tmds?.loadSampleSession
        ? await window.tmds.loadSampleSession()
        : await fetchWebSession("/api/reference");
      await applySessionWithProgress(session, "reference", { preserveEmptyReferenceSelection: true });
    } catch (error) {
      setWorkspaceError(error instanceof Error ? error.message : "Reference library load failed.");
    } finally {
      setWorkspaceBusy("");
      setWorkspaceProgress(null);
    }
  }

  async function loadReviewSampleSession() {
    if (localOnlyMode) {
      setWorkspaceError("");
      setWorkspaceBusy("Loading review logs...");
      setWorkspaceProgress(null);
      try {
        let session: SessionData | null = null;
        try {
          const response = await fetch("./data/review-sample-session.json", { cache: "no-store" });
          if (response.ok) {
            session = (await response.json()) as SessionData;
          }
        } catch {
          // network/parse failure — fall through to in-browser stub builder
        }
        if (!session) {
          session = buildStaticReviewSampleSession();
          if (!session.lines.length) {
            setWorkspaceError("No readable static review logs were bundled.");
          } else {
            setWorkspaceError("Loaded a partial in-browser review sample because no pre-baked data/review-sample-session.json was shipped with this build.");
          }
        }
        await applySessionWithProgress(session, "logs");
      } catch (error) {
        setWorkspaceError(error instanceof Error ? error.message : "Review sample load failed.");
      } finally {
        setWorkspaceBusy("");
        setWorkspaceProgress(null);
      }
      return;
    }
    setWorkspaceError("");
    setWorkspaceBusy("Loading review logs...");
    setWorkspaceProgress(null);
    try {
      const session = window.tmds?.loadReviewSampleSession
        ? await window.tmds.loadReviewSampleSession()
        : await fetchWebSession("/api/review-sample");
      await applySessionWithProgress(session, "logs");
      if (!session.lines.length) {
        setWorkspaceError("No readable review logs were found in the bounded sample set.");
      }
    } catch (error) {
      setWorkspaceError(error instanceof Error ? error.message : "Sample review load failed.");
    } finally {
      setWorkspaceBusy("");
      setWorkspaceProgress(null);
    }
  }

  async function openFiles() {
    if (workspaceBusy) {
      return;
    }
    if (!window.tmds?.pickInputPaths) {
      browserFileInputRef.current?.click();
      return;
    }
    const picked = await window.tmds.pickInputPaths();
    if (!picked.length) return;
    await parseInputs(picked, false);
  }

  function openBrowserFilePicker() {
    if (workspaceBusy) {
      return;
    }
    browserFileInputRef.current?.click();
  }

  function openBrowserFolderPicker() {
    if (workspaceBusy) {
      return;
    }
    const input = browserFolderInputRef.current;
    if (!input) {
      return;
    }
    input.setAttribute("webkitdirectory", "");
    input.click();
  }

  async function parseBrowserUploadFiles(files: BrowserUploadFile[], skipped: string[] = []) {
    if (!files.length) {
      if (skipped.length) {
        setWorkspaceError(`The dropped items could not be opened by the browser. First skipped item: ${skipped[0]}`);
      }
      return;
    }
    setWorkspaceError("");
    setWorkspaceBusy(`${localOnlyMode ? "Parsing" : "Uploading"} ${summarizeBrowserUpload(files)}...`);
    setWorkspaceProgress({
      phase: "prepare",
      message: `preparing ${summarizeBrowserUpload(files)} for ${localOnlyMode ? "local parsing" : "upload"}`,
      percent: 0,
      completed: 0,
      total: Math.max(files.length, 1),
      currentPath: files[0]?.relativePath || files[0]?.file.name,
    });
    try {
      let session: SessionData;
      let localSkipped: { name: string; reason: string }[] = [];
      if (localOnlyMode) {
        const result = await ingestBrowserFilesLocally(
          files.map((entry) => ({ file: entry.file, relativePath: entry.relativePath })),
          (progress) => setWorkspaceProgress(progress),
        );
        session = result.session;
        localSkipped = result.skipped;
      } else {
        session = await uploadBrowserFiles(files, (progress) => {
          setWorkspaceProgress(progress);
        });
      }
      await applySessionWithProgress(session, "logs");
      setLoadingLineDetailId(null);
      if (!session.lines.length) {
        setWorkspaceError("No readable log files were found in the selected browser files.");
      } else if (localSkipped.length) {
        setWorkspaceError(`Parsed locally. Skipped ${localSkipped.length} item${localSkipped.length === 1 ? "" : "s"}: ${localSkipped[0].name} (${localSkipped[0].reason})`);
      } else if (skipped.length) {
        setWorkspaceError(`Parsed readable files. Skipped ${skipped.length} dropped item${skipped.length === 1 ? "" : "s"} that the browser could not open.`);
      }
    } catch (error) {
      setWorkspaceError(error instanceof Error ? error.message : `Browser ${localOnlyMode ? "local parse" : "upload"} failed.`);
    } finally {
      setWorkspaceBusy("");
      setWorkspaceProgress(null);
    }
  }

  async function parseBrowserFiles(fileList: FileList | File[]) {
    const files = normalizeBrowserUploadFiles(fileList);
    if (workspaceBusy || !autoParseInputsRef.current) {
      queueBrowserFiles(files);
      return;
    }
    await parseBrowserUploadFiles(files);
  }

  function queueBrowserFiles(files: BrowserUploadFile[], skipped: string[] = []) {
    if (!files.length && !skipped.length) {
      return;
    }
    setWorkspaceError("");
    if (files.length) {
      setQueuedBrowserFiles((current) => {
        const seen = new Set(current.map((entry) => entry.relativePath || entry.file.name));
        const additions = files.filter((entry) => !seen.has(entry.relativePath || entry.file.name));
        return current.concat(additions);
      });
    }
    if (skipped.length) {
      setQueuedBrowserSkipped((current) => Array.from(new Set([...current, ...skipped])));
    }
  }

  function clearQueuedBrowserFiles() {
    setQueuedBrowserFiles([]);
    setQueuedBrowserSkipped([]);
  }

  async function startQueuedBrowserParse() {
    if (workspaceBusy || !queuedBrowserFiles.length) {
      return;
    }
    const filesToParse = queuedBrowserFiles;
    const skippedToReport = queuedBrowserSkipped;
    setQueuedBrowserFiles([]);
    setQueuedBrowserSkipped([]);
    try {
      await parseBrowserUploadFiles(filesToParse, skippedToReport);
    } catch {
      // parseBrowserUploadFiles surfaces its own errors via workspaceError.
    }
  }

  function normalizeInputPaths(paths: string[]): string[] {
    return Array.from(new Set(paths.filter(Boolean)));
  }

  function queueInputPaths(paths: string[]) {
    const nextPaths = normalizeInputPaths(paths);
    if (!nextPaths.length) {
      return;
    }
    setWorkspaceError("");
    setQueuedInputPaths((current) => normalizeInputPaths([...current, ...nextPaths]));
  }

  function clearQueuedInputs() {
    setQueuedInputPaths([]);
  }

  async function parseInputs(paths: string[], clearQueued = false) {
    const parsingPaths = normalizeInputPaths(paths);
    if (!parsingPaths.length) return;
    setWorkspaceError("");
    if (clearQueued) {
      setQueuedInputPaths((current) => current.filter((path) => !parsingPaths.includes(path)));
    }
    setWorkspaceBusy(`Parsing ${parsingPaths.length} input${parsingPaths.length === 1 ? "" : "s"}...`);
    setWorkspaceProgress(null);
    try {
      const session = await window.tmds.ingestDroppedPaths(parsingPaths);
      await applySessionWithProgress(session, "logs");
      setLoadingLineDetailId(null);
      if (!session.lines.length) {
        setWorkspaceError("No readable log files were found in the selected inputs.");
      }
    } catch (error) {
      if (clearQueued) {
        setQueuedInputPaths((current) => normalizeInputPaths([...parsingPaths, ...current]));
      }
      setWorkspaceError(error instanceof Error ? error.message : "Log ingestion failed.");
    } finally {
      setWorkspaceBusy("");
      setWorkspaceProgress(null);
    }
  }

  async function handleIncomingPaths(paths: string[]) {
    const nextPaths = normalizeInputPaths(paths);
    if (!nextPaths.length) {
      return;
    }
    if (workspaceBusy || !autoParseInputsRef.current) {
      queueInputPaths(nextPaths);
      return;
    }
    await parseInputs(nextPaths, false);
  }

  async function startQueuedParse() {
    if (!queuedInputPaths.length || workspaceBusy) {
      return;
    }
    await parseInputs(queuedInputPaths, true);
  }

  function resetWorkspaceToEmpty() {
    previousNonReferenceWorkspaceRef.current = null;
    const node = logListRef.current;
    if (node) {
      node.scrollTop = 0;
    }
    setLogListScrollTop(0);
    cancelFinderSearchRun();
    startTransition(() => {
      setLines([]);
      setLineDetails({});
      setSelected(null);
      setDetail(null);
      setFinderDraftQuery("");
      setSearch(defaultSearch);
      setActiveSource("all");
      setActiveTab("details");
      setReferenceSelections({});
      setWorkspaceError("");
      setWorkspaceBusy("");
      setWorkspaceProgress(null);
    });
  }

  function selectLine(line: ParsedLine) {
    selectedLineIdRef.current = line.id;
    setSelected(line);
    setDetail(lineDetails[line.id] ?? makeFallbackDetail(line));
    const selectedIndex = visible.findIndex((candidate) => candidate.id === line.id);
    if (selectedIndex >= 0) {
      requestWarmLineDetails(visible.slice(Math.max(0, selectedIndex - 80), Math.min(visible.length, selectedIndex + 81)));
    } else {
      requestWarmLineDetails([line]);
    }
    void loadLineDetail(line);
  }

  function selectWorkflowRelatedLine(entry: WorkflowRelatedDetail) {
    const target = lines.find((line) => line.id === entry.lineId);
    if (!target) {
      return;
    }
    if (!workflowViewerMode && !referenceSession && activeSource !== "all" && getLogCategory(target) !== activeSource) {
      setActiveSource("all");
    }
    selectLine(target);
  }

  useEffect(() => {
    if (referenceSession || !selected) {
      return;
    }
    const node = logListRef.current;
    if (!node) {
      return;
    }
    const selectedIndex = visible.findIndex((line) => line.id === selected.id);
    if (selectedIndex < 0) {
      return;
    }
    const targetTop = selectedIndex * logRowHeight;
    const targetBottom = targetTop + logRowHeight;
    const viewportTop = node.scrollTop;
    const viewportBottom = viewportTop + node.clientHeight;
    if (targetTop >= viewportTop && targetBottom <= viewportBottom) {
      return;
    }
    const centeredTop = Math.max(0, targetTop - Math.max(0, Math.floor((node.clientHeight - logRowHeight) / 2)));
    node.scrollTop = centeredTop;
    setLogListScrollTop(centeredTop);
  }, [referenceSession, selected, visible]);

  async function loadLineDetail(line: ParsedLine) {
    if (referenceSession || lineDetails[line.id] || loadingLineDetailId === line.id) {
      return;
    }
    if (localOnlyMode) {
      return;
    }
    setLoadingLineDetailId(line.id);
    try {
      const loadedDetail = window.tmds?.getLineDetail
        ? await window.tmds.getLineDetail(line.id, currentSessionId ?? undefined)
        : await fetchJson<DetailModel | null>(buildApiPath(`/api/line-detail?id=${encodeURIComponent(line.id)}`, currentSessionId));
      if (!loadedDetail) {
        return;
      }
      startTransition(() => {
        setLineDetails((current) => (current[line.id] ? current : { ...current, [line.id]: loadedDetail }));
        if (selectedLineIdRef.current === line.id) {
          setDetail(loadedDetail);
        }
      });
    } catch (error) {
      if (selectedLineIdRef.current === line.id) {
        setWorkspaceError(error instanceof Error ? error.message : "Line detail load failed.");
      }
    } finally {
      setLoadingLineDetailId((current) => (current === line.id ? null : current));
    }
  }

  function toggleReferenceLine(line: ParsedLine) {
    if (selected?.id === line.id) {
      setSelected(null);
      setDetail(null);
      return;
    }
    selectLine(line);
  }

  function selectReferenceDockLine(line: ParsedLine) {
    setReferenceDockSelected(line);
    setReferenceDockDetail(referenceDockLineDetails[line.id] ?? makeFallbackDetail(line));
  }

  function closeReferenceDock() {
    startTransition(() => {
      setReferenceDockLines([]);
      setReferenceDockLineDetails({});
      setReferenceDockSelected(null);
      setReferenceDockDetail(null);
      setReferenceDockSelections({});
      setReferenceDockSearchQuery("");
      setReferenceDockActiveSource("MESSAGE EXCHANGE");
      setReferenceDockActiveTab("details");
    });
  }

  function closeReferenceLibrary() {
    const snapshot = previousNonReferenceWorkspaceRef.current;
    if (!snapshot) {
      resetWorkspaceToEmpty();
      return;
    }
    const restoredSelected = snapshot.selectedLineId
      ? snapshot.lines.find((line) => line.id === snapshot.selectedLineId) ?? null
      : null;
    const fallback = restoredSelected ?? snapshot.lines[0] ?? null;
    startTransition(() => {
      setLines(snapshot.lines);
      setLineDetails(snapshot.lineDetails);
      setSelected(fallback);
      setDetail(fallback ? snapshot.lineDetails[fallback.id] ?? makeFallbackDetail(fallback) : null);
      setFinderDraftQuery(snapshot.search.query);
      setSearch(snapshot.search);
      setActiveSource(snapshot.activeSource);
      setActiveTab(snapshot.activeTab);
      setReferenceSelections(snapshot.referenceSelections);
      setWorkspaceError("");
      setWorkspaceBusy("");
      setWorkspaceProgress(null);
    });
  }

  useEffect(() => {
    if (referenceSession || !selected || workflowViewerMode) {
      return;
    }
    if (activeSource === "all") {
      return;
    }
    if (getLogCategory(selected) === activeSource) {
      return;
    }
    const fallback = visible[0] ?? sourceScopedLines[0] ?? null;
    if (fallback) {
      selectLine(fallback);
    }
  }, [activeSource, lineDetails, referenceSession, selected, sourceScopedLines, visible, workflowViewerMode]);

  useEffect(() => {
    if (referenceSession || !selected || workflowViewerMode) {
      return;
    }
    if (visible.some((line) => line.id === selected.id)) {
      return;
    }
    const fallback = visible[0] ?? null;
    if (fallback) {
      selectLine(fallback);
      return;
    }
    setSelected(null);
    setDetail(null);
  }, [referenceSession, selected, visible, workflowViewerMode]);

  useEffect(() => {
    if (!referenceSession || activeSource !== "MESSAGE EXCHANGE" || selected || sourceScopedLines.length !== 1) {
      return;
    }
    selectLine(sourceScopedLines[0]);
  }, [activeSource, referenceSession, selected, sourceScopedLines]);

  useEffect(() => {
    if (referenceSession || !visible.length) {
      return;
    }
    const timer = window.setTimeout(() => {
      requestWarmLineDetails(visible.slice(
        virtualLogWindow.start,
        Math.min(visible.length, virtualLogWindow.end + 120),
      ));
    }, 60);
    return () => window.clearTimeout(timer);
  }, [referenceSession, virtualLogWindow.end, virtualLogWindow.start, visible]);

  function getFinderSelectionText(): string {
    return window.getSelection?.()?.toString().replace(/\s+/g, " ").trim() ?? "";
  }

  function cancelFinderSearchRun() {
    finderSearchRunIdRef.current += 1;
    setFinderSearchRunning(false);
  }

  function getFinderMatchPattern(config: SearchConfig) {
    const error = getSearchPatternError(config);
    if (error) {
      setFinderError(error);
      return null;
    }
    setFinderError("");
    return buildMatchExpression(config);
  }

  function matchesFinderScopeLine(line: ParsedLine, pattern: RegExp | null) {
    return matchesWithPattern(line, pattern, referenceSession ? lineDetails[line.id] ?? null : null);
  }

  function findFirstMatchingLine(config: SearchConfig): ParsedLine | null {
    const pattern = getFinderMatchPattern(config);
    if (!pattern) {
      return null;
    }
    return timeScopedLines.find((line) => matchesFinderScopeLine(line, pattern)) ?? null;
  }

  function findAdjacentMatchingLine(direction: 1 | -1, config: SearchConfig): ParsedLine | null {
    const pattern = getFinderMatchPattern(config);
    if (!pattern || !timeScopedLines.length) {
      return null;
    }
    const total = timeScopedLines.length;
    const selectedIndex = selected ? timeScopedLines.findIndex((line) => line.id === selected.id) : -1;
    let index = selectedIndex >= 0 ? selectedIndex : (direction > 0 ? -1 : 0);
    for (let count = 0; count < total; count += 1) {
      index = (index + direction + total) % total;
      const line = timeScopedLines[index];
      if (matchesFinderScopeLine(line, pattern)) {
        return line;
      }
    }
    return null;
  }

  function applyFinderSearch(): SearchConfig | null {
    const nextQuery = search.regex ? finderDraftQuery.trim() : finderDraftQuery.replace(/\s+/g, " ").trim();
    if (finderDraftQuery !== nextQuery) {
      setFinderDraftQuery(nextQuery);
    }
    cancelFinderSearchRun();
    setFinderResults([]);
    setFinderResultsScrollTop(0);
    setFinderShowResults(false);
    setFinderError("");
    setSearch((state) => {
      const nextState = { ...state, query: nextQuery };
      return state.query === nextQuery ? state : nextState;
    });
    return nextQuery.length > 0 ? { ...search, query: nextQuery } : null;
  }

  function openFinder(seedFromSelection = false) {
    if (seedFromSelection) {
      const text = getFinderSelectionText();
      if (text.length >= 2) {
        setFinderDraftQuery(text);
        setFinderResults([]);
        setFinderError("");
        setFinderResultsScrollTop(0);
      }
    } else {
      setFinderDraftQuery(search.query);
    }
    setFinderOpen(true);
  }

  function closeFinder() {
    cancelFinderSearchRun();
    setFinderOpen(false);
    setFinderShowResults(false);
    setFinderResults([]);
    setFinderError("");
    setFinderResultsScrollTop(0);
  }

  function runFinderFind() {
    const nextSearch = applyFinderSearch();
    if (!nextSearch) {
      return;
    }
    const line = findFirstMatchingLine(nextSearch);
    if (line) {
      selectLine(line);
    }
  }

  function runFinderNavigation(direction: 1 | -1) {
    const nextSearch = applyFinderSearch();
    if (!nextSearch) {
      return;
    }
    const line = findAdjacentMatchingLine(direction, nextSearch);
    if (line) {
      selectLine(line);
    }
  }

  async function runFinderFindAll() {
    const nextSearch = applyFinderSearch();
    if (!nextSearch) {
      return;
    }
    const pattern = getFinderMatchPattern(nextSearch);
    if (!pattern) {
      return;
    }
    const runId = finderSearchRunIdRef.current + 1;
    finderSearchRunIdRef.current = runId;
    setFinderShowResults(true);
    setFinderSearchRunning(true);
    setFinderResults([]);
    setFinderResultsScrollTop(0);
    const nextResults: ParsedLine[] = [];
    let lastPublishedCount = 0;
    for (let start = 0; start < timeScopedLines.length; start += 1000) {
      if (finderSearchRunIdRef.current !== runId) {
        return;
      }
      const end = Math.min(timeScopedLines.length, start + 1000);
      for (let index = start; index < end; index += 1) {
        const line = timeScopedLines[index];
        if (!matchesFinderScopeLine(line, pattern)) {
          continue;
        }
        nextResults.push(line);
      }
      const shouldPublish = start === 0 || (nextResults.length - lastPublishedCount) >= 5000 || end === timeScopedLines.length;
      if (shouldPublish) {
        lastPublishedCount = nextResults.length;
        startTransition(() => {
          if (finderSearchRunIdRef.current !== runId) {
            return;
          }
          setFinderResults([...nextResults]);
        });
      }
      await waitForNextPaint();
    }
    if (finderSearchRunIdRef.current !== runId) {
      return;
    }
    startTransition(() => {
      setFinderResults(nextResults);
      setFinderSearchRunning(false);
    });
  }

  function clearTimeWindow() {
    setTimeWindowStartDateInput(defaultTimeWindowStartDate);
    setTimeWindowStartInput("");
    setTimeWindowEndDateInput(defaultTimeWindowEndDate);
    setTimeWindowEndInput("");
  }

  function formatLineTimestampForModes(
    line: ParsedLine,
    sourceMode: LogTimeSourceMode,
    displayMode: LogTimeDisplayMode,
  ): string | null {
    if (!line.timestamp) {
      return null;
    }
    if (displayMode === "source" && sourceMode === "original") {
      return line.timestamp;
    }
    const epoch = getLineTimestampMs(line, sourceMode);
    if (epoch === null) {
      return line.timestamp;
    }
    const resolvedDisplayMode = displayMode === "source"
      ? (sourceMode === "original" ? "original" : sourceMode)
      : displayMode;
    return formatEpochForDisplay(epoch, resolvedDisplayMode);
  }

  function formatLineTimestamp(line: ParsedLine): string | null {
    return formatLineTimestampForModes(line, logTimeSourceMode, logTimeDisplayMode);
  }

  function getViewerLineText(line: ParsedLine): string {
    const stripped = stripLeadingViewerTimestamp(line.raw);
    return stripped.length ? stripped : line.raw;
  }

  async function onDrop(event: DragEvent<HTMLDivElement>) {
    event.preventDefault();
    dragDepthRef.current = 0;
    setDragging(false);
    if (webAppMode) {
      const dropped = await collectDroppedBrowserFiles(event.dataTransfer);
      if (workspaceBusy || !autoParseInputsRef.current) {
        queueBrowserFiles(dropped.files, dropped.skipped);
        return;
      }
      await parseBrowserUploadFiles(dropped.files, dropped.skipped);
      return;
    }
    const droppedFiles = Array.from(event.dataTransfer.files);
    const paths = droppedFiles
      .map((file) => {
        const resolvedPath = window.tmds?.getPathForDroppedFile?.(file) ?? "";
        if (resolvedPath) {
          return resolvedPath;
        }
        return (file as File & { path?: string }).path ?? "";
      })
      .filter((value): value is string => Boolean(value));
    if (paths.length) {
      await handleIncomingPaths(paths);
    }
  }

  function clearSearch() {
    cancelFinderSearchRun();
    setFinderDraftQuery("");
    setFinderResults([]);
    setFinderError("");
    setFinderResultsScrollTop(0);
    setFinderShowResults(false);
    setSearch(defaultSearch);
  }

  const trainMessageFlowMode = referenceSession && activeSource === "TRAIN MESSAGES";
  const genisysInlineMode = referenceSession && activeSource === "GENISYS";
  const selectedLabel = referenceSession
    ? activeSource ? activeSource : "Select a reference category"
    : workflowViewerMode
      ? selected ? `Workflow for line ${selected.lineNumber}` : "Workflow lines"
      : selected ? `Line ${selected.lineNumber}` : "No selection";
  const referenceChipGridMode = isReferenceChipGridSource(activeSource, referenceSession);
  const referenceGroupedMode = isReferenceGroupedSource(activeSource, referenceSession);
  const searchPlaceholder = referenceSession ? "Search reference library" : "Type a word or phrase";
  const filterOnlyLabel = referenceSession ? "Only matching entries" : "Only matching lines";
  const selectedTimestampLabel = referenceSession
    ? (activeSource === "all" ? "Reference library" : activeSource)
    : selected
      ? formatLineTimestamp(selected) ?? "No timestamp"
      : "Awaiting selection";
  const viewerEmptyCopy = sourceScopedLines.length
      ? hasAppliedTimeWindow && !timeScopedLines.length
        ? `No lines fall inside the current time window (${timeFilterSummary}).`
        : (referenceSession ? "No entries match the current search." : "No lines match the current search.")
      : referenceSession && !activeSource
        ? "Select a reference category."
      : activeSource === "all"
        ? referenceSession
        ? "Open a reference section or load parsed logs."
        : (queuedInputPaths.length || queuedBrowserFiles.length)
          ? "Inputs are queued. Click Start parsing when ready."
          : "Add files or folders, load the reference library, or reload the review logs."
      : `No ${activeSource} lines in the current session.`;
  const totalQueued = queuedInputPaths.length + queuedBrowserFiles.length;
  const queuedBrowserText = queuedBrowserFiles.length ? summarizeBrowserUpload(queuedBrowserFiles) : "";
  const queuedPathText = queuedInputPaths.length === 1 ? "1 input" : `${queuedInputPaths.length} inputs`;
  const queuedStatusText = queuedBrowserText || queuedPathText;
  const queuedStatusTitle = [
    ...queuedInputPaths.map((path) => getSourceLabel(path)),
    ...queuedBrowserFiles.map((entry) => entry.relativePath || entry.file.name),
  ].join("\n");
  const busyStatusText = workspaceBusy && totalQueued ? `${workspaceBusy} | ${queuedStatusText}` : workspaceBusy;
  const busyStatusTitle = workspaceBusy && queuedStatusTitle ? `${workspaceBusy}\n\nQueued:\n${queuedStatusTitle}` : workspaceBusy;
  const showProgressPanel = Boolean(workspaceBusy && workspaceProgress);
  const workspaceStatus = workspaceBusy && !showProgressPanel
    ? { className: "status-chip accent", text: busyStatusText, title: busyStatusTitle }
    : workspaceError
      ? { className: "status-chip error", text: workspaceError, title: workspaceError }
      : null;
  const activeTabContent = buildReferenceTabDisplayText(
    detail,
    activeTab,
    tabs.find((tab) => tab.key === activeTab)?.content ?? "",
    referenceSelections,
    activeSearch,
  );
  const assignmentsTabActive = !referenceSession && !!detail && activeTab === "tmds" && getContextTabLabel(detail) === "Assignments & Assets";
  const blankAssignmentsPresent = assignmentsTabActive && hasBlankAssignments(detail);
  const inspectorTabContent = !showBlankAssignments && blankAssignmentsPresent
    ? filterBlankAssignments(activeTabContent)
    : activeTabContent;
  const workflowTabActive = !referenceSession && activeTab === "workflow";
  const referenceDiagramMode = referenceSession && activeSource === "MESSAGE EXCHANGE";
  const showPrimaryInspector = !(trainMessageFlowMode || genisysInlineMode || referenceDiagramMode);
  const workspaceClassName = [
    "workspace",
    showPrimaryInspector && !referenceDiagramMode ? "" : "reference-workspace",
    referenceDockOpen ? (showPrimaryInspector ? "reference-dock-layout" : "reference-dock-layout-compact") : "",
  ].filter(Boolean).join(" ");
  const referenceGroupedSections = useMemo(() => {
    if (!referenceGroupedMode) {
      return [];
    }

    const sections = new Map<string, ParsedLine[]>();
    for (const line of visible) {
      const title = activeSource === "CODELINES & STATIONS"
        ? "Subdivision Groups"
        : getReferenceSectionCategory(activeSource, line) || "Reference Groups";
      const bucket = sections.get(title) ?? [];
      bucket.push(line);
      sections.set(title, bucket);
    }

    return Array.from(sections.entries()).map(([title, entries]) => ({ title, entries }));
  }, [activeSource, referenceGroupedMode, visible]);
  const trainMessageFlowGroups = useMemo(() => (
    trainMessageFlowMode ? buildTrainMessageFlowGroups(visible, lineDetails) : []
  ), [trainMessageFlowMode, visible, lineDetails]);

  return (
    <div
      className={`shell workspace-shell ${dragging ? "dragging" : ""}`}
      onDragEnter={(event) => {
        event.preventDefault();
        dragDepthRef.current += 1;
        setDragging(true);
      }}
      onDragOver={(event) => {
        event.preventDefault();
        if (!dragging) {
          setDragging(true);
        }
      }}
      onDragLeave={(event) => {
        event.preventDefault();
        dragDepthRef.current = Math.max(0, dragDepthRef.current - 1);
        if (dragDepthRef.current === 0) {
          setDragging(false);
        }
      }}
      onDrop={onDrop}
    >
      <header className="topbar">
        <div className="topbar-head">
          <div className="topbar-title-block">
            <h1 className={referenceWindowMode ? "topbar-title" : "topbar-title topbar-title-workspace"}>
              {referenceWindowMode ? "TMDS Reference Library" : "Log Analyzer"}
            </h1>
            <p className={referenceWindowMode ? "topbar-subtitle" : "topbar-subtitle topbar-subtitle-workspace"}>
              {referenceWindowMode
                ? "Reference window for message-exchange, train-message, Genisys, and code-line guidance."
                : "Open TMDS logs from files, ZIP archives, and GZ-compressed sources."}
            </p>
          </div>
          {workspaceStatus ? (
            <div className="workspace-status" aria-live="polite">
              <span className={workspaceStatus.className} title={workspaceStatus.title}>{workspaceStatus.text}</span>
            </div>
          ) : null}
          {authState && authState.authenticated ? (
            <AuthChip
              authState={authState}
              onLogout={onLogout}
              onOpenAdmin={onOpenAdmin}
              onOpenAccount={onOpenAccount}
            />
          ) : null}
        </div>

        <div className="toolbar">
          {referenceWindowMode ? null : (
            <div className="toolbar-group">
              <input
                ref={browserFileInputRef}
                className="file-picker-input"
                type="file"
                multiple
                onChange={(event) => {
                  const files = event.currentTarget.files;
                  event.currentTarget.value = "";
                  if (files) void parseBrowserFiles(files);
                }}
              />
              <input
                ref={browserFolderInputRef}
                className="file-picker-input"
                type="file"
                multiple
                onChange={(event) => {
                  const files = event.currentTarget.files;
                  event.currentTarget.value = "";
                  if (files) void parseBrowserFiles(files);
                }}
              />
              {webAppMode ? (
                <div className="joined-input-actions" role="group" aria-label="Add local inputs">
                  <button
                    className="primary joined-input-action joined-input-action-left"
                    onClick={openBrowserFilePicker}
                    disabled={Boolean(workspaceBusy)}
                    title="Pick one or more files (logs, ZIP, GZ). Drag/drop also works."
                  >
                    Add files
                  </button>
                  <button
                    className="primary joined-input-action joined-input-action-right"
                    onClick={openBrowserFolderPicker}
                    disabled={Boolean(workspaceBusy)}
                    title="Pick an entire folder. The browser will ask you to confirm the upload."
                  >
                    Add folder
                  </button>
                </div>
              ) : (
                <button
                  className="primary"
                  onClick={() => void openFiles()}
                  disabled={Boolean(workspaceBusy)}
                  title="Choose files, folders, ZIP, or GZ sources."
                >
                  Add inputs
                </button>
              )}
              {webAppMode && queuedBrowserFiles.length ? (
                <span className="queued-inline" aria-live="polite" title={queuedStatusTitle}>
                  {summarizeBrowserUpload(queuedBrowserFiles)} queued
                </span>
              ) : null}
              {!webAppMode && queuedInputPaths.length ? (
                <span className="queued-inline" aria-live="polite" title={queuedStatusTitle}>
                  {queuedInputPaths.length === 1 ? "1 input" : `${queuedInputPaths.length} inputs`} queued
                </span>
              ) : null}
            </div>
          )}
          {referenceWindowMode ? null : (
            <div className="toolbar-group toolbar-group-parse">
              {webAppMode ? (
                <>
                  <button
                    className={`parse-cta primary ${queuedBrowserFiles.length ? "parse-cta-ready" : ""}`}
                    onClick={() => void startQueuedBrowserParse()}
                    disabled={Boolean(workspaceBusy) || !queuedBrowserFiles.length}
                  >
                    {queuedBrowserFiles.length ? `Start parsing (${summarizeBrowserUpload(queuedBrowserFiles)})` : "Start parsing"}
                  </button>
                  <button
                    className="ghost"
                    onClick={clearQueuedBrowserFiles}
                    disabled={Boolean(workspaceBusy) || (!queuedBrowserFiles.length && !queuedBrowserSkipped.length)}
                  >
                    Clear queued
                  </button>
                  <button
                    className="ghost"
                    onClick={resetWorkspaceToEmpty}
                    disabled={Boolean(workspaceBusy) || (!lines.length && !selected)}
                    title="Clear the currently loaded parsed log view."
                  >
                    Clear logs
                  </button>
                  <label className="toolbar-check">
                    <input
                      type="checkbox"
                      defaultChecked={autoParseInputsRef.current}
                      onChange={(e) => { autoParseInputsRef.current = e.target.checked; }}
                    /> Auto parse
                  </label>
                </>
              ) : (
                <>
                  <button
                    className={`parse-cta primary ${queuedInputPaths.length ? "parse-cta-ready" : ""}`}
                    onClick={() => void startQueuedParse()}
                    disabled={Boolean(workspaceBusy) || !queuedInputPaths.length}
                  >
                    {queuedInputPaths.length ? `Start parsing (${queuedInputPaths.length === 1 ? "1 input" : `${queuedInputPaths.length} inputs`})` : "Start parsing"}
                  </button>
                  <button className="ghost" onClick={clearQueuedInputs} disabled={Boolean(workspaceBusy) || !queuedInputPaths.length}>Clear queued</button>
                  <button className="ghost" onClick={resetWorkspaceToEmpty} disabled={Boolean(workspaceBusy) || (!lines.length && !selected)} title="Clear the currently loaded parsed log view.">Clear logs</button>
                  <label className="toolbar-check">
                    <input
                      type="checkbox"
                      defaultChecked={autoParseInputsRef.current}
                      onChange={(e) => { autoParseInputsRef.current = e.target.checked; }}
                    /> Auto parse
                  </label>
                </>
              )}
            </div>
          )}
          <div className="toolbar-group">
            {referenceSession && !referenceWindowMode
              ? (
                <button
                  className="ghost"
                  onClick={closeReferenceLibrary}
                  disabled={Boolean(workspaceBusy)}
                >
                  Close reference library
                </button>
              ) : referenceDockOpen ? (
                <button className="ghost" onClick={closeReferenceDock} disabled={Boolean(workspaceBusy)}>Close reference library</button>
              ) : (
                <button
                  className="ghost"
                  onClick={() => void loadFoundationSession()}
                  disabled={Boolean(workspaceBusy)}
                  title={localOnlyMode ? "Open the bundled static reference library available on GitHub Pages." : undefined}
                >
                  {referenceWindowMode ? "Reload reference library" : "Open reference library"}
                </button>
              )}
            {referenceWindowMode ? null : (
              <button
                className="ghost"
                onClick={() => void loadReviewSampleSession()}
                disabled={Boolean(workspaceBusy)}
                title={localOnlyMode ? "Load the review sample logs bundled into the static GitHub build." : undefined}
              >
                Reload review logs
              </button>
            )}
          </div>
          <div className="toolbar-group toolbar-group-finder">
            <button className="ghost" onClick={() => openFinder(true)}>Open finder</button>
          </div>
        </div>

        {localOnlyMode && showLocalModeBanner ? (
          <div className="local-mode-banner" role="note">
            <strong>Static GitHub mode</strong> Files are parsed in your browser. Line list, finder, ZIP/GZ archives, bundled reference/review data, and ported per-line details work without the home server. Some advanced server-only enrichment may still fall back to static detail.
            {serverReachable && onReconnect ? (
              <>
                {" "}
                <button type="button" className="ghost compact local-mode-reconnect" onClick={onReconnect}>
                  Server is back — reconnect now
                </button>
              </>
            ) : null}
          </div>
        ) : null}

        {updateAvailable ? (
          <div className="update-banner" role="note">
            <strong>Update available</strong> — a newer version of Log Analyzer was published.
            {onApplyUpdate ? (
              <>
                {" "}
                <button type="button" className="primary compact update-banner-apply" onClick={onApplyUpdate}>
                  Update now
                </button>
              </>
            ) : null}
            {onDismissUpdate ? (
              <>
                {" "}
                <button type="button" className="ghost compact" onClick={onDismissUpdate}>
                  Later
                </button>
              </>
            ) : null}
          </div>
        ) : null}

        {workspaceBusy && workspaceProgress ? (
          <div className="progress-panel" aria-live="polite">
            <div className="progress-panel-head">
              <div className="progress-panel-copy">
                <div className="progress-phase">{getProgressPhaseLabel(workspaceProgress)}</div>
                <div className="progress-label">{getProgressLabel(workspaceProgress)}</div>
                <div className="progress-source">
                  {workspaceProgress.currentPath
                    ? workspaceProgress.currentPath
                    : workspaceProgress.phase === "detail"
                      ? "Working through the upload on the server. The page is responsive — sit tight."
                      : "Waiting on next source..."}
                </div>
              </div>
              <div className="progress-percent">
                {workspaceProgress.percent}%
                {workspaceBusy && busyHeartbeatSeconds > 0
                  ? <div className="progress-elapsed">{busyHeartbeatSeconds}s elapsed</div>
                  : null}
              </div>
            </div>
            <div className={
              ((workspaceProgress.percent >= 99 && workspaceProgress.percent < 100) || workspaceProgress.phase === "detail") && workspaceBusy
                ? "progress-track import-progress indeterminate"
                : "progress-track import-progress"
            }>
              <div className="progress-fill" style={{ width: `${workspaceProgress.percent}%` }} />
            </div>
          </div>
        ) : null}

        {finderOpen ? (
          <section className="finder-panel" aria-label="Finder">
            <div className="finder-panel-head">
              <div>
                <h2>Finder</h2>
                <p>{finderPanelSubtitle}</p>
              </div>
              <button className="ghost compact" type="button" onClick={closeFinder}>Close</button>
            </div>
            <div className="finder-panel-grid">
              <label className="finder-query-field">
                <span>Search</span>
                <input
                  ref={finderInputRef}
                  value={finderDraftQuery}
                  onChange={(event) => setFinderDraftQuery(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      event.preventDefault();
                      runFinderNavigation(event.shiftKey ? -1 : 1);
                    }
                  }}
                  placeholder={searchPlaceholder}
                />
              </label>
              <div className="finder-action-row">
                <button className="primary" type="button" onClick={runFinderFind} disabled={!finderHasQuery}>Find</button>
                <button className="ghost" type="button" onClick={() => runFinderNavigation(1)} disabled={!finderHasQuery}>Find next</button>
                <button className="ghost" type="button" onClick={() => runFinderNavigation(-1)} disabled={!finderHasQuery}>Find previous</button>
                <button className="ghost" type="button" onClick={() => void runFinderFindAll()} disabled={!finderHasQuery || finderSearchRunning}>Find all</button>
                <button className="ghost" type="button" onClick={clearSearch} disabled={!finderHasQuery && !search.filterOnlyMatches && !search.wholeWord && !search.regex && !search.caseSensitive}>Clear</button>
              </div>
            </div>
            <div className="finder-option-row">
              <label className="toolbar-check">
                <input
                  type="checkbox"
                  checked={search.wholeWord}
                  onChange={(event) => setSearch((state) => ({ ...state, wholeWord: event.target.checked, regex: event.target.checked ? false : state.regex }))}
                />
                Exact word match
              </label>
              <label className="toolbar-check">
                <input
                  type="checkbox"
                  checked={search.regex}
                  onChange={(event) => setSearch((state) => ({ ...state, regex: event.target.checked, wholeWord: event.target.checked ? false : state.wholeWord }))}
                />
                Regular expression
              </label>
              <label className="toolbar-check">
                <input
                  type="checkbox"
                  checked={search.caseSensitive}
                  onChange={(event) => setSearch((state) => ({ ...state, caseSensitive: event.target.checked }))}
                />
                Match case
              </label>
              <label className="toolbar-check">
                <input
                  type="checkbox"
                  checked={search.filterOnlyMatches}
                  onChange={(event) => setSearch((state) => ({ ...state, filterOnlyMatches: event.target.checked }))}
                />
                {filterOnlyLabel}
              </label>
            </div>
            {finderError ? (
              <div className="finder-error" role="alert">
                Invalid regex: {finderError}
              </div>
            ) : null}
            {finderShowResults ? (
              <div className="finder-results-panel">
                <div className="finder-results-head">
                  <strong>Results</strong>
                  <span>
                    {finderSearchRunning
                      ? `${finderResults.length.toLocaleString()} matching ${referenceSession ? "entries" : "lines"} found so far...`
                      : finderResults.length
                      ? `${finderResults.length.toLocaleString()} matching ${referenceSession ? "entries" : "lines"}`
                      : `No matching ${referenceSession ? "entries" : "lines"}`}
                  </span>
                </div>
                <div
                  ref={finderResultsListRef}
                  className="finder-results-list"
                  onScroll={(event) => setFinderResultsScrollTop(event.currentTarget.scrollTop)}
                >
                  {finderResults.length ? (
                    <>
                      {virtualFinderResults.topPadding ? <div style={{ height: `${virtualFinderResults.topPadding}px` }} aria-hidden="true" /> : null}
                      {virtualFinderResults.lines.map((line) => {
                        const referenceBubble = referenceSession ? getReferenceBubbleParts(line, lineDetails[line.id] ?? null) : null;
                        const primaryText = referenceBubble ? referenceBubble.primary : line.raw;
                        const secondaryText = referenceBubble?.secondary ?? "";
                        return (
                          <button
                            key={`finder:${line.id}`}
                            type="button"
                            className={selected?.id === line.id ? "finder-result-card selected" : "finder-result-card"}
                            onClick={() => selectLine(line)}
                          >
                            <span className="finder-result-line-number">{referenceSession ? `Entry ${line.lineNumber}` : `Line ${line.lineNumber}`}</span>
                            <span className="finder-result-source">{getSourceLabel(line.source)}</span>
                            <span className="finder-result-primary">{renderHighlightedText(primaryText, activeSearch)}</span>
                            {secondaryText ? <span className="finder-result-secondary">{renderHighlightedText(secondaryText, activeSearch)}</span> : null}
                          </button>
                        );
                      })}
                      {virtualFinderResults.bottomPadding ? <div style={{ height: `${virtualFinderResults.bottomPadding}px` }} aria-hidden="true" /> : null}
                    </>
                  ) : (
                    <div className="finder-results-empty">No matching {referenceSession ? "entries" : "lines"} in the current view.</div>
                  )}
                </div>
              </div>
            ) : null}
          </section>
        ) : null}

      </header>

      {dragging && !referenceWindowMode ? (
        <div className="drag-overlay" aria-hidden="true">
          <div className="drag-overlay-card">
            <div className="drag-overlay-badge">Drop To Queue</div>
            <strong>Drop TMDS logs, folders, ZIP, or GZ sources</strong>
            <span>They will queue here for parsing with the current workspace actions.</span>
          </div>
        </div>
      ) : null}

      <div className="workspace-stack">
        {!referenceSession ? (
          <div className="time-inline-bar">
            <div className="time-inline-head">
              <strong>Time Window Search</strong>
              <div className="time-inline-pills">
                <span className="time-inline-pill">{timeFilterSummary}</span>
              </div>
            </div>
            <div className="time-inline-controls">
              <div className="time-range-group">
                <span className="time-range-label">From</span>
                <div className="time-range-fields">
                  <label className="time-field">
                    <span>Date</span>
                    <input
                      type="text"
                      inputMode="numeric"
                      placeholder={defaultTimeWindowStartDate || "YYYY-MM-DD"}
                      value={timeWindowStartDateInput}
                      onChange={(event) => setTimeWindowStartDateInput(normalizeDateInput(event.target.value))}
                      maxLength={10}
                      disabled={!defaultTimeWindowStartDate}
                    />
                  </label>
                  <label className="time-field">
                    <span>Time</span>
                    <input
                      type="text"
                      inputMode="numeric"
                      placeholder="00:00[:00]"
                      value={timeWindowStartInput}
                      onChange={(event) => setTimeWindowStartInput(normalizeTimeInput(event.target.value))}
                      onBlur={(event) => setTimeWindowStartInput(finalizeTimeInput(event.target.value))}
                      onKeyDown={(event) => {
                        if (event.key === "Enter") {
                          setTimeWindowStartInput(finalizeTimeInput(event.currentTarget.value));
                        }
                      }}
                      maxLength={8}
                      disabled={!defaultTimeWindowStartDate}
                    />
                  </label>
                </div>
              </div>
              <div className="time-range-group">
                <span className="time-range-label">To</span>
                <div className="time-range-fields">
                  <label className="time-field">
                    <span>Date</span>
                    <input
                      type="text"
                      inputMode="numeric"
                      placeholder={defaultTimeWindowEndDate || "YYYY-MM-DD"}
                      value={timeWindowEndDateInput}
                      onChange={(event) => setTimeWindowEndDateInput(normalizeDateInput(event.target.value))}
                      maxLength={10}
                      disabled={!defaultTimeWindowEndDate}
                    />
                  </label>
                  <label className="time-field">
                    <span>Time</span>
                    <input
                      type="text"
                      inputMode="numeric"
                      placeholder="23:59[:59]"
                      value={timeWindowEndInput}
                      onChange={(event) => setTimeWindowEndInput(normalizeTimeInput(event.target.value))}
                      onBlur={(event) => setTimeWindowEndInput(finalizeTimeInput(event.target.value))}
                      onKeyDown={(event) => {
                        if (event.key === "Enter") {
                          setTimeWindowEndInput(finalizeTimeInput(event.currentTarget.value));
                        }
                      }}
                      maxLength={8}
                      disabled={!defaultTimeWindowEndDate}
                    />
                  </label>
                </div>
              </div>
              <div className="time-inline-actions">
                <button className="ghost" type="button" onClick={clearTimeWindow} disabled={!hasTimeWindowDraft}>Clear time window</button>
                <button
                  className="ghost"
                  type="button"
                  onClick={() => {
                    if (window.tmds?.openTimeConvertTool) {
                      void window.tmds.openTimeConvertTool();
                    } else {
                      window.open("https://savvytime.com/converter/pst-to-utc", "_blank", "noopener,noreferrer");
                    }
                  }}
                >
                  Time convert
                </button>
              </div>
            </div>
          </div>
        ) : null}

        <main className={workspaceClassName}>
          <section className="viewer">
          <div className={referenceSession ? "viewer-header reference-viewer-header" : "viewer-header"}>
            <span>{referenceSession ? `${sourceScopedLines.length} entries` : workflowViewerMode ? `${visible.length} workflow lines` : `${visible.length} lines`}</span>
            <span>{selectedLabel}</span>
          </div>
          {referenceSession ? (
            <div className="tab-strip source-strip" role="tablist" aria-label="Log sources">
              {sourceTabs.map((tab) => (
                <button
                  key={tab.key}
                  className={tab.count === 0 && tab.key !== "all" ? "tab disabled" : activeSource === tab.key ? "tab active" : "tab"}
                  onClick={() => {
                    setActiveSource(tab.key);
                    setSelected(null);
                    setDetail(null);
                  }}
                  title={tab.key === "all" ? "All reference sections" : tab.key}
                  disabled={tab.key !== "all" && tab.count === 0}
                >
                  {tab.label} ({tab.count})
                </button>
              ))}
            </div>
          ) : null}
          {referenceDiagramMode ? (
            <div className="reference-diagram-page">
              {detail ? (
                <div className="card stack detail-card reference-detail-card diagram-page-card">
                  {renderReferenceDetailContent(detail, inspectorTabContent, activeSearch, referenceSelections, setReferenceSelections)}
                </div>
              ) : (
                <div className="card empty">{viewerEmptyCopy}</div>
              )}
            </div>
          ) : trainMessageFlowMode ? (
            <div className="train-message-groups" role="list">
              {trainMessageFlowGroups.length ? (
                trainMessageFlowGroups.map((group) => {
                  const groupFlows = Array.from(new Set(group.messages.map((message) => message.flow).filter(Boolean)));
                  const messageById = new Map(group.messages.map((message) => [message.id, message]));
                  return (
                    <section key={group.id} className="train-message-group-card">
                      <div className="train-message-group-head">
                        <span className="reference-badge">{`${group.messages.length} related message${group.messages.length === 1 ? "" : "s"}`}</span>
                        {groupFlows.map((flow) => (
                          <span key={`${group.id}:${flow}`} className="reference-badge train-flow-badge">{flow}</span>
                        ))}
                      </div>
                      <div className="train-message-group-members">
                        {group.messages.map((message) => (
                          <div key={message.id} className="train-message-member-chip">
                            <span className="train-message-related-id">{renderHighlightedText(message.id, activeSearch)}</span>
                            <span className="train-message-related-name">{renderHighlightedText(message.name, activeSearch)}</span>
                          </div>
                        ))}
                      </div>
                      {group.edges.length ? (
                        <div className="train-message-group-flows">
                          {group.edges.map((edge) => {
                            const fromMessage = messageById.get(edge.from);
                            const toMessage = messageById.get(edge.to);
                            if (!fromMessage || !toMessage) {
                              return null;
                            }
                            return (
                              <div key={`${group.id}:${edge.from}:${edge.to}:${edge.label}`} className="train-message-group-flow">
                                <span className="reference-badge train-message-link-label">{edge.label}</span>
                                <div className="train-message-flow-row">
                                  <span className="train-message-flow-node">
                                    <span className="train-message-related-id">{renderHighlightedText(fromMessage.id, activeSearch)}</span>
                                    <span className="train-message-related-name">{renderHighlightedText(fromMessage.name, activeSearch)}</span>
                                  </span>
                                  <span className="train-flow-arrow" aria-hidden="true">-&gt;</span>
                                  <span className="train-message-flow-node">
                                    <span className="train-message-related-id">{renderHighlightedText(toMessage.id, activeSearch)}</span>
                                    <span className="train-message-related-name">{renderHighlightedText(toMessage.name, activeSearch)}</span>
                                  </span>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      ) : (
                        <div className="train-flow-note">No grounded reply or confirmation pair was found in the local ICD text.</div>
                      )}
                    </section>
                  );
                })
              ) : (
                <div className="card empty">{viewerEmptyCopy}</div>
              )}
            </div>
          ) : genisysInlineMode ? (
            renderGenisysInlineCards(visible, lineDetails, search)
          ) : referenceChipGridMode ? (
            <div className="reference-chip-grid" role="list">
              {visible.length ? (
                visible.map((line) => {
                  const bubble = getReferenceBubbleParts(line, lineDetails[line.id] ?? null);
                  return (
                    <button
                      key={line.id}
                      type="button"
                      className={selected?.id === line.id ? "reference-chip-card selected" : "reference-chip-card"}
                      onClick={() => toggleReferenceLine(line)}
                    >
                      <span className="reference-chip-primary">{renderHighlightedText(bubble.primary, activeSearch)}</span>
                      {bubble.secondary ? <span className="reference-chip-secondary">{renderHighlightedText(bubble.secondary, activeSearch)}</span> : null}
                    </button>
                  );
                })
              ) : (
                <div className="card empty">{viewerEmptyCopy}</div>
              )}
            </div>
          ) : referenceGroupedMode ? (
            <div className={activeSource === "NETWORK" ? "reference-group-browser network-browser" : "reference-group-browser"}>
              {visible.length ? (
                referenceGroupedSections.map((section) => (
                  <section key={section.title} className="reference-group-section">
                    <div className="reference-group-heading">{section.title}</div>
                    <div className="reference-group-grid">
                      {section.entries.map((line) => {
                        const lineDetail = lineDetails[line.id] ?? makeFallbackDetail(line);
                        const visibleChoiceGroups = getVisibleReferenceChoiceGroups(lineDetail, search);
                        const bubble = getReferenceBubbleParts(line, lineDetail);
                        const showChoices = selected?.id === line.id && visibleChoiceGroups.some((group) => group.items.length > 0);
                        const expandGroupStack = showChoices && activeSource === "NETWORK";
                        return (
                          <div
                            key={line.id}
                            className={expandGroupStack ? "reference-group-stack expanded" : "reference-group-stack"}
                          >
                            <button
                              type="button"
                              className={selected?.id === line.id ? "reference-chip-card selected" : "reference-chip-card"}
                              onClick={() => toggleReferenceLine(line)}
                            >
                              <span className="reference-chip-primary">{renderHighlightedText(bubble.primary, activeSearch)}</span>
                              {bubble.secondary ? <span className="reference-chip-secondary">{renderHighlightedText(bubble.secondary, activeSearch)}</span> : null}
                            </button>
                            {showChoices ? (
                              <div className="reference-choice-panel viewer-choice-panel">
                                {visibleChoiceGroups.filter((group) => group.items.length > 0).map((group) => (
                                  <div key={group.id} className="reference-choice-group">
                                    {(() => {
                                      const selectionKey = getReferenceSelectionKey(lineDetail.lineId, group.id);
                                      const effectiveSelectedIds = getSelectedReferenceItemIds(lineDetail.lineId, group, referenceSelections);
                                      return (
                                        <>
                                          {group.label.trim().length ? <div className="reference-choice-label">{group.label}</div> : null}
                                          <div className={getReferenceChoiceListClass(group.layout)}>
                                            {(activeSource === "NETWORK"
                                              && group.id === "field-device-groups"
                                              && effectiveSelectedIds.length
                                              ? group.items.filter((item) => effectiveSelectedIds.includes(item.id))
                                              : group.items
                                            ).map((item) => {
                                              const isActive = effectiveSelectedIds.includes(item.id);
                                              return (
                                                <button
                                                  key={item.id}
                                                  type="button"
                                                  className={isActive ? "reference-choice active" : "reference-choice"}
                                                  onClick={(event) => {
                                                    selectLine(line);
                                                    setReferenceSelections((current) => {
                                                      const currentIds = getSelectedReferenceItemIds(lineDetail.lineId, group, current);
                                                      const nextIds = getNextReferenceSelectionIds(
                                                        group,
                                                        currentIds,
                                                        item.id,
                                                        event.ctrlKey || event.metaKey || event.shiftKey,
                                                      );
                                                      const removedItems = group.items.filter((candidate) =>
                                                        currentIds.includes(candidate.id) && !nextIds.includes(candidate.id),
                                                      );
                                                      return clearNestedReferenceSelections(lineDetail.lineId, removedItems, {
                                                        ...current,
                                                        [selectionKey]: nextIds,
                                                      });
                                                    });
                                                  }}
                                                >
                                                  {renderHighlightedText(item.label, activeSearch)}
                                                </button>
                                              );
                                            })}
                                          </div>
                                          {group.items
                                            .filter((item) => effectiveSelectedIds.includes(item.id))
                                            .map((item) => {
                                              const nestedContent = renderNestedReferenceChoiceSections(
                                                lineDetail,
                                                item,
                                                search,
                                                referenceSelections,
                                                setReferenceSelections,
                                              );
                                              if (!nestedContent) {
                                                return null;
                                              }
                                              return (
                                                <div key={`${group.id}:${item.id}:nested`} className="reference-choice-subsection">
                                                  {effectiveSelectedIds.length > 1 ? (
                                                    <div className="reference-choice-subheading">{renderHighlightedText(item.label, activeSearch)}</div>
                                                  ) : null}
                                                  {nestedContent}
                                                </div>
                                              );
                                            })}
                                        </>
                                      );
                                    })()}
                                  </div>
                                ))}
                              </div>
                            ) : null}
                          </div>
                        );
                      })}
                    </div>
                  </section>
                ))
              ) : (
                <div className="card empty">{viewerEmptyCopy}</div>
              )}
            </div>
          ) : (
            <div
              ref={logListRef}
              className="log-list"
              role="list"
              onScroll={(event) => setLogListScrollTop(event.currentTarget.scrollTop)}
            >
              {visible.length ? (
                <>
                  {virtualLogWindow.topPadding ? <div style={{ height: `${virtualLogWindow.topPadding}px` }} aria-hidden="true" /> : null}
                  {virtualLogWindow.lines.map((line) => (
                      <div
                        key={line.id}
                        role="button"
                        tabIndex={0}
                        className={`log-line ${selected?.id === line.id ? "selected" : ""}`}
                        onClick={() => selectLine(line)}
                        onKeyDown={(event) => {
                          if (event.key === "Enter" || event.key === " ") {
                            event.preventDefault();
                            selectLine(line);
                          }
                        }}
                      >
                        <span className="line-no">{line.lineNumber}</span>
                        <span className="line-text no-timestamp">
                          <span className="line-raw">{renderHighlightedText(line.raw, activeSearch)}</span>
                        </span>
                      </div>
                    ))}
                  {virtualLogWindow.bottomPadding ? <div style={{ height: `${virtualLogWindow.bottomPadding}px` }} aria-hidden="true" /> : null}
                </>
              ) : (
                <div className="card empty">{viewerEmptyCopy}</div>
              )}
            </div>
          )}
        </section>

        {showPrimaryInspector ? (
          <aside className="inspector">
            <div className="inspector-header">
              <h2>{referenceSession ? "Selected reference item" : "Selected line detail"}</h2>
              <span>{selectedTimestampLabel}</span>
            </div>
            <div className="inspector-body">
              {detail ? (
                <>
                  {!referenceSession && selected && loadingLineDetailId === selected.id ? (
                    <div className="status-chip accent detail-loading-chip">Loading full detail...</div>
                  ) : null}
                  {referenceSession ? null : (
                    <div className="card stack raw-card">
                      <div className="raw-card-head">
                        <span>Raw line</span>
                        <span>Line {detail.lineNumber}</span>
                      </div>
                      <pre>{renderHighlightedText(detail.raw, activeSearch)}</pre>
                    </div>
                  )}
                  {referenceSession || !detail.relatedPair ? null : (
                    <div className="card stack raw-card related-pair-card">
                      <div className="raw-card-head">
                        <span>Related pair</span>
                        <span>Line {detail.relatedPair.lineNumber}</span>
                      </div>
                      <div className="related-pair-meta">
                        <span className="status-chip related-pair-chip">{detail.relatedPair.relationLabel}</span>
                        <span>{detail.relatedPair.deltaLabel}</span>
                      </div>
                      <pre>{renderHighlightedText(detail.relatedPair.raw, activeSearch)}</pre>
                    </div>
                  )}
                  {referenceSession && activeSource !== "NETWORK" && detail.referenceBadges?.length ? (
                    <div className="reference-badge-strip">
                      {detail.referenceBadges.map((badge) => (
                        <span key={badge} className="reference-badge">{badge}</span>
                      ))}
                    </div>
                  ) : null}
                  <div className="tab-strip detail-tab-strip" role="tablist">
                    {tabs.map((tab) => (
                      <button
                        key={tab.key}
                        className={activeTab === tab.key ? "tab detail-tab active" : "tab detail-tab"}
                        onClick={() => setActiveTab(tab.key)}
                      >
                        {tab.label}
                      </button>
                    ))}
                  </div>
                  {blankAssignmentsPresent ? (
                    <div className="inspector-tab-tools">
                      <button
                        type="button"
                        className={showBlankAssignments ? "tab-tool-toggle active" : "tab-tool-toggle"}
                        onClick={() => setShowBlankAssignments((current) => !current)}
                      >
                        {showBlankAssignments ? "Hide blank bits" : "Show blank bits"}
                      </button>
                    </div>
                  ) : null}
                  <div className={referenceSession ? "card stack detail-card reference-detail-card" : "card stack detail-card"}>
                    {referenceSession
                      ? renderReferenceDetailContent(detail, inspectorTabContent, activeSearch, referenceSelections, setReferenceSelections)
                      : (
                        <>
                          {inspectorTabContent.trim().length
                            ? renderRuntimeStructuredCards(
                                workflowTabActive ? (workflowAnchorDetail ?? detail) : detail,
                                activeTab,
                                inspectorTabContent,
                                search,
                              )
                            : null}
                          {workflowTabActive ? renderWorkflowRelatedCards(workflowAnchorDetail ?? detail, search, selectWorkflowRelatedLine) : null}
                        </>
                      )}
                  </div>
                </>
              ) : (
                <div className="card empty">{referenceSession ? "Select a reference item to open the side inspector." : "Select a log line to open the side inspector."}</div>
              )}
            </div>
          </aside>
        ) : null}

        {referenceDockOpen ? (
          <aside className="reference-dock">
            <div className="reference-dock-header">
              <div className="reference-dock-title-block">
                <h2>Reference library</h2>
                <span>Docked beside the parsed logs for side-by-side review.</span>
              </div>
              <button className="ghost" onClick={closeReferenceDock} disabled={Boolean(workspaceBusy)}>Close</button>
            </div>
            <div className="reference-dock-toolbar">
              <label>
                Find
                <input
                  value={referenceDockSearchQuery}
                  onChange={(event) => setReferenceDockSearchQuery(event.target.value)}
                  placeholder="Search reference library"
                />
              </label>
            </div>
            <div className="tab-strip source-strip" role="tablist" aria-label="Reference sources">
              {referenceDockSourceTabs.map((tab) => (
                <button
                  key={tab.key}
                  className={referenceDockActiveSource === tab.key ? "tab active" : "tab"}
                  onClick={() => setReferenceDockActiveSource(tab.key)}
                >
                  {tab.label} ({tab.count})
                </button>
              ))}
            </div>
            <div className="reference-dock-body">
              <div className="reference-dock-list">
                {referenceDockVisible.length ? (
                  referenceDockVisible.map((line) => {
                    const bubble = getReferenceBubbleParts(line, referenceDockLineDetails[line.id] ?? null);
                    return (
                      <button
                        key={line.id}
                        type="button"
                        className={referenceDockSelected?.id === line.id ? "reference-chip-card selected" : "reference-chip-card"}
                        onClick={() => selectReferenceDockLine(line)}
                      >
                        <span className="reference-chip-primary">{renderHighlightedText(bubble.primary, referenceDockSearch)}</span>
                        {bubble.secondary ? <span className="reference-chip-secondary">{renderHighlightedText(bubble.secondary, referenceDockSearch)}</span> : null}
                      </button>
                    );
                  })
                ) : (
                  <div className="card empty">No reference entries match the current search.</div>
                )}
              </div>
              <div className="reference-dock-detail">
                {referenceDockDetail && referenceDockSelected ? (
                  <>
                    {referenceDockDetail.referenceBadges?.length ? (
                      <div className="reference-badge-strip">
                        {referenceDockDetail.referenceBadges.map((badge) => (
                          <span key={`${referenceDockDetail.lineId}:${badge}`} className="reference-badge">{badge}</span>
                        ))}
                      </div>
                    ) : null}
                    <div className="card stack raw-card">
                      <div className="raw-card-head">
                        <span>Reference item</span>
                        <span>{referenceDockSelected.lineNumber}</span>
                      </div>
                      <pre>{renderHighlightedText(referenceDockDetail.raw, referenceDockSearch)}</pre>
                    </div>
                    <div className="tab-strip" role="tablist" aria-label="Reference detail tabs">
                      {referenceDockTabs.map((tab) => (
                        <button
                          key={tab.key}
                          className={referenceDockActiveTab === tab.key ? "tab active" : "tab"}
                          onClick={() => setReferenceDockActiveTab(tab.key)}
                        >
                          {tab.label}
                        </button>
                      ))}
                    </div>
                    <div className="card stack detail-card reference-detail-card">
                      {renderReferenceDetailContent(
                        referenceDockDetail,
                        referenceDockActiveTabContent,
                        referenceDockSearch,
                        referenceDockSelections,
                        setReferenceDockSelections,
                      )}
                    </div>
                  </>
                ) : (
                  <div className="card empty">Select a reference entry to open the docked detail panel.</div>
                )}
              </div>
            </div>
          </aside>
        ) : null}
        </main>
      </div>

    </div>
  );
}

function detectInitialLocalOnlyMode(): boolean {
  if (typeof window === "undefined") return false;
  try {
    const params = new URLSearchParams(window.location.search);
    if (params.get("local") === "1") return true;
    if (window.location.protocol === "file:") return true;
  } catch {
    // ignore
  }
  return false;
}

function shouldShowLocalModeBanner(): boolean {
  if (typeof window === "undefined") return false;
  try {
    const params = new URLSearchParams(window.location.search);
    if (params.get("localBanner") === "1") return true;
    if (params.get("localBanner") === "0") return false;
    if (window.location.hostname.endsWith(".github.io")) return false;
    if (window.location.protocol === "file:") return false;
  } catch {
    // ignore
  }
  return true;
}

const CURRENT_BUILD_VERSION = typeof __TMDS_BUILD_VERSION__ === "string" ? __TMDS_BUILD_VERSION__ : "dev";

export default function App() {
  const webAppMode = isBrowserWebAppMode();
  const [authState, setAuthState] = useState<AuthState | null>(null);
  const [authReady, setAuthReady] = useState(!webAppMode);
  const [localOnlyMode, setLocalOnlyMode] = useState<boolean>(() => webAppMode && detectInitialLocalOnlyMode());
  const [showLocalModeBanner] = useState<boolean>(() => shouldShowLocalModeBanner());
  const [serverReachable, setServerReachable] = useState(false);
  const [showAdmin, setShowAdmin] = useState(false);
  const [showAccount, setShowAccount] = useState(false);
  const [latestBuildVersion, setLatestBuildVersion] = useState<string>(CURRENT_BUILD_VERSION);
  const [updateDismissedFor, setUpdateDismissedFor] = useState<string | null>(null);

  useEffect(() => {
    if (!webAppMode) return;
    let cancelled = false;
    const probe = async () => {
      try {
        const response = await fetch("./version.json", { cache: "no-store" });
        if (!response.ok) return;
        const data = (await response.json()) as { version?: string };
        if (cancelled) return;
        if (data && typeof data.version === "string" && data.version) {
          setLatestBuildVersion(data.version);
        }
      } catch {
        // ignore — try again next interval
      }
    };
    void probe();
    const id = window.setInterval(() => {
      void probe();
    }, 60000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [webAppMode]);

  const updateAvailable =
    webAppMode &&
    CURRENT_BUILD_VERSION !== "dev" &&
    Boolean(latestBuildVersion) &&
    latestBuildVersion !== CURRENT_BUILD_VERSION &&
    updateDismissedFor !== latestBuildVersion;

  const applyUpdate = () => {
    if (typeof window === "undefined") return;
    try {
      const url = new URL(window.location.href);
      url.searchParams.set("v", latestBuildVersion);
      window.location.replace(url.toString());
    } catch {
      window.location.reload();
    }
  };
  const dismissUpdate = () => {
    setUpdateDismissedFor(latestBuildVersion);
  };

  useEffect(() => {
    if (!webAppMode || !localOnlyMode) {
      setServerReachable(false);
      return;
    }
    let cancelled = false;
    let timer: number | null = null;
    const probe = async () => {
      try {
        const result = await fetchAuthState();
        if (cancelled) return;
        const looksLikeAuthState = result && typeof result === "object" && "configured" in result;
        setServerReachable(Boolean(looksLikeAuthState));
      } catch {
        if (!cancelled) {
          setServerReachable(false);
        }
      }
    };
    void probe();
    timer = window.setInterval(() => {
      void probe();
    }, 20000);
    return () => {
      cancelled = true;
      if (timer !== null) window.clearInterval(timer);
    };
  }, [webAppMode, localOnlyMode]);

  const reconnectToServer = () => {
    if (typeof window !== "undefined") {
      try {
        const url = new URL(window.location.href);
        url.searchParams.delete("local");
        window.location.replace(url.toString());
        return;
      } catch {
        // fall through
      }
    }
    setLocalOnlyMode(false);
    setAuthReady(false);
    setServerReachable(false);
  };

  useEffect(() => {
    if (!webAppMode) return;
    if (localOnlyMode) {
      setAuthReady(true);
      return;
    }
    let cancelled = false;
    fetchAuthState()
      .then((result) => {
        if (cancelled) return;
        const looksLikeAuthState = result && typeof result === "object" && "configured" in result;
        if (!looksLikeAuthState) {
          setLocalOnlyMode(true);
          setAuthReady(true);
          return;
        }
        setAuthState(result);
        setAuthReady(true);
      })
      .catch(() => {
        if (cancelled) return;
        setLocalOnlyMode(true);
        setAuthReady(true);
      });
    return () => {
      cancelled = true;
    };
  }, [webAppMode, localOnlyMode]);

  useEffect(() => {
    if (!webAppMode || localOnlyMode || !authState?.authenticated || authState.role !== "Administrator") {
      return;
    }
    const id = window.setInterval(() => {
      void fetchAuthState().then(setAuthState).catch(() => undefined);
    }, 15000);
    return () => window.clearInterval(id);
  }, [webAppMode, localOnlyMode, authState?.authenticated, authState?.role]);

  if (!webAppMode) {
    return <AppMain />;
  }

  if (!authReady) {
    return (
      <div className="auth-shell">
        <div className="auth-modal">
          <p>Connecting to TMDS server...</p>
        </div>
      </div>
    );
  }

  if (localOnlyMode) {
    return (
      <AppMain
        localOnlyMode
        showLocalModeBanner={showLocalModeBanner}
        serverReachable={serverReachable}
        onReconnect={reconnectToServer}
        updateAvailable={updateAvailable}
        onApplyUpdate={applyUpdate}
        onDismissUpdate={dismissUpdate}
      />
    );
  }

  if (!authState || !authState.authenticated) {
    const fallbackState: AuthState = authState ?? {
      configured: true,
      authenticated: false,
      adminUsername: "jchung",
      availableUsernames: [],
    };
    return <LoginScreen state={fallbackState} onSignedIn={(next) => setAuthState(next)} />;
  }

  async function handleLogout() {
    try {
      await logout();
    } catch {
      // ignore — we'll reset state anyway
    }
    setAuthState({
      configured: true,
      authenticated: false,
      adminUsername: authState?.adminUsername ?? "jchung",
      availableUsernames: authState?.availableUsernames ?? [],
    });
    setShowAdmin(false);
  }

  return (
    <>
      <AppMain
        authState={authState}
        onLogout={() => void handleLogout()}
        onOpenAdmin={() => setShowAdmin(true)}
        onOpenAccount={() => setShowAccount(true)}
        updateAvailable={updateAvailable}
        onApplyUpdate={applyUpdate}
        onDismissUpdate={dismissUpdate}
      />
      {showAdmin ? (
        <AdminPanel
          state={authState}
          onClose={() => {
            setShowAdmin(false);
            void fetchAuthState().then(setAuthState).catch(() => undefined);
          }}
          onAuthStateRefresh={() => {
            void fetchAuthState().then(setAuthState).catch(() => undefined);
          }}
        />
      ) : null}
      {showAccount ? (
        <AccountPanel
          state={authState}
          onClose={() => setShowAccount(false)}
          onUpdated={(next) => setAuthState(next)}
        />
      ) : null}
    </>
  );
}
