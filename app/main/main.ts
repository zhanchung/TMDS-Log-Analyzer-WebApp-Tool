import { Menu, app, BrowserWindow, dialog, ipcMain, shell, type MenuItemConstructorOptions } from "electron";
import { randomBytes, scryptSync, timingSafeEqual } from "node:crypto";
import { createWriteStream, type WriteStream } from "node:fs";
import { appendFile, mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { availableParallelism, tmpdir } from "node:os";
import { dirname, extname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { gunzipSync } from "node:zlib";
import * as yauzl from "yauzl";
import { loadWindowState, saveWindowState } from "./window-state";
import type { AdminUsersResult, AuthResult, AuthRole, AuthState } from "../shared/native-api";
import type { DetailModel, ParsedLine, SearchConfig, SessionData, SourceKind, SourceRecord, WorkflowRelatedDetail, WorkspaceProgress } from "../shared/types";
import {
  extractLogTimestamp as sharedExtractLogTimestamp,
  isGzipFile as sharedIsGzipFile,
  isTextFile as sharedIsTextFile,
  isZipFile as sharedIsZipFile,
  parseLines as sharedParseLines,
  stripLeadingLogTimestamp as sharedStripLeadingLogTimestamp,
} from "../shared/parser/primitives";
import {
  decodeGenisysSocketFrame,
  describeGenericHexByteRows,
  formatByteBinary,
  formatHexByte,
  genisysHeaderLabels,
  parseSocketHexByte,
  type DecodedGenisysSocketFrame,
} from "../shared/genisys";

const APP_DISPLAY_NAME = "Log Analyzer";
const TIME_CONVERT_TOOL_URL = "https://savvytime.com/converter/pst-to-utc";
const ADMIN_USERNAME = "jchung";
const BUILTIN_ADMIN_SALT_HEX = "ab34d20b415960f8c56d8768bb0841ca";
const BUILTIN_ADMIN_PASSWORD_HASH_HEX = "1b8da348907a67e0fab7708fbf8670b69dd0d0ea37956b78e75ae563ff0ad470c61608d20d841e61a8770fa0adbd3463261737d9e333d09cff5812517ae6083c";
app.setName(APP_DISPLAY_NAME);
const hasSingleInstanceLock = app.requestSingleInstanceLock();

let mainWindow: BrowserWindow | null = null;
let referenceWindow: BrowserWindow | null = null;
type WorkspaceWindowMode = "main" | "reference";

type ProgressReporter = (progress: WorkspaceProgress) => void;
type AuthStore = {
  version: 1 | 2;
  username?: string;
  saltHex?: string;
  passwordHashHex?: string;
  rememberedUsername?: string;
  keepSignedIn?: boolean;
  role?: AuthRole;
  keepSignedInUsername?: string;
  users?: Record<string, StoredAuthUser>;
  adminUsernameAlias?: string;
};

type StoredAuthUser = {
  username: string;
  saltHex?: string;
  passwordHashHex?: string;
  role: AuthRole;
  passwordResetRequired?: boolean;
  passwordResetRequestedAt?: string;
  lastLoginAt?: string;
};

let authSessionUsername: string | null = null;

type HttpSession = {
  username: string;
  role: AuthRole;
  createdAt: number;
  lastSeenAt: number;
};
const httpSessions = new Map<string, HttpSession>();
const HTTP_SESSION_COOKIE = "tmds_session";
const HTTP_SESSION_TTL_MS = 1000 * 60 * 60 * 24 * 14;
const assignmentLongNameFallbackSuffixes = new Set(["POK", "DOK", "MOK", "LOK"]);
const canonicalIndicationLongNamesBySuffix = new Map<string, string>([
  ["POK", "POWER OFF INDICATION"],
  ["DOK", "ILLEGAL ENTRY INDICATION"],
  ["MOK", "MANUAL OPERATE INDICATION"],
  ["LOK", "LIGHT OUT INDICATION"],
]);
const ignoredInputDirectoryNames = new Set(["build", "dist", "node_modules", "__pycache__", ".git", ".venv", "venv"]);

type SqlFoundationManifestEntry = {
  database: string;
  export_name: string;
  row_count: number | string;
  json_path: string;
  csv_path: string;
  meta_path: string;
};

type NormalizedManifestEntry = {
  name: string;
  json_path: string;
  csv_path: string;
  row_count: number | string;
};

type FoundationMetric = {
  metric: string;
  value: number | string;
};

type FoundationStatsPayload = {
  flat_metrics: FoundationMetric[];
};

type MusicSourceEntry = {
  kind: SourceKind;
  title: string;
  path: string;
  version?: string;
  section?: string;
  page?: string;
  notes?: string;
};

type SubdivisionProtocolRow = {
  subdivision_name: string;
  code_line_numbers: string;
  signal_count: number | string;
  track_count: number | string;
  switch_count: number | string;
  misc_device_count: number | string;
  route_count: number | string;
};

type TrainRuntimeJoinRow = {
  runtime_join_status: string;
  row_count: number | string;
};

type ReferenceFamilyRow = {
  component_family: string;
  reference_column: string;
  unresolved_count: number | string;
};

type GenisysStationAssignmentSummaryRow = {
  code_line_number: string;
  code_line_name: string;
  code_station_number: string;
  station_name: string;
  control_point_number: string;
  control_point_name: string;
  subdivision_name: string;
  word_type: string;
  assignment_count: number | string;
  derived_word_count: number | string;
  decode_basis: string;
};

type BosEmpMessagePrefixCountRow = {
  message_prefix: string;
  row_count: number | string;
};

type BosEmpMessageCandidateRow = {
  message_prefix: string;
  candidate_document_family: string;
  candidate_release: string;
  candidate_direction: string;
  candidate_message_name: string;
  candidate_message_version: string;
};

type RuntimeVersionFactRow = {
  fact_type: string;
  fact_value: string;
  row_count: number | string;
};

type StaticBosInterfaceVersionRow = {
  InterfaceVersion: string;
  row_count: number | string;
};

type StationFoundationRow = {
  code_line_number: string;
  code_line_name: string;
  code_station_number: string;
  station_name: string;
  control_point_number: string;
  control_point_name: string;
  subdivision_name: string;
  signal_count: number | string;
  track_count: number | string;
  switch_count: number | string;
  misc_device_count: number | string;
  route_count: number | string;
  number_of_controls: number | string;
  number_of_indications: number | string;
};

type CodeLineProtocolRow = {
  code_line_number: string;
  code_line_name: string;
  legacy_type: string;
  session_protocol: string;
  normal_codeserver_name: string;
  standby_codeserver_name: string;
  packet_switch_primary_name: string;
  packet_switch_secondary_name: string;
  packet_switch_primary_ip: string;
  packet_switch_secondary_ip: string;
  subdivision_names: string;
};

type CodeAssignmentEntry = {
  word_type: string;
  bit_position: string;
  mnemonic: string;
  long_name: string;
  family_hint?: string;
  component_hint?: string;
  secondary_hint?: string;
};

type CodeStationAssignmentMapRow = {
  code_line_number: string;
  code_line_name: string;
  code_station_number: string;
  station_name: string;
  control_point_number: string;
  control_point_name: string;
  subdivision_name: string;
  number_of_controls: number | string;
  number_of_indications: number | string;
  control_assignments: CodeAssignmentEntry[];
  indication_assignments: CodeAssignmentEntry[];
};

type RouteSwitchContextRow = {
  control_point_uid: string;
  control_point_name: string;
  entry_signal_uid?: string;
  entry_signal_name: string;
  exit_signal_uid?: string;
  exit_signal_name: string;
  switch_uid?: string;
  switch_name: string;
  switch_tooltip_name?: string;
  required_state: string;
  route_guid: string;
  switch_order?: number;
  switch_list_raw?: string;
};

type TrainRuntimeFoundationRow = {
  symbol: string;
  direction: string;
  train_type: string;
  origin: string;
  dest: string;
  control_point_name: string;
  track_name: string;
  track_route_name: string;
  authority_designation: string;
  runtime_join_status: string;
  runtime_loco_id: string;
  runtime_train_symbol: string;
  runtime_icd_interface_version: string;
  runtime_departure_test_status: string;
  runtime_head_end_track_name: string;
};

type HostInventoryRow = {
  name: string;
  configuration_type_name: string;
  operating_system_name: string;
  primary_ip: string;
  location_name: string;
  hostname: string;
  notes: string;
  manufacturer_name: string;
  model_name: string;
  dnet_connection_type: string;
  dnet_lte_status: string;
};

type StaticComponentMilepostRow = {
  ControlPoint: string;
  Milepost?: string;
  LeftLimitMPRange?: string;
  RightLimitMPRange?: string;
};

type ManualPageTextRow = {
  text?: string;
};

type StaticComponentLookupRow = {
  component_uid: string;
  component_family: string;
  parent_control_point_uid: string;
  component_name: string;
  component_secondary_name: string;
  component_detail_name: string;
};

type DynamicComponentContextRow = {
  component_family: string;
  component_name: string;
  component_secondary_name: string;
};

type ParsedLogEvent =
  | {
      family: "station-indication" | "socket-indication";
      stationToken?: string;
      controlPointNumber: string;
      codeLineNumber: string;
      wordNumber: string;
      payloadBits: string;
      host?: string;
      codeServerName?: string;
      direction?: string;
      traceClass?: string;
      traceMethod?: string;
      traceThread?: string;
    }
  | {
      family: "station-recall";
      stationToken: string;
      sequence: string;
    }
  | {
      family: "socket-keepalive" | "socket-alive";
      host: string;
      direction: string;
      codeServerName?: string;
      traceClass?: string;
      traceMethod?: string;
      traceThread?: string;
    }
  | {
      family: "code-server-queue";
      host: string;
      codeServerName: string;
      queueCount: string;
    }
  | {
      family: "code-server-thread-alive";
      host: string;
      codeServerName: string;
    }
  | {
      family: "cad-control-point-message";
      sourceKind: "stream-receiver" | "control-server-message";
      controlPointNumber: string;
      subdivisionToken: string;
      operationToken: string;
      correlationId: string;
      relatedUidA: string;
      relatedUidB: string;
      stateValue: string;
      transactionId: string;
      codeServerName?: string;
    }
  | {
      family: "cad-signal-message";
      sourceKind: "stream-receiver" | "control-server-message";
      subdivisionToken: string;
      signalUid: string;
      operationToken: string;
      transactionId: string;
      codeServerName?: string;
    }
  | {
      family: "cad-train-message";
      sourceKind: "train-event" | "stream-receiver" | "control-server-message";
      subdivisionToken: string;
      eventToken: string;
      trainUid: string;
      trainSymbol: string;
      payloadFieldCount: number;
      payloadFields: string[];
      direction?: string;
      leadEquipment?: string;
      trainGuid?: string;
      serviceSlot?: string;
      trainType?: string;
      homeRoadCode?: string;
      codeServerName?: string;
    }
  | {
      family: "process-vital-signs";
      startTime?: string;
      softwareVersion?: string;
      workingMemory?: string;
      threadCount?: string;
      handleCount?: string;
      workingPeakMemory?: string;
      pagedMemory?: string;
      pagedPeakMemory?: string;
      privilegedProcessorTime?: string;
      totalProcessorTime?: string;
      workstation?: string;
    }
  | {
      family: "process-vital-signs-header";
      component: string;
      softwareVersion: string;
    }
  | {
      family: "thread-capacity";
      capacityKind: "Max" | "Min" | "Available";
      maxWorkerThreads: string;
      maxIoCompletionThreads: string;
    }
  | {
      family: "signal-state-change";
      stateToken: string;
    }
  | {
      family: "signal-indication-update";
      signalUid: string;
      signalName: string;
      controlPointToken: string;
      statusTokens: string[];
    }
  | {
      family: "flash-name";
      flashName: string;
      sourceKind: "marker" | "event";
    }
  | {
      family: "indication-message-complete";
      codeServerName: string;
    }
  | {
      family: "train-schedule-timer-check";
      trainSymbol: string;
      locationToken: string;
      scheduleUid: string;
      timerCheckTime: string;
      scheduleTime: string;
    }
  | {
      family: "trace-metadata";
      version: string;
      fileName: string;
      methodName: string;
      lineNumberInfo?: string;
    }
  | {
      family: "genisys-control-resend";
      retryCurrent: string;
      retryTotal: string;
      controlPointNumber: string;
      stationToken: string;
      timeoutMs: string;
    }
  | {
      family: "code-line-process-indication";
      codeStationNumber: string;
      stationToken: string;
      payloadBits: string;
    }
  | {
      family: "code-line-command";
      action: "Queued" | "Sent";
      stationToken: string;
      commandKind: string;
      payloadBits?: string;
    }
  | {
      family: "code-line-queue-count";
      queueCount: string;
    }
  | {
      family: "code-line-process-send-queue";
      queueText: string;
      stationToken?: string;
      commandKind?: string;
    }
  | {
      family: "code-line-control-sent";
      codeStationNumber: string;
      stationToken: string;
      payloadBits: string;
    }
  | {
      family: "code-line-control-mnemonic";
      entries: Array<{ position: string; mnemonic: string; value: string }>;
    }
  | {
      family: "code-line-indication-mnemonic";
      entries: Array<{ position: string; mnemonic: string; value: string }>;
    }
  | {
      family: "code-line-control-payload";
      phase: "SendControl" | "ProcessControlBegin";
      codeStationNumber?: string;
      payloadBits: string;
    }
  | {
      family: "code-line-control-queue-cleared";
      payloadBits?: string;
    }
  | {
      family: "code-line-control-process-completed";
    }
  | {
      family: "code-line-control-queue-print";
    }
  | {
      family: "code-line-statistics-summary";
      stationToken: string;
      controlCount: string;
      indicationCount: string;
      failureCount: string;
    }
  | {
      family: "code-line-hex-frame";
      frameLabel: string;
      payloadBytes: string[];
    }
  | {
      family: "code-line-indication-summary";
      codeToken: string;
      stationToken?: string;
    }
  | {
      family: "code-line-control-update";
      codeToken: string;
      stationToken?: string;
      payloadBits: string;
      queueCount?: string;
      updateOnly?: boolean;
    }
  | {
      family: "code-line-control-delivered";
      codeToken: string;
      stationToken?: string;
      payloadBits: string;
    }
  | {
      family: "socket-control";
      host: string;
      codeLineNumber: string;
      controlPointNumber: string;
      wordNumber: string;
      payloadBits: string;
      updateOnly?: boolean;
    }
  | {
      family: "code-line-queue-depth";
      queueCount: string;
      component?: string;
      method?: string;
      traceLineNumber?: string;
    }
  | {
      family: "code-line-service-message";
      statusToken: string;
      codeToken: string;
      stationToken?: string;
    }
  | {
      family: "code-line-control-image";
      phase: "Queued replacement" | "Sent image";
      payloadBits: string;
    }
  | {
      family: "code-line-recall-auto";
      stationToken: string;
      auto: boolean;
    }
  | {
      family: "code-line-last-indication-auto-recall";
      stationToken: string;
    }
  | {
      family: "control-delivery-timer-stop";
    }
  | {
      family: "commserver-data-message";
      peerToken: string;
      payload: string;
    }
  | {
      family: "commserver-train-processing";
      trainSymbol: string;
    }
  | {
      family: "commserver-sql-query";
      queryKind: string;
      sqlText: string;
    }
  | {
      family: "track-indication-update";
      statusTokens: string[];
      trackName: string;
      trackUid: string;
      controlPointToken: string;
    }
  | {
      family: "track-traffic-removal-check";
      decision: string;
      trackName: string;
      trackUid: string;
      directionToken?: string;
    }
  | {
      family: "ptcbos-message";
      rawKind: "decoded" | "raw";
      payload: string;
    }
  | {
      family: "code-line-process-indication-phase";
      phase: "ProcessIndication" | "ProcessInformationBit";
      stationToken: string;
    }
  | {
      family: "socket-raw-frame";
      socketAction: "XMT" | "RCV";
      stationToken: string;
      payloadBytes: string[];
      directionGlyph: string;
    }
  | {
      family: "host-connection-refused";
      host: string;
      port: string;
    }
  | {
      family: "workstation-request-line";
      listenerPort: string;
      protocolToken: string;
      requestType: string;
      subject: string;
      traceComponent?: string;
      traceLevel?: string;
      traceLineNumber?: string;
    }
  | {
      family: "cad-forwarded-vetms";
      listenerPort: string;
      workstation: string;
      messageFamily: string;
      messageType: string;
      stateToken?: string;
      payloadFields: string[];
      traceComponent?: string;
      traceLevel?: string;
      traceLineNumber?: string;
    }
  | {
      family: "loco-log-marker";
      markerText: string;
    }
  | {
      family: "loco-log-entry";
      severity: "SYS" | "WARN" | "NOTE" | "ERR" | "INFO";
      component: string;
      message: string;
    }
  | {
      family: "office-telemetry-summary";
      direction: "TX" | "RX";
      channel: string;
      messageId: string;
      sequence: string;
      peer: string;
    }
  | {
      family: "office-telemetry-hex";
      direction: "TX" | "RX";
      channel: string;
      payloadBytes: string[];
    }
  | {
      family: "recorder-delimited-record";
      recorder: string;
      payloadFields: string[];
    }
  | {
      family: "locomotive-recorder-record";
      recordType: string;
      payloadFields: string[];
    }
  | {
      family: "raw-hex-payload";
      byteCount: number;
      payloadPreview: string;
    }
  | {
      family: "workstation-vetms-message";
      messageDirection: "SEND" | "RECV";
      route: string;
      messageCommand: string;
      messageCategory?: string;
      messageType?: string;
      stateChange?: string;
      trainSymbol?: string;
      locoUid?: string;
      reportTime?: string;
      directionOfTravel?: string;
      headMp?: string;
      rearMp?: string;
      subdivisionId?: string;
      speed?: string;
      locoState?: string;
      locoStateSummary?: string;
      headEndTrack?: string;
      rearEndTrack?: string;
      employeeId?: string;
      employeeName?: string;
      traceComponent?: string;
      traceLevel?: string;
      traceLineNumber?: string;
    }
  | {
      family: "plain-vetms-message";
      messageDirection: "SEND" | "RECV";
      messageCategory: string;
      messageType: string;
      stateChange?: string;
      payloadFields: string[];
      trainSymbol?: string;
      locoUid?: string;
      reportTime?: string;
      directionOfTravel?: string;
    }
  | {
      family: "territory-train-list";
      territoryToken: string;
    }
  | {
      family: "workstation-originated-train-log";
      payload: string;
      payloadFields: string[];
      subdivisionToken?: string;
      eventToken?: string;
      trainUid?: string;
      trainSymbol?: string;
    }
  | {
      family: "train-tracking-message";
      prefix: string;
      action: string;
      rawPayload: string;
      trainSymbol?: string;
      trackUid?: string;
      relatedTrackUid?: string;
      trackName?: string;
      directionToken?: string;
      indexValue?: string;
    }
  | {
      family: "plain-control-sent";
      stationToken: string;
      channelToken: string;
      declaredWidth: string;
      payloadBits: string;
    }
  | {
      family: "mpar-event";
      eventName: string;
      payload?: string;
    }
  | {
      family: "route-search-message";
      searchKind: string;
      action: "marker" | "component";
      marker?: string;
      componentClass?: string;
      componentUid?: string;
      rawPayload: string;
    }
  | {
      family: "component-reference-list-entry";
      entryKind: "COMP" | "GUID";
      componentUid?: string;
      componentName?: string;
      componentClass?: string;
      rawPayload: string;
    }
  | {
      family: "named-guid-catalog-entry";
      label: string;
      guid: string;
    }
  | {
      family: "sql-train-update-continuation";
      trainSymbol?: string;
      payload: string;
    }
  | {
      family: "compact-track-state";
      occupied: string;
      traffic: string;
      blocking: string;
      trackStatus: string;
    }
  | {
      family: "gbo-ptc-transmission-status";
      inProgress: string;
      guid: string;
      trainSymbol: string;
    }
  | {
      family: "code-server-online-status";
      onlineStatus: string;
      localStatus: string;
      globalStatus: string;
    }
  | {
      family: "bos-server-list-entry";
      availability: "Online" | "Offline";
      serverName: string;
      serverId: string;
      serverAssignmentId: string;
      lastHeartbeat: string;
    }
  | {
      family: "connection-endpoint-status";
      scope: "CLIENT" | "CLUSTER";
      host: string;
      port: string;
    }
  | {
      family: "workstation-transaction-marker";
      workstation: string;
      transactionId?: string;
    }
  | {
      family: "indication-change-trigger";
      caller: string;
    }
  | {
      family: "locomotive-processing-marker";
      stage: "SummaryUpdateCompleted" | "CheckPositionReportFinished";
    }
  | {
      family: "track-tracing-marker";
      phase: "START" | "END";
      subject: string;
    }
  | {
      family: "system-thread-heartbeat";
      threadName: string;
    }
  | {
      family: "control-queue-event";
      eventName: string;
    }
  | {
      family: "repeated-binary-state";
      payloadBits: string;
      stateKind: "CONTROL" | "INDICATION";
    }
  | {
      family: "network-stack-frame";
      method: string;
      signature: string;
    }
  | {
      family: "application-stack-frame";
      method: string;
      signature?: string;
    }
  | {
      family: "direction-state-entry";
      direction: "EAST" | "WEST" | "NONE";
      code: string;
      state: string;
    }
  | {
      family: "admin-click-action";
      action: string;
      actionTime?: string;
    }
  | {
      family: "user-interface-marker";
      markerText: string;
    }
  | {
      family: "control-send-phase-marker";
      routine: string;
      phase: "START" | "END";
    }
  | {
      family: "indication-bit-inversion";
      bitIndex: string;
      fromValue: string;
      toValue: string;
    }
  | {
      family: "short-workflow-marker";
      prefix: string;
      marker: string;
    }
  | {
      family: "route-selection-step";
      processName: string;
      step: string;
    }
  | {
      family: "stored-route-event-marker";
      eventGroup: string;
      step: string;
    }
  | {
      family: "stored-route-recursion-check";
      phase: "BEGIN" | "END";
    }
  | {
      family: "system-reset-marker";
      markerText: string;
    }
  | {
      family: "stored-route-status-marker";
      markerText: string;
    }
  | {
      family: "code-station-load-count";
      count: string;
    }
  | {
      family: "exception-trace-separator";
      markerText: string;
    }
  | {
      family: "blank-log-entry";
    }
  | {
      family: "pipe-exception";
      component: string;
      summary: string;
      exceptionType: string;
      exceptionMessage: string;
    }
  | {
      family: "prefixed-log-message";
      prefix: string;
      payload: string;
    }
  | {
      family: "binary-state-dump";
      payloadBits: string;
    }
  | {
      family: "other";
    };

type LogEnrichmentBundle = {
  stationByKey: Map<string, StationFoundationRow>;
  stationByAddress: Map<string, StationFoundationRow[]>;
  assignmentByKey: Map<string, CodeStationAssignmentMapRow>;
  routesByKey: Map<string, RouteSwitchContextRow[]>;
  trainsByKey: Map<string, TrainRuntimeFoundationRow[]>;
  codeLineByNumber: Map<string, CodeLineProtocolRow>;
  hostByIp: Map<string, HostInventoryRow[]>;
  componentByUid: Map<string, StaticComponentLookupRow>;
  controlPointMilepostByNumber: Map<string, number>;
};

let logEnrichmentPromise: Promise<LogEnrichmentBundle | null> | null = null;

function foundationPath(...segments: string[]): string {
  return resolve(app.getAppPath(), ...segments);
}

async function readFirstAvailableTextFile(paths: string[]): Promise<{ text: string; path: string }> {
  for (const candidatePath of paths) {
    try {
      return {
        text: await readFile(candidatePath, "utf-8"),
        path: candidatePath,
      };
    } catch {
      // Try the next packaged/local source path.
    }
  }
  return { text: "", path: paths[0] ?? "" };
}

function runtimeLogPath(): string {
  return join(app.getPath("userData"), "runtime.log");
}

function authStorePath(): string {
  return join(app.getPath("userData"), "auth.json");
}

async function readAuthStore(): Promise<AuthStore | null> {
  try {
    return JSON.parse(await readFile(authStorePath(), "utf-8")) as AuthStore;
  } catch {
    return null;
  }
}

async function writeAuthStore(store: AuthStore): Promise<void> {
  const path = authStorePath();
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, JSON.stringify(store, null, 2), "utf-8");
}

function parseCookieHeader(header: string | undefined | null): Map<string, string> {
  const out = new Map<string, string>();
  if (!header) return out;
  for (const part of header.split(";")) {
    const trimmed = part.trim();
    if (!trimmed) continue;
    const eq = trimmed.indexOf("=");
    if (eq <= 0) continue;
    const name = trimmed.slice(0, eq).trim();
    const value = trimmed.slice(eq + 1).trim();
    out.set(name, decodeURIComponent(value));
  }
  return out;
}

function pruneExpiredHttpSessions(): void {
  const cutoff = Date.now() - HTTP_SESSION_TTL_MS;
  for (const [token, session] of httpSessions) {
    if (session.lastSeenAt < cutoff) {
      httpSessions.delete(token);
    }
  }
}

function getHttpSessionFromRequest(request: IncomingMessage): { token: string; session: HttpSession } | null {
  const cookies = parseCookieHeader(request.headers["cookie"] as string | undefined);
  const token = cookies.get(HTTP_SESSION_COOKIE);
  if (!token) return null;
  pruneExpiredHttpSessions();
  const session = httpSessions.get(token);
  if (!session) return null;
  session.lastSeenAt = Date.now();
  return { token, session };
}

function createHttpSession(username: string, role: AuthRole): string {
  pruneExpiredHttpSessions();
  const token = randomBytes(32).toString("hex");
  httpSessions.set(token, { username, role, createdAt: Date.now(), lastSeenAt: Date.now() });
  return token;
}

function destroyHttpSession(token: string): void {
  httpSessions.delete(token);
}

function setSessionCookie(response: ServerResponse, token: string): void {
  const maxAge = Math.floor(HTTP_SESSION_TTL_MS / 1000);
  response.setHeader(
    "Set-Cookie",
    `${HTTP_SESSION_COOKIE}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${maxAge}`,
  );
}

function clearSessionCookie(response: ServerResponse): void {
  response.setHeader("Set-Cookie", `${HTTP_SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`);
}

function getRoleForUsername(username: string, store: AuthStore | null): AuthRole | null {
  if (!username) return null;
  if (isAdminLoginUsername(username, store)) return "Administrator";
  return getLocalUser(store, username)?.role ?? null;
}

function isPasswordValidLength(password: string): boolean {
  const len = String(password ?? "").length;
  return len >= 4 && len <= 16;
}

function getPasswordValidationMessage(): string {
  return "Password must be 4 to 16 characters.";
}

function getEffectiveAdminUsername(store: AuthStore | null): string {
  const alias = store?.adminUsernameAlias?.trim();
  return alias && alias.length ? alias : ADMIN_USERNAME;
}

function isAdminLoginUsername(username: string, store: AuthStore | null): boolean {
  const key = normalizeUsernameKey(username);
  if (key === ADMIN_USERNAME) return true;
  const alias = store?.adminUsernameAlias?.trim();
  return Boolean(alias && normalizeUsernameKey(alias) === key);
}

function isUsernameTaken(store: AuthStore | null, username: string): boolean {
  const key = normalizeUsernameKey(username);
  if (key === ADMIN_USERNAME) return true;
  if (store?.adminUsernameAlias && normalizeUsernameKey(store.adminUsernameAlias) === key) return true;
  return Object.prototype.hasOwnProperty.call(getStoreUsers(store), key);
}

const USERNAME_PATTERN = /^(?=.{1,48}$)[A-Za-z0-9._-]+(?: [A-Za-z0-9._-]+)*$/;
function isValidUsername(username: string): boolean {
  return USERNAME_PATTERN.test(username);
}

function getUsernameValidationMessage(): string {
  return "Username must be 1-48 chars using letters, numbers, spaces, dot, underscore, or dash. Spaces must be between name parts.";
}

function getAdminAliasForUsername(username: string): string | undefined {
  const cleanUsername = sanitizeAuthUsername(username);
  return cleanUsername === ADMIN_USERNAME ? undefined : cleanUsername;
}

function hashPassword(password: string, saltHex: string): string {
  return scryptSync(password, Buffer.from(saltHex, "hex"), 64).toString("hex");
}

function verifyPassword(password: string, saltHex: string, passwordHashHex: string): boolean {
  const expected = Buffer.from(passwordHashHex, "hex");
  const actual = Buffer.from(hashPassword(password, saltHex), "hex");
  return expected.length === actual.length && timingSafeEqual(expected, actual);
}

function sanitizeAuthUsername(username: string): string {
  return String(username ?? "").trim();
}

function isAuthorizedAdminStore(store: AuthStore | null): store is AuthStore {
  return Boolean(store && sanitizeAuthUsername(store.username).toLowerCase() === ADMIN_USERNAME);
}

function normalizeUsernameKey(username: string): string {
  return sanitizeAuthUsername(username).toLowerCase();
}

function getStoreUsers(store: AuthStore | null): Record<string, StoredAuthUser> {
  return store?.users && typeof store.users === "object" ? store.users : {};
}

function getLocalUser(store: AuthStore | null, username: string): StoredAuthUser | null {
  const key = normalizeUsernameKey(username);
  return getStoreUsers(store)[key] ?? null;
}

function getAuthRole(username: string | null, store: AuthStore | null): AuthRole | undefined {
  if (!username) {
    return undefined;
  }
  if (isAdminLoginUsername(username, store)) {
    return "Administrator";
  }
  return getLocalUser(store, username)?.role;
}

function buildStoreWithUser(store: AuthStore | null, user: StoredAuthUser, rememberUsername: boolean, keepSignedIn: boolean): AuthStore {
  return {
    ...(store ?? {}),
    version: 2,
    rememberedUsername: rememberUsername ? user.username : store?.rememberedUsername,
    keepSignedIn,
    keepSignedInUsername: keepSignedIn ? user.username : undefined,
    users: {
      ...getStoreUsers(store),
      [normalizeUsernameKey(user.username)]: user,
    },
  };
}

function buildStoreSessionUpdate(store: AuthStore | null, username: string, rememberUsername: boolean, keepSignedIn: boolean): AuthStore {
  return {
    ...(store ?? {}),
    version: 2,
    rememberedUsername: rememberUsername ? username : undefined,
    keepSignedIn,
    keepSignedInUsername: keepSignedIn ? username : undefined,
    users: getStoreUsers(store),
  };
}

function buildAdminUsersResult(store: AuthStore | null, ok = true, error?: string): AdminUsersResult {
  const adminUsername = getEffectiveAdminUsername(store);
  const users = [
    {
      username: adminUsername,
      role: "Administrator" as AuthRole,
      builtIn: true,
      current: isAdminLoginUsername(authSessionUsername ?? "", store),
    },
    ...Object.values(getStoreUsers(store))
      .sort((left, right) => left.username.localeCompare(right.username))
      .map((user) => ({
        username: user.username,
        role: user.role,
        builtIn: false,
        current: normalizeUsernameKey(authSessionUsername ?? "") === normalizeUsernameKey(user.username),
        passwordResetRequired: Boolean(user.passwordResetRequired || !user.saltHex || !user.passwordHashHex),
      })),
  ];
  return { ok, error, users };
}

function getAvailableUsernames(store: AuthStore | null): string[] {
  const adminName = getEffectiveAdminUsername(store);
  return [
    adminName,
    ...Object.values(getStoreUsers(store))
      .map((user) => user.username)
      .sort((left, right) => left.localeCompare(right)),
  ];
}

function getPendingPasswordResetCount(store: AuthStore | null): number {
  return Object.values(getStoreUsers(store)).filter((user) => Boolean(user.passwordResetRequestedAt)).length;
}

async function requireAdministrator(): Promise<AuthStore | null> {
  const store = await readAuthStore();
  if (getAuthRole(authSessionUsername, store) !== "Administrator") {
    throw new Error("Administrator role required.");
  }
  return store;
}

async function getAuthState(): Promise<AuthState> {
  const store = await readAuthStore();
  if (authSessionUsername && !getAuthRole(authSessionUsername, store)) {
    authSessionUsername = null;
  }
  if (!authSessionUsername && store?.keepSignedIn && store.keepSignedInUsername && getAuthRole(store.keepSignedInUsername, store)) {
    authSessionUsername = store.keepSignedInUsername;
  }
  const role = getAuthRole(authSessionUsername, store);
  return {
    configured: true,
    authenticated: Boolean(role),
    username: authSessionUsername ?? undefined,
    rememberedUsername: store?.rememberedUsername ?? getEffectiveAdminUsername(store),
    keepSignedIn: Boolean(store?.keepSignedIn),
    role,
    adminUsername: getEffectiveAdminUsername(store),
    availableUsernames: getAvailableUsernames(store),
    pendingPasswordResetCount: role === "Administrator" ? getPendingPasswordResetCount(store) : 0,
  };
}

function logRuntime(message: string): void {
  const path = runtimeLogPath();
  const line = `[${new Date().toISOString()}] ${message}\n`;
  void mkdir(dirname(path), { recursive: true })
    .then(() => appendFile(path, line, "utf-8"))
    .catch(() => undefined);
}

function sendMenuCommand(channel: string, payload?: string[]): void {
  const targetWindow = BrowserWindow.getFocusedWindow() ?? mainWindow;
  if (!targetWindow || targetWindow.isDestroyed()) {
    return;
  }
  targetWindow.webContents.send(channel, payload);
}

function focusReferenceWindow(): void {
  if (!referenceWindow || referenceWindow.isDestroyed()) {
    return;
  }
  if (referenceWindow.isMinimized()) {
    referenceWindow.restore();
  }
  referenceWindow.focus();
}

function buildApplicationMenu(): void {
  const template: MenuItemConstructorOptions[] = [
    {
      label: "File",
      submenu: [
        {
          label: "Open Files or Folders...",
          accelerator: "CmdOrCtrl+O",
          click: async () => {
            if (!mainWindow || mainWindow.isDestroyed()) {
              return;
            }
            const result = await dialog.showOpenDialog(mainWindow, {
              properties: ["openFile", "openDirectory", "multiSelections"],
            });
            if (!result.canceled && result.filePaths.length) {
              sendMenuCommand("workspace:menu-open-inputs", result.filePaths);
            }
          },
        },
        {
          label: "Open Reference Library",
          accelerator: "CmdOrCtrl+Shift+R",
          click: () => {
            if (!referenceWindow || referenceWindow.isDestroyed()) {
              referenceWindow = createWindow("reference");
              return;
            }
            focusReferenceWindow();
          },
        },
        {
          label: "Open Review Sample Logs",
          accelerator: "CmdOrCtrl+Shift+O",
          click: () => {
            sendMenuCommand("workspace:menu-load-review-sample");
          },
        },
        { type: "separator" },
        { role: "quit" },
      ],
    },
    {
      label: "Edit",
      submenu: [
        {
          label: "Open Finder",
          accelerator: "CmdOrCtrl+F",
          click: () => sendMenuCommand("workspace:menu-open-finder"),
        },
        { type: "separator" },
        { role: "copy" },
        { role: "selectAll" },
      ],
    },
  ];

  Menu.setApplicationMenu(Menu.buildFromTemplate(template));
}

async function readJsonFile<T>(...segments: string[]): Promise<T> {
  const raw = await readFile(foundationPath(...segments), "utf-8");
  return JSON.parse(raw) as T;
}

function toNumber(value: number | string | undefined | null): number {
  if (typeof value === "number") {
    return value;
  }
  const parsed = Number(String(value ?? "").trim());
  return Number.isFinite(parsed) ? parsed : 0;
}

function sumRowCounts<T extends { row_count: number | string }>(rows: T[]): number {
  return rows.reduce((sum, row) => sum + toNumber(row.row_count), 0);
}

function getMetric(stats: FoundationMetric[], metricName: string): number {
  const row = stats.find((entry) => entry.metric === metricName);
  return toNumber(row?.value);
}

function groupCountSummary<T>(rows: T[], getKey: (row: T) => string): string {
  const counts = new Map<string, number>();
  for (const row of rows) {
    const key = getKey(row);
    if (!key) continue;
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([key, count]) => `${key}=${count}`)
    .join(", ");
}

function createSourceRecord(
  id: string,
  kind: SourceKind,
  title: string,
  path: string,
  notes?: string,
): SourceRecord {
  return { id, kind, title, path, notes };
}

function toMusicSourceRecord(row: MusicSourceEntry, index: number): SourceRecord {
  return {
    id: `music:${index + 1}`,
    kind: row.kind,
    title: row.title,
    path: row.path,
    version: row.version,
    section: row.section,
    page: row.page,
    notes: row.notes,
  };
}

const isTextFile = sharedIsTextFile;
const isGzipFile = sharedIsGzipFile;
const isZipFile = sharedIsZipFile;
const extractLogTimestamp = sharedExtractLogTimestamp;
const stripLeadingLogTimestamp = sharedStripLeadingLogTimestamp;
const parseLines = sharedParseLines;

function decodeTextBuffer(buffer: Buffer): string {
  return buffer.toString("utf-8");
}

function appendAll<T>(target: T[], items: T[]): void {
  for (const item of items) {
    target.push(item);
  }
}

function clampPercent(value: number): number {
  return Math.max(0, Math.min(100, Math.round(value)));
}

function toProgressPathLabel(path: string | undefined): string | undefined {
  if (!path) {
    return undefined;
  }
  const normalized = path.replace(/\\/g, "/");
  const parts = normalized.split("/");
  return parts[parts.length - 1] || path;
}

function emitWorkspaceProgress(progress: WorkspaceProgress): void {
  mainWindow?.webContents.send("workspace:progress", {
    ...progress,
    percent: clampPercent(progress.percent),
  } satisfies WorkspaceProgress);
}

function createProgressReporter(operation: "startup" | "review" | "ingest" | "foundation"): ProgressReporter {
  return (progress) => {
    emitWorkspaceProgress({
      ...progress,
      message: operation === "startup" ? `Startup sample: ${progress.message}` :
        operation === "review" ? `Review sample: ${progress.message}` :
        operation === "foundation" ? `Foundation load: ${progress.message}` :
        `Parsing input: ${progress.message}`,
    });
  };
}

async function readLines(filePath: string): Promise<ParsedLine[]> {
  const raw = await readFile(filePath, "utf-8");
  return parseLines(raw, filePath);
}

async function readGzipLines(path: string): Promise<ParsedLine[]> {
  const buffer = await readFile(path);
  const raw = decodeTextBuffer(gunzipSync(buffer));
  return parseLines(raw, path);
}

function openZipFile(path: string): Promise<yauzl.ZipFile> {
  return new Promise((resolvePromise, reject) => {
    yauzl.open(path, { lazyEntries: true, autoClose: true }, (error, archive) => {
      if (error || !archive) {
        reject(error ?? new Error(`Unable to open zip archive: ${path}`));
        return;
      }
      resolvePromise(archive);
    });
  });
}

function openZipBuffer(buffer: Buffer): Promise<yauzl.ZipFile> {
  return new Promise((resolvePromise, reject) => {
    yauzl.fromBuffer(buffer, { lazyEntries: true, autoClose: true }, (error, archive) => {
      if (error || !archive) {
        reject(error ?? new Error("Unable to open nested zip archive from buffer."));
        return;
      }
      resolvePromise(archive);
    });
  });
}

function openZipEntryStream(archive: yauzl.ZipFile, entry: yauzl.Entry): Promise<NodeJS.ReadableStream> {
  return new Promise((resolvePromise, reject) => {
    archive.openReadStream(entry, (error, stream) => {
      if (error || !stream) {
        reject(error ?? new Error(`Unable to read zip entry: ${entry.fileName}`));
        return;
      }
      resolvePromise(stream);
    });
  });
}

function collectStreamBuffer(stream: NodeJS.ReadableStream): Promise<Buffer> {
  return new Promise((resolvePromise, reject) => {
    const chunks: Buffer[] = [];
    stream.on("data", (chunk) => {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    });
    stream.once("end", () => resolvePromise(Buffer.concat(chunks)));
    stream.once("error", reject);
  });
}

async function readZipLinesFromArchive(
  openArchive: () => Promise<yauzl.ZipFile>,
  sourcePath: string,
  depth = 0,
): Promise<ParsedLine[]> {
  if (depth > 4) {
    return [];
  }

  const archive = await openArchive();
  const out: ParsedLine[] = [];

  await new Promise<void>((resolvePromise, reject) => {
    const entryTasks: Promise<void>[] = [];
    let settled = false;

    const rejectOnce = (error: unknown) => {
      if (settled) {
        return;
      }
      settled = true;
      try {
        archive.close();
      } catch {
        // Ignore close failures while unwinding the zip reader.
      }
      reject(error instanceof Error ? error : new Error(String(error)));
    };

    archive.on("error", rejectOnce);
    archive.on("entry", (entry) => {
      if (entry.fileName.endsWith("/")) {
        archive.readEntry();
        return;
      }

      const entryTask = (async () => {
        const stream = await openZipEntryStream(archive, entry);
        const buffer = await collectStreamBuffer(stream);
        const nestedSource = `${sourcePath}!${entry.fileName}`;

        if (isZipFile(entry.fileName)) {
          appendAll(out, await readZipLinesFromArchive(() => openZipBuffer(buffer), nestedSource, depth + 1));
          return;
        }

        if (isGzipFile(entry.fileName)) {
          appendAll(out, parseLines(decodeTextBuffer(gunzipSync(buffer)), nestedSource));
          return;
        }

        if (isTextFile(entry.fileName)) {
          appendAll(out, parseLines(decodeTextBuffer(buffer), nestedSource));
        }
      })();

      entryTasks.push(entryTask);
      entryTask.then(() => {
        if (!settled) {
          archive.readEntry();
        }
      }).catch(rejectOnce);
    });
    archive.on("end", () => {
      Promise.all(entryTasks)
        .then(() => {
          if (!settled) {
            settled = true;
            resolvePromise();
          }
        })
        .catch(rejectOnce);
    });

    archive.readEntry();
  });

  return out;
}

async function readZipLines(path: string): Promise<ParsedLine[]> {
  return readZipLinesFromArchive(() => openZipFile(path), path);
}

function selectPreviewLines(lines: ParsedLine[], maxLinesPerSource: number | undefined): ParsedLine[] {
  if (!maxLinesPerSource || lines.length <= maxLinesPerSource) {
    return lines;
  }
  const headCount = Math.ceil(maxLinesPerSource / 2);
  const tailCount = Math.floor(maxLinesPerSource / 2);
  const head = lines.slice(0, headCount);
  const tail = tailCount > 0 ? lines.slice(-tailCount) : [];
  const seen = new Set<string>();
  const out: ParsedLine[] = [];

  for (const line of [...head, ...tail]) {
    if (seen.has(line.id)) {
      continue;
    }
    seen.add(line.id);
    out.push(line);
  }

  return out;
}

function normalizeLookupKey(value: string | undefined | null): string {
  return String(value ?? "")
    .toUpperCase()
    .replace(/^CP\s+/, "")
    .replace(/\s+/g, " ")
    .trim();
}

function addSingleLookup<T>(map: Map<string, T>, row: T, keys: Array<string | undefined | null>): void {
  for (const key of keys) {
    const normalized = normalizeLookupKey(key);
    if (normalized && !map.has(normalized)) {
      map.set(normalized, row);
    }
  }
}

function addMultiLookup<T>(map: Map<string, T[]>, row: T, keys: Array<string | undefined | null>): void {
  for (const key of keys) {
    const normalized = normalizeLookupKey(key);
    if (!normalized) continue;
    const bucket = map.get(normalized) ?? [];
    bucket.push(row);
    map.set(normalized, bucket);
  }
}

function parseCsvTable(raw: string): string[][] {
  const rows: string[][] = [];
  let currentRow: string[] = [];
  let currentCell = "";
  let inQuotes = false;

  for (let index = 0; index < raw.length; index += 1) {
    const char = raw[index];
    const next = raw[index + 1];

    if (char === "\"") {
      if (inQuotes && next === "\"") {
        currentCell += "\"";
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (!inQuotes && char === ",") {
      currentRow.push(currentCell);
      currentCell = "";
      continue;
    }

    if (!inQuotes && (char === "\n" || char === "\r")) {
      if (char === "\r" && next === "\n") {
        index += 1;
      }
      currentRow.push(currentCell);
      rows.push(currentRow);
      currentRow = [];
      currentCell = "";
      continue;
    }

    currentCell += char;
  }

  if (currentCell.length || currentRow.length) {
    currentRow.push(currentCell);
    rows.push(currentRow);
  }

  return rows;
}

function parseNumericMilepost(value: string | undefined | null): number | null {
  const normalized = String(value ?? "").trim();
  if (!normalized) {
    return null;
  }
  const match = /-?\d+(?:\.\d+)?/.exec(normalized);
  if (!match) {
    return null;
  }
  const parsed = Number(match[0]);
  return Number.isFinite(parsed) ? parsed : null;
}

function addControlPointMilepostCandidate(
  map: Map<string, number>,
  controlPoint: string | undefined | null,
  ...candidates: Array<number | null | undefined>
): void {
  const key = normalizeLookupKey(controlPoint);
  if (!key) {
    return;
  }
  for (const candidate of candidates) {
    if (typeof candidate !== "number" || !Number.isFinite(candidate) || candidate <= 0) {
      continue;
    }
    const current = map.get(key);
    if (current === undefined || candidate < current) {
      map.set(key, candidate);
    }
  }
}

async function loadStaticComponentMilepostRows(path: string): Promise<StaticComponentMilepostRow[]> {
  const raw = await readFile(path, "utf-8").catch(() => "");
  if (!raw) {
    return [];
  }

  const table = parseCsvTable(raw);
  const header = table[0] ?? [];
  if (!header.length) {
    return [];
  }

  const indexByName = new Map<string, number>();
  for (let index = 0; index < header.length; index += 1) {
    indexByName.set(header[index], index);
  }

  const getValue = (row: string[], key: string): string => row[indexByName.get(key) ?? -1] ?? "";
  return table.slice(1).map((row) => ({
    ControlPoint: getValue(row, "ControlPoint").trim(),
    Milepost: getValue(row, "Milepost").trim(),
    LeftLimitMPRange: getValue(row, "LeftLimitMPRange").trim(),
    RightLimitMPRange: getValue(row, "RightLimitMPRange").trim(),
  }));
}

async function loadControlPointMilepostByNumber(): Promise<Map<string, number>> {
  const [signalRows, switchRows, trackRows] = await Promise.all([
    loadStaticComponentMilepostRows(foundationPath("exports", "raw", "sql_foundation", "tmdsDatabaseStatic.signal_detail_full.csv")),
    loadStaticComponentMilepostRows(foundationPath("exports", "raw", "sql_foundation", "tmdsDatabaseStatic.switch_detail_full.csv")),
    loadStaticComponentMilepostRows(foundationPath("exports", "raw", "sql_foundation", "tmdsDatabaseStatic.track_detail_full.csv")),
  ]);

  const out = new Map<string, number>();
  for (const row of signalRows) {
    addControlPointMilepostCandidate(out, row.ControlPoint, parseNumericMilepost(row.Milepost));
  }
  for (const row of switchRows) {
    addControlPointMilepostCandidate(out, row.ControlPoint, parseNumericMilepost(row.Milepost));
  }
  for (const row of trackRows) {
    addControlPointMilepostCandidate(
      out,
      row.ControlPoint,
      parseNumericMilepost(row.LeftLimitMPRange),
      parseNumericMilepost(row.RightLimitMPRange),
    );
  }
  return out;
}

async function loadHostInventoryRows(): Promise<HostInventoryRow[]> {
  const raw = await readFile("C:\\Users\\Ji\\Music\\configurations.csv", "utf-8").catch(() => "");
  if (!raw) {
    return [];
  }

  const table = parseCsvTable(raw);
  const header = table[0] ?? [];
  if (!header.length) {
    return [];
  }

  const indexByName = new Map<string, number>();
  for (let index = 0; index < header.length; index += 1) {
    indexByName.set(header[index], index);
  }

  const getValue = (row: string[], key: string): string => row[indexByName.get(key) ?? -1] ?? "";

  return table
    .slice(1)
    .map((row) => ({
      name: getValue(row, "name").trim(),
      configuration_type_name: getValue(row, "configuration_type_name").trim(),
      operating_system_name: getValue(row, "operating_system_name").trim(),
      primary_ip: getValue(row, "primary_ip").trim(),
      location_name: getValue(row, "location_name").trim(),
      hostname: getValue(row, "hostname").trim(),
      notes: getValue(row, "notes").trim(),
      manufacturer_name: getValue(row, "manufacturer_name").trim(),
      model_name: getValue(row, "model_name").trim(),
      dnet_connection_type: getValue(row, "dnet_connection_type").trim(),
      dnet_lte_status: getValue(row, "dnet_lte_status").trim(),
    }))
    .filter((row) => row.name && row.primary_ip);
}

function getMnemonicSuffix(mnemonic: string): string {
  const match = /([A-Z]{3})$/i.exec(String(mnemonic).trim());
  return match ? match[1].toUpperCase() : "";
}

function getMnemonicNumericPrefix(mnemonic: string): string {
  const match = /^(\d+)/.exec(String(mnemonic).trim());
  return match ? match[1] : "";
}

async function loadAssignmentLongNameFallbacks(): Promise<Map<string, string>> {
  const raw = await readFile(foundationPath("exports", "normalized", "cp_assignment_resolved_patterns.csv"), "utf-8").catch(() => "");
  if (!raw) {
    return new Map();
  }

  const table = parseCsvTable(raw);
  const header = table[0] ?? [];
  if (!header.length) {
    return new Map();
  }

  const indexByName = new Map<string, number>();
  for (let index = 0; index < header.length; index += 1) {
    indexByName.set(header[index], index);
  }

  const getValue = (row: string[], key: string): string => row[indexByName.get(key) ?? -1] ?? "";
  const countsBySuffix = new Map<string, Map<string, number>>();

  for (const row of table.slice(1)) {
    const mnemonic = getValue(row, "resolved_mnemonic").trim();
    const longName = getValue(row, "resolved_long_name").trim();
    const suffix = getMnemonicSuffix(mnemonic);
    if (!assignmentLongNameFallbackSuffixes.has(suffix) || isWeakAssignmentLongNameValue(longName, mnemonic)) {
      continue;
    }
    const bucket = countsBySuffix.get(suffix) ?? new Map<string, number>();
    bucket.set(longName, (bucket.get(longName) ?? 0) + 1);
    countsBySuffix.set(suffix, bucket);
  }

  const out = new Map<string, string>();
  for (const [suffix, counts] of countsBySuffix.entries()) {
    const ranked = Array.from(counts.entries()).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
    const winner = ranked[0];
    if (!winner) {
      continue;
    }
    const winnerCount = winner[1];
    const runnerUpCount = ranked[1]?.[1] ?? 0;
    if (winnerCount >= 3 && winnerCount > runnerUpCount * 2) {
      out.set(suffix, winner[0]);
    }
  }

  return out;
}

async function loadStaticComponentLookupRows(): Promise<StaticComponentLookupRow[]> {
  const raw = await readFile(foundationPath("exports", "raw", "sql_foundation", "tmdsDatabaseStatic.component_lookup.csv"), "utf-8").catch(() => "");
  if (!raw) {
    return [];
  }

  const table = parseCsvTable(raw);
  const header = table[0] ?? [];
  if (!header.length) {
    return [];
  }

  const indexByName = new Map<string, number>();
  for (let index = 0; index < header.length; index += 1) {
    indexByName.set(header[index], index);
  }

  const getValue = (row: string[], key: string): string => row[indexByName.get(key) ?? -1] ?? "";
  return table.slice(1).map((row) => ({
    component_uid: getValue(row, "component_uid"),
    component_family: getValue(row, "component_family"),
    parent_control_point_uid: getValue(row, "parent_control_point_uid"),
    component_name: getValue(row, "component_name"),
    component_secondary_name: getValue(row, "component_secondary_name"),
    component_detail_name: getValue(row, "component_detail_name"),
  }));
}

async function loadDynamicComponentContextRows(): Promise<DynamicComponentContextRow[]> {
  const paths = [
    foundationPath("exports", "raw", "sql_foundation", "tmdsDatabaseDynamic.bulletin_component_context.csv"),
    foundationPath("exports", "raw", "sql_foundation", "tmdsDatabaseDynamic.authority_component_context.csv"),
  ];
  const rows: DynamicComponentContextRow[] = [];

  for (const path of paths) {
    const raw = await readFile(path, "utf-8").catch(() => "");
    if (!raw) {
      continue;
    }

    const table = parseCsvTable(raw);
    const header = table[0] ?? [];
    if (!header.length) {
      continue;
    }

    const indexByName = new Map<string, number>();
    for (let index = 0; index < header.length; index += 1) {
      indexByName.set(header[index], index);
    }

    const getValue = (row: string[], key: string): string => row[indexByName.get(key) ?? -1] ?? "";
    rows.push(
      ...table.slice(1).map((row) => ({
        component_family: getValue(row, "component_family"),
        component_name: getValue(row, "component_name"),
        component_secondary_name: getValue(row, "component_secondary_name"),
      })),
    );
  }

  return rows;
}

function buildAssignmentMnemonicFallbacks(rows: CodeStationAssignmentMapRow[]): Map<string, string> {
  const countsByMnemonic = new Map<string, Map<string, number>>();
  for (const row of rows) {
    for (const entry of [...row.control_assignments, ...row.indication_assignments]) {
      const mnemonic = entry.mnemonic.trim();
      const longName = entry.long_name.trim();
      if (!mnemonic || isWeakAssignmentLongNameValue(longName, mnemonic) || isBlankAssignment(entry)) {
        continue;
      }
      const bucket = countsByMnemonic.get(mnemonic) ?? new Map<string, number>();
      bucket.set(longName, (bucket.get(longName) ?? 0) + 1);
      countsByMnemonic.set(mnemonic, bucket);
    }
  }

  const out = new Map<string, string>();
  for (const [mnemonic, counts] of countsByMnemonic.entries()) {
    const ranked = Array.from(counts.entries()).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
    const winner = ranked[0];
    if (!winner) {
      continue;
    }
    const winnerCount = winner[1];
    const runnerUpCount = ranked[1]?.[1] ?? 0;
    if (ranked.length === 1 || (winnerCount >= 3 && winnerCount > runnerUpCount * 2)) {
      out.set(mnemonic, winner[0]);
    }
  }
  return out;
}

function normalizeComponentValue(value: string): string {
  const trimmed = String(value ?? "").trim();
  return trimmed && trimmed.toUpperCase() !== "NULL" && trimmed.toUpperCase() !== "NOTHING" ? trimmed : "";
}

function normalizeAssignmentLongNameValue(value: string): string {
  return normalizeComponentValue(value)
    .replace(/\s+/g, " ")
    .trim();
}

function isWeakAssignmentLongNameValue(value: string, mnemonic = ""): boolean {
  const normalized = normalizeAssignmentLongNameValue(value).toUpperCase();
  const normalizedMnemonic = normalizeComponentValue(mnemonic).toUpperCase();
  if (!normalized) {
    return true;
  }
  if (normalizedMnemonic && normalized === normalizedMnemonic) {
    return true;
  }
  if (normalizedMnemonic && normalized === `${normalizedMnemonic} MISC DEVICE`) {
    return true;
  }
  return (
    normalized === "INDICATEONLY" ||
    normalized === "INDICATE ONLY" ||
    normalized === "CONTROLONLY" ||
    normalized === "CONTROL ONLY"
  );
}

function extractComponentMnemonicTokens(value: string): string[] {
  const normalized = normalizeComponentValue(value).toUpperCase();
  if (!normalized) {
    return [];
  }
  if (/^[A-Z0-9]+$/.test(normalized)) {
    return [normalized];
  }
  return normalized
    .split(/\s+-\s+|\/|,|;/)
    .map((token) => token.trim())
    .filter((token) => /^[A-Z0-9]+$/.test(token));
}

function extractComponentLookupMnemonic(row: StaticComponentLookupRow): string {
  const componentName = normalizeComponentValue(row.component_name).toUpperCase();
  if (/^[A-Z0-9]+$/.test(componentName)) {
    return componentName;
  }
  const detailHead = normalizeComponentValue(row.component_detail_name).split(";")[0]?.trim().toUpperCase() ?? "";
  if (/^[A-Z0-9]+$/.test(detailHead)) {
    return detailHead;
  }
  return "";
}

function pickMostFrequentValue(counts: Map<string, number>): string {
  const ranked = Array.from(counts.entries()).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
  return ranked[0]?.[0] ?? "";
}

function buildStaticComponentHints(rows: StaticComponentLookupRow[]): Map<string, { family?: string; component?: string; secondary?: string }> {
  const buckets = new Map<string, { family: Map<string, number>; component: Map<string, number>; secondary: Map<string, number> }>();
  for (const row of rows) {
    const controlPoint = normalizeLookupKey(String(row.parent_control_point_uid ?? "").trim());
    const mnemonic = extractComponentLookupMnemonic(row);
    if (!controlPoint || !mnemonic) {
      continue;
    }
    const key = `${controlPoint}:${normalizeLookupKey(mnemonic)}`;
    const bucket = buckets.get(key) ?? { family: new Map(), component: new Map(), secondary: new Map() };
    const family = normalizeComponentValue(row.component_family).toLowerCase();
    const component = normalizeComponentValue(row.component_name);
    const secondary = normalizeComponentValue(row.component_secondary_name);
    if (family) {
      bucket.family.set(family, (bucket.family.get(family) ?? 0) + 1);
    }
    if (component) {
      bucket.component.set(component, (bucket.component.get(component) ?? 0) + 1);
    }
    if (secondary) {
      bucket.secondary.set(secondary, (bucket.secondary.get(secondary) ?? 0) + 1);
    }
    buckets.set(key, bucket);
  }

  const out = new Map<string, { family?: string; component?: string; secondary?: string }>();
  for (const [key, bucket] of buckets.entries()) {
    const family = pickMostFrequentValue(bucket.family);
    const component = pickMostFrequentValue(bucket.component);
    const secondary = pickMostFrequentValue(bucket.secondary);
    out.set(key, {
      family: family || undefined,
      component: component || undefined,
      secondary: secondary || undefined,
    });
  }
  return out;
}

function buildComponentContextLongName(
  mnemonic: string,
  family: string,
  component: string,
  secondary: string,
): string {
  const normalizedMnemonic = mnemonic.trim().toUpperCase();
  const normalizedFamily = normalizeComponentValue(family).toLowerCase();
  const normalizedComponent = normalizeComponentValue(component);
  const normalizedSecondary = normalizeComponentValue(secondary);
  if (normalizedFamily === "track" && normalizedSecondary) {
    return `${normalizedSecondary} TRACK`;
  }
  if (normalizedComponent && normalizedComponent.toUpperCase() !== normalizedMnemonic) {
    return normalizedComponent;
  }
  if (normalizedSecondary && normalizedSecondary.toUpperCase() !== normalizedMnemonic) {
    return normalizedSecondary;
  }
  return "";
}

function buildComponentMnemonicFallbacks(
  staticRows: StaticComponentLookupRow[],
  dynamicRows: DynamicComponentContextRow[],
): Map<string, string> {
  const countsByMnemonic = new Map<string, Map<string, number>>();
  const addCandidate = (mnemonic: string, candidate: string): void => {
    const normalizedMnemonic = mnemonic.trim().toUpperCase();
    const normalizedCandidate = candidate.trim();
    if (!normalizedMnemonic || !normalizedCandidate || normalizedCandidate.toUpperCase() === normalizedMnemonic) {
      return;
    }
    const bucket = countsByMnemonic.get(normalizedMnemonic) ?? new Map<string, number>();
    bucket.set(normalizedCandidate, (bucket.get(normalizedCandidate) ?? 0) + 1);
    countsByMnemonic.set(normalizedMnemonic, bucket);
  };

  for (const row of staticRows) {
    for (const mnemonic of extractComponentMnemonicTokens(extractComponentLookupMnemonic(row))) {
      const candidate = buildComponentContextLongName(
        mnemonic,
        row.component_family,
        row.component_name,
        row.component_secondary_name,
      );
      addCandidate(mnemonic, candidate);
    }
  }

  for (const row of dynamicRows) {
    for (const mnemonic of extractComponentMnemonicTokens(row.component_name)) {
      const candidate = buildComponentContextLongName(
        mnemonic,
        row.component_family,
        row.component_name,
        row.component_secondary_name,
      );
      addCandidate(mnemonic, candidate);
    }
  }

  const out = new Map<string, string>();
  for (const [mnemonic, counts] of countsByMnemonic.entries()) {
    const ranked = Array.from(counts.entries()).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
    const winner = ranked[0];
    if (!winner) {
      continue;
    }
    const winnerCount = winner[1];
    const runnerUpCount = ranked[1]?.[1] ?? 0;
    if (ranked.length === 1 || (winnerCount >= 3 && winnerCount > runnerUpCount * 2)) {
      out.set(mnemonic, winner[0]);
    }
  }
  return out;
}

function buildLongNameFromStaticHints(
  entry: CodeAssignmentEntry,
  staticHints: { family?: string; component?: string; secondary?: string } | undefined,
): string {
  if (!staticHints) {
    return "";
  }
  return buildComponentContextLongName(
    entry.mnemonic,
    staticHints.family ?? "",
    staticHints.component ?? "",
    staticHints.secondary ?? "",
  );
}

function normalizeAssignmentEntries(
  entries: CodeAssignmentEntry[],
  controlPointNumber: string,
  globalSuffixNames: Map<string, string>,
  globalExactNames: Map<string, string>,
  componentExactNames: Map<string, string>,
  staticComponentHints: Map<string, { family?: string; component?: string; secondary?: string }>,
): CodeAssignmentEntry[] {
  const rowSuffixNames = new Map<string, string>();
  for (const entry of entries) {
    const suffix = getMnemonicSuffix(entry.mnemonic);
    if (!assignmentLongNameFallbackSuffixes.has(suffix)) {
      continue;
    }
    if (!isWeakAssignmentLongNameValue(entry.long_name, entry.mnemonic)) {
      rowSuffixNames.set(suffix, normalizeAssignmentLongNameValue(entry.long_name));
    }
  }

  const resolvedPrefixes = new Set<string>();
  const firstPass = entries.map((entry) => {
    const suffix = getMnemonicSuffix(entry.mnemonic);
    const prefix = getMnemonicNumericPrefix(entry.mnemonic);
    const hasWeakLongName = isWeakAssignmentLongNameValue(entry.long_name, entry.mnemonic);
    const staticHints = staticComponentHints.get(`${normalizeLookupKey(controlPointNumber)}:${normalizeLookupKey(entry.mnemonic)}`);
    const staticLongName = buildLongNameFromStaticHints(entry, staticHints);
    const preferStationTrackName = String(staticHints?.family ?? "").toLowerCase() === "track" && Boolean(normalizeComponentValue(staticHints?.secondary ?? ""));
    const weakSelfNamedTrackLongName =
      String(staticHints?.family ?? "").toLowerCase() === "track" &&
      Boolean(entry.long_name) &&
      hasWeakSelfNamedAssignmentLongName(entry);
    if (entry.long_name && (hasWeakLongName || weakSelfNamedTrackLongName)) {
      if (preferStationTrackName && staticLongName) {
        if (prefix) {
          resolvedPrefixes.add(prefix);
        }
        return {
          ...entry,
          long_name: staticLongName,
          family_hint: staticHints?.family,
          component_hint: staticHints?.component,
          secondary_hint: staticHints?.secondary,
        };
      }
      const exactLongName = globalExactNames.get(entry.mnemonic.trim());
      if (exactLongName) {
        if (prefix) {
          resolvedPrefixes.add(prefix);
        }
        return {
          ...entry,
          long_name: exactLongName,
          family_hint: staticHints?.family,
          component_hint: staticHints?.component,
          secondary_hint: staticHints?.secondary,
        };
      }
      const componentExactLongName = componentExactNames.get(entry.mnemonic.trim().toUpperCase());
      if (componentExactLongName) {
        if (prefix) {
          resolvedPrefixes.add(prefix);
        }
        return {
          ...entry,
          long_name: componentExactLongName,
          family_hint: staticHints?.family,
          component_hint: staticHints?.component,
          secondary_hint: staticHints?.secondary,
        };
      }
      if (staticLongName) {
        if (prefix) {
          resolvedPrefixes.add(prefix);
        }
        return {
          ...entry,
          long_name: staticLongName,
          family_hint: staticHints?.family,
          component_hint: staticHints?.component,
          secondary_hint: staticHints?.secondary,
        };
      }
    }
    if (!assignmentLongNameFallbackSuffixes.has(suffix) || !hasWeakLongName) {
      if (prefix && !hasWeakLongName) {
        resolvedPrefixes.add(prefix);
      }
      return staticHints
        ? { ...entry, family_hint: staticHints.family, component_hint: staticHints.component, secondary_hint: staticHints.secondary }
        : entry;
    }
    const rowLongName = rowSuffixNames.get(suffix);
    if (rowLongName) {
      resolvedPrefixes.add(prefix);
      return {
        ...entry,
        long_name: rowLongName,
        family_hint: staticHints?.family,
        component_hint: staticHints?.component,
        secondary_hint: staticHints?.secondary,
      };
    }
    if (staticLongName) {
      if (prefix) {
        resolvedPrefixes.add(prefix);
      }
      return {
        ...entry,
        long_name: staticLongName,
        family_hint: staticHints?.family,
        component_hint: staticHints?.component,
        secondary_hint: staticHints?.secondary,
      };
    }
    return staticHints
      ? { ...entry, family_hint: staticHints.family, component_hint: staticHints.component, secondary_hint: staticHints.secondary }
      : entry;
  });

  return firstPass.map((entry) => {
    const suffix = getMnemonicSuffix(entry.mnemonic);
    if (!assignmentLongNameFallbackSuffixes.has(suffix) || !isWeakAssignmentLongNameValue(entry.long_name, entry.mnemonic)) {
      return entry;
    }
    const globalLongName = globalSuffixNames.get(suffix);
    if (globalLongName) {
      return { ...entry, long_name: globalLongName };
    }
    return entry;
  });
}

function summarizeIndicationWordCoverage(entries: CodeAssignmentEntry[]): string {
  const namedEntries = entries.filter((entry) => !isBlankAssignment(entry));
  const buckets = bucketConfiguredAssignments(namedEntries);
  const parts = [
    `signals=${buckets.signals.length ? `yes (${buckets.signals.length} bits)` : "no"}`,
    `tracks=${buckets.tracks.length ? `yes (${buckets.tracks.length} bits)` : "no"}`,
    `switches=${buckets.switches.length ? `yes (${buckets.switches.length} bits)` : "no"}`,
    `routes=${buckets.routes.length ? `yes (${buckets.routes.length} bits)` : "no"}`,
    `local/device=${buckets.local.length ? `yes (${buckets.local.length} bits)` : "no"}`,
    `other=${buckets.other.length ? `yes (${buckets.other.length} bits)` : "no"}`,
  ];
  return `This indication word covers: ${parts.join(", ")}`;
}

function parseLogTimestamp(timestamp?: string): number | null {
  if (!timestamp) return null;
  const match = /^(?:(\d{2})-(\d{2})-(\d{4})|(\d{4})-(\d{2})-(\d{2})) (\d{2}):(\d{2}):(\d{2})\.(\d{3,4})$/.exec(timestamp);
  const slashMatch = /^(\d{4})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{3,4})$/.exec(timestamp);
  if (!match && !slashMatch) return null;
  const year = Number(match ? (match[3] ?? match[4]) : slashMatch?.[1]);
  const month = Number(match ? (match[1] ?? match[5]) : slashMatch?.[2]);
  const day = Number(match ? (match[2] ?? match[6]) : slashMatch?.[3]);
  const hour = Number(match ? match[7] : slashMatch?.[4]);
  const minute = Number(match ? match[8] : slashMatch?.[5]);
  const second = Number(match ? match[9] : slashMatch?.[6]);
  const millisecond = Number(((match ? match[10] : slashMatch?.[7]) ?? "0").slice(0, 3).padEnd(3, "0"));
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

function describeDelta(from?: string, to?: string): string | null {
  const fromMs = parseLogTimestamp(from);
  const toMs = parseLogTimestamp(to);
  if (fromMs === null || toMs === null) {
    return null;
  }
  const delta = toMs - fromMs;
  return delta >= 0 ? `${delta} ms later` : `${Math.abs(delta)} ms earlier`;
}

function uniqueLines(values: string[]): string[] {
  return Array.from(new Set(values.filter(Boolean)));
}

function payloadStats(bits: string): { asserted: number; clear: number } {
  const asserted = bits.split("").filter((bit) => bit === "1").length;
  return { asserted, clear: bits.length - asserted };
}

function summarizeAssignmentFamilies(entries: CodeAssignmentEntry[]): string {
  const families = new Set<string>();
  for (const entry of entries) {
    const text = `${entry.long_name} ${entry.mnemonic}`.toUpperCase();
    if (text.includes("SIGNAL")) families.add("signal");
    if (text.includes("SWITCH")) families.add("switch");
    if (text.includes("TRACK")) families.add("track");
    if (text.includes("LOCAL") || text.includes("POWER") || text.includes("LIGHT")) families.add("local/power");
    if (text.includes("TIME")) families.add("timing");
    if (text.includes("ILLEGAL")) families.add("illegal-entry");
  }
  return Array.from(families).sort().join(", ");
}

function summarizeRouteContext(rows: RouteSwitchContextRow[]): string[] {
  const byRoute = new Map<string, { entry: string; exit: string; switches: string[] }>();
  for (const row of rows) {
    const key = row.route_guid || `${row.entry_signal_name}->${row.exit_signal_name}`;
    const current = byRoute.get(key) ?? {
      entry: row.entry_signal_name || "unknown entry signal",
      exit: row.exit_signal_name || "unknown exit signal",
      switches: [],
    };
    if (row.switch_name) {
      current.switches.push(`${row.switch_name}=${row.required_state}`);
    }
    byRoute.set(key, current);
  }

  return Array.from(byRoute.values())
    .slice(0, 6)
    .map((route) =>
      route.switches.length
        ? `${route.entry} -> ${route.exit}; ${route.switches.join("; ")}`
        : `${route.entry} -> ${route.exit}`,
    );
}

function describeSignalRouteLabel(component: StaticComponentLookupRow | undefined): string {
  if (!component) {
    return "unknown signal";
  }
  const secondary = normalizeComponentValue(component.component_secondary_name);
  const detail = normalizeComponentValue(component.component_detail_name);
  if (detail) {
    return detail;
  }
  return secondary || normalizeComponentValue(component.component_name) || "unknown signal";
}

function describeRouteSwitchRequirementRaw(switchName: string, requiredState: string): string {
  const normalizedSwitch = normalizeComponentValue(switchName) || "unknown switch";
  const normalizedState = String(requiredState ?? "").trim().toUpperCase();
  return normalizedState ? `${normalizedSwitch} = ${normalizedState}` : normalizedSwitch;
}

function routeMatchesSignalPair(
  row: RouteSwitchContextRow,
  entryUid: string,
  exitUid: string,
  entryName: string,
  exitName: string,
): boolean {
  const rowEntryUid = normalizeLookupKey(row.entry_signal_uid);
  const rowExitUid = normalizeLookupKey(row.exit_signal_uid);
  const normalizedEntryUid = normalizeLookupKey(entryUid);
  const normalizedExitUid = normalizeLookupKey(exitUid);
  if (rowEntryUid && rowExitUid && normalizedEntryUid && normalizedExitUid) {
    return rowEntryUid === normalizedEntryUid && rowExitUid === normalizedExitUid;
  }
  return row.entry_signal_name === entryName && row.exit_signal_name === exitName;
}

function routeTouchesSignal(row: RouteSwitchContextRow, signalUid: string, signalName: string): boolean {
  const normalizedSignalUid = normalizeLookupKey(signalUid);
  const rowEntryUid = normalizeLookupKey(row.entry_signal_uid);
  const rowExitUid = normalizeLookupKey(row.exit_signal_uid);
  if (normalizedSignalUid && (rowEntryUid || rowExitUid)) {
    return rowEntryUid === normalizedSignalUid || rowExitUid === normalizedSignalUid;
  }
  return row.entry_signal_name === signalName || row.exit_signal_name === signalName;
}

function collectCadRoutePairRowsByComponents(
  uidA: string,
  uidB: string,
  bundle: LogEnrichmentBundle | null,
): RouteSwitchContextRow[] {
  if (!bundle) {
    return [];
  }
  const componentA = bundle.componentByUid.get(normalizeLookupKey(uidA));
  const componentB = bundle.componentByUid.get(normalizeLookupKey(uidB));
  if (!componentA || !componentB) {
    return [];
  }
  const controlPointUid = normalizeLookupKey(componentA.parent_control_point_uid);
  if (!controlPointUid || controlPointUid !== normalizeLookupKey(componentB.parent_control_point_uid)) {
    return [];
  }
  const routes = bundle.routesByKey.get(controlPointUid) ?? [];
  const nameA = normalizeComponentValue(componentA.component_name);
  const nameB = normalizeComponentValue(componentB.component_name);
  return routes.filter((row) => routeMatchesSignalPair(row, uidA, uidB, nameA, nameB));
}

function collectCadSignalRouteRows(
  signalUid: string,
  bundle: LogEnrichmentBundle | null,
): RouteSwitchContextRow[] {
  if (!bundle) {
    return [];
  }
  const componentRow = bundle.componentByUid.get(normalizeLookupKey(signalUid));
  if (!componentRow) {
    return [];
  }
  const controlPointUid = normalizeLookupKey(componentRow.parent_control_point_uid);
  const routes = bundle.routesByKey.get(controlPointUid) ?? [];
  const signalName = normalizeComponentValue(componentRow.component_name);
  return routes.filter((row) => routeTouchesSignal(row, signalUid, signalName));
}

function formatRouteGroup(
  rows: RouteSwitchContextRow[],
  bundle: LogEnrichmentBundle | null,
): string[] {
  const grouped = new Map<string, { label: string; switches: string[] }>();
  for (const row of rows) {
    const entryComponent = row.entry_signal_uid ? bundle?.componentByUid.get(normalizeLookupKey(row.entry_signal_uid)) : undefined;
    const exitComponent = row.exit_signal_uid ? bundle?.componentByUid.get(normalizeLookupKey(row.exit_signal_uid)) : undefined;
    const entryLabel = describeSignalRouteLabel(entryComponent) || normalizeComponentValue(row.entry_signal_name) || "unknown entry";
    const exitLabel = describeSignalRouteLabel(exitComponent) || normalizeComponentValue(row.exit_signal_name) || "unknown exit";
    const key = row.route_guid || `${entryLabel}->${exitLabel}`;
    const current = grouped.get(key) ?? { label: `${entryLabel} -> ${exitLabel}`, switches: [] };
    if (row.switch_name || row.switch_tooltip_name) {
      const switchText = describeRouteSwitchRequirementRaw(row.switch_tooltip_name || row.switch_name, row.required_state);
      if (!current.switches.includes(switchText)) {
        current.switches.push(switchText);
      }
    }
    grouped.set(key, current);
  }
  return Array.from(grouped.values()).map((group) =>
    group.switches.length ? `${group.label}; ${group.switches.join("; ")}` : group.label,
  );
}

function findNearbyCadControlPointMessageForSignal(
  index: number,
  lines: ParsedLine[],
  events: ParsedLogEvent[],
  signalUid: string,
): ParsedLogEvent | null {
  const baseLine = lines[index];
  if (!baseLine?.timestamp) {
    return null;
  }
  const baseTime = parseLogTimestamp(baseLine.timestamp);
  if (baseTime === null) {
    return null;
  }
  let best: { event: ParsedLogEvent; delta: number } | null = null;
  for (let cursor = Math.max(0, index - 8); cursor <= Math.min(lines.length - 1, index + 8); cursor += 1) {
    if (cursor === index) {
      continue;
    }
    const candidateEvent = events[cursor];
    const candidateLine = lines[cursor];
    if (candidateEvent.family !== "cad-control-point-message" || !candidateLine?.timestamp) {
      continue;
    }
    if (candidateEvent.relatedUidA !== signalUid && candidateEvent.relatedUidB !== signalUid) {
      continue;
    }
    const candidateTime = parseLogTimestamp(candidateLine.timestamp);
    if (candidateTime === null) {
      continue;
    }
    const delta = Math.abs(candidateTime - baseTime);
    if (delta > 1500) {
      continue;
    }
    if (!best || delta < best.delta) {
      best = { event: candidateEvent, delta };
    }
  }
  return best?.event ?? null;
}

function formatAssignmentCatalog(title: string, entries: CodeAssignmentEntry[]): string[] {
  if (!entries.length) {
    return [];
  }
  const namedEntries = entries.filter((entry) => !isBlankAssignment(entry));
  const blankEntries = entries.length - namedEntries.length;
  return [
    title,
    `${entries.length} bits total${namedEntries.length !== entries.length ? `, ${namedEntries.length} named, ${blankEntries} blank/unassigned` : ""}`,
    ...entries.map((entry) => `${entry.bit_position}. ${entry.mnemonic} = ${describeAssignmentLongName(entry)}`),
  ];
}

function isBlankAssignment(entry: CodeAssignmentEntry): boolean {
  const mnemonic = entry.mnemonic.toUpperCase().trim();
  const longName = entry.long_name.toUpperCase().trim();
  return mnemonic === "BLANK" || longName === "BLANK" || longName === "BLANK CONTROL" || longName === "BLANK INDICATION";
}

function getAssertedPayloadPositions(bits: string): number[] {
  const asserted: number[] = [];
  for (let index = 0; index < bits.length; index += 1) {
    if (bits[index] === "1") {
      asserted.push(index + 1);
    }
  }
  return asserted;
}

function buildPositionMap(entries: CodeAssignmentEntry[]): Map<number, CodeAssignmentEntry> {
  const map = new Map<number, CodeAssignmentEntry>();
  for (const entry of entries) {
    const position = Number(entry.bit_position);
    if (Number.isInteger(position) && position > 0 && !map.has(position)) {
      map.set(position, entry);
    }
  }
  return map;
}

function canExpandIndicationPositionsByAssignment(
  payloadBits: string,
  wordNumber: string,
  assignmentRow: CodeStationAssignmentMapRow | null,
): boolean {
  if (!assignmentRow || Number(wordNumber) !== 1) {
    return false;
  }

  if (assignmentRow.indication_assignments.length !== payloadBits.length) {
    return false;
  }

  const positionMap = buildPositionMap(assignmentRow.indication_assignments);
  for (let position = 1; position <= payloadBits.length; position += 1) {
    if (!positionMap.has(position)) {
      return false;
    }
  }

  return true;
}

type ActiveIndicationBuckets = {
  signals: string[];
  switches: string[];
  tracks: string[];
  routes: string[];
  local: string[];
  other: string[];
  blank: string[];
};

type BucketedAssignments = {
  signals: CodeAssignmentEntry[];
  switches: CodeAssignmentEntry[];
  tracks: CodeAssignmentEntry[];
  routes: CodeAssignmentEntry[];
  local: CodeAssignmentEntry[];
  other: CodeAssignmentEntry[];
  blank: CodeAssignmentEntry[];
};

type ActiveAssignmentItem = {
  position: number;
  entry: CodeAssignmentEntry;
};

type BucketedActiveItems = {
  signals: ActiveAssignmentItem[];
  switches: ActiveAssignmentItem[];
  tracks: ActiveAssignmentItem[];
  routes: ActiveAssignmentItem[];
  local: ActiveAssignmentItem[];
  other: ActiveAssignmentItem[];
  blank: ActiveAssignmentItem[];
};

type ActiveIndicationSummary = {
  meaning: string[];
  structured: string[];
  buckets: ActiveIndicationBuckets;
  items: BucketedActiveItems;
  assertedPositions: number[];
  expanded: boolean;
};

function emptyActiveBuckets(): ActiveIndicationBuckets {
  return {
    signals: [],
    switches: [],
    tracks: [],
    routes: [],
    local: [],
    other: [],
    blank: [],
  };
}

function emptyConfiguredBuckets(): BucketedAssignments {
  return {
    signals: [],
    switches: [],
    tracks: [],
    routes: [],
    local: [],
    other: [],
    blank: [],
  };
}

function emptyActiveItemBuckets(): BucketedActiveItems {
  return {
    signals: [],
    switches: [],
    tracks: [],
    routes: [],
    local: [],
    other: [],
    blank: [],
  };
}

function classifyActiveEntry(entry: CodeAssignmentEntry): keyof ActiveIndicationBuckets {
  const familyHint = String(entry.family_hint ?? "").toLowerCase();
  if (familyHint === "switch") {
    return "switches";
  }
  if (familyHint === "signal") {
    return "signals";
  }
  if (familyHint === "track") {
    return "tracks";
  }
  if (familyHint === "route") {
    return "routes";
  }
  if (familyHint === "misc_device") {
    return "local";
  }

  const mnemonic = entry.mnemonic.toUpperCase();
  const text = `${entry.mnemonic} ${entry.long_name}`.toUpperCase();

  if (mnemonic === "BLANK" || text.includes("BLANK")) {
    return "blank";
  }
  if (text.includes("ROUTE")) {
    return "routes";
  }
  if (
    text.includes("SWITCH") ||
    mnemonic.endsWith("NWK") ||
    mnemonic.endsWith("RWK") ||
    mnemonic.endsWith("NWS") ||
    mnemonic.endsWith("RWS") ||
    mnemonic.endsWith("BLK")
  ) {
    return "switches";
  }
  if (
    text.includes("SIGNAL") ||
    mnemonic.endsWith("EGK") ||
    mnemonic.endsWith("WGK") ||
    mnemonic.endsWith("TEK")
  ) {
    return "signals";
  }
  if (
    text.includes("TRACK") ||
    mnemonic === "TK" ||
    mnemonic.endsWith("STK") ||
    mnemonic.endsWith("ATK") ||
    (mnemonic.endsWith("BK") && !mnemonic.endsWith("BLK"))
  ) {
    return "tracks";
  }
  if (
    text.includes("POWER") ||
    text.includes("LIGHT") ||
    text.includes("MANUAL") ||
    text.includes("LOCAL") ||
    text.includes("ILLEGAL") ||
    text.includes("DOOR") ||
    text.includes("MISC")
  ) {
    return "local";
  }
  return "other";
}

function hasResolvedAssignmentLongName(entry: CodeAssignmentEntry): boolean {
  return !isWeakAssignmentLongNameValue(entry.long_name, entry.mnemonic);
}

function hasWeakSelfNamedAssignmentLongName(entry: CodeAssignmentEntry): boolean {
  const mnemonic = entry.mnemonic.trim().toUpperCase();
  const longName = normalizeAssignmentLongNameValue(entry.long_name).toUpperCase();
  if (!mnemonic || !longName) {
    return false;
  }
  return longName === mnemonic || longName.startsWith(`${mnemonic} `);
}

function isIndicationAssignment(entry: CodeAssignmentEntry): boolean {
  const wordType = String(entry.word_type ?? "").trim().toUpperCase();
  return wordType === "I" || wordType === "INDICATION";
}

function isControlAssignment(entry: CodeAssignmentEntry): boolean {
  const wordType = String(entry.word_type ?? "").trim().toUpperCase();
  return wordType === "C" || wordType === "CONTROL";
}

function canonicalizeAssignmentLongName(entry: CodeAssignmentEntry): string {
  if (!isIndicationAssignment(entry)) {
    return "";
  }
  return canonicalIndicationLongNamesBySuffix.get(getMnemonicSuffix(entry.mnemonic)) ?? "";
}

function describeAssignmentLongName(entry: CodeAssignmentEntry): string {
  const familyHint = String(entry.family_hint ?? "").toLowerCase();
  const componentHint = normalizeComponentValue(entry.component_hint ?? "");
  const secondaryHint = normalizeComponentValue(entry.secondary_hint ?? "");
  const isTrackAssignment = familyHint === "track" || classifyActiveEntry(entry) === "tracks";
  const finalizeTrackLabel = (value: string): string => {
    let out = value.trim();
    if (!out) {
      return out;
    }
    if (!/\bTRACK\b/i.test(out)) {
      out = `${out} TRACK`;
    }
    if (isIndicationAssignment(entry) && !/\bINDICATION\b/i.test(out)) {
      out = `${out} INDICATION`;
    }
    return out;
  };
  const mnemonic = entry.mnemonic.trim().toUpperCase();
  const normalizedLongName = normalizeAssignmentLongNameValue(entry.long_name).toUpperCase();
  if (mnemonic === "BLANK" && (!normalizedLongName || normalizedLongName === "BLANK")) {
    if (isIndicationAssignment(entry)) {
      return "BLANK INDICATION";
    }
    if (isControlAssignment(entry)) {
      return "BLANK CONTROL";
    }
  }
  const canonicalLongName = canonicalizeAssignmentLongName(entry);
  if (canonicalLongName) {
    return canonicalLongName;
  }
  if (hasResolvedAssignmentLongName(entry)) {
    const resolved = entry.long_name.trim();
    if (isTrackAssignment && hasWeakSelfNamedAssignmentLongName(entry)) {
      if (secondaryHint) {
        return finalizeTrackLabel(secondaryHint);
      }
      if (componentHint && componentHint.toUpperCase() !== entry.mnemonic.trim().toUpperCase()) {
        return finalizeTrackLabel(componentHint);
      }
    }
    if (isTrackAssignment) {
      return finalizeTrackLabel(resolved);
    }
    return resolved;
  }
  if (isTrackAssignment && secondaryHint) {
    return finalizeTrackLabel(secondaryHint);
  }
  if (isTrackAssignment && componentHint && componentHint.toUpperCase() !== entry.mnemonic.trim().toUpperCase()) {
    return finalizeTrackLabel(componentHint);
  }
  if (isTrackAssignment) {
    return isIndicationAssignment(entry) ? "TRACK INDICATION" : "TRACK";
  }
  if (familyHint === "misc_device" && componentHint) {
    return componentHint;
  }
  if (componentHint) {
    return componentHint;
  }
  return entry.mnemonic.trim();
}

function formatActiveEntryLabel(position: number, entry: CodeAssignmentEntry): string {
  const label = describeAssignmentLongName(entry);
  if (label && label !== entry.mnemonic.trim()) {
    return `${position}. ${entry.mnemonic} = TRUE (${label})`;
  }
  return `${position}. ${entry.mnemonic} = TRUE`;
}

function formatBooleanEntryLabel(position: number | string, entry: CodeAssignmentEntry, value: string): string {
  const normalizedPosition = typeof position === "number" ? position : Number(position);
  const prefix = Number.isFinite(normalizedPosition) ? `${normalizedPosition}. ` : `${position}. `;
  const normalizedValue = String(value ?? "").trim().toUpperCase();
  const label = describeAssignmentLongName(entry);
  if (label && label !== entry.mnemonic.trim()) {
    return `${prefix}${entry.mnemonic} = ${normalizedValue || "UNKNOWN"} (${label})`;
  }
  return `${prefix}${entry.mnemonic} = ${normalizedValue || "UNKNOWN"}`;
}

function formatEntryShort(entry: CodeAssignmentEntry): string {
  return `${entry.mnemonic} (${describeAssignmentLongName(entry)})`;
}

type IndicationOperationalReading = {
  summary: string;
  details: string[];
};

function buildIndicationOperationalReading(
  activeItems: BucketedActiveItems,
  expanded: boolean,
): IndicationOperationalReading {
  if (!expanded) {
    return {
      summary: "Current state: active bits present, but this indication word is not fully grounded in the current word map.",
      details: [],
    };
  }

  const lines: string[] = [];
  const activeSummaryItems = [
    ...activeItems.switches,
    ...activeItems.signals,
    ...activeItems.tracks,
    ...activeItems.routes,
    ...activeItems.local,
    ...activeItems.other,
  ];
  const activeBlankItems = activeItems.blank;
  const summary = activeSummaryItems.length
    ? `Current state: active ${activeSummaryItems.map((item) => formatEntryShort(item.entry)).join(", ")}.`
    : activeBlankItems.length
      ? "Current state: active only in blank/unassigned indication positions."
      : "Current state: no named indication bits are active.";

  if (!activeSummaryItems.length && !activeBlankItems.length) {
    return {
      summary,
      details: [
        "Interpretation: indication snapshot; all logged bits are clear.",
      ],
    };
  }

  if (activeSummaryItems.length) {
    lines.push(
      "Interpretation: indication snapshot; the asserted bits below show the current reported field state.",
    );
  } else {
    lines.push(
      "Interpretation: indication snapshot; the asserted bits below currently map only to blank/unassigned positions in the configured indication word.",
    );
  }

  const groups: Array<{ title: string; items: ActiveAssignmentItem[] }> = [
    { title: "Active switch bits:", items: activeItems.switches },
    { title: "Active signal bits:", items: activeItems.signals },
    { title: "Active track bits:", items: activeItems.tracks },
    { title: "Active route bits:", items: activeItems.routes },
    { title: "Active local/device bits:", items: activeItems.local },
    { title: "Active other bits:", items: activeItems.other },
    { title: "Active blank/unassigned bits:", items: activeItems.blank },
  ];

  for (const group of groups) {
    if (!group.items.length) {
      continue;
    }
    lines.push(group.title);
    for (const item of group.items) {
      lines.push(formatActiveEntryLabel(item.position, item.entry));
    }
  }

  return { summary, details: lines };
}

function bucketConfiguredAssignments(entries: CodeAssignmentEntry[]): BucketedAssignments {
  const buckets = emptyConfiguredBuckets();
  for (const entry of entries) {
    const family = classifyActiveEntry(entry);
    buckets[family].push(entry);
  }
  return buckets;
}

function buildTrainRuntimeLines(rows: TrainRuntimeFoundationRow[]): string[] {
  if (!rows.length) {
    return [];
  }

  const out = ["Train / runtime context:"];
  for (const row of rows.slice(0, 4)) {
    const symbol = row.symbol || row.runtime_train_symbol || "unknown";
    const direction = row.direction || "";
    const movement = `${symbol}${direction ? ` ${direction}` : ""} ${row.origin || "unknown"} -> ${row.dest || "unknown"}`.trim();
    const routeName = row.track_route_name && row.track_route_name.toUpperCase() !== "ROUTE NAME" ? row.track_route_name : "";
    const hasMatchedRuntime =
      !!row.runtime_join_status &&
      row.runtime_join_status !== "engine_id_without_runtime_match" &&
      row.runtime_join_status !== "no_engine_id";
    const details = [
      row.track_name ? `track=${row.track_name}` : "",
      routeName ? `route=${routeName}` : "",
      row.authority_designation ? `authority=${row.authority_designation}` : "",
      ...(hasMatchedRuntime && row.runtime_join_status ? [`runtime=${row.runtime_join_status}`] : []),
      ...(hasMatchedRuntime && row.runtime_icd_interface_version ? [`ICD=${row.runtime_icd_interface_version}`] : []),
      ...(hasMatchedRuntime && row.runtime_loco_id ? [`loco=${row.runtime_loco_id}`] : []),
      ...(hasMatchedRuntime && row.runtime_departure_test_status ? [`departure test=${row.runtime_departure_test_status}`] : []),
    ].filter(Boolean);
    out.push(details.length ? `${movement}; ${details.join("; ")}` : movement);
  }
  return out;
}

function formatMemoryBytes(value: string | undefined): string {
  const trimmed = String(value ?? "").trim();
  const numeric = Number(trimmed);
  if (!trimmed || !Number.isFinite(numeric) || numeric < 0) {
    return trimmed || "unknown";
  }
  const mib = numeric / (1024 * 1024);
  return `${numeric} bytes (${mib.toFixed(mib >= 100 ? 0 : mib >= 10 ? 1 : 2)} MiB)`;
}

function normalizePayloadFieldValue(value: string | undefined): string {
  const trimmed = String(value ?? "").trim();
  return trimmed || "(blank)";
}

function describeTrainSubdivisionToken(token: string, rows: TrainRuntimeFoundationRow[]): string {
  const bulletinCodes = Array.from(
    new Set(
      rows
        .map((row) => String(row.bulletin_route_code ?? "").trim().toUpperCase())
        .filter(Boolean),
    ),
  );
  if (bulletinCodes.includes("SANDIE")) {
    return `SD SUB (token ${token})`;
  }
  const subdivisions = Array.from(
    new Set(
      rows
        .map((row) => String(row.subdivision ?? "").trim())
        .filter((value) => value && value.toUpperCase() !== "NONE"),
    ),
  );
  if (subdivisions.length === 1) {
    return `${subdivisions[0]} (token ${token})`;
  }
  return token;
}

type CadTrainPayloadDescriptor = {
  label: string;
  value: string;
};

function buildCadTrainPayloadDescriptors(
  payloadFields: string[],
  trainRows: TrainRuntimeFoundationRow[],
): CadTrainPayloadDescriptor[] {
  if (!payloadFields.length) {
    return [];
  }

  const labels = [
    "Subdivision token",
    "Event token",
    "Train UID",
    "Train symbol",
    "Direction",
    "Lead equipment",
    "Engineer name",
    "Engineer on-duty time",
    "Conductor name",
    "Conductor on-duty time",
    "Train GUID",
    "Engineer time-up",
    "Conductor time-up",
    "Trailing horsepower token",
    "Schedule status token",
    "Service slot",
    "Train length",
    "Loaded cars",
    "Empty cars",
    "Operating tons",
    "Length echo token",
    "Raw field 22",
    "Raw field 23",
    "Raw field 24",
    "Symbol echo",
    "Train type",
    "Raw field 27",
    "Raw field 28",
    "Raw field 29",
    "Raw field 30",
    "Home road",
    "Offset token",
  ];

  return payloadFields.map((field, index) => ({
    label: labels[index] ?? `Raw field ${index + 1}`,
    value:
      index === 0
        ? describeTrainSubdivisionToken(normalizePayloadFieldValue(field), trainRows)
        : normalizePayloadFieldValue(field),
  }));
}

function buildCadTrainPayloadLines(
  payloadFields: string[],
  trainRows: TrainRuntimeFoundationRow[],
): string[] {
  const descriptors = buildCadTrainPayloadDescriptors(payloadFields, trainRows);
  if (!descriptors.length) {
    return [];
  }
  return [
    "Payload breakdown:",
    ...descriptors.map((descriptor, index) => `${index + 1}. ${descriptor.label}: ${descriptor.value}`),
  ];
}

function findStationRowByNameToken(bundle: LogEnrichmentBundle | null, token: string | undefined): StationFoundationRow | null {
  if (!bundle || !token) {
    return null;
  }
  return bundle.stationByKey.get(normalizeLookupKey(token)) ?? null;
}

function parseSlashDateTime(value: string | undefined): number | null {
  if (!value) {
    return null;
  }
  const match = /^\s*(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$/.exec(value);
  if (!match) {
    return null;
  }
  const month = Number(match[1]);
  const day = Number(match[2]);
  const year = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6] ?? "0");
  if (![month, day, year, hour, minute, second].every(Number.isFinite)) {
    return null;
  }
  return new Date(year, month - 1, day, hour, minute, second).getTime();
}

function formatDurationShort(ms: number): string {
  const totalSeconds = Math.max(0, Math.round(ms / 1000));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const out: string[] = [];
  if (hours) out.push(`${hours}h`);
  if (minutes) out.push(`${minutes}m`);
  if (seconds || !out.length) out.push(`${seconds}s`);
  return out.join(" ");
}

function describeScheduleCheckDelta(timerCheckTime: string, scheduleTime: string): string | null {
  const timerMs = parseSlashDateTime(timerCheckTime);
  const scheduleMs = parseSlashDateTime(scheduleTime);
  if (timerMs === null || scheduleMs === null) {
    return null;
  }
  const delta = timerMs - scheduleMs;
  if (delta === 0) {
    return "Schedule status: timer check occurred exactly at the scheduled time.";
  }
  return delta > 0
    ? `Schedule status: timer check occurred ${formatDurationShort(delta)} after the scheduled time.`
    : `Schedule status: timer check occurred ${formatDurationShort(Math.abs(delta))} before the scheduled time.`;
}

function findNearbyFlashName(index: number, lines: ParsedLine[], events: ParsedLogEvent[]): ParsedLogEvent | null {
  const baseLine = lines[index];
  if (!baseLine?.timestamp) {
    return null;
  }
  const baseTime = parseLogTimestamp(baseLine.timestamp);
  if (baseTime === null) {
    return null;
  }
  let best: { event: ParsedLogEvent; delta: number; preferBefore: number } | null = null;
  for (let cursor = Math.max(0, index - 6); cursor <= Math.min(lines.length - 1, index + 6); cursor += 1) {
    if (cursor === index) {
      continue;
    }
    const candidateEvent = events[cursor];
    const candidateLine = lines[cursor];
    if (candidateEvent.family !== "flash-name" || !candidateLine?.timestamp) {
      continue;
    }
    const candidateTime = parseLogTimestamp(candidateLine.timestamp);
    if (candidateTime === null) {
      continue;
    }
    const delta = Math.abs(candidateTime - baseTime);
    if (delta > 1500) {
      continue;
    }
    const preferBefore = cursor < index ? 0 : 1;
    if (!best || delta < best.delta || (delta === best.delta && preferBefore < best.preferBefore)) {
      best = { event: candidateEvent, delta, preferBefore };
    }
  }
  return best?.event ?? null;
}

function describeIndicationWordScope(
  payloadBits: string,
  wordNumber: string,
  assignmentRow: CodeStationAssignmentMapRow | null,
): string {
  const wordIndex = Number(wordNumber);
  const payloadLength = payloadBits.length;
  if (!Number.isInteger(wordIndex) || wordIndex <= 0 || payloadLength <= 0) {
    return `Indication word: ${wordNumber}.`;
  }
  if (!assignmentRow) {
    return `Indication word: ${wordIndex}, carrying ${payloadLength} payload positions.`;
  }

  const totalPositions = assignmentRow.indication_assignments.length;
  const totalWords = Math.max(1, Math.ceil(totalPositions / payloadLength));
  const start = ((wordIndex - 1) * payloadLength) + 1;
  const end = Math.min(wordIndex * payloadLength, totalPositions);

  if (start > totalPositions) {
    return `Indication word: ${wordIndex}; current assignment map has ${totalPositions} configured positions.`;
  }
  if (totalWords === 1) {
    return `Indication word: ${wordIndex} of ${totalWords}, covering positions ${start}-${end}.`;
  }
  return `Indication word: ${wordIndex} of ${totalWords}, covering positions ${start}-${end}.`;
}

function formatActiveIndicationLines(
  payloadBits: string,
  wordNumber: string,
  assignmentRow: CodeStationAssignmentMapRow | null,
): ActiveIndicationSummary {
  const assertedPositions = getAssertedPayloadPositions(payloadBits);
  const structured = [
    `Asserted payload positions: ${assertedPositions.length ? assertedPositions.join(", ") : "none"}`,
  ];
  const buckets = emptyActiveBuckets();
  const items = emptyActiveItemBuckets();

  if (!assertedPositions.length) {
    return {
      meaning: ["Active at this line: no payload positions are asserted."],
      structured,
      buckets,
      items,
      assertedPositions,
      expanded: canExpandIndicationPositionsByAssignment(payloadBits, wordNumber, assignmentRow),
    };
  }

  if (!canExpandIndicationPositionsByAssignment(payloadBits, wordNumber, assignmentRow)) {
    return {
      meaning: [`Active payload positions at this line: ${assertedPositions.join(", ")}.`],
      structured,
      buckets,
      items,
      assertedPositions,
      expanded: false,
    };
  }

  const positionMap = buildPositionMap(assignmentRow?.indication_assignments ?? []);
  const activeEntries = assertedPositions
    .map((position) => ({
      position,
      entry: positionMap.get(position) ?? null,
    }))
    .filter((item) => item.entry);

  for (const item of activeEntries) {
    const family = classifyActiveEntry(item.entry as CodeAssignmentEntry);
    buckets[family].push(formatActiveEntryLabel(item.position, item.entry as CodeAssignmentEntry));
    items[family].push(item as ActiveAssignmentItem);
  }

  const meaning = ["Active at this line:"];
  if (buckets.signals.length) {
    meaning.push(`Signals: ${buckets.signals.join("; ")}`);
  }
  if (buckets.switches.length) {
    meaning.push(`Switches: ${buckets.switches.join("; ")}`);
  }
  if (buckets.tracks.length) {
    meaning.push(`Tracks: ${buckets.tracks.join("; ")}`);
  }
  if (buckets.routes.length) {
    meaning.push(`Routes: ${buckets.routes.join("; ")}`);
  }
  if (buckets.local.length) {
    meaning.push(`Local / device: ${buckets.local.join("; ")}`);
  }
  if (buckets.other.length) {
    meaning.push(`Other: ${buckets.other.join("; ")}`);
  }
  if (!buckets.signals.length && !buckets.switches.length && !buckets.tracks.length && !buckets.routes.length && !buckets.local.length && !buckets.other.length) {
    meaning.push("No named assignments are asserted in this payload.");
  }

  structured.push(...activeEntries.map((item) => formatActiveEntryLabel(item.position, item.entry as CodeAssignmentEntry)));

  return { meaning, structured, buckets, items, assertedPositions, expanded: true };
}

function parseCadTrainPayload(
  payload: string,
  sourceKind: "train-event" | "stream-receiver" | "control-server-message",
  codeServerName?: string,
): ParsedLogEvent | null {
  const parts = payload.split("|").map((part) => part.trim());
  if (parts.length < 4) {
    return null;
  }

  return {
    family: "cad-train-message",
    sourceKind,
    codeServerName,
    subdivisionToken: parts[0] ?? "",
    eventToken: parts[1] ?? "",
    trainUid: parts[2] ?? "",
    trainSymbol: parts[3] ?? "",
    payloadFieldCount: parts.length,
    payloadFields: parts,
    direction: parts[4] || undefined,
    leadEquipment: parts[5] || undefined,
    trainGuid: parts[10] || undefined,
    serviceSlot: parts[15] || undefined,
    trainType: parts[25] || undefined,
    homeRoadCode: parts[30] || undefined,
  };
}

function parseDelimitedVetmsPayload(raw: string): {
  messageCategory: string;
  messageType: string;
  stateChange?: string;
  payloadFields: string[];
  trainSymbol?: string;
  locoUid?: string;
  reportTime?: string;
  directionOfTravel?: string;
} | null {
  const normalized = normalizeAsciiControlWrappedPayload(raw).trim();
  if (!normalized) {
    return null;
  }
  const segments = normalized.split("|").map((segment) => segment.trim());
  const headerTokens = (segments[0] ?? "").split(";").map((token) => token.trim());
  if (headerTokens.length < 3 || !/^VETMS$/i.test(headerTokens[0] ?? "")) {
    return null;
  }
  const payloadFields = segments.slice(1);
  return {
    messageCategory: headerTokens[1] ?? "",
    messageType: headerTokens[2] ?? "",
    stateChange: headerTokens[3] || undefined,
    payloadFields,
    trainSymbol: payloadFields[0] || undefined,
    locoUid: payloadFields[1] || undefined,
    reportTime: payloadFields[2] || undefined,
    directionOfTravel: payloadFields[3] || undefined,
  };
}

function parseAngleBracketFields(raw: string): Map<string, string> {
  const fields = new Map<string, string>();
  for (const match of raw.matchAll(/<([^=<>]+)=([^<>]*)>/g)) {
    const key = match[1]?.trim();
    if (key) {
      fields.set(key, (match[2] ?? "").trim());
    }
  }
  return fields;
}

function parsePipeTraceEnvelope(raw: string): { level: string; component: string; traceLineNumber: string; message: string } | null {
  const timestamp = extractLogTimestamp(raw);
  if (!timestamp) {
    return null;
  }
  const body = raw.slice(timestamp.length);
  const withThread = /^\|[^|]+\|([^|]+)\|([^|]+)\|([^|]+)\|(.*)$/s.exec(body);
  if (withThread) {
    return {
      level: withThread[1].trim(),
      component: withThread[2].trim(),
      traceLineNumber: withThread[3].trim(),
      message: withThread[4].trim(),
    };
  }
  const withoutThread = /^\|([^|]+)\|([^|]+)\|([^|]+)\|(.*)$/s.exec(body);
  if (withoutThread) {
    return {
      level: withoutThread[1].trim(),
      component: withoutThread[2].trim(),
      traceLineNumber: withoutThread[3].trim(),
      message: withoutThread[4].trim(),
    };
  }
  return null;
}

function parseBooleanMnemonicEntries(raw: string): Array<{ position: string; mnemonic: string; value: string }> {
  return Array.from(raw.matchAll(/\((\d+)\)\s*([^=<>]+?)\s*=\s*(True|False)/gi))
    .map((match) => ({
      position: match[1].trim(),
      mnemonic: match[2].trim(),
      value: match[3].trim().toUpperCase(),
    }));
}

function normalizeAsciiControlWrappedPayload(raw: string): string {
  return raw.replace(/[\u0000-\u001F]+/g, "").trim();
}

function parseSpaceSeparatedHexBytes(raw: string): string[] {
  const bracketed = raw.replace(/[<>]/g, " ").trim();
  return bracketed.split(/\s+/).map((value) => value.trim()).filter((value) => /^[0-9A-F]{2}$/i.test(value));
}

function parseLogEvent(line: ParsedLine): ParsedLogEvent {
  const locoLogMarker = /^####:\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}\.\d{3,4}:\s*(.+)$/i.exec(line.raw);
  if (locoLogMarker) {
    return {
      family: "loco-log-marker",
      markerText: locoLogMarker[1].trim(),
    };
  }

  const locoLogEntry = /^(SYS|WARN|NOTE|ERR|INFO)\s*:\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}\.\d{3,4}:([^:]+):(.*)$/i.exec(line.raw);
  if (locoLogEntry) {
    return {
      family: "loco-log-entry",
      severity: locoLogEntry[1].trim().toUpperCase() as "SYS" | "WARN" | "NOTE" | "ERR" | "INFO",
      component: locoLogEntry[2].trim(),
      message: locoLogEntry[3].trim(),
    };
  }

  const officeTelemetrySummary = /^(OTX|ORX)\s*:\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}\.\d{3,4}:([^:]+):(?:Sending Office msg \[(\d+)\], Seq # \[([^\]]+)\], Dest \[([^\]]+)\]|Received msg \[(\d+)\] from Office, Seq # \[([^\]]+)\], Src \[([^\]]+)\])$/i.exec(line.raw);
  if (officeTelemetrySummary) {
    const isTx = officeTelemetrySummary[1].toUpperCase() === "OTX";
    return {
      family: "office-telemetry-summary",
      direction: isTx ? "TX" : "RX",
      channel: officeTelemetrySummary[2].trim(),
      messageId: String(isTx ? officeTelemetrySummary[3] ?? "" : officeTelemetrySummary[6] ?? "").trim(),
      sequence: String(isTx ? officeTelemetrySummary[4] ?? "" : officeTelemetrySummary[7] ?? "").trim(),
      peer: String(isTx ? officeTelemetrySummary[5] ?? "" : officeTelemetrySummary[8] ?? "").trim(),
    };
  }

  const officeTelemetryHex = /^(OTXD|ORXD):\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}\.\d{3,4}:([^:]+):\d+\s+\|\s*([0-9A-Fa-f ]+?)\s+\|/i.exec(line.raw);
  if (officeTelemetryHex) {
    return {
      family: "office-telemetry-hex",
      direction: officeTelemetryHex[1].toUpperCase() === "OTXD" ? "TX" : "RX",
      channel: officeTelemetryHex[2].trim(),
      payloadBytes: officeTelemetryHex[3].trim().split(/\s+/).filter(Boolean),
    };
  }

  const recorderDelimitedRecord = /^CHR\s*:\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}\.\d{3,4}:([^:]+):(.+)$/i.exec(line.raw);
  if (recorderDelimitedRecord) {
    return {
      family: "recorder-delimited-record",
      recorder: recorderDelimitedRecord[1].trim(),
      payloadFields: recorderDelimitedRecord[2].split("|").map((field) => field.trim()),
    };
  }

  const locomotiveRecorderRecord = /^(\d{4}\/\d{2}\/\d{2})\|(\d{2}:\d{2}:\d{2}\.\d{3,4})\|(.+)$/i.exec(line.raw);
  if (locomotiveRecorderRecord) {
    const payloadFields = locomotiveRecorderRecord[3].split("|").map((field) => field.trim());
    return {
      family: "locomotive-recorder-record",
      recordType: payloadFields[1] || "recorder",
      payloadFields,
    };
  }

  const resendControlGenisys = /^(?:(?:\d{2}-\d{2}-\d{4})|(?:\d{4}-\d{2}-\d{2})) \d{2}:\d{2}:\d{2}\.\d{3,4}\s+RESEND CONTROL GENISYS:\s+RETRY=(\d+)\/(\d+)\s+for\s+(\d+)\s+(.+?)\s+No Control Delivery Confirmed within\s+(\d+)\s+\[msec\]\.$/i.exec(line.raw);
  if (resendControlGenisys) {
    return {
      family: "genisys-control-resend",
      retryCurrent: resendControlGenisys[1],
      retryTotal: resendControlGenisys[2],
      controlPointNumber: resendControlGenisys[3],
      stationToken: resendControlGenisys[4].trim(),
      timeoutMs: resendControlGenisys[5],
    };
  }

  const socketRawFrame = /^(?:(?:\d{2}-\d{2}-\d{4})|(?:\d{4}-\d{2}-\d{2})) \d{2}:\d{2}:\d{2}\.\d{3,4}\s+([<>-]{3})\s+(XMT|RCV):([^:]+):(.+)$/i.exec(line.raw);
  if (socketRawFrame) {
    return {
      family: "socket-raw-frame",
      directionGlyph: socketRawFrame[1],
      socketAction: socketRawFrame[2].toUpperCase() as "XMT" | "RCV",
      stationToken: socketRawFrame[3].trim(),
      payloadBytes: Array.from(socketRawFrame[4].matchAll(/<([^<>]+)>/g)).map((match) => match[1].trim()).filter(Boolean),
    };
  }

  const pipeException = /^(?:(?:\d{2}-\d{2}-\d{4})|(?:\d{4}-\d{2}-\d{2})) \d{2}:\d{2}:\d{2}\.\d{3,4}\|([^|]+)\|([^|]+)\|([^:|]+(?:Exception|Error)[^:|]*):\s*(.+)$/i.exec(line.raw);
  if (pipeException) {
    return {
      family: "pipe-exception",
      component: pipeException[1].trim(),
      summary: pipeException[2].trim(),
      exceptionType: pipeException[3].trim(),
      exceptionMessage: pipeException[4].trim(),
    };
  }

  const pipeTrace = parsePipeTraceEnvelope(line.raw);
  if (pipeTrace) {
    const workstationRequestLine = /^(\d+)\s+Line Received\.\s+Record separated split message:\s+([^;]+);([^;]+);(.+?)\.?\|?$/i.exec(pipeTrace.message);
    if (workstationRequestLine) {
      return {
        family: "workstation-request-line",
        listenerPort: workstationRequestLine[1].trim(),
        protocolToken: workstationRequestLine[2].trim(),
        requestType: workstationRequestLine[3].trim(),
        subject: workstationRequestLine[4].trim().replace(/\.$/, ""),
        traceComponent: pipeTrace.component,
        traceLevel: pipeTrace.level,
        traceLineNumber: pipeTrace.traceLineNumber,
      };
    }

    const cadForwardedVetms = /^(\d+)\s+Datagram forwarded to CAD client:\s+([A-Za-z0-9-]+)\s+Message:\s*<SOH>VETMS;([^;|]+);([^;|]+)(?:;([^|]+))?\|(.+?)<EOT>\.\|?$/i.exec(pipeTrace.message);
    if (cadForwardedVetms) {
      return {
        family: "cad-forwarded-vetms",
        listenerPort: cadForwardedVetms[1].trim(),
        workstation: cadForwardedVetms[2].trim(),
        messageFamily: cadForwardedVetms[3].trim(),
        messageType: cadForwardedVetms[4].trim(),
        stateToken: cadForwardedVetms[5]?.trim() || undefined,
        payloadFields: cadForwardedVetms[6].split("|").map((field) => field.trim()),
        traceComponent: pipeTrace.component,
        traceLevel: pipeTrace.level,
        traceLineNumber: pipeTrace.traceLineNumber,
      };
    }

    const workstationMessage = /^(<<SEND>>|<<RECV>>)\s+([^:]+?)\s+Message Data:\s+(.+)$/i.exec(pipeTrace.message);
    if (workstationMessage) {
      const fields = parseAngleBracketFields(workstationMessage[3]);
      const messageCommand = fields.get("MessageCommand") ?? "";
      if (messageCommand) {
        return {
          family: "workstation-vetms-message",
          messageDirection: workstationMessage[1].toUpperCase().includes("SEND") ? "SEND" : "RECV",
          route: workstationMessage[2].trim(),
          messageCommand,
          messageCategory: fields.get("MessageCategory"),
          messageType: fields.get("MessageType"),
          stateChange: fields.get("StateChange"),
          trainSymbol: fields.get("TrainSymbol"),
          locoUid: fields.get("LocoUID"),
          reportTime: fields.get("ReportTime"),
          directionOfTravel: fields.get("Direction"),
          headMp: fields.get("HeadMP"),
          rearMp: fields.get("RearMP"),
          subdivisionId: fields.get("SubdivisionID"),
          speed: fields.get("Speed"),
          locoState: fields.get("LocoState"),
          locoStateSummary: fields.get("LocoStateSummary"),
          headEndTrack: fields.get("HeadEndTrack"),
          rearEndTrack: fields.get("RearEndTrack"),
          employeeId: fields.get("EmployeeID"),
          employeeName: fields.get("EmployeeName"),
          traceComponent: pipeTrace.component,
          traceLevel: pipeTrace.level,
          traceLineNumber: pipeTrace.traceLineNumber,
        };
      }
    }
  }

  const directIndication = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+W\s+<<\s+INDICATION;(\d+):(\d+):(\d+):([01]+)\s+FOR CODESTATION:(.+)$/i.exec(line.raw);
  if (directIndication) {
    return {
      family: "station-indication",
      codeLineNumber: directIndication[1],
      controlPointNumber: directIndication[2],
      wordNumber: directIndication[3],
      payloadBits: directIndication[4],
      stationToken: directIndication[5].trim(),
    };
  }

  const codeServerReceived = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+MESSAGE RECEIVED FROM CODE SERVER:\s*([A-Za-z0-9-]+)\s+Remote IP Address\s+(\d+\.\d+\.\d+\.\d+)\s+\((.+)\)$/i.exec(line.raw);
  if (codeServerReceived) {
    const [, codeServerName, host, payload] = codeServerReceived;
    const trimmedPayload = payload.trim();
    if (trimmedPayload.startsWith("INDICATION;")) {
      const parts = trimmedPayload.split(":");
      const header = parts[0].split(";");
      if (header.length === 2 && parts.length >= 4) {
        return {
          family: "socket-indication",
          direction: "Received",
          host,
          codeServerName,
          codeLineNumber: header[1],
          controlPointNumber: parts[1],
          wordNumber: parts[2],
          payloadBits: parts[3],
          traceClass: "CAD EventLog",
          traceMethod: "CodeServerReceive",
        };
      }
    }
    if (trimmedPayload.startsWith("KEEPALIVE")) {
      return {
        family: "socket-keepalive",
        host,
        direction: "Received",
        codeServerName,
        traceClass: "CAD EventLog",
        traceMethod: "CodeServerReceive",
      };
    }
    if (trimmedPayload.startsWith("ALIVE")) {
      return {
        family: "socket-alive",
        host,
        direction: "Received",
        codeServerName,
        traceClass: "CAD EventLog",
        traceMethod: "CodeServerReceive",
      };
    }
  }

  const codeServerQueue = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+MESSAGE QUEUE FROM CODE SERVER:\s*([A-Za-z0-9-]+)\s+Remote IP Address\s+(\d+\.\d+\.\d+\.\d+)\s+\(COUNT:(\d+)\)$/i.exec(line.raw);
  if (codeServerQueue) {
    return {
      family: "code-server-queue",
      codeServerName: codeServerQueue[1],
      host: codeServerQueue[2],
      queueCount: codeServerQueue[3],
    };
  }

  const codeServerThreadAlive = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+INDICATION-PROCESSING-THREAD-ALIVE:\s*([A-Za-z0-9-]+)\s+Remote IP Address\s+(\d+\.\d+\.\d+\.\d+)$/i.exec(line.raw);
  if (codeServerThreadAlive) {
    return {
      family: "code-server-thread-alive",
      codeServerName: codeServerThreadAlive[1],
      host: codeServerThreadAlive[2],
    };
  }

  const traceMetadata = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+Version:\s*(.+?)\s+FileName:\s*(.+?)\s+MethodName:\s*(.+?)(?:\s+LineNumber:\s*(\d+))?$/is.exec(line.raw);
  if (traceMetadata) {
    return {
      family: "trace-metadata",
      version: traceMetadata[1].trim(),
      fileName: traceMetadata[2].trim(),
      methodName: traceMetadata[3].trim(),
      lineNumberInfo: traceMetadata[4]?.trim() || undefined,
    };
  }

  const vitalSigns = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+VITAL SIGNS\|(.+)$/is.exec(line.raw);
  if (vitalSigns) {
    const values = new Map<string, string>();
    for (const part of vitalSigns[1].split("|")) {
      const [rawKey, ...rawValue] = part.split("=");
      const key = rawKey?.trim().toUpperCase();
      const value = rawValue.join("=").trim();
      if (key) {
        values.set(key, value);
      }
    }
    return {
      family: "process-vital-signs",
      startTime: values.get("START TIME"),
      softwareVersion: values.get("SOFTWARE VERSION"),
      workingMemory: values.get("WORKING MEMORY"),
      threadCount: values.get("THREAD COUNT"),
      handleCount: values.get("HANDLE COUNT"),
      workingPeakMemory: values.get("WORKING PEAK MEMORY"),
      pagedMemory: values.get("PAGED MEMORY"),
      pagedPeakMemory: values.get("PAGED PEAK MEMORY"),
      privilegedProcessorTime: values.get("PRIVILEDGED PROCESSOR TIME"),
      totalProcessorTime: values.get("TOTAL PROCESSOR TIME"),
      workstation: values.get("WRKSTN"),
    };
  }

  const vitalSignsHeader = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+(Code Server) VITAL SIGNS Version Number\s+(.+)$/i.exec(line.raw);
  if (vitalSignsHeader) {
    return {
      family: "process-vital-signs-header",
      component: vitalSignsHeader[1].trim(),
      softwareVersion: vitalSignsHeader[2].trim(),
    };
  }

  const threadCapacity = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+\((Max|Min|Available) worker threads=(\d+)\)\s+\((Max|Min|Available) I\/O completion threads=(\d+)\)$/i.exec(line.raw);
  if (threadCapacity) {
    return {
      family: "thread-capacity",
      capacityKind: threadCapacity[1].trim() as "Max" | "Min" | "Available",
      maxWorkerThreads: threadCapacity[2],
      maxIoCompletionThreads: threadCapacity[4],
    };
  }

  const signalStateChange = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+SIGNAL STATE CHANGE:([A-Z0-9_]+)$/i.exec(line.raw);
  if (signalStateChange) {
    return {
      family: "signal-state-change",
      stateToken: signalStateChange[1].trim(),
    };
  }

  const signalIndicationReceived = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+SIGNAL INDICATION RECEIVED\.\s+\(UPDATE STATUS:([^)]+)\)\(NAME:([^)]+)\)\(UID:(\d+)\)\(CP:([^)]+)\)$/i.exec(line.raw);
  if (signalIndicationReceived) {
    return {
      family: "signal-indication-update",
      statusTokens: signalIndicationReceived[1].split("|").map((token) => token.trim()).filter(Boolean),
      signalName: signalIndicationReceived[2].trim(),
      signalUid: signalIndicationReceived[3].trim(),
      controlPointToken: signalIndicationReceived[4].trim(),
    };
  }

  const flashName = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+FlashName:([A-Za-z0-9 _-]+)$/i.exec(line.raw);
  if (flashName) {
    return {
      family: "flash-name",
      flashName: flashName[1].trim(),
      sourceKind: "marker",
    };
  }

  const flashNameEvent = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+FlashNameEvent:([A-Za-z0-9 _-]+)$/i.exec(line.raw);
  if (flashNameEvent) {
    return {
      family: "flash-name",
      flashName: flashNameEvent[1].trim(),
      sourceKind: "event",
    };
  }

  const indicationMessageComplete = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+INDICATION MESSAGE COMPLETE:([A-Za-z0-9-]+)$/i.exec(line.raw);
  if (indicationMessageComplete) {
    return {
      family: "indication-message-complete",
      codeServerName: indicationMessageComplete[1].trim(),
    };
  }

  const controlServerStreamTransport = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+ControlServerStreamReceiverData:(KEEPALIVE|ALIVE)$/i.exec(line.raw);
  if (controlServerStreamTransport) {
    return {
      family: controlServerStreamTransport[1].toUpperCase() === "KEEPALIVE" ? "socket-keepalive" : "socket-alive",
      host: "",
      direction: "Received",
      traceClass: "CAD EventLog",
      traceMethod: "ControlServerStreamReceiverData",
    };
  }

  const cadTrainEvent = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+TRAIN EVENT:\|(.+)$/is.exec(line.raw);
  if (cadTrainEvent) {
    const parsed = parseCadTrainPayload(cadTrainEvent[1], "train-event");
    if (parsed) {
      return parsed;
    }
  }

  const trainScheduleTimerCheck = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+TRAIN EVENT:Train Schedule :([^:]+):(.+?) UID:\s*([A-F0-9-]+)\s+TIMER CHECK -\s*(.+?)\s+SCHEDULE TIME:\s*(.+)$/i.exec(line.raw);
  if (trainScheduleTimerCheck) {
    return {
      family: "train-schedule-timer-check",
      trainSymbol: trainScheduleTimerCheck[1].trim(),
      locationToken: trainScheduleTimerCheck[2].trim(),
      scheduleUid: trainScheduleTimerCheck[3].trim(),
      timerCheckTime: trainScheduleTimerCheck[4].trim(),
      scheduleTime: trainScheduleTimerCheck[5].trim(),
    };
  }

  const controlServerTransport = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+CONTROL SERVER MESSAGE FROM:([A-Za-z0-9-]+)\s+Message=(KEEPALIVE|ALIVE)$/i.exec(line.raw);
  if (controlServerTransport) {
    return {
      family: controlServerTransport[2].toUpperCase() === "KEEPALIVE" ? "socket-keepalive" : "socket-alive",
      host: "",
      direction: "Received",
      codeServerName: controlServerTransport[1],
      traceClass: "CAD EventLog",
      traceMethod: "ControlServerMessage",
    };
  }

  const cadControlServerTrain = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+CONTROL SERVER MESSAGE FROM:([A-Za-z0-9-]+)\s+Message=TRAIN;\|(.+)$/is.exec(line.raw);
  if (cadControlServerTrain) {
    const parsed = parseCadTrainPayload(cadControlServerTrain[2], "control-server-message", cadControlServerTrain[1]);
    if (parsed) {
      return parsed;
    }
  }

  const cadControlPointReceiver = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+ControlServerStreamReceiverData:CNTRLPT;\|(\d+)\|(\d+)\|([A-Z0-9]+)\|([A-F0-9-]+)\|(\d+)\|(\d+)\|([^;|]+);(TRANSACTION-\d+-\d+)$/i.exec(line.raw);
  if (cadControlPointReceiver) {
    return {
      family: "cad-control-point-message",
      sourceKind: "stream-receiver",
      subdivisionToken: cadControlPointReceiver[1],
      controlPointNumber: cadControlPointReceiver[2],
      operationToken: cadControlPointReceiver[3],
      correlationId: cadControlPointReceiver[4],
      relatedUidA: cadControlPointReceiver[5],
      relatedUidB: cadControlPointReceiver[6],
      stateValue: cadControlPointReceiver[7],
      transactionId: cadControlPointReceiver[8],
    };
  }

  const cadTrainReceiver = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+ControlServerStreamReceiverData:TRAIN;\|(.+)$/is.exec(line.raw);
  if (cadTrainReceiver) {
    const parsed = parseCadTrainPayload(cadTrainReceiver[1], "stream-receiver");
    if (parsed) {
      return parsed;
    }
  }

  const cadSignalReceiver = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+ControlServerStreamReceiverData:SIGNAL;\|(\d+)\|(\d+)\|([A-Z0-9]+);(TRANSACTION-\d+-\d+)$/i.exec(line.raw);
  if (cadSignalReceiver) {
    return {
      family: "cad-signal-message",
      sourceKind: "stream-receiver",
      subdivisionToken: cadSignalReceiver[1],
      signalUid: cadSignalReceiver[2],
      operationToken: cadSignalReceiver[3],
      transactionId: cadSignalReceiver[4],
    };
  }

  const cadControlServerMessage = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+CONTROL SERVER MESSAGE FROM:([A-Za-z0-9-]+)\s+Message=CNTRLPT;\|(\d+)\|(\d+)\|([A-Z0-9]+)\|([A-F0-9-]+)\|(\d+)\|(\d+)\|([^;|]+);(TRANSACTION-\d+-\d+)$/i.exec(line.raw);
  if (cadControlServerMessage) {
    return {
      family: "cad-control-point-message",
      sourceKind: "control-server-message",
      codeServerName: cadControlServerMessage[1],
      subdivisionToken: cadControlServerMessage[2],
      controlPointNumber: cadControlServerMessage[3],
      operationToken: cadControlServerMessage[4],
      correlationId: cadControlServerMessage[5],
      relatedUidA: cadControlServerMessage[6],
      relatedUidB: cadControlServerMessage[7],
      stateValue: cadControlServerMessage[8],
      transactionId: cadControlServerMessage[9],
    };
  }

  const cadControlServerSignal = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+CONTROL SERVER MESSAGE FROM:([A-Za-z0-9-]+)\s+Message=SIGNAL;\|(\d+)\|(\d+)\|([A-Z0-9]+);(TRANSACTION-\d+-\d+)$/i.exec(line.raw);
  if (cadControlServerSignal) {
    return {
      family: "cad-signal-message",
      sourceKind: "control-server-message",
      codeServerName: cadControlServerSignal[1],
      subdivisionToken: cadControlServerSignal[2],
      signalUid: cadControlServerSignal[3],
      operationToken: cadControlServerSignal[4],
      transactionId: cadControlServerSignal[5],
    };
  }

  const recall = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+RECALL SENT:(.+):(\d+)$/i.exec(line.raw);
  if (recall) {
    return {
      family: "station-recall",
      stationToken: recall[1].trim(),
      sequence: recall[2],
    };
  }

  const codeLineCommand = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+(QueueTheCommand|SendCommand):(.+?):([A-Z ]+?)(?:\s+\(([01]+)\))?$/i.exec(line.raw);
  if (codeLineCommand) {
    return {
      family: "code-line-command",
      action: codeLineCommand[1] === "QueueTheCommand" ? "Queued" : "Sent",
      stationToken: codeLineCommand[2].trim(),
      commandKind: codeLineCommand[3].trim(),
      payloadBits: codeLineCommand[4]?.trim() || undefined,
    };
  }

  const codeLineQueueCount = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+ProcessSendQueue-CommandCount(\d+)$/i.exec(line.raw);
  if (codeLineQueueCount) {
    return {
      family: "code-line-queue-count",
      queueCount: codeLineQueueCount[1].trim(),
    };
  }

  const codeLineProcessSendQueue = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+ProcessSendQueue(.+)$/i.exec(line.raw);
  if (codeLineProcessSendQueue) {
    const queueText = codeLineProcessSendQueue[1].trim();
    const queueParts = /^(.+?)(RECALL|CONTROL)$/i.exec(queueText);
    return {
      family: "code-line-process-send-queue",
      queueText,
      stationToken: queueParts?.[1]?.trim() || undefined,
      commandKind: queueParts?.[2]?.trim().toUpperCase() || undefined,
    };
  }

  const codeLineProcessIndication = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+PROCESS IND:\s*(\d+)\s+\(([^)]+)\)\s+\(([01]+)\)$/i.exec(line.raw);
  if (codeLineProcessIndication) {
    return {
      family: "code-line-process-indication",
      codeStationNumber: codeLineProcessIndication[1].trim(),
      stationToken: codeLineProcessIndication[2].trim(),
      payloadBits: codeLineProcessIndication[3].trim(),
    };
  }

  const codeLineControlSent = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+<<CONTROL SENT:\s*(\d+)\s+\(([^)]+)\)\s*-\s*\(([01]+)\)$/i.exec(line.raw);
  if (codeLineControlSent) {
    return {
      family: "code-line-control-sent",
      codeStationNumber: codeLineControlSent[1].trim(),
      stationToken: codeLineControlSent[2].trim(),
      payloadBits: codeLineControlSent[3].trim(),
    };
  }

  const codeLineControlMnemonic = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+CTL MNEM:\s*<(.+)>$/i.exec(line.raw);
  if (codeLineControlMnemonic) {
    return {
      family: "code-line-control-mnemonic",
      entries: parseBooleanMnemonicEntries(codeLineControlMnemonic[1]),
    };
  }

  const codeLineIndicationMnemonic = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+IND MNEM:\s*<(.+)>$/i.exec(line.raw);
  if (codeLineIndicationMnemonic) {
    return {
      family: "code-line-indication-mnemonic",
      entries: parseBooleanMnemonicEntries(codeLineIndicationMnemonic[1]),
    };
  }

  const codeLineControlPayload = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+(SendControl|ProcessControlBegin)(\d+)?:\s*([01]+)$/i.exec(line.raw);
  if (codeLineControlPayload) {
    return {
      family: "code-line-control-payload",
      phase: codeLineControlPayload[1] === "SendControl" ? "SendControl" : "ProcessControlBegin",
      codeStationNumber: codeLineControlPayload[2]?.trim() || undefined,
      payloadBits: codeLineControlPayload[3].trim(),
    };
  }

  const codeLineControlQueueCleared = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+Control Queue Being Cleared:\s*([01]+)?$/i.exec(line.raw);
  if (codeLineControlQueueCleared) {
    return {
      family: "code-line-control-queue-cleared",
      payloadBits: codeLineControlQueueCleared[1]?.trim() || undefined,
    };
  }

  const codeLineProcessControlCompleted = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+ProcessControlCompleted$/i.exec(line.raw);
  if (codeLineProcessControlCompleted) {
    return {
      family: "code-line-control-process-completed",
    };
  }

  const codeLinePrintControlQueue = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+PrintControlQueue:$/i.exec(line.raw);
  if (codeLinePrintControlQueue) {
    return {
      family: "code-line-control-queue-print",
    };
  }

  const codeLineStatisticsSummary = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+NAME:(.+?)\s+CTLS:(\d+)\s+INDS:(\d+)\s+FAILS:(\d+)$/i.exec(line.raw);
  if (codeLineStatisticsSummary) {
    return {
      family: "code-line-statistics-summary",
      stationToken: codeLineStatisticsSummary[1].trim(),
      controlCount: codeLineStatisticsSummary[2].trim(),
      indicationCount: codeLineStatisticsSummary[3].trim(),
      failureCount: codeLineStatisticsSummary[4].trim(),
    };
  }

  const controlDeliveryTimerStop = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+ControlDeliveryReceived-STOP-THE-CONTROL-TIMER$/i.exec(line.raw);
  if (controlDeliveryTimerStop) {
    return { family: "control-delivery-timer-stop" };
  }

  const codeLineHexFrame = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+((?:[A-Z ]+XMT >>>)|IND RVCD|SERVICE SIG)\s*:?\s*(.+)$/i.exec(line.raw);
  if (codeLineHexFrame) {
    return {
      family: "code-line-hex-frame",
      frameLabel: codeLineHexFrame[1].trim().toUpperCase(),
      payloadBytes: parseSpaceSeparatedHexBytes(codeLineHexFrame[2]),
    };
  }

  const codeLineIndicationSummary = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+>>IND:\s+([0-9A-Z]+)(?:\s+\(([^)]+)\))?$/i.exec(line.raw);
  if (codeLineIndicationSummary) {
    return {
      family: "code-line-indication-summary",
      codeToken: codeLineIndicationSummary[1].trim(),
      stationToken: codeLineIndicationSummary[2]?.trim() || undefined,
    };
  }

  const codeLineControlUpdated = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+CONTROL UPDATED:\s*([0-9A-Z]+)(?:\(([^)]+)\))?\s+\[([01]+)\](?:\s+Queue Count=(\d+))?$/i.exec(line.raw);
  if (codeLineControlUpdated) {
    return {
      family: "code-line-control-update",
      codeToken: codeLineControlUpdated[1].trim(),
      stationToken: codeLineControlUpdated[2]?.trim() || undefined,
      payloadBits: codeLineControlUpdated[3].trim(),
      queueCount: codeLineControlUpdated[4]?.trim() || undefined,
    };
  }

  const codeLineControlDelivered = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+CONTROL DELIVERED:\s*([0-9A-Z]+)(?:\s+\(([^)]+)\))?\s+\(([01]+)\)$/i.exec(line.raw);
  if (codeLineControlDelivered) {
    return {
      family: "code-line-control-delivered",
      codeToken: codeLineControlDelivered[1].trim(),
      stationToken: codeLineControlDelivered[2]?.trim() || undefined,
      payloadBits: codeLineControlDelivered[3].trim(),
    };
  }

  const codeLineQueueDepth = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+Queue Count=(\d+)(?:\s+([^,]+),\s*([^,]+),\s*(\d+):)?$/i.exec(line.raw);
  if (codeLineQueueDepth) {
    return {
      family: "code-line-queue-depth",
      queueCount: codeLineQueueDepth[1].trim(),
      component: codeLineQueueDepth[2]?.trim() || undefined,
      method: codeLineQueueDepth[3]?.trim() || undefined,
      traceLineNumber: codeLineQueueDepth[4]?.trim() || undefined,
    };
  }

  const codeLineServiceMessage = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+>>SERV MESS:\s*([A-Z0-9_-]+)\s*-\s*([0-9A-Z]+)(?:\s+\(([^)]+)\))?$/i.exec(line.raw);
  if (codeLineServiceMessage) {
    return {
      family: "code-line-service-message",
      statusToken: codeLineServiceMessage[1].trim(),
      codeToken: codeLineServiceMessage[2].trim(),
      stationToken: codeLineServiceMessage[3]?.trim() || undefined,
    };
  }

  const codeLineControlImage = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+(NEW CONTROL IMAGE RECEIVED TO REPLACE OLD IMAGE IN QUEUE|NEW CONTROL IMAGE SENT):([01]+)$/i.exec(line.raw);
  if (codeLineControlImage) {
    return {
      family: "code-line-control-image",
      phase: codeLineControlImage[1].toUpperCase().includes("RECEIVED") ? "Queued replacement" : "Sent image",
      payloadBits: codeLineControlImage[2].trim(),
    };
  }

  const codeLineAutoRecall = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+<<LRECALL\s+\((AUTO)\):\s*([0-9A-Z]+)\s+\(([^)]+)\)$/i.exec(line.raw);
  if (codeLineAutoRecall) {
    return {
      family: "code-line-recall-auto",
      auto: true,
      stationToken: codeLineAutoRecall[3].trim(),
    };
  }

  const codeLineLastIndicationAutoRecall = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+LastIndicationRcv-AutoRecall:([^)]+)$/i.exec(line.raw);
  if (codeLineLastIndicationAutoRecall) {
    return {
      family: "code-line-last-indication-auto-recall",
      stationToken: codeLineLastIndicationAutoRecall[1].trim(),
    };
  }

  const socketRawFrameLegacy = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+(?:CODELINE SOCKET (RCV|XMT):([<>]{2})\s+(.+?)\s+DATA:|([<>-]{2,3})(XMT|RCV)\s+([^<]+?)\s+)(.+)$/i.exec(line.raw);
  if (socketRawFrameLegacy) {
    const socketAction = (socketRawFrameLegacy[1] || socketRawFrameLegacy[5] || "").toUpperCase() as "XMT" | "RCV";
    const directionGlyph = (socketRawFrameLegacy[2] || socketRawFrameLegacy[4] || "").trim();
    const stationToken = (socketRawFrameLegacy[3] || socketRawFrameLegacy[6] || "").trim();
    const payloadBytes = Array.from((socketRawFrameLegacy[7] || "").matchAll(/<([^<>]+)>/g)).map((match) => match[1].trim()).filter(Boolean);
    if (socketAction && stationToken && payloadBytes.length) {
      return {
        family: "socket-raw-frame",
        socketAction,
        stationToken,
        payloadBytes,
        directionGlyph,
      };
    }
  }

  const hostConnectionRefused = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+HOST REFUSED CONNECTION REQUEST TO:(\d+\.\d+\.\d+\.\d+):(\d+)$/i.exec(line.raw);
  if (hostConnectionRefused) {
    return {
      family: "host-connection-refused",
      host: hostConnectionRefused[1],
      port: hostConnectionRefused[2],
    };
  }

  const socketData = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3}\s+W\s+([<>]{2})\s+(\d+\.\d+\.\d+\.\d+)\s+DATA:\s*(.*?)\s+([A-Za-z0-9_]+)\s*,\s*([A-Za-z0-9_]+)\s*,\s*(\d+):$/i.exec(line.raw);
  if (socketData) {
    const [, direction, host, payload, traceClass, traceMethod, traceThread] = socketData;
    const trimmedPayload = normalizeAsciiControlWrappedPayload(payload);
    if (trimmedPayload.startsWith("INDICATION;")) {
      const parts = trimmedPayload.split(":");
      const header = parts[0].split(";");
      if (header.length === 2 && parts.length >= 4) {
        return {
          family: "socket-indication",
          direction,
          host,
          codeLineNumber: header[1],
          controlPointNumber: parts[1],
          wordNumber: parts[2],
          payloadBits: parts[3],
          traceClass,
          traceMethod,
          traceThread,
        };
      }
    }
    if (trimmedPayload.startsWith("KEEPALIVE")) {
      return { family: "socket-keepalive", host, direction, traceClass, traceMethod, traceThread };
    }
    if (trimmedPayload.startsWith("ALIVE")) {
      return { family: "socket-alive", host, direction, traceClass, traceMethod, traceThread };
    }
    const controlPayload = /^CONTROL(?: UPDATE)?;(\d+):(\d+):(\d+):([01]+)$/i.exec(trimmedPayload);
    if (controlPayload) {
        return {
          family: "socket-control",
          host,
          codeLineNumber: controlPayload[1].trim(),
          controlPointNumber: controlPayload[2].trim(),
          wordNumber: controlPayload[3].trim(),
          payloadBits: controlPayload[4].trim(),
          updateOnly: /^CONTROL UPDATE;/i.test(trimmedPayload),
        };
    }
  }

  const codeServerSocketControl = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+W\s+>>\s+CONTROL(?:\s+UPDATE\s+ONLY)?:([0-9.]+):(\d+):(\d+):(\d+):([01]+)$/i.exec(line.raw);
  if (codeServerSocketControl) {
    return {
      family: "socket-control",
      host: codeServerSocketControl[1].trim(),
      codeLineNumber: codeServerSocketControl[2].trim(),
      controlPointNumber: codeServerSocketControl[3].trim(),
      wordNumber: codeServerSocketControl[4].trim(),
      payloadBits: codeServerSocketControl[5].trim(),
      updateOnly: /UPDATE ONLY/i.test(line.raw),
    };
  }

  const commserverDataMessage = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+DATA MESSAGE RCVD FROM:([^ ]+)\s+DATA:(.+)$/i.exec(line.raw);
  if (commserverDataMessage) {
    return {
      family: "commserver-data-message",
      peerToken: commserverDataMessage[1].trim(),
      payload: normalizeAsciiControlWrappedPayload(commserverDataMessage[2]),
    };
  }

  const commserverTrainProcessing = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+PROCESSING ACTIVE TRAIN\s*:\s*(.+)$/i.exec(line.raw);
  if (commserverTrainProcessing) {
    return {
      family: "commserver-train-processing",
      trainSymbol: commserverTrainProcessing[1].trim(),
    };
  }

  const commserverSqlQuery = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+(GETALLTRAINSCHEDULES QUERY|DeleteDuplicateProjectedSchedule Qry)\s*:\s*-?\s*(.+)$/i.exec(line.raw);
  if (commserverSqlQuery) {
    return {
      family: "commserver-sql-query",
      queryKind: commserverSqlQuery[1].trim(),
      sqlText: commserverSqlQuery[2].trim(),
    };
  }

  const trackIndicationReceived = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+TRACK INDICATION RECEIVED\.\s+\(UPDATE STATUS:([^)]+)\)\(NAME:([^)]+)\)\(UID:(\d+)\)\(CP:([^)]+)\)$/i.exec(line.raw);
  if (trackIndicationReceived) {
    return {
      family: "track-indication-update",
      statusTokens: trackIndicationReceived[1].split("|").map((token) => token.trim()).filter(Boolean),
      trackName: trackIndicationReceived[2].trim(),
      trackUid: trackIndicationReceived[3].trim(),
      controlPointToken: trackIndicationReceived[4].trim(),
    };
  }

  const trackTrafficRemovalCheck = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+ApplyIndicationChangeToTrack-CanTrafficBeRemoved-RemoveMyTraffic=(TRUE|FALSE):([^;:]+);(\d+):([A-Z]*)$/i.exec(line.raw);
  if (trackTrafficRemovalCheck) {
    return {
      family: "track-traffic-removal-check",
      decision: trackTrafficRemovalCheck[1].trim(),
      trackName: trackTrafficRemovalCheck[2].trim(),
      trackUid: trackTrafficRemovalCheck[3].trim(),
      directionToken: trackTrafficRemovalCheck[4]?.trim() || undefined,
    };
  }

  const ptcbosMessage = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+-PTCBOS (RAW )?MESSAGE RVCD:(.*)$/i.exec(line.raw);
  if (ptcbosMessage) {
    return {
      family: "ptcbos-message",
      rawKind: ptcbosMessage[1] ? "raw" : "decoded",
      payload: normalizeAsciiControlWrappedPayload(ptcbosMessage[2] ?? ""),
    };
  }

  const processIndicationPhase = /^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}\.\d{3,4}\s+ProcessIndication(?:-(ProcessInformationBit))?:([A-Za-z0-9-]+)$/i.exec(line.raw);
  if (processIndicationPhase) {
    return {
      family: "code-line-process-indication-phase",
      phase: processIndicationPhase[1] ? "ProcessInformationBit" : "ProcessIndication",
      stationToken: processIndicationPhase[2].trim(),
    };
  }

  const strippedLine = stripLeadingLogTimestamp(line.raw);
  const trimmedStrippedLine = strippedLine.trim();

  const namedGuidCatalogEntry = /^(.+?)\|([0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12})$/i.exec(strippedLine);
  if (namedGuidCatalogEntry) {
    return {
      family: "named-guid-catalog-entry",
      label: namedGuidCatalogEntry[1].trim(),
      guid: namedGuidCatalogEntry[2].trim(),
    };
  }

  const sqlTrainUpdateContinuation = /^',\s*EOTD\s*=.*WHERE Symbol = '([^']+)'$/i.exec(strippedLine);
  if (sqlTrainUpdateContinuation) {
    return {
      family: "sql-train-update-continuation",
      trainSymbol: sqlTrainUpdateContinuation[1].trim(),
      payload: strippedLine,
    };
  }

  const compactTrackState = /^OCCUPPIED=(TRUE|FALSE)\s+TRAFFIC=\s*([^ ]*)\s+BLOCKING=(TRUE|FALSE)\s+TRACKSTATUS=([^\s]+)$/i.exec(strippedLine);
  if (compactTrackState) {
    return {
      family: "compact-track-state",
      occupied: compactTrackState[1].trim().toUpperCase(),
      traffic: compactTrackState[2].trim(),
      blocking: compactTrackState[3].trim().toUpperCase(),
      trackStatus: compactTrackState[4].trim(),
    };
  }

  const gboPtcTransmission = /^GBO PTCElectronicTransmissionInProgress=(TRUE|FALSE)\s+<([^>]+)>\s+<([^>]+)>$/i.exec(strippedLine);
  if (gboPtcTransmission) {
    return {
      family: "gbo-ptc-transmission-status",
      inProgress: gboPtcTransmission[1].trim().toUpperCase(),
      guid: gboPtcTransmission[2].trim(),
      trainSymbol: gboPtcTransmission[3].trim(),
    };
  }

  const onlineStatus = /^OnLineStatus=(TRUE|FALSE)\s+ThisCodeServer\.OnlineStatus=(TRUE|FALSE)\s+CodeServer\.gOnlineStatus=(TRUE|FALSE)$/i.exec(strippedLine);
  if (onlineStatus) {
    return {
      family: "code-server-online-status",
      onlineStatus: onlineStatus[1].trim().toUpperCase(),
      localStatus: onlineStatus[2].trim().toUpperCase(),
      globalStatus: onlineStatus[3].trim().toUpperCase(),
    };
  }

  const bosServerListEntry = /^(Online|Offline)\s+([^(<]+)\((\d+)\)\s+<ServerAssignmentId=(\d+)>\s+<LastHeartbeat=([^>]+)>$/i.exec(strippedLine);
  if (bosServerListEntry) {
    return {
      family: "bos-server-list-entry",
      availability: (bosServerListEntry[1][0].toUpperCase() + bosServerListEntry[1].slice(1).toLowerCase()) as "Online" | "Offline",
      serverName: bosServerListEntry[2].trim(),
      serverId: bosServerListEntry[3].trim(),
      serverAssignmentId: bosServerListEntry[4].trim(),
      lastHeartbeat: bosServerListEntry[5].trim(),
    };
  }

  const connectionEndpointStatus = /^(CLIENT|CLUSTER) CONNECTION IP = ([0-9.]+) (?:CLIENT|CLUSTER) CONNECTION PORT = (\d+)$/i.exec(strippedLine);
  if (connectionEndpointStatus) {
    return {
      family: "connection-endpoint-status",
      scope: connectionEndpointStatus[1].trim().toUpperCase() as "CLIENT" | "CLUSTER",
      host: connectionEndpointStatus[2].trim(),
      port: connectionEndpointStatus[3].trim(),
    };
  }

  const workstationTransactionMarker = /^(WKST\d+)(?:;(TRANSACTION-\d+-\d+))?$/i.exec(trimmedStrippedLine);
  if (workstationTransactionMarker) {
    return {
      family: "workstation-transaction-marker",
      workstation: workstationTransactionMarker[1].trim().toUpperCase(),
      transactionId: workstationTransactionMarker[2]?.trim() || undefined,
    };
  }

  const sameBinaryState = /^([01]+)\s+SAME\s+(CONTROL|INDICATION)$/i.exec(trimmedStrippedLine);
  if (sameBinaryState) {
    return {
      family: "repeated-binary-state",
      payloadBits: sameBinaryState[1].trim(),
      stateKind: sameBinaryState[2].trim().toUpperCase() as "CONTROL" | "INDICATION",
    };
  }

  const rawHexPayload = /^(?=.*[A-Fa-f])([0-9A-Fa-f]{16,})$/.exec(trimmedStrippedLine);
  if (rawHexPayload && rawHexPayload[1].length % 2 === 0) {
    return {
      family: "raw-hex-payload",
      byteCount: rawHexPayload[1].length / 2,
      payloadPreview: rawHexPayload[1].slice(0, 96),
    };
  }

  const indicationChangeTrigger = /^INDICATION CHANGE - AICTT CALLED BY\s+(.+)$/i.exec(trimmedStrippedLine);
  if (indicationChangeTrigger) {
    return {
      family: "indication-change-trigger",
      caller: indicationChangeTrigger[1].trim(),
    };
  }

  const locomotiveProcessingMarker = /^Locomotive-(SummaryUpdate\.\.\.Completed|CheckPositionReport\.\.\.Finished)$/i.exec(trimmedStrippedLine);
  if (locomotiveProcessingMarker) {
    return {
      family: "locomotive-processing-marker",
      stage: locomotiveProcessingMarker[1].startsWith("Summary")
        ? "SummaryUpdateCompleted"
        : "CheckPositionReportFinished",
    };
  }

  const trackTracingMarker = /^(START|END)\s+TRACING\s+(TrackTrainIDOn)$/i.exec(trimmedStrippedLine);
  if (trackTracingMarker) {
    return {
      family: "track-tracing-marker",
      phase: trackTracingMarker[1].trim().toUpperCase() as "START" | "END",
      subject: trackTracingMarker[2].trim(),
    };
  }

  const systemThreadHeartbeat = /^(SYSTEM-FLASHER-THREAD-ALIVE)$/i.exec(trimmedStrippedLine);
  if (systemThreadHeartbeat) {
    return {
      family: "system-thread-heartbeat",
      threadName: systemThreadHeartbeat[1].trim(),
    };
  }

  const controlQueueEvent = /^(New Command and Command in Queue)$/i.exec(trimmedStrippedLine);
  if (controlQueueEvent) {
    return {
      family: "control-queue-event",
      eventName: controlQueueEvent[1].trim(),
    };
  }

  const networkStackFrame = /^at\s+(System\.Net\.Sockets\.TcpClient\.\.ctor)\((.+)\)$/i.exec(trimmedStrippedLine);
  if (networkStackFrame) {
    return {
      family: "network-stack-frame",
      method: networkStackFrame[1].trim(),
      signature: networkStackFrame[2].trim(),
    };
  }

  const applicationStackFrame = /^at\s+([A-Za-z0-9_.]+)(?:\((.*)\))?$/i.exec(trimmedStrippedLine);
  if (applicationStackFrame) {
    return {
      family: "application-stack-frame",
      method: applicationStackFrame[1].trim(),
      signature: applicationStackFrame[2]?.trim() || undefined,
    };
  }

  const directionStateEntry = /^(EAST|WEST|NONE)\|(\d+)\|(TRUE|FALSE)$/i.exec(trimmedStrippedLine);
  if (directionStateEntry) {
    return {
      family: "direction-state-entry",
      direction: directionStateEntry[1].trim().toUpperCase() as "EAST" | "WEST" | "NONE",
      code: directionStateEntry[2].trim(),
      state: directionStateEntry[3].trim().toUpperCase(),
    };
  }

  const adminClickAction = /^Clicked -\s+(.+?)(?:\s+@\s+(.+))?$/i.exec(trimmedStrippedLine);
  if (adminClickAction) {
    return {
      family: "admin-click-action",
      action: adminClickAction[1].trim(),
      actionTime: adminClickAction[2]?.trim() || undefined,
    };
  }

  const uiMarker = /^(CLICKED INTO THE SUMMARY|LISTING CLIENT CONNECTIONS|TRAIN ID FORM-LoadForm|OK BUTTON SELECTED ON TRAINID FORM|CANCEL Button Selected On Train ID FORM|Create Exceptions Menu(?:\s+-\s+.+)?|ESC Key Selected|ARE YOU SURE\??|YOU WILL NOT BE ABLE TO USE THIS TRAIN AGAIN!|NO WORKSTATION ASSIGNMENT FOUND\.)$/i.exec(trimmedStrippedLine);
  if (uiMarker) {
    return {
      family: "user-interface-marker",
      markerText: uiMarker[1].trim(),
    };
  }

  const sendControlsPhase = /^(CancelSendControls|SendControls)-(Start|End)$/i.exec(trimmedStrippedLine);
  if (sendControlsPhase) {
    return {
      family: "control-send-phase-marker",
      routine: sendControlsPhase[1].trim(),
      phase: sendControlsPhase[2].trim().toUpperCase() as "START" | "END",
    };
  }

  const indicationBitInversion = /^INVERTING INDICATION BIT\s+\((\d+)\)\s+FROM\s+\(([01])\)\s+TO\s+\(([01])\)$/i.exec(trimmedStrippedLine);
  if (indicationBitInversion) {
    return {
      family: "indication-bit-inversion",
      bitIndex: indicationBitInversion[1].trim(),
      fromValue: indicationBitInversion[2].trim(),
      toValue: indicationBitInversion[3].trim(),
    };
  }

  const shortWorkflowMarker = /^([A-Z])(\d+(?:\.\d+)?)$/i.exec(trimmedStrippedLine);
  if (shortWorkflowMarker) {
    return {
      family: "short-workflow-marker",
      prefix: shortWorkflowMarker[1].trim().toUpperCase(),
      marker: shortWorkflowMarker[2].trim(),
    };
  }

  const routeSelectionStep = /^(ProcessEnterExitSelection)-Step([\d.]+)$/i.exec(trimmedStrippedLine);
  if (routeSelectionStep) {
    return {
      family: "route-selection-step",
      processName: routeSelectionStep[1].trim(),
      step: routeSelectionStep[2].trim(),
    };
  }

  const storedRouteEventMarker = /^(StoredRouteEvent)([\d.]+)$/i.exec(trimmedStrippedLine);
  if (storedRouteEventMarker) {
    return {
      family: "stored-route-event-marker",
      eventGroup: storedRouteEventMarker[1].trim(),
      step: storedRouteEventMarker[2].trim(),
    };
  }

  const storedRouteRecursionCheck = /^CheckForRecursiveStoredRoutes-(BEGIN|END)$/i.exec(trimmedStrippedLine);
  if (storedRouteRecursionCheck) {
    return {
      family: "stored-route-recursion-check",
      phase: storedRouteRecursionCheck[1].trim().toUpperCase() as "BEGIN" | "END",
    };
  }

  const systemResetMarker = /^(ClearSystemVariables-Sub-Was-Called)$/i.exec(trimmedStrippedLine);
  if (systemResetMarker) {
    return {
      family: "system-reset-marker",
      markerText: systemResetMarker[1].trim(),
    };
  }

  const storedRouteStatusMarker = /^(EXECUTION OF STORED ROUTE PASSED SIGNAL CHECK|STORED ROUTE-UpdateSignalDisplays)$/i.exec(trimmedStrippedLine);
  if (storedRouteStatusMarker) {
    return {
      family: "stored-route-status-marker",
      markerText: storedRouteStatusMarker[1].trim(),
    };
  }

  const codeStationLoadCount = /^(\d+)\s+CodeStations Are Loaded From Database\.$/i.exec(trimmedStrippedLine);
  if (codeStationLoadCount) {
    return {
      family: "code-station-load-count",
      count: codeStationLoadCount[1].trim(),
    };
  }

  const exceptionTraceSeparator = /^(--- End of inner exception stack trace ---)$/i.exec(trimmedStrippedLine);
  if (exceptionTraceSeparator) {
    return {
      family: "exception-trace-separator",
      markerText: exceptionTraceSeparator[1].trim(),
    };
  }

  if (!trimmedStrippedLine) {
    return {
      family: "blank-log-entry",
    };
  }

  const territoryTrainList = /^TRAIN LIST FOR TERRITORY:(.+)$/i.exec(strippedLine);
  if (territoryTrainList) {
    return {
      family: "territory-train-list",
      territoryToken: territoryTrainList[1].trim(),
    };
  }

  const plainVetmsMessage = /^VETMS MESSAGE (RCVD|SENT):(.+)$/i.exec(strippedLine);
  if (plainVetmsMessage) {
    const parsed = parseDelimitedVetmsPayload(plainVetmsMessage[2]);
    if (parsed) {
      return {
        family: "plain-vetms-message",
        messageDirection: plainVetmsMessage[1].toUpperCase() === "SENT" ? "SEND" : "RECV",
        ...parsed,
      };
    }
  }

  const workstationOriginatedTrainLog = /^ORGINATED FORM THIS WORKSTATION DROPPING THIS MESSAGE AND LOGGING THE TRAIN EVENT:(.+)$/i.exec(strippedLine);
  if (workstationOriginatedTrainLog) {
    const payload = workstationOriginatedTrainLog[1].trim();
    const payloadFields = payload.replace(/^\|/, "").split("|").map((field) => field.trim());
    return {
      family: "workstation-originated-train-log",
      payload,
      payloadFields,
      subdivisionToken: payloadFields[0] || undefined,
      eventToken: payloadFields[1] || undefined,
      trainUid: payloadFields[1]?.toUpperCase() === "MOVE" ? payloadFields[2] || undefined : undefined,
      trainSymbol: payloadFields[1]?.toUpperCase() === "MOVE" ? payloadFields[3] || undefined : undefined,
    };
  }

  const plainControlSent = /^CONTROL SENT:([^:]+):(\d+):(\d+)\s+([01]+)$/i.exec(strippedLine);
  if (plainControlSent) {
    return {
      family: "plain-control-sent",
      stationToken: plainControlSent[1].trim(),
      channelToken: plainControlSent[2].trim(),
      declaredWidth: plainControlSent[3].trim(),
      payloadBits: plainControlSent[4].trim(),
    };
  }

  const mparEvent = /^MPAR-EVENT:([^:]+)(?::(.*))?$/i.exec(strippedLine);
  if (mparEvent) {
    return {
      family: "mpar-event",
      eventName: mparEvent[1].trim(),
      payload: mparEvent[2]?.trim() || undefined,
    };
  }

  const routeSearchMarker = /^(Search(?:Right|Left)ForRoute(?:New|Forwards|Backwards|FromSwitch)?|SearchForRoute-Step)([\d.]*)$/i.exec(trimmedStrippedLine);
  if (routeSearchMarker) {
    return {
      family: "route-search-message",
      searchKind: routeSearchMarker[1].trim(),
      action: "marker",
      marker: routeSearchMarker[2].trim() || undefined,
      rawPayload: trimmedStrippedLine,
    };
  }

  const routeSearchComponent = /^(Search(?:Right|Left)ForRoute(?:New|Forwards|Backwards|FromSwitch)?):(.+?)\s+(\d+)$/i.exec(strippedLine);
  if (routeSearchComponent) {
    return {
      family: "route-search-message",
      searchKind: routeSearchComponent[1].trim(),
      action: "component",
      componentClass: routeSearchComponent[2].trim(),
      componentUid: routeSearchComponent[3].trim(),
      rawPayload: strippedLine,
    };
  }

  const routeSearchSemicolonComponent = /^(Search(?:Right|Left)ForRoute(?:New|Forwards|Backwards))([\d.]+)\s*;(\d+);(.+)$/i.exec(trimmedStrippedLine);
  if (routeSearchSemicolonComponent) {
    return {
      family: "route-search-message",
      searchKind: routeSearchSemicolonComponent[1].trim(),
      action: "component",
      marker: routeSearchSemicolonComponent[2].trim(),
      componentUid: routeSearchSemicolonComponent[3].trim(),
      componentClass: routeSearchSemicolonComponent[4].trim(),
      rawPayload: trimmedStrippedLine,
    };
  }

  const componentRefComp = /^COMP:([^|]+)\|(.+)$/i.exec(strippedLine);
  if (componentRefComp) {
    const componentName = componentRefComp[1].trim();
    const componentUidMatch = /(\d+)$/.exec(componentName);
    return {
      family: "component-reference-list-entry",
      entryKind: "COMP",
      componentName,
      componentUid: componentUidMatch?.[1]?.trim() || undefined,
      componentClass: componentRefComp[2].trim(),
      rawPayload: strippedLine,
    };
  }

  const componentRefGuid = /^GUID:(\d+)\s+NAME:(.+)$/i.exec(strippedLine);
  if (componentRefGuid) {
    return {
      family: "component-reference-list-entry",
      entryKind: "GUID",
      componentUid: componentRefGuid[1].trim(),
      componentName: componentRefGuid[2].trim(),
      rawPayload: strippedLine,
    };
  }

  const trainTrackingMove = /^TRAIN-TRACKING:\s*BEGIN-MoveTrainIDPlace:(\d+)TRAINID:(.+)$/i.exec(strippedLine);
  if (trainTrackingMove) {
    return {
      family: "train-tracking-message",
      prefix: "TRAIN-TRACKING",
      action: "BEGIN-MoveTrainIDPlace",
      rawPayload: strippedLine,
      trackUid: trainTrackingMove[1].trim(),
      trainSymbol: trainTrackingMove[2].trim(),
    };
  }

  const trainTrackingInsertRemove = /^TRAIN-TRACKING:\s*TRAIN ID MOVE EVENT - (INSERT TO TRACK|REMOVE FROM TRACK):\s*GUID=(\d+)TRAINID:(.+)$/i.exec(strippedLine);
  if (trainTrackingInsertRemove) {
    return {
      family: "train-tracking-message",
      prefix: "TRAIN-TRACKING",
      action: trainTrackingInsertRemove[1].trim(),
      rawPayload: strippedLine,
      trackUid: trainTrackingInsertRemove[2].trim(),
      trainSymbol: trainTrackingInsertRemove[3].trim(),
    };
  }

  const simpleTrackUidMessage = /^(TRACING TRACKTRAINIDON|TrackTrainIDOn-Complete|myTrainID\.myHeadEndTrack1|myTrainID\.myRearEndTrack1|ME):(.+)$/i.exec(strippedLine);
  if (simpleTrackUidMessage) {
    return {
      family: "train-tracking-message",
      prefix: simpleTrackUidMessage[1].trim(),
      action: simpleTrackUidMessage[1].trim(),
      rawPayload: strippedLine,
      trackUid: simpleTrackUidMessage[2].trim(),
    };
  }

  const trafficDirection = /^Traffic Direction:\s*(.+)$/i.exec(strippedLine);
  if (trafficDirection) {
    return {
      family: "train-tracking-message",
      prefix: "Traffic Direction",
      action: "Traffic Direction",
      rawPayload: strippedLine,
      directionToken: trafficDirection[1].trim(),
    };
  }

  const updateTrainIdPosition = /^UpDateTrainIDPosition:Train_Name:Index-([^:]*):(.*)$/i.exec(strippedLine);
  if (updateTrainIdPosition) {
    return {
      family: "train-tracking-message",
      prefix: "UpDateTrainIDPosition",
      action: "UpDateTrainIDPosition",
      rawPayload: strippedLine,
      trainSymbol: updateTrainIdPosition[1].trim() || undefined,
      indexValue: updateTrainIdPosition[2].trim() || undefined,
    };
  }

  const updateTrainStackIndex = /^UpdateTrainStackIndex:Train_Name:Index-(.*)$/i.exec(strippedLine);
  if (updateTrainStackIndex) {
    return {
      family: "train-tracking-message",
      prefix: "UpdateTrainStackIndex",
      action: "UpdateTrainStackIndex",
      rawPayload: strippedLine,
      indexValue: updateTrainStackIndex[1].trim() || undefined,
    };
  }

  const processPositionReport = /^ProcessPositionReportForTrainTracking:(.+?) TO TRACK:(\d+):(.+)$/i.exec(strippedLine);
  if (processPositionReport) {
    return {
      family: "train-tracking-message",
      prefix: "ProcessPositionReportForTrainTracking",
      action: "TO TRACK",
      rawPayload: strippedLine,
      trainSymbol: processPositionReport[1].trim(),
      trackUid: processPositionReport[2].trim(),
      trackName: processPositionReport[3].trim(),
    };
  }

  const processPositionReportMissingTrack = /^ProcessPositionReportForTrainTracking:(Track Not Found\.*)$/i.exec(strippedLine);
  if (processPositionReportMissingTrack) {
    return {
      family: "train-tracking-message",
      prefix: "ProcessPositionReportForTrainTracking",
      action: "Track Not Found",
      rawPayload: strippedLine,
    };
  }

  const processLocoPositionVetms = /^ProcessLocomotivePostionReportVETMS:(.+)$/i.exec(strippedLine);
  if (processLocoPositionVetms) {
    const payload = processLocoPositionVetms[1].trim();
    const trainFound = /^TRAIN FOUND\s+(.+)$/i.exec(payload);
    return {
      family: "train-tracking-message",
      prefix: "ProcessLocomotivePostionReportVETMS",
      action: trainFound ? "TRAIN FOUND" : "ProcessLocomotivePostionReportVETMS",
      rawPayload: strippedLine,
      trainSymbol: trainFound?.[1]?.trim() || undefined,
    };
  }

  const sendTrainSymbolMove = /^SendTrainSymbolMove:(.+)$/i.exec(strippedLine);
  if (sendTrainSymbolMove) {
    const payload = sendTrainSymbolMove[1].trim();
    const parsedMove = /^CAD;(\d+);(.+?)\s+(\d{6})(\d{6})([EW]{2})(\d+)T(\d{14});\.$/i.exec(payload);
    return {
      family: "train-tracking-message",
      prefix: "SendTrainSymbolMove",
      action: "SendTrainSymbolMove",
      rawPayload: strippedLine,
      trainSymbol: parsedMove?.[2]?.trim() || undefined,
      trackUid: parsedMove?.[4]?.trim() || undefined,
      relatedTrackUid: parsedMove?.[3]?.trim() || undefined,
      directionToken: parsedMove?.[5]?.trim() || undefined,
      indexValue: parsedMove?.[7]?.trim() || undefined,
    };
  }

  const binaryStateDump = /^[01]{8,}$/.exec(strippedLine);
  if (binaryStateDump) {
    return {
      family: "binary-state-dump",
      payloadBits: binaryStateDump[0],
    };
  }

  const prefixedLogMessage = /^([^:=|]+):\s*(.*)$/s.exec(strippedLine);
  if (prefixedLogMessage) {
    return {
      family: "prefixed-log-message",
      prefix: prefixedLogMessage[1].replace(/\s+/g, " ").trim(),
      payload: normalizeAsciiControlWrappedPayload(prefixedLogMessage[2] ?? ""),
    };
  }

  return { family: "other" };
}

async function loadLogEnrichment(): Promise<LogEnrichmentBundle | null> {
  if (!logEnrichmentPromise) {
    logEnrichmentPromise = Promise.all([
      readJsonFile<StationFoundationRow[]>("exports", "normalized", "station_foundation_summary.json"),
      readJsonFile<CodeLineProtocolRow[]>("exports", "normalized", "code_line_protocol_summary.json"),
      readJsonFile<CodeStationAssignmentMapRow[]>("exports", "normalized", "code_station_assignment_map.json"),
      loadStaticComponentLookupRows(),
      loadDynamicComponentContextRows(),
      loadControlPointMilepostByNumber(),
      readJsonFile<RouteSwitchContextRow[]>("exports", "normalized", "route_switch_context.json"),
      readJsonFile<TrainRuntimeFoundationRow[]>("exports", "normalized", "train_runtime_foundation_summary.json"),
      loadHostInventoryRows(),
      loadAssignmentLongNameFallbacks(),
    ])
      .then(([stations, codeLines, assignments, staticComponents, dynamicComponents, controlPointMilepostByNumber, routes, trains, hosts, assignmentLongNameFallbacks]) => {
        const assignmentExactNameFallbacks = buildAssignmentMnemonicFallbacks(assignments);
        const componentExactNameFallbacks = buildComponentMnemonicFallbacks(staticComponents, dynamicComponents);
        const staticComponentHints = buildStaticComponentHints(staticComponents);
        const stationByKey = new Map<string, StationFoundationRow>();
        const stationByAddress = new Map<string, StationFoundationRow[]>();
        for (const row of stations) {
          addSingleLookup(stationByKey, row, [row.station_name, row.control_point_name, row.control_point_number]);
          addMultiLookup(stationByAddress, row, [row.control_address, row.indication_address]);
        }

        const assignmentByKey = new Map<string, CodeStationAssignmentMapRow>();
        for (const row of assignments) {
          const normalizedRow: CodeStationAssignmentMapRow = {
            ...row,
            control_assignments: normalizeAssignmentEntries(
              row.control_assignments,
              row.control_point_number,
              assignmentLongNameFallbacks,
              assignmentExactNameFallbacks,
              componentExactNameFallbacks,
              staticComponentHints,
            ),
            indication_assignments: normalizeAssignmentEntries(
              row.indication_assignments,
              row.control_point_number,
              assignmentLongNameFallbacks,
              assignmentExactNameFallbacks,
              componentExactNameFallbacks,
              staticComponentHints,
            ),
          };
          addSingleLookup(assignmentByKey, normalizedRow, [row.station_name, row.control_point_name, row.control_point_number]);
        }

        const routesByKey = new Map<string, RouteSwitchContextRow[]>();
        for (const row of routes) {
          addMultiLookup(routesByKey, row, [row.control_point_name, row.control_point_uid]);
        }

        const trainsByKey = new Map<string, TrainRuntimeFoundationRow[]>();
        for (const row of trains) {
          addMultiLookup(trainsByKey, row, [row.control_point_name, row.symbol, row.runtime_train_symbol]);
        }

        const hostByIp = new Map<string, HostInventoryRow[]>();
        for (const row of hosts) {
          addMultiLookup(hostByIp, row, [row.primary_ip]);
        }

        const componentByUid = new Map<string, StaticComponentLookupRow>();
        for (const row of staticComponents) {
          const uid = normalizeLookupKey(row.component_uid);
          if (uid && !componentByUid.has(uid)) {
            componentByUid.set(uid, row);
          }
        }

        const codeLineByNumber = new Map(codeLines.map((row) => [String(row.code_line_number), row]));
        return { stationByKey, stationByAddress, assignmentByKey, routesByKey, trainsByKey, codeLineByNumber, hostByIp, componentByUid, controlPointMilepostByNumber };
      })
      .catch(() => null);
  }

  return logEnrichmentPromise;
}

function findStationRow(bundle: LogEnrichmentBundle | null, event: ParsedLogEvent): StationFoundationRow | null {
  if (!bundle) return null;
  if ("controlPointNumber" in event && event.controlPointNumber) {
    const byNumber = bundle.stationByKey.get(normalizeLookupKey(event.controlPointNumber));
    if (byNumber) return byNumber;
  }
  if ("stationToken" in event && event.stationToken) {
    return bundle.stationByKey.get(normalizeLookupKey(event.stationToken)) ?? null;
  }
  if ("flashName" in event && event.flashName) {
    return bundle.stationByKey.get(normalizeLookupKey(event.flashName)) ?? null;
  }
  if ("locationToken" in event && event.locationToken) {
    return bundle.stationByKey.get(normalizeLookupKey(event.locationToken)) ?? null;
  }
  return null;
}

function findAssignmentRowByKeys(bundle: LogEnrichmentBundle | null, keys: Array<string | undefined>): CodeStationAssignmentMapRow | null {
  if (!bundle) {
    return null;
  }
  for (const key of keys) {
    const row = bundle.assignmentByKey.get(normalizeLookupKey(key));
    if (row) {
      return row;
    }
  }
  return null;
}

function findAssignmentRow(bundle: LogEnrichmentBundle | null, stationRow: StationFoundationRow | null, event: ParsedLogEvent): CodeStationAssignmentMapRow | null {
  return findAssignmentRowByKeys(bundle, [
    stationRow?.control_point_number,
    stationRow?.station_name,
    stationRow?.control_point_name,
    "controlPointNumber" in event ? event.controlPointNumber : "",
    "stationToken" in event ? event.stationToken : "",
  ]);
}

function findNearbyStationToken(index: number, events: ParsedLogEvent[]): string | null {
  let best: { token: string; delta: number } | null = null;
  for (let cursor = Math.max(0, index - 12); cursor <= Math.min(events.length - 1, index + 12); cursor += 1) {
    if (cursor === index) {
      continue;
    }
    const event = events[cursor];
    const token = "stationToken" in event && event.stationToken ? event.stationToken : "";
    if (!token) {
      continue;
    }
    const delta = Math.abs(cursor - index);
    if (!best || delta < best.delta) {
      best = { token, delta };
    }
  }
  return best?.token ?? null;
}

type GenisysByteToken = {
  rawIndexes: number[];
  value: number;
  escaped: boolean;
};

function parseGenisysFrameBodyTokens(bytes: number[], bodyEndExclusive: number): GenisysByteToken[] {
  const tokens: GenisysByteToken[] = [];
  for (let index = 1; index < bodyEndExclusive; index += 1) {
    const value = bytes[index];
    if (value === 0xf0 && index + 1 < bodyEndExclusive) {
      const escapedNibble = bytes[index + 1];
      tokens.push({
        rawIndexes: [index, index + 1],
        value: 0xf0 | escapedNibble,
        escaped: true,
      });
      index += 1;
      continue;
    }
    tokens.push({
      rawIndexes: [index],
      value,
      escaped: false,
    });
  }
  return tokens;
}

function describeGenisysSocketByteRoles(
  payloadBytes: string[],
  decodedFrame: DecodedGenisysSocketFrame,
  serverAddressLabel: string,
): string[] {
  const numericBytes = payloadBytes.map(parseSocketHexByte);
  const rows: string[] = [];
  const invalidIndexes = numericBytes
    .map((value, index) => (value === null ? index : -1))
    .filter((index) => index >= 0);
  if (invalidIndexes.length) {
    return payloadBytes.map((value, index) => {
      const parsed = numericBytes[index];
      return parsed === null
        ? `${index + 1}. ${value} - invalid hex octet`
        : `${index + 1}. ${value} - raw byte; full role decode skipped because the frame contains invalid hex`;
    });
  }

  const bytes = numericBytes.filter((value): value is number => value !== null);
  if (!bytes.length) {
    return ["No socket-frame bytes captured."];
  }

  const headerLabel = decodedFrame.headerCode === null
    ? "unknown Genisys header"
    : `${decodedFrame.headerLabel} (${decodedFrame.protocolDirection})`;
  rows.push(`1. ${payloadBytes[0]} - header byte - ${headerLabel}`);

  const terminatorIndex = bytes.lastIndexOf(0xf6);
  const bodyEndExclusive = terminatorIndex === -1 ? bytes.length : terminatorIndex;
  const bodyTokens = parseGenisysFrameBodyTokens(bytes, bodyEndExclusive);
  const hasCrc = decodedFrame.headerCode !== 0xf1 && !(decodedFrame.headerCode === 0xfb && bodyTokens.length < 2);
  const crcTokenStart = hasCrc ? Math.max(1, bodyTokens.length - 2) : bodyTokens.length;

  bodyTokens.forEach((token, tokenIndex) => {
    const rawIndexesLabel = token.rawIndexes.map((rawIndex) => rawIndex + 1).join("/");
    const rawBytesLabel = token.rawIndexes.map((rawIndex) => payloadBytes[rawIndex]).join(" ");
    const valueLabel = `0x${formatHexByte(token.value)}`;
    const escapedNote = token.escaped ? `escaped byte ${rawBytesLabel} -> ${valueLabel}` : rawBytesLabel;
    let role = "";
    if (tokenIndex === 0) {
      role = `server/station address - ${serverAddressLabel}`;
    } else if (hasCrc && tokenIndex === crcTokenStart) {
      role = "CRC-16 low byte (cl)";
    } else if (hasCrc && tokenIndex === crcTokenStart + 1) {
      role = "CRC-16 high byte (ch)";
    } else {
      const payloadIndex = tokenIndex - 1;
      const pairNumber = Math.floor(payloadIndex / 2) + 1;
      role = payloadIndex % 2 === 0
        ? `payload pair ${pairNumber} byte number/address (bn)`
        : `payload pair ${pairNumber} byte information/data (bi)`;
    }
    rows.push(`${rawIndexesLabel}. ${escapedNote} - ${role}`);
  });

  if (terminatorIndex >= 0) {
    rows.push(`${terminatorIndex + 1}. ${payloadBytes[terminatorIndex]} - frame terminator byte`);
    for (let index = terminatorIndex + 1; index < payloadBytes.length; index += 1) {
      rows.push(`${index + 1}. ${payloadBytes[index]} - trailing byte after terminator`);
    }
  } else {
    rows.push("Decode note: frame terminator byte 0xF6 was not present.");
  }

  return rows;
}

function findPairedSocketRawFrame(
  index: number,
  lines: ParsedLine[],
  events: ParsedLogEvent[],
): { line: ParsedLine; event: Extract<ParsedLogEvent, { family: "socket-raw-frame" }>; deltaLabel: string; decode: DecodedGenisysSocketFrame } | null {
  const currentAssembly = assembleSocketRawFrame(index, lines, events);
  const currentLine = currentAssembly?.line ?? null;
  const currentEvent = currentAssembly?.event ?? null;
  if (!currentAssembly || !currentLine || !currentEvent || !currentLine.timestamp) {
    return null;
  }
  const currentTimestamp = parseLogTimestamp(currentLine.timestamp);
  if (currentTimestamp === null) {
    return null;
  }
  const currentDecode = currentAssembly.decode;
  let best: { line: ParsedLine; event: Extract<ParsedLogEvent, { family: "socket-raw-frame" }>; deltaMs: number; decode: DecodedGenisysSocketFrame } | null = null;

  for (let cursor = Math.max(0, index - 6); cursor <= Math.min(events.length - 1, index + 6); cursor += 1) {
    if (cursor === index) {
      continue;
    }
    const candidateAssembly = assembleSocketRawFrame(cursor, lines, events);
    if (!candidateAssembly || candidateAssembly.startIndex !== cursor || !candidateAssembly.line.timestamp) {
      continue;
    }
    if (candidateAssembly.startIndex === currentAssembly.startIndex) {
      continue;
    }
    const candidateEvent = candidateAssembly.event;
    const candidateLine = candidateAssembly.line;
    if (normalizeLookupKey(candidateEvent.stationToken) !== normalizeLookupKey(currentEvent.stationToken)) {
      continue;
    }
    const candidateTimestamp = parseLogTimestamp(candidateLine.timestamp);
    if (candidateTimestamp === null) {
      continue;
    }
    const deltaMs = Math.abs(candidateTimestamp - currentTimestamp);
    if (deltaMs > 250) {
      continue;
    }
    const candidateDecode = candidateAssembly.decode;
    const sameServer =
      currentDecode.serverAddress !== null &&
      candidateDecode.serverAddress !== null &&
      currentDecode.serverAddress === candidateDecode.serverAddress;
    const oppositeAction = currentEvent.socketAction !== candidateEvent.socketAction;
    const complementaryHeaders =
      (currentDecode.headerCode === 0xfb && candidateDecode.headerCode === 0xf1) ||
      (currentDecode.headerCode === 0xf1 && candidateDecode.headerCode === 0xfb) ||
      (currentDecode.headerCode === 0xfa && candidateDecode.headerCode === 0xf2) ||
      (currentDecode.headerCode === 0xf2 && candidateDecode.headerCode === 0xfa);
    if (!(sameServer || complementaryHeaders || oppositeAction)) {
      continue;
    }
    if (!best || deltaMs < best.deltaMs) {
      best = { line: candidateLine, event: candidateEvent, deltaMs, decode: candidateDecode };
    }
  }

  if (!best) {
    return null;
  }
  return {
    line: best.line,
    event: best.event,
    deltaLabel: describeDelta(currentLine.timestamp, best.line.timestamp) ?? `${best.deltaMs} ms apart`,
    decode: best.decode,
  };
}

type SocketRawFrameEvent = Extract<ParsedLogEvent, { family: "socket-raw-frame" }>;
type PairedSocketRawFrame = ReturnType<typeof findPairedSocketRawFrame>;
type AssembledSocketRawFrame = {
  startIndex: number;
  endIndex: number;
  line: ParsedLine;
  event: SocketRawFrameEvent;
  payloadBytes: string[];
  decode: DecodedGenisysSocketFrame;
  fragmentCount: number;
};

function hasSocketFrameTerminator(payloadBytes: string[]): boolean {
  return payloadBytes.some((value) => parseSocketHexByte(value) === 0xf6);
}

function isSameSocketRawFrameStream(left: SocketRawFrameEvent, right: SocketRawFrameEvent): boolean {
  return left.socketAction === right.socketAction
    && normalizeLookupKey(left.stationToken) === normalizeLookupKey(right.stationToken);
}

function assembleSocketRawFrame(
  index: number,
  lines: ParsedLine[],
  events: ParsedLogEvent[],
): AssembledSocketRawFrame | null {
  const line = lines[index];
  const event = events[index];
  if (!line || !event || event.family !== "socket-raw-frame") {
    return null;
  }

  let startIndex = index;
  while (startIndex > 0) {
    const previous = events[startIndex - 1];
    if (!previous || previous.family !== "socket-raw-frame" || !isSameSocketRawFrameStream(previous, event)) {
      break;
    }
    if (hasSocketFrameTerminator(previous.payloadBytes)) {
      break;
    }
    startIndex -= 1;
  }

  const payloadBytes: string[] = [];
  let endIndex = startIndex;
  for (let cursor = startIndex; cursor < events.length; cursor += 1) {
    const current = events[cursor];
    if (!current || current.family !== "socket-raw-frame" || !isSameSocketRawFrameStream(current, event)) {
      break;
    }
    payloadBytes.push(...current.payloadBytes);
    endIndex = cursor;
    if (hasSocketFrameTerminator(current.payloadBytes)) {
      break;
    }
  }

  const anchorLine = lines[startIndex];
  const anchorEvent = events[startIndex];
  if (!anchorLine || !anchorEvent || anchorEvent.family !== "socket-raw-frame") {
    return null;
  }

  return {
    startIndex,
    endIndex,
    line: anchorLine,
    event: anchorEvent,
    payloadBytes,
    decode: decodeGenisysSocketFrame(payloadBytes),
    fragmentCount: endIndex - startIndex + 1,
  };
}

function describeSocketRawFramePhase(event: SocketRawFrameEvent, decodedFrame: DecodedGenisysSocketFrame): string {
  if (decodedFrame.headerCode === 0xfb && event.socketAction === "XMT") {
    return "Office poll request sent to the field endpoint";
  }
  if (decodedFrame.headerCode === 0xf1 && event.socketAction === "RCV") {
    return "Field endpoint acknowledge-client reply received";
  }
  if (decodedFrame.headerCode === 0xfc && event.socketAction === "XMT") {
    return "Office control-data request sent to the field endpoint";
  }
  if (decodedFrame.headerCode === 0xf2 && event.socketAction === "RCV") {
    return "Field indication-data response received";
  }
  if (decodedFrame.headerCode === 0xfd && event.socketAction === "XMT") {
    return "Office recall-header request sent to the field endpoint";
  }
  if (decodedFrame.headerLabel !== "Unknown") {
    return `${decodedFrame.headerLabel} ${decodedFrame.protocolDirection} frame observed`;
  }
  return event.socketAction === "XMT" ? "Raw transport frame sent to the field endpoint" : "Raw transport frame received from the field endpoint";
}

function describeSocketRawFrameCycle(decodedFrame: DecodedGenisysSocketFrame): string {
  if (decodedFrame.headerCode === 0xfb || decodedFrame.headerCode === 0xf1) {
    return "Routine poll / acknowledge cycle";
  }
  if (decodedFrame.headerCode === 0xfc || decodedFrame.headerCode === 0xf3) {
    return "Control delivery / checkback cycle";
  }
  if (decodedFrame.headerCode === 0xfa || decodedFrame.headerCode === 0xf2) {
    return "Indication return / acknowledgement cycle";
  }
  if (decodedFrame.headerCode === 0xfd) {
    return "Recall cycle";
  }
  if (decodedFrame.headerLabel !== "Unknown") {
    return `${decodedFrame.headerLabel} transport cycle`;
  }
  return "Raw Genisys transport cycle";
}

function describePairedSocketFrameRelationship(
  currentEvent: SocketRawFrameEvent,
  currentDecode: DecodedGenisysSocketFrame,
  pairedFrame: NonNullable<PairedSocketRawFrame>,
): { label: string; summary: string; reason: string } {
  const relationLabel =
    currentDecode.protocolDirection === "request" && pairedFrame.decode.protocolDirection === "response"
      ? "Related transport reply"
      : currentDecode.protocolDirection === "response" && pairedFrame.decode.protocolDirection === "request"
        ? "Related transport request"
        : "Related transport frame";
  const serverLabel =
    currentDecode.serverAddress !== null && pairedFrame.decode.serverAddress !== null && currentDecode.serverAddress === pairedFrame.decode.serverAddress
      ? `server ${currentDecode.serverAddress} (0x${formatHexByte(currentDecode.serverAddress)})`
      : "the same endpoint";

  let summary = `${pairedFrame.decode.headerLabel} ${pairedFrame.decode.protocolDirection} ${pairedFrame.deltaLabel}.`;
  if (currentDecode.headerCode === 0xfb && pairedFrame.decode.headerCode === 0xf1 && currentEvent.socketAction === "XMT") {
    summary = `This line is the office Poll request, and the related line is the field endpoint's Acknowledge Client reply ${pairedFrame.deltaLabel}.`;
  } else if (currentDecode.headerCode === 0xf1 && pairedFrame.decode.headerCode === 0xfb && currentEvent.socketAction === "RCV") {
    summary = `This line is the field endpoint's Acknowledge Client reply, and the related line is the preceding office Poll request ${pairedFrame.deltaLabel}.`;
  } else if (currentDecode.protocolDirection === "request" && pairedFrame.decode.protocolDirection === "response") {
    summary = `This line is the request side of the exchange, and the related line is the response ${pairedFrame.deltaLabel}.`;
  } else if (currentDecode.protocolDirection === "response" && pairedFrame.decode.protocolDirection === "request") {
    summary = `This line is the response side of the exchange, and the related line is the request ${pairedFrame.deltaLabel}.`;
  }

  const reasons = [
    `same endpoint ${currentEvent.stationToken}`,
    `same ${serverLabel}`,
    currentEvent.socketAction !== pairedFrame.event.socketAction ? "opposite transport direction" : "",
    `${pairedFrame.deltaLabel}`,
  ].filter(Boolean);

  return {
    label: relationLabel,
    summary,
    reason: `Linked because the frames share ${reasons.join(", ")}.`,
  };
}

function buildSocketRawFrameWorkflowDetails(
  index: number,
  lines: ParsedLine[],
  events: ParsedLogEvent[],
  bundle: LogEnrichmentBundle | null,
): WorkflowDetailsResult {
  const assembly = assembleSocketRawFrame(index, lines, events);
  const line = assembly?.line ?? null;
  const event = assembly?.event ?? null;
  if (!assembly || !line || !event) {
    return { lines: [], related: [] };
  }

  const decodedFrame = assembly.decode;
  const pairedFrame = findPairedSocketRawFrame(index, lines, events);
  const stationRow = findStationRow(bundle, event);
  const endpointLabel = stationRow?.station_name ?? event.stationToken ?? "unknown endpoint";
  const serverAddressLabel = describeGenisysServerAddress(decodedFrame.serverAddress, stationRow, bundle);

  const out = [
    "Workflow at this time:",
    `Cycle: ${describeSocketRawFrameCycle(decodedFrame)}`,
    `Current step: ${describeSocketRawFramePhase(event, decodedFrame)}`,
    `Endpoint: ${endpointLabel}`,
    `Server address: ${serverAddressLabel}`,
    ...(assembly.fragmentCount > 1 ? [`Transport fragments: ${assembly.fragmentCount} lines combined into one Genisys frame`] : []),
  ];
  const related: WorkflowRelatedDetail[] = [];

  if (pairedFrame) {
    const relation = describePairedSocketFrameRelationship(event, decodedFrame, pairedFrame);
    related.push({
      lineId: pairedFrame.line.id,
      lineNumber: pairedFrame.line.lineNumber,
      timestamp: pairedFrame.line.timestamp,
      raw: pairedFrame.line.raw,
      deltaLabel: pairedFrame.deltaLabel,
      relation: relation.summary,
    });
  }

  if (!pairedFrame) {
    out.push("");
    out.push("Pair status: no related request/response partner was found within 250 ms on the same endpoint.");
  }

  if (decodedFrame.headerCode === 0xfb || decodedFrame.headerCode === 0xf1) {
    out.push("Meaning: This is routine supervision traffic between the office and the field endpoint, not a decoded field command by itself.");
  } else if (decodedFrame.headerCode === 0xfc || decodedFrame.headerCode === 0xf2 || decodedFrame.headerCode === 0xfd) {
    out.push(`Meaning: This raw frame belongs to the ${describeSocketRawFrameCycle(decodedFrame).toLowerCase()}.`);
  }

  return { lines: out, related };
}

function describeMnemonicEntryWithAssignment(entry: { position: string; mnemonic: string; value: string }, assignment: CodeAssignmentEntry | null): string {
  if (!assignment) {
    return `${entry.position}. ${entry.mnemonic} = ${entry.value}`;
  }
  const normalizedValue = String(entry.value ?? "").trim().toUpperCase();
  if (normalizeLookupKey(assignment.mnemonic) === normalizeLookupKey(entry.mnemonic)) {
    return formatBooleanEntryLabel(entry.position, assignment, normalizedValue);
  }
  const assignmentName = describeAssignmentLongName(assignment);
  return `${entry.position}. ${entry.mnemonic} = ${entry.value}; assignment map position ${entry.position} is ${assignmentName}`;
}

function findAssignmentEntryByMnemonic(entries: CodeAssignmentEntry[], mnemonic: string): CodeAssignmentEntry | null {
  const normalizedMnemonic = normalizeLookupKey(mnemonic);
  if (!normalizedMnemonic) {
    return null;
  }
  const match = entries.find((entry) => normalizeLookupKey(entry.mnemonic) === normalizedMnemonic);
  return match ?? null;
}

function findRouteRows(bundle: LogEnrichmentBundle | null, stationRow: StationFoundationRow | null): RouteSwitchContextRow[] {
  if (!bundle || !stationRow) return [];
  const rows = [
    ...(bundle.routesByKey.get(normalizeLookupKey(stationRow.control_point_number)) ?? []),
    ...(bundle.routesByKey.get(normalizeLookupKey(stationRow.control_point_name)) ?? []),
  ];
  const seen = new Set<string>();
  return rows.filter((row) => {
    const key = `${row.route_guid}|${row.switch_uid}|${row.required_state}`;
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  }).slice(0, 8);
}

function findTrainRows(bundle: LogEnrichmentBundle | null, stationRow: StationFoundationRow | null): TrainRuntimeFoundationRow[] {
  if (!bundle || !stationRow) return [];
  return (bundle.trainsByKey.get(normalizeLookupKey(stationRow.control_point_name)) ?? []).slice(0, 6);
}

function findTrainRowsBySymbol(bundle: LogEnrichmentBundle | null, trainSymbol: string | undefined): TrainRuntimeFoundationRow[] {
  if (!bundle || !trainSymbol) return [];
  return (bundle.trainsByKey.get(normalizeLookupKey(trainSymbol)) ?? []).slice(0, 6);
}

function describeSocketDirection(direction: string | undefined, traceMethod: string | undefined): string {
  const method = (traceMethod ?? "").toUpperCase();
  if (method.includes("SEND")) {
    return "Sent";
  }
  if (method.includes("RECE")) {
    return "Received";
  }
  return direction ? "As logged" : "unknown";
}

function describeSocketTraceMethod(traceMethod: string | undefined): string {
  const method = (traceMethod ?? "").trim();
  if (!method) {
    return "unknown";
  }
  if (method.toUpperCase().includes("SEND")) {
    return `${method} (send path)`;
  }
  if (method.toUpperCase().includes("RECE")) {
    return `${method} (receive path)`;
  }
  return method;
}

function describeSocketHostRole(
  host: string,
  codeLineRow: CodeLineProtocolRow | null,
  bundle: LogEnrichmentBundle | null,
): string {
  const matchCodeLine = (row: CodeLineProtocolRow): string | null => {
    if (host === row.packet_switch_primary_ip) {
      return `packet switch primary ${row.packet_switch_primary_name} on ${row.code_line_name}`;
    }
    if (host === row.packet_switch_secondary_ip) {
      return `packet switch secondary ${row.packet_switch_secondary_name} on ${row.code_line_name}`;
    }
    return null;
  };

  if (codeLineRow) {
    const lineMatch = matchCodeLine(codeLineRow);
    if (lineMatch) {
      return lineMatch;
    }

    const knownPeers = [
      codeLineRow.packet_switch_primary_name && codeLineRow.packet_switch_primary_ip
        ? `${codeLineRow.packet_switch_primary_name} (${codeLineRow.packet_switch_primary_ip})`
        : "",
      codeLineRow.packet_switch_secondary_name && codeLineRow.packet_switch_secondary_ip
        ? `${codeLineRow.packet_switch_secondary_name} (${codeLineRow.packet_switch_secondary_ip})`
        : "",
    ].filter(Boolean);

    if (knownPeers.length) {
      return `not one of ${codeLineRow.code_line_name}'s configured packet-switch IPs; expected ${knownPeers.join(" / ")}`;
    }
  }

  if (bundle) {
    const matches = Array.from(bundle.codeLineByNumber.values())
      .map((row) => matchCodeLine(row))
      .filter((value): value is string => Boolean(value));
    if (matches.length === 1) {
      return matches[0];
    }
  }

  const inventoryMatches = bundle?.hostByIp.get(normalizeLookupKey(host)) ?? [];
  if (inventoryMatches.length === 1) {
    return inventoryMatches[0].name;
  }
  if (inventoryMatches.length > 1) {
    return inventoryMatches
      .map((row) => row.name)
      .filter((value, index, values) => values.indexOf(value) === index)
      .join(" / ");
  }

  return "no current host match";
}

function describeSocketHostName(
  host: string,
  codeLineRow: CodeLineProtocolRow | null,
  bundle: LogEnrichmentBundle | null,
): string {
  if (codeLineRow) {
    if (host === codeLineRow.packet_switch_primary_ip && codeLineRow.packet_switch_primary_name) {
      return codeLineRow.packet_switch_primary_name;
    }
    if (host === codeLineRow.packet_switch_secondary_ip && codeLineRow.packet_switch_secondary_name) {
      return codeLineRow.packet_switch_secondary_name;
    }
  }

  const inventoryMatches = bundle?.hostByIp.get(normalizeLookupKey(host)) ?? [];
  const uniqueNames = inventoryMatches
    .map((row) => row.name || row.hostname)
    .filter(Boolean)
    .filter((value, index, values) => values.indexOf(value) === index);

  if (uniqueNames.length === 1) {
    return uniqueNames[0];
  }
  if (uniqueNames.length > 1) {
    return uniqueNames.join(" / ");
  }
  return "unknown";
}

function describeGenisysServerAddress(
  serverAddress: number | null,
  stationRow: StationFoundationRow | null,
  bundle: LogEnrichmentBundle | null,
): string {
  if (serverAddress === null) {
    return "not decoded";
  }

  const hex = `0x${formatHexByte(serverAddress)}`;
  const candidates: StationFoundationRow[] = [];
  const pushCandidate = (row: StationFoundationRow | null | undefined) => {
    if (!row) {
      return;
    }
    if (toNumber(row.control_address) !== serverAddress && toNumber(row.indication_address) !== serverAddress) {
      return;
    }
    if (!candidates.some((candidate) => candidate.control_point_number === row.control_point_number)) {
      candidates.push(row);
    }
  };

  pushCandidate(stationRow);
  for (const candidate of bundle?.stationByAddress.get(String(serverAddress)) ?? []) {
    pushCandidate(candidate);
  }

  if (!candidates.length) {
    return `${serverAddress} (${hex})`;
  }

  const labels = candidates.map((row) => {
    const codeLineRow = bundle?.codeLineByNumber.get(String(row.code_line_number)) ?? null;
    const stationTitle = codeLineRow?.code_line_name || row.station_name;
    const stationLabel = uniqueLines([row.station_name, stationTitle]).join(" / ");
    return `${stationLabel} on CDLN-${row.code_line_number} (${row.subdivision_name})`;
  });

  return `${serverAddress} (${hex}) = ${labels.join(" | ")}`;
}

function summarizeCodeLineRange(rows: CodeLineProtocolRow[]): string {
  const unique = Array.from(
    new Map(rows.map((row) => [String(row.code_line_number), row])).values(),
  ).sort((a, b) => Number(a.code_line_number) - Number(b.code_line_number));
  if (!unique.length) {
    return "no grounded code-line range";
  }
  if (unique.length === 1) {
    return unique[0].code_line_name;
  }
  const numbers = unique.map((row) => Number(row.code_line_number)).filter((value) => Number.isFinite(value));
  const contiguous = numbers.length === unique.length && numbers.every((value, index) => index === 0 || value === numbers[index - 1] + 1);
  if (contiguous) {
    return `${unique[0].code_line_name} through ${unique[unique.length - 1].code_line_name}`;
  }
  if (unique.length <= 4) {
    return unique.map((row) => row.code_line_name).join(", ");
  }
  return `${unique[0].code_line_name} through ${unique[unique.length - 1].code_line_name}`;
}

function describeCodeServerPeerRole(
  codeServerName: string | undefined,
  bundle: LogEnrichmentBundle | null,
): string {
  if (!bundle || !codeServerName) {
    return "no grounded code-server role";
  }
  const normalized = normalizeLookupKey(codeServerName);
  const normalRows = Array.from(bundle.codeLineByNumber.values()).filter((row) => normalizeLookupKey(row.normal_codeserver_name) === normalized);
  const standbyRows = Array.from(bundle.codeLineByNumber.values()).filter((row) => normalizeLookupKey(row.standby_codeserver_name) === normalized);
  const parts: string[] = [];
  if (normalRows.length) {
    const subdivisions = Array.from(new Set(normalRows.map((row) => row.subdivision_names).filter(Boolean)));
    parts.push(`normal code server for ${summarizeCodeLineRange(normalRows)}${subdivisions.length === 1 ? ` (${subdivisions[0]})` : ""}`);
  }
  if (standbyRows.length) {
    const subdivisions = Array.from(new Set(standbyRows.map((row) => row.subdivision_names).filter(Boolean)));
    parts.push(`standby code server for ${summarizeCodeLineRange(standbyRows)}${subdivisions.length === 1 ? ` (${subdivisions[0]})` : ""}`);
  }
  return parts.length ? parts.join("; ") : "no grounded code-server role";
}

function describeHostInventoryContext(
  host: string | undefined,
  bundle: LogEnrichmentBundle | null,
): string {
  if (!host || !bundle) {
    return "no grounded host inventory context";
  }
  const matches = bundle.hostByIp.get(normalizeLookupKey(host)) ?? [];
  if (!matches.length) {
    return "no grounded host inventory context";
  }
  const types = Array.from(new Set(matches.map((row) => row.configuration_type_name).filter(Boolean)));
  const locations = Array.from(new Set(matches.map((row) => row.location_name).filter(Boolean)));
  const names = Array.from(new Set(matches.map((row) => row.name || row.hostname).filter(Boolean)));
  const parts = [
    names.length ? names.join(" / ") : "",
    types.length ? types.join(" / ") : "",
    locations.length ? locations.join(" / ") : "",
  ].filter(Boolean);
  return parts.length ? parts.join(" | ") : "no grounded host inventory context";
}

function describeComponentUid(
  uid: string,
  bundle: LogEnrichmentBundle | null,
): string {
  const row = bundle?.componentByUid.get(normalizeLookupKey(uid));
  if (!row) {
    return uid;
  }
  const name = normalizeComponentValue(row.component_name);
  const secondary = normalizeComponentValue(row.component_secondary_name);
  if (secondary) {
    return `${uid} (${name}; ${secondary})`;
  }
  return `${uid} (${name})`;
}

function describeComponentReference(
  uid: string,
  bundle: LogEnrichmentBundle | null,
): string {
  const row = bundle?.componentByUid.get(normalizeLookupKey(uid));
  if (!row) {
    return uid;
  }
  const family = normalizeComponentValue(row.component_family).replace(/_/g, " ").trim().toLowerCase();
  const familyLabel = family ? `${family} ` : "";
  const name = normalizeComponentValue(row.component_name);
  const secondary = normalizeComponentValue(row.component_secondary_name);
  const detail = normalizeComponentValue(row.component_detail_name);
  const context = [secondary, detail].filter(Boolean).join("; ");
  if (context) {
    return `${uid} = ${familyLabel}${name} (${context})`.trim();
  }
  return `${uid} = ${familyLabel}${name}`.trim();
}

function findStationRowByComponentUid(uid: string, bundle: LogEnrichmentBundle | null): StationFoundationRow | null {
  if (!bundle) {
    return null;
  }
  const componentRow = bundle.componentByUid.get(normalizeLookupKey(uid));
  if (!componentRow) {
    return null;
  }
  return bundle.stationByKey.get(normalizeLookupKey(componentRow.parent_control_point_uid)) ?? null;
}

function describeCadRoutePairByComponents(
  uidA: string,
  uidB: string,
  bundle: LogEnrichmentBundle | null,
): string {
  const matches = collectCadRoutePairRowsByComponents(uidA, uidB, bundle);
  const formatted = formatRouteGroup(matches, bundle);
  if (!formatted.length) {
    return "";
  }
  return formatted.join(" | ");
}

function describeCadSignalRouteContext(
  signalUid: string,
  bundle: LogEnrichmentBundle | null,
): string {
  const formatted = formatRouteGroup(collectCadSignalRouteRows(signalUid, bundle), bundle);
  return formatted.join(" | ");
}

function describeRelatedObjectsMeaning(
  uids: string[],
  bundle: LogEnrichmentBundle | null,
): string {
  const families = uids
    .map((uid) => normalizeComponentValue(bundle?.componentByUid.get(normalizeLookupKey(uid))?.component_family ?? "").replace(/_/g, " ").toLowerCase())
    .filter(Boolean);
  const uniqueFamilies = Array.from(new Set(families));
  if (uniqueFamilies.length === 1) {
    const familyLabel = uniqueFamilies[0];
    return `this message references ${uids.length} ${familyLabel} object${uids.length === 1 ? "" : "s"} tied to this control point.`;
  }
  return "this message references the TMDS objects listed above for this control point.";
}

function describeCadTransaction(transactionId: string): string {
  const match = /^TRANSACTION-(.+)$/i.exec(transactionId.trim());
  if (!match) {
    return transactionId;
  }
  return `${match[1]} (CAD correlation tag)`;
}

function describeTransportMeaning(
  family: "socket-keepalive" | "socket-alive",
  direction: string,
  peerLabel: string,
): string {
  if (family === "socket-keepalive") {
    if (direction === "Sent") {
      return `this side sent a connection check to ${peerLabel}.`;
    }
    if (direction === "Received") {
      return `this side received a connection check from ${peerLabel}.`;
    }
    return `a connection check was observed on the link with ${peerLabel}.`;
  }
  if (direction === "Sent") {
    return `this side sent a connection reply to ${peerLabel}.`;
  }
  if (direction === "Received") {
    return `this side received a connection reply from ${peerLabel}.`;
  }
  return `a connection reply was observed on the link with ${peerLabel}.`;
}

function describeTransportTraceContext(
  traceClass: string | undefined,
  traceMethod: string | undefined,
  traceThread: string | undefined,
): string {
  const parts = [String(traceClass ?? "").trim(), String(traceMethod ?? "").trim(), String(traceThread ?? "").trim()].filter(Boolean);
  if (!parts.length) {
    return "not shown in the raw line";
  }
  return parts.join(" / ");
}

function humanizeTraceToken(raw: string): string {
  return raw
    .replace(/\.[^.]+$/, "")
    .replace(/^cls/i, "")
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function describeTraceMetadataComponent(fileName: string): string {
  const fileToken = fileName.split(/[\\/]/).pop() ?? fileName;
  const humanized = humanizeTraceToken(fileToken);
  return humanized || "TMDS communication client";
}

function describeTraceMetadataAction(methodName: string): string {
  const normalized = methodName.trim();
  if (!normalized) {
    return "trace point reached";
  }
  const lower = normalized.toLowerCase();
  if (lower === ".ctor") {
    return "constructor entered";
  }
  if (lower === ".cctor") {
    return "static initialization entered";
  }
  return `${humanizeTraceToken(normalized)} method entered`;
}

function describeTraceMetadataType(fileName: string, methodName: string): string {
  const lower = methodName.trim().toLowerCase();
  if (lower === ".ctor" || lower === ".cctor") {
    return "TMDS communication client startup";
  }
  const component = describeTraceMetadataComponent(fileName);
  return `${component} trace event`;
}

function describeWorkflowCandidate(
  candidate: ParsedLine,
  candidateEvent: ParsedLogEvent,
  bundle: LogEnrichmentBundle | null,
): string {
  switch (candidateEvent.family) {
    case "station-indication": {
      const stationRow = findStationRow(bundle, candidateEvent);
      const codeLineRow = bundle?.codeLineByNumber.get(String(candidateEvent.codeLineNumber)) ?? null;
      return `Indication frame for ${stationRow?.station_name ?? candidateEvent.stationToken ?? candidateEvent.controlPointNumber} on code line ${candidateEvent.codeLineNumber}${codeLineRow ? ` (${codeLineRow.code_line_name})` : ""}`;
    }
    case "socket-indication": {
      const stationRow = findStationRow(bundle, candidateEvent);
      return `Socket indication on ${candidateEvent.host ?? "unknown host"} for ${stationRow?.station_name ?? candidateEvent.controlPointNumber}`;
    }
    case "station-recall":
      return `Recall sent for ${candidateEvent.stationToken}`;
    case "code-line-command":
      return `${candidateEvent.commandKind} command ${candidateEvent.action.toLowerCase()} for ${candidateEvent.stationToken}`;
    case "code-line-queue-count":
      return `Process-send queue count ${candidateEvent.queueCount}`;
    case "code-line-process-send-queue":
      return candidateEvent.stationToken && candidateEvent.commandKind
        ? `Process-send queue ${candidateEvent.commandKind.toLowerCase()} for ${candidateEvent.stationToken}`
        : `Process-send queue entry ${candidateEvent.queueText}`;
    case "code-line-process-indication":
      return `Processed indication for ${candidateEvent.stationToken}`;
    case "code-line-control-sent":
      return `Control payload sent for ${candidateEvent.stationToken}`;
    case "code-line-control-mnemonic":
      return `Control mnemonic breakdown (${candidateEvent.entries.length} fields)`;
    case "code-line-indication-mnemonic":
      return `Indication mnemonic breakdown (${candidateEvent.entries.length} fields)`;
    case "code-line-control-payload":
      return `${candidateEvent.phase} payload (${candidateEvent.payloadBits.length} bits)`;
    case "code-line-control-queue-cleared":
      return candidateEvent.payloadBits
        ? `Control queue cleared snapshot (${candidateEvent.payloadBits.length} bits)`
        : "Control queue cleared";
    case "code-line-control-process-completed":
      return "Control processing completed";
    case "code-line-control-queue-print":
      return "Control queue print requested";
    case "code-line-statistics-summary":
      return `Code-line statistics snapshot for ${candidateEvent.stationToken}`;
    case "genisys-control-resend":
      return `Genisys control resend retry ${candidateEvent.retryCurrent}/${candidateEvent.retryTotal} for ${candidateEvent.stationToken}`;
    case "socket-raw-frame": {
      const decodedFrame = decodeGenisysSocketFrame(candidateEvent.payloadBytes);
      const endpoint = candidateEvent.stationToken || "unknown endpoint";
      return `${candidateEvent.socketAction} ${decodedFrame.headerLabel} on ${endpoint}`;
    }
    case "socket-keepalive":
      return `KEEPALIVE on ${candidateEvent.host || candidateEvent.codeServerName || "unknown peer"}`;
    case "socket-alive":
      return `ALIVE response on ${candidateEvent.host || candidateEvent.codeServerName || "unknown peer"}`;
    case "code-server-queue":
      return `Code-server message queue count ${candidateEvent.queueCount} on ${candidateEvent.codeServerName}`;
    case "code-server-thread-alive":
      return `Indication-processing thread alive on ${candidateEvent.codeServerName}`;
    case "cad-control-point-message":
      return `CAD control-point message for ${candidateEvent.controlPointNumber} (${candidateEvent.operationToken})`;
    case "cad-signal-message":
      return `CAD signal message for ${candidateEvent.signalUid} (${candidateEvent.operationToken})`;
    case "cad-train-message":
      return `CAD train ${candidateEvent.eventToken} for ${candidateEvent.trainSymbol}`;
    case "process-vital-signs":
      return `Process vital signs on ${candidateEvent.workstation ?? "unknown workstation"}`;
    case "process-vital-signs-header":
      return `${candidateEvent.component} vital signs ${candidateEvent.softwareVersion}`;
    case "thread-capacity":
      return `${candidateEvent.capacityKind} .NET thread capacity ${candidateEvent.maxWorkerThreads}/${candidateEvent.maxIoCompletionThreads}`;
    case "signal-state-change":
      return `Signal state change ${candidateEvent.stateToken}`;
    case "signal-indication-update":
      return `Signal indication ${candidateEvent.signalName} at ${candidateEvent.controlPointToken}`;
    case "track-traffic-removal-check":
      return `Traffic-removal check ${candidateEvent.trackUid} ${candidateEvent.decision}`;
    case "named-guid-catalog-entry":
      return `GUID catalog entry ${candidateEvent.label}`;
    case "sql-train-update-continuation":
      return `SQL train update continuation ${candidateEvent.trainSymbol ?? ""}`.trim();
    case "compact-track-state":
      return `Compact track state ${candidateEvent.trackStatus}`;
    case "gbo-ptc-transmission-status":
      return `GBO PTC transmission ${candidateEvent.trainSymbol} ${candidateEvent.inProgress}`;
    case "code-server-online-status":
      return `Code-server online status ${candidateEvent.onlineStatus}`;
    case "bos-server-list-entry":
      return `${candidateEvent.availability} ${candidateEvent.serverName}(${candidateEvent.serverId})`;
    case "connection-endpoint-status":
      return `${candidateEvent.scope} connection ${candidateEvent.host}:${candidateEvent.port}`;
    case "flash-name":
      return `${candidateEvent.sourceKind === "event" ? "Flash event" : "Flash target"} ${candidateEvent.flashName}`;
    case "indication-message-complete":
      return `Indication message complete on ${candidateEvent.codeServerName}`;
    case "train-schedule-timer-check":
      return `Train schedule timer check ${candidateEvent.trainSymbol} at ${candidateEvent.locationToken}`;
    case "trace-metadata":
      return `${describeTraceMetadataType(candidateEvent.fileName, candidateEvent.methodName)} on ${describeTraceMetadataComponent(candidateEvent.fileName)}`;
    case "host-connection-refused":
      return `Connection refused by ${candidateEvent.host}:${candidateEvent.port}`;
    case "workstation-request-line":
      return `Workstation request ${candidateEvent.requestType} for ${candidateEvent.subject}`;
    case "cad-forwarded-vetms":
      return `Forwarded ${candidateEvent.messageType} update to ${candidateEvent.workstation}`;
    case "plain-vetms-message":
      return `Plain VETMS ${candidateEvent.messageDirection} ${candidateEvent.messageType}`;
    case "territory-train-list":
      return `Train list requested for ${candidateEvent.territoryToken}`;
    case "workstation-transaction-marker":
      return candidateEvent.transactionId
        ? `${candidateEvent.workstation} transaction ${candidateEvent.transactionId}`
        : `${candidateEvent.workstation} workstation marker`;
    case "indication-change-trigger":
      return `Indication change triggered by ${candidateEvent.caller}`;
    case "locomotive-processing-marker":
      return candidateEvent.stage === "SummaryUpdateCompleted"
        ? "Locomotive summary update completed"
        : "Locomotive position-report check finished";
    case "track-tracing-marker":
      return `${candidateEvent.phase} tracing ${candidateEvent.subject}`;
    case "system-thread-heartbeat":
      return `${candidateEvent.threadName} heartbeat`;
    case "control-queue-event":
      return candidateEvent.eventName;
    case "repeated-binary-state":
      return `Repeated ${candidateEvent.stateKind.toLowerCase()} payload`;
    case "network-stack-frame":
      return candidateEvent.method;
    case "application-stack-frame":
      return candidateEvent.method;
    case "direction-state-entry":
      return `${candidateEvent.direction} ${candidateEvent.code} ${candidateEvent.state}`;
    case "admin-click-action":
      return `Admin clicked ${candidateEvent.action}`;
    case "user-interface-marker":
      return candidateEvent.markerText;
    case "control-send-phase-marker":
      return `${candidateEvent.routine} ${candidateEvent.phase.toLowerCase()}`;
    case "indication-bit-inversion":
      return `Indication bit ${candidateEvent.bitIndex} inverted`;
    case "short-workflow-marker":
      return `${candidateEvent.prefix}${candidateEvent.marker}`;
    case "route-selection-step":
      return `${candidateEvent.processName} step ${candidateEvent.step}`;
    case "stored-route-event-marker":
      return `${candidateEvent.eventGroup} ${candidateEvent.step}`;
    case "stored-route-recursion-check":
      return `Stored-route recursion check ${candidateEvent.phase.toLowerCase()}`;
    case "system-reset-marker":
      return candidateEvent.markerText;
    case "stored-route-status-marker":
      return candidateEvent.markerText;
    case "code-station-load-count":
      return `${candidateEvent.count} code stations loaded from database`;
    case "exception-trace-separator":
      return "Inner exception stack trace ended";
    case "blank-log-entry":
      return "Blank log separator line";
    case "workstation-originated-train-log":
      return `Workstation-originated ${candidateEvent.eventToken ?? "train"} log`;
    case "train-tracking-message":
      return `${candidateEvent.prefix} ${candidateEvent.action}`.trim();
    case "plain-control-sent":
      return `Plain control sent for ${candidateEvent.stationToken}`;
    case "mpar-event":
      return `MPAR ${candidateEvent.eventName}`;
    case "route-search-message":
      return candidateEvent.action === "component"
        ? `${candidateEvent.searchKind} ${candidateEvent.componentUid ?? ""}`.trim()
        : `${candidateEvent.searchKind} marker ${candidateEvent.marker ?? ""}`.trim();
    case "component-reference-list-entry":
      return `${candidateEvent.entryKind} ${candidateEvent.componentUid ?? candidateEvent.componentName ?? ""}`.trim();
    case "loco-log-marker":
      return candidateEvent.markerText;
    case "loco-log-entry":
      return `${candidateEvent.severity} ${candidateEvent.component}`;
    case "office-telemetry-summary":
      return `Office ${candidateEvent.direction} message ${candidateEvent.messageId} on ${candidateEvent.channel}`;
    case "office-telemetry-hex":
      return `Office ${candidateEvent.direction} payload on ${candidateEvent.channel}`;
    case "recorder-delimited-record":
      return `${candidateEvent.recorder} recorded ${candidateEvent.payloadFields.length} pipe fields`;
    case "locomotive-recorder-record":
      return `Locomotive recorder ${candidateEvent.recordType} record`;
    case "raw-hex-payload":
      return `Raw hex payload ${candidateEvent.byteCount} bytes`;
    default:
      return "Unclassified raw line in the same workflow window";
  }
}

type WorkflowWindowIndex = {
  timestampMsByIndex: Array<number | null>;
  secondBuckets: Map<number, number[]>;
};

type WorkflowDetailsResult = {
  lines: string[];
  related: WorkflowRelatedDetail[];
};

function buildWorkflowWindowIndex(lines: ParsedLine[]): WorkflowWindowIndex {
  const timestampMsByIndex = lines.map((line) => parseLogTimestamp(line.timestamp));
  const secondBuckets = new Map<number, number[]>();
  for (let index = 0; index < timestampMsByIndex.length; index += 1) {
    const timestampMs = timestampMsByIndex[index];
    if (timestampMs === null) {
      continue;
    }
    const secondKey = Math.floor(timestampMs / 1000);
    const bucket = secondBuckets.get(secondKey) ?? [];
    bucket.push(index);
    secondBuckets.set(secondKey, bucket);
  }
  return { timestampMsByIndex, secondBuckets };
}

function findWorkflowDetails(
  index: number,
  lines: ParsedLine[],
  events: ParsedLogEvent[],
  bundle: LogEnrichmentBundle | null,
  workflowIndex?: WorkflowWindowIndex | null,
): WorkflowDetailsResult {
  const line = lines[index];
  const event = events[index];
  if (!line || !event || !line.timestamp) return { lines: [], related: [] };
  if (event.family === "socket-raw-frame") {
    return buildSocketRawFrameWorkflowDetails(index, lines, events, bundle);
  }

  const baseTimestamp = workflowIndex?.timestampMsByIndex[index] ?? parseLogTimestamp(line.timestamp);
  if (baseTimestamp === null) {
    return { lines: [], related: [] };
  }

  const stationRow = findStationRow(bundle, event);
  const scopeTokens = uniqueLines([
    stationRow?.station_name,
    stationRow?.control_point_name,
    stationRow?.control_point_number,
    "stationToken" in event ? event.stationToken : "",
    "controlPointNumber" in event ? event.controlPointNumber : "",
    "host" in event ? event.host : "",
    "trainSymbol" in event ? event.trainSymbol : "",
    "trainUid" in event ? event.trainUid : "",
  ])
    .map((token) => normalizeLookupKey(token))
    .filter(Boolean);

  const blocks = new Map<string, WorkflowRelatedDetail & { absDelta: number }>();
  const maxWorkflowDeltaMs = 1500;
  const candidateIndexes = (() => {
    if (!workflowIndex) {
      return Array.from({ length: lines.length }, (_, candidateIndex) => candidateIndex);
    }
    const seen = new Set<number>();
    const collected: number[] = [];
    const baseSecond = Math.floor(baseTimestamp / 1000);
    for (let second = baseSecond - 2; second <= baseSecond + 2; second += 1) {
      for (const candidateIndex of workflowIndex.secondBuckets.get(second) ?? []) {
        if (!seen.has(candidateIndex)) {
          seen.add(candidateIndex);
          collected.push(candidateIndex);
        }
      }
    }
    return collected;
  })();

  for (const candidateIndex of candidateIndexes) {
    if (candidateIndex === index) continue;
    const candidateEvent = events[candidateIndex];
    const candidateAssembly = candidateEvent.family === "socket-raw-frame"
      ? assembleSocketRawFrame(candidateIndex, lines, events)
      : null;
    if (candidateAssembly && candidateAssembly.startIndex !== candidateIndex) {
      continue;
    }
    const candidate = candidateAssembly?.line ?? lines[candidateIndex];
    if (!candidate.timestamp || candidate.source === line.source || !candidate.raw || candidate.raw === line.raw) {
      continue;
    }

    const candidateTimestamp = workflowIndex?.timestampMsByIndex[candidateIndex] ?? parseLogTimestamp(candidate.timestamp);
    if (candidateTimestamp === null) {
      continue;
    }
    const absDelta = Math.abs(candidateTimestamp - baseTimestamp);
    if (absDelta > maxWorkflowDeltaMs) {
      continue;
    }

    const stationMatch =
      ("stationToken" in event && event.stationToken && "stationToken" in candidateEvent && candidateEvent.stationToken &&
        normalizeLookupKey(event.stationToken) === normalizeLookupKey(candidateEvent.stationToken)) ||
      ("controlPointNumber" in event && "controlPointNumber" in candidateEvent &&
        event.controlPointNumber === candidateEvent.controlPointNumber);
    const hostMatch =
      ("host" in event && "host" in candidateEvent && event.host && candidateEvent.host && event.host === candidateEvent.host);
    const textMatch = scopeTokens.some((token) => normalizeLookupKey(candidate.raw).includes(token));
    const exactTimeMatch = candidate.timestamp === line.timestamp;

    if (!(stationMatch || hostMatch || textMatch || exactTimeMatch)) {
      continue;
    }

    const key = candidate.id;
    const delta = describeDelta(line.timestamp, candidate.timestamp) ?? "same time";
    const role = describeWorkflowCandidate(candidate, candidateEvent, bundle);
    const existing = blocks.get(key);
    if (existing) {
      if (absDelta < existing.absDelta) {
        existing.absDelta = absDelta;
        existing.deltaLabel = delta;
      }
      continue;
    }

    blocks.set(key, {
      lineId: candidate.id,
      lineNumber: candidate.lineNumber,
      timestamp: candidate.timestamp,
      raw: candidate.raw,
      relation: role,
      deltaLabel: delta,
      absDelta,
    });
  }

  if (!blocks.size) {
    return { lines: [], related: [] };
  }

  const out = ["Workflow at this time:"];
  const ordered = Array.from(blocks.values())
    .sort((a, b) => a.absDelta - b.absDelta || a.raw.localeCompare(b.raw))
    .slice(0, 8);
  if (!ordered.length) {
    return { lines: [], related: [] };
  }
  return {
    lines: out,
    related: ordered.map(({ absDelta, ...block }) => block),
  };
}

function makeGenericLineDetail(line: ParsedLine): DetailModel {
  return {
    lineId: line.id,
    lineNumber: line.lineNumber,
    timestamp: line.timestamp,
    raw: line.raw,
    translation: {
      original: line.raw,
      structured: [],
      english: [],
      unresolved: [],
    },
    workflow: {
      summary: "",
      currentStep: "",
      systems: [],
      objects: [],
      knownState: "",
      unresolved: [],
    },
    genisysContext: [],
    icdContext: [],
    databaseContext: [],
    sourceReferences: [],
  };
}

function formatActiveControlLines(
  payloadBits: string,
  assignmentRow: CodeStationAssignmentMapRow | null,
): ActiveIndicationSummary {
  const assertedPositions = getAssertedPayloadPositions(payloadBits);
  const positionMap = buildPositionMap(assignmentRow?.control_assignments ?? []);
  const structured = [
    `Asserted payload positions: ${assertedPositions.length ? assertedPositions.join(", ") : "none"}`,
  ];
  const buckets = emptyActiveBuckets();
  const items = emptyActiveItemBuckets();
  const canExpand = Boolean(
    assignmentRow &&
    assignmentRow.control_assignments.length === payloadBits.length &&
    assertedPositions.every((position) => positionMap.has(position)),
  );

  if (!assertedPositions.length) {
    return {
      meaning: ["Active at this line: no payload positions are asserted."],
      structured,
      buckets,
      items,
      assertedPositions,
      expanded: canExpand,
    };
  }

  if (!canExpand) {
    return {
      meaning: [`Active payload positions at this line: ${assertedPositions.join(", ")}.`],
      structured,
      buckets,
      items,
      assertedPositions,
      expanded: false,
    };
  }

  const activeEntries = assertedPositions
    .map((position) => ({
      position,
      entry: positionMap.get(position) ?? null,
    }))
    .filter((item) => item.entry);

  for (const item of activeEntries) {
    const family = classifyActiveEntry(item.entry as CodeAssignmentEntry);
    buckets[family].push(formatActiveEntryLabel(item.position, item.entry as CodeAssignmentEntry));
    items[family].push(item as ActiveAssignmentItem);
  }

  const meaning = ["Active at this line:"];
  if (buckets.switches.length) meaning.push(`Switches: ${buckets.switches.join("; ")}`);
  if (buckets.signals.length) meaning.push(`Signals: ${buckets.signals.join("; ")}`);
  if (buckets.tracks.length) meaning.push(`Tracks: ${buckets.tracks.join("; ")}`);
  if (buckets.routes.length) meaning.push(`Routes: ${buckets.routes.join("; ")}`);
  if (buckets.local.length) meaning.push(`Local / device: ${buckets.local.join("; ")}`);
  if (buckets.other.length) meaning.push(`Other: ${buckets.other.join("; ")}`);
  if (!buckets.switches.length && !buckets.signals.length && !buckets.tracks.length && !buckets.routes.length && !buckets.local.length && !buckets.other.length) {
    meaning.push("No named control assignments are asserted in this payload.");
  }

  structured.push(...activeEntries.map((item) => formatActiveEntryLabel(item.position, item.entry as CodeAssignmentEntry)));

  return { meaning, structured, buckets, items, assertedPositions, expanded: true };
}

function buildDetailForLine(
  index: number,
  lines: ParsedLine[],
  events: ParsedLogEvent[],
  bundle: LogEnrichmentBundle | null,
  workflowIndex?: WorkflowWindowIndex | null,
): DetailModel {
  const line = lines[index];
  const event = events[index];
  if (!line) {
    return makeGenericLineDetail({
      id: `missing:${index}`,
      lineNumber: index + 1,
      raw: "",
      tokens: [],
    });
  }

  const generic = makeGenericLineDetail(line);
  const stationRow = findStationRow(bundle, event);
  const assignmentRow = findAssignmentRow(bundle, stationRow, event);
  const eventTrainRows = "trainSymbol" in event ? findTrainRowsBySymbol(bundle, event.trainSymbol) : [];
  const eventCodeLineNumber = "codeLineNumber" in event ? event.codeLineNumber : stationRow?.code_line_number ?? "";
  const codeLineRow = bundle?.codeLineByNumber.get(String(eventCodeLineNumber)) ?? (stationRow ? bundle?.codeLineByNumber.get(String(stationRow.code_line_number)) ?? null : null);
  const workflowDetails = findWorkflowDetails(index, lines, events, bundle, workflowIndex);
  const tmdsContext: string[] = [];
  const payloadContext: string[] = [];
  const meaning: string[] = [];
  const structured: string[] = [];
  let relatedPair: DetailModel["relatedPair"] | undefined;
  let workflowRelated: DetailModel["workflowRelated"] | undefined;
  let workflowSummary = "";
  let currentStep = "Observed";
  let knownState = "Observed";

  if (stationRow) {
    tmdsContext.push(
      "Station / asset map:",
      `${stationRow.station_name} / control point ${stationRow.control_point_number} (${stationRow.control_point_name})`,
      `Subdivision: ${stationRow.subdivision_name}`,
      `Code line ${stationRow.code_line_number}: ${stationRow.code_line_name}`,
      `Station inventory: signals=${toNumber(stationRow.signal_count)}, tracks=${toNumber(stationRow.track_count)}, switches=${toNumber(stationRow.switch_count)}, routes=${toNumber(stationRow.route_count)}`,
    );
  }

  workflowRelated = workflowDetails.related;

  if (assignmentRow) {
    tmdsContext.push(...formatAssignmentCatalog("Indication bits:", assignmentRow.indication_assignments));
    tmdsContext.push(...formatAssignmentCatalog("Control bits:", assignmentRow.control_assignments));
  }
  if (eventTrainRows.length) {
    tmdsContext.push(...buildTrainRuntimeLines(eventTrainRows));
  }

  switch (event.family) {
    case "station-indication": {
      const stats = payloadStats(event.payloadBits);
      const activeState = formatActiveIndicationLines(event.payloadBits, event.wordNumber, assignmentRow);
      const operationalReading = buildIndicationOperationalReading(activeState.items, activeState.expanded);
      const wordScope = describeIndicationWordScope(event.payloadBits, event.wordNumber, assignmentRow);
      workflowSummary = "Station indication frame.";
      currentStep = "Indication received";
      knownState = stats.asserted === 0 ? "All logged payload bits clear" : "One or more payload bits asserted";
      meaning.push(
        "Selected event:",
        "Type: station indication",
        `Station: ${stationRow?.station_name ?? event.stationToken ?? "station match not found"}`,
        `Control point: ${stationRow ? `${stationRow.control_point_number} (${stationRow.control_point_name})` : event.controlPointNumber}`,
        `Code line: ${event.codeLineNumber}${codeLineRow ? ` (${codeLineRow.code_line_name})` : ""}`,
        wordScope,
        `Bits active: ${stats.asserted} of ${event.payloadBits.length}`,
        `Active bit positions: ${activeState.assertedPositions.length ? activeState.assertedPositions.join(", ") : "none"}`,
        ...operationalReading.details,
      );
      break;
    }
    case "socket-indication": {
      const stats = payloadStats(event.payloadBits);
      const activeState = formatActiveIndicationLines(event.payloadBits, event.wordNumber, assignmentRow);
      const operationalReading = buildIndicationOperationalReading(activeState.items, activeState.expanded);
      const wordScope = describeIndicationWordScope(event.payloadBits, event.wordNumber, assignmentRow);
      const hostName = event.host ? describeSocketHostName(event.host, codeLineRow, bundle) : "unknown";
      workflowSummary = "Socket indication frame.";
      currentStep = "Indication frame observed";
      knownState = stats.asserted === 0 ? "All logged payload bits clear" : "One or more payload bits asserted";
      meaning.push(
        "Selected event:",
        "Type: socket indication",
        `Host: ${event.host ?? "unknown"}`,
        `Host name: ${hostName}`,
        ...("codeServerName" in event && event.codeServerName ? [`Code server: ${event.codeServerName}`] : []),
        `Station: ${stationRow?.station_name ?? "station match not found"}`,
        `Control point: ${stationRow ? `${stationRow.control_point_number} (${stationRow.control_point_name})` : event.controlPointNumber}`,
        `Code line: ${event.codeLineNumber}${codeLineRow ? ` (${codeLineRow.code_line_name})` : ""}`,
        wordScope,
        `Bits active: ${stats.asserted} of ${event.payloadBits.length}`,
        `Active bit positions: ${activeState.assertedPositions.length ? activeState.assertedPositions.join(", ") : "none"}`,
        ...operationalReading.details,
      );
      break;
    }
    case "station-recall": {
      workflowSummary = "Recall request.";
      currentStep = "Recall sent";
      knownState = "Recall cycle logged";
      meaning.push(
        "Selected event:",
        "Type: recall sent",
        `Station: ${stationRow?.station_name ?? event.stationToken}`,
        `Control point: ${stationRow ? `${stationRow.control_point_number} (${stationRow.control_point_name})` : "station match not found"}`,
        `Recall sequence: ${event.sequence}`,
        "Action: recall request for this station.",
      );
      break;
    }
    case "code-line-command": {
      const controlState = event.payloadBits ? formatActiveControlLines(event.payloadBits, assignmentRow) : null;
      workflowSummary = "Code-line command entry.";
      currentStep = `${event.commandKind} command ${event.action.toLowerCase()}`;
      knownState = `${event.commandKind} ${event.action}`;
      meaning.push(
        "Selected event:",
        "Type: code-line command",
        `Action: ${event.action}`,
        `Command: ${event.commandKind}`,
        `Station: ${stationRow?.station_name ?? event.stationToken}`,
        ...(stationRow ? [`Control point: ${stationRow.control_point_number} (${stationRow.control_point_name})`] : []),
        ...(codeLineRow ? [`Code line: ${codeLineRow.code_line_number} (${codeLineRow.code_line_name})`] : []),
        ...(event.payloadBits ? [`Payload width: ${event.payloadBits.length} bits`] : []),
        ...(controlState ? controlState.meaning : []),
      );
      if (event.payloadBits && controlState) {
        payloadContext.push(
          `Payload bits: ${event.payloadBits}`,
          `Asserted positions: ${controlState.assertedPositions.length ? controlState.assertedPositions.join(", ") : "none"}`,
          ...controlState.structured,
        );
      }
      break;
    }
    case "code-line-queue-count": {
      workflowSummary = "Process-send queue count.";
      currentStep = "Queue count logged";
      knownState = `${event.queueCount} queued command${event.queueCount === "1" ? "" : "s"}`;
      meaning.push(
        "Selected event:",
        "Type: process-send queue count",
        `Queued commands: ${event.queueCount}`,
        `Status: the code-line send queue reported ${event.queueCount} command${event.queueCount === "1" ? "" : "s"} pending at this moment.`,
      );
      break;
    }
    case "code-line-process-send-queue": {
      const nearbyStationToken = event.stationToken || findNearbyStationToken(index, events) || undefined;
      const nearbyStationRow = stationRow ?? findStationRowByNameToken(bundle, nearbyStationToken);
      workflowSummary = "Process-send queue entry.";
      currentStep = "Queued command selected for processing";
      knownState = event.commandKind
        ? `${event.commandKind} queued for ${nearbyStationRow?.station_name ?? nearbyStationToken ?? event.queueText}`
        : event.queueText;
      meaning.push(
        "Selected event:",
        "Type: process-send queue item",
        `Queue text: ${event.queueText}`,
        ...(nearbyStationRow ? [`Station: ${nearbyStationRow.station_name}`] : nearbyStationToken ? [`Station: ${nearbyStationToken}`] : []),
        ...(nearbyStationRow ? [`Control point: ${nearbyStationRow.control_point_number} (${nearbyStationRow.control_point_name})`] : []),
        ...(event.commandKind ? [`Command: ${event.commandKind}`] : []),
        `Status: this line shows the queued item that the code-line sender is about to process.`,
      );
      break;
    }
    case "code-line-process-indication": {
      const stats = payloadStats(event.payloadBits);
      const activeState = formatActiveIndicationLines(event.payloadBits, "1", assignmentRow);
      const operationalReading = buildIndicationOperationalReading(activeState.items, activeState.expanded);
      workflowSummary = "Processed indication payload.";
      currentStep = "Indication payload interpreted";
      knownState = stats.asserted === 0 ? "All logged payload bits clear" : "One or more payload bits asserted";
      meaning.push(
        "Selected event:",
        "Type: processed code-line indication",
        `Code-station number: ${event.codeStationNumber}`,
        `Station: ${stationRow?.station_name ?? event.stationToken}`,
        ...(stationRow ? [`Control point: ${stationRow.control_point_number} (${stationRow.control_point_name})`] : []),
        ...(codeLineRow ? [`Code line: ${codeLineRow.code_line_number} (${codeLineRow.code_line_name})`] : []),
        `Bits active: ${stats.asserted} of ${event.payloadBits.length}`,
        `Active bit positions: ${activeState.assertedPositions.length ? activeState.assertedPositions.join(", ") : "none"}`,
        ...operationalReading.details,
      );
      payloadContext.push(`Payload bits: ${event.payloadBits}`, ...activeState.structured);
      break;
    }
    case "code-line-control-sent": {
      const controlState = formatActiveControlLines(event.payloadBits, assignmentRow);
      workflowSummary = "Control payload sent.";
      currentStep = "Control command transmitted";
      knownState = controlState.assertedPositions.length ? "One or more control bits asserted" : "All logged control bits clear";
      meaning.push(
        "Selected event:",
        "Type: control payload sent",
        `Code-station number: ${event.codeStationNumber}`,
        `Station: ${stationRow?.station_name ?? event.stationToken}`,
        ...(stationRow ? [`Control point: ${stationRow.control_point_number} (${stationRow.control_point_name})`] : []),
        ...(codeLineRow ? [`Code line: ${codeLineRow.code_line_number} (${codeLineRow.code_line_name})`] : []),
        `Payload width: ${event.payloadBits.length} bits`,
        `Active bit positions: ${controlState.assertedPositions.length ? controlState.assertedPositions.join(", ") : "none"}`,
        ...controlState.meaning,
      );
      payloadContext.push(`Payload bits: ${event.payloadBits}`, ...controlState.structured);
      break;
    }
    case "code-line-control-mnemonic": {
      const nearbyStationToken = findNearbyStationToken(index, events);
      const nearbyStationRow = stationRow ?? findStationRowByNameToken(bundle, nearbyStationToken ?? undefined);
      const nearbyAssignmentRow = assignmentRow ?? findAssignmentRowByKeys(bundle, [
        nearbyStationRow?.control_point_number,
        nearbyStationRow?.station_name,
        nearbyStationRow?.control_point_name,
        nearbyStationToken ?? "",
      ]);
      const controlPositionMap = buildPositionMap(nearbyAssignmentRow?.control_assignments ?? []);
      const mappedEntries = event.entries.map((entry) => ({
        entry,
        assignment:
          controlPositionMap.get(Number(entry.position))
          ?? findAssignmentEntryByMnemonic(nearbyAssignmentRow?.control_assignments ?? [], entry.mnemonic)
          ?? null,
      }));
      workflowSummary = "Control mnemonic breakdown.";
      currentStep = "Control field breakdown logged";
      knownState = `${event.entries.length} mnemonic fields`;
      meaning.push(
        "Selected event:",
        "Type: control mnemonic map",
        `Mnemonic fields: ${event.entries.length}`,
        `Station: ${nearbyStationRow?.station_name ?? nearbyStationToken ?? "station context not found in nearby lines"}`,
        ...(nearbyStationRow ? [`Control point: ${nearbyStationRow.control_point_number} (${nearbyStationRow.control_point_name})`] : []),
        ...(nearbyStationRow ? [`Code line: ${nearbyStationRow.code_line_number} (${nearbyStationRow.code_line_name})`] : []),
        `Status: this line expands the control payload into named boolean states for the same control cycle.`,
        ...mappedEntries.map(({ entry, assignment }) => describeMnemonicEntryWithAssignment(entry, assignment)),
      );
      payloadContext.push(
        "Mnemonic values:",
        ...mappedEntries.map(({ entry, assignment }) => describeMnemonicEntryWithAssignment(entry, assignment)),
      );
      break;
    }
    case "code-line-indication-mnemonic": {
      const nearbyStationToken = findNearbyStationToken(index, events);
      const nearbyStationRow = stationRow ?? findStationRowByNameToken(bundle, nearbyStationToken ?? undefined);
      const nearbyAssignmentRow = assignmentRow ?? findAssignmentRowByKeys(bundle, [
        nearbyStationRow?.control_point_number,
        nearbyStationRow?.station_name,
        nearbyStationRow?.control_point_name,
        nearbyStationToken ?? "",
      ]);
      const indicationPositionMap = buildPositionMap(nearbyAssignmentRow?.indication_assignments ?? []);
      const mappedEntries = event.entries.map((entry) => ({
        entry,
        assignment:
          indicationPositionMap.get(Number(entry.position))
          ?? findAssignmentEntryByMnemonic(nearbyAssignmentRow?.indication_assignments ?? [], entry.mnemonic)
          ?? null,
      }));
      workflowSummary = "Indication mnemonic breakdown.";
      currentStep = "Indication field breakdown logged";
      knownState = `${event.entries.length} indication mnemonic fields`;
      meaning.push(
        "Selected event:",
        "Type: indication mnemonic map",
        `Mnemonic fields: ${event.entries.length}`,
        `Station: ${nearbyStationRow?.station_name ?? nearbyStationToken ?? "station context not found in nearby lines"}`,
        ...(nearbyStationRow ? [`Control point: ${nearbyStationRow.control_point_number} (${nearbyStationRow.control_point_name})`] : []),
        ...(nearbyStationRow ? [`Code line: ${nearbyStationRow.code_line_number} (${nearbyStationRow.code_line_name})`] : []),
        `Status: this line expands nearby indication positions into named boolean states for the same code-station cycle.`,
        ...mappedEntries.map(({ entry, assignment }) => describeMnemonicEntryWithAssignment(entry, assignment)),
      );
      payloadContext.push(
        "Indication mnemonic values:",
        ...mappedEntries.map(({ entry, assignment }) => describeMnemonicEntryWithAssignment(entry, assignment)),
      );
      break;
    }
    case "code-line-control-payload": {
      const nearbyStationToken = findNearbyStationToken(index, events) || undefined;
      const nearbyStationRow = stationRow ?? findStationRowByNameToken(bundle, nearbyStationToken);
      const nearbyAssignmentRow = assignmentRow ?? findAssignmentRowByKeys(bundle, [
        nearbyStationRow?.control_point_number,
        nearbyStationRow?.station_name,
        nearbyStationRow?.control_point_name,
        nearbyStationToken ?? "",
      ]);
      const controlState = formatActiveControlLines(event.payloadBits, nearbyAssignmentRow);
      workflowSummary = event.phase === "SendControl" ? "Raw SendControl payload." : "Raw ProcessControlBegin payload.";
      currentStep = event.phase === "SendControl" ? "Control payload issued" : "Control payload processing started";
      knownState = controlState.assertedPositions.length ? "One or more control bits asserted" : "All logged control bits clear";
      meaning.push(
        "Selected event:",
        `Type: ${event.phase}`,
        ...(nearbyStationRow ? [`Station: ${nearbyStationRow.station_name}`] : nearbyStationToken ? [`Station: ${nearbyStationToken}`] : []),
        ...(nearbyStationRow ? [`Control point: ${nearbyStationRow.control_point_number} (${nearbyStationRow.control_point_name})`] : []),
        ...(nearbyStationRow ? [`Code line: ${nearbyStationRow.code_line_number} (${nearbyStationRow.code_line_name})`] : []),
        ...(event.codeStationNumber ? [`Code-station number: ${event.codeStationNumber}`] : []),
        `Payload width: ${event.payloadBits.length} bits`,
        `Active bit positions: ${controlState.assertedPositions.length ? controlState.assertedPositions.join(", ") : "none"}`,
        ...controlState.meaning,
      );
      payloadContext.push(`Payload bits: ${event.payloadBits}`, ...controlState.structured);
      break;
    }
    case "code-line-control-queue-cleared": {
      const nearbyStationToken = findNearbyStationToken(index, events) || undefined;
      const nearbyStationRow = stationRow ?? findStationRowByNameToken(bundle, nearbyStationToken);
      const nearbyAssignmentRow = assignmentRow ?? findAssignmentRowByKeys(bundle, [
        nearbyStationRow?.control_point_number,
        nearbyStationRow?.station_name,
        nearbyStationRow?.control_point_name,
        nearbyStationToken ?? "",
      ]);
      const controlState = event.payloadBits ? formatActiveControlLines(event.payloadBits, nearbyAssignmentRow) : null;
      workflowSummary = "Control queue cleared.";
      currentStep = "Queued control snapshot cleared";
      knownState = event.payloadBits
        ? (controlState?.assertedPositions.length ? "Queued control bits were present before the clear" : "Cleared queue snapshot contained no asserted bits")
        : "Queue clear logged with no payload snapshot";
      meaning.push(
        "Selected event:",
        "Type: control queue being cleared",
        ...(nearbyStationRow ? [`Station: ${nearbyStationRow.station_name}`] : nearbyStationToken ? [`Station: ${nearbyStationToken}`] : []),
        ...(nearbyStationRow ? [`Control point: ${nearbyStationRow.control_point_number} (${nearbyStationRow.control_point_name})`] : []),
        ...(nearbyStationRow ? [`Code line: ${nearbyStationRow.code_line_number} (${nearbyStationRow.code_line_name})`] : []),
        ...(event.payloadBits ? [`Queued payload width: ${event.payloadBits.length} bits`] : []),
        ...(controlState ? [`Asserted positions before clear: ${controlState.assertedPositions.length ? controlState.assertedPositions.join(", ") : "none"}`] : []),
        ...(controlState ? controlState.meaning : ["Status: the control queue clear was logged with no payload bits shown in this line."]),
      );
      if (event.payloadBits && controlState) {
        payloadContext.push(
          "Queued control snapshot:",
          `Payload bits: ${event.payloadBits}`,
          ...controlState.structured,
        );
      }
      break;
    }
    case "code-line-control-process-completed": {
      const nearbyStationToken = findNearbyStationToken(index, events) || undefined;
      const nearbyStationRow = stationRow ?? findStationRowByNameToken(bundle, nearbyStationToken);
      workflowSummary = "Control processing completed.";
      currentStep = "Control queue cycle finished";
      knownState = "Control process completed";
      meaning.push(
        "Selected event:",
        "Type: ProcessControlCompleted",
        ...(nearbyStationRow ? [`Station: ${nearbyStationRow.station_name}`] : nearbyStationToken ? [`Station: ${nearbyStationToken}`] : []),
        ...(nearbyStationRow ? [`Control point: ${nearbyStationRow.control_point_number} (${nearbyStationRow.control_point_name})`] : []),
        ...(nearbyStationRow ? [`Code line: ${nearbyStationRow.code_line_number} (${nearbyStationRow.code_line_name})`] : []),
        "Status: the current control-processing cycle reached its completion marker.",
      );
      break;
    }
    case "code-line-control-queue-print": {
      const nearbyStationToken = findNearbyStationToken(index, events) || undefined;
      const nearbyStationRow = stationRow ?? findStationRowByNameToken(bundle, nearbyStationToken);
      workflowSummary = "Control queue print requested.";
      currentStep = "Control queue dump emitted";
      knownState = "Queue print marker logged";
      meaning.push(
        "Selected event:",
        "Type: PrintControlQueue",
        ...(nearbyStationRow ? [`Station: ${nearbyStationRow.station_name}`] : nearbyStationToken ? [`Station: ${nearbyStationToken}`] : []),
        ...(nearbyStationRow ? [`Control point: ${nearbyStationRow.control_point_number} (${nearbyStationRow.control_point_name})`] : []),
        ...(nearbyStationRow ? [`Code line: ${nearbyStationRow.code_line_number} (${nearbyStationRow.code_line_name})`] : []),
        "Status: this marker indicates the control queue was being dumped/logged for inspection at this point in the cycle.",
      );
      break;
    }
    case "code-line-statistics-summary": {
      const controlCount = Number(event.controlCount);
      const indicationCount = Number(event.indicationCount);
      const failureCount = Number(event.failureCount);
      const countsAreNumeric = [controlCount, indicationCount, failureCount].every(Number.isFinite);
      const ratio = countsAreNumeric && controlCount > 0
        ? (indicationCount / controlCount).toFixed(2)
        : null;
      workflowSummary = "Code-line statistics summary.";
      currentStep = "Hourly counters snapshot logged";
      knownState = failureCount > 0
        ? `${event.failureCount} logged failure${event.failureCount === "1" ? "" : "s"} in this summary window`
        : "No logged failures in this summary window";
      meaning.push(
        "Selected event:",
        "Type: code-line statistics summary",
        `Station: ${stationRow?.station_name ?? event.stationToken}`,
        ...(stationRow ? [`Control point: ${stationRow.control_point_number} (${stationRow.control_point_name})`] : []),
        ...(codeLineRow ? [`Code line: ${codeLineRow.code_line_number} (${codeLineRow.code_line_name})`] : []),
        `Control messages counted: ${event.controlCount}`,
        `Indication messages counted: ${event.indicationCount}`,
        `Logged failures counted: ${event.failureCount}`,
        ...(ratio ? [`Indications per control: ${ratio}`] : []),
        failureCount > 0
          ? `Status: ${event.failureCount} failure event${event.failureCount === "1" ? "" : "s"} were recorded in this summary window.`
          : "Status: no failures were recorded in this summary window.",
      );
      payloadContext.push(
        "Statistics fields:",
        `NAME = ${event.stationToken}`,
        `CTLS = ${event.controlCount}`,
        `INDS = ${event.indicationCount}`,
        `FAILS = ${event.failureCount}`,
      );
      break;
    }
    case "control-delivery-timer-stop": {
      workflowSummary = "Control-delivery timer stopped.";
      currentStep = "Control-delivery timeout cleared";
      knownState = "Delivery timer stopped";
      meaning.push(
        "Selected event:",
        "Type: control-delivery timer stop",
        "Status: the control-delivery watchdog/timer was stopped after a delivery/update event.",
      );
      break;
    }
    case "code-line-hex-frame": {
      const decodedFrame = decodeGenisysSocketFrame(event.payloadBytes);
      const serverAddressLabel = describeGenisysServerAddress(decodedFrame.serverAddress, stationRow, bundle);
      const byteRows = decodedFrame.headerCode !== null && genisysHeaderLabels.has(decodedFrame.headerCode)
        ? describeGenisysSocketByteRoles(event.payloadBytes, decodedFrame, serverAddressLabel)
        : describeGenericHexByteRows(event.payloadBytes, "hex-frame");
      workflowSummary = `${event.frameLabel} raw frame`;
      currentStep = `${event.frameLabel} frame decoded`;
      knownState = decodedFrame.headerLabel;
      meaning.push(
        "Selected event:",
        `Type: ${event.frameLabel.toLowerCase()} raw frame`,
        `Decoded header: ${decodedFrame.headerLabel}${decodedFrame.headerCode === null ? "" : ` (0x${formatHexByte(decodedFrame.headerCode)})`}`,
        `Protocol role: ${decodedFrame.protocolDirection}`,
        ...(decodedFrame.serverAddress !== null ? [`Server address: ${serverAddressLabel}`] : []),
        `Payload bytes: ${event.payloadBytes.length}`,
        ...decodedFrame.issues.map((issue) => `Decode note: ${issue}`),
      );
      payloadContext.push(
        decodedFrame.headerCode !== null && genisysHeaderLabels.has(decodedFrame.headerCode)
          ? "Genisys byte roles:"
          : "Hex byte details:",
        ...byteRows,
      );
      break;
    }
    case "code-line-indication-summary": {
      const nearbyStationRow = stationRow ?? findStationRowByNameToken(bundle, event.stationToken);
      workflowSummary = "Indication summary marker.";
      currentStep = "Indication associated with control point";
      knownState = event.stationToken ?? event.codeToken;
      meaning.push(
        "Selected event:",
        "Type: indication summary",
        `Code token: ${event.codeToken}`,
        ...(event.stationToken ? [`Station: ${event.stationToken}`] : []),
        ...(nearbyStationRow ? [`Control point: ${nearbyStationRow.control_point_number} (${nearbyStationRow.control_point_name})`] : []),
        "Status: the indication payload that preceded this marker was tied to this control-point/station token.",
      );
      break;
    }
    case "code-line-process-indication-phase": {
      const nearbyStationRow = stationRow ?? findStationRowByNameToken(bundle, event.stationToken);
      workflowSummary = "ProcessIndication phase marker.";
      currentStep = event.phase === "ProcessInformationBit" ? "Information bit phase processed" : "Indication phase processed";
      knownState = event.stationToken;
      meaning.push(
        "Selected event:",
        `Type: ${event.phase}`,
        `Station token: ${event.stationToken}`,
        ...(nearbyStationRow ? [`Control point: ${nearbyStationRow.control_point_number} (${nearbyStationRow.control_point_name})`] : []),
        "Status: this phase marker shows the code-line indication-processing loop advancing on the named code server/station token.",
      );
      break;
    }
    case "code-line-control-update": {
      const nearbyStationRow = stationRow ?? findStationRowByNameToken(bundle, event.stationToken);
      const nearbyAssignmentRow = assignmentRow ?? findAssignmentRowByKeys(bundle, [
        nearbyStationRow?.control_point_number,
        nearbyStationRow?.station_name,
        nearbyStationRow?.control_point_name,
        event.stationToken ?? "",
      ]);
      const controlState = formatActiveControlLines(event.payloadBits, nearbyAssignmentRow);
      workflowSummary = event.updateOnly ? "Control update-only frame." : "Control update frame.";
      currentStep = event.updateOnly ? "Control image updated without resend" : "Control image updated";
      knownState = controlState.assertedPositions.length ? "One or more control bits asserted" : "All logged control bits clear";
      meaning.push(
        "Selected event:",
        `Type: ${event.updateOnly ? "control update only" : "control updated"}`,
        `Code token: ${event.codeToken}`,
        ...(event.stationToken ? [`Station: ${event.stationToken}`] : []),
        ...(nearbyStationRow ? [`Control point: ${nearbyStationRow.control_point_number} (${nearbyStationRow.control_point_name})`] : []),
        ...(event.queueCount ? [`Queue count: ${event.queueCount}`] : []),
        `Payload width: ${event.payloadBits.length} bits`,
        ...controlState.meaning,
      );
      payloadContext.push(`Payload bits: ${event.payloadBits}`, ...controlState.structured);
      break;
    }
    case "code-line-control-delivered": {
      const nearbyStationRow = stationRow ?? findStationRowByNameToken(bundle, event.stationToken);
      const nearbyAssignmentRow = assignmentRow ?? findAssignmentRowByKeys(bundle, [
        nearbyStationRow?.control_point_number,
        nearbyStationRow?.station_name,
        nearbyStationRow?.control_point_name,
        event.stationToken ?? "",
      ]);
      const controlState = formatActiveControlLines(event.payloadBits, nearbyAssignmentRow);
      workflowSummary = "Control delivered.";
      currentStep = "Control delivery confirmed";
      knownState = controlState.assertedPositions.length ? "Delivered control bits asserted" : "Delivered image all clear";
      meaning.push(
        "Selected event:",
        "Type: control delivered",
        `Code token: ${event.codeToken}`,
        ...(event.stationToken ? [`Station: ${event.stationToken}`] : []),
        ...(nearbyStationRow ? [`Control point: ${nearbyStationRow.control_point_number} (${nearbyStationRow.control_point_name})`] : []),
        ...controlState.meaning,
      );
      payloadContext.push(`Payload bits: ${event.payloadBits}`, ...controlState.structured);
      break;
    }
    case "socket-control": {
      const hostName = describeSocketHostName(event.host, codeLineRow, bundle);
      const controlState = formatActiveControlLines(event.payloadBits, assignmentRow);
      workflowSummary = event.updateOnly ? "Socket control update-only frame." : "Socket control frame.";
      currentStep = event.updateOnly ? "Control update-only transmitted" : "Control transmitted";
      knownState = controlState.assertedPositions.length ? "One or more control bits asserted" : "All logged control bits clear";
      meaning.push(
        "Selected event:",
        `Type: ${event.updateOnly ? "socket control update only" : "socket control"}`,
        `Host: ${event.host}`,
        `Host name: ${hostName}`,
        `Code line: ${event.codeLineNumber}${codeLineRow ? ` (${codeLineRow.code_line_name})` : ""}`,
        `Control point: ${stationRow ? `${stationRow.control_point_number} (${stationRow.control_point_name})` : event.controlPointNumber}`,
        `Word: ${event.wordNumber}`,
        ...controlState.meaning,
      );
      payloadContext.push(`Payload bits: ${event.payloadBits}`, ...controlState.structured);
      break;
    }
    case "code-line-queue-depth": {
      workflowSummary = "Control queue depth snapshot.";
      currentStep = "Queue depth logged";
      knownState = `${event.queueCount} queued item${event.queueCount === "1" ? "" : "s"}`;
      meaning.push(
        "Selected event:",
        "Type: control queue depth",
        `Queue count: ${event.queueCount}`,
        ...(event.component ? [`Component: ${event.component}`] : []),
        ...(event.method ? [`Method: ${event.method}`] : []),
        ...(event.traceLineNumber ? [`Trace line: ${event.traceLineNumber}`] : []),
        `Status: the queue depth at this point was ${event.queueCount}.`,
      );
      break;
    }
    case "code-line-service-message": {
      const nearbyStationRow = stationRow ?? findStationRowByNameToken(bundle, event.stationToken);
      workflowSummary = "Service/control outcome marker.";
      currentStep = "Service message logged";
      knownState = event.statusToken;
      meaning.push(
        "Selected event:",
        "Type: service message",
        `Status token: ${event.statusToken}`,
        `Code token: ${event.codeToken}`,
        ...(event.stationToken ? [`Station: ${event.stationToken}`] : []),
        ...(nearbyStationRow ? [`Control point: ${nearbyStationRow.control_point_number} (${nearbyStationRow.control_point_name})`] : []),
      );
      break;
    }
    case "code-line-control-image": {
      workflowSummary = "Control image snapshot.";
      currentStep = event.phase;
      knownState = event.phase;
      meaning.push(
        "Selected event:",
        "Type: control image snapshot",
        `Phase: ${event.phase}`,
        `Payload width: ${event.payloadBits.length} bits`,
      );
      payloadContext.push(`Payload bits: ${event.payloadBits}`);
      break;
    }
    case "code-line-recall-auto": {
      const nearbyStationRow = stationRow ?? findStationRowByNameToken(bundle, event.stationToken);
      workflowSummary = "Automatic local recall.";
      currentStep = "Automatic recall transmitted";
      knownState = event.stationToken;
      meaning.push(
        "Selected event:",
        "Type: automatic local recall",
        `Station: ${event.stationToken}`,
        ...(nearbyStationRow ? [`Control point: ${nearbyStationRow.control_point_number} (${nearbyStationRow.control_point_name})`] : []),
        "Status: the code-line logic issued an automatic recall for the last indication image at this location.",
      );
      break;
    }
    case "code-line-last-indication-auto-recall": {
      const nearbyStationRow = stationRow ?? findStationRowByNameToken(bundle, event.stationToken);
      workflowSummary = "Auto-recall source indication.";
      currentStep = "Last indication selected for recall";
      knownState = event.stationToken;
      meaning.push(
        "Selected event:",
        "Type: last indication for auto recall",
        `Station: ${event.stationToken}`,
        ...(nearbyStationRow ? [`Control point: ${nearbyStationRow.control_point_number} (${nearbyStationRow.control_point_name})`] : []),
      );
      break;
    }
    case "socket-keepalive":
    case "socket-alive": {
      const hostName = describeSocketHostName(event.host, null, bundle);
      const direction = describeSocketDirection(event.direction, event.traceMethod);
      const serverScope = "codeServerName" in event && event.codeServerName ? describeCodeServerPeerRole(event.codeServerName, bundle) : "";
      const endpointLabel =
        "codeServerName" in event && event.codeServerName && event.host
          ? `code server ${event.codeServerName} (${event.host})`
          : "codeServerName" in event && event.codeServerName
            ? `code server ${event.codeServerName}`
            : event.host && hostName !== "unknown"
              ? `host ${hostName}`
              : event.host
                ? `host ${event.host}`
                : "peer endpoint not shown in raw line";
      workflowSummary = "Transport session traffic.";
      currentStep = event.family === "socket-keepalive" ? "Keepalive observed" : "Alive response observed";
      knownState = "Transport session traffic";
      meaning.push(
        "Selected event:",
        `Type: ${event.family === "socket-keepalive" ? "connection check (KEEPALIVE)" : "connection reply (ALIVE)"}`,
        `Direction: ${direction}`,
        ...(event.host ? [`Host: ${event.host}`] : []),
        ...(event.host ? [`Host name: ${hostName}`] : []),
        ...("codeServerName" in event && event.codeServerName ? [`Server: ${event.codeServerName}`] : []),
        ...(serverScope && serverScope !== "no grounded code-server role" ? [`Server assignment: ${serverScope}`] : []),
        `Status: ${describeTransportMeaning(event.family, direction, endpointLabel)}`,
      );
      break;
    }
    case "commserver-data-message": {
      workflowSummary = "CommServer inbound data message.";
      currentStep = "CommServer payload received";
      knownState = event.payload || "Message received";
      meaning.push(
        "Selected event:",
        "Type: CommServer data message",
        `Peer: ${event.peerToken}`,
        `Payload: ${event.payload || "(blank)"}`,
      );
      break;
    }
    case "commserver-train-processing": {
      workflowSummary = "CommServer active-train processing.";
      currentStep = "Active-train row processed";
      knownState = event.trainSymbol;
      meaning.push(
        "Selected event:",
        "Type: active-train processing",
        `Train symbol: ${event.trainSymbol}`,
      );
      break;
    }
    case "commserver-sql-query": {
      workflowSummary = "CommServer SQL diagnostic.";
      currentStep = "Diagnostic query logged";
      knownState = event.queryKind;
      meaning.push(
        "Selected event:",
        "Type: CommServer SQL diagnostic",
        `Query kind: ${event.queryKind}`,
        `Status: this diagnostic line shows the exact SQL text the CommServer emitted while processing train-schedule state.`,
      );
      payloadContext.push("SQL text:", event.sqlText);
      break;
    }
    case "track-indication-update": {
      const trackStationRow = findStationRowByComponentUid(event.trackUid, bundle) ?? findStationRowByNameToken(bundle, event.controlPointToken);
      workflowSummary = "Track indication update.";
      currentStep = "Track indication received";
      knownState = event.statusTokens.join(" / ") || "Track update captured";
      meaning.push(
        "Selected event:",
        "Type: track indication update",
        `Control point: ${trackStationRow ? `${trackStationRow.control_point_number} (${trackStationRow.control_point_name})` : event.controlPointToken}`,
        `Track: ${describeComponentReference(event.trackUid, bundle)}`,
        `Track name token: ${event.trackName}`,
        `Update states: ${event.statusTokens.join(", ") || "none"}`,
      );
      break;
    }
    case "track-traffic-removal-check": {
      const trackStationRow = findStationRowByComponentUid(event.trackUid, bundle);
      workflowSummary = "Track traffic-removal check.";
      currentStep = "Traffic-removal decision logged";
      knownState = `${event.decision} ${event.directionToken}`;
      meaning.push(
        "Selected event:",
        "Type: track traffic-removal check",
        `Decision: ${event.decision}`,
        `Track: ${describeComponentReference(event.trackUid, bundle)}`,
        `Track name token: ${event.trackName}`,
        `Traffic direction: ${event.directionToken}`,
        ...(trackStationRow ? [`Control point: ${trackStationRow.control_point_number} (${trackStationRow.control_point_name})`] : []),
        "Status: this track-indication routine logged whether existing traffic could be removed from the named track in the stated direction.",
      );
      break;
    }
    case "ptcbos-message": {
      workflowSummary = event.rawKind === "raw" ? "PTCBOS raw payload received." : "PTCBOS decoded payload received.";
      currentStep = event.rawKind === "raw" ? "Raw PTCBOS message logged" : "Decoded PTCBOS message logged";
      knownState = event.payload || "PTCBOS message";
      meaning.push(
        "Selected event:",
        `Type: ${event.rawKind === "raw" ? "PTCBOS raw message" : "PTCBOS message"}`,
        `Payload: ${event.payload || "(blank)"}`,
      );
      break;
    }
    case "code-server-queue": {
      const hostName = describeSocketHostName(event.host, null, bundle);
      const serverScope = describeCodeServerPeerRole(event.codeServerName, bundle);
      workflowSummary = "Code-server queue status.";
      currentStep = "Queue count observed";
      knownState = `Queue count ${event.queueCount}`;
      meaning.push(
        "Selected event:",
        "Type: code-server message queue",
        "Direction: Received",
        `Host: ${event.host}`,
        `Host name: ${hostName}`,
        `Server: ${event.codeServerName}`,
        ...(serverScope !== "no grounded code-server role" ? [`Server assignment: ${serverScope}`] : []),
        `Queue count: ${event.queueCount}`,
        `Queued message type: not shown in this log.`,
        `Specific code line shown: no; this line only identifies the shared server, not one code line.`,
        `Status: ${event.codeServerName} reported ${event.queueCount} queued message${event.queueCount === "1" ? "" : "s"}.`,
      );
      break;
    }
    case "code-server-thread-alive": {
      const hostName = describeSocketHostName(event.host, null, bundle);
      const serverScope = describeCodeServerPeerRole(event.codeServerName, bundle);
      workflowSummary = "Code-server indication thread health.";
      currentStep = "Thread-alive heartbeat observed";
      knownState = "Indication-processing thread alive";
      meaning.push(
        "Selected event:",
        "Type: indication-processing-thread alive",
        "Direction: Received",
        `Host: ${event.host}`,
        `Host name: ${hostName}`,
        `Server: ${event.codeServerName}`,
        ...(serverScope !== "no grounded code-server role" ? [`Server assignment: ${serverScope}`] : []),
        `Status: indication-processing thread on code server ${event.codeServerName} is alive.`,
      );
      break;
    }
    case "trace-metadata": {
      const component = describeTraceMetadataComponent(event.fileName);
      const action = describeTraceMetadataAction(event.methodName);
      const typeLabel = describeTraceMetadataType(event.fileName, event.methodName);
      workflowSummary = typeLabel;
      currentStep = action;
      knownState = `${component}: ${action}`;
      meaning.push(
        "Selected event:",
        `Type: ${typeLabel}`,
        `Component: ${component}`,
        `Action: ${action}`,
        `Version: ${event.version}`,
        ...(event.lineNumberInfo ? [`Trace point: line ${event.lineNumberInfo}`] : []),
      );
      break;
    }
    case "cad-train-message": {
      const payloadDescriptors = buildCadTrainPayloadDescriptors(event.payloadFields, eventTrainRows);
      const payloadValueAt = (index: number): string | null => {
        const value = payloadDescriptors[index - 1]?.value?.trim();
        return value && value !== "(blank)" ? value : null;
      };
      workflowSummary = "CAD train message.";
      currentStep = event.sourceKind === "train-event" ? "Train event logged" : event.sourceKind === "control-server-message" ? "Control-server train message observed" : "Train stream message received";
      knownState = `${event.eventToken} for ${event.trainSymbol}`;
      payloadContext.push(...buildCadTrainPayloadLines(event.payloadFields, eventTrainRows));
      meaning.push(
        "Selected event:",
        "Type: CAD train message",
        `Source: ${event.sourceKind === "train-event" ? "train event" : event.sourceKind === "control-server-message" ? "control server message" : "stream receiver message"}`,
        ...(event.codeServerName ? [`Control server: ${event.codeServerName}`] : []),
        `Train event: ${event.eventToken}`,
        `Train UID: ${event.trainUid}`,
        `Train symbol: ${event.trainSymbol}`,
        ...(event.direction ? [`Direction: ${event.direction}`] : []),
        ...(event.leadEquipment ? [`Lead equipment: ${event.leadEquipment}`] : []),
        ...(payloadValueAt(7) ? [`Engineer: ${payloadValueAt(7)}`] : []),
        ...(payloadValueAt(8) ? [`Engineer on duty: ${payloadValueAt(8)}`] : []),
        ...(payloadValueAt(12) ? [`Engineer time up: ${payloadValueAt(12)}`] : []),
        ...(payloadValueAt(9) ? [`Conductor: ${payloadValueAt(9)}`] : []),
        ...(payloadValueAt(10) ? [`Conductor on duty: ${payloadValueAt(10)}`] : []),
        ...(payloadValueAt(13) ? [`Conductor time up: ${payloadValueAt(13)}`] : []),
        ...(event.serviceSlot ? [`Service slot: ${event.serviceSlot}`] : []),
        ...(payloadValueAt(17) ? [`Train length: ${payloadValueAt(17)}`] : []),
        ...(payloadValueAt(18) ? [`Loaded cars: ${payloadValueAt(18)}`] : []),
        ...(payloadValueAt(19) ? [`Empty cars: ${payloadValueAt(19)}`] : []),
        ...(payloadValueAt(20) ? [`Operating tons: ${payloadValueAt(20)}`] : []),
        ...(event.trainType ? [`Train type: ${event.trainType}`] : []),
        ...(event.homeRoadCode ? [`Home road: ${event.homeRoadCode}`] : []),
        ...(event.trainGuid ? [`Train GUID: ${event.trainGuid}`] : []),
        `Subdivision: ${describeTrainSubdivisionToken(event.subdivisionToken, eventTrainRows)}`,
        `Payload fields captured: ${event.payloadFieldCount}`,
      );
      break;
    }
    case "cad-control-point-message": {
      const routePair = describeCadRoutePairByComponents(event.relatedUidA, event.relatedUidB, bundle);
      workflowSummary = "CAD control-point message.";
      currentStep = event.sourceKind === "control-server-message" ? "Control-server message observed" : "Control-point stream message received";
      knownState = `Operation token ${event.operationToken}`;
      meaning.push(
        "Selected event:",
        "Type: CAD control-point message",
        `Source: ${event.sourceKind === "control-server-message" ? "control server message" : "stream receiver message"}`,
        ...(
          event.codeServerName
            ? [`Control server: ${event.codeServerName}`]
            : []
        ),
        `Control point: ${stationRow ? `${stationRow.control_point_number} (${stationRow.control_point_name})` : event.controlPointNumber}`,
        ...(stationRow ? [`Subdivision: ${stationRow.subdivision_name}`] : []),
        `Operation token: ${event.operationToken}`,
        `Correlation ID: ${event.correlationId}`,
        `Related object A: ${describeComponentReference(event.relatedUidA, bundle)}`,
        `Related object B: ${describeComponentReference(event.relatedUidB, bundle)}`,
        ...(routePair
          ? [
              `Current route sequence: ${routePair.split("; ")[0]}`,
              ...(routePair.includes("; ") ? [`Switch record for that route: ${routePair.split("; ").slice(1).join("; ")}`] : []),
            ]
          : [`Related objects meaning: ${describeRelatedObjectsMeaning([event.relatedUidA, event.relatedUidB], bundle)}`]),
        `State value: ${event.stateValue}`,
        `Transaction ID: ${describeCadTransaction(event.transactionId)}`,
      );
      break;
    }
    case "cad-signal-message": {
      const signalStationRow = findStationRowByComponentUid(event.signalUid, bundle);
      const signalRouteRows = collectCadSignalRouteRows(event.signalUid, bundle);
      const signalRouteContext = formatRouteGroup(signalRouteRows, bundle);
      const nearbyControlPointEvent = findNearbyCadControlPointMessageForSignal(index, lines, events, event.signalUid);
      const nearbyRouteContext =
        nearbyControlPointEvent && nearbyControlPointEvent.family === "cad-control-point-message"
          ? describeCadRoutePairByComponents(nearbyControlPointEvent.relatedUidA, nearbyControlPointEvent.relatedUidB, bundle)
          : "";
      const outgoingRoutes = signalRouteRows.filter((row) => normalizeLookupKey(row.entry_signal_uid) === normalizeLookupKey(event.signalUid));
      const incomingRoutes = signalRouteRows.filter((row) => normalizeLookupKey(row.exit_signal_uid) === normalizeLookupKey(event.signalUid));
      const outgoingFormatted = formatRouteGroup(outgoingRoutes, bundle).filter((item) => item !== nearbyRouteContext);
      const incomingFormatted = formatRouteGroup(incomingRoutes, bundle).filter((item) => item !== nearbyRouteContext);
      workflowSummary = "CAD signal message.";
      currentStep = event.sourceKind === "control-server-message" ? "Control-server signal message observed" : "Signal stream message received";
      knownState = `Signal ${event.signalUid} ${event.operationToken}`;
      meaning.push(
        "Selected event:",
        "Type: CAD signal message",
        `Source: ${event.sourceKind === "control-server-message" ? "control server message" : "stream receiver message"}`,
        ...(event.codeServerName ? [`Control server: ${event.codeServerName}`] : []),
        `Signal: ${describeComponentReference(event.signalUid, bundle)}`,
        ...(signalStationRow ? [`Control point: ${signalStationRow.control_point_number} (${signalStationRow.control_point_name})`] : []),
        ...(signalStationRow ? [`Subdivision: ${signalStationRow.subdivision_name}`] : []),
        `Operation token: ${event.operationToken}`,
        ...(nearbyRouteContext ? [`Current route sequence: ${nearbyRouteContext}`] : []),
        ...(!nearbyRouteContext && signalRouteContext.length ? [`Configured routes tied to this signal: ${signalRouteContext.length}`] : []),
        ...(outgoingFormatted.length || incomingFormatted.length ? ["Other route-table options at this signal:"] : []),
        ...outgoingFormatted.map((item, routeIndex) => `${routeIndex + 1}. ${item}`),
        ...incomingFormatted.map((item, routeIndex) => `${outgoingFormatted.length + routeIndex + 1}. ${item}`),
        `Transaction ID: ${describeCadTransaction(event.transactionId)}`,
      );
      break;
    }
    case "process-vital-signs": {
      workflowSummary = "Process vital signs.";
      currentStep = "Runtime health snapshot logged";
      knownState = "Process vitals captured";
      meaning.push(
        "Selected event:",
        "Type: process vital signs",
        ...(event.workstation ? [`Workstation: ${event.workstation}`] : []),
        ...(event.startTime ? [`Start time: ${event.startTime}`] : []),
        ...(event.softwareVersion ? [`Software version: ${event.softwareVersion}`] : []),
        ...(event.workingMemory ? [`Working memory: ${formatMemoryBytes(event.workingMemory)}`] : []),
        ...(event.threadCount ? [`Thread count: ${event.threadCount}`] : []),
        ...(event.handleCount ? [`Handle count: ${event.handleCount}`] : []),
        ...(event.workingPeakMemory ? [`Working peak memory: ${formatMemoryBytes(event.workingPeakMemory)}`] : []),
        ...(event.pagedMemory ? [`Paged memory: ${formatMemoryBytes(event.pagedMemory)}`] : []),
        ...(event.pagedPeakMemory ? [`Paged peak memory: ${formatMemoryBytes(event.pagedPeakMemory)}`] : []),
        ...(event.privilegedProcessorTime ? [`Privileged processor time: ${event.privilegedProcessorTime}`] : []),
        ...(event.totalProcessorTime ? [`Total processor time: ${event.totalProcessorTime}`] : []),
      );
      break;
    }
    case "process-vital-signs-header": {
      workflowSummary = `${event.component} vital signs header.`;
      currentStep = "Vital-signs header logged";
      knownState = event.softwareVersion;
      meaning.push(
        "Selected event:",
        "Type: vital-signs header",
        `Component: ${event.component}`,
        `Software version: ${event.softwareVersion}`,
        "Status: this header starts a multi-line runtime health snapshot block for the code server.",
      );
      break;
    }
    case "thread-capacity": {
      workflowSummary = `${event.capacityKind} thread-capacity metadata.`;
      currentStep = `${event.capacityKind} thread limits logged`;
      knownState = `${event.capacityKind} thread-pool capacity captured`;
      meaning.push(
        "Selected event:",
        "Type: thread-pool capacity",
        `Capacity kind: ${event.capacityKind}`,
        `${event.capacityKind} worker threads: ${event.maxWorkerThreads}`,
        `${event.capacityKind} I/O completion threads: ${event.maxIoCompletionThreads}`,
      );
      break;
    }
    case "named-guid-catalog-entry": {
      workflowSummary = "Named GUID catalog entry.";
      currentStep = "Catalog entry logged";
      knownState = event.label;
      meaning.push(
        "Selected event:",
        "Type: named GUID catalog entry",
        `Label: ${event.label}`,
        `GUID: ${event.guid}`,
        "Status: this line maps a free-text catalog label to a GUID value in the surrounding territory/notes listing.",
      );
      break;
    }
    case "sql-train-update-continuation": {
      workflowSummary = "SQL train-update continuation.";
      currentStep = "Wrapped SQL update continued";
      knownState = event.trainSymbol ?? "train SQL fragment";
      meaning.push(
        "Selected event:",
        "Type: SQL train update continuation",
        ...(event.trainSymbol ? [`Train symbol: ${event.trainSymbol}`] : []),
        "Status: this line is the continuation of a wrapped `UPDATE tblTrainsActive` statement whose earlier line broke inside a quoted value.",
      );
      payloadContext.push("SQL continuation:", event.payload);
      break;
    }
    case "compact-track-state": {
      workflowSummary = "Compact track-state snapshot.";
      currentStep = "Track-state summary logged";
      knownState = `${event.occupied}/${event.blocking}`;
      meaning.push(
        "Selected event:",
        "Type: compact track-state summary",
        `Occupied: ${event.occupied}`,
        `Traffic: ${event.traffic || "(blank)"}`,
        `Blocking: ${event.blocking}`,
        `Track status: ${event.trackStatus}`,
      );
      break;
    }
    case "gbo-ptc-transmission-status": {
      workflowSummary = "GBO PTC transmission status.";
      currentStep = "PTC electronic-transmission status logged";
      knownState = `${event.trainSymbol} ${event.inProgress}`;
      meaning.push(
        "Selected event:",
        "Type: GBO PTC transmission status",
        `In progress: ${event.inProgress}`,
        `GUID: ${event.guid}`,
        `Train symbol: ${event.trainSymbol}`,
      );
      break;
    }
    case "code-server-online-status": {
      workflowSummary = "Code-server online status.";
      currentStep = "Online-status transition logged";
      knownState = event.onlineStatus;
      meaning.push(
        "Selected event:",
        "Type: code-server online status",
        `OnLineStatus: ${event.onlineStatus}`,
        `ThisCodeServer.OnlineStatus: ${event.localStatus}`,
        `CodeServer.gOnlineStatus: ${event.globalStatus}`,
      );
      break;
    }
    case "bos-server-list-entry": {
      workflowSummary = "BOS server-list entry.";
      currentStep = `${event.availability} BOS server listed`;
      knownState = `${event.serverName} ${event.availability}`;
      meaning.push(
        "Selected event:",
        "Type: BOS server-list entry",
        `Availability: ${event.availability}`,
        `Server: ${event.serverName} (${event.serverId})`,
        `Server assignment ID: ${event.serverAssignmentId}`,
        `Last heartbeat: ${event.lastHeartbeat}`,
      );
      break;
    }
    case "connection-endpoint-status": {
      workflowSummary = `${event.scope} connection endpoint.`;
      currentStep = `${event.scope} endpoint logged`;
      knownState = `${event.host}:${event.port}`;
      meaning.push(
        "Selected event:",
        `Type: ${event.scope.toLowerCase()} connection endpoint`,
        `Host: ${event.host}`,
        `Port: ${event.port}`,
      );
      break;
    }
    case "workstation-transaction-marker": {
      workflowSummary = "Workstation transaction marker.";
      currentStep = event.transactionId ? "Workstation transaction logged" : "Workstation marker logged";
      knownState = event.transactionId ?? event.workstation;
      meaning.push(
        "Selected event:",
        "Type: workstation transaction marker",
        `Workstation: ${event.workstation}`,
        ...(event.transactionId ? [`Transaction ID: ${describeCadTransaction(event.transactionId)}`] : []),
        "Status: this line is the workstation trailer emitted alongside a CAD train/control event.",
      );
      break;
    }
    case "indication-change-trigger": {
      workflowSummary = "Indication change trigger.";
      currentStep = "Indication-change path entered";
      knownState = event.caller;
      meaning.push(
        "Selected event:",
        "Type: indication-change trigger",
        `Caller: ${event.caller}`,
        "Status: the ApplyIndicationChangeToTrack workflow logged which caller triggered this indication-processing pass.",
      );
      break;
    }
    case "locomotive-processing-marker": {
      workflowSummary = "Locomotive processing marker.";
      currentStep = event.stage === "SummaryUpdateCompleted" ? "Locomotive summary update completed" : "Position-report check finished";
      knownState = event.stage;
      meaning.push(
        "Selected event:",
        "Type: locomotive processing marker",
        `Stage: ${event.stage === "SummaryUpdateCompleted" ? "Locomotive summary update completed" : "Locomotive check-position-report finished"}`,
        "Status: this line marks completion of the workstation VETMS locomotive-position handling cycle.",
      );
      break;
    }
    case "track-tracing-marker": {
      workflowSummary = `${event.subject} trace marker.`;
      currentStep = event.phase === "START" ? "Track-tracing block started" : "Track-tracing block ended";
      knownState = `${event.phase} ${event.subject}`;
      meaning.push(
        "Selected event:",
        "Type: track-tracing marker",
        `Phase: ${event.phase}`,
        `Subject: ${event.subject}`,
        "Status: this marker brackets the detailed TrackTrainIDOn diagnostic block around a train-ID move event.",
      );
      break;
    }
    case "system-thread-heartbeat": {
      workflowSummary = "System thread heartbeat.";
      currentStep = "Heartbeat logged";
      knownState = event.threadName;
      meaning.push(
        "Selected event:",
        "Type: system thread heartbeat",
        `Thread: ${event.threadName}`,
        "Status: the workstation periodic flasher/heartbeat thread reported itself alive.",
      );
      break;
    }
    case "control-queue-event": {
      workflowSummary = "Control queue event.";
      currentStep = "Queued control payload compared";
      knownState = event.eventName;
      meaning.push(
        "Selected event:",
        "Type: control queue event",
        `Event: ${event.eventName}`,
        "Status: the code server logged that a new control command matched or was compared against an existing queued control image.",
      );
      break;
    }
    case "repeated-binary-state": {
      workflowSummary = `Repeated ${event.stateKind.toLowerCase()} payload.`;
      currentStep = "Duplicate payload row logged";
      knownState = `${event.payloadBits.length} bits`;
      meaning.push(
        "Selected event:",
        `Type: repeated ${event.stateKind.toLowerCase()} payload`,
        `Payload width: ${event.payloadBits.length} bits`,
        `Qualifier: SAME ${event.stateKind}`,
        "Status: this line records that the immediately compared payload matched the prior payload image.",
      );
      payloadContext.push(`Payload bits: ${event.payloadBits}`);
      break;
    }
    case "network-stack-frame": {
      workflowSummary = "Socket stack frame.";
      currentStep = "Network constructor frame logged";
      knownState = event.method;
      meaning.push(
        "Selected event:",
        "Type: network stack frame",
        `Method: ${event.method}`,
        `Signature: (${event.signature})`,
        "Status: this is a .NET stack-trace frame from the network exception block, not a TMDS device message header.",
      );
      break;
    }
    case "application-stack-frame": {
      workflowSummary = "Application stack frame.";
      currentStep = "Application exception frame logged";
      knownState = event.method;
      meaning.push(
        "Selected event:",
        "Type: application stack frame",
        `Method: ${event.method}`,
        ...(event.signature ? [`Signature: (${event.signature})`] : []),
        "Status: this is a .NET stack-trace frame from the application exception block, not a TMDS device message header.",
      );
      break;
    }
    case "direction-state-entry": {
      workflowSummary = "Direction state entry.";
      currentStep = "Directional state row logged";
      knownState = `${event.direction} ${event.state}`;
      meaning.push(
        "Selected event:",
        "Type: direction state entry",
        `Direction: ${event.direction}`,
        `Code: ${event.code}`,
        `State: ${event.state}`,
        "Status: this line is a compact direction/code/state row emitted inside the surrounding territory listing block.",
      );
      break;
    }
    case "admin-click-action": {
      workflowSummary = "Admin UI click event.";
      currentStep = "Admin click logged";
      knownState = event.action;
      meaning.push(
        "Selected event:",
        "Type: admin click action",
        `Action: ${event.action}`,
        ...(event.actionTime ? [`Action time: ${event.actionTime}`] : []),
        "Status: this line records a clicked action from the BOS administration client UI.",
      );
      break;
    }
    case "user-interface-marker": {
      workflowSummary = "User-interface marker.";
      currentStep = "UI workflow marker logged";
      knownState = event.markerText;
      meaning.push(
        "Selected event:",
        "Type: user-interface marker",
        `Marker: ${event.markerText}`,
        "Status: this line records a workstation UI workflow marker rather than a device telegram or protocol header.",
      );
      break;
    }
    case "control-send-phase-marker": {
      workflowSummary = "Control send phase marker.";
      currentStep = `${event.routine} ${event.phase.toLowerCase()} logged`;
      knownState = `${event.routine} ${event.phase}`;
      meaning.push(
        "Selected event:",
        "Type: control send phase marker",
        `Routine: ${event.routine}`,
        `Phase: ${event.phase}`,
        "Status: this line marks the start or end of a send/cancel control routine in the workstation trace.",
      );
      break;
    }
    case "indication-bit-inversion": {
      workflowSummary = "Indication bit inversion.";
      currentStep = `Indication bit ${event.bitIndex} inverted`;
      knownState = `${event.fromValue} to ${event.toValue}`;
      meaning.push(
        "Selected event:",
        "Type: indication bit inversion",
        `Bit index: ${event.bitIndex}`,
        `From: ${event.fromValue}`,
        `To: ${event.toValue}`,
        "Status: this diagnostic line records a bit-level indication inversion performed by the workstation trace logic.",
      );
      break;
    }
    case "short-workflow-marker": {
      workflowSummary = "Short workflow marker.";
      currentStep = "Compact workflow marker logged";
      knownState = `${event.prefix}${event.marker}`;
      meaning.push(
        "Selected event:",
        "Type: short workflow marker",
        `Prefix: ${event.prefix}`,
        `Marker: ${event.marker}`,
        "Status: this line is a terse workflow marker preserved as logged because the source logs do not name the step more fully on the same line.",
      );
      break;
    }
    case "route-selection-step": {
      workflowSummary = "Route-selection step.";
      currentStep = `${event.processName} step ${event.step}`;
      knownState = event.step;
      meaning.push(
        "Selected event:",
        "Type: route-selection step",
        `Process: ${event.processName}`,
        `Step: ${event.step}`,
        "Status: this line traces the workstation enter/exit route-selection workflow step sequence.",
      );
      break;
    }
    case "stored-route-event-marker": {
      workflowSummary = "Stored-route event marker.";
      currentStep = `${event.eventGroup} ${event.step}`;
      knownState = event.step;
      meaning.push(
        "Selected event:",
        "Type: stored-route event marker",
        `Event group: ${event.eventGroup}`,
        `Step: ${event.step}`,
        "Status: this line marks a numbered stage in the stored-route workflow block.",
      );
      break;
    }
    case "stored-route-recursion-check": {
      workflowSummary = "Stored-route recursion check.";
      currentStep = event.phase === "BEGIN" ? "Recursion check started" : "Recursion check finished";
      knownState = event.phase;
      meaning.push(
        "Selected event:",
        "Type: stored-route recursion check",
        `Phase: ${event.phase}`,
        "Status: this line brackets the recursive stored-route expansion check used during fast-route processing.",
      );
      break;
    }
    case "system-reset-marker": {
      workflowSummary = "System reset marker.";
      currentStep = "System variables cleared";
      knownState = event.markerText;
      meaning.push(
        "Selected event:",
        "Type: system reset marker",
        `Marker: ${event.markerText}`,
        "Status: this line records that the workstation cleared route-selection or transient system variables after the current action.",
      );
      break;
    }
    case "stored-route-status-marker": {
      workflowSummary = "Stored-route status marker.";
      currentStep = "Stored-route status logged";
      knownState = event.markerText;
      meaning.push(
        "Selected event:",
        "Type: stored-route status marker",
        `Marker: ${event.markerText}`,
        "Status: this line records a high-level stored-route execution/display status transition.",
      );
      break;
    }
    case "code-station-load-count": {
      workflowSummary = "Code-station load count.";
      currentStep = "Database load count logged";
      knownState = `${event.count} code stations`;
      meaning.push(
        "Selected event:",
        "Type: code-station load count",
        `Loaded code stations: ${event.count}`,
        "Status: this line records how many code stations were loaded from the database during code-line initialization or reload.",
      );
      break;
    }
    case "exception-trace-separator": {
      workflowSummary = "Exception trace separator.";
      currentStep = "Inner exception trace ended";
      knownState = event.markerText;
      meaning.push(
        "Selected event:",
        "Type: exception trace separator",
        `Marker: ${event.markerText}`,
        "Status: this line is the .NET stack-trace separator between the inner and outer exception blocks.",
      );
      break;
    }
    case "blank-log-entry": {
      workflowSummary = "Blank log separator.";
      currentStep = "Blank separator line";
      knownState = "blank";
      meaning.push(
        "Selected event:",
        "Type: blank log entry",
        "Status: this line is empty after the timestamp/prefix and functions only as a visual separator in the source log.",
      );
      break;
    }
    case "signal-state-change": {
      workflowSummary = "Signal state change.";
      currentStep = "Signal state transition logged";
      knownState = event.stateToken;
      meaning.push(
        "Selected event:",
        "Type: signal state change",
        `State: ${event.stateToken}`,
      );
      break;
    }
    case "signal-indication-update": {
      const signalStationRow = findStationRowByComponentUid(event.signalUid, bundle) ?? findStationRowByNameToken(bundle, event.controlPointToken);
      workflowSummary = "Signal indication update.";
      currentStep = "Signal indication received";
      knownState = event.statusTokens.join(" / ") || "Signal update captured";
      meaning.push(
        "Selected event:",
        "Type: signal indication update",
        `Control point: ${signalStationRow ? `${signalStationRow.control_point_number} (${signalStationRow.control_point_name})` : event.controlPointToken}`,
        `Signal: ${describeComponentReference(event.signalUid, bundle)}`,
        `Signal name token: ${event.signalName}`,
        `Update states: ${event.statusTokens.join(", ") || "none"}`,
      );
      break;
    }
    case "flash-name": {
      const flashStationRow = findStationRowByNameToken(bundle, event.flashName);
      workflowSummary = "Indication flash target.";
      currentStep = event.sourceKind === "event" ? "Flash event emitted" : "Flash target logged";
      knownState = event.flashName;
      meaning.push(
        "Selected event:",
        `Type: ${event.sourceKind === "event" ? "indication flash event" : "indication flash target"}`,
        `Location: ${event.flashName}`,
        ...(flashStationRow ? [`Control point: ${flashStationRow.control_point_number} (${flashStationRow.control_point_name})`] : []),
        ...(flashStationRow ? [`Subdivision: ${flashStationRow.subdivision_name}`] : []),
        `Status: ${event.sourceKind === "event" ? "flash/highlight event emitted for this indication location." : "this location was marked as the flash/highlight target for the current indication update."}`,
      );
      break;
    }
    case "indication-message-complete": {
      const nearbyFlash = findNearbyFlashName(index, lines, events);
      const nearbyFlashName = nearbyFlash && "flashName" in nearbyFlash ? nearbyFlash.flashName : "";
      const completeStationRow = findStationRowByNameToken(bundle, nearbyFlashName);
      workflowSummary = "Indication processing complete.";
      currentStep = "Indication cycle complete";
      knownState = `Complete on ${event.codeServerName}`;
      meaning.push(
        "Selected event:",
        "Type: indication message complete",
        `Code server: ${event.codeServerName}`,
        ...(nearbyFlashName ? [`Location: ${nearbyFlashName}`] : []),
        ...(completeStationRow ? [`Control point: ${completeStationRow.control_point_number} (${completeStationRow.control_point_name})`] : []),
        ...(completeStationRow ? [`Subdivision: ${completeStationRow.subdivision_name}`] : []),
        `Status: indication-processing cycle completed on ${event.codeServerName}${nearbyFlashName ? ` for ${nearbyFlashName}` : ""}.`,
      );
      break;
    }
    case "train-schedule-timer-check": {
      const scheduleStationRow = findStationRowByNameToken(bundle, event.locationToken);
      const scheduleDelta = describeScheduleCheckDelta(event.timerCheckTime, event.scheduleTime);
      workflowSummary = "Train schedule timer check.";
      currentStep = "Schedule timer checked";
      knownState = `Schedule timer checked for ${event.trainSymbol}`;
      meaning.push(
        "Selected event:",
        "Type: train schedule timer check",
        `Train symbol: ${event.trainSymbol}`,
        `Location: ${event.locationToken}`,
        ...(scheduleStationRow ? [`Control point: ${scheduleStationRow.control_point_number} (${scheduleStationRow.control_point_name})`] : []),
        ...(scheduleStationRow ? [`Subdivision: ${scheduleStationRow.subdivision_name}`] : []),
        `Schedule UID: ${event.scheduleUid}`,
        `Timer check time: ${event.timerCheckTime}`,
        `Scheduled time: ${event.scheduleTime}`,
        ...(scheduleDelta ? [scheduleDelta] : []),
        "Status: CAD checked this train's scheduled stop/timepoint at the listed location.",
      );
      break;
    }
    case "host-connection-refused": {
      const hostName = describeSocketHostName(event.host, null, bundle);
      workflowSummary = "Outbound connection rejected.";
      currentStep = "Socket open failed";
      knownState = "Connection refused";
      meaning.push(
        "Selected event:",
        "Type: host refused connection",
        `Host: ${event.host}`,
        `Host name: ${hostName}`,
        `Port: ${event.port}`,
        `Status: the remote endpoint refused the connection request from this workstation/client.`,
      );
      break;
    }
    case "workstation-request-line": {
      workflowSummary = "Workstation request received.";
      currentStep = "Delimited client request received";
      knownState = event.requestType;
      meaning.push(
        "Selected event:",
        "Type: workstation request line",
        `Listener port: ${event.listenerPort}`,
        `Protocol: ${event.protocolToken}`,
        `Request: ${event.requestType}`,
        `Subject: ${event.subject}`,
        ...(event.traceComponent ? [`Trace component: ${event.traceComponent}`] : []),
        ...(event.traceLevel ? [`Trace level: ${event.traceLevel}`] : []),
      );
      break;
    }
    case "cad-forwarded-vetms": {
      workflowSummary = `Forwarded ${event.messageType} workstation update`;
      currentStep = "CAD client datagram forwarded";
      knownState = event.stateToken ?? event.messageType;
      meaning.push(
        "Selected event:",
        "Type: forwarded CAD workstation VETMS update",
        `Listener port: ${event.listenerPort}`,
        `Workstation: ${event.workstation}`,
        `Message family: ${event.messageFamily}`,
        `Message type: ${event.messageType}`,
        ...(event.stateToken ? [`State token: ${event.stateToken}`] : []),
        `Payload fields: ${event.payloadFields.length}`,
        ...(event.traceComponent ? [`Trace component: ${event.traceComponent}`] : []),
        ...(event.traceLevel ? [`Trace level: ${event.traceLevel}`] : []),
      );
      payloadContext.push(
        "Forwarded payload fields:",
        ...event.payloadFields.map((field, fieldIndex) => `${fieldIndex + 1}. ${field || "(blank)"}`),
      );
      break;
    }
    case "plain-vetms-message": {
      workflowSummary = `${event.messageType} VETMS payload.`;
      currentStep = event.messageDirection === "SEND" ? "Delimited VETMS message sent" : "Delimited VETMS message received";
      knownState = [event.messageType, event.stateChange].filter(Boolean).join(" / ") || event.messageType;
      meaning.push(
        "Selected event:",
        "Type: plain VETMS message",
        `Direction: ${event.messageDirection === "SEND" ? "sent" : "received"}`,
        `Category: ${event.messageCategory}`,
        `Message type: ${event.messageType}`,
        ...(event.stateChange ? [`State change: ${event.stateChange}`] : []),
        ...(event.trainSymbol ? [`Train symbol: ${event.trainSymbol}`] : []),
        ...(event.locoUid ? [`Locomotive: ${event.locoUid}`] : []),
        ...(event.reportTime ? [`Report time: ${event.reportTime}`] : []),
        ...(event.directionOfTravel ? [`Direction of travel: ${event.directionOfTravel}`] : []),
        `Payload fields: ${event.payloadFields.length}`,
        "Status: this line is a delimited VETMS payload captured directly in the workstation event log.",
      );
      payloadContext.push(
        "Delimited VETMS payload fields:",
        ...event.payloadFields.map((field, fieldIndex) => `${fieldIndex + 1}. ${field || "(blank)"}`),
      );
      break;
    }
    case "territory-train-list": {
      workflowSummary = "Territory train list request.";
      currentStep = "Territory train list logged";
      knownState = event.territoryToken;
      meaning.push(
        "Selected event:",
        "Type: territory train list",
        `Territory token: ${event.territoryToken}`,
        "Status: this control-server line identifies the territory whose train list was being processed or requested.",
      );
      break;
    }
    case "workstation-originated-train-log": {
      workflowSummary = "Workstation-originated train log.";
      currentStep = "Workstation-originated train event logged";
      knownState = event.eventToken ?? "train log";
      meaning.push(
        "Selected event:",
        "Type: workstation-originated train event",
        ...(event.subdivisionToken ? [`Subdivision token: ${event.subdivisionToken}`] : []),
        ...(event.eventToken ? [`Event token: ${event.eventToken}`] : []),
        ...(event.trainUid ? [`Train UID: ${event.trainUid}`] : []),
        ...(event.trainSymbol ? [`Train symbol: ${event.trainSymbol}`] : []),
        `Payload fields captured: ${event.payloadFields.length}`,
        "Status: this workstation generated the train event locally and logged the pipe-delimited payload instead of forwarding the original message.",
      );
      payloadContext.push(
        "Workstation-originated payload fields:",
        ...event.payloadFields.map((field, fieldIndex) => `${fieldIndex + 1}. ${field || "(blank)"}`),
      );
      break;
    }
    case "train-tracking-message": {
      const trackReference = event.trackUid ? describeComponentReference(event.trackUid, bundle) : "";
      const relatedTrackReference = event.relatedTrackUid ? describeComponentReference(event.relatedTrackUid, bundle) : "";
      workflowSummary = `${event.prefix} diagnostic.`;
      currentStep = event.action;
      knownState = [
        event.trainSymbol,
        event.trackUid,
        event.directionToken,
      ].filter(Boolean).join(" / ") || event.action;
      meaning.push(
        "Selected event:",
        "Type: train-tracking diagnostic",
        `Prefix: ${event.prefix}`,
        `Action: ${event.action}`,
        ...(event.trainSymbol ? [`Train symbol: ${event.trainSymbol}`] : []),
        ...(event.trackUid ? [`Track: ${trackReference || event.trackUid}`] : []),
        ...(event.relatedTrackUid ? [`Related track: ${relatedTrackReference || event.relatedTrackUid}`] : []),
        ...(event.trackName ? [`Track name token: ${event.trackName}`] : []),
        ...(event.directionToken ? [`Direction token: ${event.directionToken}`] : []),
        ...(event.indexValue ? [`Index value: ${event.indexValue}`] : []),
        "Status: this is a train-tracking workflow trace line preserved with its explicit train/track tokens.",
      );
      payloadContext.push("Raw tracking payload:", event.rawPayload);
      break;
    }
    case "plain-control-sent": {
      const controlState = formatActiveControlLines(event.payloadBits, assignmentRow);
      workflowSummary = "Plain control-sent payload.";
      currentStep = "Control payload logged";
      knownState = controlState.assertedPositions.length ? "One or more control bits asserted" : "All logged control bits clear";
      meaning.push(
        "Selected event:",
        "Type: plain control sent",
        `Station: ${stationRow?.station_name ?? event.stationToken}`,
        ...(stationRow ? [`Control point: ${stationRow.control_point_number} (${stationRow.control_point_name})`] : []),
        `Channel token: ${event.channelToken}`,
        `Declared width token: ${event.declaredWidth}`,
        `Payload width: ${event.payloadBits.length} bits`,
        ...controlState.meaning,
      );
      payloadContext.push(`Payload bits: ${event.payloadBits}`, ...controlState.structured);
      break;
    }
    case "mpar-event": {
      workflowSummary = "MPAR event.";
      currentStep = event.eventName;
      knownState = event.eventName;
      meaning.push(
        "Selected event:",
        "Type: MPAR event",
        `Event: ${event.eventName}`,
        ...(event.payload ? [`Payload: ${event.payload}`] : []),
        "Status: this line records MPAR auto-routing or plan-loading activity from the control server.",
      );
      if (event.payload) {
        payloadContext.push(event.eventName.includes("Query") ? "SQL text:" : "Raw payload:", event.payload);
      }
      break;
    }
    case "route-search-message": {
      const componentReference = event.componentUid ? describeComponentReference(event.componentUid, bundle) : "";
      workflowSummary = `${event.searchKind} route-search trace.`;
      currentStep = event.action === "marker" ? "Route search marker logged" : "Route search component scanned";
      knownState = event.componentUid ?? event.marker ?? event.searchKind;
      meaning.push(
        "Selected event:",
        "Type: route-search diagnostic",
        `Search kind: ${event.searchKind}`,
        `Action: ${event.action}`,
        ...(event.marker ? [`Marker: ${event.marker}`] : []),
        ...(event.componentClass ? [`Component class: ${event.componentClass}`] : []),
        ...(event.componentUid ? [`Component: ${componentReference || event.componentUid}`] : []),
        "Status: this line traces the ordered components considered during a route-search walk.",
      );
      payloadContext.push("Raw route-search payload:", event.rawPayload);
      break;
    }
    case "component-reference-list-entry": {
      const componentReference = event.componentUid ? describeComponentReference(event.componentUid, bundle) : "";
      workflowSummary = `${event.entryKind} component listing.`;
      currentStep = "Component listing logged";
      knownState = event.componentUid ?? event.componentName ?? event.entryKind;
      meaning.push(
        "Selected event:",
        "Type: component reference entry",
        `Entry kind: ${event.entryKind}`,
        ...(event.componentUid ? [`Component UID: ${event.componentUid}`] : []),
        ...(event.componentName ? [`Component name: ${event.componentName}`] : []),
        ...(event.componentClass ? [`Component class: ${event.componentClass}`] : []),
        ...(componentReference ? [`Grounded component: ${componentReference}`] : []),
        "Status: this line is part of a component listing emitted during route-search or train-tracking diagnostics.",
      );
      payloadContext.push("Raw component payload:", event.rawPayload);
      break;
    }
    case "genisys-control-resend": {
      workflowSummary = "Genisys control resend.";
      currentStep = "Retrying control delivery";
      knownState = `Retry ${event.retryCurrent}/${event.retryTotal}`;
      meaning.push(
        "Selected event:",
        "Type: Genisys control resend",
        `Station: ${stationRow?.station_name ?? event.stationToken}`,
        `Control point: ${stationRow ? `${stationRow.control_point_number} (${stationRow.control_point_name})` : event.controlPointNumber}`,
        ...(stationRow ? [`Subdivision: ${stationRow.subdivision_name}`] : []),
        ...(codeLineRow ? [`Code line: ${codeLineRow.code_line_number} (${codeLineRow.code_line_name})`] : []),
        `Retry attempt: ${event.retryCurrent} of ${event.retryTotal}`,
        `Reason: no control-delivery confirmation within ${event.timeoutMs} ms`,
        "Status: the office retried a previously issued control command for this location.",
      );
      break;
    }
    case "socket-raw-frame": {
      const assembly = assembleSocketRawFrame(index, lines, events);
      const decodedFrame = assembly?.decode ?? decodeGenisysSocketFrame(event.payloadBytes);
      const socketFrameBytes = assembly?.payloadBytes ?? event.payloadBytes;
      const socketFrameLine = assembly?.line ?? line;
      const socketFrameEvent = assembly?.event ?? event;
      const pairedFrame = findPairedSocketRawFrame(index, lines, events);
      const pairedRelation = pairedFrame ? describePairedSocketFrameRelationship(socketFrameEvent, decodedFrame, pairedFrame) : null;
      const serverAddressLabel = describeGenisysServerAddress(decodedFrame.serverAddress, stationRow, bundle);
      const socketByteRoleRows = describeGenisysSocketByteRoles(socketFrameBytes, decodedFrame, serverAddressLabel);
      workflowSummary = describeSocketRawFrameCycle(decodedFrame);
      currentStep = describeSocketRawFramePhase(socketFrameEvent, decodedFrame);
      knownState = pairedRelation?.summary.replace(/\.$/, "") ?? (decodedFrame.headerLabel !== "Unknown"
        ? `${decodedFrame.headerLabel} ${decodedFrame.protocolDirection}`
        : `${socketFrameEvent.socketAction} ${socketFrameEvent.stationToken}`);
      relatedPair = pairedFrame && pairedRelation
        ? {
            lineId: pairedFrame.line.id,
            lineNumber: pairedFrame.line.lineNumber,
            raw: pairedFrame.line.raw,
            relationLabel: pairedRelation.label,
            deltaLabel: pairedFrame.deltaLabel,
            summary: pairedRelation.summary,
            reason: pairedRelation.reason,
          }
        : undefined;
      meaning.push(
        "Selected event:",
        "Type: raw socket frame",
        `Line: ${socketFrameLine.lineNumber}`,
        `Endpoint: ${stationRow?.station_name ?? socketFrameEvent.stationToken}`,
        `Direction: ${socketFrameEvent.socketAction === "XMT" ? "transmitted to field endpoint" : "received from field endpoint"}`,
        `As logged: ${socketFrameEvent.directionGlyph} ${socketFrameEvent.socketAction}`,
        ...(stationRow ? [`Control point: ${stationRow.control_point_number} (${stationRow.control_point_name})`] : []),
        ...(stationRow ? [`Subdivision: ${stationRow.subdivision_name}`] : []),
        ...(codeLineRow ? [`Code line: ${codeLineRow.code_line_number} (${codeLineRow.code_line_name})`] : []),
        `Genisys header: ${decodedFrame.headerLabel}${decodedFrame.headerCode === null ? "" : ` (0x${formatHexByte(decodedFrame.headerCode)})`}`,
        `Protocol role: ${decodedFrame.protocolDirection}`,
        `Server address: ${serverAddressLabel}`,
        ...(assembly && assembly.fragmentCount > 1 ? [`Transport fragments: ${assembly.fragmentCount} lines combined into one Genisys frame`] : []),
        ...(decodedFrame.crcHex ? [`CRC: ${decodedFrame.crcHex}`] : []),
        ...(relatedPair
          ? [
              `Related pair: Line ${relatedPair.lineNumber} ${relatedPair.deltaLabel}`,
              `Relationship: ${relatedPair.summary}`,
              `Why linked: ${relatedPair.reason}`,
            ]
          : []),
        `Payload bytes: ${socketFrameBytes.join(" ") || "none captured"}`,
        `Byte count: ${socketFrameBytes.length}`,
        decodedFrame.headerCode === 0xfb
          ? "Status: Genisys poll request observed."
          : decodedFrame.headerCode === 0xf1
            ? "Status: Genisys acknowledge-client response observed."
            : decodedFrame.headerCode === 0xfd
              ? "Status: Genisys recall-header request observed."
              : decodedFrame.headerCode === 0xfc
                ? "Status: Genisys control-data request observed."
                : decodedFrame.headerCode === 0xf2
                  ? "Status: Genisys indication-data response observed."
                  : "Status: Genisys socket frame decoded from raw transport bytes.",
        ...decodedFrame.issues.map((issue) => `Decode note: ${issue}`),
      );
      if (socketFrameBytes.length) {
        payloadContext.push(
          "Decoded Genisys frame:",
          `Header = ${decodedFrame.headerLabel}${decodedFrame.headerCode === null ? "" : ` (0x${formatHexByte(decodedFrame.headerCode)})`}`,
          `Role = ${decodedFrame.protocolDirection}`,
          `Server address = ${serverAddressLabel}`,
          ...(assembly && assembly.fragmentCount > 1 ? [`Transport fragments = ${assembly.fragmentCount}`] : []),
          ...(decodedFrame.crcHex ? [`CRC = ${decodedFrame.crcHex}`] : []),
          ...(decodedFrame.payloadPairs.length
            ? [
                "Payload address/data pairs:",
                ...decodedFrame.payloadPairs.map(
                  ({ address, data }, pairIndex) =>
                    `${pairIndex + 1}. 0x${formatHexByte(address)} = 0x${formatHexByte(data)} (${data})`,
                ),
              ]
            : ["Payload address/data pairs: none"]),
          "Byte roles:",
          ...socketByteRoleRows,
        );
      }
      break;
    }
    case "loco-log-marker": {
      workflowSummary = "Locomotive log marker.";
      currentStep = "Session marker logged";
      knownState = event.markerText;
      meaning.push(
        "Selected event:",
        "Type: locomotive log marker",
        `Marker: ${event.markerText}`,
      );
      break;
    }
    case "loco-log-entry": {
      workflowSummary = "Locomotive application/runtime log.";
      currentStep = `${event.severity} event logged`;
      knownState = `${event.severity} ${event.component}`;
      meaning.push(
        "Selected event:",
        "Type: locomotive runtime log entry",
        `Severity: ${event.severity}`,
        `Component: ${event.component}`,
        `Message: ${event.message}`,
      );
      break;
    }
    case "office-telemetry-summary": {
      workflowSummary = "Office telemetry summary.";
      currentStep = event.direction === "TX" ? "Office message transmitted" : "Office message received";
      knownState = `${event.direction} ${event.messageId}`;
      meaning.push(
        "Selected event:",
        "Type: office message summary",
        `Direction: ${event.direction}`,
        `Channel: ${event.channel}`,
        `Message ID: ${event.messageId}`,
        `Sequence: ${event.sequence}`,
        `Peer: ${event.peer}`,
      );
      break;
    }
    case "office-telemetry-hex": {
      const byteRows = describeGenericHexByteRows(event.payloadBytes, "office telemetry payload");
      workflowSummary = "Office telemetry payload.";
      currentStep = event.direction === "TX" ? "Office payload transmitted" : "Office payload received";
      knownState = `${event.direction} ${event.channel}`;
      meaning.push(
        "Selected event:",
        "Type: office payload bytes",
        `Direction: ${event.direction}`,
        `Channel: ${event.channel}`,
        `Byte count: ${event.payloadBytes.length}`,
      );
      payloadContext.push(
        "Office telemetry byte details:",
        ...byteRows,
      );
      break;
    }
    case "recorder-delimited-record": {
      workflowSummary = "Delimited recorder payload.";
      currentStep = "Recorder line captured";
      knownState = `${event.recorder} ${event.payloadFields.length} fields`;
      meaning.push(
        "Selected event:",
        "Type: recorder delimited record",
        `Recorder: ${event.recorder}`,
        `Field count: ${event.payloadFields.length}`,
        "Status: this recorder line is parsed as a pipe-delimited payload; field semantics remain source-specific.",
      );
      payloadContext.push(
        "Recorder fields:",
        ...event.payloadFields.map((value, valueIndex) => `${valueIndex + 1}. ${value || "(blank)"}`),
      );
      break;
    }
    case "locomotive-recorder-record": {
      workflowSummary = "Locomotive recorder payload.";
      currentStep = "Locomotive recorder line captured";
      knownState = `${event.recordType} ${event.payloadFields.length} fields`;
      meaning.push(
        "Selected event:",
        "Type: locomotive recorder record",
        `Record type: ${event.recordType}`,
        `Field count: ${event.payloadFields.length}`,
        "Status: this pipe-delimited locomotive recorder line is classified by record type; individual field semantics remain recorder-format specific.",
      );
      payloadContext.push(
        "Recorder fields:",
        ...event.payloadFields.map((value, valueIndex) => `${valueIndex + 1}. ${value || "(blank)"}`),
      );
      break;
    }
    case "raw-hex-payload": {
      workflowSummary = "Raw hexadecimal payload.";
      currentStep = "Hex payload line captured";
      knownState = `${event.byteCount} bytes`;
      meaning.push(
        "Selected event:",
        "Type: raw hex payload",
        `Byte count: ${event.byteCount}`,
        "Status: this line is a source-local hexadecimal payload chunk; no stronger field-level meaning is assigned without a matching source format.",
      );
      payloadContext.push("Payload preview:", event.payloadPreview);
      break;
    }
    case "workstation-vetms-message": {
      workflowSummary = `${event.messageCategory ?? event.messageCommand} ${event.messageType ?? "workstation message"}`.trim();
      currentStep = event.messageDirection === "SEND" ? "Workstation message sent" : "Workstation message received";
      knownState = [event.messageType, event.stateChange].filter(Boolean).join(" / ") || event.messageCommand;
      meaning.push(
        "Selected event:",
        "Type: workstation VETMS message",
        `Direction: ${event.messageDirection === "SEND" ? "sent" : "received"}`,
        `Route: ${event.route}`,
        `Command: ${event.messageCommand}`,
        ...(event.messageCategory ? [`Category: ${event.messageCategory}`] : []),
        ...(event.messageType ? [`Message type: ${event.messageType}`] : []),
        ...(event.stateChange ? [`State change: ${event.stateChange}`] : []),
        ...(event.trainSymbol ? [`Train symbol: ${event.trainSymbol}`] : []),
        ...(event.locoUid ? [`Locomotive: ${event.locoUid}`] : []),
        ...(event.reportTime ? [`Report time: ${event.reportTime}`] : []),
        ...(event.directionOfTravel ? [`Direction of travel: ${event.directionOfTravel}`] : []),
        ...(event.headMp ? [`Head milepost: ${event.headMp}`] : []),
        ...(event.rearMp ? [`Rear milepost: ${event.rearMp}`] : []),
        ...(event.headEndTrack ? [`Head-end track: ${event.headEndTrack}`] : []),
        ...(event.rearEndTrack ? [`Rear-end track: ${event.rearEndTrack}`] : []),
        ...(event.subdivisionId ? [`Subdivision ID: ${event.subdivisionId}`] : []),
        ...(event.speed ? [`Speed: ${event.speed}`] : []),
        ...(event.locoState ? [`Locomotive state: ${event.locoState}`] : []),
        ...(event.locoStateSummary ? [`Locomotive state summary: ${event.locoStateSummary}`] : []),
        ...(event.employeeId ? [`Employee ID: ${event.employeeId}`] : []),
        ...(event.employeeName ? [`Employee name: ${event.employeeName}`] : []),
        ...(event.traceComponent ? [`Trace component: ${event.traceComponent}`] : []),
        ...(event.traceLevel ? [`Trace level: ${event.traceLevel}`] : []),
        ...(event.traceLineNumber ? [`Trace point: line ${event.traceLineNumber}`] : []),
      );
      break;
    }
    case "prefixed-log-message": {
      workflowSummary = `${event.prefix} log entry.`;
      currentStep = "Application log entry captured";
      knownState = event.prefix;
      meaning.push(
        "Selected event:",
        "Type: prefixed application log",
        `Prefix: ${event.prefix}`,
        `Payload: ${event.payload || "(blank)"}`,
        "Status: this line is preserved as a structured prefixed log entry without claiming a stronger grounded decoder mapping.",
      );
      if (event.payload) {
        payloadContext.push("Raw payload:", event.payload);
      }
      break;
    }
    case "binary-state-dump": {
      workflowSummary = "Standalone binary state dump.";
      currentStep = "Binary payload row logged";
      knownState = `${event.payloadBits.length} bits`;
      meaning.push(
        "Selected event:",
        "Type: binary state dump",
        `Payload width: ${event.payloadBits.length} bits`,
        `Payload bits: ${event.payloadBits}`,
        "Status: this row contains a bare binary payload without its label on the same line.",
      );
      payloadContext.push(`Payload bits: ${event.payloadBits}`);
      break;
    }
    case "pipe-exception": {
      workflowSummary = "Pipe-trace exception.";
      currentStep = "Exception logged";
      knownState = event.exceptionType;
      meaning.push(
        "Selected event:",
        "Type: exception trace",
        `Component: ${event.component}`,
        `Failure summary: ${event.summary}`,
        `Exception: ${event.exceptionType}`,
        `Detail: ${event.exceptionMessage}`,
      );
      break;
    }
    default:
      return generic;
  }

  return {
    lineId: line.id,
    lineNumber: line.lineNumber,
    timestamp: line.timestamp,
    raw: line.raw,
    translation: {
      original: line.raw,
      structured,
      english: meaning,
      unresolved: [],
    },
      workflow: {
        summary: workflowSummary,
        currentStep,
        systems: [],
        objects: [],
        knownState,
        unresolved: [],
      },
    genisysContext: [],
    icdContext: [],
    databaseContext: tmdsContext,
    workflowContext: workflowDetails.lines,
    workflowRelated,
    payloadContext,
    relatedPair,
    sourceReferences: [],
  };
}

function computeRuntimeViewerPreviewCount(lineCount: number): number {
  if (lineCount <= 256) {
    return lineCount;
  }
  if (lineCount <= 2048) {
    return 224;
  }
  if (lineCount <= 8192) {
    return 160;
  }
  return 128;
}

function buildRuntimeViewerPreviewDetails(
  lines: ParsedLine[],
  events: ParsedLogEvent[],
  bundle: LogEnrichmentBundle | null,
  workflowIndex: WorkflowWindowIndex,
  reportProgress?: ProgressReporter,
): Record<string, DetailModel> {
  const previewCount = computeRuntimeViewerPreviewCount(lines.length);
  const preview: Record<string, DetailModel> = {};
  const total = Math.max(previewCount, 1);
  const progressStart = 58;
  const progressEnd = 88;
  const progressSpan = progressEnd - progressStart;
  const reportInterval = Math.max(10, Math.floor(previewCount / 40));
  for (let index = 0; index < previewCount; index += 1) {
    const line = lines[index];
    if (!line) {
      continue;
    }
    preview[line.id] = buildDetailForLine(index, lines, events, bundle, workflowIndex);
    if (reportProgress && (index === 0 || index === previewCount - 1 || (index + 1) % reportInterval === 0)) {
      reportProgress({
        phase: "detail",
        message: `building initial details ${index + 1}/${previewCount}`,
        percent: progressStart + ((index + 1) / total) * progressSpan,
        completed: index + 1,
        total: previewCount,
        currentPath: toProgressPathLabel(line.source),
      });
    }
  }
  return preview;
}

type RuntimeSessionContext = {
  sessionId: string;
  createdAt: number;
  lastAccessedAt: number;
  lines: ParsedLine[];
  events: ParsedLogEvent[];
  bundle: LogEnrichmentBundle | null;
  workflowIndex: WorkflowWindowIndex;
  referenceOnly: boolean;
  indexByLineId: Map<string, number>;
  detailCache: Record<string, DetailModel>;
  detailCacheOrder: string[];
  detailCacheLimit: number;
  warmCursor: number;
  warmPriorityQueue: number[];
  warmQueued: Set<number>;
  warmScheduled: boolean;
};

const runtimeSessions = new Map<string, RuntimeSessionContext>();
let activeRuntimeSessionId: string | null = null;
const runtimeDetailPrefetchRadius = 24;
const runtimeDetailWarmBatchSize = 72;
const runtimeSessionTtlMs = 8 * 60 * 60 * 1000;
const runtimeSessionMaxCount = 24;

function computeRuntimeDetailCacheLimit(lineCount: number): number {
  if (lineCount <= 0) {
    return 2048;
  }
  if (lineCount <= 4096) {
    return lineCount;
  }
  return Math.min(Math.max(4096, Math.ceil(lineCount * 0.4)), 16384);
}

function createRuntimeSessionId(): string {
  return randomBytes(12).toString("hex");
}

function unregisterRuntimeSession(sessionId: string): void {
  runtimeSessions.delete(sessionId);
  if (activeRuntimeSessionId === sessionId) {
    activeRuntimeSessionId = null;
  }
}

function trimRuntimeSessions(): void {
  const now = Date.now();
  for (const [sessionId, session] of runtimeSessions.entries()) {
    if (now - session.lastAccessedAt > runtimeSessionTtlMs) {
      unregisterRuntimeSession(sessionId);
    }
  }

  while (runtimeSessions.size >= runtimeSessionMaxCount) {
    let oldestSession: RuntimeSessionContext | null = null;
    for (const session of runtimeSessions.values()) {
      if (!oldestSession || session.lastAccessedAt < oldestSession.lastAccessedAt) {
        oldestSession = session;
      }
    }
    if (!oldestSession) {
      break;
    }
    unregisterRuntimeSession(oldestSession.sessionId);
  }
}

function registerRuntimeSession(
  session: Omit<RuntimeSessionContext, "sessionId" | "createdAt" | "lastAccessedAt">,
  makeActive = true,
): string {
  trimRuntimeSessions();
  const now = Date.now();
  const sessionId = createRuntimeSessionId();
  runtimeSessions.set(sessionId, {
    ...session,
    sessionId,
    createdAt: now,
    lastAccessedAt: now,
  });
  if (makeActive) {
    activeRuntimeSessionId = sessionId;
  }
  return sessionId;
}

function resolveRuntimeSession(sessionId?: string | null): RuntimeSessionContext | null {
  trimRuntimeSessions();
  const resolvedSessionId = sessionId ?? activeRuntimeSessionId;
  if (!resolvedSessionId) {
    return null;
  }
  const session = runtimeSessions.get(resolvedSessionId) ?? null;
  if (!session) {
    if (activeRuntimeSessionId === resolvedSessionId) {
      activeRuntimeSessionId = null;
    }
    return null;
  }
  session.lastAccessedAt = Date.now();
  return session;
}

function isTrackedRuntimeSession(session: RuntimeSessionContext): boolean {
  return runtimeSessions.get(session.sessionId) === session;
}

function storeRuntimeSessionDetail(session: RuntimeSessionContext, lineId: string, detail: DetailModel): DetailModel {
  if (session.detailCache[lineId]) {
    return session.detailCache[lineId];
  }
  session.detailCache[lineId] = detail;
  session.detailCacheOrder.push(lineId);
  while (session.detailCacheOrder.length > session.detailCacheLimit) {
    const evicted = session.detailCacheOrder.shift();
    if (!evicted || evicted === lineId) {
      continue;
    }
    delete session.detailCache[evicted];
  }
  return detail;
}

function enqueueRuntimeSessionWarmIndexes(session: RuntimeSessionContext, indexes: number[]): void {
  for (const candidateIndex of indexes) {
    if (candidateIndex < 0 || candidateIndex >= session.lines.length) {
      continue;
    }
    const candidateLine = session.lines[candidateIndex];
    if (!candidateLine || session.detailCache[candidateLine.id] || session.warmQueued.has(candidateIndex)) {
      continue;
    }
    session.warmPriorityQueue.push(candidateIndex);
    session.warmQueued.add(candidateIndex);
  }
  scheduleRuntimeSessionWarmup(session);
}

function prefetchRuntimeSessionDetails(session: RuntimeSessionContext, anchorIndex: number): void {
  const indexes: number[] = [];
  for (let offset = 1; offset <= runtimeDetailPrefetchRadius; offset += 1) {
    indexes.push(anchorIndex + offset, anchorIndex - offset);
  }
  enqueueRuntimeSessionWarmIndexes(session, indexes);
}

function scheduleRuntimeSessionWarmup(session: RuntimeSessionContext): void {
  if (!isTrackedRuntimeSession(session) || session.warmScheduled) {
    return;
  }
  session.warmScheduled = true;
  setImmediate(() => {
    session.warmScheduled = false;
    if (!isTrackedRuntimeSession(session)) {
      return;
    }

    let built = 0;
    while (built < runtimeDetailWarmBatchSize) {
      let candidateIndex = session.warmPriorityQueue.shift();
      if (candidateIndex !== undefined) {
        session.warmQueued.delete(candidateIndex);
      } else {
        while (session.warmCursor < session.lines.length) {
          const nextIndex = session.warmCursor;
          session.warmCursor += 1;
          const nextLine = session.lines[nextIndex];
          if (nextLine && !session.detailCache[nextLine.id]) {
            candidateIndex = nextIndex;
            break;
          }
        }
      }

      if (candidateIndex === undefined) {
        break;
      }

      const candidateLine = session.lines[candidateIndex];
      if (!candidateLine || session.detailCache[candidateLine.id]) {
        continue;
      }

      storeRuntimeSessionDetail(
        session,
        candidateLine.id,
        buildDetailForLine(candidateIndex, session.lines, session.events, session.bundle, session.workflowIndex),
      );
      built += 1;
    }

    if ((session.warmPriorityQueue.length || session.warmCursor < session.lines.length) && isTrackedRuntimeSession(session)) {
      scheduleRuntimeSessionWarmup(session);
    }
  });
}

function getRuntimeSessionDetail(lineId: string, sessionId?: string | null): DetailModel | null {
  const session = resolveRuntimeSession(sessionId);
  if (!session) {
    return null;
  }
  const cached = session.detailCache[lineId];
  if (cached) {
    return cached;
  }
  const index = session.indexByLineId.get(lineId);
  if (index === undefined) {
    return null;
  }
  const detail = storeRuntimeSessionDetail(
    session,
    lineId,
    buildDetailForLine(index, session.lines, session.events, session.bundle, session.workflowIndex),
  );
  prefetchRuntimeSessionDetails(session, index);
  return detail;
}

function warmRuntimeSessionDetails(lineIds: string[], sessionId?: string | null): void {
  const session = resolveRuntimeSession(sessionId);
  if (!session || !lineIds.length) {
    return;
  }
  const indexes: number[] = [];
  for (const lineId of lineIds) {
    const index = session.indexByLineId.get(String(lineId ?? ""));
    if (index === undefined) {
      continue;
    }
    indexes.push(index);
    for (let offset = 1; offset <= 16; offset += 1) {
      indexes.push(index + offset, index - offset);
    }
  }
  enqueueRuntimeSessionWarmIndexes(session, indexes);
}

function buildClientLines(lines: ParsedLine[]): ParsedLine[] {
  return lines.map((line) => line.tokens.length ? { ...line, tokens: [] } : line);
}

async function expandPaths(inputPaths: string[]): Promise<string[]> {
  const nested = await Promise.all(inputPaths.map(async (inputPath) => {
    const entry = await stat(inputPath).catch(() => null);
    if (!entry) {
      return [] as string[];
    }
    if (entry.isDirectory()) {
      const directoryName = inputPath.split(/[\\/]/).pop()?.toLowerCase() ?? "";
      if (ignoredInputDirectoryNames.has(directoryName)) {
        return [] as string[];
      }
      const children = await readdir(inputPath, { withFileTypes: true });
      const next = children.map((child) => resolve(inputPath, child.name));
      return expandPaths(next);
    }
    return entry.isFile() ? [inputPath] : [];
  }));
  return nested.flat();
}

type IngestOptions = {
  maxLinesPerSource?: number;
  reportProgress?: ProgressReporter;
};

type UploadedLogSource = {
  name: string;
  relativePath?: string;
  dataBase64?: string;
  tempPath?: string;
};

async function readLoadableSource(path: string, maxLinesPerSource: number | undefined): Promise<ParsedLine[]> {
  if (isTextFile(path)) {
    return selectPreviewLines(await readLines(path), maxLinesPerSource);
  }
  if (isGzipFile(path)) {
    return selectPreviewLines(await readGzipLines(path), maxLinesPerSource);
  }
  if (isZipFile(path)) {
    return selectPreviewLines(await readZipLines(path), maxLinesPerSource);
  }
  return [];
}

async function readUploadedSource(source: UploadedLogSource, maxLinesPerSource: number | undefined): Promise<ParsedLine[]> {
  const sourcePath = source.relativePath?.trim() || source.name.trim();
  if (!sourcePath) {
    return [];
  }

  if (source.tempPath) {
    if (isTextFile(sourcePath)) {
      const buffer = await readFile(source.tempPath);
      return selectPreviewLines(parseLines(decodeTextBuffer(buffer), sourcePath), maxLinesPerSource);
    }
    if (isGzipFile(sourcePath)) {
      const buffer = await readFile(source.tempPath);
      return selectPreviewLines(parseLines(decodeTextBuffer(gunzipSync(buffer)), sourcePath), maxLinesPerSource);
    }
    if (isZipFile(sourcePath)) {
      return selectPreviewLines(await readZipLinesFromArchive(() => openZipFile(source.tempPath!), sourcePath), maxLinesPerSource);
    }
    return [];
  }

  if (!source.dataBase64) {
    return [];
  }

  const buffer = Buffer.from(source.dataBase64, "base64");
  if (isTextFile(sourcePath)) {
    return selectPreviewLines(parseLines(decodeTextBuffer(buffer), sourcePath), maxLinesPerSource);
  }
  if (isGzipFile(sourcePath)) {
    return selectPreviewLines(parseLines(decodeTextBuffer(gunzipSync(buffer)), sourcePath), maxLinesPerSource);
  }
  if (isZipFile(sourcePath)) {
    return selectPreviewLines(await readZipLinesFromArchive(() => openZipBuffer(buffer), sourcePath), maxLinesPerSource);
  }
  return [];
}

function describeLoadableSource(path: string): string {
  const normalized = path.replace(/\\/g, "/").toUpperCase();
  if (normalized.endsWith(".LOG.GZ") || normalized.endsWith(".TXT.GZ") || normalized.endsWith(".GZ")) {
    return "gzip log";
  }
  if (normalized.endsWith(".ZIP")) {
    return "zip archive";
  }
  if (normalized.includes("SOCKETTRACE")) {
    return "socket trace";
  }
  if (normalized.includes("EXCEPTION")) {
    return "exception log";
  }
  if (normalized.includes("EVENTLOG")) {
    return "event log";
  }
  return "text log";
}

function getReadConcurrency(totalSources: number): number {
  if (totalSources <= 1) {
    return totalSources;
  }
  const cpuCount = typeof availableParallelism === "function" ? availableParallelism() : 4;
  return Math.max(2, Math.min(totalSources, Math.min(8, Math.ceil(cpuCount / 2))));
}

async function ingestPaths(paths: string[], options?: IngestOptions): Promise<SessionData> {
  const reportProgress = options?.reportProgress;
  const out: ParsedLine[] = [];
  reportProgress?.({
    phase: "prepare",
    message: "scanning selected files",
    percent: 2,
    completed: 0,
    total: Math.max(paths.length, 1),
  });
  const expandedPaths = await expandPaths(paths);
  const loadablePaths = expandedPaths.filter((path) => isTextFile(path) || isZipFile(path) || isGzipFile(path));
  const totalSources = Math.max(loadablePaths.length, 1);
  reportProgress?.({
    phase: "prepare",
    message: `found ${loadablePaths.length} readable source${loadablePaths.length === 1 ? "" : "s"}`,
    percent: 8,
    completed: 0,
    total: totalSources,
  });
  const enrichmentPromise = loadablePaths.length ? loadLogEnrichment() : Promise.resolve(null);
  const orderedSources = new Array<ParsedLine[]>(loadablePaths.length);
  let nextIndex = 0;
  let completed = 0;
  const workerCount = getReadConcurrency(loadablePaths.length);

  await Promise.all(Array.from({ length: workerCount }, async () => {
    while (true) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= loadablePaths.length) {
        return;
      }
      const path = loadablePaths[index];
      const sourceKind = describeLoadableSource(path);
      reportProgress?.({
        phase: "read",
        message: `reading ${sourceKind} ${index + 1}/${loadablePaths.length}`,
        percent: 8 + (completed / totalSources) * 42,
        completed,
        total: loadablePaths.length,
        currentPath: toProgressPathLabel(path),
      });
      orderedSources[index] = await readLoadableSource(path, options?.maxLinesPerSource);
      const loadedLines = orderedSources[index]?.length ?? 0;
      completed += 1;
      reportProgress?.({
        phase: "read",
        message: `loaded ${sourceKind} ${completed}/${loadablePaths.length} (${loadedLines} lines)`,
        percent: 8 + (completed / totalSources) * 42,
        completed,
        total: loadablePaths.length,
        currentPath: toProgressPathLabel(path),
      });
    }
  }));

  for (const sourceLines of orderedSources) {
    if (sourceLines?.length) {
      appendAll(out, sourceLines);
    }
  }
  reportProgress?.({
    phase: "detail",
    message: `loading TMDS enrichment for ${out.length} line${out.length === 1 ? "" : "s"}`,
    percent: 52,
    completed: 0,
    total: Math.max(out.length, 1),
  });
  const bundle = await enrichmentPromise;
  const workflowIndex = buildWorkflowWindowIndex(out);
  reportProgress?.({
    phase: "detail",
    message: `indexing parsed events for ${out.length} line${out.length === 1 ? "" : "s"}`,
    percent: 56,
    completed: 0,
    total: Math.max(out.length, 1),
  });
  const events = out.map((line) => parseLogEvent(line));
  const viewerPreviewDetails = buildRuntimeViewerPreviewDetails(out, events, bundle, workflowIndex, reportProgress);
  reportProgress?.({
    phase: "package",
    message: `assembling parsed session indexes`,
    percent: 90,
    completed: out.length,
    total: Math.max(out.length, 1),
  });
  const first = out[0] ? viewerPreviewDetails[out[0].id] ?? buildDetailForLine(0, out, events, bundle, workflowIndex) : null;
  reportProgress?.({
    phase: "package",
    message: `preparing initial viewer snapshot (${Object.keys(viewerPreviewDetails).length} details)`,
    percent: 93,
    completed: Math.min(Object.keys(viewerPreviewDetails).length, out.length),
    total: Math.max(out.length, 1),
  });
  const sessionId = registerRuntimeSession({
    lines: out,
    events,
    bundle,
    workflowIndex,
    referenceOnly: out.length > 0 && out.every((line) => String(line.source ?? "").startsWith("reference:")),
    indexByLineId: new Map(out.map((line, index) => [line.id, index])),
    detailCache: { ...viewerPreviewDetails },
    detailCacheOrder: Object.keys(viewerPreviewDetails),
    detailCacheLimit: computeRuntimeDetailCacheLimit(out.length),
    warmCursor: out.length,
    warmPriorityQueue: [],
    warmQueued: new Set<number>(),
    warmScheduled: false,
  });
  reportProgress?.({
    phase: "package",
    message: `sending parsed session to the viewer`,
    percent: 96,
    completed: out.length,
    total: Math.max(out.length, 1),
  });
  return { sessionId, lines: buildClientLines(out), detail: first, lineDetails: viewerPreviewDetails };
}

async function ingestUploadedSources(sources: UploadedLogSource[], options?: IngestOptions): Promise<SessionData> {
  const reportProgress = options?.reportProgress;
  const out: ParsedLine[] = [];
  const loadableSources = sources.filter((source) => {
    const sourcePath = source.relativePath?.trim() || source.name.trim();
    return sourcePath && (isTextFile(sourcePath) || isZipFile(sourcePath) || isGzipFile(sourcePath));
  });
  const totalSources = Math.max(loadableSources.length, 1);
  reportProgress?.({
    phase: "prepare",
    message: `received ${loadableSources.length} browser-uploaded readable source${loadableSources.length === 1 ? "" : "s"}`,
    percent: 8,
    completed: 0,
    total: totalSources,
  });
  const enrichmentPromise = loadableSources.length ? loadLogEnrichment() : Promise.resolve(null);
  const orderedSources = new Array<ParsedLine[]>(loadableSources.length);
  let nextIndex = 0;
  let completed = 0;
  const workerCount = getReadConcurrency(loadableSources.length);

  await Promise.all(Array.from({ length: workerCount }, async () => {
    while (true) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= loadableSources.length) {
        return;
      }
      const source = loadableSources[index];
      const sourcePath = source.relativePath?.trim() || source.name.trim();
      const sourceKind = describeLoadableSource(sourcePath);
      reportProgress?.({
        phase: "read",
        message: `reading uploaded ${sourceKind} ${index + 1}/${loadableSources.length}`,
        percent: 8 + (completed / totalSources) * 42,
        completed,
        total: loadableSources.length,
        currentPath: toProgressPathLabel(sourcePath),
      });
      orderedSources[index] = await readUploadedSource(source, options?.maxLinesPerSource);
      completed += 1;
      reportProgress?.({
        phase: "read",
        message: `loaded uploaded ${sourceKind} ${completed}/${loadableSources.length}`,
        percent: 8 + (completed / totalSources) * 42,
        completed,
        total: loadableSources.length,
        currentPath: toProgressPathLabel(sourcePath),
      });
    }
  }));

  for (const sourceLines of orderedSources) {
    if (sourceLines?.length) {
      appendAll(out, sourceLines);
    }
  }

  const bundle = await enrichmentPromise;
  const workflowIndex = buildWorkflowWindowIndex(out);
  const events = out.map((line) => parseLogEvent(line));
  const viewerPreviewDetails = buildRuntimeViewerPreviewDetails(out, events, bundle, workflowIndex, reportProgress);
  const first = out[0] ? viewerPreviewDetails[out[0].id] ?? buildDetailForLine(0, out, events, bundle, workflowIndex) : null;
  const sessionId = registerRuntimeSession({
    lines: out,
    events,
    bundle,
    workflowIndex,
    referenceOnly: false,
    indexByLineId: new Map(out.map((line, index) => [line.id, index])),
    detailCache: { ...viewerPreviewDetails },
    detailCacheOrder: Object.keys(viewerPreviewDetails),
    detailCacheLimit: computeRuntimeDetailCacheLimit(out.length),
    warmCursor: out.length,
    warmPriorityQueue: [],
    warmQueued: new Set<number>(),
    warmScheduled: false,
  });
  return { sessionId, lines: buildClientLines(out), detail: first, lineDetails: viewerPreviewDetails };
}

async function buildCuratedSampleSession(): Promise<SessionData> {
  return ingestPaths([
    resolve(app.getAppPath(), "sample_logs/curated"),
    "C:\\Users\\Ji\\Music\\CAD Related Files for SD SUB\\eventlogs\\EventLog.txt",
    "C:\\Users\\Ji\\Music\\CAD Related Files for SD SUB\\eventlogs\\NetworkExceptionLog.txt",
    "C:\\Users\\Ji\\Music\\CAD Related Files for ESCO SUB\\CAD Related Files for ESCO SUB\\eventlogs\\EventLog.txt",
    "C:\\Users\\Ji\\Music\\CAD Related Files for ESCO SUB\\CAD Related Files for ESCO SUB\\eventlogs\\NetworkExceptionLog.txt",
  ], { maxLinesPerSource: 300, reportProgress: createProgressReporter("review") });
}

type CorpusFallbackExample = {
  source: string;
  lineNumber: number;
  raw: string;
};

type CorpusFallbackFamilySummary = {
  family: string;
  count: number;
  examples: CorpusFallbackExample[];
};

type CorpusAuditReport = {
  scannedRootPaths: string[];
  scannedSourceCount: number;
  scannedLineCount: number;
  fallbackLineCount: number;
  families: CorpusFallbackFamilySummary[];
};

let handledDetailFamiliesCache: Set<string> | null = null;

function getHandledDetailFamilies(): Set<string> {
  if (!handledDetailFamiliesCache) {
    handledDetailFamiliesCache = new Set(
      Array.from(buildDetailForLine.toString().matchAll(/case "([^"]+)":/g)).map((match) => match[1]),
    );
  }
  return handledDetailFamiliesCache;
}

function looksLikeAuditLogLine(raw: string): boolean {
  const line = raw.trim();
  if (!line) {
    return false;
  }
  if (/^<\?xml\b/i.test(line) || /^<[A-Za-z][^>]*>/.test(line)) {
    return false;
  }
  return logTimestampPattern.test(line)
    || embeddedSlashTimestampPattern.test(line)
    || /^####:\d{4}\/\d{2}\/\d{2}/.test(line)
    || /^(SYS|WARN|NOTE|ERR|INFO|OTX|ORX|OTXD|ORXD|CHR)\s*:/.test(line)
    || /^\|[^|]+\|[^|]+\|/.test(line)
    || /^(NAME|IND MNEM|CTL MNEM|PrintControlQueue|ProcessControlCompleted|Control Queue Being Cleared|RESEND CONTROL GENISYS|HOST REFUSED CONNECTION REQUEST)\b/i.test(line);
}

function isLikelyAuditLogSource(path: string, lines: ParsedLine[]): boolean {
  const normalized = path.replace(/\\/g, "/").toUpperCase();
  if (/\.(XML|JSON|CSV|TSV|MD|INI|CFG)$/i.test(path)) {
    return false;
  }
  const sample = lines
    .map((line) => line.raw.trim())
    .filter(Boolean)
    .slice(0, 200);
  if (!sample.length) {
    return false;
  }
  const logLikeCount = sample.filter(looksLikeAuditLogLine).length;
  if (/EVENTLOG|SOCKET|TRACE|EXCEPTION|NETWORKEXCEPTION|CODELINE|CODESERVER|CAD|BOS|BOC|LOCO|5008/i.test(normalized)) {
    return logLikeCount > 0;
  }
  return logLikeCount >= Math.min(5, Math.max(2, Math.ceil(sample.length * 0.15)));
}

function describeFallbackFamilyFromLine(line: ParsedLine): string {
  const content = stripLeadingLogTimestamp(line.raw);
  const prefixMatch = /^([^:=|]+)[:=]/.exec(content);
  if (prefixMatch?.[1]?.trim()) {
    return prefixMatch[1].replace(/\s+/g, " ").trim();
  }
  const normalized = content.replace(/\s+/g, " ").trim();
  const phraseMatch = /^([A-Za-z][A-Za-z0-9./_-]*(?:\s+[A-Za-z][A-Za-z0-9./_-]*){0,7})/.exec(normalized);
  if (phraseMatch?.[1]?.trim()) {
    const phrase = phraseMatch[1]
      .replace(/[-:]?\d+$/, "")
      .replace(/\s+\((?:\d+|[01])\)$/, "")
      .trim();
    if (phrase) {
      return phrase;
    }
  }
  return "raw-unclassified";
}

async function auditCorpusFallbacks(paths: string[]): Promise<CorpusAuditReport> {
  const expandedPaths = await expandPaths(paths);
  const loadablePaths = expandedPaths.filter((path) => isTextFile(path));
  const handledFamilies = getHandledDetailFamilies();
  const families = new Map<string, CorpusFallbackFamilySummary>();
  let scannedSourceCount = 0;
  let scannedLineCount = 0;
  let fallbackLineCount = 0;

  for (let sourceIndex = 0; sourceIndex < loadablePaths.length; sourceIndex += 1) {
    const path = loadablePaths[sourceIndex];
    const lines = await readLoadableSource(path, undefined);
    if (!lines.length || !isLikelyAuditLogSource(path, lines)) {
      continue;
    }
    scannedSourceCount += 1;
    scannedLineCount += lines.length;
    const events = lines.map((line) => parseLogEvent(line));
    for (let lineIndex = 0; lineIndex < lines.length; lineIndex += 1) {
      const event = events[lineIndex];
      const family =
        event.family === "other"
          ? describeFallbackFamilyFromLine(lines[lineIndex])
          : !handledFamilies.has(event.family)
            ? event.family
            : "";
      if (!family) {
        continue;
      }
      fallbackLineCount += 1;
      const line = lines[lineIndex];
      const entry = families.get(family) ?? { family, count: 0, examples: [] };
      entry.count += 1;
      if (entry.examples.length < 5) {
        entry.examples.push({
          source: line.source || path,
          lineNumber: line.lineNumber,
          raw: line.raw,
        });
      }
      families.set(family, entry);
    }
    const fallbackFamiliesSeen = Array.from(families.values()).reduce((count, entry) => count + entry.count, 0);
    console.log(`[audit-corpus] ${sourceIndex + 1}/${loadablePaths.length} ${toProgressPathLabel(path)} lines=${lines.length} cumulativeFallbacks=${fallbackFamiliesSeen}`);
  }

  return {
    scannedRootPaths: paths,
    scannedSourceCount,
    scannedLineCount,
    fallbackLineCount,
    families: Array.from(families.values()).sort((left, right) => {
      if (right.count !== left.count) {
        return right.count - left.count;
      }
      return left.family.localeCompare(right.family);
    }),
  };
}

async function runCorpusAuditCli(paths: string[]): Promise<void> {
  const rootPaths = paths.length ? paths : ["C:\\Users\\Ji\\Music"];
  console.log(`[audit-corpus] scanning ${rootPaths.join(", ")}`);
  const report = await auditCorpusFallbacks(rootPaths);
  const reportDir = resolve(app.getAppPath(), "reports");
  await mkdir(reportDir, { recursive: true });
  const reportPath = resolve(reportDir, "music-corpus-fallback-audit.json");
  await writeFile(reportPath, JSON.stringify(report, null, 2), "utf8");
  console.log(`[audit-corpus] scannedSources=${report.scannedSourceCount} scannedLines=${report.scannedLineCount} fallbackLines=${report.fallbackLineCount}`);
  for (const entry of report.families.slice(0, 25)) {
    console.log(`[audit-corpus] family=${entry.family} count=${entry.count}`);
    for (const example of entry.examples.slice(0, 2)) {
      console.log(`  example ${toProgressPathLabel(example.source)}:${example.lineNumber} ${example.raw}`);
    }
  }
  console.log(`[audit-corpus] report=${reportPath}`);
}

function pushFoundationLine(
  lines: ParsedLine[],
  lineDetails: Record<string, DetailModel>,
  raw: string,
  detail: Omit<DetailModel, "lineId" | "lineNumber" | "raw"> & { source?: string; tokens?: string[] },
): void {
  const lineNumber = lines.length + 1;
  const id = `foundation:${lineNumber}`;
  const parsedLine: ParsedLine = {
    id,
    lineNumber,
    source: detail.source,
    raw,
    tokens: detail.tokens ?? raw.split(/\s+/).filter(Boolean),
  };
  lines.push(parsedLine);
  lineDetails[id] = {
    lineId: id,
    lineNumber,
    timestamp: detail.timestamp,
    raw,
    translation: detail.translation,
    workflow: detail.workflow,
    genisysContext: detail.genisysContext,
    icdContext: detail.icdContext,
    databaseContext: detail.databaseContext,
    workflowContext: detail.workflowContext,
    workflowRelated: detail.workflowRelated,
    payloadContext: detail.payloadContext,
    relatedPair: detail.relatedPair,
    relatedTimeline: detail.relatedTimeline,
    referenceBadges: detail.referenceBadges,
    referenceArtifact: detail.referenceArtifact,
    referenceDiagram: detail.referenceDiagram,
    referenceChoiceGroups: detail.referenceChoiceGroups,
    sourceReferences: detail.sourceReferences,
  };
}

async function buildFoundationSession(reportProgress?: ProgressReporter): Promise<SessionData> {
  reportProgress?.({
    phase: "prepare",
    message: "loading foundation manifests",
    percent: 5,
    completed: 0,
    total: 7,
  });
  const [
    sqlManifest,
    normalizedManifest,
    musicSources,
    statsPayload,
    subdivisionSummary,
    trainRuntimeJoinSummary,
    referenceFamilySummary,
  ] = await Promise.all([
    readJsonFile<SqlFoundationManifestEntry[]>("exports", "raw", "sql_foundation", "sql_foundation_manifest.json"),
    readJsonFile<NormalizedManifestEntry[]>("exports", "normalized", "tmds_foundation_manifest.json"),
    readJsonFile<MusicSourceEntry[]>("exports", "inventory", "music_sources.json"),
    readJsonFile<FoundationStatsPayload>("exports", "normalized", "tmds_foundation_stats.json"),
    readJsonFile<SubdivisionProtocolRow[]>("exports", "normalized", "subdivision_protocol_summary.json"),
    readJsonFile<TrainRuntimeJoinRow[]>("exports", "normalized", "train_runtime_join_summary.json"),
    readJsonFile<ReferenceFamilyRow[]>("exports", "normalized", "reference_family_summary.json"),
  ]);
  reportProgress?.({
    phase: "prepare",
    message: "building foundation summary",
    percent: 60,
    completed: 4,
    total: 7,
  });

  const stats = statsPayload.flat_metrics ?? [];
  const staticExports = sqlManifest.filter((entry) => entry.database === "tmdsDatabaseStatic");
  const dynamicExports = sqlManifest.filter((entry) => entry.database === "tmdsDatabaseDynamic");
  const staticExportByName = new Map(staticExports.map((entry) => [entry.export_name, entry]));
  const dynamicExportByName = new Map(dynamicExports.map((entry) => [entry.export_name, entry]));
  const normalizedByName = new Map(normalizedManifest.map((entry) => [entry.name, entry]));
  const topLevelMusicSources = musicSources.filter((row) => !row.notes?.includes("Discovered by filename sweep."));
  const trainRuntimeStatus = new Map(trainRuntimeJoinSummary.map((row) => [row.runtime_join_status, toNumber(row.row_count)]));
  const topUnresolvedFamilies = referenceFamilySummary
    .slice(0, 3)
    .map((row) => `${row.component_family}.${row.reference_column}=${toNumber(row.unresolved_count)}`)
    .join(", ");

  const sqlManifestPath = foundationPath("exports", "raw", "sql_foundation", "sql_foundation_manifest.json");
  const normalizedManifestPath = foundationPath("exports", "normalized", "tmds_foundation_manifest.json");
  const statsPath = foundationPath("exports", "normalized", "tmds_foundation_stats.json");
  const musicInventoryPath = foundationPath("exports", "inventory", "music_sources.json");
  const subdivisionSummaryPath = foundationPath("exports", "normalized", "subdivision_protocol_summary.json");
  const referenceSummaryPath = foundationPath("exports", "normalized", "reference_family_summary.json");
  const trainRuntimeJoinPath = foundationPath("exports", "normalized", "train_runtime_join_summary.json");

  const lines: ParsedLine[] = [];
  const lineDetails: Record<string, DetailModel> = {};

  const staticSummaryRaw =
    `TMDS STATIC SQL foundation: exports=${staticExports.length}, rows=${sumRowCounts(staticExports)}, ` +
    `bit assignments=${toNumber(staticExportByName.get("code_bit_lookup")?.row_count)}, stations=${toNumber(staticExportByName.get("code_station_context")?.row_count)}, ` +
    `signals=${toNumber(staticExportByName.get("signal_detail_full")?.row_count)}, tracks=${toNumber(staticExportByName.get("track_detail_full")?.row_count)}, switches=${toNumber(staticExportByName.get("switch_detail_full")?.row_count)}.`;
  pushFoundationLine(lines, lineDetails, staticSummaryRaw, {
    source: sqlManifestPath,
    translation: {
      original: staticSummaryRaw,
      structured: [
        "database=tmdsDatabaseStatic",
        `exports=${staticExports.length}`,
        `rows=${sumRowCounts(staticExports)}`,
        `bitAssignments=${toNumber(staticExportByName.get("code_bit_lookup")?.row_count)}`,
        `stations=${toNumber(staticExportByName.get("code_station_context")?.row_count)}`,
        `signals=${toNumber(staticExportByName.get("signal_detail_full")?.row_count)}`,
        `tracks=${toNumber(staticExportByName.get("track_detail_full")?.row_count)}`,
        `switches=${toNumber(staticExportByName.get("switch_detail_full")?.row_count)}`,
      ],
      english: [
        "Static SQL is the naming and assignment layer for the current build.",
        "This is where codelines, stations, components, routes, and bit assignments are grounded.",
      ],
      unresolved: ["Static row presence does not authorize guessed semantics for unresolved token families."],
    },
    workflow: {
      summary: "The static TMDS catalog defines what objects exist and what lookup-driven naming the build can rely on.",
      currentStep: "Static catalog loaded",
      systems: ["TMDS SQL static"],
      objects: ["Bit assignments", "Stations", "Signals", "Tracks", "Switches"],
      knownState: "Grounded static lookup layer present",
      unresolved: ["Some component-reference families remain unresolved even though the owning rows are known."],
    },
    genisysContext: ["Static SQL provides the target naming layer that live Genisys decode must eventually resolve into."],
    icdContext: ["ICD message catalogs do not drive this line; this line is static SQL only."],
    databaseContext: [
      `Manifest: ${sqlManifestPath}`,
      `component_lookup=${toNumber(staticExportByName.get("component_lookup")?.row_count)}`,
      `code_assignment_context=${toNumber(staticExportByName.get("code_assignment_context")?.row_count)}`,
      `route_context=${toNumber(staticExportByName.get("route_context")?.row_count)}`,
    ],
    sourceReferences: [
      createSourceRecord("generated:sql-manifest", "generated", "SQL foundation manifest", sqlManifestPath, "Source of exported SQL foundation datasets."),
      createSourceRecord("database:code-bit-lookup", "database", "Static code bit lookup export", staticExportByName.get("code_bit_lookup")?.json_path ?? "", "Direct bit-to-name lookup export."),
      createSourceRecord("database:code-station-context", "database", "Static code station context export", staticExportByName.get("code_station_context")?.json_path ?? "", "Station-level scope and addressing."),
    ].filter((record) => record.path),
  });

  const dynamicSummaryRaw =
    `TMDS DYNAMIC SQL foundation: exports=${dynamicExports.length}, rows=${sumRowCounts(dynamicExports)}, ` +
    `active trains=${toNumber(dynamicExportByName.get("active_train_context")?.row_count)}, locomotive runtime=${toNumber(dynamicExportByName.get("locomotive_runtime_context")?.row_count)}, ` +
    `OS events=${toNumber(dynamicExportByName.get("os_event_context")?.row_count)}, bulletins=${toNumber(dynamicExportByName.get("bulletin_detail_full")?.row_count)}, authorities=${toNumber(dynamicExportByName.get("authority_detail_full")?.row_count)}.`;
  pushFoundationLine(lines, lineDetails, dynamicSummaryRaw, {
    source: sqlManifestPath,
    translation: {
      original: dynamicSummaryRaw,
      structured: [
        "database=tmdsDatabaseDynamic",
        `exports=${dynamicExports.length}`,
        `rows=${sumRowCounts(dynamicExports)}`,
        `activeTrains=${toNumber(dynamicExportByName.get("active_train_context")?.row_count)}`,
        `runtimeRows=${toNumber(dynamicExportByName.get("locomotive_runtime_context")?.row_count)}`,
        `osEvents=${toNumber(dynamicExportByName.get("os_event_context")?.row_count)}`,
        `bulletins=${toNumber(dynamicExportByName.get("bulletin_detail_full")?.row_count)}`,
        `authorities=${toNumber(dynamicExportByName.get("authority_detail_full")?.row_count)}`,
      ],
      english: [
        "Dynamic SQL is the runtime context layer for the current build.",
        "This is where active trains, locomotive state, authorities, bulletins, and OS events become observable workflow context.",
      ],
      unresolved: ["Dynamic context does not by itself decode unresolved low-level wayside token families."],
    },
    workflow: {
      summary: "Dynamic TMDS rows explain current or recent operating state around trains, restrictions, and movement events.",
      currentStep: "Dynamic runtime context loaded",
      systems: ["TMDS SQL dynamic", "BOS", "MDM"],
      objects: ["Active trains", "Locomotive runtime", "OS events", "Authorities", "Bulletins"],
      knownState: "Grounded runtime context present",
      unresolved: ["Runtime joins remain partial and must stay exact-match only where proven."],
    },
    genisysContext: ["Dynamic SQL provides the operating context that can be attached after a low-level wayside decode is grounded."],
    icdContext: ["Dynamic BOS and locomotive runtime rows are the local bridge into ICD version and message-family context."],
    databaseContext: [
      `Manifest: ${sqlManifestPath}`,
      `active_train_context=${toNumber(dynamicExportByName.get("active_train_context")?.row_count)}`,
      `locomotive_runtime_context=${toNumber(dynamicExportByName.get("locomotive_runtime_context")?.row_count)}`,
      `bos_emp_messages=${toNumber(dynamicExportByName.get("bos_emp_messages")?.row_count)}`,
    ],
    sourceReferences: [
      createSourceRecord("database:active-train-context", "database", "Dynamic active train context export", dynamicExportByName.get("active_train_context")?.json_path ?? "", "Active-train workflow context."),
      createSourceRecord("database:runtime-context", "database", "Dynamic locomotive runtime context export", dynamicExportByName.get("locomotive_runtime_context")?.json_path ?? "", "Locomotive runtime and version context."),
      createSourceRecord("database:os-event-context", "database", "Dynamic OS event context export", dynamicExportByName.get("os_event_context")?.json_path ?? "", "Current OS-event chronology."),
    ].filter((record) => record.path),
  });

  const normalizedSummaryRaw =
    `Normalized TMDS foundation: outputs=${normalizedManifest.length}, reference families=${toNumber(normalizedByName.get("reference_family_summary")?.row_count)}, ` +
    `station foundation=${toNumber(normalizedByName.get("station_foundation_summary")?.row_count)}, train runtime=${toNumber(normalizedByName.get("train_runtime_foundation_summary")?.row_count)}, ` +
    `signal family=${toNumber(normalizedByName.get("signal_family_foundation_summary")?.row_count)}, track family=${toNumber(normalizedByName.get("track_family_foundation_summary")?.row_count)}.`;
  pushFoundationLine(lines, lineDetails, normalizedSummaryRaw, {
    source: normalizedManifestPath,
    translation: {
      original: normalizedSummaryRaw,
      structured: [
        `outputs=${normalizedManifest.length}`,
        `referenceFamilies=${toNumber(normalizedByName.get("reference_family_summary")?.row_count)}`,
        `stationFoundation=${toNumber(normalizedByName.get("station_foundation_summary")?.row_count)}`,
        `trainRuntime=${toNumber(normalizedByName.get("train_runtime_foundation_summary")?.row_count)}`,
        `signalFamily=${toNumber(normalizedByName.get("signal_family_foundation_summary")?.row_count)}`,
        `trackFamily=${toNumber(normalizedByName.get("track_family_foundation_summary")?.row_count)}`,
      ],
      english: [
        "Normalized outputs are the app-ready summaries built from the raw SQL exports.",
      ],
      unresolved: ["Normalization strengthens structure and counts, but it does not authorize guessed semantics."],
    },
    workflow: {
      summary: "The normalized layer condenses raw SQL into reusable slices for station, train, signal, track, switch, CP, and workflow analysis.",
      currentStep: "Normalized layer loaded",
      systems: ["Foundation normalizer"],
      objects: ["Station summaries", "Reference families", "Workflow slices"],
      knownState: "Reusable grounded summaries present",
      unresolved: ["Any unresolved family in this layer must stay unresolved in the UI."],
    },
    genisysContext: ["Genisys-related normalized outputs stay tied to SQL-backed assignment and candidate-family evidence."],
    icdContext: ["ICD-related normalized outputs stay separate from SQL translation outputs."],
    databaseContext: [`Manifest: ${normalizedManifestPath}`, `Stats: ${statsPath}`],
    sourceReferences: [
      createSourceRecord("generated:normalized-manifest", "generated", "TMDS foundation manifest", normalizedManifestPath, "App-ready normalized outputs."),
      createSourceRecord("generated:foundation-stats", "generated", "TMDS foundation stats", statsPath, "Roll-up metrics across normalized outputs."),
    ],
  });

  const musicSummaryRaw =
    `MUSIC inventory: declared roots=${topLevelMusicSources.length}, discovered records=${musicSources.length}, ${groupCountSummary(musicSources, (row) => row.kind)}.`;
  pushFoundationLine(lines, lineDetails, musicSummaryRaw, {
    source: musicInventoryPath,
    translation: {
      original: musicSummaryRaw,
      structured: [
        `declaredRoots=${topLevelMusicSources.length}`,
        `discoveredRecords=${musicSources.length}`,
        `kindBreakdown=${groupCountSummary(musicSources, (row) => row.kind)}`,
      ],
      english: [
        "The MUSIC folder is a first-class local evidence source for manuals, workflow decks, and real operational log bundles.",
        "This build is constrained to those local sources plus TMDS static and dynamic SQL exports.",
      ],
      unresolved: ["Inventory proves source presence, not semantic meaning, until a document or log is extracted and grounded."],
    },
    workflow: {
      summary: "MUSIC contributes manuals, workflow/training decks, and real local log bundles that can back SQL-grounded interpretation.",
      currentStep: "Local source inventory loaded",
      systems: ["Music folder", "Training decks", "Local log bundles"],
      objects: topLevelMusicSources.slice(0, 5).map((row) => row.title),
      knownState: "Permitted local source families inventoried",
      unresolved: ["Some extracted manuals and image-heavy documents still need additional grounding before they can drive semantics."],
    },
    genisysContext: ["Genisys manuals, trace analysis, and local log bundles all live under the MUSIC inventory."],
    icdContext: ["ICD PDFs and training extracts also come from the MUSIC inventory and remain separate from SQL-only evidence."],
    databaseContext: ["This line constrains which local non-SQL sources are allowed to feed the build."],
    sourceReferences: topLevelMusicSources.slice(0, 8).map(toMusicSourceRecord),
  });

  const subdivisionSummaryRaw = subdivisionSummary
    .map((row) => `${row.subdivision_name}: lines=${row.code_line_numbers}, signals=${toNumber(row.signal_count)}, tracks=${toNumber(row.track_count)}, switches=${toNumber(row.switch_count)}, misc=${toNumber(row.misc_device_count)}, routes=${toNumber(row.route_count)}`)
    .join(" | ");
  pushFoundationLine(lines, lineDetails, subdivisionSummaryRaw, {
    source: subdivisionSummaryPath,
    translation: {
      original: subdivisionSummaryRaw,
      structured: subdivisionSummary.map((row) => `${row.subdivision_name}|lines=${row.code_line_numbers}|signals=${toNumber(row.signal_count)}|tracks=${toNumber(row.track_count)}|switches=${toNumber(row.switch_count)}|routes=${toNumber(row.route_count)}`),
      english: ["The current build has a grounded subdivision split with distinct live asset counts and code-line scope."],
      unresolved: ["Subdivision scope does not by itself decode unresolved bit-level token families."],
    },
    workflow: {
      summary: "Subdivision scope determines which stations, assets, and line groups are relevant to the selected runtime context.",
      currentStep: "Subdivision summary loaded",
      systems: ["TMDS static"],
      objects: subdivisionSummary.map((row) => row.subdivision_name),
      knownState: "Subdivision counts grounded from SQL",
      unresolved: ["Any workflow meaning beyond row counts still needs supporting runtime or local-document evidence."],
    },
    genisysContext: ["Escondido live lines remain the active Genisys-backed station set in the current local SQL export."],
    icdContext: ["ICD message catalogs do not drive subdivision counts."],
    databaseContext: subdivisionSummary.map((row) => `${row.subdivision_name}: lines=${row.code_line_numbers}; signals=${toNumber(row.signal_count)}; tracks=${toNumber(row.track_count)}; switches=${toNumber(row.switch_count)}; routes=${toNumber(row.route_count)}`),
    sourceReferences: [
      createSourceRecord("generated:subdivision-summary", "generated", "Subdivision protocol summary", subdivisionSummaryPath, "Live subdivision counts derived from static SQL."),
    ],
  });

  const referenceSummaryRaw =
    `Reference status: total=${getMetric(stats, "component_reference_total")}, resolved=${getMetric(stats, "component_reference_resolved")}, ` +
    `unresolved=${getMetric(stats, "component_reference_unresolved")}, default_zero=${getMetric(stats, "component_reference_default_zero")}, ` +
    `top unresolved=${topUnresolvedFamilies}.`;
  pushFoundationLine(lines, lineDetails, referenceSummaryRaw, {
    source: statsPath,
    translation: {
      original: referenceSummaryRaw,
      structured: [
        `total=${getMetric(stats, "component_reference_total")}`,
        `resolved=${getMetric(stats, "component_reference_resolved")}`,
        `unresolved=${getMetric(stats, "component_reference_unresolved")}`,
        `defaultZero=${getMetric(stats, "component_reference_default_zero")}`,
        `topFamilies=${topUnresolvedFamilies}`,
      ],
      english: ["The main foundation gap is token interpretation inside already-confirmed tables, not table discovery."],
      unresolved: ["High-volume unresolved families must stay structural until repeated assignment-backed evidence promotes them."],
    },
    workflow: {
      summary: "Reference resolution status is the main readiness indicator for how far the build can go beyond structure into translation.",
      currentStep: "Reference-status summary loaded",
      systems: ["Foundation normalizer", "TMDS static"],
      objects: ["Component references", "Reference families"],
      knownState: "Resolution counts grounded",
      unresolved: ["Unresolved families remain concentrated in switch, track, signal, and CP structural surfaces."],
    },
    genisysContext: ["Unresolved reference families are the bottleneck between low-level payloads and final engineer-readable naming."],
    icdContext: ["ICD catalogs do not resolve low-level wayside reference families."],
    databaseContext: [`Stats: ${statsPath}`, `Reference summary: ${referenceSummaryPath}`],
    sourceReferences: [
      createSourceRecord("generated:foundation-stats-ref", "generated", "TMDS foundation stats", statsPath, "Component-reference resolution metrics."),
      createSourceRecord("generated:reference-family-summary", "generated", "Reference family summary", referenceSummaryPath, "Highest-volume unresolved family ranking."),
    ],
  });

  const trainRuntimeSummaryRaw =
    `Train/runtime join status: exact=${trainRuntimeStatus.get("exact_engine_to_loco_match") ?? 0}, ` +
    `engine without runtime=${trainRuntimeStatus.get("engine_id_without_runtime_match") ?? 0}, no engine ID=${trainRuntimeStatus.get("no_engine_id") ?? 0}.`;
  pushFoundationLine(lines, lineDetails, trainRuntimeSummaryRaw, {
    source: trainRuntimeJoinPath,
    translation: {
      original: trainRuntimeSummaryRaw,
      structured: trainRuntimeJoinSummary.map((row) => `${row.runtime_join_status}=${toNumber(row.row_count)}`),
      english: [
        "Train/runtime linkage is exact-match only and must stay exact-match only.",
      ],
      unresolved: ["Unmatched train rows still need additional local evidence before any broader runtime join is safe."],
    },
    workflow: {
      summary: "Active-train and locomotive-runtime joins are useful workflow context, but only where exact local identifiers match.",
      currentStep: "Train/runtime join summary loaded",
      systems: ["TMDS dynamic", "BOS runtime"],
      objects: ["Active trains", "Locomotive runtime"],
      knownState: "Exact-match train/runtime joins grounded",
      unresolved: ["Most active train rows do not carry a safe locomotive-runtime join."],
    },
    genisysContext: ["This line is runtime-context only; it does not assign any Genisys payload semantics."],
    icdContext: ["Runtime rows are where local ICD interface versions can be attached once the engine/runtime join is safe."],
    databaseContext: [`Join summary: ${trainRuntimeJoinPath}`],
    sourceReferences: [
      createSourceRecord("generated:train-runtime-join", "generated", "Train runtime join summary", trainRuntimeJoinPath, "Exact-match runtime join status."),
      createSourceRecord("generated:train-runtime-foundation", "generated", "Train runtime foundation summary", foundationPath("exports", "normalized", "train_runtime_foundation_summary.json"), "Per-train runtime summary."),
    ],
  });

  const session = {
    lines,
    detail: lines[0] ? lineDetails[lines[0].id] : null,
    lineDetails,
  };
  reportProgress?.({
    phase: "complete",
    message: `finalizing foundation summary (${lines.length} lines)`,
    percent: 97,
    completed: lines.length,
    total: Math.max(lines.length, 1),
  });
  return session;
}

function detectHostCarrier(row: HostInventoryRow): string {
  const haystack = [
    row.notes,
    row.dnet_connection_type,
    row.dnet_lte_status,
    row.model_name,
    row.name,
    row.location_name,
  ]
    .join(" ")
    .toUpperCase();
  if (haystack.includes("VERIZON")) {
    return "VERIZON";
  }
  if (haystack.includes("AT&T") || haystack.includes("ATT")) {
    return "AT&T";
  }
  return "";
}

function escapeRegExpText(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function normalizeDisplayText(value: string | undefined | null): string {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function formatCodeLineLabel(row: CodeLineProtocolRow): string {
  return row.code_line_name;
}

function stationReferenceContext(
  stationRow: StationFoundationRow,
  assignmentRow: CodeStationAssignmentMapRow | null,
): string[] {
  const out = [
    "Station / asset map:",
    `${stationRow.station_name} / control point ${stationRow.control_point_number} (${stationRow.control_point_name})`,
    `Subdivision: ${stationRow.subdivision_name}`,
    `Code line ${stationRow.code_line_number}: ${stationRow.code_line_name}`,
    `Station inventory: signals=${toNumber(stationRow.signal_count)}, tracks=${toNumber(stationRow.track_count)}, switches=${toNumber(stationRow.switch_count)}, routes=${toNumber(stationRow.route_count)}`,
  ];
  if (assignmentRow) {
    out.push(...formatAssignmentCatalog("Indication bits:", assignmentRow.indication_assignments));
    out.push(...formatAssignmentCatalog("Control bits:", assignmentRow.control_assignments));
  }
  return out;
}

function referenceSource(category: string): string {
  return `reference:${category}`;
}

function formatReferenceAssignmentCatalog(title: string, entries: CodeAssignmentEntry[]): string[] {
  const namedEntries = entries.filter((entry) => !isBlankAssignment(entry));
  return [
    title,
    `${entries.length} bits total, ${namedEntries.length} assigned`,
    ...namedEntries.map((entry) => `${entry.bit_position}. ${entry.mnemonic} = ${describeAssignmentLongName(entry)}`),
  ];
}

function indentReferenceLines(lines: string[], prefix = "  "): string[] {
  return lines.map((line) => (line ? `${prefix}${line}` : ""));
}

function stationReferenceLibraryContext(
  stationRow: StationFoundationRow,
  assignmentRow: CodeStationAssignmentMapRow | null,
): string[] {
  const out = [
    `Control point UID: ${stationRow.control_point_number}`,
    ...(normalizeLookupKey(stationRow.control_point_name) !== normalizeLookupKey(stationRow.station_name)
      ? [`Control point name: ${stationRow.control_point_name}`]
      : []),
    `Code line ${stationRow.code_line_number}: ${stationRow.code_line_name}`,
    `Inventory: signals=${toNumber(stationRow.signal_count)}, tracks=${toNumber(stationRow.track_count)}, switches=${toNumber(stationRow.switch_count)}, routes=${toNumber(stationRow.route_count)}`,
  ];
  if (assignmentRow) {
    out.push(...formatReferenceAssignmentCatalog("Indication bits:", assignmentRow.indication_assignments));
    out.push(...formatReferenceAssignmentCatalog("Control bits:", assignmentRow.control_assignments));
  }
  return out;
}

function formatMilepostLabel(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "";
  }
  return value.toFixed(4).replace(/\.?0+$/, "");
}

function getStationReferenceMilepost(
  stationRow: StationFoundationRow,
  controlPointMilepostByNumber: Map<string, number> | null | undefined,
): number | null {
  if (!controlPointMilepostByNumber) {
    return null;
  }
  return controlPointMilepostByNumber.get(normalizeLookupKey(stationRow.control_point_number)) ?? null;
}

function buildStationReferenceChoiceLabel(
  stationRow: StationFoundationRow,
  controlPointMilepostByNumber?: Map<string, number> | null,
): string {
  const milepost = getStationReferenceMilepost(stationRow, controlPointMilepostByNumber);
  const stationLabel = normalizeDisplayText(stationRow.station_name) || normalizeDisplayText(stationRow.control_point_name) || "Control point";
  const parts = [
    ...(milepost !== null ? [`MP ${formatMilepostLabel(milepost)}`] : []),
    stationLabel,
  ];
  return parts.join(" - ");
}

function buildCodeLineReferenceGroupLabel(
  row: CodeLineProtocolRow,
  stations: StationFoundationRow[],
  controlPointMilepostByNumber?: Map<string, number> | null,
): string {
  const mileposts = stations
    .map((station) => getStationReferenceMilepost(station, controlPointMilepostByNumber))
    .filter((value): value is number => value !== null)
    .sort((left, right) => left - right);
  if (!mileposts.length) {
    return `Code line ${row.code_line_number} - ${row.code_line_name}`;
  }
  return `Code line ${row.code_line_number} - ${row.code_line_name} - MP ${formatMilepostLabel(mileposts[0])} to ${formatMilepostLabel(mileposts[mileposts.length - 1])}`;
}

function buildTrainNetworkGroupContext(trainKey: string, rows: HostInventoryRow[]): string[] {
  const groupedRows = [...rows].sort((a, b) => a.name.localeCompare(b.name) || a.primary_ip.localeCompare(b.primary_ip));
  const out: string[] = [];
  groupedRows.forEach((row, index) => {
    const label = buildTrainInventoryChoiceLabel(row, trainKey);
    out.push(label);
    out.push(...buildTrainInventoryReferenceContext(row, trainKey));
    if (index < groupedRows.length - 1) {
      out.push("");
    }
  });
  return out;
}

function buildPacketSwitchGroupContext(group: {
  name: string;
  ip: string;
  roles: Set<string>;
  codeLines: Set<string>;
  subdivisions: Set<string>;
}): string[] {
  return [
    `IP: ${group.ip}`,
    `Role(s): ${Array.from(group.roles).sort().join(", ")}`,
    `Subdivision scope: ${Array.from(group.subdivisions).sort().join(", ") || "not shown"}`,
    `Code lines: ${Array.from(group.codeLines).sort().join(", ") || "not shown"}`,
  ];
}

function buildFieldNetworkGroupContext(groupName: string, rows: HostInventoryRow[]): string[] {
  void groupName;
  void rows;
  return [];
}

type NetworkLocationDisplayInfo = {
  label: string;
  compactLabel: string;
  base: string;
  mp: number | null;
};

function getNetworkLocationDisplayInfo(locationName: string): NetworkLocationDisplayInfo {
  const location = normalizeDisplayText(locationName);
  if (!location) {
    return {
      label: "",
      compactLabel: "",
      base: "",
      mp: null,
    };
  }
  const trimmed = location
    .replace(/^San Diego Subdivision - /i, "")
    .replace(/^Escondido Subdivision - /i, "")
    .trim();
  const cpMatch = /^MP\s+([0-9.]+)\s+-\s+CP\s+(.+)$/i.exec(trimmed);
  if (cpMatch) {
    return {
      label: `MP ${cpMatch[1].trim()} - ${cpMatch[2].trim()}`,
      compactLabel: `MP ${cpMatch[1].trim()} - ${cpMatch[2].trim()}`,
      base: cpMatch[2].trim(),
      mp: Number(cpMatch[1]),
    };
  }
  const mpMatch = /^MP\s+([0-9.]+)\s+-\s+(.+)$/i.exec(trimmed);
  if (mpMatch) {
    return {
      label: `MP ${mpMatch[1].trim()} - ${mpMatch[2].trim()}`,
      compactLabel: `MP ${mpMatch[1].trim()} - ${mpMatch[2].trim()}`,
      base: mpMatch[2].trim(),
      mp: Number(mpMatch[1]),
    };
  }
  const remoteCaseMatch = /^(.+?)\s+Remote Case$/i.exec(trimmed);
  if (remoteCaseMatch) {
    return {
      label: `${remoteCaseMatch[1].trim()} - Remote Case`,
      compactLabel: `${remoteCaseMatch[1].trim()} - Remote Case`,
      base: remoteCaseMatch[1].trim(),
      mp: null,
    };
  }
  const facilityMatch = /^([A-Z]{2,8})\s+-\s+(.+)$/i.exec(trimmed);
  if (facilityMatch) {
    return {
      label: trimmed,
      compactLabel: facilityMatch[1].trim().toUpperCase(),
      base: facilityMatch[1].trim().toUpperCase(),
      mp: null,
    };
  }
  return {
    label: trimmed,
    compactLabel: trimmed,
    base: trimmed,
    mp: null,
  };
}

function shortenNetworkLocationLabel(locationName: string): string {
  return getNetworkLocationDisplayInfo(locationName).label;
}

function stripInventoryHostnameDomain(value: string): string {
  return normalizeDisplayText(value).replace(/\.[A-Za-z0-9.-]+$/, "").trim();
}

function stripFieldNetworkInventoryPrefixes(value: string): string {
  let out = stripInventoryHostnameDomain(value);
  let previous = "";
  while (out && out !== previous) {
    previous = out;
    out = out
      .replace(/^(?:PTC[-\s]+)?(?:GAO|SOF|SDSUB|ESUB|NCTD|NCTC|MTSOL|OTC|MOW|SMMF)[-\s]*/i, "")
      .replace(/^(?:MP[-\s]*)?\d+(?:\.\d+)?[-\s]*/i, "")
      .replace(/^[-\s]+/, "")
      .trim();
  }
  return out;
}

function normalizeFieldNetworkFamilyToken(token: string): string {
  return token
    .toUpperCase()
    .replace(/^[^A-Z0-9]+/, "")
    .replace(/\d+[A-Z]*$/, "")
    .trim();
}

function deriveFieldNetworkSourceTitle(value: string): string | null {
  const normalized = stripFieldNetworkInventoryPrefixes(value);
  if (!normalized) {
    return null;
  }
  const dashedDescriptorMatch = /^(.+?)\s-\s.+$/.exec(normalized);
  if (dashedDescriptorMatch) {
    return dashedDescriptorMatch[1].trim();
  }
  const numericSuffixMatch = /^([A-Za-z]+(?:-[A-Za-z]+)*)[-_]?\d+[A-Za-z]*$/i.exec(normalized);
  if (numericSuffixMatch) {
    return normalizeFieldNetworkFamilyToken(numericSuffixMatch[1]);
  }
  const segmentedMatch = /^([A-Za-z]{2,})(?:[-_][A-Za-z0-9]+)+$/i.exec(normalized);
  if (segmentedMatch && segmentedMatch[1].length <= 5) {
    return normalizeFieldNetworkFamilyToken(segmentedMatch[1]);
  }
  return normalized;
}

function extractFieldNetworkFamilyToken(value: string): string | null {
  const normalized = stripFieldNetworkInventoryPrefixes(value);
  if (!normalized) {
    return null;
  }
  const ignoredTokens = new Set([
    "AAA",
    "COM",
    "LOCAL",
    "PROD",
  ]);
  const tokens = normalized.split(/[^A-Za-z0-9]+/).filter(Boolean);
  for (const token of tokens) {
    const normalizedToken = normalizeFieldNetworkFamilyToken(token);
    if (!normalizedToken || ignoredTokens.has(normalizedToken) || normalizedToken.length < 2) {
      continue;
    }
    return normalizedToken;
  }
  return null;
}

function stripFieldNetworkFamilyPrefix(label: string, groupName: string): string {
  const normalized = stripFieldNetworkInventoryPrefixes(label);
  if (!normalized) {
    return "";
  }
  const familyPattern = escapeRegExpText(groupName).replace(/\\ /g, "[-\\s]*");
  return normalized
    .replace(new RegExp(familyPattern, "ig"), "")
    .replace(/^[-\s:]+/, "")
    .replace(/[-\s:]+$/, "")
    .trim();
}

function buildFieldNetworkEndpointChoiceLabel(
  groupName: string,
  row: HostInventoryRow,
  duplicateCounts: Map<string, number>,
): string {
  const locationInfo = getNetworkLocationDisplayInfo(row.location_name);
  const location = locationInfo.label;
  const compactLocation = locationInfo.compactLabel || location;
  const name = stripFieldNetworkFamilyPrefix(normalizeDisplayText(row.name), groupName);
  const hostname = stripFieldNetworkFamilyPrefix(normalizeDisplayText(row.hostname), groupName);
  const ip = normalizeDisplayText(row.primary_ip);
  const locationKey = location || "Location not shown";
  if (location && (duplicateCounts.get(locationKey) ?? 0) <= 1) {
    return location;
  }
  let suffix = name || hostname;
  const locationMp = /MP\s+([0-9.]+)/i.exec(location)?.[1];
  if (suffix && locationMp) {
    suffix = suffix.replace(new RegExp(`^${escapeRegExpText(locationMp)}[-\\s]*`, "i"), "").trim();
  }
  if (suffix && location && normalizeLookupKey(location).includes(normalizeLookupKey(suffix))) {
    suffix = "";
  }
  if (compactLocation && suffix && normalizeLookupKey(suffix) !== normalizeLookupKey(compactLocation)) {
    return `${compactLocation} - ${suffix}`;
  }
  if (location && ip) {
    return `${location} - ${ip}`;
  }
  return location || compactLocation || suffix || ip || normalizeDisplayText(row.hostname) || normalizeDisplayText(row.name) || groupName;
}

function buildFieldNetworkDetailChoiceGroups(groupName: string, rows: HostInventoryRow[]) {
  const groupedRows = [...rows];
  const labelCounts = new Map<string, number>();
  const baseMileposts = new Map<string, number>();
  for (const row of groupedRows) {
    const locationInfo = getNetworkLocationDisplayInfo(row.location_name);
    const locationKey = locationInfo.label || "Location not shown";
    labelCounts.set(locationKey, (labelCounts.get(locationKey) ?? 0) + 1);
    if (locationInfo.base && locationInfo.mp !== null) {
      const current = baseMileposts.get(locationInfo.base);
      if (current === undefined || locationInfo.mp < current) {
        baseMileposts.set(locationInfo.base, locationInfo.mp);
      }
    }
  }
  const items = groupedRows.map((row, index) => ({
    id: `${normalizeLookupKey(groupName)}:${normalizeLookupKey(row.location_name || row.hostname || row.primary_ip)}:${index}`,
    label: buildFieldNetworkEndpointChoiceLabel(groupName, row, labelCounts),
    content: buildFieldInventoryReferenceContext(groupName, row),
    sortKey: (() => {
      const info = getNetworkLocationDisplayInfo(row.location_name);
      if (info.mp !== null) {
        return info.mp;
      }
      if (info.base && baseMileposts.has(info.base)) {
        return (baseMileposts.get(info.base) ?? 0) + 0.001;
      }
      return Number.MAX_SAFE_INTEGER;
    })(),
  })).sort((left, right) =>
    left.sortKey - right.sortKey
    || left.label.localeCompare(right.label)
    || left.id.localeCompare(right.id),
  ).map(({ sortKey, ...item }) => item);
  return [
    {
      id: `field-endpoints:${normalizeLookupKey(groupName)}`,
      label: "",
      layout: "horizontal" as const,
      selectionMode: "multiple" as const,
      items,
    },
  ];
}

function getFieldNetworkFamilyLabel(row: HostInventoryRow): string {
  const typeName = normalizeDisplayText(row.configuration_type_name);
  const normalizedType = normalizeLookupKey(typeName);
  const genericTypes = new Set(["VIRTUAL SERVER", "SERVER", "OTHER"]);
  if (normalizedType && !genericTypes.has(normalizedType)) {
    if (normalizedType === "BASE RADIO" && /\bPTC RADIO\b/i.test(`${row.name} ${row.hostname}`)) {
      return "PTC Radio";
    }
    return typeName;
  }
  return (
    deriveFieldNetworkSourceTitle(row.name)
    || deriveFieldNetworkSourceTitle(row.hostname)
    || extractFieldNetworkFamilyToken(row.name)
    || extractFieldNetworkFamilyToken(row.hostname)
    || typeName
    || stripInventoryHostnameDomain(row.name)
    || stripInventoryHostnameDomain(row.hostname)
    || "Network device"
  );
}

function buildWorkflowReferenceEntries(): Array<{
  title: string;
  summary: string;
  steps: Array<{
    text: string;
    rawExamples: string[];
    notes?: string[];
  }>;
}> {
  return [
    {
      title: "TMDS field indication flow",
      summary: "Field side to dispatcher side indication flow.",
      steps: [
        {
          text: "Field -> code line: a field device changes state and the field sends an INDICATION word.",
          rawExamples: [
            "INDICATION;27:200027:01:101010101010000000000000000100110000000001000000 FOR CODESTATION:YARD",
            "INDICATION;14:200009:01:000000000000000000000000111000110000000000000000 FOR CODESTATION:MELROSE",
          ],
        },
        {
          text: "Code server -> TMDS/CAD: the indication word is received for the target station and word.",
          rawExamples: [
            "W << 172.20.20.61 DATA: INDICATION;27:200027:01:101010101010000000000000000100110000000001000000 clsClientC, SendData, 295:",
          ],
        },
        {
          text: "TMDS/CAD -> dispatcher view: the returned indication word is decoded into active bits and displayed.",
          rawExamples: [
            "SIGNAL INDICATION RECEIVED. (UPDATE STATUS:CLEARED|REQUESTEDCLEAR|REQUESTINGCLEAR)(NAME:E-SIGNAL)(UID:200251)(CP:AVENUE)",
          ],
        },
        {
          text: "TMDS/CAD -> field: when a refresh is needed, TMDS sends RECALL for the station word.",
          rawExamples: [
            "RECALL SENT:PALOMAR:1",
          ],
        },
        {
          text: "Field -> code line: the field returns a fresh indication snapshot back after the recall.",
          rawExamples: [
            "W << INDICATION;21:200015:01:000000000000000000000000000000000000000000000000 FOR CODESTATION:PALOMAR",
          ],
        },
      ],
    },
    {
      title: "TMDS field control flow",
      summary: "Dispatcher side to field side control flow and returned indication confirmation flow.",
      steps: [
        {
          text: "Dispatcher -> TMDS/CAD: a control request is issued.",
          rawExamples: [
            "CONTROL SERVER MESSAGE FROM:CTRL-01A Message=CNTRLPT;|100|100023|SROFF|FDFEB13D-79B2-4814-961B-E660BD2A8D5C|100394|100907|0;TRANSACTION-100-0132",
          ],
        },
        {
          text: "TMDS/CAD -> code server: the request is handed to the code-server path.",
          rawExamples: [
            "ControlServerStreamReceiverData:CNTRLPT;|100|100023|SROFF|FDFEB13D-79B2-4814-961B-E660BD2A8D5C|100394|100907|0;TRANSACTION-100-0131",
          ],
        },
        {
          text: "Code server: the command is queued for the target station / control point.",
          rawExamples: [
            "QueueTheCommand:EL CAMINO:RECALL",
            "ProcessSendQueue-CommandCount1",
          ],
        },
        {
          text: "Code server -> field: the send queue transmits the command.",
          rawExamples: [
            "ProcessSendQueueEL CAMINORECALL",
            "SendCommand:EL CAMINO:RECALL",
          ],
        },
        {
          text: "Field -> code server: if checkback is used, the field echoes the control-side step back.",
          rawExamples: [
            "ControlServerStreamReceiverData:SIGNAL;|100|100394|RC;TRANSACTION-100-0133",
          ],
        },
        {
          text: "Field -> TMDS/CAD: the resulting indication word returns and confirms the actual field state after the command was sent.",
          rawExamples: [
            "W << INDICATION;27:200027:01:101010101010000000000000000100110000000001000000 FOR CODESTATION:YARD",
          ],
        },
      ],
    },
    {
      title: "CAD / code-server session flow",
      summary: "Connection maintenance and processing-cycle flow between CAD/control-server side and the code-server side.",
      steps: [
        {
          text: "CAD/control-server side -> code server: KEEPALIVE checks that the connection is still up.",
          rawExamples: [
            "ControlServerStreamReceiverData:KEEPALIVE",
            "W >> 172.20.20.61 DATA:  KEEPALIVE  clsClientC,StreamRece,  349:",
          ],
        },
        {
          text: "Code server -> CAD/control-server side: ALIVE replies that the connection is still up.",
          rawExamples: [
            "W << 172.20.20.61 DATA: ALIVE  clsClientC,  SendData,  295:",
          ],
        },
        {
          text: "Code server -> CAD/control-server side: queue-count messages report pending work still waiting.",
          rawExamples: [
            "MESSAGE QUEUE FROM CODE SERVER:CODE-01A Remote IP Address 172.20.20.52 (COUNT:1)",
          ],
        },
        {
          text: "Code server -> CAD/control-server side: thread-alive messages report that indication processing is still running.",
          rawExamples: [
            "INDICATION-PROCESSING-THREAD-ALIVE: CODE-01A Remote IP Address 172.20.20.52",
          ],
        },
        {
          text: "Code server -> CAD/control-server side: indication-message-complete marks the end of one indication-processing cycle and the return side finished that pass.",
          rawExamples: [
            "INDICATION MESSAGE COMPLETE:CODE-01A",
          ],
        },
      ],
    },
    {
      title: "Dispatch / office / locomotive train flow",
      summary: "Office-to-locomotive and locomotive-to-office message flow for registration, poll, consist, bulletin, system state, and position.",
      steps: [
        {
          text: "Locomotive -> office: 02020 Poll Registration = locomotive registers for a subdivision / district.",
          rawExamples: [
            "<<RECV>> 2020 (Poll Registration) ... <Loco_ID=SCAX   692> <Train_ID=M605> <PTC_Subdivision/District_ID=100>",
          ],
          notes: [
            "Flow detail: Locomotive starts the subdivision / district registration exchange.",
            "Expected office reply: 01020 Confirmation of Poll Registration.",
          ],
        },
        {
          text: "Office -> locomotive: 01020 Confirmation of Poll Registration = office confirms that registration.",
          rawExamples: [
            "<<SEND>> 1020 (Confirmation of Poll Registration) ... <Reason_for_Sending=1 - Confirm receipt of valid poll registration>",
          ],
          notes: [
            "Flow detail: Office confirms the locomotive's 02020 Poll Registration message.",
          ],
        },
        {
          text: "Office -> locomotive: 01021 Office Segment Poll = office sends current subdivision / district poll state and dataset context.",
          rawExamples: [
            "<<SEND>> 1021 (Office Segment Poll) ... <PTC_Subdivision/District_ID[1]=100> <Office_state[1]=1 - Explicit>",
          ],
          notes: [
            "Flow detail: Office pushes subdivision / district state, CRC context, and poll state to the locomotive.",
          ],
        },
        {
          text: "Locomotive -> office: 02030 Request Train Consist = locomotive asks for consist data.",
          rawExamples: [],
          notes: [
            "ICD relationship: Office responds with 01030 Train Consist.",
            "Local trace status: no direct BOS send / receive sample was found in the local logs checked.",
          ],
        },
        {
          text: "Office -> locomotive: 01030 Train Consist = office sends consist data back.",
          rawExamples: [],
          notes: [
            "ICD relationship: Sent in response to 02030 Request Train Consist or unsolicited when the office has new consist data.",
            "Expected locomotive confirmation: 02031 Confirmation of Train Consist.",
            "Local trace status: no direct BOS send / receive sample was found in the local logs checked.",
          ],
        },
        {
          text: "Locomotive -> office: 02031 Confirmation of Train Consist = locomotive confirms receipt of consist data.",
          rawExamples: [],
          notes: [
            "ICD relationship: Confirms receipt of an unsolicited 01030 Train Consist message.",
            "Local trace status: no direct BOS send / receive sample was found in the local logs checked.",
          ],
        },
        {
          text: "Locomotive -> office: 02041 Request Bulletin Dataset = locomotive asks for bulletin data.",
          rawExamples: [],
          notes: [
            "ICD relationship: Sent after 01022 Current Dataset List when the locomotive needs bulletin datasets.",
            "Expected office reply: one or more 01041 Bulletin Dataset messages.",
            "Local trace status: no direct BOS send / receive sample was found in the local logs checked.",
          ],
        },
        {
          text: "Office -> locomotive: 01041 Bulletin Dataset = office sends bulletin data back.",
          rawExamples: [],
          notes: [
            "ICD relationship: Sent in response to 02041 Request Bulletin Dataset or unsolicited when the office has new bulletin data.",
            "Expected locomotive confirmation: 02042 Confirmation of Bulletin Dataset.",
            "MIB status: EV101 Bulletin Dataset (01041) is marked Not Constructed in the local TMDS-BOS-MIB files.",
            "Local trace status: no direct BOS send / receive sample was found in the local logs checked.",
          ],
        },
        {
          text: "Locomotive -> office: 02042 Confirmation of Bulletin Dataset = locomotive confirms receipt of bulletin data.",
          rawExamples: [],
          notes: [
            "ICD relationship: Confirms receipt of 01041 Bulletin Dataset.",
            "Local trace status: no direct BOS send / receive sample was found in the local logs checked.",
          ],
        },
        {
          text: "Locomotive -> office: 02010 Locomotive System State = locomotive reports operating/system state.",
          rawExamples: [
            "<<RECV>> 2010 (Locomotive System State) ... <Current_Locomotive_State=2 - Initializing> <Locomotive_State_Summary=2 - Non-controlling>",
          ],
          notes: [
            "Expected office follow-up: 01010 Command/Confirm Locomotive System State when the office confirms receipt.",
          ],
        },
        {
          text: "Office -> locomotive: 01010 Command/Confirm Locomotive System State = office commands or confirms that system-state exchange.",
          rawExamples: [
            "<<SEND>> 1010 (Command/Confirm Locomotive System State) ... <Reason_for_Sending=2 - Confirm receipt of 02010 message>",
          ],
          notes: [
            "Flow detail: Office can use this message as the confirmation back to the locomotive after 02010.",
          ],
        },
        {
          text: "Office -> locomotive: 01080 Request Locomotive Position Report = office asks for current train position.",
          rawExamples: [
            "<<SEND>> 1080 (Request Locomotive Position Report) ... <Action_Requested=1 - Send position report only>",
          ],
          notes: [
            "Expected locomotive reply: 02080 Locomotive Position Report.",
          ],
        },
        {
          text: "Locomotive -> office: 02080 Locomotive Position Report = locomotive returns track, direction, position, and state back to the office.",
          rawExamples: [
            "<<RECV>> 2080 (Locomotive Position Report) ... <Head_End_Track_name=MT1> <Reason_for_Report=0 - Requested By Office>",
          ],
          notes: [
            "Flow detail: This is the locomotive response back to the office after 01080.",
          ],
        },
      ],
    },
  ];
}

function describeWorkflowEvidenceLabel(stepText: string, example: string, exampleIndex: number): string {
  const step = stepText.toLowerCase();
  const sample = example.toLowerCase();
  const sent = /<<send>>|w\s*>>|sendcommand|recall sent|processsendqueue/.test(sample);
  const received = /<<recv>>|w\s*<</.test(sample) || /\breceived\b/.test(sample);

  if (/queue/.test(step) || /message queue/.test(sample)) {
    return "Queue status";
  }
  if (/thread-alive|keepalive|alive\b/.test(step) || /thread-alive|keepalive|alive\b/.test(sample)) {
    if (sent || /checks that the connection is still up/.test(step)) {
      return "Sent health check";
    }
    if (received || /repl(y|ies)/.test(step)) {
      return "Received health reply";
    }
    return "Health status";
  }
  if (/complete/.test(step) || /message complete/.test(sample)) {
    return "Completion marker";
  }
  if (/request|asks for|recall/.test(step)) {
    return sent ? "Sent request" : received ? "Received request" : "Request evidence";
  }
  if (/confirm|confirmation|acknowledg/.test(step)) {
    return sent ? "Sent confirmation" : received ? "Received confirmation" : "Confirmation evidence";
  }
  if (/indication/.test(step)) {
    return sent ? "Sent indication" : received ? "Received indication" : "Indication evidence";
  }
  if (/control|command/.test(step)) {
    return sent ? "Sent control" : received ? "Received control" : "Control evidence";
  }
  if (/position/.test(step)) {
    return sent ? "Sent position message" : received ? "Received position report" : "Position evidence";
  }
  if (/state/.test(step)) {
    return sent ? "Sent state message" : received ? "Received state report" : "State evidence";
  }
  return sent ? "Sent sample" : received ? "Received sample" : `Grounded sample ${exampleIndex + 1}`;
}

function buildGenisysReferenceEntries(): Array<{
  code: string;
  title: string;
  meaning: string;
  badges?: string[];
  notes?: string[];
  examples?: string[];
}> {
  return [
    {
      code: "FA",
      title: "Office acknowledge",
      meaning: "Office acknowledgement sent to the field.",
      badges: ["Office -> field", "Ack"],
      examples: [
        "Office send: FA 09 82 A6 F6",
        "Field reply: F1 09 F6",
      ],
    },
    {
      code: "FB",
      title: "Office poll",
      meaning: "Office poll asking the field for current indication status.",
      badges: ["Office -> field", "Poll"],
      examples: [
        "Office send: FB 06 C3 32 F6",
        "Field reply example: F1 06 F6",
      ],
    },
    {
      code: "FC",
      title: "Office control",
      meaning: "Office control command sent toward the field.",
      badges: ["Office -> field", "Control"],
      examples: [
        "Office send: FC OF 00 25 01 00 02 00 EO 07 FO 08 A5 F6",
        "Field checkback: F3 OF 00 25 01 00 02 00 EO 07 C8 95 F6",
      ],
    },
    {
      code: "FD",
      title: "Office recall",
      meaning: "Office recall requesting a full indication snapshot from the field.",
      badges: ["Office -> field", "Recall"],
      examples: [
        "Office send: FD OB 01 57 F6",
        "Field reply: F2 OB 00 9A 01 90 02 04 03 00 04 00 05 00 06 00 07 00 21 A4 F6",
      ],
    },
    {
      code: "FE",
      title: "Office execute",
      meaning: "Office execute step used after control checkback when that mode is enabled.",
      badges: ["Office -> field", "Execute"],
      examples: [
        "Office send: FE OF 00 64 F6",
        "Field reply: F2 OF 00 09 01 02 02 00 03 00 04 00 05 00 02 8E F6",
      ],
    },
    {
      code: "F1",
      title: "Field acknowledgement / no-data response",
      meaning: "Field response with no indication payload data.",
      badges: ["Field -> office", "Ack"],
      examples: [
        "Field send: F1 09 F6",
      ],
    },
    {
      code: "F2",
      title: "Field indication",
      meaning: "Field response carrying indication data.",
      badges: ["Field -> office", "Indication"],
      examples: [
        "Field send: F2 OB 00 9A 01 90 02 04 03 00 04 00 05 00 06 00 07 00 21 A4 F6",
        "Office acknowledgement after indication: FA 09 82 A6 F6",
      ],
    },
    {
      code: "F3",
      title: "Field control checkback",
      meaning: "Field control response used when control checkback is enabled.",
      badges: ["Field -> office", "Checkback"],
      examples: [
        "Field send: F3 OF 00 25 01 00 02 00 EO 07 C8 95 F6",
        "Office follow-up: FE OF 00 64 F6",
      ],
    },
    {
      code: "F6",
      title: "End of message",
      meaning: "Message terminator for Genisys frames.",
      badges: ["Frame terminator"],
      examples: [
        "Example frame ending: FA 09 82 A6 F6",
      ],
    },
    {
      code: "F0",
      title: "Escaped high-byte marker",
      meaning: "Marks a split data byte when the original data value would otherwise fall in the reserved F0-FF range.",
      badges: ["Escaping"],
      notes: [
        "Example: FF data is sent as F0 0F.",
      ],
      examples: [
        "Control trace example: FC OF 00 25 01 00 02 00 EO 07 FO 08 A5 F6",
      ],
    },
    {
      code: "FF",
      title: "Undefined / not allowed",
      meaning: "FF is not used as a valid standalone Genisys message byte.",
      badges: ["Reserved"],
      notes: [
        "The local Genisys trace notes say FF is undefined and not allowed.",
      ],
    },
    {
      code: "E0 07",
      title: "Mode bytes: non-secure polls + control checkback",
      meaning: "Initialization/mode setting where non-secure polling is used and control checkback is enabled.",
      badges: ["Mode bytes"],
      examples: [
        "Control trace carries mode bytes: EO 07",
      ],
    },
    {
      code: "E0 05",
      title: "Mode bytes: non-secure polls + no checkback",
      meaning: "Initialization/mode setting where non-secure polling is used and control checkback is not used.",
      badges: ["Mode bytes"],
      notes: [
        "No direct E0 05 trace example was found in the local Genisys trace manual.",
      ],
    },
  ];
}

function normalizeManualText(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function extractTrainInventoryKey(row: HostInventoryRow): string | null {
  const combined = `${row.name} ${row.location_name}`.trim();
  const trainMatch = /\bNCTC\s*(\d{3,4})\b/i.exec(combined);
  if (trainMatch) {
    return `NCTC ${trainMatch[1]}`;
  }
  if (/HI-RAIL/i.test(combined)) {
    return "Hi-Rail";
  }
  return null;
}

function buildTrainInventoryChoiceLabel(row: HostInventoryRow, trainKey: string): string {
  let label = normalizeDisplayText(row.name);
  if (label) {
    if (normalizeLookupKey(trainKey) === "HI RAIL") {
      label = label.replace(/^HI[-\s]?RAIL\s*/i, "");
    } else {
      const flexibleTrainKey = escapeRegExpText(trainKey).replace(/\\ /g, "\\s+");
      label = label.replace(new RegExp(`^${flexibleTrainKey}\\s*`, "i"), "");
    }
    label = label.replace(/^VEHICLE\s*-\s*/i, "").replace(/^[-:]+/, "").trim();
  }
  return label || normalizeDisplayText(row.configuration_type_name) || normalizeDisplayText(row.hostname) || normalizeDisplayText(row.primary_ip) || trainKey;
}

function formatTrainInventoryLocationSuffix(locationName: string, trainKey: string): string {
  const trimmedLocation = String(locationName ?? "").trim();
  if (!trimmedLocation) {
    return "";
  }
  const normalizedLocation = normalizeLookupKey(trimmedLocation);
  const normalizedTrainKey = normalizeLookupKey(trainKey);
  if (normalizedTrainKey && normalizedLocation.includes(normalizedTrainKey)) {
    return "";
  }
  return ` ${trimmedLocation}`;
}

function buildTrainInventoryReferenceContext(row: HostInventoryRow, trainKey: string): string[] {
  const location = normalizeDisplayText(row.location_name);
  const carrier = detectHostCarrier(row);
  const hostname = normalizeDisplayText(row.hostname);
  const configurationType = normalizeDisplayText(row.configuration_type_name);
  return [
    `IP: ${normalizeDisplayText(row.primary_ip) || "not shown"}`,
    ...(carrier ? [`Carrier / provider: ${carrier}`] : []),
    ...(configurationType && normalizeLookupKey(configurationType) !== normalizeLookupKey(buildTrainInventoryChoiceLabel(row, trainKey))
      ? [`Type: ${configurationType}`]
      : []),
    ...(location && !normalizeLookupKey(location).includes(normalizeLookupKey(trainKey)) ? [`Location: ${location}`] : []),
    ...(hostname && !normalizeLookupKey(hostname).includes(normalizeLookupKey(trainKey)) ? [`Hostname: ${hostname}`] : []),
    ...(normalizeDisplayText(row.manufacturer_name) ? [`Manufacturer: ${normalizeDisplayText(row.manufacturer_name)}`] : []),
    ...(normalizeDisplayText(row.model_name) ? [`Model: ${normalizeDisplayText(row.model_name)}`] : []),
    ...(normalizeDisplayText(row.dnet_connection_type) ? [`Connection type: ${normalizeDisplayText(row.dnet_connection_type)}`] : []),
    ...(normalizeDisplayText(row.dnet_lte_status) ? [`LTE status: ${normalizeDisplayText(row.dnet_lte_status)}`] : []),
  ];
}

function buildFieldInventoryChoiceLabel(row: HostInventoryRow): string {
  const ip = normalizeDisplayText(row.primary_ip);
  const host = normalizeDisplayText(row.hostname);
  const location = normalizeDisplayText(row.location_name);
  const fallback = normalizeDisplayText(row.configuration_type_name) || normalizeDisplayText(row.name) || "Network item";
  if (ip && host) {
    return `${ip} / ${host}`;
  }
  if (ip && location) {
    return `${ip} / ${location}`;
  }
  return ip || host || location || fallback;
}

function buildFieldInventoryReferenceContext(groupName: string, row: HostInventoryRow): string[] {
  const carrier = detectHostCarrier(row);
  const location = shortenNetworkLocationLabel(row.location_name);
  const configurationType = normalizeDisplayText(row.configuration_type_name);
  return [
    `IP: ${normalizeDisplayText(row.primary_ip) || "not shown"}`,
    ...(configurationType && normalizeLookupKey(configurationType) !== normalizeLookupKey(groupName)
      ? [`Type: ${configurationType}`]
      : []),
    ...(carrier ? [`Carrier / provider: ${carrier}`] : []),
    ...(location ? [`Location: ${location}`] : []),
    ...(normalizeDisplayText(row.hostname) ? [`Hostname: ${normalizeDisplayText(row.hostname)}`] : []),
    ...(normalizeDisplayText(row.manufacturer_name) ? [`Manufacturer: ${normalizeDisplayText(row.manufacturer_name)}`] : []),
    ...(normalizeDisplayText(row.model_name) ? [`Model: ${normalizeDisplayText(row.model_name)}`] : []),
    ...(normalizeDisplayText(row.dnet_connection_type) ? [`Connection type: ${normalizeDisplayText(row.dnet_connection_type)}`] : []),
    ...(normalizeDisplayText(row.dnet_lte_status) ? [`LTE status: ${normalizeDisplayText(row.dnet_lte_status)}`] : []),
  ];
}

function describeTrainMessageDirection(messageId: string): string {
  const normalizedMessageId = formatTrainMessageId(messageId);
  if (/^01\d{3}$/.test(normalizedMessageId)) return "Office to locomotive";
  if (/^02\d{3}$/.test(normalizedMessageId)) return "Locomotive to office";
  if (/^03\d{3}$/.test(normalizedMessageId)) return "On-board data distribution";
  return "Direction not grounded";
}

function describeTrainMessageFlow(messageId: string): string {
  const normalizedMessageId = formatTrainMessageId(messageId);
  if (/^01\d{3}$/.test(normalizedMessageId)) return "Office -> Locomotive";
  if (/^02\d{3}$/.test(normalizedMessageId)) return "Locomotive -> Office";
  if (/^03\d{3}$/.test(normalizedMessageId)) return "On-board data distribution";
  return "Direction not grounded";
}

function formatTrainMessageId(messageId: string): string {
  const trimmed = String(messageId ?? "").trim();
  return /^\d{1,5}$/.test(trimmed) ? trimmed.padStart(5, "0") : trimmed;
}

type TrainMessageDefinition = {
  messageId: string;
  messageName: string;
  release: string;
  version: string;
  description: string;
};

type TrainMessageTroubleshootingFacts = {
  triggeredBy: Set<string>;
  confirmsReceiptOf: Set<string>;
  expectedReplies: Set<string>;
  expectedConfirmations: Set<string>;
  canBeUnsolicited: boolean;
};

type BounceDiagramField = {
  name: string;
  size: string;
  type: string;
  description: string;
};

type BounceDiagramMessage = {
  messageId: string;
  title: string;
  section: string;
  direction: string;
  description: string;
  fields: BounceDiagramField[];
  previousMessageId?: string;
  nextMessageId?: string;
};

function decodeJavascriptStringLiteral(value: string): string {
  return value
    .replace(/\\\\/g, "\\")
    .replace(/\\"/g, "\"")
    .replace(/\\'/g, "'")
    .replace(/\\n/g, " ")
    .replace(/\\r/g, " ")
    .replace(/\\t/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function decodeHtmlText(value: string): string {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, "\"")
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeBounceDiagramDirection(directionClass: string): string {
  if (directionClass.includes("to-right-wsrs")) return "Locomotive -> WSRS";
  if (directionClass.includes("to-left-wsrs")) return "WSRS -> Locomotive";
  if (directionClass.includes("to-right")) return "Locomotive -> Office";
  if (directionClass.includes("to-left")) return "Office -> Locomotive";
  return "Direction not grounded";
}

function getBounceDiagramLanes(direction: string): { fromLane: string; toLane: string } {
  if (direction === "Office -> Locomotive") {
    return { fromLane: "Office", toLane: "Locomotive" };
  }
  if (direction === "Locomotive -> Office") {
    return { fromLane: "Locomotive", toLane: "Office" };
  }
  if (direction === "Locomotive -> WSRS") {
    return { fromLane: "Locomotive", toLane: "WSRS" };
  }
  if (direction === "WSRS -> Locomotive") {
    return { fromLane: "WSRS", toLane: "Locomotive" };
  }
  return { fromLane: "Unknown", toLane: "Unknown" };
}

function buildBounceDiagramReferenceDiagram(messages: BounceDiagramMessage[]) {
  return {
    kind: "message-exchange" as const,
    title: "ITC Office-Locomotive Segment Bounce Diagram",
    subtitle: "Structural flow grounded from the local office-locomotive chart and MSRP message references",
    lanes: ["Office", "Locomotive", "WSRS"],
    steps: messages.map((message, index) => {
      const lanes = getBounceDiagramLanes(message.direction);
      return {
        id: `bounce-step:${index + 1}:${message.messageId}`,
        messageId: message.messageId,
        title: message.title,
        section: message.section,
        direction: message.direction,
        fromLane: lanes.fromLane,
        toLane: lanes.toLane,
        description: message.description || undefined,
        previousMessageId: message.previousMessageId,
        nextMessageId: message.nextMessageId,
      };
    }),
  };
}

function parseBounceDiagramMessages(html: string): BounceDiagramMessage[] {
  if (!html.trim()) {
    return [];
  }

  const sectionEntries: Array<{ messageId: string; title: string; section: string; direction: string }> = [];
  let currentSection = "Unsectioned";
  const sectionRegex = /<h2>([^<]+)<\/h2>|<div class="arrow ([^"]+)"><span class="msg-text" onclick="showDetails\('(\d+)', '([^']+)'\)">/g;
  for (const match of html.matchAll(sectionRegex)) {
    if (match[1]) {
      currentSection = decodeHtmlText(match[1]);
      continue;
    }
    const directionClass = match[2] ?? "";
    const messageId = match[3]?.trim();
    const title = match[4] ? decodeHtmlText(match[4]) : "";
    if (!messageId || !title) {
      continue;
    }
    sectionEntries.push({
      messageId,
      title,
      section: currentSection,
      direction: normalizeBounceDiagramDirection(directionClass),
    });
  }

  const metadataById = new Map<string, { description: string; fields: BounceDiagramField[] }>();
  const metadataRegex = /"(\d{5})":\s*\{\s*description:\s*"((?:\\.|[^"\\])*)",\s*fields:\s*\[((?:.|\r?\n)*?)\]\s*\}/g;
  for (const match of html.matchAll(metadataRegex)) {
    const messageId = match[1];
    const description = decodeJavascriptStringLiteral(match[2] ?? "");
    const fieldsBlock = match[3] ?? "";
    const fields: BounceDiagramField[] = [];
    const fieldRegex = /\{\s*name:\s*"((?:\\.|[^"\\])*)",\s*size:\s*"((?:\\.|[^"\\])*)",\s*type:\s*"((?:\\.|[^"\\])*)",\s*desc:\s*"((?:\\.|[^"\\])*)"\s*\}/g;
    for (const fieldMatch of fieldsBlock.matchAll(fieldRegex)) {
      fields.push({
        name: decodeJavascriptStringLiteral(fieldMatch[1] ?? ""),
        size: decodeJavascriptStringLiteral(fieldMatch[2] ?? ""),
        type: decodeJavascriptStringLiteral(fieldMatch[3] ?? ""),
        description: decodeJavascriptStringLiteral(fieldMatch[4] ?? ""),
      });
    }
    metadataById.set(messageId, { description, fields });
  }

  const messages = sectionEntries.map((entry, index) => {
    const sectionMessages = sectionEntries.filter((candidate) => candidate.section === entry.section);
    const sectionIndex = sectionMessages.findIndex((candidate) => candidate.messageId === entry.messageId && candidate.title === entry.title);
    const metadata = metadataById.get(entry.messageId);
    return {
      ...entry,
      description: metadata?.description ?? "",
      fields: metadata?.fields ?? [],
      previousMessageId: sectionIndex > 0 ? sectionMessages[sectionIndex - 1]?.messageId : undefined,
      nextMessageId: sectionIndex >= 0 && sectionIndex < sectionMessages.length - 1 ? sectionMessages[sectionIndex + 1]?.messageId : undefined,
    };
  });

  return messages;
}

function buildBounceDiagramReferenceContext(
  messageId: string,
  chartMessagesById: Map<string, BounceDiagramMessage>,
  trainCatalogById: Map<string, { name: string; releases: Set<string>; versions: Set<string> }>,
): string[] {
  const message = chartMessagesById.get(messageId);
  if (!message) {
    return [];
  }

  const lines = [
    `Bounce diagram section: ${message.section}`,
    `Bounce diagram title: ${message.title}`,
    `Bounce diagram direction: ${message.direction}`,
  ];
  if (message.previousMessageId) {
    lines.push(`Previous diagram step: ${formatTrainMessageReference(message.previousMessageId, trainCatalogById)}`);
  }
  if (message.nextMessageId) {
    lines.push(`Next diagram step: ${formatTrainMessageReference(message.nextMessageId, trainCatalogById)}`);
  }
  if (message.description) {
    lines.push(`Diagram summary: ${message.description}`);
  }
  return lines;
}

function extractTrainMessageDefinitions(
  rawText: string,
  release: string,
): TrainMessageDefinition[] {
  const normalizedLines = rawText.split(/\r?\n/).map((rawLine) =>
    normalizeManualText(
      rawLine
        .replace(/\u00a0/g, " ")
        .replace(/[\u2013\u2014]/g, " - ")
        .replace(/\s*[.]{3,}\s*\d+\s*$/, ""),
    ),
  );
  const candidates: TrainMessageDefinition[] = [];

  for (let index = 0; index < normalizedLines.length; index += 1) {
    const line = normalizedLines[index];
    if (!line) {
      continue;
    }

    const headingMatch = /^(?:\d+(?:\.\d+)*\s+)?(?:L\d+I\d+\s+)?\((\d{5})\)\s+(.+?)\s*-\s*Version\s+(\d+)\b/.exec(line);
    if (!headingMatch) {
      continue;
    }

    const [, messageId, messageName, version] = headingMatch;
    const body: string[] = [];
    for (let innerIndex = index + 1; innerIndex < Math.min(index + 140, normalizedLines.length); innerIndex += 1) {
      const bodyLine = normalizedLines[innerIndex];
      if (/^(?:\d+(?:\.\d+)*\s+)?(?:L\d+I\d+\s+)?\((\d{5})\)\s+.+?\s*-\s*Version\s+\d+\b/.test(bodyLine)) {
        break;
      }
      body.push(bodyLine);
    }

    const descriptionIndex = body.findIndex((bodyLine) => bodyLine.startsWith("Description:"));
    const descriptionParts: string[] = [];
    if (descriptionIndex >= 0) {
      descriptionParts.push(body[descriptionIndex].replace(/^Description:\s*/, ""));
      for (let bodyIndex = descriptionIndex + 1; bodyIndex < body.length; bodyIndex += 1) {
        const bodyLine = body[bodyIndex];
        if (/^(Functional Content:|\[Constraints\]|\[Unique error handling requirements\]|\[Design notes\]|Field Size )/.test(bodyLine)) {
          break;
        }
        if (
          bodyLine &&
          !/^PTC Office-Locomotive Segment ICD/.test(bodyLine) &&
          !/^I-ETMS/.test(bodyLine) &&
          !/^Release \d/.test(bodyLine) &&
          !/^Distribution limited/.test(bodyLine) &&
          !/^-?\d+-?$/.test(bodyLine) &&
          !/^Rev\./.test(bodyLine)
        ) {
          descriptionParts.push(bodyLine);
        }
      }
    }

    candidates.push({
      messageId,
      messageName: messageName.trim(),
      release,
      version,
      description: normalizeManualText(descriptionParts.join(" ")),
    });
  }

  const bestByReleaseAndId = new Map<string, TrainMessageDefinition>();
  for (const candidate of candidates) {
    const key = `${candidate.release}:${candidate.messageId}`;
    const existing = bestByReleaseAndId.get(key);
    if (!existing || candidate.description.length > existing.description.length) {
      bestByReleaseAndId.set(key, candidate);
    }
  }

  return Array.from(bestByReleaseAndId.values());
}

function classifyTrainMessageSentence(
  sentence: string,
): "triggered_by" | "confirms_receipt_of" | "expected_reply" | "expected_confirmation" | null {
  const normalizedSentence = normalizeManualText(sentence).toLowerCase();
  if (!normalizedSentence.includes("(") && !/\b\d{5}\b/.test(normalizedSentence)) {
    return null;
  }
  if (normalizedSentence.includes("responds with")) {
    return "expected_reply";
  }
  if (normalizedSentence.includes("acknowledges receipt")) {
    return "expected_confirmation";
  }
  if (/confirms receipt[^.]{0,120}with/.test(normalizedSentence)) {
    return "expected_confirmation";
  }
  if (
    normalizedSentence.includes("confirm receipt of") ||
    normalizedSentence.includes("confirm the receipt of") ||
    normalizedSentence.includes("confirming the receipt of") ||
    normalizedSentence.includes("confirming receipt of")
  ) {
    return "confirms_receipt_of";
  }
  if (
    normalizedSentence.includes("sent in response to") ||
    normalizedSentence.includes("in response to receiving") ||
    normalizedSentence.includes("upon receipt of") ||
    normalizedSentence.includes("in response to the receipt of") ||
    normalizedSentence.includes("pattern described in the")
  ) {
    return "triggered_by";
  }
  if (
    normalizedSentence.includes("to request ") ||
    normalizedSentence.includes("request the issuance of")
  ) {
    return "expected_reply";
  }
  return null;
}

function extractTrainMessageReferenceIds(sentence: string): string[] {
  const ids = new Set<string>();
  for (const match of sentence.matchAll(/(?:\(\s*|\b)(\d{5})(?:\s*\)|\b)/g)) {
    ids.add(match[1]);
  }
  return Array.from(ids);
}

function createTrainMessageTroubleshootingFacts(): TrainMessageTroubleshootingFacts {
  return {
    triggeredBy: new Set<string>(),
    confirmsReceiptOf: new Set<string>(),
    expectedReplies: new Set<string>(),
    expectedConfirmations: new Set<string>(),
    canBeUnsolicited: false,
  };
}

function formatTrainMessageReference(
  messageId: string,
  trainCatalogById: Map<string, { name: string; releases: Set<string>; versions: Set<string> }>,
  includeFlow = false,
): string {
  const normalizedMessageId = formatTrainMessageId(messageId);
  const row = trainCatalogById.get(messageId);
  const label = row?.name ? `${normalizedMessageId} ${row.name}` : normalizedMessageId;
  return includeFlow ? `${label} (${describeTrainMessageFlow(normalizedMessageId)})` : label;
}

function appendTrainMessageRelationBlock(
  lines: string[],
  label: string,
  messageIds: Iterable<string>,
  trainCatalogById: Map<string, { name: string; releases: Set<string>; versions: Set<string> }>,
): void {
  const sortedIds = Array.from(new Set(messageIds)).sort((left, right) => left.localeCompare(right));
  if (!sortedIds.length) {
    return;
  }
  lines.push(`${label}:`);
  for (const messageId of sortedIds) {
    lines.push(`  ${formatTrainMessageReference(messageId, trainCatalogById)}`);
  }
}

function buildTrainMessageReferenceContext(
  messageId: string,
  trainCatalogById: Map<string, { name: string; releases: Set<string>; versions: Set<string> }>,
  trainFactsById: Map<string, TrainMessageTroubleshootingFacts>,
  reverseRepliesById: Map<string, Set<string>>,
  reverseConfirmationsById: Map<string, Set<string>>,
  reverseTriggeredByExpectedReplyById: Map<string, Set<string>>,
  reverseConfirmedByExpectedConfirmationById: Map<string, Set<string>>,
): string[] {
  const facts = trainFactsById.get(messageId) ?? createTrainMessageTroubleshootingFacts();
  const lines = [`Message flow: ${describeTrainMessageFlow(messageId)}`];
  const sentAfter = new Set<string>([
    ...facts.triggeredBy,
    ...(reverseTriggeredByExpectedReplyById.get(messageId) ?? new Set<string>()),
  ]);
  const confirms = new Set<string>([
    ...facts.confirmsReceiptOf,
    ...(reverseConfirmedByExpectedConfirmationById.get(messageId) ?? new Set<string>()),
  ]);
  const repliesToExpect = new Set<string>([
    ...facts.expectedReplies,
    ...(reverseRepliesById.get(messageId) ?? new Set<string>()),
  ]);
  const confirmationsToExpect = new Set<string>([
    ...facts.expectedConfirmations,
    ...(reverseConfirmationsById.get(messageId) ?? new Set<string>()),
  ]);

  appendTrainMessageRelationBlock(
    lines,
    "This message is sent after",
    sentAfter,
    trainCatalogById,
  );
  appendTrainMessageRelationBlock(
    lines,
    "This message confirms receipt of",
    confirms,
    trainCatalogById,
  );
  appendTrainMessageRelationBlock(
    lines,
    "When this message is sent, the other side replies with",
    repliesToExpect,
    trainCatalogById,
  );
  appendTrainMessageRelationBlock(
    lines,
    "After this message is sent, the other side confirms with",
    confirmationsToExpect,
    trainCatalogById,
  );

  if (facts.canBeUnsolicited) {
    lines.push("May also be sent without a matching request.");
  }

  if (lines.length === 1) {
    lines.push("No direct request, reply, or confirmation pair was grounded in the local ICD text.");
  }

  return lines;
}

function buildTrainMessageDisplayOrder(
  trainCatalogById: Map<string, { name: string; releases: Set<string>; versions: Set<string> }>,
  trainFactsById: Map<string, TrainMessageTroubleshootingFacts>,
): string[] {
  const allMessageIds = Array.from(trainCatalogById.keys()).sort((left, right) => toNumber(left) - toNumber(right));
  const outgoing = new Map<string, Set<string>>();
  const undirected = new Map<string, Set<string>>();
  const indegree = new Map<string, number>();

  const ensureNode = (messageId: string): void => {
    if (!outgoing.has(messageId)) {
      outgoing.set(messageId, new Set<string>());
    }
    if (!undirected.has(messageId)) {
      undirected.set(messageId, new Set<string>());
    }
    if (!indegree.has(messageId)) {
      indegree.set(messageId, 0);
    }
  };

  const addEdge = (sourceMessageId: string, targetMessageId: string): void => {
    if (sourceMessageId === targetMessageId || !trainCatalogById.has(sourceMessageId) || !trainCatalogById.has(targetMessageId)) {
      return;
    }
    ensureNode(sourceMessageId);
    ensureNode(targetMessageId);
    const sourceBucket = outgoing.get(sourceMessageId)!;
    if (!sourceBucket.has(targetMessageId)) {
      sourceBucket.add(targetMessageId);
      indegree.set(targetMessageId, (indegree.get(targetMessageId) ?? 0) + 1);
    }
    undirected.get(sourceMessageId)!.add(targetMessageId);
    undirected.get(targetMessageId)!.add(sourceMessageId);
  };

  for (const messageId of allMessageIds) {
    ensureNode(messageId);
  }

  for (const [messageId, facts] of trainFactsById.entries()) {
    ensureNode(messageId);
    facts.expectedReplies.forEach((targetMessageId) => addEdge(messageId, targetMessageId));
    facts.expectedConfirmations.forEach((targetMessageId) => addEdge(messageId, targetMessageId));
    facts.triggeredBy.forEach((sourceMessageId) => addEdge(sourceMessageId, messageId));
    facts.confirmsReceiptOf.forEach((sourceMessageId) => addEdge(sourceMessageId, messageId));
  }

  const visitedComponents = new Set<string>();
  const orderedMessageIds: string[] = [];

  for (const seedMessageId of allMessageIds) {
    if (visitedComponents.has(seedMessageId)) {
      continue;
    }

    const component = new Set<string>();
    const stack = [seedMessageId];
    while (stack.length) {
      const currentMessageId = stack.pop()!;
      if (component.has(currentMessageId)) {
        continue;
      }
      component.add(currentMessageId);
      visitedComponents.add(currentMessageId);
      const neighbors = Array.from(undirected.get(currentMessageId) ?? []).sort((left, right) => toNumber(left) - toNumber(right));
      neighbors.forEach((neighbor) => {
        if (!component.has(neighbor)) {
          stack.push(neighbor);
        }
      });
    }

    const componentIds = Array.from(component).sort((left, right) => toNumber(left) - toNumber(right));
    const componentVisited = new Set<string>();
    const visit = (messageId: string): void => {
      if (componentVisited.has(messageId) || !component.has(messageId)) {
        return;
      }
      componentVisited.add(messageId);
      orderedMessageIds.push(messageId);
      const nextMessages = Array.from(outgoing.get(messageId) ?? [])
        .filter((candidateId) => component.has(candidateId))
        .sort((left, right) => toNumber(left) - toNumber(right));
      nextMessages.forEach((nextMessageId) => visit(nextMessageId));
    };

    const roots = componentIds.filter((messageId) => (indegree.get(messageId) ?? 0) === 0 && (outgoing.get(messageId)?.size ?? 0) > 0);
    roots.forEach((messageId) => visit(messageId));
    componentIds.forEach((messageId) => visit(messageId));
  }

  return orderedMessageIds;
}

async function buildReferenceLibrarySession(reportProgress?: ProgressReporter): Promise<SessionData> {
  reportProgress?.({
    phase: "prepare",
    message: "loading reference library",
    percent: 5,
    completed: 0,
    total: 7,
  });

  reportProgress?.({
    phase: "prepare",
    message: "loading reference datasets",
    percent: 35,
    completed: 1,
    total: 7,
  });

  const localMessageExchangeChartPath = "C:\\Users\\Ji\\Music\\Message exchange.html";
  const packagedMessageExchangeChartPath = foundationPath("reference_assets", "Message exchange.html");
  const [bundle, stationRows, officeIcd211Text, officeIcd31Text, dataDistributionText, bounceDiagramSource] = await Promise.all([
    loadLogEnrichment(),
    readJsonFile<StationFoundationRow[]>("exports", "normalized", "station_foundation_summary.json"),
    readFile(foundationPath("exports", "manuals", "icd_training", "Office-Locomotive_Segment_ICD__2.11.1_.txt"), "utf-8"),
    readFile(foundationPath("exports", "manuals", "icd_training", "WCR-ICD-1214_Office-Locomotive_Segment_ICD__3.1_.txt"), "utf-8"),
    readFile(foundationPath("exports", "manuals", "icd_training", "I-ETMS_On-Board_Data_Distribution_Messages_ICD_v1.1.txt"), "utf-8"),
    readFirstAvailableTextFile([packagedMessageExchangeChartPath, localMessageExchangeChartPath]),
  ]);
  const bounceDiagramHtml = bounceDiagramSource.text;
  const messageExchangeChartPath = bounceDiagramSource.path;

  reportProgress?.({
    phase: "detail",
    message: "building reference sections",
    percent: 60,
    completed: 3,
    total: 7,
  });

  const lines: ParsedLine[] = [];
  const lineDetails: Record<string, DetailModel> = {};
  const enrichment = bundle;

  const codeLineRows = enrichment
    ? Array.from(enrichment.codeLineByNumber.values())
        .filter((row) =>
          row.subdivision_names &&
          normalizeLookupKey(row.normal_codeserver_name) !== "TEST",
        )
        .sort((a, b) => toNumber(a.code_line_number) - toNumber(b.code_line_number))
    : [];
  const assignmentRowsByCp = enrichment ? enrichment.assignmentByKey : new Map<string, CodeStationAssignmentMapRow>();
  const controlPointMilepostByNumber = enrichment?.controlPointMilepostByNumber ?? new Map<string, number>();
  const hostRows = enrichment
    ? Array.from(new Map(
        Array.from(enrichment.hostByIp.values())
          .flat()
          .map((row) => [`${row.primary_ip}|${row.name}`, row] as const),
      ).values())
        .filter((row) => row.primary_ip && row.primary_ip !== "127.0.0.1")
        .sort((a, b) => a.primary_ip.localeCompare(b.primary_ip) || a.name.localeCompare(b.name))
    : [];
  const workflowEntries = buildWorkflowReferenceEntries();
  const genisysEntries = buildGenisysReferenceEntries();
  const trainMessageDefinitions = [
    ...extractTrainMessageDefinitions(officeIcd211Text, "2.11.1"),
    ...extractTrainMessageDefinitions(officeIcd31Text, "3.1"),
    ...extractTrainMessageDefinitions(dataDistributionText, "1.1"),
  ];
  const bounceDiagramMessages = parseBounceDiagramMessages(bounceDiagramHtml);
  const bounceDiagramById = new Map<string, BounceDiagramMessage>();
  for (const message of bounceDiagramMessages) {
    if (!bounceDiagramById.has(message.messageId)) {
      bounceDiagramById.set(message.messageId, message);
    }
  }
  const trainCatalogRows = trainMessageDefinitions.map((definition) => ({
    messageId: definition.messageId,
    messageName: definition.messageName,
    release: definition.release,
    version: definition.version,
  }));
  const trainCatalogById = new Map<string, { name: string; releases: Set<string>; versions: Set<string> }>();
  for (const row of trainCatalogRows) {
    const existing = trainCatalogById.get(row.messageId) ?? {
      name: row.messageName,
      releases: new Set<string>(),
      versions: new Set<string>(),
    };
    if (!existing.name || existing.name.length > row.messageName.length) {
      existing.name = row.messageName;
    }
    existing.releases.add(row.release);
    existing.versions.add(row.version);
    trainCatalogById.set(row.messageId, existing);
  }
  for (const message of bounceDiagramMessages) {
    const cleanedTitle = message.title.replace(/\s*\(\d{5}\)\s*$/u, "").trim();
    const existing = trainCatalogById.get(message.messageId) ?? {
      name: cleanedTitle || message.messageId,
      releases: new Set<string>(),
      versions: new Set<string>(),
    };
    if (!existing.name && cleanedTitle) {
      existing.name = cleanedTitle;
    }
    trainCatalogById.set(message.messageId, existing);
  }
  const trainFactsById = new Map<string, TrainMessageTroubleshootingFacts>();
  for (const definition of trainMessageDefinitions) {
    const facts = trainFactsById.get(definition.messageId) ?? createTrainMessageTroubleshootingFacts();
    facts.canBeUnsolicited ||= /\b(?:is also|also|may be|is)?\s*sent unsolicited\b/.test(definition.description.toLowerCase());
    for (const sentence of definition.description.match(/[^.]+(?:\.|$)/g) ?? []) {
      const sentenceKind = classifyTrainMessageSentence(sentence);
      if (!sentenceKind) {
        continue;
      }
      const referencedIds = extractTrainMessageReferenceIds(sentence).filter((candidateId) => candidateId !== definition.messageId);
      if (!referencedIds.length) {
        continue;
      }
      if (sentenceKind === "triggered_by") {
        referencedIds.forEach((candidateId) => facts.triggeredBy.add(candidateId));
      } else if (sentenceKind === "confirms_receipt_of") {
        referencedIds.forEach((candidateId) => facts.confirmsReceiptOf.add(candidateId));
      } else if (sentenceKind === "expected_reply") {
        referencedIds.forEach((candidateId) => facts.expectedReplies.add(candidateId));
      } else if (sentenceKind === "expected_confirmation") {
        referencedIds.forEach((candidateId) => facts.expectedConfirmations.add(candidateId));
      }
    }
    trainFactsById.set(definition.messageId, facts);
  }
  for (const message of bounceDiagramMessages) {
    const facts = trainFactsById.get(message.messageId) ?? createTrainMessageTroubleshootingFacts();
    facts.canBeUnsolicited ||= /unsolicited/i.test(message.title) || /unsolicited/i.test(message.description);
    for (const sentence of message.description.match(/[^.]+(?:\.|$)/g) ?? []) {
      const sentenceKind = classifyTrainMessageSentence(sentence);
      if (!sentenceKind) {
        continue;
      }
      const referencedIds = extractTrainMessageReferenceIds(sentence).filter((candidateId) => candidateId !== message.messageId);
      if (!referencedIds.length) {
        continue;
      }
      if (sentenceKind === "triggered_by") {
        referencedIds.forEach((candidateId) => facts.triggeredBy.add(candidateId));
      } else if (sentenceKind === "confirms_receipt_of") {
        referencedIds.forEach((candidateId) => facts.confirmsReceiptOf.add(candidateId));
      } else if (sentenceKind === "expected_reply") {
        referencedIds.forEach((candidateId) => facts.expectedReplies.add(candidateId));
      } else if (sentenceKind === "expected_confirmation") {
        referencedIds.forEach((candidateId) => facts.expectedConfirmations.add(candidateId));
      }
    }
    trainFactsById.set(message.messageId, facts);
  }
  const reverseRepliesById = new Map<string, Set<string>>();
  const reverseConfirmationsById = new Map<string, Set<string>>();
  const reverseTriggeredByExpectedReplyById = new Map<string, Set<string>>();
  const reverseConfirmedByExpectedConfirmationById = new Map<string, Set<string>>();
  for (const [sourceMessageId, facts] of trainFactsById.entries()) {
    for (const targetMessageId of facts.triggeredBy) {
      const bucket = reverseRepliesById.get(targetMessageId) ?? new Set<string>();
      bucket.add(sourceMessageId);
      reverseRepliesById.set(targetMessageId, bucket);
    }
    for (const targetMessageId of facts.confirmsReceiptOf) {
      const bucket = reverseConfirmationsById.get(targetMessageId) ?? new Set<string>();
      bucket.add(sourceMessageId);
      reverseConfirmationsById.set(targetMessageId, bucket);
    }
    for (const targetMessageId of facts.expectedReplies) {
      const bucket = reverseTriggeredByExpectedReplyById.get(targetMessageId) ?? new Set<string>();
      bucket.add(sourceMessageId);
      reverseTriggeredByExpectedReplyById.set(targetMessageId, bucket);
    }
    for (const targetMessageId of facts.expectedConfirmations) {
      const bucket = reverseConfirmedByExpectedConfirmationById.get(targetMessageId) ?? new Set<string>();
      bucket.add(sourceMessageId);
      reverseConfirmedByExpectedConfirmationById.set(targetMessageId, bucket);
    }
  }
  const orderedTrainMessageIds = buildTrainMessageDisplayOrder(trainCatalogById, trainFactsById);

  const bounceDiagramSectionCount = new Set(
    bounceDiagramMessages
      .map((message) => message.section.trim())
      .filter(Boolean),
  ).size;

  pushFoundationLine(lines, lineDetails, "Office / locomotive message exchange diagram", {
    source: referenceSource("message-exchange"),
    translation: {
      original: "Office / locomotive message exchange diagram",
      structured: [],
      english: [
        "Sequence diagram rebuilt from the local office-locomotive chart.",
      ],
      unresolved: [],
    },
    workflow: {
      summary: "Office / locomotive bounce diagram reference.",
      currentStep: "Reference view",
      systems: ["Office", "Locomotive", "WSRS", "PTC"],
      objects: ["Message exchange diagram"],
      knownState: bounceDiagramMessages.length
        ? `${bounceDiagramMessages.length} charted messages grounded from the local diagram sources`
        : "Diagram source not loaded",
      unresolved: bounceDiagramMessages.length ? [] : ["Message exchange HTML companion was not found in the packaged assets or local Music folder."],
    },
    databaseContext: [
      "Sequence diagram view:",
      "This reference entry draws a clean sequence diagram from the local office-locomotive chart instead of embedding the raw PDF viewer.",
      ...(bounceDiagramMessages.length
        ? [
            `Grounded chart metadata: ${bounceDiagramMessages.length} messages across ${Math.max(bounceDiagramSectionCount, 1)} sections from ${messageExchangeChartPath}.`,
          ]
        : ["Diagram chart metadata is unavailable because the HTML companion was not loaded."]),
    ],
    referenceBadges: [
      "Sequence diagram",
      ...(bounceDiagramMessages.length ? [`${bounceDiagramMessages.length} charted messages`] : []),
      ...(bounceDiagramSectionCount ? [`${bounceDiagramSectionCount} sections`] : []),
    ],
    referenceDiagram: buildBounceDiagramReferenceDiagram(bounceDiagramMessages),
    sourceReferences: [
      createSourceRecord(
        "manual:message-exchange-chart",
        "manual",
        "Message exchange HTML companion",
        messageExchangeChartPath,
        "Local HTML companion used to ground message IDs, sections, and flow metadata.",
      ),
    ],
  });

  const codelineReferenceGroups = new Map<string, {
    subdivision: string;
    serverName: string;
    codeLines: CodeLineProtocolRow[];
    stationsByCodeLine: Map<string, StationFoundationRow[]>;
  }>();
  for (const row of codeLineRows) {
    const serverName = row.normal_codeserver_name;
    if (!serverName) {
      continue;
    }
    const key = normalizeLookupKey(row.subdivision_names);
    const bucket = codelineReferenceGroups.get(key) ?? {
      subdivision: row.subdivision_names || "Unknown",
      serverName,
      codeLines: [],
      stationsByCodeLine: new Map<string, StationFoundationRow[]>(),
    };
    bucket.codeLines.push(row);
    codelineReferenceGroups.set(key, bucket);
  }

  for (const station of stationRows) {
    const group = codelineReferenceGroups.get(normalizeLookupKey(station.subdivision_name));
    if (!group) {
      continue;
    }
    const codeLineKey = String(station.code_line_number);
    const bucket = group.stationsByCodeLine.get(codeLineKey) ?? [];
    bucket.push(station);
    group.stationsByCodeLine.set(codeLineKey, bucket);
  }

  const packetSwitchGroups = new Map<string, {
    name: string;
    ip: string;
    roles: Set<string>;
    codeLines: Set<string>;
    subdivisions: Set<string>;
  }>();
  for (const row of codeLineRows) {
    for (const [name, ip, role] of [
      [row.packet_switch_primary_name, row.packet_switch_primary_ip, "primary packet switch"],
      [row.packet_switch_secondary_name, row.packet_switch_secondary_ip, "secondary packet switch"],
    ] as const) {
      if (!name || !ip || ip === "127.0.0.1" || name === "NONE") {
        continue;
      }
      const key = `${normalizeLookupKey(name)}|${normalizeLookupKey(ip)}`;
      const bucket = packetSwitchGroups.get(key) ?? {
        name,
        ip,
        roles: new Set<string>(),
        codeLines: new Set<string>(),
        subdivisions: new Set<string>(),
      };
      bucket.roles.add(role);
      bucket.codeLines.add(row.code_line_name);
      if (row.subdivision_names) {
        bucket.subdivisions.add(row.subdivision_names);
      }
      packetSwitchGroups.set(key, bucket);
    }
  }

  const locomotiveNetworkGroups = new Map<string, {
    trainKey: string;
    rows: HostInventoryRow[];
  }>();
  const fieldNetworkGroups = new Map<string, {
    family: string;
    rows: HostInventoryRow[];
  }>();
  for (const row of hostRows) {
    const trainKey = extractTrainInventoryKey(row);
    if (trainKey) {
      const bucket = locomotiveNetworkGroups.get(trainKey) ?? { trainKey, rows: [] };
      bucket.rows.push(row);
      locomotiveNetworkGroups.set(trainKey, bucket);
      continue;
    }
    const family = getFieldNetworkFamilyLabel(row);
    const key = normalizeLookupKey(family);
    const bucket = fieldNetworkGroups.get(key) ?? { family, rows: [] };
    bucket.rows.push(row);
    fieldNetworkGroups.set(key, bucket);
  }

  for (const entry of workflowEntries) {
    pushFoundationLine(lines, lineDetails, entry.title, {
      source: referenceSource("workflow"),
      translation: {
        original: entry.title,
        structured: [],
        english: [entry.summary],
        unresolved: [],
      },
      workflow: {
        summary: entry.summary,
        currentStep: "Reference section",
        systems: [],
        objects: [entry.title],
        knownState: `${entry.steps.length} grounded workflow steps`,
        unresolved: [],
      },
      workflowContext: [
        "Flow steps:",
        ...entry.steps.flatMap((step, index) => [
          `${index + 1}. ${step.text}`,
          ...((step.notes ?? []).map((note) => `   ${note}`)),
          ...(step.rawExamples.length
            ? step.rawExamples.map((example, exampleIndex) =>
                `   ${describeWorkflowEvidenceLabel(step.text, example, exampleIndex)}: ${example}`)
            : (step.notes ?? []).some((note) => /^Local trace status:/i.test(note))
              ? []
              : ["   Local trace status: no direct BOS send / receive sample was found in the local logs checked."]),
          ...(index < entry.steps.length - 1 ? [""] : []),
        ]),
      ],
      sourceReferences: [],
    });
  }

  for (const entry of genisysEntries) {
    pushFoundationLine(lines, lineDetails, `Genisys ${entry.code}: ${entry.title}`, {
      source: referenceSource("genisys"),
      translation: {
        original: entry.code,
        structured: [],
        english: [entry.meaning],
        unresolved: [],
      },
      workflow: {
        summary: "Genisys protocol reference.",
        currentStep: "Reference view",
        systems: ["Genisys"],
        objects: [entry.code],
        knownState: entry.title,
        unresolved: [],
      },
      databaseContext: [
        "Genisys reference:",
        `Meaning: ${entry.meaning}`,
        ...(entry.notes ?? []),
      ],
      payloadContext: entry.examples?.length
        ? [
          "Grounded code examples:",
          ...entry.examples,
        ]
        : undefined,
      referenceBadges: [
        ...(entry.badges ?? []),
      ],
      sourceReferences: [],
    });
  }

  for (const messageId of orderedTrainMessageIds) {
    const row = trainCatalogById.get(messageId);
    if (!row) {
      continue;
    }
    const normalizedMessageId = formatTrainMessageId(messageId);
    const sortedReleases = Array.from(row.releases).sort();
    const sortedVersions = Array.from(row.versions).sort((a, b) => toNumber(a) - toNumber(b));
    const bounceDiagramMessage = bounceDiagramById.get(messageId) ?? null;
    pushFoundationLine(lines, lineDetails, `Train message ${normalizedMessageId}: ${row.name}`, {
      source: referenceSource("train-messages"),
      translation: {
        original: normalizedMessageId,
        structured: [],
        english: [`${row.name}.`],
        unresolved: [],
      },
      workflow: {
        summary: "Train/PTC message reference.",
        currentStep: "Reference view",
        systems: ["Office", "Locomotive", "PTC"],
        objects: [normalizedMessageId, row.name],
        knownState: describeTrainMessageDirection(normalizedMessageId),
        unresolved: [],
      },
      databaseContext: buildTrainMessageReferenceContext(
        messageId,
        trainCatalogById,
        trainFactsById,
        reverseRepliesById,
        reverseConfirmationsById,
        reverseTriggeredByExpectedReplyById,
        reverseConfirmedByExpectedConfirmationById,
      ).concat(buildBounceDiagramReferenceContext(messageId, bounceDiagramById, trainCatalogById)),
      payloadContext: bounceDiagramMessage?.fields.length
        ? [
            "Bounce-diagram fields:",
            ...bounceDiagramMessage.fields.map(
              (field, fieldIndex) =>
                `${fieldIndex + 1}. ${field.name} [size=${field.size}; type=${field.type}] ${field.description}`,
            ),
          ]
        : undefined,
      referenceBadges: [
        describeTrainMessageDirection(messageId),
        ...(bounceDiagramMessage ? ["Bounce diagram"] : []),
        ...sortedReleases.map((releaseValue) => `Release ${releaseValue}`),
        ...sortedVersions.map((versionValue) => `Version ${versionValue}`),
      ],
      sourceReferences: bounceDiagramMessage
        ? [
            createSourceRecord(
              `manual:message-exchange-chart:${normalizedMessageId}`,
              "manual",
              "ITC Office-Locomotive Bounce Diagram",
              messageExchangeChartPath,
              `Chart-backed section: ${bounceDiagramMessage.section}`,
            ),
          ]
        : [],
    });
  }

  for (const group of Array.from(codelineReferenceGroups.values())) {
    const groupedRows = group.codeLines.sort((a, b) => toNumber(a.code_line_number) - toNumber(b.code_line_number));
    const stationItems = groupedRows
      .flatMap((row) =>
        [...(group.stationsByCodeLine.get(String(row.code_line_number)) ?? [])].map((station) => ({ row, station })),
      )
      .sort(({ station: left }, { station: right }) => {
        const leftMilepost = getStationReferenceMilepost(left, controlPointMilepostByNumber);
        const rightMilepost = getStationReferenceMilepost(right, controlPointMilepostByNumber);
        if (leftMilepost !== null || rightMilepost !== null) {
          if (leftMilepost === null) {
            return 1;
          }
          if (rightMilepost === null) {
            return -1;
          }
          if (leftMilepost !== rightMilepost) {
            return leftMilepost - rightMilepost;
          }
        }
        return toNumber(left.code_line_number) - toNumber(right.code_line_number)
          || toNumber(left.code_station_number) - toNumber(right.code_station_number)
          || toNumber(left.control_point_number) - toNumber(right.control_point_number)
          || left.station_name.localeCompare(right.station_name)
          || left.control_point_name.localeCompare(right.control_point_name);
      })
      .map(({ station }) => {
        const assignmentRow = assignmentRowsByCp.get(normalizeLookupKey(station.control_point_number))
          ?? assignmentRowsByCp.get(normalizeLookupKey(station.control_point_name))
          ?? null;
        return {
          id: `${station.code_line_number}:${station.control_point_number}:${station.station_name}`,
          label: buildStationReferenceChoiceLabel(station, controlPointMilepostByNumber),
          content: stationReferenceLibraryContext(station, assignmentRow),
        };
      });
    pushFoundationLine(lines, lineDetails, `${group.subdivision} / ${group.serverName}`, {
      source: referenceSource("codelines"),
      translation: {
        original: group.serverName,
        structured: [],
        english: [`${group.serverName} is the code server for ${group.subdivision}.`],
        unresolved: [],
      },
      workflow: {
        summary: "Code line / station reference.",
        currentStep: "Reference view",
        systems: ["Code server", "Code line", "Station assignments"],
        objects: [group.serverName, group.subdivision],
        knownState: "code server",
        unresolved: [],
      },
      databaseContext: [
        "Code line / station reference:",
        "Select a station / control point bubble on the left to inspect its assignment map.",
      ],
      referenceBadges: [
        `${groupedRows.length} code lines`,
      ],
      referenceChoiceGroups: stationItems.length
        ? [
          {
            id: "stations",
            label: "Stations / Control Points",
            layout: "column",
            items: stationItems,
          },
        ]
        : undefined,
      sourceReferences: [],
    });
  }

  const groupedPacketSwitches = Array.from(packetSwitchGroups.values()).sort((a, b) => a.name.localeCompare(b.name) || a.ip.localeCompare(b.ip));
  if (groupedPacketSwitches.length) {
    pushFoundationLine(lines, lineDetails, "Packet switches: network inventory", {
      source: referenceSource("network"),
      translation: {
        original: "Packet switches",
        structured: [],
        english: ["Packet-switch endpoints grouped into one reference inventory."],
        unresolved: [],
      },
      workflow: {
        summary: "Packet-switch reference.",
        currentStep: "Reference view",
        systems: ["Packet switch", "Code line"],
        objects: ["Packet switches"],
        knownState: `${groupedPacketSwitches.length} packet-switch endpoints`,
        unresolved: [],
      },
      databaseContext: [
        "Packet switch reference:",
      ],
      referenceBadges: [
        "Packet switches",
        `${groupedPacketSwitches.length} endpoints`,
      ],
      referenceChoiceGroups: [
        {
          id: "packet-switches",
          label: "Packet switches",
          layout: "horizontal",
          selectionMode: "multiple",
          items: groupedPacketSwitches.map((group) => ({
            id: `${normalizeLookupKey(group.name)}|${normalizeLookupKey(group.ip)}`,
            label: `${group.name} / ${group.ip}`,
            content: buildPacketSwitchGroupContext(group),
          })),
        },
      ],
      sourceReferences: [],
    });
  }

  const groupedTrainNetworks = Array.from(locomotiveNetworkGroups.values()).sort((a, b) => a.trainKey.localeCompare(b.trainKey));
  if (groupedTrainNetworks.length) {
    const trainNetworkRows = groupedTrainNetworks.flatMap((group) => group.rows);
    const carriers = Array.from(new Set(trainNetworkRows.map((row) => detectHostCarrier(row)).filter(Boolean))) as string[];
    pushFoundationLine(lines, lineDetails, "Train network: locomotive inventory", {
      source: referenceSource("network"),
      translation: {
        original: "Train network",
        structured: [],
        english: ["Train-side network inventory grouped by locomotive / vehicle."],
        unresolved: [],
      },
      workflow: {
        summary: "Train radio / modem reference.",
        currentStep: "Reference view",
        systems: ["Train network", "Inventory"],
        objects: ["Train network"],
        knownState: `${groupedTrainNetworks.length} grounded train inventories`,
        unresolved: [],
      },
      databaseContext: [
        "Train radio / modem reference:",
      ],
      referenceBadges: [
        "Train network",
        `${groupedTrainNetworks.length} trains`,
        ...carriers,
      ],
      referenceChoiceGroups: [
        {
          id: "train-groups",
          label: "Locomotives / vehicles",
          layout: "horizontal",
          selectionMode: "multiple",
          items: groupedTrainNetworks.map((group) => ({
            id: normalizeLookupKey(group.trainKey),
            label: group.trainKey,
            content: buildTrainNetworkGroupContext(group.trainKey, group.rows),
          })),
        },
      ],
      sourceReferences: [],
    });
  }

  const groupedFieldNetworks = Array.from(fieldNetworkGroups.values()).sort((a, b) => a.family.localeCompare(b.family));
  if (groupedFieldNetworks.length) {
    const allRows = groupedFieldNetworks.flatMap((group) => group.rows);
    const carriers = Array.from(new Set(allRows.map((row) => detectHostCarrier(row)).filter(Boolean))) as string[];
    pushFoundationLine(lines, lineDetails, "Field / device network: infrastructure inventory", {
      source: referenceSource("network"),
      translation: {
        original: "Field / device network",
        structured: [],
        english: ["Field and device network inventory grouped by repeated source-grounded component titles."],
        unresolved: [],
      },
      workflow: {
        summary: "Field/device IP reference.",
        currentStep: "Reference view",
        systems: ["Field devices", "Inventory"],
        objects: ["Field / device network"],
        knownState: `${groupedFieldNetworks.length} component families across ${allRows.length} grounded infrastructure endpoints`,
        unresolved: [],
      },
      databaseContext: [
        "Device / IP reference:",
      ],
      referenceBadges: [
        `${groupedFieldNetworks.length} families`,
        `${allRows.length} endpoints`,
        ...carriers,
      ],
      referenceChoiceGroups: [
        {
          id: "field-device-groups",
          label: "Component titles",
          layout: "horizontal",
          selectionMode: "multiple",
          items: groupedFieldNetworks.map((group) => ({
            id: normalizeLookupKey(group.family),
            label: group.family,
            content: buildFieldNetworkGroupContext(group.family, group.rows),
            detailChoiceGroups: buildFieldNetworkDetailChoiceGroups(group.family, group.rows),
          })),
        },
      ],
      sourceReferences: [],
    });
  }

  const hiddenReferenceSources = new Set([
    referenceSource("workflow"),
    referenceSource("network"),
  ]);
  const visibleLines = lines.filter((line) => !hiddenReferenceSources.has(line.source ?? ""));
  for (const line of lines) {
    if (hiddenReferenceSources.has(line.source ?? "")) {
      delete lineDetails[line.id];
    }
  }

  reportProgress?.({
    phase: "complete",
    message: `finalizing reference library (${visibleLines.length} entries)`,
    percent: 97,
    completed: visibleLines.length,
    total: Math.max(visibleLines.length, 1),
  });

  return {
    lines: visibleLines,
    detail: visibleLines[0] ? lineDetails[visibleLines[0].id] ?? null : null,
    lineDetails,
  };
}

function createWindow(mode: WorkspaceWindowMode = "main"): BrowserWindow {
  const state = loadWindowState();
  const win = new BrowserWindow({
    width: mode === "reference" ? Math.max(1180, Math.min(1600, state.width + 120)) : state.width,
    height: mode === "reference" ? Math.max(860, state.height) : state.height,
    x: mode === "reference" && typeof state.x === "number" ? state.x + 40 : state.x,
    y: mode === "reference" && typeof state.y === "number" ? state.y + 40 : state.y,
    show: false,
    backgroundColor: "#0b1016",
    title: mode === "reference" ? "TMDS Reference Library" : "Log Analyzer",
    webPreferences: {
      preload: join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
  });
  let rendererRecoveryAttempted = false;
  let showFallbackTimer: NodeJS.Timeout | null = null;
  const revealWindow = () => {
    if (win.isDestroyed()) {
      return;
    }
    if (!win.isVisible()) {
      win.show();
    }
    if (mode === "main") {
      win.focus();
    }
  };

  if (mode === "main" && state.maximized) {
    win.maximize();
  }

  const devUrl = process.env.VITE_DEV_SERVER_URL;
  if (devUrl) {
    const url = new URL(devUrl);
    url.searchParams.set("mode", mode);
    win.loadURL(url.toString());
  } else {
    const fileUrl = pathToFileURL(resolve(app.getAppPath(), "dist/renderer/index.html"));
    fileUrl.searchParams.set("mode", mode);
    win.loadURL(fileUrl.toString());
  }

  win.once("ready-to-show", () => {
    if (showFallbackTimer) {
      clearTimeout(showFallbackTimer);
      showFallbackTimer = null;
    }
    revealWindow();
  });
  win.webContents.on("render-process-gone", (_event, details) => {
    logRuntime(`renderer-process-gone reason=${details.reason} exitCode=${details.exitCode}`);
    if (!rendererRecoveryAttempted && !win.isDestroyed()) {
      rendererRecoveryAttempted = true;
      win.webContents.reloadIgnoringCache();
    }
  });
  win.webContents.on("did-fail-load", (_event, errorCode, errorDescription, validatedURL) => {
    logRuntime(`did-fail-load code=${errorCode} description=${errorDescription} url=${validatedURL}`);
  });
  win.on("unresponsive", () => {
    logRuntime("window-unresponsive");
  });
  win.webContents.on("did-finish-load", () => {
    rendererRecoveryAttempted = false;
    if (showFallbackTimer) {
      clearTimeout(showFallbackTimer);
      showFallbackTimer = null;
    }
    revealWindow();
  });
  showFallbackTimer = setTimeout(() => {
    showFallbackTimer = null;
    logRuntime(`window-show-fallback mode=${mode}`);
    revealWindow();
  }, 1800);

  win.on("close", () => {
    if (mode === "main") {
      const bounds = win.getBounds();
      saveWindowState({
        width: bounds.width,
        height: bounds.height,
        x: bounds.x,
        y: bounds.y,
        maximized: win.isMaximized(),
      });
    }
  });
  win.on("closed", () => {
    if (showFallbackTimer) {
      clearTimeout(showFallbackTimer);
      showFallbackTimer = null;
    }
    if (mainWindow === win) {
      mainWindow = null;
    }
    if (referenceWindow === win) {
      referenceWindow = null;
    }
  });

  return win;
}

ipcMain.handle("auth:get-state", async () => getAuthState());

ipcMain.handle("auth:setup-admin", async (): Promise<AuthResult> => {
  return { ...(await getAuthState()), ok: false, error: "Administrator account is already provisioned." };
});

ipcMain.handle("auth:create-user", async (_event, username: string, password: string, rememberUsername: boolean, keepSignedIn: boolean): Promise<AuthResult> => {
  const store = await readAuthStore();
  const cleanUsername = sanitizeAuthUsername(username);
  if (!cleanUsername) {
    return { ...(await getAuthState()), ok: false, error: "Enter a username." };
  }
  if (isUsernameTaken(store, cleanUsername)) {
    return { ...(await getAuthState()), ok: false, error: "User already exists. Sign in instead." };
  }
  if (!isValidUsername(cleanUsername)) {
    return { ...(await getAuthState()), ok: false, error: getUsernameValidationMessage() };
  }
  const saltHex = randomBytes(16).toString("hex");
  const user: StoredAuthUser = {
    username: cleanUsername,
    saltHex,
    passwordHashHex: hashPassword(String(password ?? ""), saltHex),
    role: "User",
  };
  await writeAuthStore(buildStoreWithUser(store, user, rememberUsername, keepSignedIn));
  authSessionUsername = cleanUsername;
  return { ...(await getAuthState()), ok: true };
});

ipcMain.handle("auth:login", async (_event, username: string, password: string, rememberUsername: boolean, keepSignedIn: boolean): Promise<AuthResult> => {
  const store = await readAuthStore();
  const cleanUsername = sanitizeAuthUsername(username);
  if (!cleanUsername) {
    return { ...(await getAuthState()), ok: false, error: "Enter a username." };
  }
  const isAdminLogin = isAdminLoginUsername(cleanUsername, store);
  const localUser = isAdminLogin ? null : getLocalUser(store, cleanUsername);
  const enteredPassword = String(password ?? "");
  if (!isAdminLogin && localUser && (localUser.passwordResetRequired || !localUser.saltHex || !localUser.passwordHashHex)) {
    if (!enteredPassword.length) {
      return { ...(await getAuthState()), ok: false, error: "Enter a new password." };
    }
    const saltHex = randomBytes(16).toString("hex");
    const nextUser: StoredAuthUser = {
      ...localUser,
      saltHex,
      passwordHashHex: hashPassword(enteredPassword, saltHex),
      passwordResetRequired: false,
    };
    const nextStore: AuthStore = {
      ...(store ?? {}),
      version: 2,
      rememberedUsername: rememberUsername ? nextUser.username : undefined,
      keepSignedIn: Boolean(keepSignedIn),
      keepSignedInUsername: keepSignedIn ? nextUser.username : undefined,
      users: {
        ...getStoreUsers(store),
        [normalizeUsernameKey(nextUser.username)]: nextUser,
      },
    };
    authSessionUsername = nextUser.username;
    await writeAuthStore(nextStore);
    return { ...(await getAuthState()), ok: true };
  }
  const passwordValid = isAdminLogin
    ? verifyPassword(enteredPassword, BUILTIN_ADMIN_SALT_HEX, BUILTIN_ADMIN_PASSWORD_HASH_HEX)
    : Boolean(localUser?.saltHex && localUser.passwordHashHex && verifyPassword(enteredPassword, localUser.saltHex, localUser.passwordHashHex));
  if (!passwordValid) {
    return { ...(await getAuthState()), ok: false, error: "Invalid username or password." };
  }
  authSessionUsername = isAdminLogin ? getEffectiveAdminUsername(store) : localUser?.username ?? cleanUsername;
  await writeAuthStore(buildStoreSessionUpdate(store, authSessionUsername, rememberUsername, keepSignedIn));
  return { ...(await getAuthState()), ok: true };
});

ipcMain.handle("auth:logout", async (): Promise<AuthState> => {
  const store = await readAuthStore();
  if (store?.keepSignedIn) {
    await writeAuthStore({ ...store, keepSignedIn: false, keepSignedInUsername: undefined });
  }
  authSessionUsername = null;
  return getAuthState();
});

ipcMain.handle("auth:admin-list-users", async (): Promise<AdminUsersResult> => {
  try {
    const store = await requireAdministrator();
    return buildAdminUsersResult(store);
  } catch (error) {
    return buildAdminUsersResult(await readAuthStore(), false, error instanceof Error ? error.message : "Administrator role required.");
  }
});

ipcMain.handle("auth:admin-create-user", async (_event, username: string, role: AuthRole): Promise<AdminUsersResult> => {
  let store: AuthStore | null = null;
  try {
    store = await requireAdministrator();
    const cleanUsername = sanitizeAuthUsername(username);
    if (!cleanUsername) {
      return buildAdminUsersResult(store, false, "Enter a username.");
    }
    if (isUsernameTaken(store, cleanUsername)) {
      return buildAdminUsersResult(store, false, "User already exists.");
    }
    if (!isValidUsername(cleanUsername)) {
      return buildAdminUsersResult(store, false, getUsernameValidationMessage());
    }
    if (role !== "Administrator" && role !== "User") {
      return buildAdminUsersResult(store, false, "Select a valid role.");
    }
    const user: StoredAuthUser = {
      username: cleanUsername,
      role,
      passwordResetRequired: true,
    };
    const nextStore: AuthStore = {
      ...(store ?? {}),
      version: 2,
      rememberedUsername: store?.rememberedUsername,
      keepSignedIn: store?.keepSignedIn,
      keepSignedInUsername: store?.keepSignedInUsername,
      users: {
        ...getStoreUsers(store),
        [normalizeUsernameKey(cleanUsername)]: user,
      },
    };
    await writeAuthStore(nextStore);
    return buildAdminUsersResult(nextStore);
  } catch (error) {
    return buildAdminUsersResult(store ?? await readAuthStore(), false, error instanceof Error ? error.message : "Create user failed.");
  }
});

ipcMain.handle("auth:admin-delete-user", async (_event, username: string): Promise<AdminUsersResult> => {
  let store: AuthStore | null = null;
  try {
    store = await requireAdministrator();
    const key = normalizeUsernameKey(username);
    if (!key || isAdminLoginUsername(username, store)) {
      return buildAdminUsersResult(store, false, "The built-in administrator cannot be deleted.");
    }
    if (normalizeUsernameKey(authSessionUsername ?? "") === key) {
      return buildAdminUsersResult(store, false, "The signed-in account cannot delete itself.");
    }
    const users = { ...getStoreUsers(store) };
    if (!users[key]) {
      return buildAdminUsersResult(store, false, "User was not found.");
    }
    delete users[key];
    const nextStore: AuthStore = {
      ...store,
      version: 2,
      users,
      keepSignedInUsername: normalizeUsernameKey(store?.keepSignedInUsername ?? "") === key ? undefined : store?.keepSignedInUsername,
      keepSignedIn: normalizeUsernameKey(store?.keepSignedInUsername ?? "") === key ? false : store?.keepSignedIn,
      rememberedUsername: normalizeUsernameKey(store?.rememberedUsername ?? "") === key ? undefined : store?.rememberedUsername,
    };
    await writeAuthStore(nextStore);
    return buildAdminUsersResult(nextStore);
  } catch (error) {
    return buildAdminUsersResult(store ?? await readAuthStore(), false, error instanceof Error ? error.message : "Delete user failed.");
  }
});

ipcMain.handle("auth:admin-reset-password", async (_event, username: string): Promise<AdminUsersResult> => {
  let store: AuthStore | null = null;
  try {
    store = await requireAdministrator();
    const key = normalizeUsernameKey(username);
    if (!key || isAdminLoginUsername(username, store)) {
      return buildAdminUsersResult(store, false, "The built-in administrator password is fixed in this build.");
    }
    const users = { ...getStoreUsers(store) };
    const existing = users[key];
    if (!existing) {
      return buildAdminUsersResult(store, false, "User was not found.");
    }
    users[key] = {
      ...existing,
      saltHex: undefined,
      passwordHashHex: undefined,
      passwordResetRequired: true,
    };
    const nextStore: AuthStore = { ...store, version: 2, users };
    await writeAuthStore(nextStore);
    return buildAdminUsersResult(nextStore);
  } catch (error) {
    return buildAdminUsersResult(store ?? await readAuthStore(), false, error instanceof Error ? error.message : "Reset password failed.");
  }
});

ipcMain.handle("auth:admin-set-role", async (_event, username: string, role: AuthRole): Promise<AdminUsersResult> => {
  let store: AuthStore | null = null;
  try {
    store = await requireAdministrator();
    const key = normalizeUsernameKey(username);
    if (!key || isAdminLoginUsername(username, store)) {
      return buildAdminUsersResult(store, false, "The built-in administrator role cannot be changed.");
    }
    if (role !== "Administrator" && role !== "User") {
      return buildAdminUsersResult(store, false, "Select a valid role.");
    }
    const users = { ...getStoreUsers(store) };
    const existing = users[key];
    if (!existing) {
      return buildAdminUsersResult(store, false, "User was not found.");
    }
    users[key] = { ...existing, role };
    const nextStore: AuthStore = { ...store, version: 2, users };
    await writeAuthStore(nextStore);
    return buildAdminUsersResult(nextStore);
  } catch (error) {
    return buildAdminUsersResult(store ?? await readAuthStore(), false, error instanceof Error ? error.message : "Change role failed.");
  }
});

ipcMain.handle("workspace:pick-inputs", async () => {
  const result = await dialog.showOpenDialog({
    properties: ["openFile", "openDirectory", "multiSelections"],
  });
  return result.canceled ? [] : result.filePaths;
});

ipcMain.handle("workspace:open-reference-window", async () => {
  if (!referenceWindow || referenceWindow.isDestroyed()) {
    referenceWindow = createWindow("reference");
  } else {
    focusReferenceWindow();
  }
});

ipcMain.handle("workspace:load-samples", async () => {
  try {
    return await buildReferenceLibrarySession(createProgressReporter("foundation"));
  } catch {
    const sampleFile = resolve(app.getAppPath(), "sample_logs/curated/genisys_sample.log");
    return ingestPaths([sampleFile], { reportProgress: createProgressReporter("foundation") });
  }
});
ipcMain.handle("workspace:load-review-sample", async () => {
  return buildCuratedSampleSession();
});

ipcMain.handle("workspace:ingest-paths", async (_event, paths: string[]) =>
  {
    return ingestPaths(Array.isArray(paths) ? paths : [], { reportProgress: createProgressReporter("ingest") });
  },
);
ipcMain.handle("workspace:get-line-detail", async (_event, request: { lineId?: string; sessionId?: string } | string) =>
  {
    if (typeof request === "string") {
      return getRuntimeSessionDetail(String(request ?? ""));
    }
    return getRuntimeSessionDetail(String(request?.lineId ?? ""), request?.sessionId);
  },
);
ipcMain.handle("workspace:warm-line-details", async (_event, request: { lineIds?: string[]; sessionId?: string } | string[]) => {
  if (Array.isArray(request)) {
    warmRuntimeSessionDetails(request);
    return;
  }
  warmRuntimeSessionDetails(Array.isArray(request?.lineIds) ? request.lineIds : [], request?.sessionId);
});
ipcMain.handle("workspace:update-search", async (_event, _config: SearchConfig) => {
  return undefined;
});
ipcMain.handle("workspace:open-time-convert-tool", async () => {
  await shell.openExternal(TIME_CONVERT_TOOL_URL);
  return undefined;
});

function sendJson(response: ServerResponse, statusCode: number, value: unknown): void {
  const body = JSON.stringify(value);
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(body),
    "Cache-Control": "no-store",
  });
  response.end(body);
}

function sendText(response: ServerResponse, statusCode: number, value: string): void {
  response.writeHead(statusCode, {
    "Content-Type": "text/plain; charset=utf-8",
    "Content-Length": Buffer.byteLength(value),
    "Cache-Control": "no-store",
  });
  response.end(value);
}

function readRequestBody(request: IncomingMessage, maxBytes = 512 * 1024 * 1024): Promise<Buffer> {
  return new Promise((resolvePromise, reject) => {
    const chunks: Buffer[] = [];
    let total = 0;
    request.on("data", (chunk: Buffer) => {
      total += chunk.length;
      if (total > maxBytes) {
        reject(new Error("Upload is too large for this webapp session."));
        request.destroy();
        return;
      }
      chunks.push(chunk);
    });
    request.once("end", () => resolvePromise(Buffer.concat(chunks)));
    request.once("error", reject);
  });
}

async function readJsonRequest<T>(request: IncomingMessage): Promise<T> {
  const body = await readRequestBody(request);
  return JSON.parse(body.toString("utf-8")) as T;
}

type MultipartManifestEntry = { fieldName: string; name: string; relativePath: string };
type MultipartManifest = { files?: MultipartManifestEntry[] };

type StagedMultipartFile = {
  fieldName: string;
  filename: string;
  tempPath: string;
  size: number;
};

type MultipartUpload = {
  manifest: MultipartManifest;
  staged: StagedMultipartFile[];
  tempDir: string;
  cleanup: () => Promise<void>;
};

async function readMultipartUpload(request: IncomingMessage): Promise<MultipartUpload> {
  const ct = request.headers["content-type"] ?? "";
  const boundaryMatch = /boundary=(?:"([^"]+)"|([^;\s]+))/.exec(ct);
  if (!boundaryMatch) {
    throw new Error("multipart/form-data boundary missing");
  }
  const boundary = boundaryMatch[1] ?? boundaryMatch[2];
  const dashDashBoundary = Buffer.from(`--${boundary}`);
  const crlfBoundary = Buffer.from(`\r\n--${boundary}`);
  const headerEnd = Buffer.from("\r\n\r\n");
  const tempDir = await mkdtemp(join(tmpdir(), "tmds-upload-"));
  const cleanup = async (): Promise<void> => {
    await rm(tempDir, { recursive: true, force: true }).catch(() => {});
  };

  const staged: StagedMultipartFile[] = [];
  let manifest: MultipartManifest = {};

  let buffer = Buffer.alloc(0);
  let state: "preamble" | "headers" | "body" | "done" = "preamble";
  let currentHeaders: { fieldName: string; filename: string } | null = null;
  let currentWriter: WriteStream | null = null;
  let currentTempPath = "";
  let currentSize = 0;
  let currentManifestChunks: Buffer[] | null = null;
  let partIndex = 0;

  const flushBody = async (slice: Buffer): Promise<void> => {
    if (!slice.length) return;
    if (currentManifestChunks) {
      currentManifestChunks.push(slice);
      return;
    }
    if (currentWriter) {
      const writer = currentWriter;
      if (!writer.write(slice)) {
        await new Promise<void>((res, rej) => {
          const onDrain = () => { writer.off("error", onError); res(); };
          const onError = (err: Error) => { writer.off("drain", onDrain); rej(err); };
          writer.once("drain", onDrain);
          writer.once("error", onError);
        });
      }
      currentSize += slice.length;
    }
  };

  const endCurrentPart = async (): Promise<void> => {
    if (currentManifestChunks) {
      const text = Buffer.concat(currentManifestChunks).toString("utf-8");
      currentManifestChunks = null;
      try {
        manifest = JSON.parse(text) as MultipartManifest;
      } catch {
        manifest = {};
      }
      currentHeaders = null;
      return;
    }
    if (currentWriter) {
      const writer = currentWriter;
      currentWriter = null;
      await new Promise<void>((res, rej) => {
        writer.end((err?: Error | null) => err ? rej(err) : res());
      });
      const headers = currentHeaders;
      if (headers) {
        staged.push({
          fieldName: headers.fieldName,
          filename: headers.filename,
          tempPath: currentTempPath,
          size: currentSize,
        });
      }
    }
    currentHeaders = null;
  };

  const advance = async (): Promise<boolean> => {
    while (true) {
      if (state === "preamble") {
        const idx = buffer.indexOf(dashDashBoundary);
        if (idx < 0) return false;
        const after = idx + dashDashBoundary.length;
        if (buffer.length < after + 2) return false;
        if (buffer[after] === 0x2d && buffer[after + 1] === 0x2d) {
          state = "done";
          return false;
        }
        if (buffer[after] !== 0x0d || buffer[after + 1] !== 0x0a) {
          throw new Error("malformed multipart: expected CRLF after first boundary");
        }
        buffer = buffer.subarray(after + 2);
        state = "headers";
        continue;
      }
      if (state === "headers") {
        const idx = buffer.indexOf(headerEnd);
        if (idx < 0) return false;
        const headersText = buffer.subarray(0, idx).toString("utf-8");
        const fieldNameMatch = /name="([^"]*)"/.exec(headersText);
        const filenameMatch = /filename="([^"]*)"/.exec(headersText);
        currentHeaders = {
          fieldName: fieldNameMatch?.[1] ?? "",
          filename: filenameMatch?.[1] ?? "",
        };
        buffer = buffer.subarray(idx + headerEnd.length);
        currentSize = 0;
        if (currentHeaders.fieldName === "manifest") {
          currentManifestChunks = [];
        } else {
          partIndex += 1;
          currentTempPath = join(tempDir, `part-${partIndex}.bin`);
          currentWriter = createWriteStream(currentTempPath);
        }
        state = "body";
        continue;
      }
      if (state === "body") {
        const idx = buffer.indexOf(crlfBoundary);
        if (idx < 0) {
          const safe = Math.max(0, buffer.length - crlfBoundary.length);
          if (safe > 0) {
            await flushBody(buffer.subarray(0, safe));
            buffer = buffer.subarray(safe);
          }
          return false;
        }
        if (idx > 0) {
          await flushBody(buffer.subarray(0, idx));
        }
        await endCurrentPart();
        const after = idx + crlfBoundary.length;
        if (buffer.length < after + 2) {
          buffer = buffer.subarray(after);
          return false;
        }
        if (buffer[after] === 0x2d && buffer[after + 1] === 0x2d) {
          state = "done";
          return false;
        }
        if (buffer[after] === 0x0d && buffer[after + 1] === 0x0a) {
          buffer = buffer.subarray(after + 2);
          state = "headers";
          continue;
        }
        throw new Error("malformed multipart: unexpected bytes after boundary");
      }
      return false;
    }
  };

  try {
    for await (const chunk of request) {
      const buf = chunk as Buffer;
      buffer = buffer.length === 0 ? buf : Buffer.concat([buffer, buf]);
      await advance();
      if (state === "done") break;
    }
    if (state !== "done") {
      await advance();
    }
  } catch (error) {
    if (currentWriter) {
      try { currentWriter.destroy(); } catch { /* ignore */ }
    }
    await cleanup();
    throw error;
  }

  return { manifest, staged, tempDir, cleanup };
}

function getStaticMimeType(filePath: string): string {
  const ext = extname(filePath).toLowerCase();
  if (ext === ".html") return "text/html; charset=utf-8";
  if (ext === ".js") return "text/javascript; charset=utf-8";
  if (ext === ".css") return "text/css; charset=utf-8";
  if (ext === ".map") return "application/json; charset=utf-8";
  if (ext === ".json") return "application/json; charset=utf-8";
  if (ext === ".svg") return "image/svg+xml";
  return "application/octet-stream";
}

async function serveWebStatic(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const rendererRoot = foundationPath("dist", "renderer");
  const url = new URL(request.url ?? "/", "http://127.0.0.1");
  const decodedPath = decodeURIComponent(url.pathname);
  const relativePath = decodedPath === "/" ? "index.html" : decodedPath.replace(/^\/+/, "");
  const targetPath = resolve(rendererRoot, relativePath);
  if (!targetPath.startsWith(rendererRoot)) {
    sendText(response, 403, "Forbidden");
    return;
  }
  try {
    const body = await readFile(targetPath);
    const ext = extname(targetPath).toLowerCase();
    const isHashedAsset = /^\/?assets\//.test(relativePath) && /-[A-Za-z0-9_-]{8,}\.[a-z]+$/.test(targetPath);
    const cacheHeader = ext === ".html"
      ? "no-store, no-cache, must-revalidate, max-age=0"
      : isHashedAsset
        ? "public, max-age=31536000, immutable"
        : "no-cache, must-revalidate";
    response.writeHead(200, {
      "Content-Type": getStaticMimeType(targetPath),
      "Content-Length": body.length,
      "Cache-Control": cacheHeader,
    });
    response.end(body);
  } catch {
    sendText(response, 404, "Not found");
  }
}

async function buildHttpAuthState(session: HttpSession | null): Promise<AuthState> {
  const store = await readAuthStore();
  return {
    configured: true,
    authenticated: Boolean(session),
    username: session?.username,
    rememberedUsername: store?.rememberedUsername ?? getEffectiveAdminUsername(store),
    keepSignedIn: false,
    role: session?.role,
    adminUsername: getEffectiveAdminUsername(store),
    availableUsernames: getAvailableUsernames(store),
    pendingPasswordResetCount: session?.role === "Administrator" ? getPendingPasswordResetCount(store) : 0,
  };
}

async function handleHttpAuthRoutes(
  request: IncomingMessage,
  response: ServerResponse,
  url: URL,
  sessionInfo: { token: string; session: HttpSession } | null,
): Promise<boolean> {
  if (request.method === "GET" && url.pathname === "/api/auth/state") {
    sendJson(response, 200, await buildHttpAuthState(sessionInfo?.session ?? null));
    return true;
  }
  if (request.method === "GET" && url.pathname === "/api/auth/user-status") {
    const username = sanitizeAuthUsername(url.searchParams.get("username") ?? "");
    if (!username) {
      sendJson(response, 200, { exists: false, requiresPasswordCreation: false, requiresPasswordReset: false });
      return true;
    }
    const store = await readAuthStore();
    if (isAdminLoginUsername(username, store)) {
      sendJson(response, 200, { exists: true, requiresPasswordCreation: false, requiresPasswordReset: false, builtIn: true });
      return true;
    }
    const user = getLocalUser(store, username);
    if (!user) {
      sendJson(response, 200, { exists: false, requiresPasswordCreation: false, requiresPasswordReset: false });
      return true;
    }
    const requiresPasswordCreation = !user.saltHex || !user.passwordHashHex || Boolean(user.passwordResetRequired);
    sendJson(response, 200, {
      exists: true,
      requiresPasswordCreation,
      requiresPasswordReset: Boolean(user.passwordResetRequired),
    });
    return true;
  }
  if (request.method === "POST" && url.pathname === "/api/auth/login") {
    const body = await readJsonRequest<{ username?: string; password?: string }>(request);
    const username = sanitizeAuthUsername(body.username ?? "");
    const password = String(body.password ?? "");
    if (!username) {
      sendJson(response, 400, { ok: false, error: "Enter a username." });
      return true;
    }
    if (!isPasswordValidLength(password)) {
      sendJson(response, 400, { ok: false, error: "Password must be 4 to 16 characters." });
      return true;
    }
    const store = await readAuthStore();
    if (isAdminLoginUsername(username, store)) {
      const ok = verifyPassword(password, BUILTIN_ADMIN_SALT_HEX, BUILTIN_ADMIN_PASSWORD_HASH_HEX);
      if (!ok) {
        sendJson(response, 401, { ok: false, error: "Invalid username or password." });
        return true;
      }
      const adminDisplayName = getEffectiveAdminUsername(store);
      const token = createHttpSession(adminDisplayName, "Administrator");
      setSessionCookie(response, token);
      sendJson(response, 200, { ok: true, ...(await buildHttpAuthState(httpSessions.get(token) ?? null)) });
      return true;
    }
    const user = getLocalUser(store, username);
    if (!user) {
      sendJson(response, 401, { ok: false, error: "Invalid username or password." });
      return true;
    }
    const needsPasswordSetup = !user.saltHex || !user.passwordHashHex || Boolean(user.passwordResetRequired);
    if (needsPasswordSetup) {
      sendJson(response, 409, {
        ok: false,
        requiresPasswordCreation: true,
        error: "Set a new password to continue.",
      });
      return true;
    }
    if (!verifyPassword(password, user.saltHex!, user.passwordHashHex!)) {
      sendJson(response, 401, { ok: false, error: "Invalid username or password." });
      return true;
    }
    user.lastLoginAt = new Date().toISOString();
    await writeAuthStore({ ...store!, version: 2, users: { ...getStoreUsers(store), [normalizeUsernameKey(user.username)]: user } });
    const token = createHttpSession(user.username, user.role);
    setSessionCookie(response, token);
    sendJson(response, 200, { ok: true, ...(await buildHttpAuthState(httpSessions.get(token) ?? null)) });
    return true;
  }
  if (request.method === "POST" && url.pathname === "/api/auth/set-password") {
    const body = await readJsonRequest<{ username?: string; password?: string }>(request);
    const username = sanitizeAuthUsername(body.username ?? "");
    const password = String(body.password ?? "");
    if (!username) {
      sendJson(response, 400, { ok: false, error: "Enter a username." });
      return true;
    }
    if (!isPasswordValidLength(password)) {
      sendJson(response, 400, { ok: false, error: "Password must be 4 to 16 characters." });
      return true;
    }
    const store = await readAuthStore();
    if (isAdminLoginUsername(username, store)) {
      sendJson(response, 403, { ok: false, error: "The administrator password cannot be reset here." });
      return true;
    }
    const user = getLocalUser(store, username);
    if (!user) {
      sendJson(response, 404, { ok: false, error: "User not found." });
      return true;
    }
    const allowed = !user.saltHex || !user.passwordHashHex || Boolean(user.passwordResetRequired);
    if (!allowed) {
      sendJson(response, 403, { ok: false, error: "Password is already set. Ask the administrator to reset it first." });
      return true;
    }
    const saltHex = randomBytes(16).toString("hex");
    const updated: StoredAuthUser = {
      ...user,
      saltHex,
      passwordHashHex: hashPassword(password, saltHex),
      passwordResetRequired: false,
      passwordResetRequestedAt: undefined,
      lastLoginAt: new Date().toISOString(),
    };
    const nextStore: AuthStore = {
      ...(store ?? { version: 2 }),
      version: 2,
      users: { ...getStoreUsers(store), [normalizeUsernameKey(user.username)]: updated },
    };
    await writeAuthStore(nextStore);
    const token = createHttpSession(updated.username, updated.role);
    setSessionCookie(response, token);
    sendJson(response, 200, { ok: true, ...(await buildHttpAuthState(httpSessions.get(token) ?? null)) });
    return true;
  }
  if (request.method === "POST" && url.pathname === "/api/auth/change-password") {
    if (!sessionInfo) {
      sendJson(response, 401, { ok: false, error: "Sign in required." });
      return true;
    }
    const body = await readJsonRequest<{ currentPassword?: string; newPassword?: string }>(request);
    const currentPassword = String(body.currentPassword ?? "");
    const newPassword = String(body.newPassword ?? "");
    if (!isPasswordValidLength(currentPassword) || !isPasswordValidLength(newPassword)) {
      sendJson(response, 400, { ok: false, error: getPasswordValidationMessage() });
      return true;
    }
    const store = await readAuthStore();
    if (isAdminLoginUsername(sessionInfo.session.username, store)) {
      sendJson(response, 403, { ok: false, error: "The built-in administrator password is fixed in this build." });
      return true;
    }
    const user = getLocalUser(store, sessionInfo.session.username);
    if (!user) {
      sendJson(response, 404, { ok: false, error: "Account not found." });
      return true;
    }
    if (!user.saltHex || !user.passwordHashHex || user.passwordResetRequired) {
      sendJson(response, 409, { ok: false, error: "Set a new password at sign-in before changing it here." });
      return true;
    }
    if (!verifyPassword(currentPassword, user.saltHex, user.passwordHashHex)) {
      sendJson(response, 401, { ok: false, error: "Current password is incorrect." });
      return true;
    }
    const saltHex = randomBytes(16).toString("hex");
    const updated: StoredAuthUser = {
      ...user,
      saltHex,
      passwordHashHex: hashPassword(newPassword, saltHex),
      passwordResetRequired: false,
      passwordResetRequestedAt: undefined,
    };
    const nextStore: AuthStore = {
      ...(store ?? { version: 2 }),
      version: 2,
      users: { ...getStoreUsers(store), [normalizeUsernameKey(user.username)]: updated },
    };
    await writeAuthStore(nextStore);
    sendJson(response, 200, { ok: true, ...(await buildHttpAuthState(sessionInfo.session)) });
    return true;
  }
  if (request.method === "POST" && url.pathname === "/api/auth/request-reset") {
    const body = await readJsonRequest<{ username?: string }>(request);
    const username = sanitizeAuthUsername(body.username ?? "");
    if (!username) {
      sendJson(response, 400, { ok: false, error: "Enter a username." });
      return true;
    }
    if (!isValidUsername(username)) {
      sendJson(response, 400, { ok: false, error: getUsernameValidationMessage() });
      return true;
    }
    const store = await readAuthStore();
    if (isAdminLoginUsername(username, store)) {
      sendJson(response, 403, { ok: false, error: "Administrator cannot request a reset." });
      return true;
    }
    const user = getLocalUser(store, username);
    if (!user) {
      sendJson(response, 200, { ok: true });
      return true;
    }
    const updated: StoredAuthUser = { ...user, passwordResetRequestedAt: new Date().toISOString() };
    const nextStore: AuthStore = {
      ...(store ?? { version: 2 }),
      version: 2,
      users: { ...getStoreUsers(store), [normalizeUsernameKey(user.username)]: updated },
    };
    await writeAuthStore(nextStore);
    sendJson(response, 200, { ok: true });
    return true;
  }
  if (request.method === "POST" && url.pathname === "/api/auth/rename") {
    if (!sessionInfo) {
      sendJson(response, 401, { ok: false, error: "Sign in required." });
      return true;
    }
    const body = await readJsonRequest<{ newUsername?: string }>(request);
    const newUsername = sanitizeAuthUsername(body.newUsername ?? "");
    if (!isValidUsername(newUsername)) {
      sendJson(response, 400, { ok: false, error: getUsernameValidationMessage() });
      return true;
    }
    const store = await readAuthStore();
    const currentUsername = sessionInfo.session.username;
    if (currentUsername === newUsername) {
      sendJson(response, 200, { ok: true, ...(await buildHttpAuthState(sessionInfo.session)) });
      return true;
    }
    if (sessionInfo.session.role === "Administrator" && isAdminLoginUsername(currentUsername, store)) {
      if (isUsernameTaken(store, newUsername) && !isAdminLoginUsername(newUsername, store)) {
        sendJson(response, 409, { ok: false, error: "That username is taken." });
        return true;
      }
      const nextStore: AuthStore = {
        ...(store ?? { version: 2 }),
        version: 2,
        adminUsernameAlias: getAdminAliasForUsername(newUsername),
      };
      await writeAuthStore(nextStore);
      const nextAdminUsername = getEffectiveAdminUsername(nextStore);
      for (const s of httpSessions.values()) {
        if (s.role === "Administrator" && isAdminLoginUsername(s.username, store)) {
          s.username = nextAdminUsername;
        }
      }
      sendJson(response, 200, { ok: true, ...(await buildHttpAuthState(sessionInfo.session)) });
      return true;
    }
    if (normalizeUsernameKey(currentUsername) !== normalizeUsernameKey(newUsername) && isUsernameTaken(store, newUsername)) {
      sendJson(response, 409, { ok: false, error: "That username is taken." });
      return true;
    }
    const user = getLocalUser(store, currentUsername);
    if (!user) {
      sendJson(response, 404, { ok: false, error: "Account not found." });
      return true;
    }
    const users = { ...getStoreUsers(store) };
    if (normalizeUsernameKey(currentUsername) !== normalizeUsernameKey(newUsername)) {
      delete users[normalizeUsernameKey(currentUsername)];
    }
    users[normalizeUsernameKey(newUsername)] = { ...user, username: newUsername };
    await writeAuthStore({ ...(store ?? { version: 2 }), version: 2, users });
    for (const s of httpSessions.values()) {
      if (normalizeUsernameKey(s.username) === normalizeUsernameKey(currentUsername)) {
        s.username = newUsername;
      }
    }
    sendJson(response, 200, { ok: true, ...(await buildHttpAuthState(sessionInfo.session)) });
    return true;
  }
  if (request.method === "POST" && url.pathname === "/api/auth/logout") {
    if (sessionInfo) destroyHttpSession(sessionInfo.token);
    clearSessionCookie(response);
    sendJson(response, 200, { ok: true });
    return true;
  }
  return false;
}

async function handleHttpAdminRoutes(
  request: IncomingMessage,
  response: ServerResponse,
  url: URL,
  session: HttpSession,
): Promise<boolean> {
  if (session.role !== "Administrator") {
    sendJson(response, 403, { ok: false, error: "Administrator role required." });
    return true;
  }
  if (request.method === "GET" && url.pathname === "/api/admin/users") {
    const store = await readAuthStore();
    const adminUsername = getEffectiveAdminUsername(store);
    const users = [
      { username: adminUsername, role: "Administrator" as AuthRole, builtIn: true, current: isAdminLoginUsername(session.username, store) },
      ...Object.values(getStoreUsers(store))
        .sort((a, b) => a.username.localeCompare(b.username))
        .map((user) => ({
          username: user.username,
          role: user.role,
          builtIn: false,
          current: normalizeUsernameKey(session.username) === normalizeUsernameKey(user.username),
          passwordResetRequired: Boolean(user.passwordResetRequired || !user.saltHex || !user.passwordHashHex),
          passwordResetRequestedAt: user.passwordResetRequestedAt,
          lastLoginAt: user.lastLoginAt,
        })),
    ];
    sendJson(response, 200, { ok: true, users });
    return true;
  }
  if (request.method === "GET" && url.pathname === "/api/admin/sessions") {
    pruneExpiredHttpSessions();
    const sessionsByUser = new Map<string, HttpSession>();
    for (const sessionEntry of httpSessions.values()) {
      const key = `${normalizeUsernameKey(sessionEntry.username)}:${sessionEntry.role}`;
      const existing = sessionsByUser.get(key);
      if (!existing || sessionEntry.lastSeenAt > existing.lastSeenAt) {
        sessionsByUser.set(key, sessionEntry);
      }
    }
    const sessions = Array.from(sessionsByUser.values())
      .sort((left, right) => left.username.localeCompare(right.username))
      .map((s) => ({
        username: s.username,
        role: s.role,
        createdAt: new Date(s.createdAt).toISOString(),
        lastSeenAt: new Date(s.lastSeenAt).toISOString(),
      }));
    sendJson(response, 200, { ok: true, sessions });
    return true;
  }
  if (request.method === "POST" && url.pathname === "/api/admin/users") {
    const body = await readJsonRequest<{ username?: string; role?: AuthRole }>(request);
    const username = sanitizeAuthUsername(body.username ?? "");
    const role = body.role === "Administrator" ? "Administrator" : "User";
    if (!username) {
      sendJson(response, 400, { ok: false, error: "Enter a username." });
      return true;
    }
    if (!isValidUsername(username)) {
      sendJson(response, 400, { ok: false, error: getUsernameValidationMessage() });
      return true;
    }
    const store = await readAuthStore();
    if (isUsernameTaken(store, username)) {
      sendJson(response, 409, { ok: false, error: "User already exists." });
      return true;
    }
    const newUser: StoredAuthUser = { username, role, passwordResetRequired: true };
    const nextStore: AuthStore = {
      ...(store ?? { version: 2 }),
      version: 2,
      users: { ...getStoreUsers(store), [normalizeUsernameKey(username)]: newUser },
    };
    await writeAuthStore(nextStore);
    sendJson(response, 200, { ok: true });
    return true;
  }
  const userMatch = url.pathname.match(/^\/api\/admin\/users\/([^/]+)(?:\/(role|reset|rename))?$/);
  if (userMatch) {
    const targetUsername = decodeURIComponent(userMatch[1]);
    const action = userMatch[2];
    const storeForAdminCheck = await readAuthStore();
    const targetIsAdmin = isAdminLoginUsername(targetUsername, storeForAdminCheck);
    if (targetIsAdmin && action !== "rename") {
      sendJson(response, 403, { ok: false, error: "The built-in administrator cannot be modified here." });
      return true;
    }
    if (targetIsAdmin && action === "rename") {
      const body = await readJsonRequest<{ newUsername?: string }>(request);
      const newUsername = sanitizeAuthUsername(body.newUsername ?? "");
      if (!isValidUsername(newUsername)) {
        sendJson(response, 400, { ok: false, error: getUsernameValidationMessage() });
        return true;
      }
      if (isUsernameTaken(storeForAdminCheck, newUsername) && !isAdminLoginUsername(newUsername, storeForAdminCheck)) {
        sendJson(response, 409, { ok: false, error: "That username is taken." });
        return true;
      }
      const nextStore: AuthStore = {
        ...(storeForAdminCheck ?? { version: 2 }),
        version: 2,
        adminUsernameAlias: getAdminAliasForUsername(newUsername),
      };
      await writeAuthStore(nextStore);
      const nextAdminUsername = getEffectiveAdminUsername(nextStore);
      for (const s of httpSessions.values()) {
        if (s.role === "Administrator" && isAdminLoginUsername(s.username, storeForAdminCheck)) {
          s.username = nextAdminUsername;
        }
      }
      sendJson(response, 200, { ok: true });
      return true;
    }
    const store = await readAuthStore();
    const user = getLocalUser(store, targetUsername);
    if (!user) {
      sendJson(response, 404, { ok: false, error: "User not found." });
      return true;
    }
    if (request.method === "DELETE" && !action) {
      if (normalizeUsernameKey(session.username) === normalizeUsernameKey(targetUsername)) {
        sendJson(response, 403, { ok: false, error: "You cannot delete your own account." });
        return true;
      }
      const users = { ...getStoreUsers(store) };
      delete users[normalizeUsernameKey(targetUsername)];
      await writeAuthStore({ ...(store ?? { version: 2 }), version: 2, users });
      for (const [token, s] of httpSessions) {
        if (normalizeUsernameKey(s.username) === normalizeUsernameKey(targetUsername)) {
          httpSessions.delete(token);
        }
      }
      sendJson(response, 200, { ok: true });
      return true;
    }
    if (request.method === "POST" && action === "reset") {
      const updated: StoredAuthUser = {
        ...user,
        passwordResetRequired: true,
        passwordResetRequestedAt: undefined,
        saltHex: undefined,
        passwordHashHex: undefined,
      };
      await writeAuthStore({
        ...(store ?? { version: 2 }),
        version: 2,
        users: { ...getStoreUsers(store), [normalizeUsernameKey(user.username)]: updated },
      });
      for (const [token, s] of httpSessions) {
        if (normalizeUsernameKey(s.username) === normalizeUsernameKey(targetUsername)) {
          httpSessions.delete(token);
        }
      }
      sendJson(response, 200, { ok: true });
      return true;
    }
    if (request.method === "POST" && action === "role") {
      const body = await readJsonRequest<{ role?: AuthRole }>(request);
      const newRole: AuthRole = body.role === "Administrator" ? "Administrator" : "User";
      const updated: StoredAuthUser = { ...user, role: newRole };
      await writeAuthStore({
        ...(store ?? { version: 2 }),
        version: 2,
        users: { ...getStoreUsers(store), [normalizeUsernameKey(user.username)]: updated },
      });
      for (const [token, s] of httpSessions) {
        if (normalizeUsernameKey(s.username) === normalizeUsernameKey(targetUsername)) {
          s.role = newRole;
        }
      }
      sendJson(response, 200, { ok: true });
      return true;
    }
    if (request.method === "POST" && action === "rename") {
      const body = await readJsonRequest<{ newUsername?: string }>(request);
      const newUsername = sanitizeAuthUsername(body.newUsername ?? "");
      if (!isValidUsername(newUsername)) {
        sendJson(response, 400, { ok: false, error: getUsernameValidationMessage() });
        return true;
      }
      if (targetUsername === newUsername) {
        sendJson(response, 200, { ok: true });
        return true;
      }
      if (normalizeUsernameKey(targetUsername) !== normalizeUsernameKey(newUsername) && isUsernameTaken(store, newUsername)) {
        sendJson(response, 409, { ok: false, error: "That username is taken." });
        return true;
      }
      const users = { ...getStoreUsers(store) };
      if (normalizeUsernameKey(targetUsername) !== normalizeUsernameKey(newUsername)) {
        delete users[normalizeUsernameKey(targetUsername)];
      }
      users[normalizeUsernameKey(newUsername)] = { ...user, username: newUsername };
      await writeAuthStore({ ...(store ?? { version: 2 }), version: 2, users });
      for (const s of httpSessions.values()) {
        if (normalizeUsernameKey(s.username) === normalizeUsernameKey(targetUsername)) {
          s.username = newUsername;
        }
      }
      sendJson(response, 200, { ok: true });
      return true;
    }
  }
  sendJson(response, 404, { ok: false, error: "Unknown admin endpoint." });
  return true;
}

async function handleWebApi(request: IncomingMessage, response: ServerResponse): Promise<boolean> {
  const url = new URL(request.url ?? "/", "http://127.0.0.1");
  if (!url.pathname.startsWith("/api/")) {
    return false;
  }

  try {
    const sessionInfo = getHttpSessionFromRequest(request);

    if (request.method === "GET" && url.pathname === "/api/health") {
      sendJson(response, 200, { ok: true, app: APP_DISPLAY_NAME, timestamp: new Date().toISOString() });
      return true;
    }

    if (url.pathname.startsWith("/api/auth/")) {
      const handled = await handleHttpAuthRoutes(request, response, url, sessionInfo);
      if (handled) return true;
    }

    if (url.pathname.startsWith("/api/admin/")) {
      if (!sessionInfo) {
        sendJson(response, 401, { ok: false, error: "Sign in required." });
        return true;
      }
      return await handleHttpAdminRoutes(request, response, url, sessionInfo.session);
    }

    if (!sessionInfo) {
      sendJson(response, 401, { ok: false, error: "Sign in required." });
      return true;
    }

    if (request.method === "GET" && url.pathname === "/api/reference") {
      sendJson(response, 200, await buildReferenceLibrarySession());
      return true;
    }
    if (request.method === "GET" && url.pathname === "/api/review-sample") {
      sendJson(response, 200, await buildCuratedSampleSession());
      return true;
    }
    if (request.method === "GET" && url.pathname === "/api/line-detail") {
      sendJson(response, 200, getRuntimeSessionDetail(url.searchParams.get("id") ?? "", url.searchParams.get("sessionId")));
      return true;
    }
    if (request.method === "POST" && url.pathname === "/api/warm-line-details") {
      const body = await readJsonRequest<{ lineIds?: string[]; sessionId?: string }>(request);
      warmRuntimeSessionDetails(Array.isArray(body.lineIds) ? body.lineIds : [], body.sessionId);
      sendJson(response, 200, { ok: true });
      return true;
    }
    if (request.method === "POST" && url.pathname === "/api/ingest-upload") {
      const contentType = String(request.headers["content-type"] ?? "").toLowerCase();
      const isMultipart = contentType.startsWith("multipart/form-data");
      let files: UploadedLogSource[] = [];
      let cleanupTempUpload: (() => Promise<void>) | null = null;
      if (isMultipart) {
        const upload = await readMultipartUpload(request);
        cleanupTempUpload = upload.cleanup;
        const manifestEntries = Array.isArray(upload.manifest.files) ? upload.manifest.files : [];
        const stagedByFieldName = new Map(upload.staged.map((entry) => [entry.fieldName, entry]));
        files = manifestEntries
          .map((entry) => {
            const staged = stagedByFieldName.get(entry.fieldName);
            if (!staged) return null;
            return {
              name: entry.name,
              relativePath: entry.relativePath,
              tempPath: staged.tempPath,
            } satisfies UploadedLogSource;
          })
          .filter((entry): entry is UploadedLogSource => Boolean(entry));
      } else {
        const body = await readJsonRequest<{ files?: UploadedLogSource[] }>(request);
        files = Array.isArray(body.files) ? body.files : [];
      }
      response.writeHead(200, {
        "Content-Type": "application/x-ndjson; charset=utf-8",
        "Cache-Control": "no-store",
      });
      const waitForResponseDrain = (): Promise<boolean> => new Promise((resolvePromise) => {
        const cleanup = () => {
          response.off("drain", handleDrain);
          response.off("error", handleError);
          response.off("close", handleClose);
        };
        const handleDrain = () => {
          cleanup();
          resolvePromise(true);
        };
        const handleError = (error: Error) => {
          cleanup();
          logRuntime(`upload-stream-write-error ${error.stack ?? error.message}`);
          resolvePromise(false);
        };
        const handleClose = () => {
          cleanup();
          resolvePromise(false);
        };
        response.once("drain", handleDrain);
        response.once("error", handleError);
        response.once("close", handleClose);
      });
      const writeEvent = async (event: { type: string; [key: string]: unknown }): Promise<boolean> => {
        let line: string;
        try {
          line = JSON.stringify(event) + "\n";
        } catch (error) {
          logRuntime(`upload-stream-json-error ${error instanceof Error ? error.stack ?? error.message : String(error)}`);
          return false;
        }
        if (response.destroyed || response.writableEnded) {
          return false;
        }
        try {
          return response.write(line) ? true : await waitForResponseDrain();
        } catch (error) {
          logRuntime(`upload-stream-write-error ${error instanceof Error ? error.stack ?? error.message : String(error)}`);
          return false;
        }
      };
      let writeQueue = Promise.resolve(true);
      const queueEvent = (event: { type: string; [key: string]: unknown }): Promise<boolean> => {
        writeQueue = writeQueue.then((ok) => ok ? writeEvent(event) : false);
        return writeQueue;
      };
      const queueSessionResult = async (result: SessionData): Promise<boolean> => {
        const chunkSize = 5000;
        if (!await queueEvent({
          type: "result-start",
          sessionId: result.sessionId,
          detail: result.detail,
          lineDetails: result.lineDetails ?? {},
          totalLines: result.lines.length,
        })) {
          return false;
        }
        for (let index = 0; index < result.lines.length; index += chunkSize) {
          const lines = result.lines.slice(index, index + chunkSize);
          if (!await queueEvent({ type: "result-lines", lines })) {
            return false;
          }
        }
        return queueEvent({ type: "result-end" });
      };
      try {
        const result = await ingestUploadedSources(files, {
          reportProgress: (progress) => {
            void queueEvent({ type: "progress", progress });
          },
        });
        const resultSent = await queueSessionResult(result);
        if (!resultSent && !response.destroyed && !response.writableEnded) {
          await writeEvent({
            type: "error",
            message: "TMDS server parsed the upload but could not send the result to the browser. Try a smaller folder or parse it from the desktop app.",
          });
        }
      } catch (error) {
        if (!await queueEvent({ type: "error", message: error instanceof Error ? error.message : String(error) })) {
          logRuntime(`upload-stream-error-event-failed ${error instanceof Error ? error.stack ?? error.message : String(error)}`);
        }
      } finally {
        if (cleanupTempUpload) {
          await cleanupTempUpload().catch((err) => {
            logRuntime(`upload-temp-cleanup-failed ${err instanceof Error ? err.stack ?? err.message : String(err)}`);
          });
        }
      }
      await writeQueue;
      response.end();
      return true;
    }
    sendJson(response, 404, { error: "Unknown API endpoint." });
    return true;
  } catch (error) {
    sendJson(response, 500, { error: error instanceof Error ? error.message : String(error) });
    return true;
  }
}

async function startWebAppServer(): Promise<void> {
  const requestedPort = Number(process.env.TMDS_WEB_PORT ?? "4173");
  const port = Number.isFinite(requestedPort) && requestedPort > 0 ? requestedPort : 4173;
  const host = process.env.TMDS_WEB_HOST || "127.0.0.1";
  const server = createServer((request, response) => {
    void (async () => {
      if (await handleWebApi(request, response)) {
        return;
      }
      await serveWebStatic(request, response);
    })().catch((error) => {
      sendJson(response, 500, { error: error instanceof Error ? error.message : String(error) });
    });
  });
  await new Promise<void>((resolvePromise) => server.listen(port, host, resolvePromise));
  const openHost = host === "0.0.0.0" || host === "::" ? "127.0.0.1" : host;
  const url = `http://${openHost}:${port}/`;
  logRuntime(`webapp-server started ${url}`);
  console.log(`[Log Analyzer] Webapp running at ${url}`);
  if (!process.argv.includes("--no-open")) {
    await shell.openExternal(url);
  }
}

process.on("uncaughtException", (error) => {
  logRuntime(`uncaught-exception ${error?.stack ?? String(error)}`);
});
process.on("unhandledRejection", (reason) => {
  logRuntime(`unhandled-rejection ${reason instanceof Error ? reason.stack ?? reason.message : String(reason)}`);
});

const auditFlagIndex = process.argv.findIndex((value) => value === "--audit-corpus");
const isCorpusAuditMode = auditFlagIndex >= 0;

const dumpFlagIndex = process.argv.findIndex((value) => value === "--dump-static-data");
const isDumpStaticDataMode = dumpFlagIndex >= 0;

async function runDumpStaticDataCli(outputDir: string): Promise<void> {
  const targetDir = resolve(outputDir);
  await mkdir(targetDir, { recursive: true });
  console.log(`[dump-static-data] writing static session JSON to ${targetDir}`);

  console.log("[dump-static-data] building reference library session...");
  const referenceSession = await buildReferenceLibrarySession();
  const referencePath = resolve(targetDir, "reference-session.json");
  await writeFile(referencePath, JSON.stringify(referenceSession), "utf-8");
  console.log(`[dump-static-data] wrote ${referencePath} (${referenceSession.lines.length} lines)`);

  console.log("[dump-static-data] building curated review-sample session...");
  const sampleSession = await buildCuratedSampleSession();
  const samplePath = resolve(targetDir, "review-sample-session.json");
  await writeFile(samplePath, JSON.stringify(sampleSession), "utf-8");
  console.log(`[dump-static-data] wrote ${samplePath} (${sampleSession.lines.length} lines)`);

  await writeFile(
    resolve(targetDir, "static-data-manifest.json"),
    JSON.stringify({
      generatedAt: new Date().toISOString(),
      referenceSession: { file: "reference-session.json", lineCount: referenceSession.lines.length },
      reviewSampleSession: { file: "review-sample-session.json", lineCount: sampleSession.lines.length },
    }, null, 2),
    "utf-8",
  );
  console.log("[dump-static-data] done");
}

if (!hasSingleInstanceLock && !isCorpusAuditMode && !isDumpStaticDataMode) {
  app.quit();
}

app.on("second-instance", () => {
  if (isCorpusAuditMode || isDumpStaticDataMode) {
    return;
  }
  const requestedPort = Number(process.env.TMDS_WEB_PORT ?? "4173");
  const port = Number.isFinite(requestedPort) && requestedPort > 0 ? requestedPort : 4173;
  const host = process.env.TMDS_WEB_HOST || "127.0.0.1";
  const openHost = host === "0.0.0.0" || host === "::" ? "127.0.0.1" : host;
  void shell.openExternal(`http://${openHost}:${port}/`);
});

app.whenReady().then(() => {
  if (isCorpusAuditMode) {
    const auditPaths = process.argv.slice(auditFlagIndex + 1).filter((value) => !value.startsWith("--"));
    runCorpusAuditCli(auditPaths)
      .catch((error) => {
        console.error(`[audit-corpus] failed ${error instanceof Error ? error.stack ?? error.message : String(error)}`);
        process.exitCode = 1;
      })
      .finally(() => {
        app.quit();
      });
    return;
  }

  if (isDumpStaticDataMode) {
    const dumpArgs = process.argv.slice(dumpFlagIndex + 1).filter((value) => !value.startsWith("--"));
    const outDir = dumpArgs[0] ?? "data";
    runDumpStaticDataCli(outDir)
      .catch((error) => {
        console.error(`[dump-static-data] failed ${error instanceof Error ? error.stack ?? error.message : String(error)}`);
        process.exitCode = 1;
      })
      .finally(() => {
        app.quit();
      });
    return;
  }

  startWebAppServer().catch((error) => {
    console.error(`[webapp] failed ${error instanceof Error ? error.stack ?? error.message : String(error)}`);
    process.exitCode = 1;
    app.quit();
  });
});

app.on("window-all-closed", () => {
  return;
});
