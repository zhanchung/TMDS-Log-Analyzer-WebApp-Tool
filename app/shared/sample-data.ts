import type { DetailModel, ParsedLine } from "./types";
import { seedSourceRecords } from "./knowledge-model";

export const sampleLines: ParsedLine[] = [
  {
    id: "sample-1",
    lineNumber: 1,
    timestamp: "2026-02-17 00:00:06.454",
    source: "sample_logs/curated/sockettrace_sample.log",
    raw: "02-17-2026 00:00:06.454     W >> 172.20.20.61 DATA:  KEEPALIVE  clsClientC,StreamRece,  349:",
    tokens: ["KEEPALIVE", "172.20.20.61", "StreamRece"],
  },
  {
    id: "sample-2",
    lineNumber: 2,
    timestamp: "2026-02-17 00:00:47.642",
    source: "sample_logs/curated/genisys_sample.log",
    raw: "02-17-2026 00:00:47.642      W << INDICATION;21:200015:01:000000000000000000000000000000000000000000000000 FOR CODESTATION:PALOMAR",
    tokens: ["INDICATION", "21", "200015", "PALOMAR"],
  },
  {
    id: "sample-3",
    lineNumber: 3,
    timestamp: "2026-02-26 00:00:21.346",
    source: "sample_logs/curated/sockettrace_sample.log",
    raw: "02-26-2026 00:00:21.346     W << 172.20.20.61 DATA:  INDICATION;5:100001:01:0010010000000100  clsClientC,  SendData,  295:",
    tokens: ["INDICATION", "5", "100001", "0010010000000100"],
  },
];

export const sampleDetail: DetailModel = {
  lineId: sampleLines[2].id,
  lineNumber: sampleLines[2].lineNumber,
  timestamp: sampleLines[2].timestamp,
  raw: sampleLines[2].raw,
  translation: {
    original: sampleLines[2].raw,
    structured: [
      "prefix=INDICATION",
      "codeLine=5",
      "codeStation=CP SONGS",
      "payloadBits=0010010000000100",
    ],
    english: ["CodeServer indication recorded for code station CP SONGS."],
    unresolved: ["Long-name lookup table not yet imported from TMDS dynamic/static."],
  },
  workflow: {
    summary: "CodeServer indication, recall, and control traffic is present for a named code station. The workflow explanation should stay tied to the code-line evidence, not nearby logs.",
    currentStep: "CodeServer indication observation",
    priorStep: "Unknown",
    nextStep: "Unknown",
    systems: ["CodeServer", "CodeLine"],
    objects: ["CP SONGS", "Code line 5"],
    knownState: "Code-server control/indication cycle active",
    unresolved: ["Long-name mapping table not yet imported from TMDS dynamic/static.", "No broader workflow context has been inferred from this single sample line."],
  },
  sourceReferences: seedSourceRecords(),
};
