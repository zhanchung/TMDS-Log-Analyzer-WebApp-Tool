export type SourceKind =
  | "manual"
  | "workflow"
  | "icd"
  | "database"
  | "sample_log"
  | "old_project"
  | "generated";

export interface SourceRecord {
  id: string;
  kind: SourceKind;
  title: string;
  path: string;
  version?: string;
  section?: string;
  page?: string;
  notes?: string;
}

export interface ParsedLine {
  id: string;
  lineNumber: number;
  timestamp?: string;
  source?: string;
  raw: string;
  tokens: string[];
}

export interface TranslationLayer {
  original: string;
  structured: string[];
  english: string[];
  unresolved: string[];
}

export interface WorkflowStep {
  label: string;
  value: string;
}

export interface WorkflowFrame {
  summary: string;
  currentStep: string;
  priorStep?: string;
  nextStep?: string;
  systems: string[];
  objects: string[];
  knownState: string;
  unresolved: string[];
}

export interface ReferenceChoiceItem {
  id: string;
  label: string;
  content: string[];
  detailChoiceGroups?: ReferenceChoiceGroup[];
}

export interface ReferenceChoiceGroup {
  id: string;
  label: string;
  layout?: "wrap" | "horizontal" | "column";
  selectionMode?: "single" | "multiple";
  items: ReferenceChoiceItem[];
}

export interface RelatedPairDetail {
  lineId: string;
  lineNumber: number;
  raw: string;
  relationLabel: string;
  deltaLabel: string;
  summary: string;
  reason: string;
}

export interface WorkflowRelatedDetail {
  lineId: string;
  lineNumber: number;
  timestamp?: string;
  raw: string;
  deltaLabel: string;
  relation: string;
}

export interface ReferenceArtifact {
  kind: "pdf";
  path: string;
  title: string;
  subtitle?: string;
}

export interface ReferenceDiagramStep {
  id: string;
  messageId: string;
  title: string;
  section: string;
  direction: string;
  fromLane: string;
  toLane: string;
  description?: string;
  previousMessageId?: string;
  nextMessageId?: string;
}

export interface ReferenceDiagram {
  kind: "message-exchange";
  title: string;
  subtitle?: string;
  lanes: string[];
  steps: ReferenceDiagramStep[];
}

export interface DetailModel {
  lineId: string;
  lineNumber: number;
  timestamp?: string;
  raw: string;
  translation: TranslationLayer;
  workflow: WorkflowFrame;
  genisysContext?: string[];
  icdContext?: string[];
  databaseContext?: string[];
  workflowContext?: string[];
  workflowRelated?: WorkflowRelatedDetail[];
  payloadContext?: string[];
  relatedPair?: RelatedPairDetail;
  relatedTimeline?: string[];
  referenceBadges?: string[];
  referenceArtifact?: ReferenceArtifact;
  referenceDiagram?: ReferenceDiagram;
  referenceChoiceGroups?: ReferenceChoiceGroup[];
  sourceReferences: SourceRecord[];
}

export interface SessionData {
  sessionId?: string;
  lines: ParsedLine[];
  detail: DetailModel | null;
  lineDetails?: Record<string, DetailModel>;
}

export interface WorkspaceProgress {
  phase: string;
  message: string;
  percent: number;
  completed: number;
  total: number;
  currentPath?: string;
}

export interface SearchConfig {
  query: string;
  caseSensitive: boolean;
  wholeWord: boolean;
  regex: boolean;
  wrapAround: boolean;
  filterOnlyMatches: boolean;
}
