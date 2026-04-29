import type { SourceRecord } from "./types";

export type CanonicalEntity =
  | "source_record"
  | "source_document"
  | "document_section"
  | "workflow_definition"
  | "workflow_relation"
  | "workflow_context_type"
  | "import_run"
  | "database_definition"
  | "database_schema_definition"
  | "database_table_definition"
  | "database_column_definition"
  | "database_key_definition"
  | "database_relation_definition"
  | "database_row_sample"
  | "database_lookup_definition"
  | "genisys_bit_definition"
  | "genisys_byte_definition"
  | "office_header_definition"
  | "field_header_definition"
  | "genisys_message_definition"
  | "icd_version_definition"
  | "icd_message_definition"
  | "icd_field_definition"
  | "code_definition"
  | "message_definition"
  | "field_definition"
  | "enum_definition"
  | "codeline_definition"
  | "codeline_activity_definition"
  | "bit_assignment_definition"
  | "location_definition"
  | "milepost_definition"
  | "cp_definition"
  | "station_definition"
  | "alias_definition"
  | "subsystem_definition"
  | "module_definition"
  | "device_definition"
  | "node_definition"
  | "socket_definition"
  | "cad_definition"
  | "translation_template"
  | "unresolved_token"
  | "relation_map";

export interface EntitySpec {
  name: CanonicalEntity;
  purpose: string;
  fields: string[];
  sourceMapping: string;
  normalizationRules: string[];
  provenanceRules: string[];
  fallbackBehavior: string;
}

export const canonicalEntities: EntitySpec[] = [
  {
    name: "source_record",
    purpose: "Preserve one imported source item with stable provenance.",
    fields: ["id", "kind", "title", "path", "version", "section", "page", "notes"],
    sourceMapping: "Manuals, workflow documents, SQL rows, and sample logs.",
    normalizationRules: ["Keep original path", "Preserve source kind", "Use deterministic IDs"],
    provenanceRules: ["Store source path and extraction timestamp", "Never drop version or section references"],
    fallbackBehavior: "If only a filename exists, store filename and mark the source incomplete.",
  },
  {
    name: "database_definition",
    purpose: "Represent a database catalog imported from the TMDS SQL instance.",
    fields: ["database_name", "server_name", "kind", "source_refs"],
    sourceMapping: "TMDS static and dynamic catalogs plus any supporting admin catalogs.",
    normalizationRules: ["Keep the database name exact", "Separate static and dynamic catalogs"],
    provenanceRules: ["Store the server, login path, and extraction time"],
    fallbackBehavior: "Keep the catalog as unresolved if only a screenshot or config reference exists.",
  },
  {
    name: "database_schema_definition",
    purpose: "Represent a schema within a TMDS database.",
    fields: ["database_name", "schema_name", "source_refs"],
    sourceMapping: "SQL schema enumeration.",
    normalizationRules: ["Preserve schema names exactly"],
    provenanceRules: ["Record the catalog and query used to enumerate the schema"],
    fallbackBehavior: "Keep the schema unresolved until the catalog is readable.",
  },
  {
    name: "database_table_definition",
    purpose: "Represent a table or view that may carry translation-relevant data.",
    fields: ["database_name", "schema_name", "table_name", "object_type", "row_count", "source_refs"],
    sourceMapping: "SQL table enumeration and sampling exports.",
    normalizationRules: ["Record object type explicitly", "Preserve empty-vs-non-empty state"],
    provenanceRules: ["Store the source database, schema, and object name"],
    fallbackBehavior: "Keep the table definition even if the row count is unknown.",
  },
  {
    name: "database_column_definition",
    purpose: "Represent a database column used by translation or workflow mapping.",
    fields: ["database_name", "schema_name", "table_name", "column_name", "data_type", "nullable", "source_refs"],
    sourceMapping: "SQL column metadata.",
    normalizationRules: ["Preserve column order where available", "Keep exact data types"],
    provenanceRules: ["Record the catalog and table that supplied the metadata"],
    fallbackBehavior: "Keep the column as raw metadata when type details cannot be read.",
  },
  {
    name: "database_key_definition",
    purpose: "Represent a primary, unique, or foreign key constraint.",
    fields: ["database_name", "schema_name", "table_name", "key_type", "key_name", "columns", "source_refs"],
    sourceMapping: "SQL metadata views.",
    normalizationRules: ["Store key type separately from key columns"],
    provenanceRules: ["Tie the key to the catalog metadata query"],
    fallbackBehavior: "Leave the key unresolved if the catalog blocks constraint inspection.",
  },
  {
    name: "database_relation_definition",
    purpose: "Represent a foreign-key or lookup relation between tables.",
    fields: ["from_table", "to_table", "relation_type", "join_columns", "source_refs"],
    sourceMapping: "SQL relationships and inferred lookup links.",
    normalizationRules: ["Preserve relation direction"],
    provenanceRules: ["Store the metadata source or source row pair"],
    fallbackBehavior: "Keep the relation unresolved when only a soft lookup is implied.",
  },
  {
    name: "database_row_sample",
    purpose: "Preserve one sampled row from a translation-relevant table.",
    fields: ["database_name", "schema_name", "table_name", "row_key", "sample_json", "source_refs"],
    sourceMapping: "Read-only SQL row sampling.",
    normalizationRules: ["Store row samples as JSON", "Keep deterministic row selection"],
    provenanceRules: ["Capture the query, order, and sample limit"],
    fallbackBehavior: "Skip the row if the table is empty or unreadable.",
  },
  {
    name: "database_lookup_definition",
    purpose: "Represent a lookup table or code-value map used for translation.",
    fields: ["database_name", "schema_name", "table_name", "lookup_key", "lookup_value", "source_refs"],
    sourceMapping: "Static/dynamic TMDS lookups, code maps, and alias tables.",
    normalizationRules: ["Keep key/value direction explicit", "Preserve the long name or prefix as stored"],
    provenanceRules: ["Store the exact source table and row identity"],
    fallbackBehavior: "Keep the raw key when the lookup is not readable.",
  },
  {
    name: "workflow_definition",
    purpose: "Describe an evidence-backed workflow or operational sequence.",
    fields: ["workflow_id", "name", "description", "stage_order", "source_refs"],
    sourceMapping: "Workflow memos, charts, and extracted operational notes.",
    normalizationRules: ["Use explanation-first summaries", "Keep unknown steps explicit"],
    provenanceRules: ["Tie every step to a source record"],
    fallbackBehavior: "If the workflow is not supported by source data, keep the tab unavailable.",
  },
  {
    name: "workflow_relation",
    purpose: "Link workflows, stages, and dependent operational contexts.",
    fields: ["from_workflow", "to_workflow", "relation_type", "evidence", "source_refs"],
    sourceMapping: "Workflow memos, training decks, and runtime traces.",
    normalizationRules: ["Keep relation type explicit", "Preserve directionality"],
    provenanceRules: ["Capture the source sentence or slide that establishes the relation"],
    fallbackBehavior: "Leave the relation unresolved when evidence is indirect.",
  },
  {
    name: "workflow_context_type",
    purpose: "Classify the operational context of a selected line.",
    fields: ["name", "description", "trigger_examples", "source_refs"],
    sourceMapping: "Workflow training, admin training, and operational logs.",
    normalizationRules: ["Keep context types mutually distinct", "Avoid merging dispatcher and maintenance workflows"],
    provenanceRules: ["Tie each context type to a training deck or log family"],
    fallbackBehavior: "Use a generic context label only if no stronger evidence exists.",
  },
  {
    name: "genisys_bit_definition",
    purpose: "Capture a single Genisys bit meaning from source manuals.",
    fields: ["byte_offset", "bit_index", "meaning", "conditions", "source_refs"],
    sourceMapping: "Genisys manuals and any verified packet samples.",
    normalizationRules: ["Store byte and bit coordinates exactly", "Keep message family scope"],
    provenanceRules: ["Record page/section where available"],
    fallbackBehavior: "Mark as unresolved when the manual does not define the bit.",
  },
  {
    name: "icd_message_definition",
    purpose: "Describe a versioned ICD message with layout and semantics.",
    fields: ["icd_version", "message_name", "direction", "fields", "source_refs"],
    sourceMapping: "ICD PDFs and related vendor docs.",
    normalizationRules: ["Version the message name", "Preserve field order"],
    provenanceRules: ["Store PDF title and page number where available"],
    fallbackBehavior: "Keep the raw message label when semantics are unclear.",
  },
  {
    name: "translation_template",
    purpose: "Define a reusable explanation pattern for decoded messages.",
    fields: ["pattern", "structured_layers", "english_output", "trace_refs"],
    sourceMapping: "Manuals, SQL lookups, and verified runtime evidence.",
    normalizationRules: ["Prefer traceable statements", "No speculation"],
    provenanceRules: ["Capture why the template was selected"],
    fallbackBehavior: "Emit unresolved tokens instead of guessing meaning.",
  },
  {
    name: "code_definition",
    purpose: "Represent a code/control/indication token with its prefix and long-name translation.",
    fields: ["code", "prefix", "long_name", "family", "source_refs"],
    sourceMapping: "TMDS static/dynamic lookup tables, Code Server logs, and admin configuration exports.",
    normalizationRules: ["Preserve the raw code token", "Store the display prefix separately from the long name"],
    provenanceRules: ["Store the source table or source log line that supplied the mapping"],
    fallbackBehavior: "Keep the raw code token and leave the long name unresolved if no source mapping exists.",
  },
  {
    name: "codeline_definition",
    purpose: "Represent a code line, its stations, and its operational role.",
    fields: ["line_id", "name", "stations", "protocol", "source_refs"],
    sourceMapping: "Code Server training, code-line logs, and database lookups.",
    normalizationRules: ["Separate line identity from station identity"],
    provenanceRules: ["Tie to the code-line file or code-server log entry"],
    fallbackBehavior: "Keep the line identifier raw until the mapping is confirmed.",
  },
  {
    name: "codeline_activity_definition",
    purpose: "Represent a code-line activity sample such as control, indication, recall, resend, or failure.",
    fields: ["line_id", "activity_type", "payload", "bit_length", "source_refs"],
    sourceMapping: "Code Server logs, code-line event logs, and socket traces.",
    normalizationRules: ["Preserve the original payload string", "Keep the displayed bit length exactly as logged"],
    provenanceRules: ["Store the raw log line and the code line number"],
    fallbackBehavior: "Keep the activity as an unresolved raw line when the action type is not explicit.",
  },
  {
    name: "bit_assignment_definition",
    purpose: "Map a bit position or bit range to a named control or indication meaning.",
    fields: ["code", "line_id", "station", "bit_offset", "bit_length", "meaning", "prefix", "long_name", "source_refs"],
    sourceMapping: "TMDS dynamic/static lookup tables, code-line logs, and code-server function assignments.",
    normalizationRules: ["Preserve original bit offsets and lengths", "Keep control and indication assignments distinct"],
    provenanceRules: ["Store the source row, table, or line that asserts the assignment"],
    fallbackBehavior: "Leave the assignment unresolved and retain the raw payload when the source table is unavailable.",
  },
  {
    name: "icd_version_definition",
    purpose: "Capture a distinct ICD version family and revision metadata.",
    fields: ["document_title", "revision", "date", "scope", "source_refs"],
    sourceMapping: "ICD PDFs, operational PDFs, and export metadata.",
    normalizationRules: ["Preserve exact revision labels", "Do not merge revisions"],
    provenanceRules: ["Store the opening-page metadata and any extracted version text"],
    fallbackBehavior: "Keep the document title only if revision metadata cannot be extracted.",
  },
  {
    name: "location_definition",
    purpose: "Represent a named operational location used in logs and workflow context.",
    fields: ["name", "alias", "kind", "source_refs"],
    sourceMapping: "Track editor, code-server logs, CAD, and database lookups.",
    normalizationRules: ["Keep the displayed location name intact"],
    provenanceRules: ["Tie each alias to its originating source"],
    fallbackBehavior: "Store the raw token when no alias is known.",
  },
  {
    name: "subsystem_definition",
    purpose: "Represent an operational subsystem such as BOS, CODE, CTRL, MDM, CAD, or WebApp.",
    fields: ["name", "role", "source_refs"],
    sourceMapping: "TMDS training decks, manuals, and logs.",
    normalizationRules: ["Keep subsystem boundaries explicit"],
    provenanceRules: ["Record the source deck or manual sentence"],
    fallbackBehavior: "Use the raw subsystem label when no role text is extracted.",
  },
];

export function seedSourceRecords(): SourceRecord[] {
  return [
    {
      id: "music:03_genisys_manuals",
      kind: "manual",
      title: "Genisys manuals archive",
      path: "C:\\Users\\Ji\\Music\\03_GenisysManuals.zip",
      notes: "Contains the primary Genisys manuals folder.",
    },
    {
      id: "music:icd_ptc_zip",
      kind: "icd",
      title: "ICD PDF for PTC archive",
      path: "C:\\Users\\Ji\\Music\\ICD PDF FOR PTC.zip",
      notes: "Contains multiple ICD PDFs and version variants.",
    },
    {
      id: "music:tmds_memo",
      kind: "workflow",
      title: "TMDS Technical Memo - Near Side Signal Control",
      path: "C:\\Users\\Ji\\Music\\TMDS Technical Memo - Near Side Signal Control.pdf",
      notes: "Primary workflow/context source identified in Music.",
    },
    {
      id: "music:tcos_flow",
      kind: "workflow",
      title: "TCOS Flow",
      path: "C:\\Users\\Ji\\Music\\TCOS TRAINING\\TCOS Flow.pptx",
      notes: "TMDS component overview and operational flow deck.",
    },
    {
      id: "music:cad_training",
      kind: "workflow",
      title: "CAD Training",
      path: "C:\\Users\\Ji\\Music\\TCOS TRAINING\\CAD Training.pptx",
      notes: "Dispatcher workflow deck covering sign-in, territory loading, and near-side controls.",
    },
    {
      id: "music:bos_training",
      kind: "workflow",
      title: "BOS System Administrator Training",
      path: "C:\\Users\\Ji\\Music\\TCOS TRAINING\\NCTD Training Materials-System Administrator - Instructor's Guide BOS Presentation.pptx",
      notes: "Back Office Server workflow deck covering trains, logs, and message flow.",
    },
    {
      id: "music:code_training",
      kind: "workflow",
      title: "Code Server Training",
      path: "C:\\Users\\Ji\\Music\\TCOS TRAINING\\Training Materials-System Administrator - Instructor's Guide CODE Presentation - QC.pptx",
      notes: "Code Server workflow deck covering code lines and protocol emulation.",
    },
    {
      id: "music:code_server_build",
      kind: "sample_log",
      title: "01_CodeServerBuild archive",
      path: "C:\\Users\\Ji\\Music\\01_CodeServerBuild.zip",
      notes: "Code Server build wrapper containing code-line event logs, socket traces, and runtime binaries.",
    },
    {
      id: "music:code_server_backups",
      kind: "sample_log",
      title: "CodeServer backup event logs",
      path: "C:\\Users\\Ji\\Music\\CodeServer-BackupEventLogs-03-02-26.zip",
      notes: "Code Server backup log bundle with indication/recall/control samples and code-line statistics.",
    },
    {
      id: "music:ted_training",
      kind: "workflow",
      title: "Track Editor Training",
      path: "C:\\Users\\Ji\\Music\\TCOS TRAINING\\Training Materials-System Administrator - Instructor's Guide TED Presentation.pptx",
      notes: "Track Editor workflow deck covering territories, control points, and devices.",
    },
    {
      id: "music:mdm_training",
      kind: "workflow",
      title: "MDM Training",
      path: "C:\\Users\\Ji\\Music\\TCOS TRAINING\\MDM Training.pptx",
      notes: "MDM workflow deck covering file transfers, versioning, and queue behavior.",
    },
    {
      id: "music:admin_client_training",
      kind: "workflow",
      title: "Admin Client Training",
      path: "C:\\Users\\Ji\\Music\\TCOS TRAINING\\Admin Client Training Presentation.pptx",
      notes: "Admin workflow deck covering database-facing maintenance functions.",
    },
    {
      id: "music:webapp_training",
      kind: "workflow",
      title: "WebApp Training",
      path: "C:\\Users\\Ji\\Music\\TCOS TRAINING\\WebApp Training Presentation.pptx",
      notes: "WebApp workflow deck covering configuration, users, roles, and data maintenance.",
    },
    {
      id: "music:more_data",
      kind: "workflow",
      title: "More Data operational reports",
      path: "C:\\Users\\Ji\\Music\\More Data",
      notes: "Operational report wrapper with version-bearing PDFs and back-office logs.",
    },
    {
      id: "music:more_stuff",
      kind: "sample_log",
      title: "More Stuff server logs",
      path: "C:\\Users\\Ji\\Music\\More Stuff",
      notes: "Server log wrapper with CodeServer, CommServer, ControlServer, and MDM traces.",
    },
    {
      id: "music:more_boc",
      kind: "sample_log",
      title: "MORE BOC back-office control logs",
      path: "C:\\Users\\Ji\\Music\\MORE BOC",
      notes: "Back-office control log wrapper with operational trace bundles.",
    },
  ];
}
