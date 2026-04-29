from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[2]
EXPORTS_DIR = REPO_ROOT / "exports" / "mappings"


ICD_SOURCES = [
    {
        "path": REPO_ROOT / "exports" / "manuals" / "icd_training" / "WCR-ICD-1214_Office-Locomotive_Segment_ICD__3.1_.txt",
        "family": "office_locomotive",
        "release": "3.1",
        "document_title": "PTC Office-Locomotive Segment ICD",
    },
    {
        "path": REPO_ROOT / "exports" / "manuals" / "icd_training" / "Office-Locomotive_Segment_ICD__2.11.1_.txt",
        "family": "office_locomotive",
        "release": "2.11.1",
        "document_title": "PTC Office-Locomotive Segment ICD",
    },
    {
        "path": REPO_ROOT / "exports" / "manuals" / "icd" / "WCR-ICD-1095_PTC_On-Board_Segment_-_Energy_Management_ICD__1.9_.txt",
        "family": "energy_management",
        "release": "1.9",
        "document_title": "PTC On-board Segment-Energy Management ICD",
    },
    {
        "path": REPO_ROOT / "exports" / "manuals" / "icd_training" / "I-ETMS_On-Board_Data_Distribution_Messages_ICD_v1.1.txt",
        "family": "data_distribution",
        "release": "1.1",
        "document_title": "I-ETMS On-Board Segment Data Distribution Messages ICD",
    },
]


TOC_PATTERNS = [
    re.compile(
        r"^(?P<section>\d+(?:\.\d+)+)\s+\((?P<message_id>\d{4,5})\)\s+(?P<message_name>.+?)\s+[^A-Za-z0-9]*Version\s+(?P<message_version>\d+)\s*\.{2,}\s*(?P<page>\d+)\s*$"
    ),
    re.compile(
        r"^(?P<section>\d+(?:\.\d+)+)\s+(?P<message_name>.+?)\s+\((?P<message_id>\d{4,5})\)\s+[^A-Za-z0-9]*Version\s+(?P<message_version>\d+)\s*\.{2,}\s*(?P<page>\d+)\s*$"
    ),
]


DETAIL_PATTERNS = [
    re.compile(
        r"^(?P<section>\d+(?:\.\d+)+)\s+(?:(?P<detail_code>[A-Za-z0-9-]+)\s+)?\((?P<message_id>\d{4,5})\)\s+(?P<message_name>.+?)\s+[^A-Za-z0-9]*Version\s+(?P<message_version>\d+)\s*$"
    ),
    re.compile(
        r"^(?P<section>\d+(?:\.\d+)+)\s+(?P<detail_code>[A-Za-z0-9-]+)\s+(?P<message_name>.+?)\s+\((?P<message_id>\d{4,5})\)\s+[^A-Za-z0-9]*Version\s+(?P<message_version>\d+)\s*$"
    ),
]


def normalize_whitespace(value: str) -> str:
    value = value.replace("\u2013", "-").replace("\u2014", "-")
    value = value.replace("\u00a0", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def infer_direction(family: str, message_id: str) -> str:
    if family == "office_locomotive":
        if message_id.startswith("01"):
            return "office_to_locomotive"
        if message_id.startswith("02"):
            return "locomotive_to_office"
    if family == "energy_management":
        if message_id.startswith("05"):
            return "ptc_to_energy_management"
        if message_id.startswith("06"):
            return "energy_management_to_ptc"
    if family == "data_distribution":
        if message_id.startswith("03"):
            return "onboard_broadcast"
    return "unknown"


def parse_icd_catalog() -> list[dict]:
    records: list[dict] = []
    seen: set[tuple[str, str, str]] = set()

    for source in ICD_SOURCES:
        text = source["path"].read_text(encoding="utf-8", errors="ignore")
        for line_number, raw_line in enumerate(text.splitlines(), start=1):
            line = normalize_whitespace(raw_line)
            if not line or "Version" not in line or "(" not in line or ")" not in line:
                continue

            match = None
            for pattern in TOC_PATTERNS:
                match = pattern.match(line)
                if match:
                    break

            if not match:
                continue

            message_id = match.group("message_id")
            record_key = (source["family"], source["release"], message_id)
            if record_key in seen:
                continue

            seen.add(record_key)
            records.append(
                {
                    "document_family": source["family"],
                    "document_title": source["document_title"],
                    "release": source["release"],
                    "source_file": str(source["path"].relative_to(REPO_ROOT)),
                    "line_number": line_number,
                    "section": match.group("section"),
                    "message_id": message_id,
                    "message_name": normalize_whitespace(match.group("message_name")),
                    "message_version": int(match.group("message_version")),
                    "page": int(match.group("page")),
                    "direction": infer_direction(source["family"], message_id),
                    "source_line": line,
                }
            )

    records.sort(
        key=lambda row: (
            row["document_family"],
            row["release"],
            row["direction"],
            int(row["message_id"]),
        )
    )
    return records


def extract_labeled_block(lines: list[str], label: str, stop_labels: set[str]) -> str:
    label_index = -1
    for index, line in enumerate(lines):
        if line.startswith(label):
            label_index = index
            break

    if label_index == -1:
        return ""

    collected: list[str] = [lines[label_index][len(label):].strip()]
    for next_line in lines[label_index + 1 :]:
        if next_line in stop_labels:
            break
        if re.match(r"^[A-Za-z][A-Za-z /-]+:$", next_line):
            break
        if re.match(r"^\d+(?:\.\d+)+", next_line):
            break
        if next_line.startswith("PTC ") or next_line.startswith("Distribution limited"):
            continue
        if re.match(r"^-\d+-$", next_line):
            continue
        collected.append(next_line)

    return normalize_whitespace(" ".join(part for part in collected if part))


def extract_functional_content_excerpt(lines: list[str]) -> str:
    start_index = -1
    for index, line in enumerate(lines):
        if line.startswith("Functional Content:"):
            start_index = index + 1
            break
    if start_index == -1:
        return ""

    collected: list[str] = []
    for line in lines[start_index:]:
        if line in {"[Constraints]", "[Unique error handling requirements]", "[Design notes]"}:
            break
        if re.match(r"^\d+(?:\.\d+)+", line):
            break
        if line.startswith("PTC ") or line.startswith("Distribution limited"):
            continue
        if re.match(r"^-\d+-$", line):
            continue
        collected.append(line)

    excerpt = normalize_whitespace(" ".join(collected))
    return excerpt[:2000]


def parse_icd_message_details(catalog_records: Iterable[dict]) -> list[dict]:
    catalog_index = {
        (record["document_family"], record["release"], record["message_id"]): record
        for record in catalog_records
    }
    details: list[dict] = []

    for source in ICD_SOURCES:
        raw_lines = source["path"].read_text(encoding="utf-8", errors="ignore").splitlines()
        normalized_lines = [normalize_whitespace(line) for line in raw_lines]
        current_block: dict | None = None

        for line_number, line in enumerate(normalized_lines, start=1):
            if not line or "Version" not in line or "(" not in line or ")" not in line:
                if current_block is not None:
                    current_block["block_lines"].append(line)
                continue

            match = None
            for pattern in DETAIL_PATTERNS:
                match = pattern.match(line)
                if match:
                    break

            if match:
                if current_block is not None:
                    details.append(current_block)
                current_block = {
                    "document_family": source["family"],
                    "document_title": source["document_title"],
                    "release": source["release"],
                    "source_file": str(source["path"].relative_to(REPO_ROOT)),
                    "heading_line_number": line_number,
                    "section": match.group("section"),
                    "detail_code": normalize_whitespace(match.groupdict().get("detail_code") or ""),
                    "message_id": match.group("message_id"),
                    "detail_message_name": normalize_whitespace(match.group("message_name")),
                    "detail_message_version": int(match.group("message_version")),
                    "source_heading_line": line,
                    "block_lines": [],
                }
            elif current_block is not None:
                current_block["block_lines"].append(line)

        if current_block is not None:
            details.append(current_block)

    normalized_details: list[dict] = []
    stop_labels = {"Functional Content:", "[Constraints]", "[Unique error handling requirements]", "[Design notes]"}
    for detail in details:
        block_lines = [line for line in detail.pop("block_lines") if line]
        description = extract_labeled_block(block_lines, "Description:", stop_labels)
        functional_content_excerpt = extract_functional_content_excerpt(block_lines)
        related_message_ids = sorted(set(re.findall(r"\((\d{4,5})\)", " ".join(block_lines))))

        toc_record = catalog_index.get((detail["document_family"], detail["release"], detail["message_id"]))
        toc_message_name = toc_record["message_name"] if toc_record else ""
        toc_message_version = toc_record["message_version"] if toc_record else None
        toc_page = toc_record["page"] if toc_record else None
        version_conflict = (
            bool(toc_message_version is not None and toc_message_version != detail["detail_message_version"])
        )

        normalized_details.append(
            {
                "document_family": detail["document_family"],
                "document_title": detail["document_title"],
                "release": detail["release"],
                "source_file": detail["source_file"],
                "heading_line_number": detail["heading_line_number"],
                "section": detail["section"],
                "detail_code": detail["detail_code"],
                "message_id": detail["message_id"],
                "toc_message_name": toc_message_name,
                "toc_message_version": toc_message_version,
                "toc_page": toc_page,
                "detail_message_name": detail["detail_message_name"],
                "detail_message_version": detail["detail_message_version"],
                "direction": infer_direction(detail["document_family"], detail["message_id"]),
                "description": description,
                "related_message_ids": ",".join(related_message_ids),
                "functional_content_excerpt": functional_content_excerpt,
                "has_functional_content": bool(functional_content_excerpt),
                "version_conflict_with_catalog": version_conflict,
                "source_heading_line": detail["source_heading_line"],
            }
        )

    normalized_details.sort(
        key=lambda row: (
            row["document_family"],
            row["release"],
            row["direction"],
            int(row["message_id"]),
            row["heading_line_number"],
        )
    )
    return normalized_details


def build_office_locomotive_comparison(records: Iterable[dict]) -> list[dict]:
    office_records = [row for row in records if row["document_family"] == "office_locomotive"]
    by_release: dict[str, dict[str, dict]] = {"2.11.1": {}, "3.1": {}}
    for record in office_records:
        by_release.setdefault(record["release"], {})[record["message_id"]] = record

    message_ids = sorted(
        set(by_release.get("2.11.1", {}).keys()) | set(by_release.get("3.1", {}).keys()),
        key=int,
    )
    comparison: list[dict] = []
    for message_id in message_ids:
        old = by_release.get("2.11.1", {}).get(message_id)
        new = by_release.get("3.1", {}).get(message_id)
        old_version = old["message_version"] if old else None
        new_version = new["message_version"] if new else None

        if old and new:
            if old["message_name"] != new["message_name"]:
                status = "name_changed"
            elif old_version != new_version:
                status = "version_changed"
            else:
                status = "unchanged"
        elif old:
            status = "removed_after_2.11.1"
        else:
            status = "added_by_3.1"

        comparison.append(
            {
                "message_id": message_id,
                "direction": old["direction"] if old else new["direction"],
                "status": status,
                "release_2_11_1_name": old["message_name"] if old else "",
                "release_2_11_1_version": old_version,
                "release_2_11_1_page": old["page"] if old else None,
                "release_3_1_name": new["message_name"] if new else "",
                "release_3_1_version": new_version,
                "release_3_1_page": new["page"] if new else None,
                "version_delta": (new_version - old_version) if old_version is not None and new_version is not None else None,
            }
        )
    return comparison


def build_genisys_reference() -> dict:
    return {
        "document_family": "genisys_protocol",
        "sources": [
            "exports/manuals/genisys/Reading_Genisys_Data-DRAFT_1.txt",
            "exports/manuals/genisys/P2346F_Genisys_Code_System.txt",
            "exports/manuals/genisys/Genisys_Trace_Analysis.ocr.txt",
        ],
        "framing": {
            "message_terminator": "0xF6",
            "station_address_position": 2,
            "data_escape_trigger": "0xF0",
            "data_escape_rule": "bytes from 0xF0 to 0xFF inside the frame are split into two transmitted bytes",
            "mode_byte": "0xE0",
        },
        "office_headers": [
            {"byte": "0xFA", "meaning": "Acknowledge"},
            {"byte": "0xFB", "meaning": "Poll"},
            {"byte": "0xFC", "meaning": "Control"},
            {"byte": "0xFD", "meaning": "Recall"},
            {"byte": "0xFE", "meaning": "Execute"},
        ],
        "field_headers": [
            {"byte": "0xF1", "meaning": "Poll Response No Data"},
            {"byte": "0xF2", "meaning": "Poll Response With Data"},
            {"byte": "0xF3", "meaning": "Control Response With Data"},
        ],
        "mode_examples": [
            {"bytes": ["0xE0", "0x07"], "meaning": "non-secure polling and control checkback"},
            {"bytes": ["0xE0", "0x05"], "meaning": "non-secure polling and no control checkback"},
        ],
        "mode_scope_note": "TMDS reading guide states the TMDS wayside server supports non-secure polling; the general Genisys protocol manuals also describe secure-poll behavior. Translation must follow actual mode bytes and TMDS-specific configuration, not a single global assumption.",
        "mode_bit_definitions": [
            {"bit": 0, "meaning": "Data Base Complete"},
            {"bit": 1, "meaning": "Checkback Control Enable"},
            {"bit": 2, "meaning": "Secure Poll Enable"},
            {"bit": 3, "meaning": "Common Command Enable"},
        ],
        "trace_record_types": [
            {"record_type": "A", "meaning": "Acknowledgement"},
            {"record_type": "P", "meaning": "Poll"},
            {"record_type": "R", "meaning": "Recall"},
            {"record_type": "C", "meaning": "Control"},
            {"record_type": "E", "meaning": "Execute"},
        ],
        "trace_analysis_rules": [
            {
                "rule_id": "trace_record_structure",
                "meaning": "A trace record contains a sent office message, a received field message, and a trailing information line."
            },
            {
                "rule_id": "trace_info_line_fields",
                "meaning": "The information line contains timestamp, trace record type, code line number/name, and station number/name."
            },
            {
                "rule_id": "control_word_value_pairs",
                "meaning": "Control messages send only the control words that need to change, as [word number] [value] pairs."
            },
            {
                "rule_id": "trace_word_numbering",
                "meaning": "Word numbering starts with 0."
            },
            {
                "rule_id": "trace_bit_order_reversal",
                "meaning": "Trace data values are bit-reversed from the code system database tabling order before device names are matched."
            },
            {
                "rule_id": "unused_bits_hidden",
                "meaning": "Unused bits are not shown in the trace bit-state display."
            },
            {
                "rule_id": "indication_trace_full_station",
                "meaning": "The trace bit-state display shows all indication bit states for the station even when the field only sends changed indication words."
            },
            {
                "rule_id": "recall_returns_full_indications",
                "meaning": "A recall returns all indications for the station."
            },
            {
                "rule_id": "reserved_f_escape",
                "meaning": "Data or checksum bytes starting with F are split into two transmitted bytes, such as F8 -> F0 08 and F0 -> F0 00."
            },
            {
                "rule_id": "ff_not_allowed",
                "meaning": "FF is undefined and not allowed."
            },
        ],
        "message_forms": {
            "transit_messages": [
                {"name": "Acknowledge", "form": "xF1 st xF6"},
                {"name": "Change", "form": "xF2 st {bn bi} cl ch xF6"},
                {"name": "Control Checkback", "form": "xF3 st {bn bi} cl ch xF6"},
            ],
            "receive_messages": [
                {"name": "Common Controls", "form": "xF9"},
                {"name": "Acknowledge", "form": "xFA st cl ch xF6"},
                {"name": "Poll", "form": "xFB st cl ch xF6 in secure mode; xFB st xF6 in non-secure mode"},
                {"name": "Control", "form": "xFC st {bn bi} cl ch xF6"},
                {"name": "Recall", "form": "xFD st cl ch xF6"},
                {"name": "Execute", "form": "xFE st cl ch xF6"},
            ],
        },
        "numbering_rules": {
            "word_zero_is_first_word": True,
            "word_zero_contains_bits": "1-8",
            "display_order": "least-significant-bit first",
            "example": "0x59 -> 01011001 binary -> 10011010 displayed",
        },
        "sample_caa_mapping": {
            "scope": "sample manual mapping only; not an NCTD site-specific assignment",
            "indications": [
                {"bit": 1, "name": "2NWK"},
                {"bit": 2, "name": "2RWK"},
                {"bit": 3, "name": "1EGK"},
                {"bit": 4, "name": "1WGK"},
                {"bit": 5, "name": "2TPK"},
                {"bit": 6, "name": "EATK"},
                {"bit": 7, "name": "WATK"},
                {"bit": 8, "name": "SATK"},
                {"bit": 9, "name": "2ATK"},
                {"bit": 10, "name": "TROUBLE"},
                {"bit": 11, "name": "2LK"},
            ],
            "controls": [
                {"bit": 1, "name": "2NW-CS"},
                {"bit": 2, "name": "2RW-CS"},
                {"bit": 3, "name": "1WGZ-CS"},
                {"bit": 4, "name": "1EGZ-CS"},
                {"bit": 5, "name": "1GZCAN-CS"},
                {"bit": 6, "name": "MCON-CS"},
                {"bit": 7, "name": "MCOFF-CS"},
            ],
            "special_control_bits": [
                {"bit": 1, "name": "EXECUTE REC"},
                {"bit": 2, "name": "RECALL REC"},
                {"bit": 3, "name": "CONTROL REC"},
                {"bit": 4, "name": "POLL REC"},
                {"bit": 5, "name": "ACK REC"},
                {"bit": 6, "name": "COMMON CONTROLS REC"},
                {"bit": 7, "name": "SENT ACK"},
                {"bit": 8, "name": "SENT CHANGE"},
                {"bit": 9, "name": "CONTROL CHECKBACK SENT"},
                {"bit": 10, "name": "SENT CONTROL CONTAINING ALL BITS"},
            ],
        },
        "manual_code_indication_booleans": [
            {
                "application": "CODE INDICATIONS",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-11",
                "mnemonic": "2NWK",
                "expression": "(2NWC)",
            },
            {
                "application": "CODE INDICATIONS",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-11",
                "mnemonic": "2RWK",
                "expression": "(2RWC)",
            },
            {
                "application": "CODE INDICATIONS",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-11",
                "mnemonic": "1WGK",
                "expression": "(.N.1WRGP)",
            },
            {
                "application": "CODE INDICATIONS",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-11",
                "mnemonic": "1EGK",
                "expression": "(.N.1EARGP + .N.1EBRGP)",
            },
            {
                "application": "CODE INDICATIONS",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-11",
                "mnemonic": "2TPK",
                "expression": "(.N.2TPP)",
            },
            {
                "application": "CODE INDICATIONS",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-11",
                "mnemonic": "EATK",
                "expression": "(.N.ET)",
            },
            {
                "application": "CODE INDICATIONS",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-11",
                "mnemonic": "WATK",
                "expression": "(.N.WT)",
            },
            {
                "application": "CODE INDICATIONS",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-11",
                "mnemonic": "SATK",
                "expression": "(.N.ST)",
            },
            {
                "application": "CODE INDICATIONS",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-11",
                "mnemonic": "2ATK",
                "expression": "(.N.2TPP * 2RWC)",
            },
            {
                "application": "CODE INDICATIONS",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-11",
                "mnemonic": "2LK",
                "expression": "(.N.2L)",
            },
        ],
        "manual_local_control_panel_outputs": [
            {
                "application": "LOCAL CONTROL PANEL LIGHTING",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-10 to B-11",
                "slot": 4,
                "output_name": "1WGE-NVO",
                "flash_output_name": "1WGE-F",
                "source_boolean": "1WGE",
                "expression": "(.N.1WRGP + .N.VRDFRNT-DI)",
            },
            {
                "application": "LOCAL CONTROL PANEL LIGHTING",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-10 to B-11",
                "slot": 8,
                "output_name": "1EAGE-NVO",
                "flash_output_name": "1EAGE-F",
                "source_boolean": "1EAGE",
                "expression": "(.N.1EARGP * 2NWC + .N.VRDFRNT-DI)",
            },
            {
                "application": "LOCAL CONTROL PANEL LIGHTING",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-10 to B-11",
                "slot": 9,
                "output_name": "1EBGE-NVO",
                "flash_output_name": "1EBGE-F",
                "source_boolean": "1EBGE",
                "expression": "(.N.1EBRGP + .N.VRDFRNT-DI)",
            },
            {
                "application": "LOCAL CONTROL PANEL LIGHTING",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-10 to B-11",
                "slot": 10,
                "output_name": "2NE-NVO",
                "flash_output_name": "2NE-F",
                "source_boolean": "2NE",
                "expression": "(2NWZ)",
            },
            {
                "application": "LOCAL CONTROL PANEL LIGHTING",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-10 to B-11",
                "slot": 12,
                "output_name": "2RE-NVO",
                "flash_output_name": "2RE-F",
                "source_boolean": "2RE",
                "expression": "(2RWZ)",
            },
            {
                "application": "LOCAL CONTROL PANEL LIGHTING",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-10 to B-11",
                "slot": 18,
                "output_name": "1WAE-NVO",
                "flash_output_name": "1WAE-F",
                "source_boolean": "1WAE",
                "expression": "(.N.ET)",
            },
            {
                "application": "LOCAL CONTROL PANEL LIGHTING",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-10 to B-11",
                "slot": 19,
                "output_name": "1EAAE-NVO",
                "flash_output_name": "1EAAE-F",
                "source_boolean": "1EAAE",
                "expression": "(.N.WT)",
            },
            {
                "application": "LOCAL CONTROL PANEL LIGHTING",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-10 to B-11",
                "slot": 20,
                "output_name": "1EBAE-NVO",
                "flash_output_name": "1EBAE-F",
                "source_boolean": "1EBAE",
                "expression": "(.N.ST)",
            },
            {
                "application": "LOCAL CONTROL PANEL LIGHTING",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-10 to B-11",
                "slot": 21,
                "output_name": "2TE-NVO",
                "flash_output_name": "",
                "source_boolean": "2TE",
                "expression": "(.N.2TPP)",
            },
            {
                "application": "LOCAL CONTROL PANEL LIGHTING",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-10 to B-11",
                "slot": 29,
                "output_name": "LOCAL-NVO",
                "flash_output_name": "",
                "source_boolean": "LOCALE",
                "expression": "(.N.REMOTE-SLO)",
            },
            {
                "application": "LOCAL CONTROL PANEL LIGHTING",
                "source_scope": "sample manual mapping only; not an NCTD site-specific assignment",
                "manual_pages": "B-10 to B-11",
                "slot": 30,
                "output_name": "MCALM-NVO",
                "flash_output_name": "",
                "source_boolean": "MCALM",
                "expression": "(MCE * .N.MCALMOFF)",
            },
        ],
    }


def build_genisys_header_rows(reference: dict) -> list[dict]:
    rows: list[dict] = []
    for header_family, headers in (
        ("office", reference["office_headers"]),
        ("field", reference["field_headers"]),
    ):
        for item in headers:
            rows.append(
                {
                    "header_family": header_family,
                    "byte": item["byte"],
                    "meaning": item["meaning"],
                }
            )
    return rows


def build_genisys_mode_bit_rows(reference: dict) -> list[dict]:
    rows: list[dict] = []
    for item in reference["mode_bit_definitions"]:
        rows.append(
            {
                "mode_byte": reference["framing"]["mode_byte"],
                "bit": item["bit"],
                "meaning": item["meaning"],
            }
        )
    return rows


def build_genisys_message_form_rows(reference: dict) -> list[dict]:
    rows: list[dict] = []
    for form_family, forms in reference["message_forms"].items():
        for item in forms:
            rows.append(
                {
                    "message_family": form_family,
                    "message_name": item["name"],
                    "form": item["form"],
                }
            )
    return rows


def build_genisys_trace_record_type_rows(reference: dict) -> list[dict]:
    return list(reference["trace_record_types"])


def build_genisys_trace_rule_rows(reference: dict) -> list[dict]:
    return list(reference["trace_analysis_rules"])


def build_genisys_manual_sample_bit_rows(reference: dict) -> list[dict]:
    rows: list[dict] = []
    sample_mapping = reference["sample_caa_mapping"]
    for sample_family in ("indications", "controls", "special_control_bits"):
        for item in sample_mapping[sample_family]:
            rows.append(
                {
                    "sample_family": sample_family,
                    "bit": item["bit"],
                    "name": item["name"],
                    "scope": sample_mapping["scope"],
                }
            )
    return rows


def build_genisys_manual_code_indication_rows(reference: dict) -> list[dict]:
    return list(reference["manual_code_indication_booleans"])


def build_genisys_manual_local_control_panel_rows(reference: dict) -> list[dict]:
    return list(reference["manual_local_control_panel_outputs"])


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    icd_catalog = parse_icd_catalog()
    icd_details = parse_icd_message_details(icd_catalog)
    icd_comparison = build_office_locomotive_comparison(icd_catalog)
    genisys_reference = build_genisys_reference()
    genisys_header_rows = build_genisys_header_rows(genisys_reference)
    genisys_mode_bit_rows = build_genisys_mode_bit_rows(genisys_reference)
    genisys_message_form_rows = build_genisys_message_form_rows(genisys_reference)
    genisys_trace_record_type_rows = build_genisys_trace_record_type_rows(genisys_reference)
    genisys_trace_rule_rows = build_genisys_trace_rule_rows(genisys_reference)
    genisys_manual_sample_bit_rows = build_genisys_manual_sample_bit_rows(genisys_reference)
    genisys_manual_code_indication_rows = build_genisys_manual_code_indication_rows(genisys_reference)
    genisys_manual_local_control_panel_rows = build_genisys_manual_local_control_panel_rows(genisys_reference)

    (EXPORTS_DIR / "icd_message_catalog.json").write_text(
        json.dumps(icd_catalog, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(EXPORTS_DIR / "icd_message_catalog.csv", icd_catalog)

    (EXPORTS_DIR / "icd_message_detail_catalog.json").write_text(
        json.dumps(icd_details, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(EXPORTS_DIR / "icd_message_detail_catalog.csv", icd_details)

    (EXPORTS_DIR / "icd_office_locomotive_version_comparison.json").write_text(
        json.dumps(icd_comparison, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(EXPORTS_DIR / "icd_office_locomotive_version_comparison.csv", icd_comparison)

    (EXPORTS_DIR / "genisys_protocol_reference.json").write_text(
        json.dumps(genisys_reference, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(EXPORTS_DIR / "genisys_header_bytes.csv", genisys_header_rows)
    write_csv(EXPORTS_DIR / "genisys_mode_bits.csv", genisys_mode_bit_rows)
    write_csv(EXPORTS_DIR / "genisys_message_forms.csv", genisys_message_form_rows)
    write_csv(EXPORTS_DIR / "genisys_trace_record_types.csv", genisys_trace_record_type_rows)
    write_csv(EXPORTS_DIR / "genisys_trace_rules.csv", genisys_trace_rule_rows)
    write_csv(EXPORTS_DIR / "genisys_manual_sample_bits.csv", genisys_manual_sample_bit_rows)
    write_csv(EXPORTS_DIR / "genisys_manual_code_indications.csv", genisys_manual_code_indication_rows)
    write_csv(EXPORTS_DIR / "genisys_manual_local_control_panel_outputs.csv", genisys_manual_local_control_panel_rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
