from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "exports" / "raw" / "sql_foundation"
NORMALIZED_DIR = REPO_ROOT / "exports" / "normalized"
MAPPINGS_DIR = REPO_ROOT / "exports" / "mappings"


DEFAULT_EMPTY_REFERENCE_VALUES = {
    "",
    "0",
    "0;0;0",
    "0;0;0;",
    "0;0;0;0",
    "0;0;0;0;",
    "NONE",
    "NULL",
}


@dataclass
class ParsedReference:
    raw_reference_part: str
    reference_format: str
    code_line_number: str | None
    code_station_number: str | None
    control_point_number: str | None
    bit_position: str | None
    token_count: int
    is_default_zero_reference: bool


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()


def split_non_empty_tokens(value: str | None, delimiter: str) -> list[str]:
    normalized_value = normalize_text(value)
    if not normalized_value:
        return []
    return [token for token in normalized_value.split(delimiter) if normalize_text(token)]


def infer_assignment_owner_type(component_family: str) -> str:
    family = normalize_text(component_family)
    if family == "control_point":
        return "direct_control_point_slot"
    if family == "misc_device":
        return "misc_device_cp_slot"
    return f"{family}_reference"


def infer_subdivision_name_from_uid(control_point_uid: str) -> str:
    normalized_uid = normalize_text(control_point_uid)
    if normalized_uid.startswith("100"):
        return "SAN DIEGO"
    if normalized_uid.startswith("200"):
        return "ESCONDIDO"
    return ""


def infer_reference_kind(reference_column: str) -> str:
    column = reference_column.upper()
    control_columns = {
        "PCTL",
        "SCTL",
        "TCTL",
        "QCTL",
        "CALLONBIT",
        "CPCTL1",
        "CPCTL2",
        "CPCTL3",
        "CPCTL4",
        "CPCTL5",
        "CPCTL6",
        "CPCTL7",
        "CPCTL8",
        "CPCTL9",
        "CPCTL10",
        "CPCTL11",
        "CPCTL12",
        "CPCTL13",
        "JOINTCONTROLAUTHORIZATIONCTLBIT",
        "NEARSIDESIGNALBITCONTROL",
        "DERAILMENTDETECTORCTLBIT",
        "SWITCHBLOCKCONTROLBIT",
        "RCPSCONTROLBITS",
    }
    indication_columns = {
        "PIND",
        "SIND",
        "TIND",
        "QIND",
        "INTIMEBIT",
        "CPIND1",
        "CPIND2",
        "CPIND3",
        "CPIND4",
        "CPIND5",
        "CPIND6",
        "CPIND7",
        "CPIND8",
        "CPIND9",
        "CPIND10",
        "CPIND11",
        "CPIND12",
        "CPIND13",
        "SCADAALARMREPORTINGINDICATIONBIT",
        "EXPSLOTCPIND",
        "BLOCKINDBIT",
        "JOINTCONTROLAUTHORIZATIONINDBIT",
        "NEARSIDESIGNABITLINDICATION",
        "DERAILMENTDETECTORINDBIT",
        "RCPSINDICATIONBITS",
        "IGNOREBOTHINDBITS",
    }
    if column in control_columns or "CONTROL" in column or column.endswith("CTL"):
        return "control"
    if column in indication_columns or "IND" in column:
        return "indication"
    return "unknown"


def join_unique(values: Iterable[str]) -> str:
    ordered: list[str] = []
    for value in values:
        normalized = normalize_text(value)
        if normalized and normalized not in ordered:
            ordered.append(normalized)
    return "|".join(ordered)


def classify_candidate_long_name_family(long_name: str | None) -> str:
    normalized = normalize_text(long_name).upper()
    if not normalized:
        return "no_candidate"
    if "SWITCH INDICATION" in normalized:
        return "switch_indication"
    if "SIGNAL INDICATION" in normalized or "SIGNAL CLEAR" in normalized:
        return "signal_indication"
    if "RAILMOVEMENT" in normalized:
        return "railmovement_indication"
    if "BLANK" in normalized:
        return "blank"
    return "other"


def build_candidate_family_set(long_names_value: str | None) -> str:
    families: list[str] = []
    for long_name in split_non_empty_tokens(long_names_value, "|"):
        family = classify_candidate_long_name_family(long_name)
        if family not in families:
            families.append(family)
    return "|".join(families) if families else "no_candidate"


def normalize_bit_position(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    if stripped.isdigit():
        return str(int(stripped))
    return stripped


def parse_reference_part(raw_reference_part: str) -> ParsedReference:
    raw = normalize_text(raw_reference_part)
    normalized_for_default = raw.upper()
    if normalized_for_default in DEFAULT_EMPTY_REFERENCE_VALUES:
        return ParsedReference(
            raw_reference_part=raw,
            reference_format="default_zero",
            code_line_number=None,
            code_station_number=None,
            control_point_number=None,
            bit_position=None,
            token_count=0,
            is_default_zero_reference=True,
        )

    tokens = [token.strip() for token in raw.split(";")]
    while tokens and tokens[-1] == "":
        tokens.pop()

    if len(tokens) == 4:
        return ParsedReference(
            raw_reference_part=raw,
            reference_format="code_line_station_control_point_bit",
            code_line_number=tokens[0],
            code_station_number=tokens[1],
            control_point_number=tokens[2],
            bit_position=normalize_bit_position(tokens[3]),
            token_count=4,
            is_default_zero_reference=False,
        )

    if len(tokens) == 3:
        return ParsedReference(
            raw_reference_part=raw,
            reference_format="code_line_station_control_point",
            code_line_number=tokens[0],
            code_station_number=tokens[1],
            control_point_number=tokens[2],
            bit_position=None,
            token_count=3,
            is_default_zero_reference=False,
        )

    return ParsedReference(
        raw_reference_part=raw,
        reference_format="unparsed",
        code_line_number=None,
        code_station_number=None,
        control_point_number=None,
        bit_position=None,
        token_count=len(tokens),
        is_default_zero_reference=False,
    )


def split_reference_values(raw_value: str) -> list[str]:
    return [part.strip() for part in str(raw_value).split("|") if part.strip()]


def canonicalize_reference_part(raw_value: str | None) -> str:
    normalized_value = normalize_text(raw_value)
    if not normalized_value:
        return ""
    tokens = [token.strip() for token in normalized_value.split(";")]
    while tokens and tokens[-1] == "":
        tokens.pop()
    return ";".join(token for token in tokens if token != "")


def canonicalize_reference_parts(raw_value: str | None) -> list[str]:
    canonical_parts: list[str] = []
    for part in split_reference_values(normalize_text(raw_value)):
        canonical_part = canonicalize_reference_part(part)
        if canonical_part:
            canonical_parts.append(canonical_part)
    return canonical_parts


def classify_reference_cp_alignment(raw_value: str | None, parent_control_point_uid: str) -> str:
    normalized_value = normalize_text(raw_value)
    if not normalized_value:
        return "blank"

    parsed = parse_reference_part(normalized_value)
    if parsed.is_default_zero_reference:
        return "default_zero"
    control_point_number = normalize_text(parsed.control_point_number)
    if not control_point_number:
        return "unparsed"
    if control_point_number == "0":
        return "zero_cp"
    if control_point_number == normalize_text(parent_control_point_uid):
        return "same_cp"
    return "different_cp"


def classify_reference_trailing_token_alignment(raw_value: str | None, codeline: str | None) -> str:
    normalized_value = normalize_text(raw_value)
    if not normalized_value:
        return "blank"

    parsed = parse_reference_part(normalized_value)
    if parsed.is_default_zero_reference:
        return "default_zero"
    trailing_token = normalize_text(parsed.bit_position)
    if not trailing_token:
        return "blank"
    normalized_codeline = normalize_text(codeline)
    if not normalized_codeline:
        return "unknown_codeline"
    if trailing_token == normalized_codeline:
        return "matches_codeline"
    return "differs_from_codeline"


def build_code_bit_lookup(rows: Iterable[dict[str, str]]) -> dict[tuple[str, str, str, str, str], list[dict[str, str]]]:
    lookup: dict[tuple[str, str, str, str, str], list[dict[str, str]]] = {}
    for row in rows:
        key = (
            normalize_text(row["CDLNNumber"]),
            normalize_text(row["CSNumber"]),
            normalize_text(row["CPNumber"]),
            normalize_text(normalize_bit_position(row["BPAssignment"])),
            normalize_text(row["WordType"]).upper(),
        )
        lookup.setdefault(key, []).append(row)
    return lookup


def build_code_scope_lookup(rows: Iterable[dict[str, str]]) -> dict[tuple[str, str, str, str], list[dict[str, str]]]:
    lookup: dict[tuple[str, str, str, str], list[dict[str, str]]] = {}
    for row in rows:
        key = (
            normalize_text(row["CDLNNumber"]),
            normalize_text(row["CSNumber"]),
            normalize_text(row["CPNumber"]),
            normalize_text(row["WordType"]).upper(),
        )
        lookup.setdefault(key, []).append(row)
    return lookup


def build_assignment_group_lookup(rows: Iterable[dict[str, str]]) -> dict[tuple[str, str, str], list[dict[str, str]]]:
    lookup: dict[tuple[str, str, str], list[dict[str, str]]] = {}
    for row in rows:
        key = (
            normalize_text(row["CDLNNumber"]),
            normalize_text(row["CSNumber"]),
            normalize_text(row["CPNumber"]),
        )
        lookup.setdefault(key, []).append(row)
    return lookup


def build_assignment_line_lookup(rows: Iterable[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    lookup: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        key = normalize_text(row["CDLNNumber"])
        lookup.setdefault(key, []).append(row)
    return lookup


def build_control_point_line_lookup(rows: Iterable[dict[str, str]]) -> dict[str, list[str]]:
    lookup: dict[str, set[str]] = {}
    for row in rows:
        cp_number = normalize_text(row["CPNumber"])
        code_line_number = normalize_text(row["CDLNNumber"])
        if not cp_number or not code_line_number:
            continue
        lookup.setdefault(cp_number, set()).add(code_line_number)
    return {
        cp_number: sorted(code_lines, key=lambda value: int(value) if value.isdigit() else value)
        for cp_number, code_lines in lookup.items()
    }


def build_component_bit_reference_map() -> list[dict]:
    component_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.component_reference_rows.csv")
    code_bit_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_bit_lookup.csv")
    code_assignment_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_assignment_context.csv")
    code_bit_lookup = build_code_bit_lookup(code_bit_rows)
    control_point_line_lookup = build_control_point_line_lookup(code_assignment_rows)

    normalized_rows: list[dict] = []
    for row in component_rows:
        raw_value = row["reference_value"]
        reference_kind = infer_reference_kind(row["reference_column"])
        expected_word_type = {"control": "C", "indication": "I"}.get(reference_kind, "")

        for part_index, raw_part in enumerate(split_reference_values(raw_value), start=1):
            parsed = parse_reference_part(raw_part)
            match_rows: list[dict[str, str]] = []
            resolved_code_line_number = ""
            resolved_code_station_number = ""
            resolved_control_point_number = ""
            resolved_bp_assignment = ""
            resolution_method = ""
            token_1 = ""
            token_2 = ""
            token_3 = ""
            token_4 = ""
            raw_tokens = [token.strip() for token in parsed.raw_reference_part.split(";") if token.strip()]
            if raw_tokens:
                token_1 = raw_tokens[0] if len(raw_tokens) >= 1 else ""
                token_2 = raw_tokens[1] if len(raw_tokens) >= 2 else ""
                token_3 = raw_tokens[2] if len(raw_tokens) >= 3 else ""
                token_4 = raw_tokens[3] if len(raw_tokens) >= 4 else ""

            if (
                parsed.reference_format == "code_line_station_control_point_bit"
                and expected_word_type
                and parsed.code_line_number
                and parsed.code_station_number
                and parsed.control_point_number
                and parsed.bit_position
            ):
                match_rows = code_bit_lookup.get(
                    (
                        parsed.code_line_number,
                        parsed.code_station_number,
                        parsed.control_point_number,
                        normalize_text(parsed.bit_position),
                        expected_word_type,
                    ),
                    [],
                )
                if match_rows:
                    resolved_code_line_number = parsed.code_line_number
                    resolved_code_station_number = parsed.code_station_number
                    resolved_control_point_number = parsed.control_point_number
                    resolved_bp_assignment = normalize_text(parsed.bit_position)
                    resolution_method = "explicit_code_line_station_control_point_bp_assignment"
                elif token_1 and token_1 != "0" and token_2 and token_3 and token_4:
                    inferred_code_line = normalize_text(normalize_bit_position(token_4))
                    inferred_bp_assignment = normalize_text(normalize_bit_position(token_1))
                    match_rows = code_bit_lookup.get(
                        (
                            inferred_code_line,
                            token_2,
                            token_3,
                            inferred_bp_assignment,
                            expected_word_type,
                        ),
                        [],
                    )
                    if match_rows:
                        resolved_code_line_number = inferred_code_line
                        resolved_code_station_number = token_2
                        resolved_control_point_number = token_3
                        resolved_bp_assignment = inferred_bp_assignment
                        resolution_method = "inferred_bp_assignment_station_control_point_with_trailing_code_line"
            elif (
                parsed.reference_format == "code_line_station_control_point"
                and expected_word_type
                and parsed.code_line_number
                and parsed.code_station_number
                and parsed.control_point_number
                and parsed.code_line_number != "0"
            ):
                inferred_code_lines = control_point_line_lookup.get(parsed.control_point_number, [])
                if len(inferred_code_lines) == 1:
                    inferred_code_line = inferred_code_lines[0]
                    inferred_bp_assignment = normalize_text(normalize_bit_position(parsed.code_line_number))
                    match_rows = code_bit_lookup.get(
                        (
                            inferred_code_line,
                            parsed.code_station_number,
                            parsed.control_point_number,
                            inferred_bp_assignment,
                            expected_word_type,
                        ),
                        [],
                    )
                    if match_rows:
                        resolved_code_line_number = inferred_code_line
                        resolved_code_station_number = parsed.code_station_number
                        resolved_control_point_number = parsed.control_point_number
                        resolved_bp_assignment = inferred_bp_assignment
                        resolution_method = "inferred_bp_assignment_station_control_point_via_unique_cp_line"

            if match_rows:
                for match in match_rows:
                    normalized_rows.append(
                        {
                            "component_family": row["component_family"],
                            "component_uid": row["component_uid"],
                            "parent_control_point_uid": row["parent_control_point_uid"],
                            "component_name": row["component_name"],
                            "component_secondary_name": row["component_secondary_name"],
                            "component_detail_name": row["component_detail_name"],
                            "component_codeline": row["component_codeline"],
                            "reference_column": row["reference_column"],
                            "reference_kind": reference_kind,
                            "raw_reference_value": raw_value,
                            "raw_reference_part": parsed.raw_reference_part,
                            "part_index": part_index,
                            "reference_format": parsed.reference_format,
                            "code_line_number": parsed.code_line_number,
                            "code_station_number": parsed.code_station_number,
                            "control_point_number": parsed.control_point_number,
                            "bit_position": parsed.bit_position,
                            "token_1": token_1,
                            "token_2": token_2,
                            "token_3": token_3,
                            "token_4": token_4,
                            "expected_word_type": expected_word_type,
                            "resolved_code_line_number": resolved_code_line_number,
                            "resolved_code_station_number": resolved_code_station_number,
                            "resolved_control_point_number": resolved_control_point_number,
                            "resolved_bp_assignment": resolved_bp_assignment,
                            "resolved_word_type": match["WordType"],
                            "resolved_mnemonic": match["Mnemonic"],
                            "resolved_long_name": match["LongName"],
                            "resolved_bit_type": match["BitType"],
                            "resolved_assignment_uid": match["UID"],
                            "match_status": "resolved",
                            "resolution_method": resolution_method,
                            "is_default_zero_reference": parsed.is_default_zero_reference,
                        }
                    )
            else:
                normalized_rows.append(
                    {
                        "component_family": row["component_family"],
                        "component_uid": row["component_uid"],
                        "parent_control_point_uid": row["parent_control_point_uid"],
                        "component_name": row["component_name"],
                        "component_secondary_name": row["component_secondary_name"],
                        "component_detail_name": row["component_detail_name"],
                        "component_codeline": row["component_codeline"],
                        "reference_column": row["reference_column"],
                        "reference_kind": reference_kind,
                        "raw_reference_value": raw_value,
                        "raw_reference_part": parsed.raw_reference_part,
                        "part_index": part_index,
                        "reference_format": parsed.reference_format,
                        "code_line_number": parsed.code_line_number,
                        "code_station_number": parsed.code_station_number,
                        "control_point_number": parsed.control_point_number,
                        "bit_position": parsed.bit_position,
                        "token_1": token_1,
                        "token_2": token_2,
                        "token_3": token_3,
                        "token_4": token_4,
                        "expected_word_type": expected_word_type,
                        "resolved_code_line_number": "",
                        "resolved_code_station_number": "",
                        "resolved_control_point_number": "",
                        "resolved_bp_assignment": "",
                        "resolved_word_type": "",
                        "resolved_mnemonic": "",
                        "resolved_long_name": "",
                        "resolved_bit_type": "",
                        "resolved_assignment_uid": "",
                        "match_status": "default_zero" if parsed.is_default_zero_reference else "unresolved",
                        "resolution_method": "",
                        "is_default_zero_reference": parsed.is_default_zero_reference,
                    }
                )

    normalized_rows.sort(
        key=lambda item: (
            item["component_family"],
            int(item["parent_control_point_uid"] or 0),
            int(item["component_uid"] or 0),
            item["reference_column"],
            item["part_index"],
        )
    )
    return normalized_rows


def build_code_station_inventory() -> list[dict]:
    station_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_station_context.csv")
    assignment_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_assignment_context.csv")
    assignment_lookup = build_assignment_group_lookup(assignment_rows)

    normalized_rows: list[dict] = []
    for row in station_rows:
        key = (
            normalize_text(row["CodeLineNumber"]),
            normalize_text(row["CodeStationNumber"]),
            normalize_text(row["ControlPointNumber"]),
        )
        assignments = assignment_lookup.get(key, [])
        control_assignments = [item for item in assignments if normalize_text(item["WordType"]).upper() == "C"]
        indication_assignments = [item for item in assignments if normalize_text(item["WordType"]).upper() == "I"]

        def sample_join(items: list[dict[str, str]], field: str, limit: int = 16) -> str:
            values = [normalize_text(item.get(field)) for item in items]
            values = [value for value in values if value]
            return "|".join(values[:limit])

        normalized_rows.append(
            {
                "code_line_number": normalize_text(row["CodeLineNumber"]),
                "code_line_name": normalize_text(row["CodeLineName"]),
                "code_line_limits": normalize_text(row["CodeLineLimits"]),
                "legacy_type": normalize_text(row["LegacyType"]),
                "session_protocol": normalize_text(row["SessionProtocol"]),
                "normal_codeserver_name": normalize_text(row["NormalCodeserverName"]),
                "standby_codeserver_name": normalize_text(row["StandbyCodeserverName"]),
                "packet_switch_primary_name": normalize_text(row["PacketSwitchPName"]),
                "packet_switch_secondary_name": normalize_text(row["PacketSwitchSName"]),
                "packet_switch_primary_ip": normalize_text(row["PacketSwitchPIP"]),
                "packet_switch_secondary_ip": normalize_text(row["PacketSwitchSIP"]),
                "code_station_number": normalize_text(row["CodeStationNumber"]),
                "control_point_number": normalize_text(row["ControlPointNumber"]),
                "station_name": normalize_text(row["StationName"]),
                "control_point_name": normalize_text(row["ControlPointName"]),
                "ptc_site_name": normalize_text(row["PTCSiteName"]),
                "subdivision_uid": normalize_text(row["SubdivisionUID"]),
                "subdivision_name": normalize_text(row["SubdivisionName"]),
                "territory_assignment": normalize_text(row["TerritoryAssignment"]),
                "control_address": normalize_text(row["ControlAddress"]),
                "indication_address": normalize_text(row["IndicationAddress"]),
                "wayside_atcs_address": normalize_text(row["WaysideATCSAddress"]),
                "wayside_emp_address": normalize_text(row["WaysideEMPAddress"]),
                "number_of_controls": normalize_text(row["NumberOfControls"]),
                "number_of_indications": normalize_text(row["NumberOfIndications"]),
                "control_assignment_count": len(control_assignments),
                "indication_assignment_count": len(indication_assignments),
                "control_mnemonic_samples": sample_join(control_assignments, "Mnemonic"),
                "control_long_name_samples": sample_join(control_assignments, "LongName"),
                "indication_mnemonic_samples": sample_join(indication_assignments, "Mnemonic"),
                "indication_long_name_samples": sample_join(indication_assignments, "LongName"),
            }
        )

    normalized_rows.sort(
        key=lambda item: (
            int(item["code_line_number"] or 0),
            int(item["code_station_number"] or 0),
            int(item["control_point_number"] or 0),
        )
    )
    return normalized_rows


def build_code_station_assignment_map(code_station_inventory: list[dict]) -> tuple[list[dict], list[dict]]:
    assignment_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_assignment_context.csv")
    assignment_lookup = build_assignment_group_lookup(assignment_rows)

    json_rows: list[dict] = []
    csv_rows: list[dict] = []
    for row in code_station_inventory:
        key = (
            normalize_text(row["code_line_number"]),
            normalize_text(row["code_station_number"]),
            normalize_text(row["control_point_number"]),
        )
        assignments = assignment_lookup.get(key, [])
        control_assignments = [item for item in assignments if normalize_text(item["WordType"]).upper() == "C"]
        indication_assignments = [item for item in assignments if normalize_text(item["WordType"]).upper() == "I"]

        def normalize_assignment(item: dict[str, str]) -> dict[str, str]:
            return {
                "word_type": normalize_text(item.get("WordType")),
                "bit_position": normalize_text(normalize_bit_position(item.get("BPAssignment"))),
                "mnemonic": normalize_text(item.get("Mnemonic")),
                "long_name": normalize_text(item.get("LongName")),
                "bit_type": normalize_text(item.get("BitType")),
                "assignment_uid": normalize_text(item.get("UID")),
            }

        normalized_controls = [normalize_assignment(item) for item in control_assignments]
        normalized_indications = [normalize_assignment(item) for item in indication_assignments]

        json_rows.append(
            {
                "code_line_number": row["code_line_number"],
                "code_line_name": row["code_line_name"],
                "code_station_number": row["code_station_number"],
                "station_name": row["station_name"],
                "control_point_number": row["control_point_number"],
                "control_point_name": row["control_point_name"],
                "ptc_site_name": row["ptc_site_name"],
                "subdivision_name": row["subdivision_name"],
                "territory_assignment": row["territory_assignment"],
                "control_address": row["control_address"],
                "indication_address": row["indication_address"],
                "number_of_controls": row["number_of_controls"],
                "number_of_indications": row["number_of_indications"],
                "control_assignments": normalized_controls,
                "indication_assignments": normalized_indications,
            }
        )

        csv_rows.append(
            {
                "code_line_number": row["code_line_number"],
                "code_line_name": row["code_line_name"],
                "code_station_number": row["code_station_number"],
                "station_name": row["station_name"],
                "control_point_number": row["control_point_number"],
                "control_point_name": row["control_point_name"],
                "ptc_site_name": row["ptc_site_name"],
                "subdivision_name": row["subdivision_name"],
                "territory_assignment": row["territory_assignment"],
                "control_address": row["control_address"],
                "indication_address": row["indication_address"],
                "number_of_controls": row["number_of_controls"],
                "number_of_indications": row["number_of_indications"],
                "control_assignment_count": len(normalized_controls),
                "indication_assignment_count": len(normalized_indications),
                "control_assignments_full": "|".join(
                    f"{item['bit_position']}={item['mnemonic']}:{item['long_name']}".rstrip(":")
                    for item in normalized_controls
                ),
                "indication_assignments_full": "|".join(
                    f"{item['bit_position']}={item['mnemonic']}:{item['long_name']}".rstrip(":")
                    for item in normalized_indications
                ),
            }
        )

    json_rows.sort(
        key=lambda item: (
            int(item["code_line_number"] or 0),
            int(item["code_station_number"] or 0),
            int(item["control_point_number"] or 0),
        )
    )
    csv_rows.sort(
        key=lambda item: (
            int(item["code_line_number"] or 0),
            int(item["code_station_number"] or 0),
            int(item["control_point_number"] or 0),
        )
    )
    return json_rows, csv_rows


def build_code_line_protocol_summary(code_station_inventory: list[dict]) -> list[dict]:
    line_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_line_context.csv")
    assignment_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_assignment_context.csv")
    assignment_line_lookup = build_assignment_line_lookup(assignment_rows)

    groups: dict[str, dict] = {}
    for row in line_rows:
        key = normalize_text(row["CodelineNumber"])
        groups[key] = {
            "code_line_number": key,
            "code_line_name": normalize_text(row["CodeLineName"]),
            "code_line_limits": normalize_text(row["CodeLineLimits"]),
            "legacy_type": normalize_text(row["LegacyType"]),
            "session_protocol": normalize_text(row["SessionProtocol"]),
            "normal_codeserver_name": normalize_text(row["NormalCodeserverName"]),
            "standby_codeserver_name": normalize_text(row["StandbyCodeserverName"]),
            "packet_switch_primary_name": normalize_text(row["PacketSwitchPName"]),
            "packet_switch_secondary_name": normalize_text(row["PacketSwitchSName"]),
            "packet_switch_primary_ip": normalize_text(row["PacketSwitchPIP"]),
            "packet_switch_secondary_ip": normalize_text(row["PacketSwitchSIP"]),
            "station_count": 0,
            "control_point_count": 0,
            "control_assignment_count": 0,
            "indication_assignment_count": 0,
            "_stations": set(),
            "_control_points": set(),
            "_subdivisions": set(),
        }

    for row in code_station_inventory:
        key = row["code_line_number"]
        entry = groups.setdefault(
            key,
            {
                "code_line_number": row["code_line_number"],
                "code_line_name": row["code_line_name"],
                "code_line_limits": row["code_line_limits"],
                "legacy_type": row["legacy_type"],
                "session_protocol": row["session_protocol"],
                "normal_codeserver_name": row["normal_codeserver_name"],
                "standby_codeserver_name": row["standby_codeserver_name"],
                "packet_switch_primary_name": row["packet_switch_primary_name"],
                "packet_switch_secondary_name": row["packet_switch_secondary_name"],
                "packet_switch_primary_ip": row["packet_switch_primary_ip"],
                "packet_switch_secondary_ip": row["packet_switch_secondary_ip"],
                "station_count": 0,
                "control_point_count": 0,
                "control_assignment_count": 0,
                "indication_assignment_count": 0,
                "_stations": set(),
                "_control_points": set(),
                "_subdivisions": set(),
            },
        )
        entry["_stations"].add((row["code_station_number"], row["station_name"]))
        entry["_control_points"].add(row["control_point_number"])
        if row["subdivision_name"]:
            entry["_subdivisions"].add(row["subdivision_name"])
        entry["control_assignment_count"] += int(row["control_assignment_count"])
        entry["indication_assignment_count"] += int(row["indication_assignment_count"])

    for key, line_assignments in assignment_line_lookup.items():
        entry = groups.get(key)
        if not entry:
            continue
        if entry["control_assignment_count"] == 0 and entry["indication_assignment_count"] == 0:
            entry["control_assignment_count"] = sum(
                1 for item in line_assignments if normalize_text(item["WordType"]).upper() == "C"
            )
            entry["indication_assignment_count"] = sum(
                1 for item in line_assignments if normalize_text(item["WordType"]).upper() == "I"
            )

    normalized_rows: list[dict] = []
    for entry in groups.values():
        normalized_rows.append(
            {
                "code_line_number": entry["code_line_number"],
                "code_line_name": entry["code_line_name"],
                "code_line_limits": entry["code_line_limits"],
                "legacy_type": entry["legacy_type"],
                "session_protocol": entry["session_protocol"],
                "normal_codeserver_name": entry["normal_codeserver_name"],
                "standby_codeserver_name": entry["standby_codeserver_name"],
                "packet_switch_primary_name": entry["packet_switch_primary_name"],
                "packet_switch_secondary_name": entry["packet_switch_secondary_name"],
                "packet_switch_primary_ip": entry["packet_switch_primary_ip"],
                "packet_switch_secondary_ip": entry["packet_switch_secondary_ip"],
                "station_count": len(entry["_stations"]),
                "control_point_count": len(entry["_control_points"]),
                "subdivision_names": "|".join(sorted(entry["_subdivisions"])),
                "control_assignment_count": entry["control_assignment_count"],
                "indication_assignment_count": entry["indication_assignment_count"],
            }
        )

    normalized_rows.sort(key=lambda item: int(item["code_line_number"] or 0))
    return normalized_rows


def build_subdivision_protocol_summary(
    code_line_protocol_summary: list[dict],
    station_foundation_summary: list[dict],
) -> list[dict]:
    line_detail_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_line_detail_full.csv")
    line_summary_by_number = {
        normalize_text(row["code_line_number"]): row
        for row in code_line_protocol_summary
    }
    line_detail_by_number = {
        normalize_text(row["CodelineNumber"]): row
        for row in line_detail_rows
    }

    def sort_mixed(values: set[str]) -> list[str]:
        def sort_key(value: str) -> tuple[int, int | str]:
            normalized_value = normalize_text(value)
            return (0, int(normalized_value)) if normalized_value.isdigit() else (1, normalized_value)

        return sorted((value for value in values if normalize_text(value)), key=sort_key)

    def to_prefix_bucket(control_point_number: str) -> str:
        normalized_value = normalize_text(control_point_number)
        return f"{normalized_value[:3]}xxx" if len(normalized_value) >= 3 else normalized_value

    groups: dict[str, dict] = {}
    for row in station_foundation_summary:
        subdivision_name = normalize_text(row["subdivision_name"])
        if not subdivision_name:
            continue

        entry = groups.setdefault(
            subdivision_name,
            {
                "subdivision_name": subdivision_name,
                "_subdivision_uids": set(),
                "_territory_assignments": set(),
                "_code_line_numbers": set(),
                "_code_line_names": {},
                "_legacy_types": set(),
                "_session_protocols": set(),
                "_normal_codeserver_names": set(),
                "_standby_codeserver_names": set(),
                "_packet_switch_primary_names": set(),
                "_packet_switch_secondary_names": set(),
                "_packet_switch_primary_ips": set(),
                "_packet_switch_secondary_ips": set(),
                "_link_layer_types": set(),
                "_link_layer_protocols": set(),
                "_address_atcs_values": set(),
                "_control_points": set(),
                "_stations": set(),
                "_control_point_uid_prefixes": set(),
                "_seen_line_numbers": set(),
                "station_row_count": 0,
                "control_point_row_count": 0,
                "live_code_line_count": 0,
                "control_assignment_count": 0,
                "indication_assignment_count": 0,
                "signal_count": 0,
                "track_count": 0,
                "switch_count": 0,
                "misc_device_count": 0,
                "route_count": 0,
            },
        )

        subdivision_uid = normalize_text(row["subdivision_uid"])
        if subdivision_uid:
            entry["_subdivision_uids"].add(subdivision_uid)

        territory_assignment = normalize_text(row["territory_assignment"])
        if territory_assignment:
            entry["_territory_assignments"].add(territory_assignment)

        code_line_number = normalize_text(row["code_line_number"])
        if code_line_number:
            entry["_code_line_numbers"].add(code_line_number)

        code_line_name = normalize_text(row["code_line_name"])
        if code_line_number and code_line_name:
            entry["_code_line_names"][code_line_number] = code_line_name

        station_key = (
            normalize_text(row["code_station_number"]),
            normalize_text(row["station_name"]),
            normalize_text(row["control_point_number"]),
        )
        entry["_stations"].add(station_key)

        control_point_number = normalize_text(row["control_point_number"])
        if control_point_number:
            entry["_control_points"].add(control_point_number)
            entry["_control_point_uid_prefixes"].add(to_prefix_bucket(control_point_number))

        if code_line_number and code_line_number not in entry["_seen_line_numbers"]:
            entry["_seen_line_numbers"].add(code_line_number)
            line_summary = line_summary_by_number.get(code_line_number, {})
            line_detail = line_detail_by_number.get(code_line_number, {})

            entry["live_code_line_count"] += 1
            entry["control_assignment_count"] += int(normalize_text(line_summary.get("control_assignment_count")) or 0)
            entry["indication_assignment_count"] += int(normalize_text(line_summary.get("indication_assignment_count")) or 0)

            for field_name, target_key in (
                ("legacy_type", "_legacy_types"),
                ("session_protocol", "_session_protocols"),
                ("normal_codeserver_name", "_normal_codeserver_names"),
                ("standby_codeserver_name", "_standby_codeserver_names"),
                ("packet_switch_primary_name", "_packet_switch_primary_names"),
                ("packet_switch_secondary_name", "_packet_switch_secondary_names"),
                ("packet_switch_primary_ip", "_packet_switch_primary_ips"),
                ("packet_switch_secondary_ip", "_packet_switch_secondary_ips"),
            ):
                value = normalize_text(line_summary.get(field_name))
                if value:
                    entry[target_key].add(value)

            for field_name, target_key in (
                ("LinkLayerType", "_link_layer_types"),
                ("LinkLayerProtocol", "_link_layer_protocols"),
                ("AddressATCS", "_address_atcs_values"),
            ):
                value = normalize_text(line_detail.get(field_name))
                if value:
                    entry[target_key].add(value)

        entry["signal_count"] += int(normalize_text(row["signal_count"]) or 0)
        entry["track_count"] += int(normalize_text(row["track_count"]) or 0)
        entry["switch_count"] += int(normalize_text(row["switch_count"]) or 0)
        entry["misc_device_count"] += int(normalize_text(row["misc_device_count"]) or 0)
        entry["route_count"] += int(normalize_text(row["route_count"]) or 0)

    normalized_rows: list[dict] = []
    for entry in groups.values():
        code_line_numbers = sort_mixed(entry["_code_line_numbers"])
        normalized_rows.append(
            {
                "subdivision_uid": "|".join(sort_mixed(entry["_subdivision_uids"])),
                "subdivision_name": entry["subdivision_name"],
                "territory_assignments": "|".join(sort_mixed(entry["_territory_assignments"])),
                "live_code_line_count": entry["live_code_line_count"],
                "code_line_numbers": "|".join(code_line_numbers),
                "code_line_names": "|".join(
                    normalize_text(entry["_code_line_names"].get(code_line_number))
                    for code_line_number in code_line_numbers
                    if normalize_text(entry["_code_line_names"].get(code_line_number))
                ),
                "legacy_types": "|".join(sorted(entry["_legacy_types"])),
                "session_protocols": "|".join(sorted(entry["_session_protocols"])),
                "normal_codeserver_names": "|".join(sorted(entry["_normal_codeserver_names"])),
                "standby_codeserver_names": "|".join(sorted(entry["_standby_codeserver_names"])),
                "packet_switch_primary_names": "|".join(sorted(entry["_packet_switch_primary_names"])),
                "packet_switch_secondary_names": "|".join(sorted(entry["_packet_switch_secondary_names"])),
                "packet_switch_primary_ips": "|".join(sorted(entry["_packet_switch_primary_ips"])),
                "packet_switch_secondary_ips": "|".join(sorted(entry["_packet_switch_secondary_ips"])),
                "link_layer_types": "|".join(sorted(entry["_link_layer_types"])),
                "link_layer_protocols": "|".join(sorted(entry["_link_layer_protocols"])),
                "address_atcs_values": "|".join(sort_mixed(entry["_address_atcs_values"])),
                "station_row_count": len(entry["_stations"]),
                "control_point_row_count": len(entry["_control_points"]),
                "control_point_uid_prefixes": "|".join(sorted(entry["_control_point_uid_prefixes"])),
                "control_assignment_count": entry["control_assignment_count"],
                "indication_assignment_count": entry["indication_assignment_count"],
                "signal_count": entry["signal_count"],
                "track_count": entry["track_count"],
                "switch_count": entry["switch_count"],
                "misc_device_count": entry["misc_device_count"],
                "route_count": entry["route_count"],
            }
        )

    normalized_rows.sort(
        key=lambda item: (
            int(item["subdivision_uid"].split("|")[0] or 0) if item["subdivision_uid"] else 0,
            item["subdivision_name"],
        )
    )
    return normalized_rows


def build_genisys_station_assignment_summary() -> list[dict]:
    assignment_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_assignment_context.csv")
    groups: dict[tuple[str, str, str, str], list[dict[str, str]]] = {}

    for row in assignment_rows:
        if normalize_text(row.get("SessionProtocol")) != "GENESYS":
            continue
        key = (
            normalize_text(row["CDLNNumber"]),
            normalize_text(row["CSNumber"]),
            normalize_text(row["CPNumber"]),
            normalize_text(row["WordType"]).upper(),
        )
        groups.setdefault(key, []).append(row)

    normalized_rows: list[dict] = []
    for key, items in groups.items():
        code_line_number, code_station_number, control_point_number, word_type = key
        bit_positions = sorted(int(normalize_text(item["BPAssignment"])) for item in items if normalize_text(item["BPAssignment"]).isdigit())
        min_assignment = min(bit_positions) if bit_positions else 0
        max_assignment = max(bit_positions) if bit_positions else 0
        contiguous = bit_positions == list(range(min_assignment, max_assignment + 1)) if bit_positions else False
        exemplar = items[0]
        normalized_rows.append(
            {
                "code_line_number": code_line_number,
                "code_line_name": normalize_text(exemplar.get("CodeLineName")),
                "code_station_number": code_station_number,
                "station_name": normalize_text(exemplar.get("StationName")),
                "control_point_number": control_point_number,
                "control_point_name": normalize_text(exemplar.get("ControlPointName")),
                "subdivision_name": normalize_text(exemplar.get("SubdivisionName")),
                "word_type": word_type,
                "assignment_count": len(bit_positions),
                "min_bp_assignment": min_assignment,
                "max_bp_assignment": max_assignment,
                "is_contiguous_assignment_range": contiguous,
                "derived_word_count": max_assignment // 8 if max_assignment else 0,
                "decode_basis": "bp_assignment = word_number * 8 + bit_number (1-based)" if contiguous else "",
            }
        )

    normalized_rows.sort(
        key=lambda item: (
            int(item["code_line_number"] or 0),
            int(item["code_station_number"] or 0),
            int(item["control_point_number"] or 0),
            item["word_type"],
        )
    )
    return normalized_rows


def build_component_reference_scope_summary() -> list[dict]:
    component_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.component_reference_rows.csv")
    code_assignment_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_assignment_context.csv")
    code_scope_lookup = build_code_scope_lookup(code_assignment_rows)

    normalized_rows: list[dict] = []
    for row in component_rows:
        raw_value = row["reference_value"]
        reference_kind = infer_reference_kind(row["reference_column"])
        expected_word_type = {"control": "C", "indication": "I"}.get(reference_kind, "")

        for part_index, raw_part in enumerate(split_reference_values(raw_value), start=1):
            parsed = parse_reference_part(raw_part)
            if (
                parsed.reference_format != "code_line_station_control_point"
                or not expected_word_type
                or not parsed.code_line_number
                or not parsed.code_station_number
                or not parsed.control_point_number
            ):
                continue

            candidates = code_scope_lookup.get(
                (
                    parsed.code_line_number,
                    parsed.code_station_number,
                    parsed.control_point_number,
                    expected_word_type,
                ),
                [],
            )
            code_line_name = ""
            code_line_limits = ""
            station_name = ""
            control_address = ""
            indication_address = ""
            if candidates:
                exemplar = candidates[0]
                code_line_name = exemplar.get("CodeLineName", "")
                code_line_limits = exemplar.get("CodeLineLimits", "")
                station_name = exemplar.get("StationName", "")
                control_address = exemplar.get("ControlAddress", "")
                indication_address = exemplar.get("IndicationAddress", "")

            candidate_pairs = [
                {
                    "bit": normalize_text(normalize_bit_position(candidate.get("BPAssignment"))),
                    "mnemonic": normalize_text(candidate.get("Mnemonic")),
                    "long_name": normalize_text(candidate.get("LongName")),
                }
                for candidate in candidates
            ]
            candidate_pairs.sort(key=lambda item: (item["bit"], item["mnemonic"], item["long_name"]))

            normalized_rows.append(
                {
                    "component_family": row["component_family"],
                    "component_uid": row["component_uid"],
                    "parent_control_point_uid": row["parent_control_point_uid"],
                    "component_name": row["component_name"],
                    "component_secondary_name": row["component_secondary_name"],
                    "component_detail_name": row["component_detail_name"],
                    "component_codeline": row["component_codeline"],
                    "reference_column": row["reference_column"],
                    "reference_kind": reference_kind,
                    "raw_reference_value": raw_value,
                    "raw_reference_part": parsed.raw_reference_part,
                    "part_index": part_index,
                    "reference_format": parsed.reference_format,
                    "code_line_number": parsed.code_line_number,
                    "code_station_number": parsed.code_station_number,
                    "control_point_number": parsed.control_point_number,
                    "expected_word_type": expected_word_type,
                    "code_line_name": code_line_name,
                    "code_line_limits": code_line_limits,
                    "station_name": station_name,
                    "control_address": control_address,
                    "indication_address": indication_address,
                    "candidate_assignment_count": len(candidate_pairs),
                    "candidate_bits": "|".join(item["bit"] for item in candidate_pairs if item["bit"]),
                    "candidate_mnemonics": "|".join(item["mnemonic"] for item in candidate_pairs if item["mnemonic"]),
                    "candidate_long_names": "|".join(item["long_name"] for item in candidate_pairs if item["long_name"]),
                    "candidate_examples": "|".join(
                        f"{item['bit']}={item['mnemonic']}:{item['long_name']}".strip(":")
                        for item in candidate_pairs[:16]
                    ),
                    "scope_status": "has_scope_candidates" if candidate_pairs else "no_scope_candidates",
                }
            )

    normalized_rows.sort(
        key=lambda item: (
            item["component_family"],
            int(item["parent_control_point_uid"] or 0),
            int(item["component_uid"] or 0),
            item["reference_column"],
            item["part_index"],
        )
    )
    return normalized_rows


def build_zero_first_reference_summary(component_bit_reference_map: list[dict]) -> list[dict]:
    component_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.component_reference_rows.csv")
    code_assignment_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_assignment_context.csv")
    code_scope_lookup = build_code_scope_lookup(code_assignment_rows)
    control_point_line_lookup = build_control_point_line_lookup(code_assignment_rows)

    resolved_sibling_lookup: dict[tuple[str, str, str, str], list[dict[str, str]]] = {}
    for row in component_bit_reference_map:
        if row["match_status"] != "resolved":
            continue
        key = (
            normalize_text(row["component_family"]),
            normalize_text(row["component_uid"]),
            normalize_text(row["reference_column"]),
            normalize_text(row["raw_reference_value"]),
        )
        resolved_sibling_lookup.setdefault(key, []).append(row)

    normalized_rows: list[dict] = []
    for row in component_rows:
        raw_value = row["reference_value"]
        reference_kind = infer_reference_kind(row["reference_column"])
        expected_word_type = {"control": "C", "indication": "I"}.get(reference_kind, "")

        field_key = (
            normalize_text(row["component_family"]),
            normalize_text(row["component_uid"]),
            normalize_text(row["reference_column"]),
            normalize_text(raw_value),
        )
        resolved_siblings = resolved_sibling_lookup.get(field_key, [])
        resolved_sibling_examples = []
        for sibling in resolved_siblings[:8]:
            example = f"{sibling['resolved_bp_assignment']}={sibling['resolved_mnemonic']}:{sibling['resolved_long_name']}".strip(":")
            if example:
                resolved_sibling_examples.append(example)

        for part_index, raw_part in enumerate(split_reference_values(raw_value), start=1):
            parsed = parse_reference_part(raw_part)
            raw_tokens = [token.strip() for token in parsed.raw_reference_part.split(";")]
            while raw_tokens and raw_tokens[-1] == "":
                raw_tokens.pop()
            if not raw_tokens or raw_tokens[0] != "0" or parsed.is_default_zero_reference:
                continue

            token_1 = raw_tokens[0] if len(raw_tokens) >= 1 else ""
            token_2 = raw_tokens[1] if len(raw_tokens) >= 2 else ""
            token_3 = raw_tokens[2] if len(raw_tokens) >= 3 else ""
            token_4 = raw_tokens[3] if len(raw_tokens) >= 4 else ""

            component_line = normalize_text(row["component_codeline"])
            unique_cp_lines = control_point_line_lookup.get(token_3, []) if token_3 else []
            unique_cp_line = unique_cp_lines[0] if len(unique_cp_lines) == 1 else ""

            scope_sources: list[tuple[str, str, list[dict[str, str]]]] = []
            if expected_word_type and token_4:
                trailing_candidates = code_scope_lookup.get((token_4, token_2, token_3, expected_word_type), [])
                if trailing_candidates:
                    scope_sources.append(("trailing_token4", token_4, trailing_candidates))
            if expected_word_type and component_line:
                component_candidates = code_scope_lookup.get((component_line, token_2, token_3, expected_word_type), [])
                if component_candidates:
                    scope_sources.append(("component_codeline", component_line, component_candidates))
            if expected_word_type and unique_cp_line:
                unique_cp_candidates = code_scope_lookup.get((unique_cp_line, token_2, token_3, expected_word_type), [])
                if unique_cp_candidates:
                    scope_sources.append(("unique_cp_line", unique_cp_line, unique_cp_candidates))

            preferred_scope_source = ""
            preferred_scope_code_line = ""
            preferred_scope_candidates: list[dict[str, str]] = []
            if scope_sources:
                preferred_scope_source, preferred_scope_code_line, preferred_scope_candidates = scope_sources[0]

            preferred_examples = []
            for candidate in preferred_scope_candidates[:8]:
                bit = normalize_text(normalize_bit_position(candidate.get("BPAssignment")))
                mnemonic = normalize_text(candidate.get("Mnemonic"))
                long_name = normalize_text(candidate.get("LongName"))
                example = f"{bit}={mnemonic}:{long_name}".strip(":")
                if example:
                    preferred_examples.append(example)

            scope_source_names = [item[0] for item in scope_sources]
            scope_source_key = "+".join(scope_source_names) if scope_source_names else "none"
            has_resolved_sibling = bool(resolved_siblings)
            if not expected_word_type:
                structural_class = "unknown_word_type_zero_first"
                scope_source_key = "unknown_word_type"
            elif scope_sources and has_resolved_sibling:
                structural_class = "scoped_zero_first_with_resolved_sibling"
            elif scope_sources:
                structural_class = "scoped_zero_first_without_resolved_sibling"
            elif has_resolved_sibling:
                structural_class = "unscoped_zero_first_with_resolved_sibling"
            else:
                structural_class = "unscoped_zero_first_without_resolved_sibling"

            normalized_rows.append(
                {
                    "component_family": row["component_family"],
                    "component_uid": row["component_uid"],
                    "parent_control_point_uid": row["parent_control_point_uid"],
                    "component_name": row["component_name"],
                    "component_secondary_name": row["component_secondary_name"],
                    "component_detail_name": row["component_detail_name"],
                    "component_codeline": row["component_codeline"],
                    "reference_column": row["reference_column"],
                    "reference_kind": reference_kind,
                    "raw_reference_value": raw_value,
                    "raw_reference_part": parsed.raw_reference_part,
                    "part_index": part_index,
                    "reference_format": parsed.reference_format,
                    "token_count": parsed.token_count,
                    "token_1": token_1,
                    "token_2": token_2,
                    "token_3": token_3,
                    "token_4": token_4,
                    "expected_word_type": expected_word_type,
                    "component_scope_code_line": component_line,
                    "unique_cp_scope_code_line": unique_cp_line,
                    "trailing_scope_code_line": token_4,
                    "component_scope_candidate_count": len(
                        code_scope_lookup.get((component_line, token_2, token_3, expected_word_type), [])
                    )
                    if component_line
                    else 0,
                    "unique_cp_scope_candidate_count": len(
                        code_scope_lookup.get((unique_cp_line, token_2, token_3, expected_word_type), [])
                    )
                    if unique_cp_line
                    else 0,
                    "trailing_scope_candidate_count": len(
                        code_scope_lookup.get((token_4, token_2, token_3, expected_word_type), [])
                    )
                    if token_4
                    else 0,
                    "preferred_scope_source": preferred_scope_source,
                    "preferred_scope_code_line": preferred_scope_code_line,
                    "preferred_scope_candidate_count": len(preferred_scope_candidates),
                    "preferred_scope_candidate_examples": "|".join(preferred_examples),
                    "scope_source_key": scope_source_key,
                    "has_resolved_sibling_in_same_field": has_resolved_sibling,
                    "resolved_sibling_count": len(resolved_siblings),
                    "resolved_sibling_examples": "|".join(resolved_sibling_examples),
                    "structural_class": structural_class,
                }
            )

    normalized_rows.sort(
        key=lambda item: (
            item["component_family"],
            int(item["parent_control_point_uid"] or 0),
            int(item["component_uid"] or 0),
            item["reference_column"],
            item["part_index"],
        )
    )
    return normalized_rows


def build_zero_first_reference_class_counts(zero_first_reference_summary: list[dict]) -> list[dict]:
    counts: dict[tuple[str, str, str, str, str, str], int] = {}
    for row in zero_first_reference_summary:
        key = (
            row["structural_class"],
            row["component_family"],
            row["reference_column"],
            row["reference_format"],
            row["scope_source_key"],
            str(row["has_resolved_sibling_in_same_field"]).lower(),
        )
        counts[key] = counts.get(key, 0) + 1

    normalized_rows = [
        {
            "structural_class": key[0],
            "component_family": key[1],
            "reference_column": key[2],
            "reference_format": key[3],
            "scope_source_key": key[4],
            "has_resolved_sibling_in_same_field": key[5],
            "row_count": count,
        }
        for key, count in counts.items()
    ]
    normalized_rows.sort(
        key=lambda item: (
            item["structural_class"],
            item["component_family"],
            item["reference_column"],
            item["reference_format"],
            item["scope_source_key"],
        )
    )
    return normalized_rows


def build_cp_assignment_summary(component_bit_reference_map: list[dict]) -> list[dict]:
    misc_device_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.misc_device_context.csv")
    code_station_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_station_context.csv")

    misc_lookup = {
        normalize_text(row["UID"]): row
        for row in misc_device_rows
    }
    station_lookup = {
        normalize_text(row["ControlPointNumber"]): row
        for row in code_station_rows
    }

    normalized_rows: list[dict] = []
    for row in component_bit_reference_map:
        reference_column = normalize_text(row["reference_column"])
        if not (
            reference_column.startswith("CPCTL")
            or reference_column.startswith("CPIND")
            or reference_column in {"SCADAALARMREPORTINGINDICATIONBIT", "EXPSLOTCPIND"}
        ):
            continue

        parent_control_point_uid = normalize_text(row["parent_control_point_uid"])
        component_uid = normalize_text(row["component_uid"])
        component_family = normalize_text(row["component_family"])
        station_row = station_lookup.get(parent_control_point_uid, {})
        misc_row = misc_lookup.get(component_uid, {}) if component_family == "misc_device" else {}

        slot_number = ""
        if reference_column.startswith("CPCTL"):
            slot_number = reference_column.replace("CPCTL", "")
        elif reference_column.startswith("CPIND"):
            slot_number = reference_column.replace("CPIND", "")

        normalized_rows.append(
            {
                "component_family": component_family,
                "assignment_owner_type": (
                    "direct_control_point_slot"
                    if component_family == "control_point"
                    else "misc_device_cp_slot"
                    if component_family == "misc_device"
                    else "other_cp_slot"
                ),
                "component_uid": component_uid,
                "component_name": normalize_text(row["component_name"]),
                "component_secondary_name": normalize_text(row["component_secondary_name"]),
                "component_detail_name": normalize_text(row["component_detail_name"]),
                "parent_control_point_uid": parent_control_point_uid,
                "parent_control_point_name": normalize_text(station_row.get("ControlPointName")),
                "ptc_site_name": normalize_text(station_row.get("PTCSiteName")),
                "subdivision_uid": normalize_text(station_row.get("SubdivisionUID")),
                "subdivision_name": normalize_text(station_row.get("SubdivisionName")),
                "territory_assignment": normalize_text(station_row.get("TerritoryAssignment")),
                "station_name": normalize_text(station_row.get("StationName")),
                "component_codeline": normalize_text(row["component_codeline"]),
                "station_code_line_number": normalize_text(station_row.get("CodeLineNumber")),
                "station_code_line_name": normalize_text(station_row.get("CodeLineName")),
                "station_code_station_number": normalize_text(station_row.get("CodeStationNumber")),
                "reference_column": reference_column,
                "reference_kind": normalize_text(row["reference_kind"]),
                "slot_number": slot_number,
                "raw_reference_value": normalize_text(row["raw_reference_value"]),
                "raw_reference_part": normalize_text(row["raw_reference_part"]),
                "reference_format": normalize_text(row["reference_format"]),
                "match_status": normalize_text(row["match_status"]),
                "resolution_method": normalize_text(row["resolution_method"]),
                "resolved_code_line_number": normalize_text(row["resolved_code_line_number"]),
                "resolved_code_station_number": normalize_text(row["resolved_code_station_number"]),
                "resolved_control_point_number": normalize_text(row["resolved_control_point_number"]),
                "resolved_bp_assignment": normalize_text(row["resolved_bp_assignment"]),
                "resolved_mnemonic": normalize_text(row["resolved_mnemonic"]),
                "resolved_long_name": normalize_text(row["resolved_long_name"]),
                "resolved_bit_type": normalize_text(row["resolved_bit_type"]),
                "misc_type": normalize_text(misc_row.get("Type")),
                "misc_type_of_device": normalize_text(misc_row.get("TypeOfDevice")),
                "misc_device_category": normalize_text(misc_row.get("DeviceCategory")),
                "misc_secondary_text": normalize_text(misc_row.get("SecondaryText")),
                "misc_device_control": normalize_text(misc_row.get("DeviceControl")),
                "misc_control_only": normalize_text(misc_row.get("ControlOnly")),
                "misc_indicate_only": normalize_text(misc_row.get("IndicateOnly")),
                "misc_flash_control": normalize_text(misc_row.get("FlashControl")),
                "misc_flash_indicate": normalize_text(misc_row.get("FlashIndicate")),
                "misc_scada_mode": normalize_text(misc_row.get("ScadaMode")),
                "misc_track_interlock_mode": normalize_text(misc_row.get("TrackInterlockMode")),
            }
        )

    normalized_rows.sort(
        key=lambda item: (
            item["component_family"],
            int(item["parent_control_point_uid"] or 0),
            int(item["component_uid"] or 0),
            item["reference_column"],
            item["raw_reference_part"],
        )
    )
    return normalized_rows


def build_cp_assignment_slot_summary(cp_assignment_summary: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str, str, str], dict] = {}
    for row in cp_assignment_summary:
        key = (
            row["component_family"],
            row["assignment_owner_type"],
            row["reference_column"],
            row["match_status"],
        )
        entry = grouped.setdefault(
            key,
            {
                "component_family": row["component_family"],
                "assignment_owner_type": row["assignment_owner_type"],
                "reference_column": row["reference_column"],
                "match_status": row["match_status"],
                "reference_kind": row["reference_kind"],
                "row_count": 0,
                "_components": set(),
                "_parents": set(),
                "_subdivisions": set(),
                "_resolved_examples": [],
                "_raw_examples": [],
            },
        )
        entry["row_count"] += 1
        if row["component_uid"]:
            entry["_components"].add(row["component_uid"])
        if row["parent_control_point_uid"]:
            entry["_parents"].add(row["parent_control_point_uid"])
        if row["subdivision_name"]:
            entry["_subdivisions"].add(row["subdivision_name"])
        if row["resolved_mnemonic"] and len(entry["_resolved_examples"]) < 12:
            example = f"{row['resolved_mnemonic']}:{row['resolved_long_name']}".strip(":")
            if example not in entry["_resolved_examples"]:
                entry["_resolved_examples"].append(example)
        if row["raw_reference_part"] and len(entry["_raw_examples"]) < 12:
            if row["raw_reference_part"] not in entry["_raw_examples"]:
                entry["_raw_examples"].append(row["raw_reference_part"])

    normalized_rows: list[dict] = []
    for entry in grouped.values():
        normalized_rows.append(
            {
                "component_family": entry["component_family"],
                "assignment_owner_type": entry["assignment_owner_type"],
                "reference_column": entry["reference_column"],
                "reference_kind": entry["reference_kind"],
                "match_status": entry["match_status"],
                "row_count": entry["row_count"],
                "distinct_component_count": len(entry["_components"]),
                "distinct_parent_control_point_count": len(entry["_parents"]),
                "subdivision_names": "|".join(sorted(entry["_subdivisions"])),
                "resolved_examples": "|".join(entry["_resolved_examples"]),
                "raw_reference_examples": "|".join(entry["_raw_examples"]),
            }
        )

    normalized_rows.sort(
        key=lambda item: (
            item["component_family"],
            item["reference_column"],
            item["match_status"],
        )
    )
    return normalized_rows


def build_cp_assignment_resolved_patterns(cp_assignment_summary: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str, str, str], dict] = {}
    for row in cp_assignment_summary:
        if row["match_status"] != "resolved":
            continue
        key = (
            row["component_family"],
            row["reference_column"],
            row["resolved_mnemonic"],
            row["resolved_long_name"],
        )
        entry = grouped.setdefault(
            key,
            {
                "component_family": row["component_family"],
                "reference_column": row["reference_column"],
                "resolved_mnemonic": row["resolved_mnemonic"],
                "resolved_long_name": row["resolved_long_name"],
                "row_count": 0,
                "_components": set(),
                "_parents": set(),
                "_samples": [],
            },
        )
        entry["row_count"] += 1
        if row["component_uid"]:
            entry["_components"].add(row["component_uid"])
        if row["parent_control_point_uid"]:
            entry["_parents"].add(row["parent_control_point_uid"])
        sample = f"{row['parent_control_point_name']}:{row['component_name']}".strip(":")
        if sample and sample not in entry["_samples"] and len(entry["_samples"]) < 12:
            entry["_samples"].append(sample)

    normalized_rows: list[dict] = []
    for entry in grouped.values():
        normalized_rows.append(
            {
                "component_family": entry["component_family"],
                "reference_column": entry["reference_column"],
                "resolved_mnemonic": entry["resolved_mnemonic"],
                "resolved_long_name": entry["resolved_long_name"],
                "row_count": entry["row_count"],
                "distinct_component_count": len(entry["_components"]),
                "distinct_parent_control_point_count": len(entry["_parents"]),
                "sample_component_paths": "|".join(entry["_samples"]),
            }
        )

    normalized_rows.sort(
        key=lambda item: (
            item["component_family"],
            item["reference_column"],
            -int(item["row_count"]),
            item["resolved_mnemonic"],
        )
    )
    return normalized_rows


def build_cp_zero_first_slot_summary(cp_assignment_summary: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str, str, str, str], dict] = {}
    for row in cp_assignment_summary:
        raw_reference_value = normalize_text(row["raw_reference_value"])
        if row["match_status"] != "unresolved" or not raw_reference_value.startswith("0;"):
            continue
        parsed = parse_reference_part(raw_reference_value)
        if parsed.is_default_zero_reference:
            continue

        cp_alignment = classify_reference_cp_alignment(raw_reference_value, row["parent_control_point_uid"])
        trailing_alignment = classify_reference_trailing_token_alignment(raw_reference_value, row["component_codeline"])
        key = (
            row["component_family"],
            row["assignment_owner_type"],
            row["reference_column"],
            row["reference_format"],
            row["subdivision_name"],
        )
        entry = grouped.setdefault(
            key,
            {
                "component_family": row["component_family"],
                "assignment_owner_type": row["assignment_owner_type"],
                "reference_column": row["reference_column"],
                "reference_kind": row["reference_kind"],
                "reference_format": row["reference_format"],
                "subdivision_name": row["subdivision_name"],
                "row_count": 0,
                "_components": set(),
                "_parents": set(),
                "_raw_values": set(),
                "_component_paths": [],
                "_station_names": set(),
                "_cp_alignment_counts": {},
                "_trailing_alignment_counts": {},
            },
        )
        entry["row_count"] += 1
        if row["component_uid"]:
            entry["_components"].add(row["component_uid"])
        if row["parent_control_point_uid"]:
            entry["_parents"].add(row["parent_control_point_uid"])
        if raw_reference_value:
            entry["_raw_values"].add(raw_reference_value)
        if row["station_name"]:
            entry["_station_names"].add(row["station_name"])
        component_path = f"{row['parent_control_point_name']}:{row['component_name']}".strip(":")
        if component_path and component_path not in entry["_component_paths"] and len(entry["_component_paths"]) < 12:
            entry["_component_paths"].append(component_path)
        entry["_cp_alignment_counts"][cp_alignment] = entry["_cp_alignment_counts"].get(cp_alignment, 0) + 1
        entry["_trailing_alignment_counts"][trailing_alignment] = (
            entry["_trailing_alignment_counts"].get(trailing_alignment, 0) + 1
        )

    normalized_rows: list[dict] = []
    for entry in grouped.values():
        normalized_rows.append(
            {
                "component_family": entry["component_family"],
                "assignment_owner_type": entry["assignment_owner_type"],
                "reference_column": entry["reference_column"],
                "reference_kind": entry["reference_kind"],
                "reference_format": entry["reference_format"],
                "subdivision_name": entry["subdivision_name"],
                "row_count": entry["row_count"],
                "distinct_component_count": len(entry["_components"]),
                "distinct_parent_control_point_count": len(entry["_parents"]),
                "distinct_raw_reference_count": len(entry["_raw_values"]),
                "cp_alignment_same_cp_count": entry["_cp_alignment_counts"].get("same_cp", 0),
                "cp_alignment_different_cp_count": entry["_cp_alignment_counts"].get("different_cp", 0),
                "cp_alignment_zero_cp_count": entry["_cp_alignment_counts"].get("zero_cp", 0),
                "cp_alignment_unparsed_count": entry["_cp_alignment_counts"].get("unparsed", 0),
                "trailing_matches_codeline_count": entry["_trailing_alignment_counts"].get("matches_codeline", 0),
                "trailing_differs_from_codeline_count": entry["_trailing_alignment_counts"].get("differs_from_codeline", 0),
                "trailing_default_zero_count": entry["_trailing_alignment_counts"].get("default_zero", 0),
                "trailing_blank_count": entry["_trailing_alignment_counts"].get("blank", 0),
                "station_names": "|".join(sorted(entry["_station_names"])),
                "raw_reference_examples": "|".join(sorted(entry["_raw_values"])[:12]),
                "sample_component_paths": "|".join(entry["_component_paths"]),
            }
        )

    normalized_rows.sort(
        key=lambda item: (
            item["component_family"],
            item["assignment_owner_type"],
            -int(item["row_count"]),
            item["reference_column"],
            item["subdivision_name"],
        )
    )
    return normalized_rows


def build_cp_zero_first_pattern_summary(cp_assignment_summary: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str, str, str, str], dict] = {}
    for row in cp_assignment_summary:
        raw_reference_value = normalize_text(row["raw_reference_value"])
        if row["match_status"] != "unresolved" or not raw_reference_value.startswith("0;"):
            continue
        parsed = parse_reference_part(raw_reference_value)
        if parsed.is_default_zero_reference:
            continue

        cp_alignment = classify_reference_cp_alignment(raw_reference_value, row["parent_control_point_uid"])
        trailing_alignment = classify_reference_trailing_token_alignment(raw_reference_value, row["component_codeline"])
        key = (
            row["component_family"],
            row["assignment_owner_type"],
            row["reference_column"],
            raw_reference_value,
            row["subdivision_name"],
        )
        entry = grouped.setdefault(
            key,
            {
                "component_family": row["component_family"],
                "assignment_owner_type": row["assignment_owner_type"],
                "reference_column": row["reference_column"],
                "reference_kind": row["reference_kind"],
                "reference_format": row["reference_format"],
                "raw_reference_value": raw_reference_value,
                "subdivision_name": row["subdivision_name"],
                "row_count": 0,
                "_components": set(),
                "_parents": set(),
                "_station_names": set(),
                "_component_codelines": set(),
                "_component_paths": [],
                "_cp_alignments": set(),
                "_trailing_alignments": set(),
            },
        )
        entry["row_count"] += 1
        if row["component_uid"]:
            entry["_components"].add(row["component_uid"])
        if row["parent_control_point_uid"]:
            entry["_parents"].add(row["parent_control_point_uid"])
        if row["station_name"]:
            entry["_station_names"].add(row["station_name"])
        if row["component_codeline"]:
            entry["_component_codelines"].add(row["component_codeline"])
        component_path = f"{row['parent_control_point_name']}:{row['component_name']}".strip(":")
        if component_path and component_path not in entry["_component_paths"] and len(entry["_component_paths"]) < 12:
            entry["_component_paths"].append(component_path)
        if cp_alignment:
            entry["_cp_alignments"].add(cp_alignment)
        if trailing_alignment:
            entry["_trailing_alignments"].add(trailing_alignment)

    normalized_rows: list[dict] = []
    for entry in grouped.values():
        normalized_rows.append(
            {
                "component_family": entry["component_family"],
                "assignment_owner_type": entry["assignment_owner_type"],
                "reference_column": entry["reference_column"],
                "reference_kind": entry["reference_kind"],
                "reference_format": entry["reference_format"],
                "raw_reference_value": entry["raw_reference_value"],
                "subdivision_name": entry["subdivision_name"],
                "row_count": entry["row_count"],
                "distinct_component_count": len(entry["_components"]),
                "distinct_parent_control_point_count": len(entry["_parents"]),
                "distinct_component_codeline_count": len(entry["_component_codelines"]),
                "component_codelines": "|".join(sorted(entry["_component_codelines"])),
                "cp_alignment_values": "|".join(sorted(entry["_cp_alignments"])),
                "trailing_alignment_values": "|".join(sorted(entry["_trailing_alignments"])),
                "station_names": "|".join(sorted(entry["_station_names"])),
                "sample_component_paths": "|".join(entry["_component_paths"]),
            }
        )

    normalized_rows.sort(
        key=lambda item: (
            item["component_family"],
            item["assignment_owner_type"],
            -int(item["row_count"]),
            item["reference_column"],
            item["raw_reference_value"],
        )
    )
    return normalized_rows


def build_cp_zero_first_candidate_scope_summary(zero_first_reference_summary: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str, str, str, str, str], dict] = {}
    for row in zero_first_reference_summary:
        component_family = normalize_text(row["component_family"])
        if component_family not in {"control_point", "misc_device"}:
            continue
        reference_column = normalize_text(row["reference_column"])
        if not (reference_column.startswith("CPCTL") or reference_column.startswith("CPIND")):
            continue

        assignment_owner_type = infer_assignment_owner_type(component_family)
        subdivision_name = infer_subdivision_name_from_uid(row["parent_control_point_uid"])
        key = (
            component_family,
            assignment_owner_type,
            reference_column,
            normalize_text(row["structural_class"]),
            normalize_text(row["preferred_scope_source"]),
            subdivision_name,
        )
        entry = grouped.setdefault(
            key,
            {
                "component_family": component_family,
                "assignment_owner_type": assignment_owner_type,
                "reference_column": reference_column,
                "reference_kind": normalize_text(row["reference_kind"]),
                "reference_format": normalize_text(row["reference_format"]),
                "structural_class": normalize_text(row["structural_class"]),
                "preferred_scope_source": normalize_text(row["preferred_scope_source"]),
                "subdivision_name": subdivision_name,
                "row_count": 0,
                "_components": set(),
                "_parents": set(),
                "_scope_code_lines": set(),
                "_candidate_count_values": set(),
                "_candidate_examples": [],
                "_resolved_sibling_examples": [],
                "_structural_classes": set(),
                "has_resolved_sibling_row_count": 0,
            },
        )
        entry["row_count"] += 1
        if row["component_uid"]:
            entry["_components"].add(normalize_text(row["component_uid"]))
        if row["parent_control_point_uid"]:
            entry["_parents"].add(normalize_text(row["parent_control_point_uid"]))
        if row["preferred_scope_code_line"]:
            entry["_scope_code_lines"].add(normalize_text(row["preferred_scope_code_line"]))
        if row["preferred_scope_candidate_count"]:
            entry["_candidate_count_values"].add(normalize_text(row["preferred_scope_candidate_count"]))
        if normalize_text(row["has_resolved_sibling_in_same_field"]) == "True":
            entry["has_resolved_sibling_row_count"] += 1
        if row["structural_class"]:
            entry["_structural_classes"].add(normalize_text(row["structural_class"]))
        for example in split_non_empty_tokens(row["preferred_scope_candidate_examples"], "|"):
            if example not in entry["_candidate_examples"] and len(entry["_candidate_examples"]) < 24:
                entry["_candidate_examples"].append(example)
        for example in split_non_empty_tokens(row["resolved_sibling_examples"], "|"):
            if example not in entry["_resolved_sibling_examples"] and len(entry["_resolved_sibling_examples"]) < 12:
                entry["_resolved_sibling_examples"].append(example)

    normalized_rows: list[dict] = []
    for entry in grouped.values():
        normalized_rows.append(
            {
                "component_family": entry["component_family"],
                "assignment_owner_type": entry["assignment_owner_type"],
                "reference_column": entry["reference_column"],
                "reference_kind": entry["reference_kind"],
                "reference_format": entry["reference_format"],
                "structural_class": entry["structural_class"],
                "preferred_scope_source": entry["preferred_scope_source"],
                "subdivision_name": entry["subdivision_name"],
                "row_count": entry["row_count"],
                "distinct_component_count": len(entry["_components"]),
                "distinct_parent_control_point_count": len(entry["_parents"]),
                "scope_code_lines": "|".join(sorted(entry["_scope_code_lines"])),
                "scope_candidate_count_values": "|".join(
                    sorted(entry["_candidate_count_values"], key=lambda value: int(value) if str(value).isdigit() else str(value))
                ),
                "has_resolved_sibling_row_count": entry["has_resolved_sibling_row_count"],
                "candidate_examples": "|".join(entry["_candidate_examples"]),
                "resolved_sibling_examples": "|".join(entry["_resolved_sibling_examples"]),
            }
        )

    normalized_rows.sort(
        key=lambda item: (
            item["component_family"],
            item["assignment_owner_type"],
            -int(item["row_count"]),
            item["reference_column"],
            item["preferred_scope_source"],
            item["subdivision_name"],
        )
    )
    return normalized_rows


def build_cp_direct_four_token_local_bit_diagnostic(cp_assignment_summary: list[dict]) -> tuple[list[dict], list[dict]]:
    station_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_station_context.csv")
    code_assignment_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_assignment_context.csv")
    station_lookup = {
        normalize_text(row["ControlPointNumber"]): row
        for row in station_rows
    }
    code_bit_lookup = build_code_bit_lookup(code_assignment_rows)

    per_row_rows: list[dict] = []
    for row in cp_assignment_summary:
        if row["assignment_owner_type"] != "direct_control_point_slot":
            continue
        if row["reference_column"] not in {"CPIND11", "CPIND12", "CPIND13"}:
            continue
        if row["match_status"] != "unresolved":
            continue

        raw_reference_value = normalize_text(row["raw_reference_value"])
        parsed = parse_reference_part(raw_reference_value)
        station_row = station_lookup.get(normalize_text(row["parent_control_point_uid"]), {})
        local_code_line = normalize_text(station_row.get("CodeLineNumber"))
        local_code_station_number = normalize_text(station_row.get("CodeStationNumber"))
        local_bit_token = normalize_text(parsed.bit_position)
        diagnostic_matches = code_bit_lookup.get(
            (
                local_code_line,
                local_code_station_number,
                normalize_text(row["parent_control_point_uid"]),
                local_bit_token,
                "I",
            ),
            [],
        )
        diagnostic_status = (
            "one_candidate"
            if len(diagnostic_matches) == 1
            else "multi_candidate"
            if len(diagnostic_matches) > 1
            else "zero_candidate"
        )
        diagnostic_mnemonics = []
        diagnostic_long_names = []
        diagnostic_examples = []
        for match in diagnostic_matches:
            mnemonic = normalize_text(match.get("Mnemonic"))
            long_name = normalize_text(match.get("LongName"))
            if mnemonic:
                diagnostic_mnemonics.append(mnemonic)
            if long_name:
                diagnostic_long_names.append(long_name)
            example = f"{normalize_text(match.get('BPAssignment'))}={mnemonic}:{long_name}".strip(":")
            if example:
                diagnostic_examples.append(example)

        per_row_rows.append(
            {
                "reference_column": normalize_text(row["reference_column"]),
                "component_uid": normalize_text(row["component_uid"]),
                "component_name": normalize_text(row["component_name"]),
                "parent_control_point_uid": normalize_text(row["parent_control_point_uid"]),
                "parent_control_point_name": normalize_text(row["parent_control_point_name"]),
                "subdivision_name": infer_subdivision_name_from_uid(row["parent_control_point_uid"]),
                "raw_reference_value": raw_reference_value,
                "reference_format": normalize_text(row["reference_format"]),
                "diagnostic_rule": "parent_station_local_bit_lookup",
                "local_code_line": local_code_line,
                "local_code_station_number": local_code_station_number,
                "local_bit_token": local_bit_token,
                "diagnostic_match_count": str(len(diagnostic_matches)),
                "diagnostic_status": diagnostic_status,
                "diagnostic_candidate_mnemonics": "|".join(diagnostic_mnemonics),
                "diagnostic_candidate_long_names": "|".join(diagnostic_long_names),
                "diagnostic_candidate_family_set": build_candidate_family_set("|".join(diagnostic_long_names)),
                "diagnostic_candidate_examples": "|".join(diagnostic_examples),
            }
        )

    rows_by_cp: dict[str, list[dict[str, str]]] = {}
    for row in per_row_rows:
        rows_by_cp.setdefault(row["parent_control_point_uid"], []).append(row)

    for cp_rows in rows_by_cp.values():
        distinct_raw = {row["raw_reference_value"] for row in cp_rows if row["raw_reference_value"]}
        distinct_status = {row["diagnostic_status"] for row in cp_rows if row["diagnostic_status"]}
        distinct_examples = {row["diagnostic_candidate_examples"] for row in cp_rows if row["diagnostic_candidate_examples"]}
        for row in cp_rows:
            row["cp_triplet_same_raw"] = str(len(distinct_raw) == 1 and len(cp_rows) == 3)
            row["cp_triplet_same_diagnostic_status"] = str(len(distinct_status) == 1 and len(cp_rows) == 3)
            row["cp_triplet_same_candidate_examples"] = str(len(distinct_examples) == 1 and len(cp_rows) == 3)

    grouped: dict[tuple[str, str, str], dict] = {}
    for row in per_row_rows:
        key = (
            row["reference_column"],
            row["subdivision_name"],
            row["diagnostic_status"],
        )
        entry = grouped.setdefault(
            key,
            {
                "reference_column": row["reference_column"],
                "subdivision_name": row["subdivision_name"],
                "diagnostic_status": row["diagnostic_status"],
                "row_count": 0,
                "_parent_control_points": set(),
                "_local_code_lines": set(),
                "_local_code_station_numbers": set(),
                "_local_bit_tokens": set(),
                "_candidate_mnemonics": set(),
                "_candidate_long_names": set(),
                "_candidate_examples": [],
                "triplet_same_raw_count": 0,
                "triplet_same_status_count": 0,
                "triplet_same_candidate_examples_count": 0,
            },
        )
        entry["row_count"] += 1
        if row["parent_control_point_uid"]:
            entry["_parent_control_points"].add(row["parent_control_point_uid"])
        if row["local_code_line"]:
            entry["_local_code_lines"].add(row["local_code_line"])
        if row["local_code_station_number"]:
            entry["_local_code_station_numbers"].add(row["local_code_station_number"])
        if row["local_bit_token"]:
            entry["_local_bit_tokens"].add(row["local_bit_token"])
        for mnemonic in split_non_empty_tokens(row["diagnostic_candidate_mnemonics"], "|"):
            entry["_candidate_mnemonics"].add(mnemonic)
        for long_name in split_non_empty_tokens(row["diagnostic_candidate_long_names"], "|"):
            entry["_candidate_long_names"].add(long_name)
        for example in split_non_empty_tokens(row["diagnostic_candidate_examples"], "|"):
            if example not in entry["_candidate_examples"] and len(entry["_candidate_examples"]) < 24:
                entry["_candidate_examples"].append(example)
        if row["cp_triplet_same_raw"] == "True":
            entry["triplet_same_raw_count"] += 1
        if row["cp_triplet_same_diagnostic_status"] == "True":
            entry["triplet_same_status_count"] += 1
        if row["cp_triplet_same_candidate_examples"] == "True":
            entry["triplet_same_candidate_examples_count"] += 1

    summary_rows: list[dict] = []
    for entry in grouped.values():
        summary_rows.append(
            {
                "reference_column": entry["reference_column"],
                "subdivision_name": entry["subdivision_name"],
                "diagnostic_status": entry["diagnostic_status"],
                "row_count": entry["row_count"],
                "distinct_parent_control_point_count": len(entry["_parent_control_points"]),
                "local_code_lines": "|".join(sorted(entry["_local_code_lines"])),
                "local_code_station_numbers": "|".join(sorted(entry["_local_code_station_numbers"])),
                "local_bit_tokens": "|".join(sorted(entry["_local_bit_tokens"], key=lambda value: int(value) if value.isdigit() else value)),
                "candidate_mnemonics": "|".join(sorted(entry["_candidate_mnemonics"])),
                "candidate_long_names": "|".join(sorted(entry["_candidate_long_names"])),
                "candidate_examples": "|".join(entry["_candidate_examples"]),
                "triplet_same_raw_count": entry["triplet_same_raw_count"],
                "triplet_same_status_count": entry["triplet_same_status_count"],
                "triplet_same_candidate_examples_count": entry["triplet_same_candidate_examples_count"],
            }
        )

    per_row_rows.sort(
        key=lambda item: (
            item["subdivision_name"],
            int(item["parent_control_point_uid"] or 0),
            item["reference_column"],
        )
    )
    summary_rows.sort(
        key=lambda item: (
            item["subdivision_name"],
            item["reference_column"],
            item["diagnostic_status"],
        )
    )
    return per_row_rows, summary_rows


def build_cp_direct_four_token_candidate_family_summary(
    diagnostic_rows: list[dict],
) -> list[dict]:
    grouped: dict[tuple[str, str, str], dict] = {}
    for row in diagnostic_rows:
        key = (
            normalize_text(row["subdivision_name"]),
            normalize_text(row["diagnostic_status"]),
            normalize_text(row["diagnostic_candidate_family_set"]) or "no_candidate",
        )
        entry = grouped.setdefault(
            key,
            {
                "subdivision_name": key[0],
                "diagnostic_status": key[1],
                "candidate_family_set": key[2],
                "row_count": 0,
                "_reference_columns": set(),
                "_parent_control_points": set(),
                "_local_code_lines": set(),
                "_local_bit_tokens": set(),
                "_candidate_mnemonics": set(),
                "_candidate_long_names": set(),
                "_candidate_examples": [],
            },
        )
        entry["row_count"] += 1
        if row["reference_column"]:
            entry["_reference_columns"].add(row["reference_column"])
        if row["parent_control_point_uid"]:
            entry["_parent_control_points"].add(row["parent_control_point_uid"])
        if row["local_code_line"]:
            entry["_local_code_lines"].add(row["local_code_line"])
        if row["local_bit_token"]:
            entry["_local_bit_tokens"].add(row["local_bit_token"])
        for mnemonic in split_non_empty_tokens(row["diagnostic_candidate_mnemonics"], "|"):
            entry["_candidate_mnemonics"].add(mnemonic)
        for long_name in split_non_empty_tokens(row["diagnostic_candidate_long_names"], "|"):
            entry["_candidate_long_names"].add(long_name)
        for example in split_non_empty_tokens(row["diagnostic_candidate_examples"], "|"):
            if example not in entry["_candidate_examples"] and len(entry["_candidate_examples"]) < 24:
                entry["_candidate_examples"].append(example)

    summary_rows: list[dict] = []
    for entry in grouped.values():
        summary_rows.append(
            {
                "subdivision_name": entry["subdivision_name"],
                "diagnostic_status": entry["diagnostic_status"],
                "candidate_family_set": entry["candidate_family_set"],
                "row_count": entry["row_count"],
                "distinct_reference_column_count": len(entry["_reference_columns"]),
                "reference_columns": "|".join(sorted(entry["_reference_columns"])),
                "distinct_parent_control_point_count": len(entry["_parent_control_points"]),
                "local_code_lines": "|".join(sorted(entry["_local_code_lines"])),
                "local_bit_tokens": "|".join(
                    sorted(
                        entry["_local_bit_tokens"],
                        key=lambda value: int(value) if value.isdigit() else value,
                    )
                ),
                "candidate_mnemonics": "|".join(sorted(entry["_candidate_mnemonics"])),
                "candidate_long_names": "|".join(sorted(entry["_candidate_long_names"])),
                "candidate_examples": "|".join(entry["_candidate_examples"]),
            }
        )

    summary_rows.sort(
        key=lambda item: (
            item["subdivision_name"],
            item["diagnostic_status"],
            item["candidate_family_set"],
        )
    )
    return summary_rows


def build_reference_family_summary(component_bit_reference_map: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str, str], list[dict[str, str]]] = {}
    for row in component_bit_reference_map:
        component_family = normalize_text(row["component_family"])
        key = (
            component_family,
            infer_assignment_owner_type(component_family),
            normalize_text(row["reference_column"]),
        )
        grouped.setdefault(key, []).append(row)

    normalized_rows: list[dict] = []
    for (component_family, assignment_owner_type, reference_column), rows in grouped.items():
        reference_kind = normalize_text(rows[0]["reference_kind"]) if rows else ""
        status_counts: dict[str, int] = {}
        resolution_method_counts: dict[str, int] = {}
        subdivision_names: set[str] = set()
        codeline_numbers: set[str] = set()
        component_uids: set[str] = set()
        parent_control_point_uids: set[str] = set()
        unresolved_raw_values: list[str] = []
        default_zero_raw_values: list[str] = []
        resolved_mnemonics: list[str] = []
        resolved_long_names: list[str] = []
        resolved_reference_values: list[str] = []
        reference_formats: set[str] = set()

        for row in rows:
            match_status = normalize_text(row["match_status"])
            status_counts[match_status] = status_counts.get(match_status, 0) + 1
            resolution_method = normalize_text(row["resolution_method"])
            if resolution_method:
                resolution_method_counts[resolution_method] = resolution_method_counts.get(resolution_method, 0) + 1
            subdivision_name = infer_subdivision_name_from_uid(row["parent_control_point_uid"])
            if subdivision_name:
                subdivision_names.add(subdivision_name)
            codeline_number = normalize_text(row["component_codeline"])
            if codeline_number:
                codeline_numbers.add(codeline_number)
            component_uid = normalize_text(row["component_uid"])
            if component_uid:
                component_uids.add(component_uid)
            parent_uid = normalize_text(row["parent_control_point_uid"])
            if parent_uid:
                parent_control_point_uids.add(parent_uid)
            reference_format = normalize_text(row["reference_format"])
            if reference_format:
                reference_formats.add(reference_format)
            raw_reference_value = normalize_text(row["raw_reference_value"])
            if match_status == "unresolved" and raw_reference_value:
                unresolved_raw_values.append(raw_reference_value)
            elif match_status == "default_zero" and raw_reference_value:
                default_zero_raw_values.append(raw_reference_value)
            elif match_status == "resolved":
                resolved_reference_values.append(raw_reference_value)
                mnemonic = normalize_text(row["resolved_mnemonic"])
                long_name = normalize_text(row["resolved_long_name"])
                if mnemonic:
                    resolved_mnemonics.append(mnemonic)
                if long_name:
                    resolved_long_names.append(long_name)

        unresolved_counter = {}
        for value in unresolved_raw_values:
            unresolved_counter[value] = unresolved_counter.get(value, 0) + 1
        default_zero_counter = {}
        for value in default_zero_raw_values:
            default_zero_counter[value] = default_zero_counter.get(value, 0) + 1
        resolved_mnemonic_counter = {}
        for value in resolved_mnemonics:
            resolved_mnemonic_counter[value] = resolved_mnemonic_counter.get(value, 0) + 1
        resolved_long_name_counter = {}
        for value in resolved_long_names:
            resolved_long_name_counter[value] = resolved_long_name_counter.get(value, 0) + 1

        normalized_rows.append(
            {
                "component_family": component_family,
                "assignment_owner_type": assignment_owner_type,
                "reference_column": reference_column,
                "reference_kind": reference_kind,
                "row_count": len(rows),
                "resolved_count": status_counts.get("resolved", 0),
                "unresolved_count": status_counts.get("unresolved", 0),
                "default_zero_count": status_counts.get("default_zero", 0),
                "distinct_component_count": len(component_uids),
                "distinct_parent_control_point_count": len(parent_control_point_uids),
                "distinct_codeline_count": len(codeline_numbers),
                "subdivision_names": "|".join(sorted(subdivision_names)),
                "reference_formats": "|".join(sorted(reference_formats)),
                "resolution_methods": "|".join(
                    method for method, _ in sorted(resolution_method_counts.items(), key=lambda item: (-item[1], item[0]))
                ),
                "top_unresolved_raw_values": "|".join(
                    value for value, _ in sorted(unresolved_counter.items(), key=lambda item: (-item[1], item[0]))[:12]
                ),
                "top_default_zero_values": "|".join(
                    value for value, _ in sorted(default_zero_counter.items(), key=lambda item: (-item[1], item[0]))[:4]
                ),
                "top_resolved_reference_values": "|".join(
                    value for value in list(dict.fromkeys(resolved_reference_values))[:12]
                ),
                "top_resolved_mnemonics": "|".join(
                    value for value, _ in sorted(resolved_mnemonic_counter.items(), key=lambda item: (-item[1], item[0]))[:12]
                ),
                "top_resolved_long_names": "|".join(
                    value for value, _ in sorted(resolved_long_name_counter.items(), key=lambda item: (-item[1], item[0]))[:12]
                ),
            }
        )

    normalized_rows.sort(
        key=lambda item: (
            -int(item["unresolved_count"]),
            -int(item["resolved_count"]),
            item["component_family"],
            item["reference_column"],
        )
    )
    return normalized_rows


def build_switch_rcps_foundation_summary(component_bit_reference_map: list[dict]) -> tuple[list[dict], list[dict]]:
    switch_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.switch_detail_full.csv")
    component_reference_rows = [
        row
        for row in component_bit_reference_map
        if normalize_text(row["component_family"]) == "switch"
    ]

    references_by_uid: dict[str, list[dict[str, str]]] = {}
    for row in component_reference_rows:
        references_by_uid.setdefault(normalize_text(row["component_uid"]), []).append(row)

    per_switch_rows: list[dict] = []
    grouped_pattern_rows: dict[tuple[str, str, str, str, str, str], dict[str, object]] = {}

    for row in switch_rows:
        switch_uid = normalize_text(row["UID"])
        switch_name = normalize_text(row["Name"])
        control_point_uid = normalize_text(row["ControlPoint"])
        subdivision_name = infer_subdivision_name_from_uid(control_point_uid)
        switch_refs = references_by_uid.get(switch_uid, [])

        sibling_counts: dict[str, dict[str, int]] = {}
        sibling_mnemonics: dict[str, list[str]] = {}
        sibling_long_names: dict[str, list[str]] = {}
        for ref in switch_refs:
            column = normalize_text(ref["reference_column"])
            sibling_counts.setdefault(column, {})
            status = normalize_text(ref["match_status"])
            sibling_counts[column][status] = sibling_counts[column].get(status, 0) + 1
            if status == "resolved":
                mnemonic = normalize_text(ref["resolved_mnemonic"])
                long_name = normalize_text(ref["resolved_long_name"])
                if mnemonic:
                    sibling_mnemonics.setdefault(column, [])
                    if mnemonic not in sibling_mnemonics[column]:
                        sibling_mnemonics[column].append(mnemonic)
                if long_name:
                    sibling_long_names.setdefault(column, [])
                    if long_name not in sibling_long_names[column]:
                        sibling_long_names[column].append(long_name)

        rcps_control_tokens = split_non_empty_tokens(row.get("RCPSControlBits"), "|")
        rcps_indication_tokens = split_non_empty_tokens(row.get("RCPSIndicationBits"), "|")
        rcps_control_distinct = list(dict.fromkeys(rcps_control_tokens))
        rcps_indication_distinct = list(dict.fromkeys(rcps_indication_tokens))

        per_switch_rows.append(
            {
                "switch_uid": switch_uid,
                "control_point_uid": control_point_uid,
                "subdivision_name": subdivision_name,
                "codeline": normalize_text(row.get("Codeline")),
                "switch_name": switch_name,
                "track_name": normalize_text(row.get("TrkName")),
                "type_of_switch": normalize_text(row.get("TypeOfSwitch")),
                "switch_point_monitor_mode": normalize_text(row.get("SwitchPointMonitorMode")),
                "switch_lock_no_control": normalize_text(row.get("SwitchLockNoControl")),
                "rcps_flag": normalize_text(row.get("RCPS")),
                "rcps_control_token_count": len(rcps_control_tokens),
                "rcps_control_distinct_token_count": len(rcps_control_distinct),
                "rcps_control_distinct_tokens": "|".join(rcps_control_distinct),
                "rcps_control_all_tokens_same": str(len(rcps_control_distinct) == 1 and len(rcps_control_tokens) > 0),
                "rcps_indication_token_count": len(rcps_indication_tokens),
                "rcps_indication_distinct_token_count": len(rcps_indication_distinct),
                "rcps_indication_distinct_tokens": "|".join(rcps_indication_distinct),
                "rcps_indication_all_tokens_same": str(len(rcps_indication_distinct) == 1 and len(rcps_indication_tokens) > 0),
                "lock_bit_raw": normalize_text(row.get("LockBit")),
                "switch_block_bit_raw": normalize_text(row.get("SwitchBlkBit")),
                "pctl_resolved_count": sibling_counts.get("PCTL", {}).get("resolved", 0),
                "pctl_unresolved_count": sibling_counts.get("PCTL", {}).get("unresolved", 0),
                "pctl_resolved_mnemonics": "|".join(sibling_mnemonics.get("PCTL", [])),
                "pind_resolved_count": sibling_counts.get("PIND", {}).get("resolved", 0),
                "pind_resolved_mnemonics": "|".join(sibling_mnemonics.get("PIND", [])),
                "sctl_resolved_count": sibling_counts.get("SCTL", {}).get("resolved", 0),
                "sctl_unresolved_count": sibling_counts.get("SCTL", {}).get("unresolved", 0),
                "sctl_resolved_mnemonics": "|".join(sibling_mnemonics.get("SCTL", [])),
                "sind_resolved_count": sibling_counts.get("SIND", {}).get("resolved", 0),
                "sind_resolved_mnemonics": "|".join(sibling_mnemonics.get("SIND", [])),
                "tctl_unresolved_count": sibling_counts.get("TCTL", {}).get("unresolved", 0),
                "tind_unresolved_count": sibling_counts.get("TIND", {}).get("unresolved", 0),
                "lockbit_unresolved_count": sibling_counts.get("LockBit", {}).get("unresolved", 0),
                "switchblk_default_zero_count": sibling_counts.get("SwitchBlkBit", {}).get("default_zero", 0),
            }
        )

        pattern_key = (
            subdivision_name,
            normalize_text(row.get("TypeOfSwitch")),
            normalize_text(row.get("SwitchPointMonitorMode")),
            normalize_text(row.get("RCPS")),
            "|".join(rcps_control_distinct),
            "|".join(rcps_indication_distinct),
        )
        entry = grouped_pattern_rows.setdefault(
            pattern_key,
            {
                "subdivision_name": subdivision_name,
                "type_of_switch": normalize_text(row.get("TypeOfSwitch")),
                "switch_point_monitor_mode": normalize_text(row.get("SwitchPointMonitorMode")),
                "rcps_flag": normalize_text(row.get("RCPS")),
                "rcps_control_distinct_tokens": "|".join(rcps_control_distinct),
                "rcps_indication_distinct_tokens": "|".join(rcps_indication_distinct),
                "rcps_control_token_count": len(rcps_control_tokens),
                "rcps_indication_token_count": len(rcps_indication_tokens),
                "switch_count": 0,
                "_switch_examples": [],
                "_control_points": set(),
                "_resolved_pctl": set(),
                "_resolved_sctl": set(),
            },
        )
        entry["switch_count"] += 1
        if switch_name and len(entry["_switch_examples"]) < 8:
            entry["_switch_examples"].append(f"{switch_uid}:{switch_name}")
        if control_point_uid:
            entry["_control_points"].add(control_point_uid)
        for mnemonic in sibling_mnemonics.get("PCTL", []):
            entry["_resolved_pctl"].add(mnemonic)
        for mnemonic in sibling_mnemonics.get("SCTL", []):
            entry["_resolved_sctl"].add(mnemonic)

    grouped_rows: list[dict] = []
    for entry in grouped_pattern_rows.values():
        grouped_rows.append(
            {
                "subdivision_name": entry["subdivision_name"],
                "type_of_switch": entry["type_of_switch"],
                "switch_point_monitor_mode": entry["switch_point_monitor_mode"],
                "rcps_flag": entry["rcps_flag"],
                "rcps_control_token_count": entry["rcps_control_token_count"],
                "rcps_indication_token_count": entry["rcps_indication_token_count"],
                "rcps_control_distinct_tokens": entry["rcps_control_distinct_tokens"],
                "rcps_indication_distinct_tokens": entry["rcps_indication_distinct_tokens"],
                "switch_count": entry["switch_count"],
                "distinct_control_point_count": len(entry["_control_points"]),
                "switch_examples": "|".join(entry["_switch_examples"]),
                "resolved_pctl_mnemonics": "|".join(sorted(entry["_resolved_pctl"])),
                "resolved_sctl_mnemonics": "|".join(sorted(entry["_resolved_sctl"])),
            }
        )

    per_switch_rows.sort(key=lambda item: (item["subdivision_name"], item["control_point_uid"], item["switch_uid"]))
    grouped_rows.sort(key=lambda item: (-int(item["switch_count"]), item["subdivision_name"], item["type_of_switch"]))
    return per_switch_rows, grouped_rows


def build_switch_shared_anchor_summary(component_bit_reference_map: list[dict]) -> tuple[list[dict], list[dict]]:
    switch_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.switch_detail_full.csv")
    component_reference_rows = [
        row
        for row in component_bit_reference_map
        if normalize_text(row["component_family"]) == "switch"
    ]

    references_by_uid: dict[str, list[dict[str, str]]] = {}
    for row in component_reference_rows:
        references_by_uid.setdefault(normalize_text(row["component_uid"]), []).append(row)

    per_switch_rows: list[dict] = []
    grouped_rows_by_cp: dict[str, dict[str, object]] = {}

    for row in switch_rows:
        switch_uid = normalize_text(row["UID"])
        switch_name = normalize_text(row["Name"])
        control_point_uid = normalize_text(row["ControlPoint"])
        subdivision_name = infer_subdivision_name_from_uid(control_point_uid)
        switch_refs = references_by_uid.get(switch_uid, [])

        sibling_counts: dict[str, dict[str, int]] = {}
        sibling_mnemonics: dict[str, list[str]] = {}
        for ref in switch_refs:
            column = normalize_text(ref["reference_column"])
            sibling_counts.setdefault(column, {})
            status = normalize_text(ref["match_status"])
            sibling_counts[column][status] = sibling_counts[column].get(status, 0) + 1
            if status == "resolved":
                mnemonic = normalize_text(ref["resolved_mnemonic"])
                if mnemonic:
                    sibling_mnemonics.setdefault(column, [])
                    if mnemonic not in sibling_mnemonics[column]:
                        sibling_mnemonics[column].append(mnemonic)

        rcps_control_parts = canonicalize_reference_parts(row.get("RCPSControlBits"))
        rcps_indication_parts = canonicalize_reference_parts(row.get("RCPSIndicationBits"))
        shared_anchor_raw = rcps_control_parts[0] if rcps_control_parts else ""
        rcps_control_distinct = list(dict.fromkeys(rcps_control_parts))
        rcps_indication_distinct = list(dict.fromkeys(rcps_indication_parts))

        lockbit_raw = canonicalize_reference_part(row.get("LockBit"))
        tind_raw = canonicalize_reference_part(row.get("TIND"))
        tctl_parts = canonicalize_reference_parts(row.get("TCTL"))
        tctl_non_anchor_siblings = [
            part
            for part in tctl_parts
            if part and part != shared_anchor_raw
        ]
        switch_blk_bit_raw = canonicalize_reference_part(row.get("SwitchBlkBit"))
        switch_blk_bit_is_default_zero = parse_reference_part(switch_blk_bit_raw).is_default_zero_reference
        switch_block_control_parts = canonicalize_reference_parts(row.get("SwitchBlockControlBit"))
        switch_block_control_nondefault_parts = [
            part
            for part in switch_block_control_parts
            if not parse_reference_part(part).is_default_zero_reference
        ]

        anchor_equals_rcps_indication = (
            len(rcps_indication_distinct) == 1 and shared_anchor_raw == (rcps_indication_distinct[0] if rcps_indication_distinct else "")
        )
        anchor_equals_lockbit = shared_anchor_raw == lockbit_raw
        anchor_equals_tind = shared_anchor_raw == tind_raw
        anchor_present_in_tctl = shared_anchor_raw in tctl_parts if shared_anchor_raw else False
        tctl_all_parts_anchor = bool(tctl_parts) and all(part == shared_anchor_raw for part in tctl_parts)

        per_switch_rows.append(
            {
                "switch_uid": switch_uid,
                "control_point_uid": control_point_uid,
                "subdivision_name": subdivision_name,
                "codeline": normalize_text(row.get("Codeline")),
                "switch_name": switch_name,
                "track_name": normalize_text(row.get("TrkName")),
                "type_of_switch": normalize_text(row.get("TypeOfSwitch")),
                "switch_point_monitor_mode": normalize_text(row.get("SwitchPointMonitorMode")),
                "rcps_flag": normalize_text(row.get("RCPS")),
                "shared_anchor_raw": shared_anchor_raw,
                "rcps_control_distinct_tokens": "|".join(rcps_control_distinct),
                "rcps_indication_distinct_tokens": "|".join(rcps_indication_distinct),
                "anchor_equals_rcps_indication": str(anchor_equals_rcps_indication),
                "lockbit_raw": lockbit_raw,
                "tind_raw": tind_raw,
                "anchor_equals_lockbit": str(anchor_equals_lockbit),
                "anchor_equals_tind": str(anchor_equals_tind),
                "tctl_parts": "|".join(tctl_parts),
                "anchor_present_in_tctl": str(anchor_present_in_tctl),
                "tctl_all_parts_anchor": str(tctl_all_parts_anchor),
                "tctl_non_anchor_sibling_count": len(tctl_non_anchor_siblings),
                "tctl_non_anchor_siblings": "|".join(tctl_non_anchor_siblings),
                "switch_blk_bit_raw": switch_blk_bit_raw,
                "switch_blk_bit_is_default_zero": str(switch_blk_bit_is_default_zero),
                "switch_blk_bit_is_nondefault": str(bool(switch_blk_bit_raw) and not switch_blk_bit_is_default_zero),
                "switch_block_control_parts": "|".join(switch_block_control_parts),
                "switch_block_control_part_count": len(switch_block_control_parts),
                "switch_block_control_nondefault_part_count": len(switch_block_control_nondefault_parts),
                "switch_block_control_resolved_count": sibling_counts.get("SwitchBlockControlBit", {}).get("resolved", 0),
                "switch_block_control_unresolved_count": sibling_counts.get("SwitchBlockControlBit", {}).get("unresolved", 0),
                "switch_block_control_default_zero_count": sibling_counts.get("SwitchBlockControlBit", {}).get("default_zero", 0),
                "switch_block_control_resolved_mnemonics": "|".join(sibling_mnemonics.get("SwitchBlockControlBit", [])),
                "pctl_resolved_mnemonics": "|".join(sibling_mnemonics.get("PCTL", [])),
                "pind_resolved_mnemonics": "|".join(sibling_mnemonics.get("PIND", [])),
                "sctl_resolved_mnemonics": "|".join(sibling_mnemonics.get("SCTL", [])),
                "sind_resolved_mnemonics": "|".join(sibling_mnemonics.get("SIND", [])),
            }
        )

        entry = grouped_rows_by_cp.setdefault(
            control_point_uid,
            {
                "control_point_uid": control_point_uid,
                "subdivision_name": subdivision_name,
                "codeline": normalize_text(row.get("Codeline")),
                "shared_anchor_raw": shared_anchor_raw,
                "switch_count": 0,
                "tctl_non_anchor_switch_count": 0,
                "switch_blk_nondefault_switch_count": 0,
                "switch_block_control_nondefault_switch_count": 0,
                "switch_block_control_resolved_switch_count": 0,
                "switch_block_control_unresolved_switch_count": 0,
                "_switch_examples": [],
                "_type_of_switch": set(),
                "_switch_point_monitor_mode": set(),
                "_tctl_non_anchor_examples": [],
                "_switch_block_control_resolved_mnemonics": set(),
                "_pctl": set(),
                "_sctl": set(),
                "_pind": set(),
                "_sind": set(),
            },
        )
        entry["switch_count"] += 1
        if tctl_non_anchor_siblings:
            entry["tctl_non_anchor_switch_count"] += 1
            if len(entry["_tctl_non_anchor_examples"]) < 8:
                entry["_tctl_non_anchor_examples"].append(
                    f"{switch_uid}:{switch_name}:{'|'.join(tctl_non_anchor_siblings)}"
                )
        if bool(switch_blk_bit_raw) and not switch_blk_bit_is_default_zero:
            entry["switch_blk_nondefault_switch_count"] += 1
        if switch_block_control_nondefault_parts:
            entry["switch_block_control_nondefault_switch_count"] += 1
        if sibling_counts.get("SwitchBlockControlBit", {}).get("resolved", 0) > 0:
            entry["switch_block_control_resolved_switch_count"] += 1
        if sibling_counts.get("SwitchBlockControlBit", {}).get("unresolved", 0) > 0:
            entry["switch_block_control_unresolved_switch_count"] += 1
        if switch_uid and len(entry["_switch_examples"]) < 10:
            entry["_switch_examples"].append(f"{switch_uid}:{switch_name}")
        if normalize_text(row.get("TypeOfSwitch")):
            entry["_type_of_switch"].add(normalize_text(row.get("TypeOfSwitch")))
        if normalize_text(row.get("SwitchPointMonitorMode")):
            entry["_switch_point_monitor_mode"].add(normalize_text(row.get("SwitchPointMonitorMode")))
        for field_name, key in [
            ("SwitchBlockControlBit", "_switch_block_control_resolved_mnemonics"),
            ("PCTL", "_pctl"),
            ("SCTL", "_sctl"),
            ("PIND", "_pind"),
            ("SIND", "_sind"),
        ]:
            for mnemonic in sibling_mnemonics.get(field_name, []):
                entry[key].add(mnemonic)

    grouped_rows: list[dict] = []
    for entry in grouped_rows_by_cp.values():
        grouped_rows.append(
            {
                "control_point_uid": entry["control_point_uid"],
                "subdivision_name": entry["subdivision_name"],
                "codeline": entry["codeline"],
                "shared_anchor_raw": entry["shared_anchor_raw"],
                "switch_count": entry["switch_count"],
                "type_of_switch_values": "|".join(sorted(entry["_type_of_switch"])),
                "switch_point_monitor_mode_values": "|".join(sorted(entry["_switch_point_monitor_mode"])),
                "tctl_non_anchor_switch_count": entry["tctl_non_anchor_switch_count"],
                "switch_blk_nondefault_switch_count": entry["switch_blk_nondefault_switch_count"],
                "switch_block_control_nondefault_switch_count": entry["switch_block_control_nondefault_switch_count"],
                "switch_block_control_resolved_switch_count": entry["switch_block_control_resolved_switch_count"],
                "switch_block_control_unresolved_switch_count": entry["switch_block_control_unresolved_switch_count"],
                "switch_examples": "|".join(entry["_switch_examples"]),
                "tctl_non_anchor_examples": "|".join(entry["_tctl_non_anchor_examples"]),
                "switch_block_control_resolved_mnemonics": "|".join(sorted(entry["_switch_block_control_resolved_mnemonics"])),
                "pctl_resolved_mnemonics": "|".join(sorted(entry["_pctl"])),
                "sctl_resolved_mnemonics": "|".join(sorted(entry["_sctl"])),
                "pind_resolved_mnemonics": "|".join(sorted(entry["_pind"])),
                "sind_resolved_mnemonics": "|".join(sorted(entry["_sind"])),
            }
        )

    per_switch_rows.sort(key=lambda item: (item["subdivision_name"], item["control_point_uid"], item["switch_uid"]))
    grouped_rows.sort(key=lambda item: (item["subdivision_name"], item["control_point_uid"]))
    return per_switch_rows, grouped_rows


def build_track_family_foundation_summary(component_bit_reference_map: list[dict]) -> tuple[list[dict], list[dict]]:
    track_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.track_detail_full.csv")
    component_reference_rows = [
        row
        for row in component_bit_reference_map
        if normalize_text(row["component_family"]) == "track"
    ]

    references_by_uid: dict[str, list[dict[str, str]]] = {}
    for row in component_reference_rows:
        references_by_uid.setdefault(normalize_text(row["component_uid"]), []).append(row)

    per_track_rows: list[dict] = []
    grouped_pattern_rows: dict[tuple[str, str, str, str, str, str, str], dict[str, object]] = {}

    for row in track_rows:
        track_uid = normalize_text(row["UID"])
        control_point_uid = normalize_text(row["ControlPoint"])
        subdivision_name = infer_subdivision_name_from_uid(control_point_uid)
        codeline = normalize_text(row.get("Codeline"))
        track_refs = references_by_uid.get(track_uid, [])

        sibling_counts: dict[str, dict[str, int]] = {}
        sibling_mnemonics: dict[str, list[str]] = {}
        sibling_long_names: dict[str, list[str]] = {}
        for ref in track_refs:
            column = normalize_text(ref["reference_column"])
            sibling_counts.setdefault(column, {})
            status = normalize_text(ref["match_status"])
            sibling_counts[column][status] = sibling_counts[column].get(status, 0) + 1
            if status == "resolved":
                mnemonic = normalize_text(ref["resolved_mnemonic"])
                long_name = normalize_text(ref["resolved_long_name"])
                if mnemonic:
                    sibling_mnemonics.setdefault(column, [])
                    if mnemonic not in sibling_mnemonics[column]:
                        sibling_mnemonics[column].append(mnemonic)
                if long_name:
                    sibling_long_names.setdefault(column, [])
                    if long_name not in sibling_long_names[column]:
                        sibling_long_names[column].append(long_name)

        pind_raw = normalize_text(row.get("PIND"))
        sind_raw = normalize_text(row.get("SIND"))
        tind_raw = normalize_text(row.get("TIND"))
        pctl_raw = normalize_text(row.get("PCTL"))
        sctl_raw = normalize_text(row.get("SCTL"))
        tctl_raw = normalize_text(row.get("TCTL"))
        qind_raw = normalize_text(row.get("QIND"))
        qctl_raw = normalize_text(row.get("QCTL"))
        trackblocking_raw = normalize_text(row.get("TrackBlockingReferences"))
        tracklock_raw = normalize_text(row.get("TrackLockBit"))

        control_cluster_distinct = [
            value
            for value in dict.fromkeys([pctl_raw, sctl_raw, tctl_raw])
            if normalize_text(value)
        ]
        control_cluster_raw = (
            control_cluster_distinct[0]
            if len(control_cluster_distinct) == 1
            else "|".join(control_cluster_distinct)
        )
        control_cluster_all_equal = len(control_cluster_distinct) == 1 and len(control_cluster_distinct) > 0

        qctl_equals_control_cluster = bool(qctl_raw) and qctl_raw == pctl_raw == tctl_raw == sctl_raw
        qind_equals_tind = bool(qind_raw) and qind_raw == tind_raw

        per_track_rows.append(
            {
                "track_uid": track_uid,
                "control_point_uid": control_point_uid,
                "subdivision_name": subdivision_name,
                "codeline": codeline,
                "track_name": normalize_text(row.get("Name")),
                "track_type": normalize_text(row.get("Type")),
                "track_name_group": normalize_text(row.get("TrkName")),
                "territory_assignment": normalize_text(row.get("TerritoryAssignment")),
                "authority_type": normalize_text(row.get("AuthorityType")),
                "route_name": normalize_text(row.get("RouteName")),
                "turn_out_track": normalize_text(row.get("TurnOutTrack")),
                "os_reporting": normalize_text(row.get("OSReporting")),
                "track_uid_raw": normalize_text(row.get("TrackUID")),
                "pind_raw": pind_raw,
                "sind_raw": sind_raw,
                "tind_raw": tind_raw,
                "pctl_raw": pctl_raw,
                "sctl_raw": sctl_raw,
                "tctl_raw": tctl_raw,
                "qind_raw": qind_raw,
                "qctl_raw": qctl_raw,
                "track_lock_bit_raw": tracklock_raw,
                "track_blocking_references_raw": trackblocking_raw,
                "control_cluster_raw": control_cluster_raw,
                "control_cluster_distinct_count": len(control_cluster_distinct),
                "control_cluster_all_equal": str(control_cluster_all_equal),
                "pctl_equals_tctl": str(pctl_raw == tctl_raw),
                "pctl_equals_sctl": str(pctl_raw == sctl_raw),
                "sctl_equals_tctl": str(sctl_raw == tctl_raw),
                "sind_equals_tind": str(sind_raw == tind_raw),
                "qctl_equals_control_cluster": str(qctl_equals_control_cluster),
                "qind_equals_tind": str(qind_equals_tind),
                "pind_cp_alignment": classify_reference_cp_alignment(pind_raw, control_point_uid),
                "sind_cp_alignment": classify_reference_cp_alignment(sind_raw, control_point_uid),
                "tind_cp_alignment": classify_reference_cp_alignment(tind_raw, control_point_uid),
                "pctl_cp_alignment": classify_reference_cp_alignment(pctl_raw, control_point_uid),
                "sctl_cp_alignment": classify_reference_cp_alignment(sctl_raw, control_point_uid),
                "tctl_cp_alignment": classify_reference_cp_alignment(tctl_raw, control_point_uid),
                "qind_cp_alignment": classify_reference_cp_alignment(qind_raw, control_point_uid),
                "qctl_cp_alignment": classify_reference_cp_alignment(qctl_raw, control_point_uid),
                "pind_trailing_alignment": classify_reference_trailing_token_alignment(pind_raw, codeline),
                "sind_trailing_alignment": classify_reference_trailing_token_alignment(sind_raw, codeline),
                "tind_trailing_alignment": classify_reference_trailing_token_alignment(tind_raw, codeline),
                "pctl_trailing_alignment": classify_reference_trailing_token_alignment(pctl_raw, codeline),
                "sctl_trailing_alignment": classify_reference_trailing_token_alignment(sctl_raw, codeline),
                "tctl_trailing_alignment": classify_reference_trailing_token_alignment(tctl_raw, codeline),
                "qind_trailing_alignment": classify_reference_trailing_token_alignment(qind_raw, codeline),
                "qctl_trailing_alignment": classify_reference_trailing_token_alignment(qctl_raw, codeline),
                "pind_resolved_count": sibling_counts.get("PIND", {}).get("resolved", 0),
                "pind_unresolved_count": sibling_counts.get("PIND", {}).get("unresolved", 0),
                "pind_resolved_mnemonics": join_unique(sibling_mnemonics.get("PIND", [])),
                "pind_resolved_long_names": join_unique(sibling_long_names.get("PIND", [])),
                "sind_resolved_count": sibling_counts.get("SIND", {}).get("resolved", 0),
                "sind_unresolved_count": sibling_counts.get("SIND", {}).get("unresolved", 0),
                "sind_resolved_mnemonics": join_unique(sibling_mnemonics.get("SIND", [])),
                "sind_resolved_long_names": join_unique(sibling_long_names.get("SIND", [])),
                "tind_resolved_count": sibling_counts.get("TIND", {}).get("resolved", 0),
                "tind_unresolved_count": sibling_counts.get("TIND", {}).get("unresolved", 0),
                "pctl_resolved_count": sibling_counts.get("PCTL", {}).get("resolved", 0),
                "pctl_unresolved_count": sibling_counts.get("PCTL", {}).get("unresolved", 0),
                "sctl_resolved_count": sibling_counts.get("SCTL", {}).get("resolved", 0),
                "sctl_unresolved_count": sibling_counts.get("SCTL", {}).get("unresolved", 0),
                "sctl_resolved_mnemonics": join_unique(sibling_mnemonics.get("SCTL", [])),
                "sctl_resolved_long_names": join_unique(sibling_long_names.get("SCTL", [])),
                "tctl_resolved_count": sibling_counts.get("TCTL", {}).get("resolved", 0),
                "tctl_unresolved_count": sibling_counts.get("TCTL", {}).get("unresolved", 0),
                "qind_resolved_count": sibling_counts.get("QIND", {}).get("resolved", 0),
                "qind_unresolved_count": sibling_counts.get("QIND", {}).get("unresolved", 0),
                "qind_default_zero_count": sibling_counts.get("QIND", {}).get("default_zero", 0),
                "qctl_resolved_count": sibling_counts.get("QCTL", {}).get("resolved", 0),
                "qctl_unresolved_count": sibling_counts.get("QCTL", {}).get("unresolved", 0),
                "qctl_default_zero_count": sibling_counts.get("QCTL", {}).get("default_zero", 0),
                "trackblocking_unresolved_count": sibling_counts.get("TrackBlockingReferences", {}).get("unresolved", 0),
                "trackblocking_default_zero_count": sibling_counts.get("TrackBlockingReferences", {}).get("default_zero", 0),
            }
        )

        pattern_key = (
            subdivision_name,
            normalize_text(row.get("TrkName")),
            control_cluster_raw,
            tind_raw,
            sind_raw,
            qctl_raw,
            qind_raw,
        )
        entry = grouped_pattern_rows.setdefault(
            pattern_key,
            {
                "subdivision_name": subdivision_name,
                "track_name_group": normalize_text(row.get("TrkName")),
                "control_cluster_raw": control_cluster_raw,
                "tind_raw": tind_raw,
                "sind_raw": sind_raw,
                "qctl_raw": qctl_raw,
                "qind_raw": qind_raw,
                "control_cluster_all_equal": str(control_cluster_all_equal),
                "sind_equals_tind": str(sind_raw == tind_raw),
                "qctl_equals_control_cluster": str(qctl_equals_control_cluster),
                "qind_equals_tind": str(qind_equals_tind),
                "track_count": 0,
                "_track_examples": [],
                "_control_points": set(),
                "_pind_mnemonics": set(),
                "_sind_mnemonics": set(),
                "_sctl_mnemonics": set(),
            },
        )
        entry["track_count"] += 1
        if track_uid and len(entry["_track_examples"]) < 10:
            entry["_track_examples"].append(f"{track_uid}:{normalize_text(row.get('Name'))}")
        if control_point_uid:
            entry["_control_points"].add(control_point_uid)
        for mnemonic in sibling_mnemonics.get("PIND", []):
            entry["_pind_mnemonics"].add(mnemonic)
        for mnemonic in sibling_mnemonics.get("SIND", []):
            entry["_sind_mnemonics"].add(mnemonic)
        for mnemonic in sibling_mnemonics.get("SCTL", []):
            entry["_sctl_mnemonics"].add(mnemonic)

    grouped_rows: list[dict] = []
    for entry in grouped_pattern_rows.values():
        grouped_rows.append(
            {
                "subdivision_name": entry["subdivision_name"],
                "track_name_group": entry["track_name_group"],
                "control_cluster_raw": entry["control_cluster_raw"],
                "tind_raw": entry["tind_raw"],
                "sind_raw": entry["sind_raw"],
                "qctl_raw": entry["qctl_raw"],
                "qind_raw": entry["qind_raw"],
                "control_cluster_all_equal": entry["control_cluster_all_equal"],
                "sind_equals_tind": entry["sind_equals_tind"],
                "qctl_equals_control_cluster": entry["qctl_equals_control_cluster"],
                "qind_equals_tind": entry["qind_equals_tind"],
                "track_count": entry["track_count"],
                "distinct_control_point_count": len(entry["_control_points"]),
                "track_examples": "|".join(entry["_track_examples"]),
                "resolved_pind_mnemonics": "|".join(sorted(entry["_pind_mnemonics"])),
                "resolved_sind_mnemonics": "|".join(sorted(entry["_sind_mnemonics"])),
                "resolved_sctl_mnemonics": "|".join(sorted(entry["_sctl_mnemonics"])),
            }
        )

    per_track_rows.sort(key=lambda item: (item["subdivision_name"], item["control_point_uid"], item["track_uid"]))
    grouped_rows.sort(
        key=lambda item: (
            -int(item["track_count"]),
            item["subdivision_name"],
            item["track_name_group"],
            item["control_cluster_raw"],
        )
    )
    return per_track_rows, grouped_rows


def build_track_control_cluster_summary(track_family_foundation_summary: list[dict]) -> tuple[list[dict], list[dict]]:
    per_track_rows: list[dict] = []
    grouped_pattern_rows: dict[tuple[str, str, str, str, str, str, str, str, str], dict[str, object]] = {}

    def classify_relation(control_cluster_raw: str, raw_value: str) -> str:
        normalized_control_cluster = normalize_text(control_cluster_raw)
        normalized_raw_value = normalize_text(raw_value)
        if not normalized_raw_value:
            return "blank"
        parsed = parse_reference_part(normalized_raw_value)
        if parsed.is_default_zero_reference:
            return "default_zero"
        if normalized_control_cluster and normalized_raw_value == normalized_control_cluster:
            return "same_as_control_cluster"
        return "different_from_control_cluster"

    for row in track_family_foundation_summary:
        control_cluster_raw = normalize_text(row["control_cluster_raw"])
        tind_relation = classify_relation(control_cluster_raw, row["tind_raw"])
        sind_relation = classify_relation(control_cluster_raw, row["sind_raw"])
        qctl_relation = classify_relation(control_cluster_raw, row["qctl_raw"])
        qind_relation = classify_relation(control_cluster_raw, row["qind_raw"])
        control_cluster_split = normalize_text(row["control_cluster_all_equal"]) != "True"

        per_track_rows.append(
            {
                "track_uid": normalize_text(row["track_uid"]),
                "control_point_uid": normalize_text(row["control_point_uid"]),
                "subdivision_name": normalize_text(row["subdivision_name"]),
                "codeline": normalize_text(row["codeline"]),
                "track_name": normalize_text(row["track_name"]),
                "track_name_group": normalize_text(row["track_name_group"]),
                "route_name": normalize_text(row["route_name"]),
                "control_cluster_raw": control_cluster_raw,
                "control_cluster_split": str(control_cluster_split),
                "control_cluster_distinct_count": normalize_text(row["control_cluster_distinct_count"]),
                "pctl_raw": normalize_text(row["pctl_raw"]),
                "sctl_raw": normalize_text(row["sctl_raw"]),
                "tctl_raw": normalize_text(row["tctl_raw"]),
                "tind_raw": normalize_text(row["tind_raw"]),
                "sind_raw": normalize_text(row["sind_raw"]),
                "qctl_raw": normalize_text(row["qctl_raw"]),
                "qind_raw": normalize_text(row["qind_raw"]),
                "tind_relation_to_control_cluster": tind_relation,
                "sind_relation_to_control_cluster": sind_relation,
                "qctl_relation_to_control_cluster": qctl_relation,
                "qind_relation_to_control_cluster": qind_relation,
                "tind_cp_alignment": normalize_text(row["tind_cp_alignment"]),
                "sind_cp_alignment": normalize_text(row["sind_cp_alignment"]),
                "qctl_cp_alignment": normalize_text(row["qctl_cp_alignment"]),
                "qind_cp_alignment": normalize_text(row["qind_cp_alignment"]),
                "tind_trailing_alignment": normalize_text(row["tind_trailing_alignment"]),
                "sind_trailing_alignment": normalize_text(row["sind_trailing_alignment"]),
                "qctl_trailing_alignment": normalize_text(row["qctl_trailing_alignment"]),
                "qind_trailing_alignment": normalize_text(row["qind_trailing_alignment"]),
                "pind_resolved_mnemonics": normalize_text(row["pind_resolved_mnemonics"]),
                "sind_resolved_mnemonics": normalize_text(row["sind_resolved_mnemonics"]),
                "sctl_resolved_mnemonics": normalize_text(row["sctl_resolved_mnemonics"]),
                "pind_resolved_count": normalize_text(row["pind_resolved_count"]),
                "sind_resolved_count": normalize_text(row["sind_resolved_count"]),
                "sctl_resolved_count": normalize_text(row["sctl_resolved_count"]),
            }
        )

        pattern_key = (
            normalize_text(row["subdivision_name"]),
            normalize_text(row["track_name_group"]),
            control_cluster_raw,
            tind_relation,
            normalize_text(row["tind_raw"]),
            sind_relation,
            normalize_text(row["sind_raw"]),
            qctl_relation,
            qind_relation,
        )
        entry = grouped_pattern_rows.setdefault(
            pattern_key,
            {
                "subdivision_name": normalize_text(row["subdivision_name"]),
                "track_name_group": normalize_text(row["track_name_group"]),
                "control_cluster_raw": control_cluster_raw,
                "tind_relation_to_control_cluster": tind_relation,
                "tind_raw": normalize_text(row["tind_raw"]),
                "sind_relation_to_control_cluster": sind_relation,
                "sind_raw": normalize_text(row["sind_raw"]),
                "qctl_relation_to_control_cluster": qctl_relation,
                "qctl_raw": normalize_text(row["qctl_raw"]),
                "qind_relation_to_control_cluster": qind_relation,
                "qind_raw": normalize_text(row["qind_raw"]),
                "track_count": 0,
                "control_cluster_split_count": 0,
                "_control_points": set(),
                "_track_examples": [],
                "_pind_mnemonics": set(),
                "_sind_mnemonics": set(),
                "_sctl_mnemonics": set(),
            },
        )
        entry["track_count"] += 1
        if control_cluster_split:
            entry["control_cluster_split_count"] += 1
        control_point_uid = normalize_text(row["control_point_uid"])
        if control_point_uid:
            entry["_control_points"].add(control_point_uid)
        track_uid = normalize_text(row["track_uid"])
        track_name = normalize_text(row["track_name"])
        if track_uid and len(entry["_track_examples"]) < 10:
            entry["_track_examples"].append(f"{track_uid}:{track_name}")
        for field_name, key in [
            ("pind_resolved_mnemonics", "_pind_mnemonics"),
            ("sind_resolved_mnemonics", "_sind_mnemonics"),
            ("sctl_resolved_mnemonics", "_sctl_mnemonics"),
        ]:
            for mnemonic in split_non_empty_tokens(row[field_name], "|"):
                entry[key].add(mnemonic)

    grouped_rows: list[dict] = []
    for entry in grouped_pattern_rows.values():
        grouped_rows.append(
            {
                "subdivision_name": entry["subdivision_name"],
                "track_name_group": entry["track_name_group"],
                "control_cluster_raw": entry["control_cluster_raw"],
                "tind_relation_to_control_cluster": entry["tind_relation_to_control_cluster"],
                "tind_raw": entry["tind_raw"],
                "sind_relation_to_control_cluster": entry["sind_relation_to_control_cluster"],
                "sind_raw": entry["sind_raw"],
                "qctl_relation_to_control_cluster": entry["qctl_relation_to_control_cluster"],
                "qctl_raw": entry["qctl_raw"],
                "qind_relation_to_control_cluster": entry["qind_relation_to_control_cluster"],
                "qind_raw": entry["qind_raw"],
                "track_count": entry["track_count"],
                "control_cluster_split_count": entry["control_cluster_split_count"],
                "distinct_control_point_count": len(entry["_control_points"]),
                "track_examples": "|".join(entry["_track_examples"]),
                "resolved_pind_mnemonics": "|".join(sorted(entry["_pind_mnemonics"])),
                "resolved_sind_mnemonics": "|".join(sorted(entry["_sind_mnemonics"])),
                "resolved_sctl_mnemonics": "|".join(sorted(entry["_sctl_mnemonics"])),
            }
        )

    per_track_rows.sort(key=lambda item: (item["subdivision_name"], item["control_point_uid"], item["track_uid"]))
    grouped_rows.sort(
        key=lambda item: (
            -int(item["track_count"]),
            item["subdivision_name"],
            item["track_name_group"],
            item["control_cluster_raw"],
        )
    )
    return per_track_rows, grouped_rows


def build_signal_family_foundation_summary(component_bit_reference_map: list[dict]) -> tuple[list[dict], list[dict]]:
    signal_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.signal_detail_full.csv")
    route_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.route_context.csv")
    component_reference_rows = [
        row
        for row in component_bit_reference_map
        if normalize_text(row["component_family"]) == "signal"
    ]

    references_by_uid: dict[str, list[dict[str, str]]] = {}
    for row in component_reference_rows:
        references_by_uid.setdefault(normalize_text(row["component_uid"]), []).append(row)

    entry_route_counts: dict[str, int] = {}
    exit_route_counts: dict[str, int] = {}
    for row in route_rows:
        entry_signal_uid = normalize_text(row.get("EntrySignal"))
        exit_signal_uid = normalize_text(row.get("ExitSignal"))
        if entry_signal_uid:
            entry_route_counts[entry_signal_uid] = entry_route_counts.get(entry_signal_uid, 0) + 1
        if exit_signal_uid:
            exit_route_counts[exit_signal_uid] = exit_route_counts.get(exit_signal_uid, 0) + 1

    per_signal_rows: list[dict] = []
    grouped_pattern_rows: dict[tuple[str, str, str, str, str, str, str, str, str, str], dict[str, object]] = {}

    for row in signal_rows:
        signal_uid = normalize_text(row["UID"])
        control_point_uid = normalize_text(row["ControlPoint"])
        subdivision_name = infer_subdivision_name_from_uid(control_point_uid)
        codeline = normalize_text(row.get("Codeline"))
        signal_refs = references_by_uid.get(signal_uid, [])

        sibling_counts: dict[str, dict[str, int]] = {}
        sibling_mnemonics: dict[str, list[str]] = {}
        sibling_long_names: dict[str, list[str]] = {}
        for ref in signal_refs:
            column = normalize_text(ref["reference_column"])
            sibling_counts.setdefault(column, {})
            status = normalize_text(ref["match_status"])
            sibling_counts[column][status] = sibling_counts[column].get(status, 0) + 1
            if status == "resolved":
                mnemonic = normalize_text(ref["resolved_mnemonic"])
                long_name = normalize_text(ref["resolved_long_name"])
                if mnemonic:
                    sibling_mnemonics.setdefault(column, [])
                    if mnemonic not in sibling_mnemonics[column]:
                        sibling_mnemonics[column].append(mnemonic)
                if long_name:
                    sibling_long_names.setdefault(column, [])
                    if long_name not in sibling_long_names[column]:
                        sibling_long_names[column].append(long_name)

        pind_raw = normalize_text(row.get("PIND"))
        pctl_raw = normalize_text(row.get("PCTL"))
        sind_raw = normalize_text(row.get("SIND"))
        sctl_raw = normalize_text(row.get("SCTL"))
        tind_raw = normalize_text(row.get("TIND"))
        tctl_raw = normalize_text(row.get("TCTL"))
        fleet_raw = normalize_text(row.get("FleetControlBit"))
        in_time_raw = normalize_text(row.get("InTimeBit"))
        nearside_ctl_raw = normalize_text(row.get("NearSideSignalBitControl"))
        nearside_ind_raw = normalize_text(row.get("NearSideSignaBitlIndication"))
        callon_raw = normalize_text(row.get("CallOnBit"))
        blockind_raw = normalize_text(row.get("BlockIndBit"))

        pctl_parts = split_non_empty_tokens(pctl_raw, "|")
        sctl_parts = split_non_empty_tokens(sctl_raw, "|")
        tctl_parts = split_non_empty_tokens(tctl_raw, "|")

        pctl_primary_part = pctl_parts[0] if len(pctl_parts) >= 1 else ""
        pctl_secondary_part = pctl_parts[1] if len(pctl_parts) >= 2 else ""
        sctl_primary_part = sctl_parts[0] if len(sctl_parts) >= 1 else ""
        sctl_secondary_part = sctl_parts[1] if len(sctl_parts) >= 2 else ""
        tctl_primary_part = tctl_parts[0] if len(tctl_parts) >= 1 else ""
        tctl_secondary_part = tctl_parts[1] if len(tctl_parts) >= 2 else ""

        call_on_flag = normalize_text(row.get("CallOn"))
        near_side_enabled = normalize_text(row.get("NearSideSignalEnabled"))
        signal_name = normalize_text(row.get("Name"))
        tool_tip_name = normalize_text(row.get("ToolTipName"))

        entry_route_count = entry_route_counts.get(signal_uid, 0)
        exit_route_count = exit_route_counts.get(signal_uid, 0)

        per_signal_rows.append(
            {
                "signal_uid": signal_uid,
                "control_point_uid": control_point_uid,
                "subdivision_name": subdivision_name,
                "codeline": codeline,
                "signal_name": signal_name,
                "tool_tip_name": tool_tip_name,
                "signal_type": normalize_text(row.get("Type")),
                "authority_type": normalize_text(row.get("AuthorityType")),
                "territory_assignment": normalize_text(row.get("TerritoryAssignment")),
                "subdivision_number": normalize_text(row.get("Subdivision")),
                "in_time_type": normalize_text(row.get("InTimeType")),
                "call_on_flag": call_on_flag,
                "near_side_signal_enabled": near_side_enabled,
                "holding_signal": normalize_text(row.get("HoldingSignal")),
                "fleeting": normalize_text(row.get("Fleeting")),
                "stacking": normalize_text(row.get("Stacking")),
                "blocking": normalize_text(row.get("Blocking")),
                "allow_automatic_mode": normalize_text(row.get("AllowAutomaticMode")),
                "call_on_controlled": normalize_text(row.get("CallOnControlled")),
                "signal_controlled": normalize_text(row.get("SignalControlled")),
                "pind_raw": pind_raw,
                "pctl_raw": pctl_raw,
                "sind_raw": sind_raw,
                "sctl_raw": sctl_raw,
                "tind_raw": tind_raw,
                "tctl_raw": tctl_raw,
                "fleet_control_bit_raw": fleet_raw,
                "in_time_bit_raw": in_time_raw,
                "near_side_signal_bit_control_raw": nearside_ctl_raw,
                "near_side_signal_bit_indication_raw": nearside_ind_raw,
                "call_on_bit_raw": callon_raw,
                "block_ind_bit_raw": blockind_raw,
                "pctl_primary_part": pctl_primary_part,
                "pctl_secondary_part": pctl_secondary_part,
                "sctl_primary_part": sctl_primary_part,
                "sctl_secondary_part": sctl_secondary_part,
                "tctl_primary_part": tctl_primary_part,
                "tctl_secondary_part": tctl_secondary_part,
                "tctl_equals_sctl_raw": str(tctl_raw == sctl_raw),
                "tind_equals_sind_raw": str(tind_raw == sind_raw),
                "fleet_equals_tind_raw": str(fleet_raw == tind_raw),
                "pctl_secondary_equals_sctl_primary": str(bool(pctl_secondary_part) and pctl_secondary_part == sctl_primary_part),
                "pctl_secondary_equals_tctl_primary": str(bool(pctl_secondary_part) and pctl_secondary_part == tctl_primary_part),
                "sctl_primary_equals_tctl_primary": str(bool(sctl_primary_part) and sctl_primary_part == tctl_primary_part),
                "sctl_secondary_equals_tctl_primary": str(bool(sctl_secondary_part) and sctl_secondary_part == tctl_primary_part),
                "sctl_secondary_equals_tctl_secondary": str(bool(sctl_secondary_part) and sctl_secondary_part == tctl_secondary_part),
                "near_side_ctl_equals_ind": str(nearside_ctl_raw == nearside_ind_raw),
                "call_on_non_default_matches_flag": str(
                    (parse_reference_part(callon_raw).is_default_zero_reference and call_on_flag == "False")
                    or (not parse_reference_part(callon_raw).is_default_zero_reference and call_on_flag == "True")
                ),
                "entry_route_count": entry_route_count,
                "exit_route_count": exit_route_count,
                "route_total_count": entry_route_count + exit_route_count,
                "pind_cp_alignment": classify_reference_cp_alignment(pind_raw, control_point_uid),
                "sind_cp_alignment": classify_reference_cp_alignment(sind_raw, control_point_uid),
                "tind_cp_alignment": classify_reference_cp_alignment(tind_raw, control_point_uid),
                "fleet_cp_alignment": classify_reference_cp_alignment(fleet_raw, control_point_uid),
                "in_time_cp_alignment": classify_reference_cp_alignment(in_time_raw, control_point_uid),
                "near_side_ctl_cp_alignment": classify_reference_cp_alignment(nearside_ctl_raw, control_point_uid),
                "near_side_ind_cp_alignment": classify_reference_cp_alignment(nearside_ind_raw, control_point_uid),
                "call_on_cp_alignment": classify_reference_cp_alignment(callon_raw, control_point_uid),
                "pctl_primary_cp_alignment": classify_reference_cp_alignment(pctl_primary_part, control_point_uid),
                "pctl_secondary_cp_alignment": classify_reference_cp_alignment(pctl_secondary_part, control_point_uid),
                "sctl_primary_cp_alignment": classify_reference_cp_alignment(sctl_primary_part, control_point_uid),
                "sctl_secondary_cp_alignment": classify_reference_cp_alignment(sctl_secondary_part, control_point_uid),
                "tctl_primary_cp_alignment": classify_reference_cp_alignment(tctl_primary_part, control_point_uid),
                "tctl_secondary_cp_alignment": classify_reference_cp_alignment(tctl_secondary_part, control_point_uid),
                "pind_resolved_count": sibling_counts.get("PIND", {}).get("resolved", 0),
                "pind_resolved_mnemonics": join_unique(sibling_mnemonics.get("PIND", [])),
                "pind_resolved_long_names": join_unique(sibling_long_names.get("PIND", [])),
                "pctl_resolved_count": sibling_counts.get("PCTL", {}).get("resolved", 0),
                "pctl_unresolved_count": sibling_counts.get("PCTL", {}).get("unresolved", 0),
                "pctl_resolved_mnemonics": join_unique(sibling_mnemonics.get("PCTL", [])),
                "pctl_resolved_long_names": join_unique(sibling_long_names.get("PCTL", [])),
                "sind_resolved_count": sibling_counts.get("SIND", {}).get("resolved", 0),
                "sind_unresolved_count": sibling_counts.get("SIND", {}).get("unresolved", 0),
                "sind_resolved_mnemonics": join_unique(sibling_mnemonics.get("SIND", [])),
                "sind_resolved_long_names": join_unique(sibling_long_names.get("SIND", [])),
                "sctl_resolved_count": sibling_counts.get("SCTL", {}).get("resolved", 0),
                "sctl_unresolved_count": sibling_counts.get("SCTL", {}).get("unresolved", 0),
                "sctl_resolved_mnemonics": join_unique(sibling_mnemonics.get("SCTL", [])),
                "sctl_resolved_long_names": join_unique(sibling_long_names.get("SCTL", [])),
                "tctl_resolved_count": sibling_counts.get("TCTL", {}).get("resolved", 0),
                "tctl_unresolved_count": sibling_counts.get("TCTL", {}).get("unresolved", 0),
                "tctl_resolved_mnemonics": join_unique(sibling_mnemonics.get("TCTL", [])),
                "tctl_resolved_long_names": join_unique(sibling_long_names.get("TCTL", [])),
                "tind_resolved_count": sibling_counts.get("TIND", {}).get("resolved", 0),
                "tind_unresolved_count": sibling_counts.get("TIND", {}).get("unresolved", 0),
                "fleet_resolved_count": sibling_counts.get("FleetControlBit", {}).get("resolved", 0),
                "fleet_unresolved_count": sibling_counts.get("FleetControlBit", {}).get("unresolved", 0),
                "in_time_resolved_count": sibling_counts.get("InTimeBit", {}).get("resolved", 0),
                "in_time_unresolved_count": sibling_counts.get("InTimeBit", {}).get("unresolved", 0),
                "in_time_resolved_mnemonics": join_unique(sibling_mnemonics.get("InTimeBit", [])),
                "in_time_resolved_long_names": join_unique(sibling_long_names.get("InTimeBit", [])),
                "near_side_ctl_resolved_count": sibling_counts.get("NearSideSignalBitControl", {}).get("resolved", 0),
                "near_side_ctl_unresolved_count": sibling_counts.get("NearSideSignalBitControl", {}).get("unresolved", 0),
                "near_side_ctl_default_zero_count": sibling_counts.get("NearSideSignalBitControl", {}).get("default_zero", 0),
                "near_side_ctl_resolved_mnemonics": join_unique(sibling_mnemonics.get("NearSideSignalBitControl", [])),
                "near_side_ind_resolved_count": sibling_counts.get("NearSideSignaBitlIndication", {}).get("resolved", 0),
                "near_side_ind_unresolved_count": sibling_counts.get("NearSideSignaBitlIndication", {}).get("unresolved", 0),
                "near_side_ind_default_zero_count": sibling_counts.get("NearSideSignaBitlIndication", {}).get("default_zero", 0),
                "near_side_ind_resolved_mnemonics": join_unique(sibling_mnemonics.get("NearSideSignaBitlIndication", [])),
                "call_on_resolved_count": sibling_counts.get("CallOnBit", {}).get("resolved", 0),
                "call_on_unresolved_count": sibling_counts.get("CallOnBit", {}).get("unresolved", 0),
                "call_on_default_zero_count": sibling_counts.get("CallOnBit", {}).get("default_zero", 0),
                "call_on_resolved_mnemonics": join_unique(sibling_mnemonics.get("CallOnBit", [])),
                "call_on_resolved_long_names": join_unique(sibling_long_names.get("CallOnBit", [])),
                "block_ind_resolved_count": sibling_counts.get("BlockIndBit", {}).get("resolved", 0),
                "block_ind_unresolved_count": sibling_counts.get("BlockIndBit", {}).get("unresolved", 0),
                "block_ind_default_zero_count": sibling_counts.get("BlockIndBit", {}).get("default_zero", 0),
            }
        )

        pattern_key = (
            subdivision_name,
            normalize_text(row.get("Type")),
            normalize_text(row.get("InTimeType")),
            call_on_flag,
            near_side_enabled,
            tctl_raw,
            tind_raw,
            fleet_raw,
            nearside_ctl_raw,
            callon_raw,
        )
        entry = grouped_pattern_rows.setdefault(
            pattern_key,
            {
                "subdivision_name": subdivision_name,
                "signal_type": normalize_text(row.get("Type")),
                "in_time_type": normalize_text(row.get("InTimeType")),
                "call_on_flag": call_on_flag,
                "near_side_signal_enabled": near_side_enabled,
                "tctl_raw": tctl_raw,
                "tind_raw": tind_raw,
                "fleet_control_bit_raw": fleet_raw,
                "near_side_signal_bit_control_raw": nearside_ctl_raw,
                "call_on_bit_raw": callon_raw,
                "signal_count": 0,
                "entry_route_total": 0,
                "exit_route_total": 0,
                "_control_points": set(),
                "_signal_examples": [],
                "_resolved_pind": set(),
                "_resolved_pctl": set(),
                "_resolved_sind": set(),
                "_resolved_tctl": set(),
                "_resolved_in_time": set(),
                "_resolved_call_on": set(),
                "_resolved_nearside_ctl": set(),
            },
        )
        entry["signal_count"] += 1
        entry["entry_route_total"] += entry_route_count
        entry["exit_route_total"] += exit_route_count
        if control_point_uid:
            entry["_control_points"].add(control_point_uid)
        example_name = tool_tip_name or signal_name
        if signal_uid and len(entry["_signal_examples"]) < 10:
            entry["_signal_examples"].append(f"{signal_uid}:{example_name}")
        for mnemonic in sibling_mnemonics.get("PIND", []):
            entry["_resolved_pind"].add(mnemonic)
        for mnemonic in sibling_mnemonics.get("PCTL", []):
            entry["_resolved_pctl"].add(mnemonic)
        for mnemonic in sibling_mnemonics.get("SIND", []):
            entry["_resolved_sind"].add(mnemonic)
        for mnemonic in sibling_mnemonics.get("TCTL", []):
            entry["_resolved_tctl"].add(mnemonic)
        for mnemonic in sibling_mnemonics.get("InTimeBit", []):
            entry["_resolved_in_time"].add(mnemonic)
        for mnemonic in sibling_mnemonics.get("CallOnBit", []):
            entry["_resolved_call_on"].add(mnemonic)
        for mnemonic in sibling_mnemonics.get("NearSideSignalBitControl", []):
            entry["_resolved_nearside_ctl"].add(mnemonic)

    grouped_rows: list[dict] = []
    for entry in grouped_pattern_rows.values():
        grouped_rows.append(
            {
                "subdivision_name": entry["subdivision_name"],
                "signal_type": entry["signal_type"],
                "in_time_type": entry["in_time_type"],
                "call_on_flag": entry["call_on_flag"],
                "near_side_signal_enabled": entry["near_side_signal_enabled"],
                "tctl_raw": entry["tctl_raw"],
                "tind_raw": entry["tind_raw"],
                "fleet_control_bit_raw": entry["fleet_control_bit_raw"],
                "near_side_signal_bit_control_raw": entry["near_side_signal_bit_control_raw"],
                "call_on_bit_raw": entry["call_on_bit_raw"],
                "signal_count": entry["signal_count"],
                "distinct_control_point_count": len(entry["_control_points"]),
                "entry_route_total": entry["entry_route_total"],
                "exit_route_total": entry["exit_route_total"],
                "signal_examples": "|".join(entry["_signal_examples"]),
                "resolved_pind_mnemonics": "|".join(sorted(entry["_resolved_pind"])),
                "resolved_pctl_mnemonics": "|".join(sorted(entry["_resolved_pctl"])),
                "resolved_sind_mnemonics": "|".join(sorted(entry["_resolved_sind"])),
                "resolved_tctl_mnemonics": "|".join(sorted(entry["_resolved_tctl"])),
                "resolved_in_time_mnemonics": "|".join(sorted(entry["_resolved_in_time"])),
                "resolved_call_on_mnemonics": "|".join(sorted(entry["_resolved_call_on"])),
                "resolved_near_side_ctl_mnemonics": "|".join(sorted(entry["_resolved_nearside_ctl"])),
            }
        )

    per_signal_rows.sort(key=lambda item: (item["subdivision_name"], item["control_point_uid"], item["signal_uid"]))
    grouped_rows.sort(
        key=lambda item: (
            -int(item["signal_count"]),
            item["subdivision_name"],
            item["signal_type"],
            item["in_time_type"],
            item["tctl_raw"],
        )
    )
    return per_signal_rows, grouped_rows


def build_signal_opposing_foundation_summary(component_bit_reference_map: list[dict]) -> tuple[list[dict], list[dict]]:
    signal_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.signal_detail_full.csv")
    route_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.route_context.csv")

    signal_row_by_uid = {
        normalize_text(row["UID"]): row
        for row in signal_rows
    }

    component_reference_rows = [
        row
        for row in component_bit_reference_map
        if normalize_text(row["component_family"]) == "signal"
    ]
    references_by_uid: dict[str, list[dict[str, str]]] = {}
    for row in component_reference_rows:
        references_by_uid.setdefault(normalize_text(row["component_uid"]), []).append(row)

    route_rows_by_signal_uid: dict[str, list[dict[str, str]]] = {}
    for row in route_rows:
        entry_signal_uid = normalize_text(row.get("EntrySignal"))
        exit_signal_uid = normalize_text(row.get("ExitSignal"))
        if entry_signal_uid:
            route_rows_by_signal_uid.setdefault(entry_signal_uid, []).append(row)
        if exit_signal_uid:
            route_rows_by_signal_uid.setdefault(exit_signal_uid, []).append(row)

    per_signal_rows: list[dict] = []
    grouped_rows_by_cp: dict[str, dict[str, object]] = {}

    for signal_uid, row in signal_row_by_uid.items():
        in_time_type = normalize_text(row.get("InTimeType"))
        if in_time_type != "OPPOSING":
            continue

        control_point_uid = normalize_text(row.get("ControlPoint"))
        subdivision_name = infer_subdivision_name_from_uid(control_point_uid)
        signal_name = normalize_text(row.get("Name"))
        tool_tip_name = normalize_text(row.get("ToolTipName"))
        signal_refs = references_by_uid.get(signal_uid, [])
        related_route_rows = route_rows_by_signal_uid.get(signal_uid, [])

        resolved_by_column: dict[str, list[dict[str, str]]] = {}
        for ref in signal_refs:
            if normalize_text(ref["match_status"]) == "resolved":
                resolved_by_column.setdefault(normalize_text(ref["reference_column"]), []).append(ref)

        partner_signal_uids: list[str] = []
        partner_signal_names: list[str] = []
        partner_tool_tips: list[str] = []
        route_guids: list[str] = []
        switch_lists: list[str] = []
        switch_uids: list[str] = []
        control_point_names: list[str] = []
        for route in related_route_rows:
            route_guids.append(normalize_text(route.get("RouteGUID")))
            switch_list_value = normalize_text(route.get("SwitchList"))
            if switch_list_value:
                switch_lists.append(switch_list_value)
                switch_tokens = split_non_empty_tokens(switch_list_value, "|")
                for token in switch_tokens:
                    switch_uid = token.split(";")[0].strip() if ";" in token else token
                    if switch_uid:
                        switch_uids.append(switch_uid)
            control_point_names.append(normalize_text(route.get("ControlPointName")))
            entry_signal_uid = normalize_text(route.get("EntrySignal"))
            exit_signal_uid = normalize_text(route.get("ExitSignal"))
            if entry_signal_uid == signal_uid:
                partner_uid = exit_signal_uid
                partner_name = normalize_text(route.get("ExitSignalName"))
                partner_tool_tip = normalize_text(route.get("ExitSignalToolTipName"))
            else:
                partner_uid = entry_signal_uid
                partner_name = normalize_text(route.get("EntrySignalName"))
                partner_tool_tip = normalize_text(route.get("EntrySignalToolTipName"))
            if partner_uid:
                partner_signal_uids.append(partner_uid)
            if partner_name:
                partner_signal_names.append(partner_name)
            if partner_tool_tip:
                partner_tool_tips.append(partner_tool_tip)

        pind_mnemonics = join_unique(ref["resolved_mnemonic"] for ref in resolved_by_column.get("PIND", []))
        pind_long_names = join_unique(ref["resolved_long_name"] for ref in resolved_by_column.get("PIND", []))
        pctl_mnemonics = join_unique(ref["resolved_mnemonic"] for ref in resolved_by_column.get("PCTL", []))
        pctl_long_names = join_unique(ref["resolved_long_name"] for ref in resolved_by_column.get("PCTL", []))
        tctl_mnemonics = join_unique(ref["resolved_mnemonic"] for ref in resolved_by_column.get("TCTL", []))
        tctl_long_names = join_unique(ref["resolved_long_name"] for ref in resolved_by_column.get("TCTL", []))
        in_time_mnemonics = join_unique(ref["resolved_mnemonic"] for ref in resolved_by_column.get("InTimeBit", []))
        in_time_long_names = join_unique(ref["resolved_long_name"] for ref in resolved_by_column.get("InTimeBit", []))

        per_signal_rows.append(
            {
                "signal_uid": signal_uid,
                "control_point_uid": control_point_uid,
                "control_point_name": join_unique(control_point_names),
                "subdivision_name": subdivision_name,
                "codeline": normalize_text(row.get("Codeline")),
                "signal_name": signal_name,
                "tool_tip_name": tool_tip_name,
                "signal_type": normalize_text(row.get("Type")),
                "pind_raw": normalize_text(row.get("PIND")),
                "pctl_raw": normalize_text(row.get("PCTL")),
                "tctl_raw": normalize_text(row.get("TCTL")),
                "in_time_bit_raw": normalize_text(row.get("InTimeBit")),
                "fleet_control_bit_raw": normalize_text(row.get("FleetControlBit")),
                "call_on_bit_raw": normalize_text(row.get("CallOnBit")),
                "pind_resolved_mnemonics": pind_mnemonics,
                "pind_resolved_long_names": pind_long_names,
                "pctl_resolved_mnemonics": pctl_mnemonics,
                "pctl_resolved_long_names": pctl_long_names,
                "tctl_resolved_mnemonics": tctl_mnemonics,
                "tctl_resolved_long_names": tctl_long_names,
                "in_time_resolved_mnemonics": in_time_mnemonics,
                "in_time_resolved_long_names": in_time_long_names,
                "entry_route_count": sum(1 for route in related_route_rows if normalize_text(route.get("EntrySignal")) == signal_uid),
                "exit_route_count": sum(1 for route in related_route_rows if normalize_text(route.get("ExitSignal")) == signal_uid),
                "route_total_count": len(related_route_rows),
                "route_guids": join_unique(route_guids),
                "switch_lists": join_unique(switch_lists),
                "switch_uids": join_unique(switch_uids),
                "partner_signal_uids": join_unique(partner_signal_uids),
                "partner_signal_names": join_unique(partner_signal_names),
                "partner_signal_tool_tips": join_unique(partner_tool_tips),
                "tctl_is_sigstop": str("SIGSTOP" in tctl_mnemonics),
                "in_time_equals_pind_mnemonic": str(bool(in_time_mnemonics) and in_time_mnemonics == pind_mnemonics),
                "call_on_flag": normalize_text(row.get("CallOn")),
                "near_side_signal_enabled": normalize_text(row.get("NearSideSignalEnabled")),
            }
        )

        entry = grouped_rows_by_cp.setdefault(
            control_point_uid,
            {
                "control_point_uid": control_point_uid,
                "control_point_name": join_unique(control_point_names),
                "subdivision_name": subdivision_name,
                "codeline": normalize_text(row.get("Codeline")),
                "signal_count": 0,
                "route_reference_count": 0,
                "_signal_examples": [],
                "_route_guids": set(),
                "_switch_lists": set(),
                "_switch_uids": set(),
                "_partner_signals": set(),
                "_pind": set(),
                "_pctl": set(),
                "_tctl": set(),
                "_in_time": set(),
            },
        )
        entry["signal_count"] += 1
        entry["route_reference_count"] += len(related_route_rows)
        if signal_uid and len(entry["_signal_examples"]) < 8:
            entry["_signal_examples"].append(f"{signal_uid}:{tool_tip_name or signal_name}")
        for value in route_guids:
            if value:
                entry["_route_guids"].add(value)
        for value in switch_lists:
            if value:
                entry["_switch_lists"].add(value)
        for value in switch_uids:
            if value:
                entry["_switch_uids"].add(value)
        for value in partner_tool_tips or partner_signal_names:
            if value:
                entry["_partner_signals"].add(value)
        for value in split_non_empty_tokens(pind_mnemonics, "|"):
            entry["_pind"].add(value)
        for value in split_non_empty_tokens(pctl_mnemonics, "|"):
            entry["_pctl"].add(value)
        for value in split_non_empty_tokens(tctl_mnemonics, "|"):
            entry["_tctl"].add(value)
        for value in split_non_empty_tokens(in_time_mnemonics, "|"):
            entry["_in_time"].add(value)

    grouped_rows: list[dict] = []
    for entry in grouped_rows_by_cp.values():
        grouped_rows.append(
            {
                "control_point_uid": entry["control_point_uid"],
                "control_point_name": entry["control_point_name"],
                "subdivision_name": entry["subdivision_name"],
                "codeline": entry["codeline"],
                "signal_count": entry["signal_count"],
                "route_reference_count": entry["route_reference_count"],
                "distinct_route_count": len(entry["_route_guids"]),
                "distinct_switch_list_count": len(entry["_switch_lists"]),
                "distinct_switch_uid_count": len(entry["_switch_uids"]),
                "signal_examples": "|".join(entry["_signal_examples"]),
                "route_guids": "|".join(sorted(entry["_route_guids"])),
                "switch_lists": "|".join(sorted(entry["_switch_lists"])),
                "switch_uids": "|".join(sorted(entry["_switch_uids"])),
                "partner_signals": "|".join(sorted(entry["_partner_signals"])),
                "pind_resolved_mnemonics": "|".join(sorted(entry["_pind"])),
                "pctl_resolved_mnemonics": "|".join(sorted(entry["_pctl"])),
                "tctl_resolved_mnemonics": "|".join(sorted(entry["_tctl"])),
                "in_time_resolved_mnemonics": "|".join(sorted(entry["_in_time"])),
            }
        )

    per_signal_rows.sort(key=lambda item: (item["control_point_uid"], item["signal_uid"]))
    grouped_rows.sort(key=lambda item: (item["subdivision_name"], item["control_point_uid"]))
    return per_signal_rows, grouped_rows


def build_signal_shared_anchor_summary(signal_family_foundation_summary: list[dict]) -> tuple[list[dict], list[dict]]:
    per_signal_rows: list[dict] = []
    grouped_rows_by_cp: dict[str, dict[str, object]] = {}

    for row in signal_family_foundation_summary:
        control_point_uid = normalize_text(row["control_point_uid"])
        subdivision_name = normalize_text(row["subdivision_name"])
        shared_anchor_raw = normalize_text(row["tind_raw"])
        signal_uid = normalize_text(row["signal_uid"])
        signal_name = normalize_text(row["signal_name"])
        tool_tip_name = normalize_text(row["tool_tip_name"])

        anchor_equals_fleet = shared_anchor_raw == normalize_text(row["fleet_control_bit_raw"])
        anchor_equals_pctl_secondary = shared_anchor_raw == normalize_text(row["pctl_secondary_part"])
        anchor_equals_sctl_primary = shared_anchor_raw == normalize_text(row["sctl_primary_part"])
        anchor_equals_sctl_secondary = shared_anchor_raw == normalize_text(row["sctl_secondary_part"])
        anchor_equals_tctl_secondary = shared_anchor_raw == normalize_text(row["tctl_secondary_part"])
        core_anchor_all_equal = all(
            [
                anchor_equals_fleet,
                anchor_equals_pctl_secondary,
                anchor_equals_sctl_primary,
                anchor_equals_tctl_secondary,
            ]
        )

        per_signal_rows.append(
            {
                "signal_uid": signal_uid,
                "control_point_uid": control_point_uid,
                "subdivision_name": subdivision_name,
                "codeline": normalize_text(row["codeline"]),
                "signal_name": signal_name,
                "tool_tip_name": tool_tip_name,
                "signal_type": normalize_text(row["signal_type"]),
                "in_time_type": normalize_text(row["in_time_type"]),
                "shared_anchor_raw": shared_anchor_raw,
                "fleet_control_bit_raw": normalize_text(row["fleet_control_bit_raw"]),
                "pctl_secondary_part": normalize_text(row["pctl_secondary_part"]),
                "sctl_primary_part": normalize_text(row["sctl_primary_part"]),
                "sctl_secondary_part": normalize_text(row["sctl_secondary_part"]),
                "tctl_secondary_part": normalize_text(row["tctl_secondary_part"]),
                "anchor_equals_fleet": str(anchor_equals_fleet),
                "anchor_equals_pctl_secondary": str(anchor_equals_pctl_secondary),
                "anchor_equals_sctl_primary": str(anchor_equals_sctl_primary),
                "anchor_equals_sctl_secondary": str(anchor_equals_sctl_secondary),
                "anchor_equals_tctl_secondary": str(anchor_equals_tctl_secondary),
                "core_anchor_all_equal_excluding_sctl_secondary": str(core_anchor_all_equal),
                "sctl_secondary_exception": str(not anchor_equals_sctl_secondary),
                "pind_resolved_mnemonics": normalize_text(row["pind_resolved_mnemonics"]),
                "pind_resolved_long_names": normalize_text(row["pind_resolved_long_names"]),
                "pctl_resolved_mnemonics": normalize_text(row["pctl_resolved_mnemonics"]),
                "pctl_resolved_long_names": normalize_text(row["pctl_resolved_long_names"]),
                "sctl_resolved_mnemonics": normalize_text(row["sctl_resolved_mnemonics"]),
                "sctl_resolved_long_names": normalize_text(row["sctl_resolved_long_names"]),
                "tctl_resolved_mnemonics": normalize_text(row["tctl_resolved_mnemonics"]),
                "tctl_resolved_long_names": normalize_text(row["tctl_resolved_long_names"]),
                "in_time_resolved_mnemonics": normalize_text(row["in_time_resolved_mnemonics"]),
                "in_time_resolved_long_names": normalize_text(row["in_time_resolved_long_names"]),
                "call_on_resolved_mnemonics": normalize_text(row["call_on_resolved_mnemonics"]),
                "call_on_resolved_long_names": normalize_text(row["call_on_resolved_long_names"]),
                "near_side_ctl_resolved_mnemonics": normalize_text(row["near_side_ctl_resolved_mnemonics"]),
                "entry_route_count": normalize_text(row["entry_route_count"]),
                "exit_route_count": normalize_text(row["exit_route_count"]),
                "route_total_count": normalize_text(row["route_total_count"]),
            }
        )

        entry = grouped_rows_by_cp.setdefault(
            control_point_uid,
            {
                "control_point_uid": control_point_uid,
                "subdivision_name": subdivision_name,
                "codeline": normalize_text(row["codeline"]),
                "shared_anchor_raw": shared_anchor_raw,
                "signal_count": 0,
                "route_total_count": 0,
                "sctl_secondary_exception_count": 0,
                "opposing_signal_count": 0,
                "call_on_signal_count": 0,
                "near_side_signal_count": 0,
                "_signal_examples": [],
                "_signal_types": set(),
                "_pind": set(),
                "_pctl": set(),
                "_sctl": set(),
                "_tctl": set(),
                "_in_time": set(),
                "_call_on": set(),
                "_near_side": set(),
            },
        )
        entry["signal_count"] += 1
        entry["route_total_count"] += int(normalize_text(row["route_total_count"]) or 0)
        if not anchor_equals_sctl_secondary:
            entry["sctl_secondary_exception_count"] += 1
        if normalize_text(row["in_time_type"]) == "OPPOSING":
            entry["opposing_signal_count"] += 1
        if int(normalize_text(row["call_on_resolved_count"]) or 0) > 0:
            entry["call_on_signal_count"] += 1
        if int(normalize_text(row["near_side_ctl_resolved_count"]) or 0) > 0:
            entry["near_side_signal_count"] += 1
        if signal_uid and len(entry["_signal_examples"]) < 10:
            entry["_signal_examples"].append(f"{signal_uid}:{tool_tip_name or signal_name}")
        if normalize_text(row["signal_type"]):
            entry["_signal_types"].add(normalize_text(row["signal_type"]))
        for key, field in [
            ("_pind", "pind_resolved_mnemonics"),
            ("_pctl", "pctl_resolved_mnemonics"),
            ("_sctl", "sctl_resolved_mnemonics"),
            ("_tctl", "tctl_resolved_mnemonics"),
            ("_in_time", "in_time_resolved_mnemonics"),
            ("_call_on", "call_on_resolved_mnemonics"),
            ("_near_side", "near_side_ctl_resolved_mnemonics"),
        ]:
            for value in split_non_empty_tokens(row[field], "|"):
                entry[key].add(value)

    grouped_rows: list[dict] = []
    for entry in grouped_rows_by_cp.values():
        grouped_rows.append(
            {
                "control_point_uid": entry["control_point_uid"],
                "subdivision_name": entry["subdivision_name"],
                "codeline": entry["codeline"],
                "shared_anchor_raw": entry["shared_anchor_raw"],
                "signal_count": entry["signal_count"],
                "route_total_count": entry["route_total_count"],
                "sctl_secondary_exception_count": entry["sctl_secondary_exception_count"],
                "opposing_signal_count": entry["opposing_signal_count"],
                "call_on_signal_count": entry["call_on_signal_count"],
                "near_side_signal_count": entry["near_side_signal_count"],
                "signal_examples": "|".join(entry["_signal_examples"]),
                "signal_types": "|".join(sorted(entry["_signal_types"])),
                "pind_resolved_mnemonics": "|".join(sorted(entry["_pind"])),
                "pctl_resolved_mnemonics": "|".join(sorted(entry["_pctl"])),
                "sctl_resolved_mnemonics": "|".join(sorted(entry["_sctl"])),
                "tctl_resolved_mnemonics": "|".join(sorted(entry["_tctl"])),
                "in_time_resolved_mnemonics": "|".join(sorted(entry["_in_time"])),
                "call_on_resolved_mnemonics": "|".join(sorted(entry["_call_on"])),
                "near_side_ctl_resolved_mnemonics": "|".join(sorted(entry["_near_side"])),
            }
        )

    per_signal_rows.sort(key=lambda item: (item["subdivision_name"], item["control_point_uid"], item["signal_uid"]))
    grouped_rows.sort(key=lambda item: (item["subdivision_name"], item["control_point_uid"]))
    return per_signal_rows, grouped_rows


def build_table_profile_summary() -> tuple[list[dict], list[dict]]:
    schema_rows = read_csv(REPO_ROOT / "exports" / "inventory" / "tmds_schema_inventory.csv")
    schema_lookup: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in schema_rows:
        key = (
            normalize_text(row.get("database_name")),
            normalize_text(row.get("table_name")),
            normalize_text(row.get("column_name")),
        )
        schema_lookup[key] = row

    profile_targets = [
        ("tmdsDatabaseStatic", "tblCodeLines", RAW_DIR / "tmdsDatabaseStatic.code_line_detail_full.csv"),
        ("tmdsDatabaseStatic", "tblCodeStations", RAW_DIR / "tmdsDatabaseStatic.code_station_detail_full.csv"),
        ("tmdsDatabaseStatic", "tblCompControlPoints", RAW_DIR / "tmdsDatabaseStatic.control_point_detail_full.csv"),
        ("tmdsDatabaseStatic", "tblCompSignals", RAW_DIR / "tmdsDatabaseStatic.signal_detail_full.csv"),
        ("tmdsDatabaseStatic", "tblCompTracks", RAW_DIR / "tmdsDatabaseStatic.track_detail_full.csv"),
        ("tmdsDatabaseStatic", "tblCompSwitches", RAW_DIR / "tmdsDatabaseStatic.switch_detail_full.csv"),
        ("tmdsDatabaseStatic", "tblCompMiscDevices", RAW_DIR / "tmdsDatabaseStatic.misc_device_detail_full.csv"),
        ("tmdsDatabaseDynamic", "tblTrainsActive", RAW_DIR / "tmdsDatabaseDynamic.active_train_detail_full.csv"),
        ("tmdsDatabaseDynamic", "tblLocomotivePositionReport", RAW_DIR / "tmdsDatabaseDynamic.locomotive_position_detail_full.csv"),
        ("tmdsDatabaseDynamic", "MdmTrainObjectData", RAW_DIR / "tmdsDatabaseDynamic.mdm_train_object_detail_full.csv"),
        ("tmdsDatabaseDynamic", "tblBosDepartureTestData", RAW_DIR / "tmdsDatabaseDynamic.bos_departure_test_detail_full.csv"),
        ("tmdsDatabaseDynamic", "tblTrainOsEventsActive", RAW_DIR / "tmdsDatabaseDynamic.os_event_detail_full.csv"),
        ("tmdsDatabaseDynamic", "tblAuthoritiesActive", RAW_DIR / "tmdsDatabaseDynamic.authority_detail_full.csv"),
        ("tmdsDatabaseDynamic", "tblSystemBulletins", RAW_DIR / "tmdsDatabaseDynamic.bulletin_detail_full.csv"),
    ]

    table_summary_rows: list[dict] = []
    column_profile_rows: list[dict] = []

    for database_name, table_name, path in profile_targets:
        rows = read_csv(path)
        if not rows:
            table_summary_rows.append(
                {
                    "database_name": database_name,
                    "table_name": table_name,
                    "row_count": 0,
                    "column_count": 0,
                    "profile_source_csv": str(path.relative_to(REPO_ROOT)),
                }
            )
            continue

        columns = list(rows[0].keys())
        table_summary_rows.append(
            {
                "database_name": database_name,
                "table_name": table_name,
                "row_count": len(rows),
                "column_count": len(columns),
                "profile_source_csv": str(path.relative_to(REPO_ROOT)),
            }
        )

        for ordinal_position, column_name in enumerate(columns, start=1):
            non_empty_values: list[str] = []
            sample_values: list[str] = []
            seen_samples: set[str] = set()
            max_length = 0
            for row in rows:
                raw_value = row.get(column_name)
                value = "" if raw_value is None else str(raw_value)
                if len(value) > max_length:
                    max_length = len(value)
                if value != "":
                    normalized_value = normalize_text(value)
                    non_empty_values.append(normalized_value)
                    if normalized_value not in seen_samples and len(sample_values) < 8:
                        seen_samples.add(normalized_value)
                        sample_values.append(normalized_value)

            distinct_non_empty_values = sorted(set(non_empty_values))
            schema_row = schema_lookup.get((database_name, table_name, column_name), {})
            column_profile_rows.append(
                {
                    "database_name": database_name,
                    "table_name": table_name,
                    "column_name": column_name,
                    "ordinal_position": ordinal_position,
                    "data_type": normalize_text(schema_row.get("data_type")),
                    "row_count": len(rows),
                    "non_empty_count": len(non_empty_values),
                    "empty_count": len(rows) - len(non_empty_values),
                    "distinct_non_empty_count": len(distinct_non_empty_values),
                    "max_length": max_length,
                    "sample_values": "|".join(sample_values),
                    "profile_source_csv": str(path.relative_to(REPO_ROOT)),
                }
            )

    table_summary_rows.sort(key=lambda item: (item["database_name"], item["table_name"]))
    column_profile_rows.sort(
        key=lambda item: (
            item["database_name"],
            item["table_name"],
            int(item["ordinal_position"]),
        )
    )
    return table_summary_rows, column_profile_rows


def build_station_foundation_summary(
    component_bit_reference_map: list[dict],
    cp_assignment_summary: list[dict],
) -> list[dict]:
    station_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.code_station_context.csv")
    component_lookup_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.component_lookup.csv")
    route_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.route_context.csv")

    component_family_counts: dict[str, dict[str, int]] = {}
    for row in component_lookup_rows:
        cp_uid = normalize_text(row["parent_control_point_uid"])
        family = normalize_text(row["component_family"])
        component_family_counts.setdefault(cp_uid, {})
        component_family_counts[cp_uid][family] = component_family_counts[cp_uid].get(family, 0) + 1

    component_status_counts: dict[str, dict[str, int]] = {}
    for row in component_bit_reference_map:
        cp_uid = normalize_text(row["parent_control_point_uid"])
        status = normalize_text(row["match_status"])
        component_status_counts.setdefault(cp_uid, {})
        component_status_counts[cp_uid][status] = component_status_counts[cp_uid].get(status, 0) + 1

    cp_status_counts: dict[str, dict[str, int]] = {}
    cp_family_status_counts: dict[str, dict[str, int]] = {}
    for row in cp_assignment_summary:
        cp_uid = normalize_text(row["parent_control_point_uid"])
        status = normalize_text(row["match_status"])
        family = normalize_text(row["component_family"])
        cp_status_counts.setdefault(cp_uid, {})
        cp_status_counts[cp_uid][status] = cp_status_counts[cp_uid].get(status, 0) + 1
        cp_family_status_counts.setdefault(cp_uid, {})
        family_key = f"{family}|{status}"
        cp_family_status_counts[cp_uid][family_key] = cp_family_status_counts[cp_uid].get(family_key, 0) + 1

    route_count_by_cp: dict[str, int] = {}
    for row in route_rows:
        cp_uid = normalize_text(row["CPUID"])
        route_count_by_cp[cp_uid] = route_count_by_cp.get(cp_uid, 0) + 1

    normalized_rows: list[dict] = []
    for row in station_rows:
        cp_uid = normalize_text(row["ControlPointNumber"])
        family_counts = component_family_counts.get(cp_uid, {})
        ref_counts = component_status_counts.get(cp_uid, {})
        cp_counts = cp_status_counts.get(cp_uid, {})
        cp_family_counts = cp_family_status_counts.get(cp_uid, {})
        normalized_rows.append(
            {
                "code_line_number": normalize_text(row["CodeLineNumber"]),
                "code_line_name": normalize_text(row["CodeLineName"]),
                "code_station_number": normalize_text(row["CodeStationNumber"]),
                "station_name": normalize_text(row["StationName"]),
                "control_point_number": cp_uid,
                "control_point_name": normalize_text(row["ControlPointName"]),
                "ptc_site_name": normalize_text(row["PTCSiteName"]),
                "subdivision_uid": normalize_text(row["SubdivisionUID"]),
                "subdivision_name": normalize_text(row["SubdivisionName"]),
                "territory_assignment": normalize_text(row["TerritoryAssignment"]),
                "control_address": normalize_text(row["ControlAddress"]),
                "indication_address": normalize_text(row["IndicationAddress"]),
                "number_of_controls": normalize_text(row["NumberOfControls"]),
                "number_of_indications": normalize_text(row["NumberOfIndications"]),
                "signal_count": family_counts.get("signal", 0),
                "track_count": family_counts.get("track", 0),
                "switch_count": family_counts.get("switch", 0),
                "misc_device_count": family_counts.get("misc_device", 0),
                "route_count": route_count_by_cp.get(cp_uid, 0),
                "component_reference_resolved_count": ref_counts.get("resolved", 0),
                "component_reference_unresolved_count": ref_counts.get("unresolved", 0),
                "component_reference_default_zero_count": ref_counts.get("default_zero", 0),
                "cp_reference_resolved_count": cp_counts.get("resolved", 0),
                "cp_reference_unresolved_count": cp_counts.get("unresolved", 0),
                "cp_reference_default_zero_count": cp_counts.get("default_zero", 0),
                "direct_cp_resolved_count": cp_family_counts.get("control_point|resolved", 0),
                "direct_cp_unresolved_count": cp_family_counts.get("control_point|unresolved", 0),
                "misc_device_cp_resolved_count": cp_family_counts.get("misc_device|resolved", 0),
                "misc_device_cp_unresolved_count": cp_family_counts.get("misc_device|unresolved", 0),
            }
        )

    normalized_rows.sort(
        key=lambda item: (
            int(item["code_line_number"] or 0),
            int(item["code_station_number"] or 0),
            int(item["control_point_number"] or 0),
        )
    )
    return normalized_rows


def build_train_runtime_foundation_summary() -> tuple[list[dict], list[dict]]:
    active_rows = read_csv(RAW_DIR / "tmdsDatabaseDynamic.active_train_context.csv")
    runtime_rows = read_csv(RAW_DIR / "tmdsDatabaseDynamic.locomotive_runtime_context.csv")
    authority_rows = read_csv(RAW_DIR / "tmdsDatabaseDynamic.authority_detail_full.csv")

    runtime_by_loco: dict[str, list[dict[str, str]]] = {}
    for row in runtime_rows:
        key = normalize_text(row["LocoId"])
        if key:
            runtime_by_loco.setdefault(key, []).append(row)

    authority_count_by_symbol: dict[str, int] = {}
    for row in authority_rows:
        key = normalize_text(row.get("TrainSymbol"))
        if key:
            authority_count_by_symbol[key] = authority_count_by_symbol.get(key, 0) + 1

    normalized_rows: list[dict] = []
    join_status_counts: dict[str, int] = {}
    for row in active_rows:
        engine_id = normalize_text(row["EngineID"])
        symbol = normalize_text(row["Symbol"])
        runtime_matches = runtime_by_loco.get(engine_id, []) if engine_id else []
        if engine_id and runtime_matches:
            join_status = "exact_engine_to_loco_match"
            runtime_row = runtime_matches[0]
        elif not engine_id:
            join_status = "no_engine_id"
            runtime_row = {}
        else:
            join_status = "engine_id_without_runtime_match"
            runtime_row = {}

        join_status_counts[join_status] = join_status_counts.get(join_status, 0) + 1
        normalized_rows.append(
            {
                "symbol": symbol,
                "train_guid": normalize_text(row["TrainGUID"]),
                "engine_id": engine_id,
                "direction": normalize_text(row["Direction"]),
                "train_type": normalize_text(row["TrainType"]),
                "origin": normalize_text(row["Origin"]),
                "dest": normalize_text(row["Dest"]),
                "subdivision": normalize_text(row["Subdivision"]),
                "track_guid": normalize_text(row["TrackGUID"]),
                "track_component_name": normalize_text(row["TrackComponentName"]),
                "track_name": normalize_text(row["TrackName"]),
                "track_alias": normalize_text(row["TrackAlias"]),
                "track_route_name": normalize_text(row["TrackRouteName"]),
                "control_point_name": normalize_text(row["ControlPointName"]),
                "schedule_status": normalize_text(row["ScheduleStatus"]),
                "ptc_status": normalize_text(row["PTCStatus"]),
                "bulletin_route_code": normalize_text(row["BulletinRouteCode"]),
                "authority_designation": normalize_text(row["AuthorityDesignation"]),
                "ctc_authority_designation": normalize_text(row["CTCAuthorityDesignation"]),
                "home_road_code": normalize_text(row["HomeRoadCode"]),
                "active_sub_train_sheet": normalize_text(row["ActiveSubTrainSheet"]),
                "authority_count_by_symbol": authority_count_by_symbol.get(symbol, 0),
                "runtime_join_status": join_status,
                "runtime_loco_id": normalize_text(runtime_row.get("LocoId")),
                "runtime_train_symbol": normalize_text(runtime_row.get("train_symbol")),
                "runtime_last_updated": normalize_text(runtime_row.get("last_updated")),
                "runtime_icd_interface_version": normalize_text(runtime_row.get("IcdInterfaceVersion")),
                "runtime_departure_test_status": normalize_text(runtime_row.get("DepartureTestStatus")),
                "runtime_onboard_software_version": normalize_text(runtime_row.get("OnboardSoftwareVersion")),
                "runtime_head_end_track_name": normalize_text(runtime_row.get("head_end_track_name")),
                "runtime_departure_track_name": normalize_text(runtime_row.get("DepartureTrackName")),
                "runtime_departure_location_name": normalize_text(runtime_row.get("DepartureLocationName")),
            }
        )

    join_summary_rows = [
        {
            "runtime_join_status": key,
            "row_count": value,
        }
        for key, value in sorted(join_status_counts.items(), key=lambda item: item[0])
    ]

    normalized_rows.sort(key=lambda item: (item["symbol"], item["train_guid"]))
    return normalized_rows, join_summary_rows


def build_authority_foundation_summary() -> list[dict]:
    authority_rows = read_csv(RAW_DIR / "tmdsDatabaseDynamic.authority_detail_full.csv")
    normalized_rows: list[dict] = []
    for row in authority_rows:
        component_uids = split_non_empty_tokens(row.get("Components"), "|")
        ptc_component_uids = split_non_empty_tokens(row.get("PTCComponents"), "|")
        switch_list = split_non_empty_tokens(row.get("SwitchList"), "|")
        route_components = split_non_empty_tokens(row.get("RouteCompData"), "|")
        ptc_segments = split_non_empty_tokens(row.get("PTCAuthoritySegments"), "|")
        ptc_restrictions = split_non_empty_tokens(row.get("PTCRestrictionSegments"), "|")
        ptc_contingencies = split_non_empty_tokens(row.get("PTCContingencySegments"), "|")
        tactical_routes = split_non_empty_tokens(row.get("TacticalRouteList"), "|")
        normalized_rows.append(
            {
                "authority_number": normalize_text(row.get("AuthorityNumber")),
                "authority_uid": normalize_text(row.get("AuthorityUID")),
                "railroad_name": normalize_text(row.get("RailRoadName")),
                "dispatcher_name": normalize_text(row.get("DispatcherName")),
                "create_date": normalize_text(row.get("CreateDate")),
                "authority_date": normalize_text(row.get("AuthorityDate")),
                "direction": normalize_text(row.get("Direction")),
                "authority_type": normalize_text(row.get("AuthorityType")),
                "territory_assignment": normalize_text(row.get("TerritoryAssignment")),
                "issue_to": normalize_text(row.get("IssueTo")),
                "train_symbol": normalize_text(row.get("TrainSymbol")),
                "issued_to_train": normalize_text(row.get("IssuedToTrain")),
                "joint_authority": normalize_text(row.get("JointAuthority")),
                "until_released": normalize_text(row.get("UntilReleased")),
                "is_ptc": normalize_text(row.get("IsPTC")),
                "ptc_ack_pending": normalize_text(row.get("PTCAckPending")),
                "ptc_bos_authority_status": normalize_text(row.get("PTCBOSAuthorityStatus")),
                "ptc_authority_status": normalize_text(row.get("PTCAuthorityStatus")),
                "authority_status": normalize_text(row.get("AuthorityStatus")),
                "issued_from_workstation": normalize_text(row.get("IssuedFromWorkstation")),
                "authority_limits": normalize_text(row.get("AuthorityLimits")),
                "limits_info": normalize_text(row.get("LimitsInfo")),
                "authority_other_information": normalize_text(row.get("AuthorityOtherInformation")),
                "component_uid_count": len(component_uids),
                "ptc_component_uid_count": len(ptc_component_uids),
                "switch_list_count": len(switch_list),
                "route_component_group_count": len(route_components),
                "ptc_authority_segment_count": len(ptc_segments),
                "ptc_restriction_segment_count": len(ptc_restrictions),
                "ptc_contingency_segment_count": len(ptc_contingencies),
                "tactical_route_count": len(tactical_routes),
                "ptc_reference": normalize_text(row.get("PTCReference")),
                "version": normalize_text(row.get("Version")),
            }
        )

    normalized_rows.sort(key=lambda item: int(item["authority_number"] or 0))
    return normalized_rows


def build_bulletin_foundation_summary() -> list[dict]:
    bulletin_rows = read_csv(RAW_DIR / "tmdsDatabaseDynamic.bulletin_detail_full.csv")
    normalized_rows: list[dict] = []
    for row in bulletin_rows:
        component_uids = split_non_empty_tokens(row.get("CompList"), "-")
        ptc_track_segments = split_non_empty_tokens(row.get("PTCTrackSegments"), "|")
        track_names = split_non_empty_tokens(row.get("TrackNameList"), "|")
        normalized_rows.append(
            {
                "restriction_uid": normalize_text(row.get("RestrictionUID")),
                "restriction_type": normalize_text(row.get("RestrictionType")),
                "restriction_subtype": normalize_text(row.get("SubType")),
                "status": normalize_text(row.get("Status")),
                "restriction_date": normalize_text(row.get("RestrictionDate")),
                "activated_time": normalize_text(row.get("ActivatedTime")),
                "creator": normalize_text(row.get("Creator")),
                "subdivision_name": normalize_text(row.get("RestrictionSubName")),
                "subdivision_from_name": normalize_text(row.get("SubdivisionFromName")),
                "subdivision_to_name": normalize_text(row.get("SubdivisionToName")),
                "direction": normalize_text(row.get("Direction")),
                "track_name": normalize_text(row.get("TrackName")),
                "track_name_count": len(track_names),
                "milepost_begin": normalize_text(row.get("MilePostBegin")),
                "milepost_end": normalize_text(row.get("MilePostEnd")),
                "train_speed_freight": normalize_text(row.get("TrainSpeedFreight")),
                "train_speed_passenger": normalize_text(row.get("TrainSpeedPassengar")),
                "train_speed_expedite": normalize_text(row.get("TrainSpeedExpedite")),
                "train_specific": normalize_text(row.get("TrainSpecific")),
                "train_symbol": normalize_text(row.get("TrainSymbol")),
                "track_blocked": normalize_text(row.get("TrackBlocked")),
                "track_out_of_service": normalize_text(row.get("TrackOutOfService")),
                "on_train": normalize_text(row.get("OnTrain")),
                "is_ptc": normalize_text(row.get("IsPTC")),
                "ptc_status": normalize_text(row.get("PTCStatus")),
                "system_wide_bulletin": normalize_text(row.get("SystemWideBulletin")),
                "non_blocking_bulletin": normalize_text(row.get("NonBlockingBulletin")),
                "component_uid_count": len(component_uids),
                "ptc_track_segment_count": len(ptc_track_segments),
                "bulletin_read_data": normalize_text(row.get("BulletinReadData")),
                "ptc_bulletin_text": normalize_text(row.get("PTCBulletinText")),
                "information": normalize_text(row.get("Information")),
                "comments": normalize_text(row.get("Comments")),
            }
        )

    normalized_rows.sort(key=lambda item: int(item["restriction_uid"] or 0))
    return normalized_rows


def build_os_event_foundation_summary() -> list[dict]:
    os_rows = read_csv(RAW_DIR / "tmdsDatabaseDynamic.os_event_detail_full.csv")
    normalized_rows: list[dict] = []
    for row in os_rows:
        report_tokens = split_non_empty_tokens(row.get("ReportData"), "|")
        normalized_rows.append(
            {
                "train_sheet_guid": normalize_text(row.get("TrainSheetGUID")),
                "os_point": normalize_text(row.get("OsPoint")),
                "os_time": normalize_text(row.get("OsTime")),
                "track_guid": normalize_text(row.get("TrackGUID")),
                "subdivision_name": normalize_text(row.get("SubName")),
                "direction": normalize_text(row.get("Direction")),
                "event_creator": normalize_text(row.get("EventCreator")),
                "report_token_count": len(report_tokens),
                "report_data": normalize_text(row.get("ReportData")),
            }
        )

    normalized_rows.sort(key=lambda item: (item["subdivision_name"], item["os_point"], item["os_time"]))
    return normalized_rows


def build_route_switch_context() -> list[dict]:
    route_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.route_context.csv")
    component_lookup_rows = read_csv(RAW_DIR / "tmdsDatabaseStatic.component_lookup.csv")
    switch_lookup = {
        normalize_text(row["component_uid"]): row
        for row in component_lookup_rows
        if row["component_family"] == "switch"
    }

    normalized_rows: list[dict] = []
    for route in route_rows:
        switch_list = normalize_text(route["SwitchList"])
        if not switch_list:
            normalized_rows.append(
                {
                    "route_guid": route["RouteGUID"],
                    "system_uid": route["SystemUID"],
                    "control_point_uid": route["CPUID"],
                    "control_point_name": route["ControlPointName"],
                    "entry_signal_uid": route["EntrySignal"],
                    "entry_signal_name": route["EntrySignalName"],
                    "exit_signal_uid": route["ExitSignal"],
                    "exit_signal_name": route["ExitSignalName"],
                    "switch_uid": "",
                    "switch_name": "",
                    "switch_tooltip_name": "",
                    "required_state": "",
                    "switch_order": 0,
                    "switch_list_raw": switch_list,
                }
            )
            continue

        for switch_order, switch_part in enumerate([part for part in switch_list.split("|") if part], start=1):
            tokens = [token.strip() for token in switch_part.split(";")]
            switch_uid = tokens[0] if tokens else ""
            required_state = tokens[2] if len(tokens) >= 3 else ""
            switch_lookup_row = switch_lookup.get(normalize_text(switch_uid), {})
            normalized_rows.append(
                {
                    "route_guid": route["RouteGUID"],
                    "system_uid": route["SystemUID"],
                    "control_point_uid": route["CPUID"],
                    "control_point_name": route["ControlPointName"],
                    "entry_signal_uid": route["EntrySignal"],
                    "entry_signal_name": route["EntrySignalName"],
                    "exit_signal_uid": route["ExitSignal"],
                    "exit_signal_name": route["ExitSignalName"],
                    "switch_uid": switch_uid,
                    "switch_name": switch_lookup_row.get("component_name", ""),
                    "switch_tooltip_name": switch_lookup_row.get("component_secondary_name", ""),
                    "required_state": required_state,
                    "switch_order": switch_order,
                    "switch_list_raw": switch_list,
                }
            )

    normalized_rows.sort(
        key=lambda item: (
            normalize_text(item["control_point_uid"]),
            normalize_text(item["route_guid"]),
            item["switch_order"],
        )
    )
    return normalized_rows


def build_bos_emp_message_candidates() -> list[dict]:
    bos_rows = read_csv(RAW_DIR / "tmdsDatabaseDynamic.bos_emp_messages.csv")
    icd_catalog_rows = read_csv(MAPPINGS_DIR / "icd_message_catalog.csv")
    icd_by_id = {}
    for row in icd_catalog_rows:
        icd_by_id.setdefault(row["message_id"], []).append(row)

    normalized_rows: list[dict] = []
    for row in bos_rows:
        prefix = normalize_text(row["MessagePrefix"])
        if prefix.isdigit() and len(prefix) == 4:
            candidate_message_id = prefix.zfill(5)
            candidates = icd_by_id.get(candidate_message_id, [])
        else:
            candidate_message_id = ""
            candidates = []

        if candidates:
            for candidate in candidates:
                normalized_rows.append(
                    {
                        "id": row["Id"],
                        "key": row["Key"],
                        "ttl": row["Ttl"],
                        "message_prefix": prefix,
                        "emp_address": row["EmpAddress"],
                        "candidate_message_id": candidate_message_id,
                        "candidate_document_family": candidate["document_family"],
                        "candidate_release": candidate["release"],
                        "candidate_direction": candidate["direction"],
                        "candidate_message_name": candidate["message_name"],
                        "candidate_message_version": candidate["message_version"],
                        "match_basis": "inferred_leading_zero_normalization",
                    }
                )
        else:
            normalized_rows.append(
                {
                    "id": row["Id"],
                    "key": row["Key"],
                    "ttl": row["Ttl"],
                    "message_prefix": prefix,
                    "emp_address": row["EmpAddress"],
                    "candidate_message_id": candidate_message_id,
                    "candidate_document_family": "",
                    "candidate_release": "",
                    "candidate_direction": "",
                    "candidate_message_name": "",
                    "candidate_message_version": "",
                    "match_basis": "none" if not candidate_message_id else "inferred_leading_zero_normalization_no_catalog_match",
                }
            )

    normalized_rows.sort(key=lambda item: (item["message_prefix"], item["id"]))
    return normalized_rows


def build_foundation_stats(
    component_bit_reference_map: list[dict],
    component_reference_scope_summary: list[dict],
    bos_emp_message_candidates: list[dict],
    zero_first_reference_summary: list[dict],
    cp_assignment_summary: list[dict],
) -> tuple[list[dict], dict]:
    component_status_counts: dict[str, int] = {}
    component_family_counts: dict[str, int] = {}
    resolution_method_counts: dict[str, int] = {}
    for row in component_bit_reference_map:
        component_status_counts[row["match_status"]] = component_status_counts.get(row["match_status"], 0) + 1
        component_family_counts[row["component_family"]] = component_family_counts.get(row["component_family"], 0) + 1
        if row["resolution_method"]:
            resolution_method_counts[row["resolution_method"]] = resolution_method_counts.get(row["resolution_method"], 0) + 1

    scope_status_counts: dict[str, int] = {}
    for row in component_reference_scope_summary:
        scope_status_counts[row["scope_status"]] = scope_status_counts.get(row["scope_status"], 0) + 1

    bos_match_counts: dict[str, int] = {}
    bos_prefix_counts: dict[str, int] = {}
    bos_message_match_counts: dict[str, set[str]] = {}
    bos_source_ids: set[str] = set()
    for row in bos_emp_message_candidates:
        bos_match_counts[row["match_basis"]] = bos_match_counts.get(row["match_basis"], 0) + 1
        prefix = row["message_prefix"]
        bos_prefix_counts[prefix] = bos_prefix_counts.get(prefix, 0) + 1
        row_id = normalize_text(row["id"])
        if row_id:
            bos_source_ids.add(row_id)
            bos_message_match_counts.setdefault(row["match_basis"], set()).add(row_id)

    zero_first_class_counts: dict[str, int] = {}
    zero_first_scope_source_counts: dict[str, int] = {}
    zero_first_reference_column_counts: dict[str, int] = {}
    for row in zero_first_reference_summary:
        zero_first_class_counts[row["structural_class"]] = zero_first_class_counts.get(row["structural_class"], 0) + 1
        zero_first_scope_source_counts[row["scope_source_key"]] = zero_first_scope_source_counts.get(
            row["scope_source_key"],
            0,
        ) + 1
        zero_first_reference_column_counts[row["reference_column"]] = zero_first_reference_column_counts.get(
            row["reference_column"],
            0,
        ) + 1

    cp_status_counts: dict[str, int] = {}
    cp_component_family_status_counts: dict[str, int] = {}
    for row in cp_assignment_summary:
        cp_status_counts[row["match_status"]] = cp_status_counts.get(row["match_status"], 0) + 1
        family_status_key = f"{row['component_family']}|{row['match_status']}"
        cp_component_family_status_counts[family_status_key] = cp_component_family_status_counts.get(
            family_status_key,
            0,
        ) + 1

    flat_metrics = [
        {"metric": "component_reference_total", "value": len(component_bit_reference_map)},
        {"metric": "component_reference_resolved", "value": component_status_counts.get("resolved", 0)},
        {
            "metric": "component_reference_resolved_explicit",
            "value": resolution_method_counts.get("explicit_code_line_station_control_point_bp_assignment", 0),
        },
        {
            "metric": "component_reference_resolved_inferred_bp_assignment",
            "value": resolution_method_counts.get("inferred_bp_assignment_station_control_point_via_unique_cp_line", 0),
        },
        {
            "metric": "component_reference_resolved_inferred_trailing_code_line",
            "value": resolution_method_counts.get(
                "inferred_bp_assignment_station_control_point_with_trailing_code_line",
                0,
            ),
        },
        {"metric": "component_reference_unresolved", "value": component_status_counts.get("unresolved", 0)},
        {"metric": "component_reference_default_zero", "value": component_status_counts.get("default_zero", 0)},
        {"metric": "scope_reference_total", "value": len(component_reference_scope_summary)},
        {
            "metric": "scope_reference_with_candidates",
            "value": scope_status_counts.get("has_scope_candidates", 0),
        },
        {
            "metric": "scope_reference_without_candidates",
            "value": scope_status_counts.get("no_scope_candidates", 0),
        },
        {"metric": "zero_first_reference_total", "value": len(zero_first_reference_summary)},
        {
            "metric": "zero_first_reference_scoped_with_resolved_sibling",
            "value": zero_first_class_counts.get("scoped_zero_first_with_resolved_sibling", 0),
        },
        {
            "metric": "zero_first_reference_scoped_without_resolved_sibling",
            "value": zero_first_class_counts.get("scoped_zero_first_without_resolved_sibling", 0),
        },
        {
            "metric": "zero_first_reference_unscoped_with_resolved_sibling",
            "value": zero_first_class_counts.get("unscoped_zero_first_with_resolved_sibling", 0),
        },
        {
            "metric": "zero_first_reference_unscoped_without_resolved_sibling",
            "value": zero_first_class_counts.get("unscoped_zero_first_without_resolved_sibling", 0),
        },
        {"metric": "cp_reference_total", "value": len(cp_assignment_summary)},
        {"metric": "cp_reference_resolved", "value": cp_status_counts.get("resolved", 0)},
        {"metric": "cp_reference_unresolved", "value": cp_status_counts.get("unresolved", 0)},
        {"metric": "cp_reference_default_zero", "value": cp_status_counts.get("default_zero", 0)},
        {
            "metric": "cp_control_point_resolved",
            "value": cp_component_family_status_counts.get("control_point|resolved", 0),
        },
        {
            "metric": "cp_control_point_unresolved",
            "value": cp_component_family_status_counts.get("control_point|unresolved", 0),
        },
        {
            "metric": "cp_control_point_default_zero",
            "value": cp_component_family_status_counts.get("control_point|default_zero", 0),
        },
        {
            "metric": "cp_misc_device_resolved",
            "value": cp_component_family_status_counts.get("misc_device|resolved", 0),
        },
        {
            "metric": "cp_misc_device_unresolved",
            "value": cp_component_family_status_counts.get("misc_device|unresolved", 0),
        },
        {
            "metric": "cp_misc_device_default_zero",
            "value": cp_component_family_status_counts.get("misc_device|default_zero", 0),
        },
        {"metric": "bos_emp_source_message_total", "value": len(bos_source_ids)},
        {"metric": "bos_emp_candidate_row_total", "value": len(bos_emp_message_candidates)},
        {
            "metric": "bos_emp_source_messages_with_catalog_match",
            "value": len(bos_message_match_counts.get("inferred_leading_zero_normalization", set())),
        },
        {
            "metric": "bos_emp_source_messages_without_catalog_match",
            "value": len(
                bos_message_match_counts.get(
                    "inferred_leading_zero_normalization_no_catalog_match",
                    set(),
                )
            ),
        },
        {
            "metric": "bos_emp_source_messages_without_message_id",
            "value": len(bos_message_match_counts.get("none", set())),
        },
    ]

    nested_metrics = {
        "component_status_counts": component_status_counts,
        "component_family_counts": component_family_counts,
        "resolution_method_counts": resolution_method_counts,
        "scope_status_counts": scope_status_counts,
        "zero_first_class_counts": zero_first_class_counts,
        "zero_first_scope_source_counts": zero_first_scope_source_counts,
        "zero_first_reference_column_counts": dict(
            sorted(zero_first_reference_column_counts.items(), key=lambda item: item[0])
        ),
        "cp_status_counts": cp_status_counts,
        "cp_component_family_status_counts": dict(sorted(cp_component_family_status_counts.items(), key=lambda item: item[0])),
        "bos_match_counts": bos_match_counts,
        "bos_message_match_counts": {
            key: len(value)
            for key, value in sorted(bos_message_match_counts.items(), key=lambda item: item[0])
        },
        "bos_prefix_counts": dict(sorted(bos_prefix_counts.items())),
    }
    return flat_metrics, nested_metrics


def main() -> int:
    NORMALIZED_DIR.mkdir(parents=True, exist_ok=True)

    code_station_inventory = build_code_station_inventory()
    code_station_assignment_map, code_station_assignment_map_csv = build_code_station_assignment_map(code_station_inventory)
    code_line_protocol_summary = build_code_line_protocol_summary(code_station_inventory)
    component_bit_reference_map = build_component_bit_reference_map()
    cp_assignment_summary = build_cp_assignment_summary(component_bit_reference_map)
    station_foundation_summary = build_station_foundation_summary(component_bit_reference_map, cp_assignment_summary)
    subdivision_protocol_summary = build_subdivision_protocol_summary(code_line_protocol_summary, station_foundation_summary)
    genisys_station_assignment_summary = build_genisys_station_assignment_summary()
    component_reference_scope_summary = build_component_reference_scope_summary()
    zero_first_reference_summary = build_zero_first_reference_summary(component_bit_reference_map)
    zero_first_reference_class_counts = build_zero_first_reference_class_counts(zero_first_reference_summary)
    cp_assignment_slot_summary = build_cp_assignment_slot_summary(cp_assignment_summary)
    cp_assignment_resolved_patterns = build_cp_assignment_resolved_patterns(cp_assignment_summary)
    cp_zero_first_slot_summary = build_cp_zero_first_slot_summary(cp_assignment_summary)
    cp_zero_first_pattern_summary = build_cp_zero_first_pattern_summary(cp_assignment_summary)
    cp_zero_first_candidate_scope_summary = build_cp_zero_first_candidate_scope_summary(zero_first_reference_summary)
    cp_direct_four_token_local_bit_diagnostic_rows, cp_direct_four_token_local_bit_diagnostic_summary = (
        build_cp_direct_four_token_local_bit_diagnostic(cp_assignment_summary)
    )
    cp_direct_four_token_candidate_family_summary = build_cp_direct_four_token_candidate_family_summary(
        cp_direct_four_token_local_bit_diagnostic_rows
    )
    reference_family_summary = build_reference_family_summary(component_bit_reference_map)
    switch_rcps_foundation_summary, switch_rcps_pattern_summary = build_switch_rcps_foundation_summary(component_bit_reference_map)
    switch_shared_anchor_summary, switch_shared_anchor_pattern_summary = build_switch_shared_anchor_summary(component_bit_reference_map)
    track_family_foundation_summary, track_family_pattern_summary = build_track_family_foundation_summary(component_bit_reference_map)
    track_control_cluster_summary, track_control_cluster_pattern_summary = build_track_control_cluster_summary(track_family_foundation_summary)
    signal_family_foundation_summary, signal_family_pattern_summary = build_signal_family_foundation_summary(component_bit_reference_map)
    signal_shared_anchor_summary, signal_shared_anchor_pattern_summary = build_signal_shared_anchor_summary(signal_family_foundation_summary)
    signal_opposing_foundation_summary, signal_opposing_pattern_summary = build_signal_opposing_foundation_summary(component_bit_reference_map)
    table_profile_summary, table_column_profile = build_table_profile_summary()
    train_runtime_foundation_summary, train_runtime_join_summary = build_train_runtime_foundation_summary()
    authority_foundation_summary = build_authority_foundation_summary()
    bulletin_foundation_summary = build_bulletin_foundation_summary()
    os_event_foundation_summary = build_os_event_foundation_summary()
    route_switch_context = build_route_switch_context()
    bos_emp_message_candidates = build_bos_emp_message_candidates()
    foundation_stats, foundation_stats_nested = build_foundation_stats(
        component_bit_reference_map,
        component_reference_scope_summary,
        bos_emp_message_candidates,
        zero_first_reference_summary,
        cp_assignment_summary,
    )

    (NORMALIZED_DIR / "code_station_inventory.json").write_text(
        json.dumps(code_station_inventory, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "code_station_inventory.csv", code_station_inventory)

    (NORMALIZED_DIR / "code_station_assignment_map.json").write_text(
        json.dumps(code_station_assignment_map, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "code_station_assignment_map.csv", code_station_assignment_map_csv)

    (NORMALIZED_DIR / "code_line_protocol_summary.json").write_text(
        json.dumps(code_line_protocol_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "code_line_protocol_summary.csv", code_line_protocol_summary)

    (NORMALIZED_DIR / "subdivision_protocol_summary.json").write_text(
        json.dumps(subdivision_protocol_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "subdivision_protocol_summary.csv", subdivision_protocol_summary)

    (NORMALIZED_DIR / "genisys_station_assignment_summary.json").write_text(
        json.dumps(genisys_station_assignment_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "genisys_station_assignment_summary.csv", genisys_station_assignment_summary)

    (NORMALIZED_DIR / "component_bit_reference_map.json").write_text(
        json.dumps(component_bit_reference_map, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "component_bit_reference_map.csv", component_bit_reference_map)

    (NORMALIZED_DIR / "component_reference_scope_summary.json").write_text(
        json.dumps(component_reference_scope_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "component_reference_scope_summary.csv", component_reference_scope_summary)

    (NORMALIZED_DIR / "zero_first_reference_summary.json").write_text(
        json.dumps(zero_first_reference_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "zero_first_reference_summary.csv", zero_first_reference_summary)

    (NORMALIZED_DIR / "zero_first_reference_class_counts.json").write_text(
        json.dumps(zero_first_reference_class_counts, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "zero_first_reference_class_counts.csv", zero_first_reference_class_counts)

    (NORMALIZED_DIR / "cp_assignment_summary.json").write_text(
        json.dumps(cp_assignment_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "cp_assignment_summary.csv", cp_assignment_summary)

    (NORMALIZED_DIR / "cp_assignment_slot_summary.json").write_text(
        json.dumps(cp_assignment_slot_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "cp_assignment_slot_summary.csv", cp_assignment_slot_summary)

    (NORMALIZED_DIR / "cp_assignment_resolved_patterns.json").write_text(
        json.dumps(cp_assignment_resolved_patterns, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "cp_assignment_resolved_patterns.csv", cp_assignment_resolved_patterns)

    (NORMALIZED_DIR / "cp_zero_first_slot_summary.json").write_text(
        json.dumps(cp_zero_first_slot_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "cp_zero_first_slot_summary.csv", cp_zero_first_slot_summary)

    (NORMALIZED_DIR / "cp_zero_first_pattern_summary.json").write_text(
        json.dumps(cp_zero_first_pattern_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "cp_zero_first_pattern_summary.csv", cp_zero_first_pattern_summary)

    (NORMALIZED_DIR / "cp_zero_first_candidate_scope_summary.json").write_text(
        json.dumps(cp_zero_first_candidate_scope_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "cp_zero_first_candidate_scope_summary.csv", cp_zero_first_candidate_scope_summary)

    (NORMALIZED_DIR / "cp_direct_four_token_local_bit_diagnostic_rows.json").write_text(
        json.dumps(cp_direct_four_token_local_bit_diagnostic_rows, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(
        NORMALIZED_DIR / "cp_direct_four_token_local_bit_diagnostic_rows.csv",
        cp_direct_four_token_local_bit_diagnostic_rows,
    )

    (NORMALIZED_DIR / "cp_direct_four_token_local_bit_diagnostic_summary.json").write_text(
        json.dumps(cp_direct_four_token_local_bit_diagnostic_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(
        NORMALIZED_DIR / "cp_direct_four_token_local_bit_diagnostic_summary.csv",
        cp_direct_four_token_local_bit_diagnostic_summary,
    )

    (NORMALIZED_DIR / "cp_direct_four_token_candidate_family_summary.json").write_text(
        json.dumps(cp_direct_four_token_candidate_family_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(
        NORMALIZED_DIR / "cp_direct_four_token_candidate_family_summary.csv",
        cp_direct_four_token_candidate_family_summary,
    )

    (NORMALIZED_DIR / "reference_family_summary.json").write_text(
        json.dumps(reference_family_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "reference_family_summary.csv", reference_family_summary)

    (NORMALIZED_DIR / "switch_rcps_foundation_summary.json").write_text(
        json.dumps(switch_rcps_foundation_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "switch_rcps_foundation_summary.csv", switch_rcps_foundation_summary)

    (NORMALIZED_DIR / "switch_rcps_pattern_summary.json").write_text(
        json.dumps(switch_rcps_pattern_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "switch_rcps_pattern_summary.csv", switch_rcps_pattern_summary)

    (NORMALIZED_DIR / "switch_shared_anchor_summary.json").write_text(
        json.dumps(switch_shared_anchor_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "switch_shared_anchor_summary.csv", switch_shared_anchor_summary)

    (NORMALIZED_DIR / "switch_shared_anchor_pattern_summary.json").write_text(
        json.dumps(switch_shared_anchor_pattern_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "switch_shared_anchor_pattern_summary.csv", switch_shared_anchor_pattern_summary)

    (NORMALIZED_DIR / "track_family_foundation_summary.json").write_text(
        json.dumps(track_family_foundation_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "track_family_foundation_summary.csv", track_family_foundation_summary)

    (NORMALIZED_DIR / "track_family_pattern_summary.json").write_text(
        json.dumps(track_family_pattern_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "track_family_pattern_summary.csv", track_family_pattern_summary)

    (NORMALIZED_DIR / "track_control_cluster_summary.json").write_text(
        json.dumps(track_control_cluster_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "track_control_cluster_summary.csv", track_control_cluster_summary)

    (NORMALIZED_DIR / "track_control_cluster_pattern_summary.json").write_text(
        json.dumps(track_control_cluster_pattern_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "track_control_cluster_pattern_summary.csv", track_control_cluster_pattern_summary)

    (NORMALIZED_DIR / "signal_family_foundation_summary.json").write_text(
        json.dumps(signal_family_foundation_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "signal_family_foundation_summary.csv", signal_family_foundation_summary)

    (NORMALIZED_DIR / "signal_family_pattern_summary.json").write_text(
        json.dumps(signal_family_pattern_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "signal_family_pattern_summary.csv", signal_family_pattern_summary)

    (NORMALIZED_DIR / "signal_shared_anchor_summary.json").write_text(
        json.dumps(signal_shared_anchor_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "signal_shared_anchor_summary.csv", signal_shared_anchor_summary)

    (NORMALIZED_DIR / "signal_shared_anchor_pattern_summary.json").write_text(
        json.dumps(signal_shared_anchor_pattern_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "signal_shared_anchor_pattern_summary.csv", signal_shared_anchor_pattern_summary)

    (NORMALIZED_DIR / "signal_opposing_foundation_summary.json").write_text(
        json.dumps(signal_opposing_foundation_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "signal_opposing_foundation_summary.csv", signal_opposing_foundation_summary)

    (NORMALIZED_DIR / "signal_opposing_pattern_summary.json").write_text(
        json.dumps(signal_opposing_pattern_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "signal_opposing_pattern_summary.csv", signal_opposing_pattern_summary)

    (NORMALIZED_DIR / "table_profile_summary.json").write_text(
        json.dumps(table_profile_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "table_profile_summary.csv", table_profile_summary)

    (NORMALIZED_DIR / "table_column_profile.json").write_text(
        json.dumps(table_column_profile, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "table_column_profile.csv", table_column_profile)

    (NORMALIZED_DIR / "station_foundation_summary.json").write_text(
        json.dumps(station_foundation_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "station_foundation_summary.csv", station_foundation_summary)

    (NORMALIZED_DIR / "train_runtime_foundation_summary.json").write_text(
        json.dumps(train_runtime_foundation_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "train_runtime_foundation_summary.csv", train_runtime_foundation_summary)

    (NORMALIZED_DIR / "train_runtime_join_summary.json").write_text(
        json.dumps(train_runtime_join_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "train_runtime_join_summary.csv", train_runtime_join_summary)

    (NORMALIZED_DIR / "authority_foundation_summary.json").write_text(
        json.dumps(authority_foundation_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "authority_foundation_summary.csv", authority_foundation_summary)

    (NORMALIZED_DIR / "bulletin_foundation_summary.json").write_text(
        json.dumps(bulletin_foundation_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "bulletin_foundation_summary.csv", bulletin_foundation_summary)

    (NORMALIZED_DIR / "os_event_foundation_summary.json").write_text(
        json.dumps(os_event_foundation_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "os_event_foundation_summary.csv", os_event_foundation_summary)

    (NORMALIZED_DIR / "route_switch_context.json").write_text(
        json.dumps(route_switch_context, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "route_switch_context.csv", route_switch_context)

    (NORMALIZED_DIR / "bos_emp_message_candidates.json").write_text(
        json.dumps(bos_emp_message_candidates, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "bos_emp_message_candidates.csv", bos_emp_message_candidates)

    (NORMALIZED_DIR / "tmds_foundation_stats.json").write_text(
        json.dumps(
            {
                "flat_metrics": foundation_stats,
                "nested_metrics": foundation_stats_nested,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    write_csv(NORMALIZED_DIR / "tmds_foundation_stats.csv", foundation_stats)

    manifest = [
        {
            "name": "code_station_inventory",
            "json_path": str((NORMALIZED_DIR / "code_station_inventory.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "code_station_inventory.csv").relative_to(REPO_ROOT)),
            "row_count": len(code_station_inventory),
        },
        {
            "name": "code_station_assignment_map",
            "json_path": str((NORMALIZED_DIR / "code_station_assignment_map.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "code_station_assignment_map.csv").relative_to(REPO_ROOT)),
            "row_count": len(code_station_assignment_map),
        },
        {
            "name": "code_line_protocol_summary",
            "json_path": str((NORMALIZED_DIR / "code_line_protocol_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "code_line_protocol_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(code_line_protocol_summary),
        },
        {
            "name": "subdivision_protocol_summary",
            "json_path": str((NORMALIZED_DIR / "subdivision_protocol_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "subdivision_protocol_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(subdivision_protocol_summary),
        },
        {
            "name": "genisys_station_assignment_summary",
            "json_path": str((NORMALIZED_DIR / "genisys_station_assignment_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "genisys_station_assignment_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(genisys_station_assignment_summary),
        },
        {
            "name": "component_bit_reference_map",
            "json_path": str((NORMALIZED_DIR / "component_bit_reference_map.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "component_bit_reference_map.csv").relative_to(REPO_ROOT)),
            "row_count": len(component_bit_reference_map),
        },
        {
            "name": "component_reference_scope_summary",
            "json_path": str((NORMALIZED_DIR / "component_reference_scope_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "component_reference_scope_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(component_reference_scope_summary),
        },
        {
            "name": "zero_first_reference_summary",
            "json_path": str((NORMALIZED_DIR / "zero_first_reference_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "zero_first_reference_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(zero_first_reference_summary),
        },
        {
            "name": "zero_first_reference_class_counts",
            "json_path": str((NORMALIZED_DIR / "zero_first_reference_class_counts.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "zero_first_reference_class_counts.csv").relative_to(REPO_ROOT)),
            "row_count": len(zero_first_reference_class_counts),
        },
        {
            "name": "cp_assignment_summary",
            "json_path": str((NORMALIZED_DIR / "cp_assignment_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "cp_assignment_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(cp_assignment_summary),
        },
        {
            "name": "cp_assignment_slot_summary",
            "json_path": str((NORMALIZED_DIR / "cp_assignment_slot_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "cp_assignment_slot_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(cp_assignment_slot_summary),
        },
        {
            "name": "cp_assignment_resolved_patterns",
            "json_path": str((NORMALIZED_DIR / "cp_assignment_resolved_patterns.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "cp_assignment_resolved_patterns.csv").relative_to(REPO_ROOT)),
            "row_count": len(cp_assignment_resolved_patterns),
        },
        {
            "name": "cp_zero_first_slot_summary",
            "json_path": str((NORMALIZED_DIR / "cp_zero_first_slot_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "cp_zero_first_slot_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(cp_zero_first_slot_summary),
        },
        {
            "name": "cp_zero_first_pattern_summary",
            "json_path": str((NORMALIZED_DIR / "cp_zero_first_pattern_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "cp_zero_first_pattern_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(cp_zero_first_pattern_summary),
        },
        {
            "name": "cp_zero_first_candidate_scope_summary",
            "json_path": str((NORMALIZED_DIR / "cp_zero_first_candidate_scope_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "cp_zero_first_candidate_scope_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(cp_zero_first_candidate_scope_summary),
        },
        {
            "name": "cp_direct_four_token_local_bit_diagnostic_rows",
            "json_path": str((NORMALIZED_DIR / "cp_direct_four_token_local_bit_diagnostic_rows.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "cp_direct_four_token_local_bit_diagnostic_rows.csv").relative_to(REPO_ROOT)),
            "row_count": len(cp_direct_four_token_local_bit_diagnostic_rows),
        },
        {
            "name": "cp_direct_four_token_local_bit_diagnostic_summary",
            "json_path": str((NORMALIZED_DIR / "cp_direct_four_token_local_bit_diagnostic_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "cp_direct_four_token_local_bit_diagnostic_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(cp_direct_four_token_local_bit_diagnostic_summary),
        },
        {
            "name": "cp_direct_four_token_candidate_family_summary",
            "json_path": str((NORMALIZED_DIR / "cp_direct_four_token_candidate_family_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "cp_direct_four_token_candidate_family_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(cp_direct_four_token_candidate_family_summary),
        },
        {
            "name": "reference_family_summary",
            "json_path": str((NORMALIZED_DIR / "reference_family_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "reference_family_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(reference_family_summary),
        },
        {
            "name": "switch_rcps_foundation_summary",
            "json_path": str((NORMALIZED_DIR / "switch_rcps_foundation_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "switch_rcps_foundation_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(switch_rcps_foundation_summary),
        },
        {
            "name": "switch_rcps_pattern_summary",
            "json_path": str((NORMALIZED_DIR / "switch_rcps_pattern_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "switch_rcps_pattern_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(switch_rcps_pattern_summary),
        },
        {
            "name": "switch_shared_anchor_summary",
            "json_path": str((NORMALIZED_DIR / "switch_shared_anchor_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "switch_shared_anchor_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(switch_shared_anchor_summary),
        },
        {
            "name": "switch_shared_anchor_pattern_summary",
            "json_path": str((NORMALIZED_DIR / "switch_shared_anchor_pattern_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "switch_shared_anchor_pattern_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(switch_shared_anchor_pattern_summary),
        },
        {
            "name": "track_family_foundation_summary",
            "json_path": str((NORMALIZED_DIR / "track_family_foundation_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "track_family_foundation_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(track_family_foundation_summary),
        },
        {
            "name": "track_family_pattern_summary",
            "json_path": str((NORMALIZED_DIR / "track_family_pattern_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "track_family_pattern_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(track_family_pattern_summary),
        },
        {
            "name": "track_control_cluster_summary",
            "json_path": str((NORMALIZED_DIR / "track_control_cluster_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "track_control_cluster_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(track_control_cluster_summary),
        },
        {
            "name": "track_control_cluster_pattern_summary",
            "json_path": str((NORMALIZED_DIR / "track_control_cluster_pattern_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "track_control_cluster_pattern_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(track_control_cluster_pattern_summary),
        },
        {
            "name": "signal_family_foundation_summary",
            "json_path": str((NORMALIZED_DIR / "signal_family_foundation_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "signal_family_foundation_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(signal_family_foundation_summary),
        },
        {
            "name": "signal_family_pattern_summary",
            "json_path": str((NORMALIZED_DIR / "signal_family_pattern_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "signal_family_pattern_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(signal_family_pattern_summary),
        },
        {
            "name": "signal_shared_anchor_summary",
            "json_path": str((NORMALIZED_DIR / "signal_shared_anchor_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "signal_shared_anchor_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(signal_shared_anchor_summary),
        },
        {
            "name": "signal_shared_anchor_pattern_summary",
            "json_path": str((NORMALIZED_DIR / "signal_shared_anchor_pattern_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "signal_shared_anchor_pattern_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(signal_shared_anchor_pattern_summary),
        },
        {
            "name": "signal_opposing_foundation_summary",
            "json_path": str((NORMALIZED_DIR / "signal_opposing_foundation_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "signal_opposing_foundation_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(signal_opposing_foundation_summary),
        },
        {
            "name": "signal_opposing_pattern_summary",
            "json_path": str((NORMALIZED_DIR / "signal_opposing_pattern_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "signal_opposing_pattern_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(signal_opposing_pattern_summary),
        },
        {
            "name": "table_profile_summary",
            "json_path": str((NORMALIZED_DIR / "table_profile_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "table_profile_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(table_profile_summary),
        },
        {
            "name": "table_column_profile",
            "json_path": str((NORMALIZED_DIR / "table_column_profile.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "table_column_profile.csv").relative_to(REPO_ROOT)),
            "row_count": len(table_column_profile),
        },
        {
            "name": "station_foundation_summary",
            "json_path": str((NORMALIZED_DIR / "station_foundation_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "station_foundation_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(station_foundation_summary),
        },
        {
            "name": "train_runtime_foundation_summary",
            "json_path": str((NORMALIZED_DIR / "train_runtime_foundation_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "train_runtime_foundation_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(train_runtime_foundation_summary),
        },
        {
            "name": "train_runtime_join_summary",
            "json_path": str((NORMALIZED_DIR / "train_runtime_join_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "train_runtime_join_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(train_runtime_join_summary),
        },
        {
            "name": "authority_foundation_summary",
            "json_path": str((NORMALIZED_DIR / "authority_foundation_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "authority_foundation_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(authority_foundation_summary),
        },
        {
            "name": "bulletin_foundation_summary",
            "json_path": str((NORMALIZED_DIR / "bulletin_foundation_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "bulletin_foundation_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(bulletin_foundation_summary),
        },
        {
            "name": "os_event_foundation_summary",
            "json_path": str((NORMALIZED_DIR / "os_event_foundation_summary.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "os_event_foundation_summary.csv").relative_to(REPO_ROOT)),
            "row_count": len(os_event_foundation_summary),
        },
        {
            "name": "route_switch_context",
            "json_path": str((NORMALIZED_DIR / "route_switch_context.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "route_switch_context.csv").relative_to(REPO_ROOT)),
            "row_count": len(route_switch_context),
        },
        {
            "name": "bos_emp_message_candidates",
            "json_path": str((NORMALIZED_DIR / "bos_emp_message_candidates.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "bos_emp_message_candidates.csv").relative_to(REPO_ROOT)),
            "row_count": len(bos_emp_message_candidates),
        },
        {
            "name": "tmds_foundation_stats",
            "json_path": str((NORMALIZED_DIR / "tmds_foundation_stats.json").relative_to(REPO_ROOT)),
            "csv_path": str((NORMALIZED_DIR / "tmds_foundation_stats.csv").relative_to(REPO_ROOT)),
            "row_count": len(foundation_stats),
        },
    ]
    (NORMALIZED_DIR / "tmds_foundation_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
