from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple
from xml.etree import ElementTree as ET
import json
import re
import unicodedata
import zipfile

try:  # pragma: no cover - script vs package execution
    from .env_loader import get_runtime_root, load_project_dotenv
except ImportError:  # type: ignore
    from env_loader import get_runtime_root, load_project_dotenv  # type: ignore

load_project_dotenv()


@dataclass(frozen=True)
class MappingRule:
    field: str
    section: str
    path: str
    getter: Callable[[Dict[str, Any]], Any]


ROUTE_CLASSES = [
    "A",
    "B",
    "B1",
    "B2",
    "C",
    "C2",
    "C3",
    "C4",
    "CE",
    "CM2",
    "CM3",
    "CM4",
    "D",
    "D2",
    "D3",
    "D4",
    "E",
    "E4",
    "E5",
    "F",
    "G",
]

_KNICKWINKEL_RE = re.compile(r"<?\s*(\d+)\s*\u00b0\s*(?:(\d+)\s*')?")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
_MM_RE = re.compile(r"(\d+(?:[.,]\d+)?)mm")
SKIP = object()
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOT = get_runtime_root()


def _find_upload_dataset_path() -> Path | None:
    base = RUNTIME_ROOT / "rsrd_upload_tool"
    if not base.exists():
        base = PROJECT_ROOT / "data" / "rsrd_upload_tool"
    for candidate in base.glob("Schnittstelle RSRD*/RSRD2 - Informationen/RSRD Dataset v4.1_new.xlsx"):
        if candidate.exists():
            return candidate
    return None


def _load_upload_requirements() -> Dict[str, str]:
    dataset_path = _find_upload_dataset_path()
    if not dataset_path:
        return {}

    def cell_value(cell: ET.Element, shared: List[str]) -> str:
        value = cell.findtext("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v", default="")
        if cell.attrib.get("t") == "s":
            try:
                return shared[int(value or 0)]
            except (ValueError, IndexError):
                return value or ""
        return value or ""

    def col_index(ref: str) -> int:
        letters = "".join(ch for ch in ref if ch.isalpha())
        number = 0
        for ch in letters:
            number = number * 26 + (ord(ch.upper()) - ord("A") + 1)
        return number

    try:
        with zipfile.ZipFile(dataset_path) as zf:
            shared: List[str] = []
            if "xl/sharedStrings.xml" in zf.namelist():
                sst = ET.fromstring(zf.read("xl/sharedStrings.xml"))
                for si in sst.findall("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}si"):
                    texts = [
                        t.text or ""
                        for t in si.findall(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")
                    ]
                    shared.append("".join(texts))
            sheet = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
    except (OSError, zipfile.BadZipFile, ET.ParseError):
        return {}

    rows: List[List[str]] = []
    for row in sheet.findall("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}sheetData/"
                              "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}row"):
        cells = {}
        for cell in row.findall("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}c"):
            ref = cell.attrib.get("r", "")
            if not ref:
                continue
            cells[ref] = cell_value(cell, shared)
        if not cells:
            rows.append([])
            continue
        max_col = max(col_index(ref) for ref in cells)
        lst = [""] * max_col
        for ref, value in cells.items():
            lst[col_index(ref) - 1] = value
        rows.append(lst)

    header_idx = None
    for idx, row in enumerate(rows):
        if "Element Name" in row:
            header_idx = idx
            break
    if header_idx is None:
        return {}
    headers = rows[header_idx]
    try:
        element_idx = headers.index("Element Name")
        ref_idx = headers.index("Reference Schema")
        upload_idx = headers.index("Upload")
    except ValueError:
        return {}

    name_cols = list(range(element_idx, ref_idx))
    stack: List[str | None] = []
    mapping: Dict[str, str] = {}
    for row in rows[header_idx + 1:]:
        if not row:
            continue
        if len(row) <= upload_idx:
            row = row + [""] * (upload_idx + 1 - len(row))
        name_values = [row[i] if i < len(row) else "" for i in name_cols]
        if not any(name_values):
            continue
        depth = None
        name = None
        for idx, value in enumerate(name_values):
            if value:
                depth = idx
                name = value
        if depth is None or not name:
            continue
        if len(stack) <= depth:
            stack.extend([None] * (depth + 1 - len(stack)))
        stack = stack[: depth + 1]
        stack[depth] = name
        for idx in range(depth + 1, len(stack)):
            stack[idx] = None
        path = ".".join([item for item in stack if item])
        upload = row[upload_idx] if upload_idx < len(row) else ""
        if path:
            mapping[path] = upload

    return mapping


UPLOAD_REQUIREMENTS: Dict[str, str] = _load_upload_requirements()


def _upload_requirement_for(field: str) -> str:
    if field in UPLOAD_REQUIREMENTS:
        return UPLOAD_REQUIREMENTS[field]
    if field.startswith("DesignDataSet.LoadTable.RouteClassPayloads["):
        return UPLOAD_REQUIREMENTS.get("DesignDataSet.LoadTable.RouteClassPayloads.MaxPayload", "")
    if field.startswith("DesignDataSet.LoadTable.RouteClassPayloads"):
        return UPLOAD_REQUIREMENTS.get("DesignDataSet.LoadTable.RouteClassPayloads", "")
    return ""


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    return text.upper()


def _normalize_number_str(value: Any) -> str:
    text = str(value).strip()
    if text.count(",") == 1 and "." not in text:
        text = text.replace(",", ".")
    return text


def _parse_int(value: Any) -> int | None:
    if value is None:
        return None
    text = _normalize_number_str(value)
    text = text.replace(" ", "").replace("-", "")
    if text == "":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    text = _normalize_number_str(value)
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _normalize_min_temperature(value: Any) -> float | None:
    parsed = _parse_float(value)
    if parsed is None:
        return None
    return -abs(parsed)


def _parse_bool_yn(value: Any) -> bool | None:
    if value is None:
        return None
    text = _normalize_text(value)
    if text in {"Y", "J", "1", "TRUE"}:
        return True
    if text in {"N", "0", "FALSE"}:
        return False
    return None


def _parse_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    if text.replace("0", "").replace(".", "").replace("-", "") == "":
        return None
    if _DATE_RE.match(text):
        return text.split("T")[0]
    digits = re.sub(r"\D", "", text)
    if len(digits) == 8:
        year = int(digits[0:4])
        month = int(digits[4:6])
        day = int(digits[6:8])
        return f"{year:04d}-{month:02d}-{day:02d}"
    if len(digits) == 6:
        year_short = int(digits[0:2])
        year = 1900 + year_short if year_short >= 70 else 2000 + year_short
        month = int(digits[2:4])
        day = int(digits[4:6])
        return f"{year:04d}-{month:02d}-{day:02d}"
    return text.split("T")[0]


def _parse_knickwinkel(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    match = _KNICKWINKEL_RE.match(text)
    if not match:
        return _parse_float(text)
    deg = _parse_float(match.group(1))
    minute = _parse_float(match.group(2)) if match.group(2) else None
    if deg is None:
        return None
    if minute is None:
        return deg
    return deg + minute / 60


def _split_external_reference(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    parts = text.split("_")
    if len(parts) >= 2 and parts[1]:
        return parts[1]
    return text


def _air_brake_values(value: Any) -> Dict[str, Any]:
    text = _normalize_text(value)
    if text == "2XKE-GP-A":
        return {"NumberOfBrakes": 2, "BrakeSystem": "KE", "AirBrakeType": 3}
    if text == "KE-GP-A":
        return {"NumberOfBrakes": 1, "BrakeSystem": "KE", "AirBrakeType": 3}
    return {}


def _interop_capability(value: Any) -> int | None:
    mapping = {
        "BI-/MULTILATERAL": 1,
        "BI/MULTILATERAL": 1,
        "NATIONAL": 2,
        "RIV": 3,
        "TEN": 5,
        "TEN-CW": 7,
        "TEN-GE": 6,
    }
    text = _normalize_text(value)
    return mapping.get(text)


def _company_code_3838(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "3838":
        return 110601
    return None


def _owner_company_code(value: Any) -> int | None:
    text = _normalize_text(value)
    if text == "MFD RAIL":
        return 110601
    if text == "3838":
        return 110601
    return None


def _authorisation_nsa(value: Any) -> int | None:
    text = _normalize_text(value)
    parsed = _parse_int(text)
    if parsed is not None:
        return parsed
    if text == "ERA":
        return 110242
    return None


def _ec_verification_issuing_body(value: Any) -> int | None:
    text = _normalize_text(value)
    if text in {"DRAZNI URAD", "DRAZNIURAD"}:
        return 101008
    if text == "ERA":
        return 110242
    return None


def _technical_forwarding(value: Any) -> List[int] | None:
    text = _normalize_text(value)
    if text == "BELADEN":
        return [70]
    if text == "NICHT BELADEN":
        return []
    return None


def _letter_marking(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    return text[0].upper() + text[1:]


def _combined_transport(value: Any) -> str | None:
    text = _normalize_text(value)
    if not text:
        return None
    if text == "CONTAINER":
        return "C"
    if text == "TW-SATTELAUFLIEGER_BA":
        return "N"
    if text == "TW-SATTELAUFLIEGER":
        return "P"
    if text in {"SWAP-BODY CARRIER WAGONS", "SWAP BODY CARRIER WAGONS"}:
        return "C"
    if text in {"ROLLER-UNIT CARRIER WAGONS", "ROLLER UNIT CARRIER WAGONS"}:
        return "B"
    return None


def _coupling_type(value: Any) -> int | None:
    parsed = _parse_float(value)
    if parsed is None:
        return None
    if parsed == 850:
        return 2
    if parsed > 850:
        return 3
    return None


def _buffer_type(value: Any) -> str | None:
    text = _normalize_text(value)
    if text == "A/105":
        return "A"
    if text == "C/105":
        return "C"
    if text == "L4/150":
        return "L4 (150)"
    return None


def _derailment_device(value: Any) -> str | None:
    text = _normalize_text(value)
    if text == "KEINE":
        return None
    return str(value).strip() if value is not None else None


def _maintenance_plan(value: Any) -> str | None:
    text = _normalize_text(value)
    if text == "VPI":
        return "VPI-EMG"
    return None


def _planned_change_ecm(value: Any) -> Any:
    text = _normalize_text(value)
    if text == "KEINE" or text == "":
        return None
    return SKIP


def _previous_keeper(value: Any) -> Any:
    text = _normalize_text(value)
    if text == "KEINE" or text == "":
        return None
    return SKIP


RULES: List[MappingRule] = [
    MappingRule(
        field="AdministrativeDataSet.WagonNumberFreight",
        section="admin",
        path="WagonNumberFreight",
        getter=lambda row: _parse_int(row.get("WAGEN_SERIENNUMMER")),
    ),
    MappingRule(
        field="AdministrativeDataSet.PreviousWagonNumberFreight",
        section="admin",
        path="PreviousWagonNumberFreight",
        getter=lambda row: _parse_int(row.get("WG_WAGENNR_ALT")),
    ),
    MappingRule(
        field="RSRD2MetaData.ExternalReferenceID",
        section="meta",
        path="ExternalReferenceID",
        getter=lambda row: _split_external_reference(row.get("WG_BAUREIHE")),
    ),
    MappingRule(
        field="AdministrativeDataSet.RegistrationCountry",
        section="admin",
        path="RegistrationCountry",
        getter=lambda row: row.get("WG_REGIST_LAND"),
    ),
    MappingRule(
        field="AdministrativeDataSet.DatePutIntoService",
        section="admin",
        path="DatePutIntoService",
        getter=lambda row: _parse_date(row.get("WG_ZULASSDATUM")),
    ),
    MappingRule(
        field="AdministrativeDataSet.Authorisation.NSACompanyCode",
        section="admin",
        path="Authorisation.NSACompanyCode",
        getter=lambda row: _authorisation_nsa(row.get("WG_ZULASSSTELLE")),
    ),
    MappingRule(
        field="AdministrativeDataSet.Authorisation.AuthorisationReference",
        section="admin",
        path="Authorisation.AuthorisationReference",
        getter=lambda row: row.get("WG_ZULASSREFNR"),
    ),
    MappingRule(
        field="AdministrativeDataSet.Authorisation.AuthorisationDate",
        section="admin",
        path="Authorisation.AuthorisationDate",
        getter=lambda row: _parse_date(row.get("WG_ZULASSDATUM")),
    ),
    MappingRule(
        field="AdministrativeDataSet.AuthorisationValidUntil",
        section="admin",
        path="AuthorisationValidUntil",
        getter=lambda row: _parse_date(row.get("WG_ZULASSENDDAT")),
    ),
    MappingRule(
        field="AdministrativeDataSet.SuspensionOfAuthorisation",
        section="admin",
        path="SuspensionOfAuthorisation",
        getter=lambda row: _parse_bool_yn(row.get("WG_ZULAUSGESETZ")),
    ),
    MappingRule(
        field="AdministrativeDataSet.DateSuspensionOfAuthorisation",
        section="admin",
        path="DateSuspensionOfAuthorisation",
        getter=lambda row: _parse_date(row.get("WG_ZULAUSDATUM")),
    ),
    MappingRule(
        field="AdministrativeDataSet.ECVerification.IssuingBodyCompanyCode",
        section="admin",
        path="ECVerification.IssuingBodyCompanyCode",
        getter=lambda row: _ec_verification_issuing_body(row.get("WG_ECVERSTELLE")),
    ),
    MappingRule(
        field="AdministrativeDataSet.ECVerification.ECVerificationDate",
        section="admin",
        path="ECVerification.ECVerificationDate",
        getter=lambda row: _parse_date(row.get("WG_ECVERDATUM")),
    ),
    MappingRule(
        field="AdministrativeDataSet.ECVerification.ECDeclarationofVerificationReference",
        section="admin",
        path="ECVerification.ECDeclarationofVerificationReference",
        getter=lambda row: row.get("WG_ECVERNR") or row.get("WG_ERATVREF"),
    ),
    MappingRule(
        field="AdministrativeDataSet.ECVerification.ERATVReference",
        section="admin",
        path="ECVerification.ERATVReference",
        getter=lambda row: _parse_int(str(row.get("WG_ERATVREF")) if row.get("WG_ERATVREF") is not None else None),
    ),
    MappingRule(
        field="AdministrativeDataSet.ECVerification.AdditionalCertification",
        section="admin",
        path="ECVerification.AdditionalCertification",
        getter=lambda row: row.get("WG_TSI_ZUS_ZERT"),
    ),
    MappingRule(
        field="AdministrativeDataSet.ChannelTunnelPermitted",
        section="admin",
        path="ChannelTunnelPermitted",
        getter=lambda row: _parse_bool_yn(row.get("WG_TUNNELFAEHIG")),
    ),
    MappingRule(
        field="AdministrativeDataSet.OwnerCompanyCode",
        section="admin",
        path="OwnerCompanyCode",
        getter=lambda row: _owner_company_code(row.get("WG_EIGENTUEMER") or row.get("WG_HALTER_CODE")),
    ),
    MappingRule(
        field="AdministrativeDataSet.KeeperCompanyCode",
        section="admin",
        path="KeeperCompanyCode",
        getter=lambda row: _company_code_3838(row.get("WG_HALTER_CODE")),
    ),
    MappingRule(
        field="AdministrativeDataSet.ECMCompanyCode",
        section="admin",
        path="ECMCompanyCode",
        getter=lambda row: _company_code_3838(row.get("WG_ECM_CODE")),
    ),
    MappingRule(
        field="AdministrativeDataSet.PlannedChangeOfECM.CurrentECMAssignedUntil",
        section="admin",
        path="PlannedChangeOfECM.CurrentECMAssignedUntil",
        getter=lambda row: _parse_date(row.get("WG_ECMWECHSDAT")),
    ),
    MappingRule(
        field="AdministrativeDataSet.PlannedChangeOfECM.SubsequentECMCompanyCode",
        section="admin",
        path="PlannedChangeOfECM.SubsequentECMCompanyCode",
        getter=lambda row: _planned_change_ecm(row.get("WG_ECMWECHSNEXT")),
    ),
    MappingRule(
        field="AdministrativeDataSet.PreviousKeeperCompanyCode",
        section="admin",
        path="PreviousKeeperCompanyCode",
        getter=lambda row: _previous_keeper(row.get("WG_HALTER_VORHE")),
    ),
    MappingRule(
        field="AdministrativeDataSet.InteropCapability",
        section="admin",
        path="InteropCapability",
        getter=lambda row: _interop_capability(row.get("WG_AUSTAUSCHVER")),
    ),
    MappingRule(
        field="AdministrativeDataSet.OutOfServiceFlag",
        section="admin",
        path="OutOfServiceFlag",
        getter=lambda row: _parse_bool_yn(row.get("WG_AUSSERBETRIE")),
    ),
    MappingRule(
        field="AdministrativeDataSet.GCUWagon",
        section="admin",
        path="GCUWagon",
        getter=lambda row: _parse_bool_yn(row.get("WG_AVVWAGEN")),
    ),
    MappingRule(
        field="DesignDataSet.LetterMarking",
        section="design",
        path="LetterMarking",
        getter=lambda row: _letter_marking(row.get("WG_UIC_TYP")),
    ),
    MappingRule(
        field="DesignDataSet.CombinedTransportWagonType",
        section="design",
        path="CombinedTransportWagonType",
        getter=lambda row: _combined_transport(row.get("AB_KVTYP") or row.get("AB_TRAGWAGENTYP")),
    ),
    MappingRule(
        field="DesignDataSet.WagonNumberOfAxles",
        section="design",
        path="WagonNumberOfAxles",
        getter=lambda row: _parse_int(row.get("WG_ANZ_ACHSEN")),
    ),
    MappingRule(
        field="DesignDataSet.WheelDiameter",
        section="design",
        path="WheelDiameter",
        getter=lambda row: _parse_float(row.get("DG_RS_NENNLKD")),
    ),
    MappingRule(
        field="DesignDataSet.WheelsetGauge",
        section="design",
        path="WheelsetGauge",
        getter=lambda row: [_parse_float(row.get("WG_SPURWEITE"))] if _parse_float(row.get("WG_SPURWEITE")) is not None else None,
    ),
    MappingRule(
        field="DesignDataSet.NumberOfBogies",
        section="design",
        path="NumberOfBogies",
        getter=lambda row: _parse_int(row.get("WG_ANZAHL_DREHG")),
    ),
    MappingRule(
        field="DesignDataSet.BogiePitch",
        section="design",
        path="BogiePitch",
        getter=lambda row: (_parse_float(row.get("DG_RS_ABSTAND")) * 1000) if _parse_float(row.get("DG_RS_ABSTAND")) is not None else None,
    ),
    MappingRule(
        field="DesignDataSet.BogiePivotPitch",
        section="design",
        path="BogiePivotPitch",
        getter=lambda row: _parse_float(row.get("WG_DREHZAPFENAB")),
    ),
    MappingRule(
        field="DesignDataSet.InnerWheelbase",
        section="design",
        path="InnerWheelbase",
        getter=lambda row: _parse_float(row.get("WG_RSABSTINNEN")),
    ),
    MappingRule(
        field="DesignDataSet.CouplingType",
        section="design",
        path="CouplingType",
        getter=lambda row: _coupling_type(row.get("KU_BRUCHLAST")),
    ),
    MappingRule(
        field="DesignDataSet.BufferType",
        section="design",
        path="BufferType",
        getter=lambda row: _buffer_type(row.get("PU_PUFFERKATEGO")),
    ),
    MappingRule(
        field="DesignDataSet.NormalLoadingGauge",
        section="design",
        path="NormalLoadingGauge",
        getter=lambda row: row.get("WG_BEGRENZPROFI"),
    ),
    MappingRule(
        field="DesignDataSet.MinCurveRadius",
        section="design",
        path="MinCurveRadius",
        getter=lambda row: _parse_float(row.get("WG_BOGENHALBMES")),
    ),
    MappingRule(
        field="DesignDataSet.MinVerticalRadiusYardHump",
        section="design",
        path="MinVerticalRadiusYardHump",
        getter=lambda row: _parse_float(row.get("WG_KRUEMMHM_MIN")),
    ),
    MappingRule(
        field="DesignDataSet.WagonWeightEmpty",
        section="design",
        path="WagonWeightEmpty",
        getter=lambda row: _parse_float(row.get("WG_EIGENGEWICHT")),
    ),
    MappingRule(
        field="DesignDataSet.LengthOverBuffers",
        section="design",
        path="LengthOverBuffers",
        getter=lambda row: (_parse_float(row.get("WG_LAENGEUEBPUF")) / 10) if _parse_float(row.get("WG_LAENGEUEBPUF")) is not None else None,
    ),
    MappingRule(
        field="DesignDataSet.MaxAxleWeight",
        section="design",
        path="MaxAxleWeight",
        getter=lambda row: _parse_float(row.get("WG_ZUL_RS_LAST")),
    ),
    MappingRule(
        field="DesignDataSet.MaxDesignSpeed",
        section="design",
        path="MaxDesignSpeed",
        getter=lambda row: _parse_float(row.get("WG_VMAX")),
    ),
    MappingRule(
        field="DesignDataSet.AirBrake.NumberOfBrakes",
        section="design",
        path="AirBrake.NumberOfBrakes",
        getter=lambda row: _air_brake_values(row.get("BR_BAUART")).get("NumberOfBrakes"),
    ),
    MappingRule(
        field="DesignDataSet.AirBrake.BrakeSystem",
        section="design",
        path="AirBrake.BrakeSystem",
        getter=lambda row: _air_brake_values(row.get("BR_BAUART")).get("BrakeSystem"),
    ),
    MappingRule(
        field="DesignDataSet.AirBrake.AirBrakeType",
        section="design",
        path="AirBrake.AirBrakeType",
        getter=lambda row: _air_brake_values(row.get("BR_BAUART")).get("AirBrakeType"),
    ),
    MappingRule(
        field="DesignDataSet.AirBrake.BrakingPowerVariationDevice",
        section="design",
        path="AirBrake.BrakingPowerVariationDevice",
        getter=lambda row: 8 if _normalize_text(row.get("BR_LASTABBREMSU")) == "AUTOKONTINUIERL" else None,
    ),
    MappingRule(
        field="DesignDataSet.AirBrake.AirBrakedMass",
        section="design",
        path="AirBrake.AirBrakedMass",
        getter=lambda row: _parse_float(row.get("BR_MAX_BREMSGEW")),
    ),
    MappingRule(
        field="DesignDataSet.AirBrake.BrakeSpecialCharacteristics",
        section="design",
        path="AirBrake.BrakeSpecialCharacteristics",
        getter=lambda row: 2 if _normalize_text(row.get("BR_SOHLEN_MATER")) == "K-VERBUNDSTOFF" else None,
    ),
    MappingRule(
        field="DesignDataSet.HandBrake.HandBrakedWeight",
        section="design",
        path="HandBrake.HandBrakedWeight",
        getter=lambda row: _parse_float(row.get("BR_HANDBRGEWI")),
    ),
    MappingRule(
        field="DesignDataSet.HandBrake.HandBrakeType",
        section="design",
        path="HandBrake.HandBrakeType",
        getter=lambda row: 0
        if not _normalize_text(row.get("BR_TYP_HANDBREM"))
        else 1
        if _normalize_text(row.get("BR_TYP_HANDBREM")) == "FLUR-BEDIEN."
        else 2
        if _normalize_text(row.get("BR_TYP_HANDBREM")) == "BUEHNE-BEDIEN."
        else None,
    ),
    MappingRule(
        field="DesignDataSet.HandBrake.ParkingBrakeForce",
        section="design",
        path="HandBrake.ParkingBrakeForce",
        getter=lambda row: _parse_float(row.get("BR_SBREMSKRAFTS")) or None,
    ),
    MappingRule(
        field="DesignDataSet.DerailmentDetectionDevice",
        section="design",
        path="DerailmentDetectionDevice",
        getter=lambda row: _derailment_device(row.get("WG_ENTGLEISDET")),
    ),
    MappingRule(
        field="DesignDataSet.BrakeBlock.BrakeBlockName",
        section="design",
        path="BrakeBlock.BrakeBlockName",
        getter=lambda row: _build_brake_block_name(row),
    ),
    MappingRule(
        field="DesignDataSet.BrakeBlock.CompositeBrakeBlockRetrofitted",
        section="design",
        path="BrakeBlock.CompositeBrakeBlockRetrofitted",
        getter=lambda row: False,
    ),
    MappingRule(
        field="DesignDataSet.BrakeBlock.CompositeBrakeBlockInstallationDate",
        section="design",
        path="BrakeBlock.CompositeBrakeBlockInstallationDate",
        getter=lambda row: _parse_date(row.get("WG_INBETRIEBNAHME") or row.get("WG_ZULASSDATUM")),
    ),
    MappingRule(
        field="DesignDataSet.MaxLengthOfLoad",
        section="design",
        path="MaxLengthOfLoad",
        getter=lambda row: _parse_float(row.get("AB_LADELAENG_GE")),
    ),
    MappingRule(
        field="DesignDataSet.HeightOfLoadingPlaneUnladen",
        section="design",
        path="HeightOfLoadingPlaneUnladen",
        getter=lambda row: _parse_float(row.get("WG_HOEH_LADKANT")),
    ),
    MappingRule(
        field="DesignDataSet.MaxGrossWeight",
        section="design",
        path="MaxGrossWeight",
        getter=lambda row: (_parse_float(row.get("WG_ZUL_GES_GEWI")) * 1000)
        if _parse_float(row.get("WG_ZUL_GES_GEWI")) is not None
        else None,
    ),
    MappingRule(
        field="DesignDataSet.FerryPermittedFlag",
        section="design",
        path="FerryPermittedFlag",
        getter=lambda row: _parse_bool_yn(row.get("WG_FAEHRFAEHIG")),
    ),
    MappingRule(
        field="DesignDataSet.FerryRampAngle",
        section="design",
        path="FerryRampAngle",
        getter=lambda row: _parse_knickwinkel(row.get("WG_KNICKWINKEL_LT")),
    ),
    MappingRule(
        field="DesignDataSet.TemperatureRange.MaxTemp",
        section="design",
        path="TemperatureRange.MaxTemp",
        getter=lambda row: _parse_float(row.get("WG_TEMPBER_MAX")),
    ),
    MappingRule(
        field="DesignDataSet.TemperatureRange.MinTemp",
        section="design",
        path="TemperatureRange.MinTemp",
        getter=lambda row: _normalize_min_temperature(row.get("WG_TEMPBER_MIN")),
    ),
    MappingRule(
        field="DesignDataSet.TechnicalForwardingRestrictions",
        section="design",
        path="TechnicalForwardingRestrictions",
        getter=lambda row: _technical_forwarding(row.get("WG_ABSTOS_AUFLA")),
    ),
    MappingRule(
        field="DesignDataSet.MaintenancePlanRef",
        section="design",
        path="MaintenancePlanRef",
        getter=lambda row: _maintenance_plan(row.get("WG_IHREGIME")),
    ),
    MappingRule(
        field="DesignDataSet.DateLastOverhaul",
        section="design",
        path="DateLastOverhaul",
        getter=lambda row: _parse_date(row.get("WG_DATLETZG4_0")),
    ),
    MappingRule(
        field="DesignDataSet.OverhaulValidityPeriod",
        section="design",
        path="OverhaulValidityPeriod",
        getter=lambda row: _parse_float(row.get("WG_REVPERIODE")),
    ),
    MappingRule(
        field="DesignDataSet.PermittedTolerance",
        section="design",
        path="PermittedTolerance",
        getter=lambda row: _parse_float(row.get("WG_REVFRISTVERL")),
    ),
]

ERP_FIELDS: Dict[str, str] = {
    "AdministrativeDataSet.WagonNumberFreight": "WAGEN_SERIENNUMMER",
    "AdministrativeDataSet.PreviousWagonNumberFreight": "WG_WAGENNR_ALT",
    "RSRD2MetaData.ExternalReferenceID": "WG_BAUREIHE",
    "AdministrativeDataSet.RegistrationCountry": "WG_REGIST_LAND",
    "AdministrativeDataSet.DatePutIntoService": "WG_ZULASSDATUM",
    "AdministrativeDataSet.Authorisation.NSACompanyCode": "WG_ZULASSSTELLE",
    "AdministrativeDataSet.Authorisation.AuthorisationReference": "WG_ZULASSREFNR",
    "AdministrativeDataSet.Authorisation.AuthorisationDate": "WG_ZULASSDATUM",
    "AdministrativeDataSet.AuthorisationValidUntil": "WG_ZULASSENDDAT",
    "AdministrativeDataSet.SuspensionOfAuthorisation": "WG_ZULAUSGESETZ",
    "AdministrativeDataSet.DateSuspensionOfAuthorisation": "WG_ZULAUSDATUM",
    "AdministrativeDataSet.ECVerification.IssuingBodyCompanyCode": "WG_ECVERSTELLE",
    "AdministrativeDataSet.ECVerification.ECVerificationDate": "WG_ECVERDATUM",
    "AdministrativeDataSet.ECVerification.ECDeclarationofVerificationReference": "WG_ECVERNR",
    "AdministrativeDataSet.ECVerification.ERATVReference": "WG_ERATVREF",
    "AdministrativeDataSet.ECVerification.AdditionalCertification": "WG_TSI_ZUS_ZERT",
    "AdministrativeDataSet.ChannelTunnelPermitted": "WG_TUNNELFAEHIG",
    "AdministrativeDataSet.OwnerCompanyCode": "WG_EIGENTUEMER",
    "AdministrativeDataSet.KeeperCompanyCode": "WG_HALTER_CODE",
    "AdministrativeDataSet.ECMCompanyCode": "WG_ECM_CODE",
    "AdministrativeDataSet.PlannedChangeOfECM.CurrentECMAssignedUntil": "WG_ECMWECHSDAT",
    "AdministrativeDataSet.PlannedChangeOfECM.SubsequentECMCompanyCode": "WG_ECMWECHSNEXT",
    "AdministrativeDataSet.PreviousKeeperCompanyCode": "WG_HALTER_VORHE",
    "AdministrativeDataSet.InteropCapability": "WG_AUSTAUSCHVER",
    "AdministrativeDataSet.OutOfServiceFlag": "WG_AUSSERBETRIE",
    "AdministrativeDataSet.GCUWagon": "WG_AVVWAGEN",
    "DesignDataSet.LetterMarking": "WG_UIC_TYP",
    "DesignDataSet.CombinedTransportWagonType": "AB_KVTYP",
    "DesignDataSet.WagonNumberOfAxles": "WG_ANZ_ACHSEN",
    "DesignDataSet.WheelDiameter": "DG_RS_NENNLKD",
    "DesignDataSet.WheelsetGauge": "WG_SPURWEITE",
    "DesignDataSet.NumberOfBogies": "WG_ANZAHL_DREHG",
    "DesignDataSet.BogiePitch": "DG_RS_ABSTAND",
    "DesignDataSet.BogiePivotPitch": "WG_DREHZAPFENAB",
    "DesignDataSet.InnerWheelbase": "WG_RSABSTINNEN",
    "DesignDataSet.CouplingType": "KU_BRUCHLAST",
    "DesignDataSet.BufferType": "PU_PUFFERKATEGO",
    "DesignDataSet.NormalLoadingGauge": "WG_BEGRENZPROFI",
    "DesignDataSet.MinCurveRadius": "WG_BOGENHALBMES",
    "DesignDataSet.MinVerticalRadiusYardHump": "WG_KRUEMMHM_MIN",
    "DesignDataSet.WagonWeightEmpty": "WG_EIGENGEWICHT",
    "DesignDataSet.LengthOverBuffers": "WG_LAENGEUEBPUF",
    "DesignDataSet.MaxAxleWeight": "WG_ZUL_RS_LAST",
    "DesignDataSet.MaxDesignSpeed": "WG_VMAX",
    "DesignDataSet.AirBrake.NumberOfBrakes": "BR_BAUART",
    "DesignDataSet.AirBrake.BrakeSystem": "BR_BAUART",
    "DesignDataSet.AirBrake.AirBrakeType": "BR_BAUART",
    "DesignDataSet.AirBrake.BrakingPowerVariationDevice": "BR_LASTABBREMSU",
    "DesignDataSet.AirBrake.AirBrakedMass": "BR_MAX_BREMSGEW",
    "DesignDataSet.AirBrake.BrakeSpecialCharacteristics": "BR_SOHLEN_MATER",
    "DesignDataSet.HandBrake.HandBrakedWeight": "BR_HANDBRGEWI",
    "DesignDataSet.HandBrake.HandBrakeType": "BR_TYP_HANDBREM",
    "DesignDataSet.HandBrake.ParkingBrakeForce": "BR_SBREMSKRAFTS",
    "DesignDataSet.DerailmentDetectionDevice": "WG_ENTGLEISDET",
    "DesignDataSet.BrakeBlock.BrakeBlockName": "BR_SOHLEN_BEZEI, BR_ANZ_BREMSSOH, BR_BREMSSO_DIM",
    "DesignDataSet.BrakeBlock.CompositeBrakeBlockRetrofitted": "computed:false",
    "DesignDataSet.BrakeBlock.CompositeBrakeBlockInstallationDate": "WG_INBETRIEBNAHME (Fallback: WG_ZULASSDATUM)",
    "DesignDataSet.MaxLengthOfLoad": "AB_LADELAENG_GE",
    "DesignDataSet.HeightOfLoadingPlaneUnladen": "WG_HOEH_LADKANT",
    "DesignDataSet.MaxGrossWeight": "WG_ZUL_GES_GEWI",
    "DesignDataSet.FerryPermittedFlag": "WG_FAEHRFAEHIG",
    "DesignDataSet.FerryRampAngle": "WG_KNICKWINKEL_LT",
    "DesignDataSet.TemperatureRange.MaxTemp": "WG_TEMPBER_MAX",
    "DesignDataSet.TemperatureRange.MinTemp": "WG_TEMPBER_MIN",
    "DesignDataSet.TechnicalForwardingRestrictions": "WG_ABSTOS_AUFLA",
    "DesignDataSet.MaintenancePlanRef": "WG_IHREGIME",
    "DesignDataSet.DateLastOverhaul": "WG_DATLETZG4_0",
    "DesignDataSet.OverhaulValidityPeriod": "WG_REVPERIODE",
    "DesignDataSet.PermittedTolerance": "WG_REVFRISTVERL",
}

UPLOAD_FIELDS: Dict[str, str] = {
    "AdministrativeDataSet.WagonNumberFreight": "xsd:AdministrativeDataSet/xsd:WagonNumberFreight",
    "AdministrativeDataSet.PreviousWagonNumberFreight": "xsd:AdministrativeDataSet/xsd:PreviousWagonNumberFreight",
    "RSRD2MetaData.ExternalReferenceID": "xsd:RSRD2MetaData/xsd:ExternalReferenceID",
    "AdministrativeDataSet.RegistrationCountry": "xsd:AdministrativeDataSet/xsd:RegistrationCountry",
    "AdministrativeDataSet.DatePutIntoService": "xsd:AdministrativeDataSet/xsd:DatePutIntoService",
    "AdministrativeDataSet.Authorisation.NSACompanyCode": "xsd:AdministrativeDataSet/xsd:Authorisation/xsd:NSACompanyCode",
    "AdministrativeDataSet.Authorisation.AuthorisationReference": "xsd:AdministrativeDataSet/xsd:Authorisation/xsd:AuthorisationReference",
    "AdministrativeDataSet.Authorisation.AuthorisationDate": "xsd:AdministrativeDataSet/xsd:Authorisation/xsd:AuthorisationDate",
    "AdministrativeDataSet.AuthorisationValidUntil": "xsd:AdministrativeDataSet/xsd:AuthorisationValidUntil",
    "AdministrativeDataSet.SuspensionOfAuthorisation": "xsd:AdministrativeDataSet/xsd:SuspensionOfAuthorisation",
    "AdministrativeDataSet.DateSuspensionOfAuthorisation": "xsd:AdministrativeDataSet/xsd:DateSuspensionOfAuthorisation",
    "AdministrativeDataSet.ECVerification.IssuingBodyCompanyCode": (
        "xsd:AdministrativeDataSet/xsd:ECVerification/xsd:IssuingBodyCompanyCode"
    ),
    "AdministrativeDataSet.ECVerification.ECVerificationDate": (
        "xsd:AdministrativeDataSet/xsd:ECVerification/xsd:ECVerificationDate"
    ),
    "AdministrativeDataSet.ECVerification.ECDeclarationofVerificationReference": (
        "xsd:AdministrativeDataSet/xsd:ECVerification/xsd:ECDeclarationofVerificationReference"
    ),
    "AdministrativeDataSet.ECVerification.ERATVReference": "xsd:AdministrativeDataSet/xsd:ECVerification/xsd:ERATVReference",
    "AdministrativeDataSet.ECVerification.AdditionalCertification": (
        "xsd:AdministrativeDataSet/xsd:ECVerification/xsd:AdditionalCertification"
    ),
    "AdministrativeDataSet.ChannelTunnelPermitted": "xsd:AdministrativeDataSet/xsd:ChannelTunnelPermitted",
    "AdministrativeDataSet.OwnerCompanyCode": "xsd:AdministrativeDataSet/xsd:OwnerCompanyCode",
    "AdministrativeDataSet.KeeperCompanyCode": "xsd:AdministrativeDataSet/xsd:KeeperCompanyCode",
    "AdministrativeDataSet.ECMCompanyCode": "xsd:AdministrativeDataSet/xsd:ECMCompanyCode",
    "AdministrativeDataSet.PlannedChangeOfECM.CurrentECMAssignedUntil": (
        "xsd:AdministrativeDataSet/xsd:PlannedChangeOfECM/xsd:CurrentECMAssignedUntil"
    ),
    "AdministrativeDataSet.PlannedChangeOfECM.SubsequentECMCompanyCode": (
        "xsd:AdministrativeDataSet/xsd:PlannedChangeOfECM/xsd:SubsequentECMCompanyCode"
    ),
    "AdministrativeDataSet.PreviousKeeperCompanyCode": "xsd:AdministrativeDataSet/xsd:PreviousKeeperCompanyCode",
    "AdministrativeDataSet.InteropCapability": "xsd:AdministrativeDataSet/xsd:InteropCapability",
    "AdministrativeDataSet.OutOfServiceFlag": "xsd:AdministrativeDataSet/xsd:OutOfServiceFlag",
    "AdministrativeDataSet.GCUWagon": "xsd:AdministrativeDataSet/xsd:GCUWagon",
    "DesignDataSet.LetterMarking": "xsd:DesignDataSet/xsd:LetterMarking",
    "DesignDataSet.CombinedTransportWagonType": "xsd:DesignDataSet/xsd:CombinedTransportWagonType",
    "DesignDataSet.WagonNumberOfAxles": "xsd:DesignDataSet/xsd:WagonNumberOfAxles",
    "DesignDataSet.WheelDiameter": "xsd:DesignDataSet/xsd:WheelDiameter",
    "DesignDataSet.WheelsetGauge": "xsd:DesignDataSet/xsd:WheelsetGauge",
    "DesignDataSet.NumberOfBogies": "xsd:DesignDataSet/xsd:NumberOfBogies",
    "DesignDataSet.BogiePitch": "xsd:DesignDataSet/xsd:BogiePitch",
    "DesignDataSet.BogiePivotPitch": "xsd:DesignDataSet/xsd:BogiePivotPitch",
    "DesignDataSet.InnerWheelbase": "xsd:DesignDataSet/xsd:InnerWheelbase",
    "DesignDataSet.CouplingType": "xsd:DesignDataSet/xsd:CouplingType",
    "DesignDataSet.BufferType": "xsd:DesignDataSet/xsd:BufferType",
    "DesignDataSet.NormalLoadingGauge": "xsd:DesignDataSet/xsd:NormalLoadingGauge",
    "DesignDataSet.MinCurveRadius": "xsd:DesignDataSet/xsd:MinCurveRadius",
    "DesignDataSet.MinVerticalRadiusYardHump": "xsd:DesignDataSet/xsd:MinVerticalRadiusYardHump",
    "DesignDataSet.WagonWeightEmpty": "xsd:DesignDataSet/xsd:WagonWeightEmpty",
    "DesignDataSet.LengthOverBuffers": "xsd:DesignDataSet/xsd:LengthOverBuffers",
    "DesignDataSet.MaxAxleWeight": "xsd:DesignDataSet/xsd:MaxAxleWeight",
    "DesignDataSet.MaxDesignSpeed": "xsd:DesignDataSet/xsd:MaxDesignSpeed",
    "DesignDataSet.AirBrake.NumberOfBrakes": "xsd:DesignDataSet/xsd:AirBrake/xsd:NumberOfBrakes",
    "DesignDataSet.AirBrake.BrakeSystem": "xsd:DesignDataSet/xsd:AirBrake/xsd:BrakeSystem",
    "DesignDataSet.AirBrake.AirBrakeType": "xsd:DesignDataSet/xsd:AirBrake/xsd:AirBrakeType",
    "DesignDataSet.AirBrake.BrakingPowerVariationDevice": "xsd:DesignDataSet/xsd:AirBrake/xsd:BrakingPowerVariationDevice",
    "DesignDataSet.AirBrake.AirBrakedMass": "xsd:DesignDataSet/xsd:AirBrake/xsd:AirBrakedMass",
    "DesignDataSet.AirBrake.BrakeSpecialCharacteristics": "xsd:DesignDataSet/xsd:AirBrake/xsd:BrakeSpecialCharacteristics",
    "DesignDataSet.HandBrake.HandBrakedWeight": "xsd:DesignDataSet/xsd:HandBrake/xsd:HandBrakedWeight",
    "DesignDataSet.HandBrake.HandBrakeType": "xsd:DesignDataSet/xsd:HandBrake/xsd:HandBrakeType",
    "DesignDataSet.HandBrake.ParkingBrakeForce": "xsd:DesignDataSet/xsd:HandBrake/xsd:ParkingBrakeForce",
    "DesignDataSet.DerailmentDetectionDevice": "xsd:DesignDataSet/xsd:DerailmentDetectionDevice",
    "DesignDataSet.BrakeBlock.BrakeBlockName": "xsd:DesignDataSet/xsd:BrakeBlock/xsd:BrakeBlockName",
    "DesignDataSet.BrakeBlock.CompositeBrakeBlockRetrofitted": (
        "xsd:DesignDataSet/xsd:BrakeBlock/xsd:CompositeBrakeBlockRetrofitted"
    ),
    "DesignDataSet.BrakeBlock.CompositeBrakeBlockInstallationDate": (
        "xsd:DesignDataSet/xsd:BrakeBlock/xsd:CompositeBrakeBlockInstallationDate"
    ),
    "DesignDataSet.LoadTable.SpeedCategory": "xsd:DesignDataSet/xsd:LoadTable/xsd:SpeedCategory",
    "DesignDataSet.MaxLengthOfLoad": "xsd:DesignDataSet/xsd:MaxLengthOfLoad",
    "DesignDataSet.HeightOfLoadingPlaneUnladen": "xsd:DesignDataSet/xsd:HeightOfLoadingPlaneUnladen",
    "DesignDataSet.MaxGrossWeight": "xsd:DesignDataSet/xsd:MaxGrossWeight",
    "DesignDataSet.FerryPermittedFlag": "xsd:DesignDataSet/xsd:FerryPermittedFlag",
    "DesignDataSet.FerryRampAngle": "xsd:DesignDataSet/xsd:FerryRampAngle",
    "DesignDataSet.TemperatureRange.MaxTemp": "xsd:DesignDataSet/xsd:TemperatureRange/xsd:MaxTemp",
    "DesignDataSet.TemperatureRange.MinTemp": "xsd:DesignDataSet/xsd:TemperatureRange/xsd:MinTemp",
    "DesignDataSet.TechnicalForwardingRestrictions": "xsd:DesignDataSet/xsd:TechnicalForwardingRestrictions",
    "DesignDataSet.MaintenancePlanRef": "xsd:DesignDataSet/xsd:MaintenancePlanRef",
    "DesignDataSet.DateLastOverhaul": "xsd:DesignDataSet/xsd:DateLastOverhaul",
    "DesignDataSet.OverhaulValidityPeriod": "xsd:DesignDataSet/xsd:OverhaulValidityPeriod",
    "DesignDataSet.PermittedTolerance": "xsd:DesignDataSet/xsd:PermittedTolerance",
}


def _build_brake_block_name(row: Dict[str, Any]) -> str | None:
    bezeichner = row.get("BR_SOHLEN_BEZEI")
    anz = _parse_int(row.get("BR_ANZ_BREMSSOH"))
    dim = _parse_float(row.get("BR_BREMSSO_DIM"))
    if bezeichner is None or anz is None or dim is None:
        return None
    return f"{bezeichner} - {dim}mm - {anz}x"


def _build_removable_accessories(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    entry: Dict[str, Any] = {}
    if _normalize_text(row.get("AB_LOSEBESTTYP")) == "ANDERE":
        entry["TypeOfRemovableAccessories"] = 99
    count = _parse_float(row.get("AB_LOSEBESTZAHL"))
    if count is not None and count > 0:
        entry["NumberOfAccessorOfSpecType"] = count
    if entry:
        items.append(entry)
    return items


def _load_table_speed_categories(row: Dict[str, Any]) -> List[int]:
    vmax = _parse_int(row.get("WG_VMAX"))
    if vmax is None:
        return [100, 120]
    if vmax == 100:
        return [100]
    return [100, vmax]


def _build_load_table(row: Dict[str, Any]) -> Tuple[int | None, List[int], Dict[str, List[float]]]:
    stars = _normalize_load_table_stars(row.get("AS_STERNE"))
    speed_categories = _load_table_speed_categories(row)
    route_payloads: Dict[str, List[float]] = {}
    for route_class in ROUTE_CLASSES:
        col_100 = f"AS_{route_class}_100"
        col_120 = f"AS_{route_class}_120"
        value_100 = _parse_float(row.get(col_100))
        value_120 = _parse_float(row.get(col_120))
        if value_100 is None and value_120 is None:
            continue
        values: List[float] = []
        for speed in speed_categories:
            if speed == 100:
                values.append(value_100 if value_100 is not None else 0.0)
            elif speed == 120:
                values.append(value_120 if value_120 is not None else 0.0)
            else:
                values.append(0.0)
        if any(val != 0 for val in values):
            route_payloads[route_class] = values
    return stars, speed_categories, route_payloads


def _normalize_speed_categories(values: Any) -> List[Any]:
    normalized: List[Any] = []
    for item in _as_list(values):
        if item is None or item == "":
            continue
        parsed = _parse_int(item)
        normalized.append(parsed if parsed is not None else item)
    return normalized


def _select_payloads(values: List[Any] | None, source_speeds: List[Any], target_speeds: List[Any]) -> List[Any] | None:
    if values is None:
        return None
    if not target_speeds or not source_speeds:
        return values
    selected: List[Any] = []
    for speed in target_speeds:
        try:
            idx = source_speeds.index(speed)
        except ValueError:
            return values
        if idx < len(values):
            selected.append(values[idx])
        else:
            selected.append(0)
    return selected


def _normalize_load_table_stars(value: Any) -> int | None:
    parsed = _parse_int(value)
    if parsed in (None, 0):
        return None
    return parsed


def _set_path(target: Dict[str, Any], path: str, value: Any) -> None:
    keys = path.split(".")
    node = target
    for key in keys[:-1]:
        node = node.setdefault(key, {})
    node[keys[-1]] = value


def _extract_path(source: Dict[str, Any], path: str) -> Any:
    node = source
    for key in path.split("."):
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def _prune(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned = {k: _prune(v) for k, v in value.items()}
        cleaned = {k: v for k, v in cleaned.items() if v is not None}
        if not cleaned:
            return None
        return cleaned
    if isinstance(value, list):
        cleaned_list = [v for v in (_prune(v) for v in value) if v is not None]
        if not cleaned_list:
            return None
        return cleaned_list
    return value


def _normalize_scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value).strip()


def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    text = _normalize_number_str(value)
    try:
        return float(text)
    except ValueError:
        return None


def _normalize_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    if not _DATE_RE.match(text):
        return None
    return text.split("T")[0]


def _normalize_brake_block_name(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = " ".join(text.split())

    def replace_mm(match: re.Match[str]) -> str:
        number = match.group(1).replace(",", ".")
        try:
            parsed = float(number)
        except ValueError:
            return match.group(0)
        if abs(parsed - int(parsed)) < 1e-9:
            return f"{int(parsed)}mm"
        trimmed = f"{parsed}".rstrip("0").rstrip(".")
        return f"{trimmed}mm"

    return _MM_RE.sub(replace_mm, text)


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _lists_equal(left: List[Any], right: List[Any]) -> bool:
    if len(left) != len(right):
        return False
    for a, b in zip(left, right):
        if not _values_equal(a, b):
            return False
    return True


def _is_zeroish(value: Any) -> bool:
    num = _to_number(value)
    if num is not None:
        return abs(num) < 1e-9
    text = _normalize_text(value)
    return text in {"0", "0.0"}


def _normalize_list_for_compare(values: List[Any]) -> List[Any]:
    cleaned = []
    for value in values:
        if value is None or value == "":
            continue
        if _is_zeroish(value):
            continue
        cleaned.append(value)
    return cleaned


def _values_equal(left: Any, right: Any) -> bool:
    if left is None and right is None:
        return True
    if left is None and right == "":
        return True
    if right is None and left == "":
        return True
    if isinstance(left, list) or isinstance(right, list):
        left_list = _normalize_list_for_compare(_as_list(left))
        right_list = _normalize_list_for_compare(_as_list(right))
        if isinstance(left, list) and not isinstance(right, list):
            return any(_values_equal(item, right) for item in left_list) if left_list else False
        if isinstance(right, list) and not isinstance(left, list):
            return any(_values_equal(left, item) for item in right_list) if right_list else False
        if not left_list and not right_list:
            return True
        # ERP is leading: accept extra values in RSRD as long as all ERP values exist in RSRD list.
        remaining = right_list[:]
        for item in left_list:
            match_index = next((idx for idx, cand in enumerate(remaining) if _values_equal(item, cand)), None)
            if match_index is None:
                return False
            remaining.pop(match_index)
        return True
    left_date = _normalize_date(left)
    right_date = _normalize_date(right)
    if left_date is not None or right_date is not None:
        return left_date == right_date
    left_num = _to_number(left)
    right_num = _to_number(right)
    if left_num is not None and right_num is not None:
        return abs(left_num - right_num) < 1e-6
    left_bool = _parse_bool_yn(left)
    right_bool = _parse_bool_yn(right)
    if left_bool is not None and right_bool is not None:
        return left_bool == right_bool
    return _normalize_scalar(left) == _normalize_scalar(right)


def _normalize_output(value: Any) -> Any:
    if isinstance(value, list):
        cleaned = []
        for v in value:
            if v is None or v == "":
                continue
            if _is_zeroish(v):
                continue
            cleaned.append(_normalize_output(v))
        return cleaned
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def build_erp_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    meta: Dict[str, Any] = {}
    admin: Dict[str, Any] = {}
    design: Dict[str, Any] = {}

    for rule in RULES:
        value = rule.getter(row)
        if value is SKIP:
            continue
        target = meta if rule.section == "meta" else admin if rule.section == "admin" else design
        _set_path(target, rule.path, value)

    removable = _build_removable_accessories(row)
    if removable:
        design["RemovableAccessories"] = removable

    stars, speed_categories, route_payloads = _build_load_table(row)
    if stars is not None or route_payloads or speed_categories:
        load_table: Dict[str, Any] = {}
        if speed_categories:
            load_table["SpeedCategory"] = speed_categories
        if stars is not None:
            load_table["LoadTableStars"] = stars
        if route_payloads:
            payload_list = []
            for route_class in ROUTE_CLASSES:
                if route_class not in route_payloads:
                    continue
                payload_list.append(
                    {
                        "RouteClass": route_class,
                        "MaxPayload": route_payloads[route_class],
                    }
                )
            load_table["RouteClassPayloads"] = payload_list
        design["LoadTable"] = [load_table]

    return {
        "RSRD2MetaData": meta,
        "AdministrativeDataSet": admin,
        "DesignDataSet": design,
    }


def build_erp_values(row: Dict[str, Any]) -> Dict[str, Any]:
    values = {}
    for rule in RULES:
        value = rule.getter(row)
        if value is SKIP:
            continue
        values[rule.field] = value
    return values


def compare_erp_to_rsrd(
    erp_row: Dict[str, Any],
    rsrd_admin: Dict[str, Any] | None,
    rsrd_design: Dict[str, Any] | None,
    rsrd_meta: Dict[str, Any] | None,
    include_all: bool = False,
) -> List[Dict[str, Any]]:
    def resolve_erp_field_name(field: str) -> str:
        if field == "DesignDataSet.DateLastOverhaul":
            return "WG_DATLETZG4_0"
        return ERP_FIELDS.get(field, "")

    diffs: List[Dict[str, Any]] = []
    rsrd_admin = rsrd_admin or {}
    rsrd_design = rsrd_design or {}
    rsrd_meta = rsrd_meta or {}

    values = build_erp_values(erp_row)
    for rule in RULES:
        erp_value = values.get(rule.field)
        if rule.section == "admin":
            rsrd_value = _extract_path(rsrd_admin, rule.path)
        elif rule.section == "design":
            rsrd_value = _extract_path(rsrd_design, rule.path)
        else:
            rsrd_value = _extract_path(rsrd_meta, rule.path)
        if rule.field == "DesignDataSet.BrakeBlock.BrakeBlockName":
            erp_norm = _normalize_brake_block_name(erp_value)
            rsrd_norm = _normalize_brake_block_name(rsrd_value)
            equal = _values_equal(erp_norm, rsrd_norm)
        elif rule.field == "DesignDataSet.TemperatureRange.MinTemp":
            erp_num = _to_number(erp_value)
            rsrd_num = _to_number(rsrd_value)
            if erp_num is None or rsrd_num is None:
                equal = _values_equal(erp_value, rsrd_value)
            else:
                equal = abs(abs(erp_num) - abs(rsrd_num)) < 1e-6
        else:
            equal = _values_equal(erp_value, rsrd_value)
        if not include_all and equal:
            continue
        diffs.append(
            {
                "field": rule.field,
                "erp_field": resolve_erp_field_name(rule.field),
                "rsrd_field": rule.field,
                "upload_field": UPLOAD_FIELDS.get(rule.field, ""),
                "upload_requirement": _upload_requirement_for(rule.field),
                "erp": _normalize_output(erp_value),
                "rsrd": _normalize_output(rsrd_value),
                "equal": equal,
            }
        )

    stars, speed_categories, erp_route_payloads = _build_load_table(erp_row)
    rsrd_load_table = (rsrd_design.get("LoadTable") or [{}])
    if isinstance(rsrd_load_table, list) and rsrd_load_table:
        rsrd_load_table = rsrd_load_table[0]
    if isinstance(rsrd_load_table, dict):
        rsrd_speed_raw = rsrd_load_table.get("SpeedCategory") or []
        rsrd_speed = _normalize_speed_categories(rsrd_speed_raw)
        erp_speed = speed_categories or []
        vmax = _parse_int(erp_row.get("WG_VMAX"))
        compare_speed = erp_speed
        compare_rsrd_speed = rsrd_speed
        if vmax is not None and vmax in rsrd_speed:
            compare_speed = [vmax]
            compare_rsrd_speed = [vmax]
        if include_all or not _values_equal(compare_speed, compare_rsrd_speed):
            diffs.append(
                {
                    "field": "DesignDataSet.LoadTable.SpeedCategory",
                    "erp_field": "WG_VMAX",
                    "rsrd_field": "DesignDataSet.LoadTable.SpeedCategory",
                    "upload_field": "xsd:DesignDataSet/xsd:LoadTable/xsd:SpeedCategory",
                    "upload_requirement": _upload_requirement_for("DesignDataSet.LoadTable.SpeedCategory"),
                    "erp": _normalize_output(compare_speed),
                    "rsrd": _normalize_output(compare_rsrd_speed),
                    "equal": _values_equal(compare_speed, compare_rsrd_speed),
                }
            )
        rsrd_stars = _normalize_load_table_stars(rsrd_load_table.get("LoadTableStars"))
        stars_equal = _values_equal(stars, rsrd_stars)
        if include_all or not stars_equal:
            diffs.append(
                {
                    "field": "DesignDataSet.LoadTable.LoadTableStars",
                    "erp_field": "AS_STERNE",
                    "rsrd_field": "DesignDataSet.LoadTable.LoadTableStars",
                    "upload_field": "xsd:DesignDataSet/xsd:LoadTable/xsd:LoadTableStars",
                    "upload_requirement": _upload_requirement_for("DesignDataSet.LoadTable.LoadTableStars"),
                    "erp": _normalize_output(stars),
                    "rsrd": _normalize_output(rsrd_stars),
                    "equal": stars_equal,
                }
            )
        rsrd_payloads = {}
        for entry in rsrd_load_table.get("RouteClassPayloads") or []:
            route_class = entry.get("RouteClass")
            if not route_class:
                continue
            max_payload = entry.get("MaxPayload")
            rsrd_payloads[route_class] = _as_list(max_payload)
        for route_class in ROUTE_CLASSES:
            erp_values = erp_route_payloads.get(route_class)
            rsrd_values = rsrd_payloads.get(route_class)
            erp_values = _select_payloads(erp_values, erp_speed, compare_speed)
            rsrd_values = _select_payloads(rsrd_values, rsrd_speed, compare_speed)
            if erp_values is None and rsrd_values is None and not include_all:
                continue
            equal = _values_equal(erp_values, rsrd_values)
            if not include_all and equal:
                continue
            diffs.append(
                {
                    "field": f"DesignDataSet.LoadTable.RouteClassPayloads[{route_class}]",
                    "erp_field": f"AS_{route_class}_100, AS_{route_class}_120",
                    "rsrd_field": f"DesignDataSet.LoadTable.RouteClassPayloads[{route_class}]",
                    "upload_field": (
                        "xsd:DesignDataSet/xsd:LoadTable/xsd:RouteClassPayloads"
                        f"(xsd:RouteClass={route_class})/xsd:MaxPayload"
                    ),
                    "upload_requirement": _upload_requirement_for("DesignDataSet.LoadTable.RouteClassPayloads"),
                    "erp": _normalize_output(erp_values),
                    "rsrd": _normalize_output(rsrd_values),
                    "equal": equal,
                }
            )

    return diffs


def serialize_payload(payload: Dict[str, Any]) -> str:
    pruned = _prune(payload) or {}
    return json.dumps(pruned, ensure_ascii=False)


def serialize_diffs(diffs: List[Dict[str, Any]]) -> str:
    return json.dumps(diffs, ensure_ascii=False)
