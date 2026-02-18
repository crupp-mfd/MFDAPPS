#!/usr/bin/env python3
"""Erstellt eine Jahresabrechnungs-Uebersicht fuer Gueterwagen aus Excel-Quellen."""

from __future__ import annotations

import argparse
import re
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET

MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
NS = {"a": MAIN_NS, "r": REL_NS, "pr": PKG_REL_NS}


@dataclass(frozen=True)
class Tariff:
    rate: float
    per_km: int  # 1 oder 10000


@dataclass
class TemplateRow:
    row_no: int
    customer: str
    customer_contract: str
    internal_contracts: list[str]
    free_km: float
    tariff: Tariff
    tariff_raw: str
    is_fleet: bool
    is_wagon: bool


@dataclass
class WagonUsage:
    contract: str
    wagon_no: str
    bill_start: date | None
    bill_end: date | None
    start_km: float
    end_km: float
    days_2025: int
    km_2025: float
    km_valid: bool


def parse_args() -> argparse.Namespace:
    base = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Erstellt eine Vertragsuebersicht inkl. Wagendetails fuer die Jahresabrechnung."
    )
    parser.add_argument(
        "--vorlage",
        type=Path,
        default=base / "Quellen" / "KM_Vorlage 2025.xlsx",
        help="Pfad zur KM-Vorlage",
    )
    parser.add_argument(
        "--kilometer",
        type=Path,
        default=base / "Quellen" / "KILOMETER.xlsx",
        help="Pfad zur Kilometerdatei",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=base / "Output",
        help="Zielordner fuer die erzeugte Excel-Datei",
    )
    parser.add_argument(
        "--jahr",
        type=int,
        default=2025,
        help="Abrechnungsjahr (Standard: 2025)",
    )
    return parser.parse_args()


def normalize_target(target: str) -> str:
    clean = target.lstrip("/")
    return clean if clean.startswith("xl/") else f"xl/{clean}"


def get_text_from_si(si: ET.Element) -> str:
    return "".join(node.text or "" for node in si.findall(".//a:t", NS))


def read_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []

    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    return [get_text_from_si(si) for si in root.findall("a:si", NS)]


def parse_cell_value(cell: ET.Element, shared: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        texts = [node.text or "" for node in cell.findall("a:is/a:t", NS)]
        return "".join(texts)

    value = cell.find("a:v", NS)
    if value is None or value.text is None:
        return ""

    if cell_type == "s":
        index = int(value.text)
        return shared[index] if 0 <= index < len(shared) else value.text

    return value.text


def col_to_index(col: str) -> int:
    number = 0
    for char in col:
        number = number * 26 + (ord(char) - 64)
    return number


def index_to_col(index: int) -> str:
    result = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        result = chr(65 + remainder) + result
    return result


def split_ref(cell_ref: str) -> tuple[int, int]:
    match = re.match(r"([A-Z]+)(\d+)", cell_ref)
    if not match:
        return 0, 0
    return col_to_index(match.group(1)), int(match.group(2))


def read_sheet_rows(path: Path, sheet_name: str | None = None) -> list[dict[int, str]]:
    with zipfile.ZipFile(path) as archive:
        shared = read_shared_strings(archive)

        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        rel_map = {
            rel.attrib["Id"]: normalize_target(rel.attrib["Target"])
            for rel in relationships.findall("pr:Relationship", NS)
        }

        selected_target: str | None = None
        for sheet in workbook.findall("a:sheets/a:sheet", NS):
            name = sheet.attrib.get("name", "")
            rel_id = sheet.attrib.get(f"{{{REL_NS}}}id", "")
            target = rel_map.get(rel_id)
            if not target:
                continue
            if sheet_name is None and selected_target is None:
                selected_target = target
            if sheet_name is not None and name == sheet_name:
                selected_target = target
                break

        if selected_target is None:
            raise ValueError(f"Kein passendes Sheet in Datei gefunden: {path}")

        root = ET.fromstring(archive.read(selected_target))
        rows: list[dict[int, str]] = []

        for row in root.findall("a:sheetData/a:row", NS):
            values: dict[int, str] = {}
            for cell in row.findall("a:c", NS):
                col_idx, _ = split_ref(cell.attrib.get("r", ""))
                if col_idx <= 0:
                    continue
                values[col_idx] = parse_cell_value(cell, shared)
            rows.append(values)

        return rows


def parse_float(raw: str) -> float | None:
    text = str(raw or "").strip()
    if not text:
        return None

    text = text.replace("'", "")
    text = text.replace(" ", "")

    if text.lower() in {"null", "none", "nan"}:
        return None

    number_match = re.search(r"[-+]?\d+(?:[.,]\d+)?(?:[eE][-+]?\d+)?", text)
    if not number_match:
        return None

    normalized = number_match.group(0).replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None


def parse_yyyymmdd(raw: str) -> date | None:
    text = str(raw or "").strip()
    if not text or text in {"0", "00000000"}:
        return None

    number = parse_float(text)
    if number is None:
        return None

    as_int = int(number)
    digits = f"{as_int:08d}"
    try:
        return datetime.strptime(digits, "%Y%m%d").date()
    except ValueError:
        return None


def format_date(value: date | None) -> str:
    return value.strftime("%Y-%m-%d") if value else ""


def normalize_customer_contract(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""

    text = re.sub(r"\s+", "", text)

    eight_digits = re.fullmatch(r"(\d{4})(\d{4})", text)
    if eight_digits:
        return f"{eight_digits.group(1)}-{eight_digits.group(2)}"

    three_plus_four = re.fullmatch(r"(\d{3})-(\d{4})", text)
    if three_plus_four:
        year_part = three_plus_four.group(1)
        if year_part == "204":
            return f"2024-{three_plus_four.group(2)}"
        return f"2{year_part}-{three_plus_four.group(2)}"

    return text


def parse_internal_contracts(raw: str) -> list[str]:
    source = str(raw or "").upper()
    if not source.strip():
        return []

    source = source.replace(";", "/").replace(",", "/").replace("+", "/")
    source = re.sub(r"\s+", "", source)

    tokens = [token for token in source.split("/") if token]
    result: list[str] = []
    last_digits: str | None = None

    for token in tokens:
        explicit = re.findall(r"V\d{6}", token)
        if explicit:
            for contract in explicit:
                result.append(contract)
                last_digits = contract[1:]
            continue

        six_digits = re.fullmatch(r"\d{6}", token)
        if six_digits:
            contract = f"V{token}"
            result.append(contract)
            last_digits = token
            continue

        short_digits = re.fullmatch(r"\d{1,5}", token)
        if short_digits and last_digits is not None:
            padded = f"{last_digits[: 6 - len(token)]}{token}"
            if re.fullmatch(r"\d{6}", padded):
                contract = f"V{padded}"
                result.append(contract)
                last_digits = padded
                continue

        fallback = re.findall(r"\d{6}", token)
        if fallback:
            for digits in fallback:
                contract = f"V{digits}"
                result.append(contract)
                last_digits = digits

    deduped: list[str] = []
    seen: set[str] = set()
    for contract in result:
        if contract not in seen:
            seen.add(contract)
            deduped.append(contract)
    return deduped


def parse_tariff(raw: str) -> Tariff:
    text = str(raw or "").strip()
    lower = text.lower()
    per_km = 10000 if ("10'000" in lower or "10000" in lower) else 1

    number = parse_float(text)
    rate = number if number is not None else 0.0
    return Tariff(rate=rate, per_km=per_km)


def tariff_key(tariff: Tariff) -> tuple[int, int]:
    return (int(round(tariff.rate * 1_000_000)), tariff.per_km)


def tariff_display(tariff: Tariff) -> str:
    if tariff.rate == 0:
        return "0 je 1 KM"

    formatted = f"{tariff.rate:.6f}".rstrip("0").rstrip(".")
    formatted = formatted.replace(".", ",")
    per_text = "10.000 KM" if tariff.per_km == 10000 else "1 KM"
    return f"{formatted} je {per_text}"


def calc_amount(excess_km: float, tariff: Tariff) -> float:
    if excess_km <= 0 or tariff.rate <= 0:
        return 0.0

    if tariff.per_km == 10000:
        return (excess_km / 10000.0) * tariff.rate
    return excess_km * tariff.rate


def overlap_days_in_year(start: date | None, end: date | None, year: int) -> int:
    if start is None or end is None or end < start:
        return 0

    from_day = max(start, date(year, 1, 1))
    to_day = min(end, date(year, 12, 31))
    if to_day < from_day:
        return 0
    return (to_day - from_day).days + 1


def read_template(path: Path) -> list[TemplateRow]:
    rows = read_sheet_rows(path)
    template: list[TemplateRow] = []

    for idx, row in enumerate(rows, start=1):
        if idx < 3:
            continue

        customer = str(row.get(1, "")).strip()
        customer_contract = normalize_customer_contract(row.get(5, ""))
        internal_contracts = parse_internal_contracts(row.get(6, ""))
        free_km = parse_float(row.get(7, "")) or 0.0
        tariff_raw = str(row.get(8, "")).strip()
        tariff = parse_tariff(tariff_raw)
        is_fleet = str(row.get(10, "")).strip().lower() == "x"
        is_wagon = str(row.get(11, "")).strip().lower() == "x"

        if not customer or not internal_contracts or (not is_fleet and not is_wagon):
            continue

        template.append(
            TemplateRow(
                row_no=idx,
                customer=customer,
                customer_contract=customer_contract,
                internal_contracts=internal_contracts,
                free_km=free_km,
                tariff=tariff,
                tariff_raw=tariff_raw,
                is_fleet=is_fleet,
                is_wagon=is_wagon,
            )
        )

    return template


def read_kilometer(path: Path, year: int) -> dict[str, list[WagonUsage]]:
    rows = read_sheet_rows(path, sheet_name="Tabelle1")
    if not rows:
        return {}

    header_row = rows[0]
    headers = {col: str(value).strip() for col, value in header_row.items()}

    by_contract: dict[str, list[WagonUsage]] = {}

    for row in rows[1:]:
        def get_any(*header_names: str) -> str:
            for header_name in header_names:
                for col, name in headers.items():
                    if name == header_name:
                        return str(row.get(col, ""))
            return ""

        contract_raw = get_any("AGNB")
        contract_ids = parse_internal_contracts(contract_raw)
        contract = contract_ids[0] if contract_ids else ""
        if not contract:
            continue

        wagon_no = str(get_any("BANO") or "").strip()
        if not wagon_no:
            wagon_no = f"{contract}-unbekannt"

        # SQL-Herkunft: START = erster KM >= CalcStartDate, END_KM = letzter KM <= CalcEndDate
        start_km_raw = parse_float(get_any("START"))
        end_km_raw = parse_float(get_any("ENDE_KM", "END_KM"))
        if end_km_raw is None:
            end_km_raw = parse_float(get_any("ENDEJAHR", "ENDJAHR"))

        km_valid = (
            start_km_raw is not None
            and end_km_raw is not None
            and end_km_raw >= start_km_raw
        )
        if km_valid:
            start_km = float(start_km_raw)
            end_km = float(end_km_raw)
            km_2025 = end_km - start_km
        else:
            # Wagen ohne gueltige KM bleiben sichtbar, gehen aber mit 0/0/0 in die Abrechnung.
            start_km = 0.0
            end_km = 0.0
            km_2025 = 0.0

        bill_start = (
            parse_yyyymmdd(get_any("BILL_START", "CalcStartDate", "CALCSTARTDATE"))
            or parse_yyyymmdd(get_any("START_ZEIT", "STARTZEIT"))
        )
        bill_end = (
            parse_yyyymmdd(get_any("BILL_END", "CalcEndDate", "CALCENDDATE"))
            or parse_yyyymmdd(get_any("ENDE_ZEIT", "END_ZEIT", "ENDEZEIT", "ENDZEIT"))
        )
        days_2025 = overlap_days_in_year(bill_start, bill_end, year) if km_valid else 0

        usage = WagonUsage(
            contract=contract,
            wagon_no=wagon_no,
            bill_start=bill_start,
            bill_end=bill_end,
            start_km=start_km,
            end_km=end_km,
            days_2025=days_2025,
            km_2025=km_2025,
            km_valid=km_valid,
        )
        by_contract.setdefault(contract, []).append(usage)

    return by_contract


def build_overview(
    template_rows: list[TemplateRow],
    kilometer_by_contract: dict[str, list[WagonUsage]],
    year: int,
) -> tuple[list[list[object]], list[list[object]]]:
    overview: list[list[object]] = [
        [
            "Kunde",
            "KundenVertragsnummer",
            "M3 Vertragnummer",
            "Abrechnung",
            "Anzahl Wagen",
            f"Anzahl Kilometer {year}",
            f"Freikilometer {year}",
            "Mehrkilometer",
            "Tarif",
            "Betrag",
        ]
    ]

    details: list[list[object]] = [
        [
            "Kunde",
            "Abrechnung",
            "KundenVertragsnummer",
            "M3 Vertragnummer",
            "Wagennummer",
            "Bill Start",
            "Bill End",
            f"Tage {year}",
            f"Kilometer {year}",
            f"Freikilometer {year}",
            "Tarif",
            "Betrag",
            "_Mileage from",
            "_Mileage to",
            "_KM Valid",
        ]
    ]

    fleet_processed_customers: set[str] = set()

    for tpl in template_rows:
        if tpl.is_fleet:
            if tpl.customer in fleet_processed_customers:
                continue

            customer_fleet_rows = [
                row for row in template_rows if row.customer == tpl.customer and row.is_fleet
            ]
            fleet_processed_customers.add(tpl.customer)

            contract_to_row: dict[str, TemplateRow] = {}
            ordered_contracts: list[str] = []
            for row in customer_fleet_rows:
                for contract in row.internal_contracts:
                    if contract not in contract_to_row:
                        contract_to_row[contract] = row
                        ordered_contracts.append(contract)

            contracts_with_data: list[str] = [
                c for c in ordered_contracts if kilometer_by_contract.get(c)
            ]
            if not contracts_with_data:
                continue

            wagon_keys: set[str] = set()
            km_total = 0.0
            free_total = 0.0
            contract_km: dict[str, float] = {}
            contract_tariff: dict[str, Tariff] = {}
            customer_contracts: list[str] = []

            for contract in contracts_with_data:
                row = contract_to_row[contract]
                if row.customer_contract and row.customer_contract not in customer_contracts:
                    customer_contracts.append(row.customer_contract)

                contract_tariff[contract] = row.tariff
                usages = kilometer_by_contract.get(contract, [])
                km_contract = 0.0

                for usage in usages:
                    wagon_keys.add(usage.wagon_no)
                    km_total += usage.km_2025
                    km_contract += usage.km_2025

                    free_for_wagon = row.free_km * (usage.days_2025 / 365.0)
                    free_total += free_for_wagon

                    details.append(
                        [
                            tpl.customer,
                            "Flotte (kundenweit)",
                            row.customer_contract,
                            contract,
                            usage.wagon_no,
                            format_date(usage.bill_start),
                            format_date(usage.bill_end),
                            usage.days_2025,
                            round(usage.km_2025, 6),
                            round(free_for_wagon, 12),
                            tariff_display(row.tariff),
                            0.0,
                            round(usage.start_km, 6),
                            round(usage.end_km, 6),
                            1 if usage.km_valid else 0,
                        ]
                    )

                contract_km[contract] = km_contract

            excess = max(0.0, km_total - free_total)

            unique_tariffs: dict[tuple[int, int], Tariff] = {}
            for contract in contracts_with_data:
                tariff = contract_tariff[contract]
                unique_tariffs[tariff_key(tariff)] = tariff

            if len(unique_tariffs) == 1:
                only_tariff = next(iter(unique_tariffs.values()))
                amount = calc_amount(excess, only_tariff)
                tariff_text = tariff_display(only_tariff)
            else:
                amount = 0.0
                km_sum_for_alloc = sum(contract_km.values())
                if excess > 0 and km_sum_for_alloc > 0:
                    for contract, km_value in contract_km.items():
                        allocated_excess = excess * (km_value / km_sum_for_alloc)
                        amount += calc_amount(allocated_excess, contract_tariff[contract])
                tariff_text = "gemischt"

            contracts_display = sorted(contracts_with_data)
            customer_contracts_display = sorted(customer_contracts)

            overview.append(
                [
                    tpl.customer,
                    " + ".join(customer_contracts_display),
                    " + ".join(contracts_display),
                    "Flotte (kundenweit)",
                    len(wagon_keys),
                    round(km_total, 6),
                    round(free_total, 12),
                    round(excess, 12),
                    tariff_text,
                    round(amount, 12),
                ]
            )

        if tpl.is_wagon:
            contracts_with_data = [
                contract for contract in tpl.internal_contracts if kilometer_by_contract.get(contract)
            ]
            if not contracts_with_data:
                continue

            for contract in contracts_with_data:
                usages = kilometer_by_contract.get(contract, [])
                if not usages:
                    continue

                wagon_totals: dict[str, dict[str, float]] = {}
                for usage in usages:
                    free_for_wagon = tpl.free_km * (usage.days_2025 / 365.0)
                    bucket = wagon_totals.setdefault(
                        usage.wagon_no, {"km": 0.0, "free": 0.0}
                    )
                    bucket["km"] += usage.km_2025
                    bucket["free"] += free_for_wagon

                    details.append(
                        [
                            tpl.customer,
                            "Wagen",
                            tpl.customer_contract,
                            contract,
                            usage.wagon_no,
                            format_date(usage.bill_start),
                            format_date(usage.bill_end),
                            usage.days_2025,
                            round(usage.km_2025, 6),
                            round(free_for_wagon, 12),
                            tariff_display(tpl.tariff),
                            round(
                                calc_amount(
                                    max(0.0, usage.km_2025 - free_for_wagon),
                                    tpl.tariff,
                                ),
                                12,
                            ),
                            round(usage.start_km, 6),
                            round(usage.end_km, 6),
                            1 if usage.km_valid else 0,
                        ]
                    )

                km_total = sum(item["km"] for item in wagon_totals.values())
                free_total = sum(item["free"] for item in wagon_totals.values())
                # Wagenlogik: Freikilometer gelten je Wagenposition.
                excess = 0.0
                amount = 0.0
                for item in wagon_totals.values():
                    wagon_excess = max(0.0, item["km"] - item["free"])
                    excess += wagon_excess
                    amount += calc_amount(wagon_excess, tpl.tariff)

                overview.append(
                    [
                        tpl.customer,
                        tpl.customer_contract,
                        contract,
                        "Wagen",
                        len(wagon_totals),
                        round(km_total, 6),
                        round(free_total, 12),
                        round(excess, 12),
                        tariff_display(tpl.tariff),
                        round(amount, 12),
                    ]
                )

    return overview, details


def split_plus_list(text: str) -> list[str]:
    return [item.strip() for item in str(text).split(" + ") if item.strip()]


def parse_iso_date_to_ddmmyyyy(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    try:
        return datetime.strptime(raw, "%Y-%m-%d").strftime("%d/%m/%Y")
    except ValueError:
        return raw


def parse_tariff_display_value(tariff_text: str) -> str:
    text = str(tariff_text or "").strip().lower()
    if not text:
        return "0"
    if text == "gemischt":
        return "gemischt"
    match = re.search(r"[-+]?\d+(?:[.,]\d+)?", text)
    if not match:
        return str(tariff_text)
    return match.group(0)


def infer_base_free_km(detail_rows: list[list[object]]) -> str:
    inferred: list[int] = []
    for row in detail_rows:
        days = int(float(row[7]))
        free = float(row[9])
        if days <= 0:
            continue
        base = round((free * 365.0) / days)
        inferred.append(base)

    if not inferred:
        return "0"

    unique = sorted(set(inferred))
    if len(unique) == 1:
        return str(unique[0])
    return "gemischt"


def make_unique_sheet_name(raw: str, used: set[str]) -> str:
    cleaned = re.sub(r"[\[\]\*\?/\\:]", "_", raw).strip()
    if not cleaned:
        cleaned = "Abrechnung"
    base = cleaned[:31]
    if base not in used:
        used.add(base)
        return base

    counter = 2
    while True:
        suffix = f"_{counter}"
        candidate = f"{base[: 31 - len(suffix)]}{suffix}"
        if candidate not in used:
            used.add(candidate)
            return candidate
        counter += 1


@dataclass
class SheetSpec:
    name: str
    data: list[list[object]]
    kind: str
    auto_filter: str | None = None
    tab_color: str | None = None
    highlight_rows: set[int] | None = None


def build_detail_workbook_sheets(
    overview: list[list[object]],
    details: list[list[object]],
    year: int,
) -> list[SheetSpec]:
    detail_rows = details[1:]
    prepared: list[
        tuple[tuple[int, float, str, str], str, float, list[list[object]], set[int]]
    ] = []
    used_sheet_names: set[str] = set()

    for ov in overview[1:]:
        customer = str(ov[0])
        customer_contracts = str(ov[1])
        contract_display = str(ov[2])
        billing_mode = str(ov[3])
        km_total = float(ov[5])
        free_total = float(ov[6])
        excess_total = float(ov[7])
        tariff_text = str(ov[8])
        billed_amount = float(ov[9])

        contracts = set(split_plus_list(contract_display))
        if not contracts:
            contracts = {contract_display}

        if billing_mode == "Flotte (kundenweit)":
            matched = [
                row for row in detail_rows
                if str(row[0]) == customer
                and str(row[1]) == "Flotte (kundenweit)"
                and str(row[3]) in contracts
            ]
        else:
            matched = [
                row for row in detail_rows
                if str(row[0]) == customer
                and str(row[1]) == "Wagen"
                and str(row[3]) in contracts
                and str(row[2]) == customer_contracts
            ]

        matched.sort(key=lambda row: (str(row[2]), str(row[4])))

        agreed_km = infer_base_free_km(matched)
        tariff_value = parse_tariff_display_value(tariff_text)

        def blank_row() -> list[object]:
            return [""] * 12

        # Kopfbereich in B2:L7, Datenkopf in Zeile 8 (B-L)
        sheet_rows: list[list[object]] = [blank_row()]
        row2 = blank_row()
        row2[1] = "Agreement"
        row2[2] = f"{contract_display} / {customer_contracts}"
        sheet_rows.append(row2)

        sheet_rows.append(blank_row())

        row4 = blank_row()
        row4[1] = "agreed milage"
        row4[2] = agreed_km
        row4[6] = "Exceeding mileage from"
        row4[7] = agreed_km
        sheet_rows.append(row4)

        sheet_rows.append(blank_row())

        row6 = blank_row()
        row6[1] = "Billing type"
        row6[6] = "amount €/exceeding km"
        row6[7] = tariff_value
        sheet_rows.append(row6)

        row7 = blank_row()
        row7[1] = "per Fleet" if billing_mode == "Flotte (kundenweit)" else "per Wagon"
        sheet_rows.append(row7)

        row8 = blank_row()
        row8[1] = "Customer Contract"
        row8[2] = "Internal Contract"
        row8[3] = "Wagon"
        row8[4] = "Date from"
        row8[5] = "Mileage from"
        row8[6] = "Date to"
        row8[7] = "Mileage to"
        row8[8] = "running km"
        row8[9] = "annual km pro rata"
        row8[10] = "exceeding km"
        row8[11] = "Amount"
        sheet_rows.append(row8)

        highlight_rows: set[int] = set()
        for row in matched:
            km_valid = bool(int(float(row[14]))) if len(row) > 14 else True
            mileage_from = int(round(float(row[12]))) if len(row) > 12 else 0
            mileage_to = int(round(float(row[13]))) if len(row) > 13 else 0
            if not km_valid:
                mileage_from = 0
                mileage_to = 0
            running_km = float(row[8])
            annual_pro_rata = float(row[9])
            exceeding = running_km - annual_pro_rata
            amount_row = float(row[11]) if len(row) > 11 else 0.0
            excel_row = len(sheet_rows) + 1
            data_row = blank_row()
            data_row[1] = str(row[2])
            data_row[2] = str(row[3])
            data_row[3] = str(row[4])
            data_row[4] = parse_iso_date_to_ddmmyyyy(str(row[5]))
            data_row[5] = mileage_from
            data_row[6] = parse_iso_date_to_ddmmyyyy(str(row[6]))
            data_row[7] = mileage_to
            data_row[8] = round(running_km, 6)
            data_row[9] = round(annual_pro_rata, 6)
            data_row[10] = round(exceeding, 6)
            data_row[11] = round(amount_row, 6)
            sheet_rows.append(data_row)
            if not km_valid:
                highlight_rows.add(excel_row)

        sheet_rows.append(blank_row())
        total_row = blank_row()
        total_row[1] = "TOTAL"
        total_row[8] = round(km_total, 6)
        total_row[9] = round(free_total, 6)
        total_row[10] = round(excess_total, 6)
        total_row[11] = round(billed_amount, 6)
        sheet_rows.append(total_row)

        sort_key = (
            0 if billed_amount > 0 else 1,
            -billed_amount if billed_amount > 0 else 0.0,
            customer,
            contract_display,
        )
        raw_name = f"{customer}_{contract_display}"
        prepared.append((sort_key, raw_name, billed_amount, sheet_rows, highlight_rows))

    prepared.sort(key=lambda item: item[0])
    sheets: list[SheetSpec] = []
    for index, (_, raw_name, billed_amount, sheet_rows, row_highlights) in enumerate(prepared, start=1):
        sheet_name = make_unique_sheet_name(f"{index:03d}_{raw_name}", used_sheet_names)
        tab_color = "FFF4CCCC" if billed_amount > 0 else "FFD9EAD3"
        sheets.append(
            SheetSpec(
                name=sheet_name,
                data=sheet_rows,
                kind="detail",
                auto_filter="B8:L8",
                tab_color=tab_color,
                highlight_rows=row_highlights,
            )
        )
    return sheets


def excel_number(value: float) -> str:
    if abs(value - round(value)) < 1e-12:
        return str(int(round(value)))
    return f"{value:.15f}".rstrip("0").rstrip(".")


def style_for_cell(sheet: SheetSpec, row: int, col: int) -> int:
    if sheet.kind == "overview":
        if row == 1:
            return 1
        if col in (5, 6, 7, 8):
            return 4
        if col == 10:
            return 5
        return 0

    if sheet.kind == "wagendetails":
        if row == 1:
            return 1
        if col in (8, 9, 10):
            return 4
        if col == 12:
            return 5
        return 0

    if sheet.kind == "detail":
        if row == 8 and 2 <= col <= 12:
            return 1

        if 2 <= row <= 7 and 2 <= col <= 12:
            if (row, col) in {(4, 3), (4, 8), (6, 8)}:
                return 3
            return 2

        if row >= 9:
            if sheet.highlight_rows and row in sheet.highlight_rows:
                if col in (6, 8, 9, 10, 11):
                    return 7
                if col == 12:
                    return 8
                if 2 <= col <= 12:
                    return 6
            if col in (6, 8, 9, 10, 11):
                return 4
            if col == 12:
                return 5
        return 0

    return 0


def xml_cell(col: int, row: int, value: object, style_id: int) -> str:
    ref = f"{index_to_col(col)}{row}"

    style_attr = f' s="{style_id}"' if style_id > 0 else ""

    if isinstance(value, (int, float)):
        return f'<c r="{ref}"{style_attr}><v>{excel_number(float(value))}</v></c>'

    text = escape(str(value))
    return f'<c r="{ref}"{style_attr} t="inlineStr"><is><t>{text}</t></is></c>'


def worksheet_xml(sheet: SheetSpec) -> str:
    xml_rows: list[str] = []
    for row_index, values in enumerate(sheet.data, start=1):
        cells = "".join(
            xml_cell(
                col_index,
                row_index,
                value,
                style_for_cell(sheet, row_index, col_index),
            )
            for col_index, value in enumerate(values, start=1)
        )
        xml_rows.append(f'<row r="{row_index}">{cells}</row>')

    body = "".join(xml_rows)
    tab_color_xml = (
        f'<sheetPr><tabColor rgb="{sheet.tab_color}"/></sheetPr>'
        if sheet.tab_color
        else ""
    )
    auto_filter_xml = (
        f'<autoFilter ref="{sheet.auto_filter}"/>'
        if sheet.auto_filter
        else ""
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<worksheet xmlns="{MAIN_NS}">'
        f"{tab_color_xml}"
        f"<sheetData>{body}</sheetData>"
        f"{auto_filter_xml}"
        "</worksheet>"
    )


def write_xlsx(output_file: Path, sheets: list[SheetSpec]) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    worksheets_xml = [worksheet_xml(sheet) for sheet in sheets]

    sheet_overrides = "\n".join(
        f'  <Override PartName="/xl/worksheets/sheet{i}.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for i in range(1, len(sheets) + 1)
    )

    content_types = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
        "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">\n"
        "  <Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>\n"
        "  <Default Extension=\"xml\" ContentType=\"application/xml\"/>\n"
        "  <Override PartName=\"/xl/workbook.xml\" "
        "ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>\n"
        f"{sheet_overrides}\n"
        "  <Override PartName=\"/xl/styles.xml\" "
        "ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/>\n"
        "  <Override PartName=\"/docProps/core.xml\" "
        "ContentType=\"application/vnd.openxmlformats-package.core-properties+xml\"/>\n"
        "  <Override PartName=\"/docProps/app.xml\" "
        "ContentType=\"application/vnd.openxmlformats-officedocument.extended-properties+xml\"/>\n"
        "</Types>"
    )

    rels = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
        "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">\n"
        "  <Relationship Id=\"rId1\" "
        "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" "
        "Target=\"xl/workbook.xml\"/>\n"
        "  <Relationship Id=\"rId2\" "
        "Type=\"http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties\" "
        "Target=\"docProps/core.xml\"/>\n"
        "  <Relationship Id=\"rId3\" "
        "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties\" "
        "Target=\"docProps/app.xml\"/>\n"
        "</Relationships>"
    )

    sheet_entries = "\n".join(
        f'    <sheet name="{escape(sheet.name[:31])}" sheetId="{i}" r:id="rId{i}"/>'
        for i, sheet in enumerate(sheets, start=1)
    )
    workbook_xml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
        f"<workbook xmlns=\"{MAIN_NS}\" xmlns:r=\"{REL_NS}\">\n"
        "  <sheets>\n"
        f"{sheet_entries}\n"
        "  </sheets>\n"
        "</workbook>"
    )

    wb_rels_entries = "\n".join(
        f'  <Relationship Id="rId{i}" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        f'Target="worksheets/sheet{i}.xml"/>'
        for i in range(1, len(sheets) + 1)
    )
    workbook_rels = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
        "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">\n"
        f"{wb_rels_entries}\n"
        "  <Relationship Id=\"rId999\" "
        "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles\" "
        "Target=\"styles.xml\"/>\n"
        "</Relationships>"
    )

    styles = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
        f"<styleSheet xmlns=\"{MAIN_NS}\">\n"
        "  <fonts count=\"2\">"
        "<font><sz val=\"11\"/><name val=\"Arial\"/></font>"
        "<font><sz val=\"11\"/><name val=\"Arial\"/><b/></font>"
        "</fonts>\n"
        "  <fills count=\"6\">"
        "<fill><patternFill patternType=\"none\"/></fill>"
        "<fill><patternFill patternType=\"gray125\"/></fill>"
        "<fill><patternFill patternType=\"solid\"><fgColor rgb=\"FFF2F2F2\"/><bgColor indexed=\"64\"/></patternFill></fill>"
        "<fill><patternFill patternType=\"solid\"><fgColor rgb=\"FFFFFFFF\"/><bgColor indexed=\"64\"/></patternFill></fill>"
        "<fill><patternFill patternType=\"solid\"><fgColor rgb=\"FFD9D9D9\"/><bgColor indexed=\"64\"/></patternFill></fill>"
        "<fill><patternFill patternType=\"solid\"><fgColor rgb=\"FFFFF2F2\"/><bgColor indexed=\"64\"/></patternFill></fill>"
        "</fills>\n"
        "  <borders count=\"1\"><border/></borders>\n"
        "  <cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>\n"
        "  <cellXfs count=\"9\">"
        "<xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/>"
        "<xf numFmtId=\"0\" fontId=\"1\" fillId=\"4\" borderId=\"0\" xfId=\"0\" applyFont=\"1\" applyFill=\"1\"/>"
        "<xf numFmtId=\"0\" fontId=\"0\" fillId=\"2\" borderId=\"0\" xfId=\"0\" applyFill=\"1\"/>"
        "<xf numFmtId=\"0\" fontId=\"0\" fillId=\"3\" borderId=\"0\" xfId=\"0\" applyFill=\"1\"/>"
        "<xf numFmtId=\"1\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyNumberFormat=\"1\"/>"
        "<xf numFmtId=\"2\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyNumberFormat=\"1\"/>"
        "<xf numFmtId=\"0\" fontId=\"0\" fillId=\"5\" borderId=\"0\" xfId=\"0\" applyFill=\"1\"/>"
        "<xf numFmtId=\"1\" fontId=\"0\" fillId=\"5\" borderId=\"0\" xfId=\"0\" applyFill=\"1\" applyNumberFormat=\"1\"/>"
        "<xf numFmtId=\"2\" fontId=\"0\" fillId=\"5\" borderId=\"0\" xfId=\"0\" applyFill=\"1\" applyNumberFormat=\"1\"/>"
        "</cellXfs>\n"
        "  <cellStyles count=\"1\"><cellStyle name=\"Normal\" xfId=\"0\" builtinId=\"0\"/></cellStyles>\n"
        "</styleSheet>"
    )

    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    core = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
        "<cp:coreProperties xmlns:cp=\"http://schemas.openxmlformats.org/package/2006/metadata/core-properties\"\n"
        " xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
        " xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
        " xmlns:dcmitype=\"http://purl.org/dc/dcmitype/\"\n"
        " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
        "  <dc:creator>Jahresabrechnung Script</dc:creator>\n"
        "  <cp:lastModifiedBy>Jahresabrechnung Script</cp:lastModifiedBy>\n"
        f"  <dcterms:created xsi:type=\"dcterms:W3CDTF\">{timestamp}</dcterms:created>\n"
        f"  <dcterms:modified xsi:type=\"dcterms:W3CDTF\">{timestamp}</dcterms:modified>\n"
        "</cp:coreProperties>"
    )

    app = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
        "<Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\"\n"
        " xmlns:vt=\"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes\">\n"
        "  <Application>Python</Application>\n"
        "</Properties>"
    )

    with zipfile.ZipFile(output_file, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", rels)
        archive.writestr("docProps/core.xml", core)
        archive.writestr("docProps/app.xml", app)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        archive.writestr("xl/styles.xml", styles)
        for index, xml in enumerate(worksheets_xml, start=1):
            archive.writestr(f"xl/worksheets/sheet{index}.xml", xml)


def main() -> int:
    args = parse_args()

    if not args.vorlage.exists():
        raise FileNotFoundError(f"Vorlage nicht gefunden: {args.vorlage}")
    if not args.kilometer.exists():
        raise FileNotFoundError(f"Kilometerdatei nicht gefunden: {args.kilometer}")

    template_rows = read_template(args.vorlage)
    kilometer_data = read_kilometer(args.kilometer, args.jahr)
    overview, details = build_overview(template_rows, kilometer_data, args.jahr)
    abrechnungs_sheets = build_detail_workbook_sheets(overview, details, args.jahr)
    details_export = [row[:12] for row in details]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = args.output / f"vertragsuebersicht_{args.jahr}_{timestamp}.xlsx"
    einzel_file = args.output / f"einzelabrechnungen_detail_{args.jahr}_{timestamp}.xlsx"

    write_xlsx(
        output_file,
        [
            SheetSpec(
                name="Vertragsuebersicht",
                data=overview,
                kind="overview",
                auto_filter="A1:J1",
            ),
            SheetSpec(
                name="Wagendetails",
                data=details_export,
                kind="wagendetails",
                auto_filter="A1:L1",
            ),
        ],
    )
    write_xlsx(
        einzel_file,
        abrechnungs_sheets,
    )

    print(f"Datei erstellt: {output_file}")
    print(f"Datei erstellt: {einzel_file}")
    print(f"Uebersichtszeilen (ohne Header): {len(overview) - 1}")
    print(f"Wagendetails (ohne Header): {len(details_export) - 1}")
    print(f"Abrechnungssheets: {len(abrechnungs_sheets)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
