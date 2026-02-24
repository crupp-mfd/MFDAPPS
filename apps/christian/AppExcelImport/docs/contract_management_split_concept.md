# Contract Management Split Konzept

## Ziel

Das Sheet `CM` wird beim GO-Import in mehrere fachliche Tabellen zerlegt:

- `landing.ContractManagement_Header`
- `landing.ContractManagement_Batch`
- `landing.ContractManagement_Wagon`
- `landing.ContractManagement_Structure` (Reihenfolge + Wiederaufbau)
- `landing.ContractManagement_raw` (technischer Rohimport)

## Aufteilungsregel

`row_type` (Spalte G im CM-Sheet) steuert die Ziel-Tabelle:

- `Header` -> `ContractManagement_Header`
- `Batch` -> `ContractManagement_Batch`
- `Wagon` -> `ContractManagement_Wagon`

Leere Zeilen werden bereits im Reader verworfen. Summenzeilen (`Total`, `Sum`, `Gesamt`) werden fuer `CM` gefiltert.

## Spaltenstrategie

Pro Datentyp-Tabelle werden nur Spalten angelegt/verwendet, die fuer den jeweiligen `row_type` nicht leer sind.
Zusatzregeln:

- Key-Spalten bleiben immer enthalten:
  - `contract_number_m3`
  - `contract_number_customer`
  - `rentalposition_m3`
  - `remarks`
  - `row_type`
  - `wagon_number`
  - `customer`
- `row_json` bleibt pro Datensatz gespeichert (vollstaendiger Zeilen-Payload).
- Neue Spalten, die spaeter auftauchen, werden per `ALTER TABLE` automatisch ergaenzt.

## Delta-Logik

Je Datentyp-Tabelle gilt SCD2-aehnliche Historisierung:

- unveraendert: `last_seen_import_utc` wird aktualisiert
- geaendert: alter Datensatz wird geschlossen (`valid_to_utc`), neuer Datensatz wird aktiv (`is_current = 1`)

Business-Key basiert auf:

- `contract_number_m3`
- `contract_number_customer`
- `rentalposition_m3`
- `remarks`
- `row_type`
- `wagon_number`
- `customer`

## Structure-Tabelle

`ContractManagement_Structure` enthaelt je importierter CM-Zeile:

- `batch_id`, `import_timestamp_utc`
- `row_sequence` (exakte Reihenfolge im Sheet)
- `row_type`, `entity_table`
- Hashes (`business_key_hash`, `record_hash`)
- `row_json` (voller Zeileninhalt)

Damit kann ein Snapshot immer wieder in Originalreihenfolge zusammengebaut werden.

## Excel-Export (Stichtag)

1. Stichtag bestimmen (`snapshot_at_utc` oder letzter Import).
2. Passenden Batch aus `ContractManagement_Structure` laden.
3. Zeilen in Reihenfolge (`row_sequence`) aus `row_json` rekonstruieren.
4. Wenn die importierte Quelldatei noch lokal vorhanden ist:
   - diese als Vorlage laden
   - Datenbereich neu schreiben
   - Zeilen-Styles je `row_type` (`header/batch/wagon`) uebernehmen
5. Fallback ohne Vorlage: generische Export-Excel mit Header + Daten.

So bleibt die Formatierung so nah wie moeglich am Original, inklusive Row-Type-Styling.
