# Jahresabrechnung Gueterwagen

Projektstruktur:

- `Quellen/`: Excel-Eingabedateien
- `Script/`: Python-Skript zur Abrechnung
- `Output/`: erzeugte Excel-Dateien

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
```

## Ausfuehrung

```bash
python Script/jahresabrechnung.py --jahr 2025
```

Optional:

- `--vorlage <pfad>`: Pfad zu `KM_Vorlage 2025.xlsx`
- `--kilometer <pfad>`: Pfad zu `KILOMETER.xlsx` (Sheet `Tabelle1`)
- `--output <pfad>`: Zielordner
- `--jahr <jahr>`: Abrechnungsjahr

## Ergebnis

Das Skript erzeugt zwei Excel-Dateien:

- `vertragsuebersicht_...xlsx` mit:
  - `Vertragsuebersicht`: Vertrags-/Kundenaggregation
  - `Wagendetails`: Einzelwagen zur Kontrolle
- `einzelabrechnungen_detail_...xlsx` mit:
  - einem Sheet je Zeile aus der `Vertragsuebersicht` im Detail-Abrechnungsstil
