#!/usr/bin/env bash
set -euo pipefail

MESSAGE="${1:-chore: update $(date '+%Y-%m-%d %H:%M:%S')}"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Fehler: Kein Git-Repository."
  exit 1
fi

if [[ -z "$(git status --porcelain)" ]]; then
  echo "Keine Aenderungen zum Commit."
  exit 0
fi

git add -A
git commit -m "${MESSAGE}"

echo "Commit erstellt: ${MESSAGE}"
