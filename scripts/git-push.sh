#!/usr/bin/env bash
set -euo pipefail

REMOTE="${REMOTE:-origin}"
BRANCH="${1:-$(git rev-parse --abbrev-ref HEAD)}"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Fehler: Kein Git-Repository."
  exit 1
fi

if [[ -z "${BRANCH}" || "${BRANCH}" == "HEAD" ]]; then
  echo "Fehler: Konnte aktuellen Branch nicht bestimmen."
  exit 1
fi

git push -u "${REMOTE}" "${BRANCH}"
echo "Push abgeschlossen: ${REMOTE}/${BRANCH}"
