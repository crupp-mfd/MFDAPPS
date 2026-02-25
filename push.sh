#!/usr/bin/env bash
set -euo pipefail

BRANCH="dev/timo"
REMOTE="origin"
COMMIT_MSG="${1:-}"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Fehler: Dieses Verzeichnis ist kein Git-Repository."
  exit 1
fi

# Sicherstellen, dass der Ziel-Branch existiert und aktiv ist.
if git show-ref --verify --quiet "refs/heads/$BRANCH"; then
  git switch "$BRANCH"
else
  git switch -c "$BRANCH"
fi

# Nur getrackte/geänderte Dateien stagen.
git add -u

# Commit nur dann erstellen, wenn gestagte Änderungen vorhanden sind.
if ! git diff --cached --quiet; then
  if [[ -z "$COMMIT_MSG" ]]; then
    COMMIT_MSG="chore: update"
  fi
  git commit -m "$COMMIT_MSG"
else
  echo "Keine gestagten Änderungen zum Committen."
fi

# Beim ersten Push upstream setzen, sonst normal pushen.
if git rev-parse --abbrev-ref --symbolic-full-name "@{u}" >/dev/null 2>&1; then
  git push
else
  git push -u "$REMOTE" "$BRANCH"
fi
