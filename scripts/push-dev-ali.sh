#!/usr/bin/env bash
set -euo pipefail

TARGET_BRANCH="dev/ali"
REMOTE="origin"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Fehler: Kein Git-Repository."
  exit 1
fi

current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$current_branch" != "$TARGET_BRANCH" ]]; then
  echo "Fehler: Aktueller Branch ist '$current_branch' (erwartet '$TARGET_BRANCH')."
  echo "Wechsle zuerst mit: git switch $TARGET_BRANCH"
  exit 1
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "Fehler: Uncommitted Änderungen vorhanden. Bitte zuerst committen."
  git status --short
  exit 1
fi

echo "Push nach $REMOTE/$TARGET_BRANCH ..."
git push -u "$REMOTE" "$TARGET_BRANCH"
echo "Push abgeschlossen."
