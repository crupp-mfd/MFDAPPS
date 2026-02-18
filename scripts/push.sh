#!/usr/bin/env bash
set -euo pipefail

BRANCH="dev/christian"
REMOTE="${REMOTE:-origin}"
MESSAGE="${1:-chore: update $(date '+%Y-%m-%d %H:%M:%S')}"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Fehler: Kein Git-Repository."
  exit 1
fi

if git show-ref --verify --quiet "refs/heads/${BRANCH}"; then
  git switch "${BRANCH}"
elif git show-ref --verify --quiet "refs/remotes/${REMOTE}/${BRANCH}"; then
  git switch -c "${BRANCH}" --track "${REMOTE}/${BRANCH}"
else
  git switch -c "${BRANCH}"
fi

if [[ -n "$(git status --porcelain)" ]]; then
  git add -A
  git commit -m "${MESSAGE}"
else
  echo "Keine lokalen Änderungen zum Commit."
fi

git push -u "${REMOTE}" "${BRANCH}"
echo "Push abgeschlossen: ${REMOTE}/${BRANCH}"
