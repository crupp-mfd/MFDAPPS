#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: ./scripts/codex.sh {commit|push|deploy} [args]

Examples:
  ./scripts/codex.sh commit "feat: add portal"
  ./scripts/codex.sh push
  ./scripts/codex.sh deploy
EOF
}

cmd="${1:-}"
shift || true

case "${cmd}" in
  commit)
    /Users/crupp/dev/MFDAPPS/scripts/git-commit.sh "$@"
    ;;
  push)
    /Users/crupp/dev/MFDAPPS/scripts/git-push.sh "$@"
    ;;
  deploy)
    /Users/crupp/dev/MFDAPPS/scripts/deploy-main.sh "$@"
    ;;
  *)
    usage
    exit 1
    ;;
esac
