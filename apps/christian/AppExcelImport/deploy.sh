#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
APP_NAME="${APP_NAME:-appexcelimport-api}"
RESOURCE_GROUP="${RESOURCE_GROUP:-rg-mfd-automation}"
ACR_NAME="${ACR_NAME:-acrmfdauto10028}"
IMAGE_REPO="${IMAGE_REPO:-appexcelimport}"
TAG="${TAG:-$(date +%Y%m%d-%H%M%S)}"
DOCKERFILE="$ROOT_DIR/apps/christian/AppExcelImport/Dockerfile"

az acr build -r "$ACR_NAME" -t "$IMAGE_REPO:$TAG" -f "$DOCKERFILE" "$ROOT_DIR"
az containerapp update -n "$APP_NAME" -g "$RESOURCE_GROUP" --image "$ACR_NAME.azurecr.io/$IMAGE_REPO:$TAG" --revision-suffix "r$(date +%H%M%S)"

echo "Deployed $APP_NAME with image $ACR_NAME.azurecr.io/$IMAGE_REPO:$TAG"
