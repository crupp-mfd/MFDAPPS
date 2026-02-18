#!/usr/bin/env bash
set -euo pipefail

APP_NAME="${APP_NAME:-mfd-automation}"
RESOURCE_GROUP="${RESOURCE_GROUP:-rg-mfd-automation}"
ACR_NAME="${ACR_NAME:-acrmfdauto10028}"
IMAGE_REPO="${IMAGE_REPO:-appmfd}"
BRANCH_REQUIRED="${BRANCH_REQUIRED:-main}"

if ! command -v az >/dev/null 2>&1; then
  echo "Fehler: Azure CLI (az) ist nicht installiert."
  exit 1
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Fehler: Kein Git-Repository."
  exit 1
fi

current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "${current_branch}" != "${BRANCH_REQUIRED}" ]]; then
  echo "Fehler: Du bist auf '${current_branch}'. Deploy ist nur von '${BRANCH_REQUIRED}' erlaubt."
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Fehler: Working tree ist nicht clean. Bitte committe/stashe zuerst."
  exit 1
fi

az account show >/dev/null

timestamp="$(date +%Y%m%d-%H%M%S)"
short_sha="$(git rev-parse --short HEAD)"
image_tag="${timestamp}-${short_sha}"
image_ref="${ACR_NAME}.azurecr.io/${IMAGE_REPO}:${image_tag}"
revision_suffix="r$(date +%H%M%S)"

printf "Build image: %s\n" "${image_ref}"
az acr build -r "${ACR_NAME}" -t "${IMAGE_REPO}:${image_tag}" .

printf "Update container app: %s\n" "${APP_NAME}"
az containerapp update \
  -n "${APP_NAME}" \
  -g "${RESOURCE_GROUP}" \
  --image "${image_ref}" \
  --revision-suffix "${revision_suffix}" >/dev/null

echo "Deploy gestartet."

echo "Status pruefen..."
az containerapp revision list -n "${APP_NAME}" -g "${RESOURCE_GROUP}" \
  --query "[0:3].{name:name,health:properties.healthState,running:properties.runningState,active:properties.active}" -o table

fqdn="$(az containerapp show -n "${APP_NAME}" -g "${RESOURCE_GROUP}" --query "properties.configuration.ingress.fqdn" -o tsv)"
if [[ -n "${fqdn}" ]]; then
  echo "URL: https://${fqdn}/"
fi

echo "Deploy abgeschlossen."
