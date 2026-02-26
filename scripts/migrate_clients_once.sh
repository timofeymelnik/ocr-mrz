#!/usr/bin/env bash

set -euo pipefail

# One-shot CRM clients migration trigger.
# Purpose:
# - Collect client IDs from runtime storage.
# - Call GET /api/crm/clients/{client_id} once for each ID to trigger lazy
#   profile backfill/migration on the backend.
#
# Usage:
#   bash scripts/migrate_clients_once.sh
#   API_BASE="http://127.0.0.1:8000" RUNTIME_DIR="./runtime" bash scripts/migrate_clients_once.sh
#   DRY_RUN=1 bash scripts/migrate_clients_once.sh
#
# Remote one-time (SSH) example:
#   ssh user@host 'cd /path/to/ocr-mrz && bash scripts/migrate_clients_once.sh'

API_BASE="${API_BASE:-http://127.0.0.1:8000}"
RUNTIME_DIR="${RUNTIME_DIR:-./runtime}"
DRY_RUN="${DRY_RUN:-0}"
API_DISCOVERY="${API_DISCOVERY:-1}"

CRM_CLIENTS_DIR="${RUNTIME_DIR}/crm_clients"
CRM_STORE_DIR="${RUNTIME_DIR}/crm_store"

if ! command -v curl >/dev/null 2>&1; then
  echo "ERROR: curl is required." >&2
  exit 1
fi

HAS_JQ=1
if ! command -v jq >/dev/null 2>&1; then
  HAS_JQ=0
fi

if [[ ! -d "${RUNTIME_DIR}" ]]; then
  echo "ERROR: runtime dir not found: ${RUNTIME_DIR}" >&2
  exit 1
fi

TMP_IDS_FILE="$(mktemp)"
TMP_BODY_FILE="$(mktemp)"
trap 'rm -f "${TMP_IDS_FILE}" "${TMP_BODY_FILE}"' EXIT

# 1) Existing crm_clients files (filename == client_id)
if [[ -d "${CRM_CLIENTS_DIR}" ]]; then
  find "${CRM_CLIENTS_DIR}" -type f -name '*.json' -maxdepth 1 2>/dev/null \
    | sed -E 's#^.*/##; s#\.json$##' \
    | awk 'NF' >> "${TMP_IDS_FILE}" || true
fi

# 2) client_id references from crm_store docs
if [[ -d "${CRM_STORE_DIR}" ]]; then
  if [[ "${HAS_JQ}" == "1" ]]; then
    find "${CRM_STORE_DIR}" -type f -name '*.json' -maxdepth 1 2>/dev/null \
      -exec jq -r '.client_id // empty' {} + \
      | awk 'NF' >> "${TMP_IDS_FILE}" || true
  else
    # Fallback parser without jq: safely extract only `"client_id": "..."`.
    find "${CRM_STORE_DIR}" -type f -name '*.json' -maxdepth 1 2>/dev/null \
      | while IFS= read -r file; do
          tr -d '\n' < "${file}" \
            | grep -oE '"client_id"[[:space:]]*:[[:space:]]*"[^"]+"' \
            | sed -E 's/^"client_id"[[:space:]]*:[[:space:]]*"([^"]+)"$/\1/' \
            | awk 'NF'
        done >> "${TMP_IDS_FILE}" || true
  fi
fi

sort -u "${TMP_IDS_FILE}" \
  | awk 'length($0) <= 128 && $0 !~ /[[:space:]{}]/' \
  > "${TMP_IDS_FILE}.clean"
mv "${TMP_IDS_FILE}.clean" "${TMP_IDS_FILE}"

TOTAL_IDS="$(wc -l < "${TMP_IDS_FILE}" | tr -d ' ')"
if [[ "${TOTAL_IDS}" == "0" && "${API_DISCOVERY}" == "1" ]]; then
  DOCS_URL="${API_BASE}/api/crm/documents?limit=1000"
  if curl -sS --connect-timeout 5 --max-time 30 "${DOCS_URL}" > "${TMP_BODY_FILE}" 2>/dev/null; then
    if [[ "${HAS_JQ}" == "1" ]]; then
      jq -r '.items[]?.client_id // empty' "${TMP_BODY_FILE}" \
        | awk 'NF' >> "${TMP_IDS_FILE}" || true
    else
      tr -d '\n' < "${TMP_BODY_FILE}" \
        | grep -oE '"client_id"[[:space:]]*:[[:space:]]*"[^"]+"' \
        | sed -E 's/^"client_id"[[:space:]]*:[[:space:]]*"([^"]+)"$/\1/' \
        | awk 'NF' >> "${TMP_IDS_FILE}" || true
    fi
    sort -u "${TMP_IDS_FILE}" \
      | awk 'length($0) <= 128 && $0 !~ /[[:space:]{}]/' \
      > "${TMP_IDS_FILE}.clean"
    mv "${TMP_IDS_FILE}.clean" "${TMP_IDS_FILE}"
    TOTAL_IDS="$(wc -l < "${TMP_IDS_FILE}" | tr -d ' ')"
  fi
fi

if [[ "${TOTAL_IDS}" == "0" ]]; then
  echo "No client IDs found in runtime or API discovery. Nothing to migrate."
  exit 0
fi

echo "Found ${TOTAL_IDS} client(s)."
echo "API_BASE=${API_BASE}"
echo "RUNTIME_DIR=${RUNTIME_DIR}"
if [[ "${DRY_RUN}" == "1" ]]; then
  echo "DRY_RUN enabled. No API calls will be made."
fi
echo

SUCCESS=0
FAILED=0
INDEX=0

while IFS= read -r CLIENT_ID; do
  INDEX=$((INDEX + 1))
  [[ -z "${CLIENT_ID}" ]] && continue

  if [[ "${CLIENT_ID}" =~ [[:space:]{}] ]]; then
    FAILED=$((FAILED + 1))
    printf '[%s/%s] %s\n' "${INDEX}" "${TOTAL_IDS}" "${CLIENT_ID}"
    echo "  FAIL invalid client_id format, skipping"
    continue
  fi

  URL="${API_BASE}/api/crm/clients/${CLIENT_ID}"
  printf '[%s/%s] %s\n' "${INDEX}" "${TOTAL_IDS}" "${CLIENT_ID}"

  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "  DRY_RUN GET ${URL}"
    SUCCESS=$((SUCCESS + 1))
    continue
  fi

  HTTP_CODE="$(
    curl -sS -o "${TMP_BODY_FILE}" -w '%{http_code}' \
      --connect-timeout 5 \
      --max-time 30 \
      "${URL}" || echo "000"
  )"

  if [[ "${HTTP_CODE}" == "200" ]]; then
    SUCCESS=$((SUCCESS + 1))
    echo "  OK"
  else
    FAILED=$((FAILED + 1))
    if [[ "${HAS_JQ}" == "1" ]]; then
      ERROR_TEXT="$(jq -r '.detail // .message // .error_code // .status // "unknown_error"' "${TMP_BODY_FILE}" 2>/dev/null || true)"
    else
      ERROR_TEXT="$(
        tr -d '\n' < "${TMP_BODY_FILE}" \
          | sed -E 's/.*"(detail|message|error_code|status)"[[:space:]]*:[[:space:]]*"([^"]*)".*/\2/' \
          || true
      )"
    fi
    [[ -z "${ERROR_TEXT}" || "${ERROR_TEXT}" == "null" ]] && ERROR_TEXT="unknown_error"
    echo "  FAIL HTTP ${HTTP_CODE}: ${ERROR_TEXT}"
  fi
done < "${TMP_IDS_FILE}"

echo
echo "Migration trigger finished."
echo "  Success: ${SUCCESS}"
echo "  Failed:  ${FAILED}"

if [[ "${FAILED}" -gt 0 ]]; then
  exit 2
fi
