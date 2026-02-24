const envSwitch = document.getElementById("envSwitch");
const envToggle = document.getElementById("envToggle");
const refreshBtn = document.getElementById("refreshBtn");
const openStatusBtn = document.getElementById("openStatusBtn");
const closeStatusBtn = document.getElementById("closeStatusBtn");
const statusText = document.getElementById("statusText");
const progressText = document.getElementById("progressText");
const tableBody = document.getElementById("tableBody");

const progressOverlay = document.getElementById("progressOverlay");
const modalPhaseText = document.getElementById("modalPhaseText");
const modalCurrentTable = document.getElementById("modalCurrentTable");
const modalLog = document.getElementById("modalLog");

const tableDetailModal = document.getElementById("tableDetailModal");
const tableDetailWindow = document.getElementById("tableDetailWindow");
const tableDetailDragHandle = document.getElementById("tableDetailDragHandle");
const tableDetailTitle = document.getElementById("tableDetailTitle");
const tableDetailCloseBtn = document.getElementById("tableDetailCloseBtn");
const tableDetailMeta = document.getElementById("tableDetailMeta");
const tableDetailDiffBody = document.getElementById("tableDetailDiffBody");
const tableDiffSummary = document.getElementById("tableDiffSummary");
const tableSyncAddBtn = document.getElementById("tableSyncAddBtn");
const tableSyncRemoveBtn = document.getElementById("tableSyncRemoveBtn");
const tableDetailLoadState = document.getElementById("tableDetailLoadState");

const POLL_MS = 2000;
const API_ORIGIN =
  window.location.hostname === "localhost"
    ? "http://127.0.0.1:8000"
    : window.location.origin;
let pollTimer = null;
let currentEnv = "live";
let manualStatusOverlay = false;
let activeDetailTable = "";
let activeDetailSyncSelected = false;
const detailModalDrag = {
  dragging: false,
  pointerId: null,
  startClientX: 0,
  startClientY: 0,
  startLeft: 0,
  startTop: 0,
};
const latestSnapshot = { live: null, tst: null };
const statusHistory = { live: [], tst: [] };
const lastLogKey = { live: "", tst: "" };

function envLabel(env) {
  return env === "live" ? "ERP LIVE" : "ERP TST";
}

function apiUrl(path) {
  return `${API_ORIGIN}${path}`;
}

function setEnv(env) {
  currentEnv = env;
  if (envSwitch) {
    envSwitch.dataset.env = env === "tst" ? "test" : "live";
  }
  renderStatusLog();
}

function nowTimeLabel() {
  return new Intl.DateTimeFormat("de-DE", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date());
}

function renderStatusLog() {
  if (!modalLog) return;
  const entries = statusHistory[currentEnv] || [];
  if (!entries.length) {
    modalLog.innerHTML = "<li>Warte auf Start ...</li>";
    return;
  }
  modalLog.innerHTML = entries
    .slice(-10)
    .map((entry) => `<li><span class="status-log-time">${escapeHtml(entry.time)}</span>${escapeHtml(entry.text)}</li>`)
    .join("");
}

function addStatusLog(text, key = "") {
  const message = String(text || "").trim();
  if (!message) return;
  if (key && lastLogKey[currentEnv] === key) return;
  if (key) lastLogKey[currentEnv] = key;
  const list = statusHistory[currentEnv] || [];
  const last = list[list.length - 1];
  if (last && last.text === message) return;
  list.push({ time: nowTimeLabel(), text: message });
  if (list.length > 30) {
    list.splice(0, list.length - 30);
  }
  statusHistory[currentEnv] = list;
  renderStatusLog();
}

function phaseFallback(phase) {
  if (phase === "queued") return "Job ist in der Warteschlange.";
  if (phase === "init") return "Verbindungen werden aufgebaut ...";
  if (phase === "discover_datalake") return "DataLake Tabellen werden geladen ...";
  if (phase === "discover_fabric") return "Fabric Tabellen werden geladen ...";
  if (phase === "discover_columns") return "Spaltenmetadaten werden geladen ...";
  if (phase === "processing") return "Tabellenvergleich läuft ...";
  if (phase === "idle") return "Aktualisierung pausiert.";
  if (phase === "finished") return "Aktualisierung abgeschlossen.";
  if (phase === "error") return "Aktualisierung mit Fehler beendet.";
  return "";
}

function isAutoOverlayActive(snapshot) {
  return Boolean(snapshot?.running) && (snapshot?.completed_tables ?? 0) === 0;
}

function setProgressOverlay(visible, phaseText = "", currentTable = "") {
  if (!progressOverlay) return;
  const isVisible = Boolean(visible);
  progressOverlay.hidden = !isVisible;
  document.body.classList.toggle("sync-busy", isVisible);
  if (modalPhaseText) {
    modalPhaseText.textContent = phaseText || "Aktualisierung läuft ...";
  }
  if (modalCurrentTable) {
    modalCurrentTable.textContent = currentTable ? `Aktive Tabelle: ${currentTable}` : "";
  }
}

function updateOverlayFromSnapshot(snapshot) {
  const detailText = String(snapshot?.phase_detail || "").trim() || phaseFallback(String(snapshot?.phase || "").trim());
  const activeTable = String(snapshot?.current_table || "").trim();
  const autoVisible = isAutoOverlayActive(snapshot);
  const showOverlay = autoVisible || manualStatusOverlay;
  setProgressOverlay(showOverlay, detailText || "Lade Tabellenmetadaten ...", activeTable);
  if (closeStatusBtn) {
    closeStatusBtn.disabled = autoVisible;
  }
}

async function refreshTables({ force = false } = {}) {
  const url = apiUrl(
    `/api/datalake-sync/datalake/tables/refresh?env=${encodeURIComponent(currentEnv)}&force=${force ? "true" : "false"}`
  );
  const response = await fetch(url, { method: "POST" });
  if (response.status === 404 || response.status === 405) {
    return fetchSnapshot({ autostart: false });
  }
  if (!response.ok) {
    throw new Error(`Refresh fehlgeschlagen (${response.status})`);
  }
  return response.json();
}

async function fetchSnapshot({ autostart = false } = {}) {
  const url = apiUrl(
    `/api/datalake-sync/datalake/tables?env=${encodeURIComponent(currentEnv)}&autostart=${autostart ? "true" : "false"}`
  );
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Laden fehlgeschlagen (${response.status})`);
  }
  return response.json();
}

async function fetchTableDetail(tableName) {
  const url = apiUrl(
    `/api/datalake-sync/table-detail?env=${encodeURIComponent(currentEnv)}&table=${encodeURIComponent(tableName)}`
  );
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), 120000);
  let response;
  try {
    response = await fetch(url, { signal: controller.signal });
  } catch (error) {
    if (error && error.name === "AbortError") {
      throw new Error("Detailabfrage Timeout nach 120s.");
    }
    throw error;
  } finally {
    window.clearTimeout(timeoutId);
  }
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Detail laden fehlgeschlagen (${response.status})`);
  }
  return response.json();
}

async function setTableSyncSelection(tableName, enabled) {
  const url = apiUrl(`/api/datalake-sync/table-sync-selection?env=${encodeURIComponent(currentEnv)}`);
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ table_name: tableName, enabled: Boolean(enabled) }),
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Sync-Auswahl speichern fehlgeschlagen (${response.status})`);
  }
  return response.json();
}

function formatNumber(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "n/a";
  }
  return new Intl.NumberFormat("de-DE").format(value);
}

function formatDateTime(value) {
  if (value === null || value === undefined) return "n/a";
  const text = String(value).trim();
  if (!text) return "n/a";
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return text;
  return new Intl.DateTimeFormat("de-DE", {
    dateStyle: "short",
    timeStyle: "medium",
  }).format(parsed);
}

function timestampDeltaLabel(a, b) {
  const left = String(a || "").trim();
  const right = String(b || "").trim();
  if (!left && !right) return "-";
  if (!left || !right) return "fehlt";
  const leftDate = new Date(left);
  const rightDate = new Date(right);
  if (Number.isNaN(leftDate.getTime()) || Number.isNaN(rightDate.getTime())) {
    return left === right ? "0s" : "abweichend";
  }
  const diffMs = leftDate.getTime() - rightDate.getTime();
  if (diffMs === 0) return "0s";
  const sign = diffMs > 0 ? "+" : "-";
  let remaining = Math.abs(diffMs);
  const seconds = Math.floor(remaining / 1000);
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = seconds % 60;
  const parts = [];
  if (days) parts.push(`${days}d`);
  if (hours || days) parts.push(`${hours}h`);
  if (minutes || hours || days) parts.push(`${minutes}m`);
  parts.push(`${secs}s`);
  return `${sign}${parts.join(" ")}`;
}

function numericDeltaLabel(a, b) {
  if (typeof a !== "number" || !Number.isFinite(a) || typeof b !== "number" || !Number.isFinite(b)) {
    return "-";
  }
  const delta = a - b;
  if (delta === 0) return "0";
  const sign = delta > 0 ? "+" : "-";
  return `${sign}${formatNumber(Math.abs(delta))}`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function statusClass(status, timeoutTier = "normal") {
  if (status === "queued" && timeoutTier === "timeout") return "status-waitlist-1";
  if (status === "queued" && timeoutTier === "superheavy") return "status-waitlist-2";
  if (status === "ok") return "status-ok";
  if (status === "done") return "status-done";
  if (status === "running") return "status-running";
  if (status === "queued") return "status-queued";
  if (status === "error") return "status-error";
  if (status === "timeout") return "status-timeout";
  return "status-pending";
}

function statusLabel(status, compareStatus = "", timeoutTier = "normal") {
  if (status === "running") return "läuft";
  if (status === "queued" && timeoutTier === "timeout") return "Warteliste 1";
  if (status === "queued" && timeoutTier === "superheavy") return "Warteliste 2";
  if (status === "queued") return "wartet";
  if (status === "done") return "fertig";
  if (status === "ok") return "ok";
  if (status === "error") return "error";
  if (status === "timeout") return "timeout";
  if (status === "missing") return "fehlt";
  if (status === "pending" && ["equal", "diff", "missing", "error", "timeout"].includes(compareStatus)) {
    return "fertig";
  }
  return "wartet";
}

function metricValue({ value, status, nullWhenDone = "n/a", missingText = "fehlt" }) {
  if (status === "missing") return `<span class="missing-value">${missingText}</span>`;
  if (status === "skipped") return '<span class="unsupported-value">-</span>';
  if (status === "timeout") return '<span class="timeout-value">timeout</span>';
  if (typeof value === "number" && Number.isFinite(value)) return formatNumber(value);
  if (status === "error") return "n/a";
  if (status === "ok") return nullWhenDone;
  return '<span class="pending-value">...</span>';
}

function metricDateValue({ value, status }) {
  if (status === "missing") return '<span class="missing-value">fehlt</span>';
  if (status === "skipped") return '<span class="unsupported-value">-</span>';
  if (status === "timeout") return '<span class="timeout-value">timeout</span>';
  if (value !== null && value !== undefined && String(value).trim()) {
    return escapeHtml(formatDateTime(value));
  }
  if (status === "error") return "n/a";
  if (status === "ok") return "n/a";
  return '<span class="pending-value">...</span>';
}

function compareLabel(status) {
  if (status === "equal") return "gleich";
  if (status === "diff") return "abweichung";
  if (status === "missing") return "kein Fabric";
  if (status === "timeout") return "timeout";
  if (status === "error") return "fehler";
  return "...";
}

function compareClass(status) {
  if (status === "equal") return "compare-equal";
  if (status === "diff") return "compare-diff";
  if (status === "missing") return "compare-missing";
  if (status === "timeout") return "compare-timeout";
  if (status === "error") return "compare-error";
  return "compare-pending";
}

function rowClass(status, timeoutTier = "normal") {
  if (status === "running") return "row-running";
  if (status === "queued" && timeoutTier === "timeout") return "row-waitlist-1";
  if (status === "queued" && timeoutTier === "superheavy") return "row-waitlist-2";
  if (status === "queued") return "row-queued";
  if (["ok", "done", "error", "timeout", "missing"].includes(status)) return "row-done";
  return "";
}

function renderTables(tables, activeTable = "") {
  const rowsData = Array.isArray(tables) ? tables : [];
  if (!rowsData.length) {
    tableBody.innerHTML = '<tr><td colspan="9">Keine Tabellen gefunden.</td></tr>';
    return 0;
  }
  const rows = rowsData.map((entry) => {
    const tableName = String(entry.table_name || "-");
    const timeoutTier = String(entry.timeout_tier || "normal").toLowerCase();
    let status = String(entry.status || "pending");
    const isTerminal = ["ok", "done", "error", "timeout", "missing"].includes(status);
    if (activeTable && tableName === activeTable && !isTerminal) {
      status = "running";
    } else if (status === "pending") {
      status = "queued";
    }
    const dlStatus = String(entry.datalake_status || status || "pending");
    const fbStatus = String(entry.fabric_status || "pending");
    const compareStatus = String(entry.compare_status || "pending");
    const dlRowCount = metricValue({ value: entry.row_count, status: dlStatus });
    const dlFieldCount = metricValue({ value: entry.field_count, status: dlStatus });
    const dlLastUpdate = metricDateValue({ value: entry.last_update, status: dlStatus });
    const fbRowCount = metricValue({ value: entry.fabric_row_count, status: fbStatus });
    const fbFieldCount = metricValue({ value: entry.fabric_field_count, status: fbStatus });
    const fbLastUpdate = metricDateValue({ value: entry.fabric_last_update, status: fbStatus });
    const statusTextValue = statusLabel(status, compareStatus, timeoutTier);
    const rowClassName = rowClass(status, timeoutTier);
    const openPayload = encodeURIComponent(tableName);

    const errorParts = [];
    if (entry.datalake_error) errorParts.push(`DL: ${entry.datalake_error}`);
    if (entry.fabric_error) errorParts.push(`FB: ${entry.fabric_error}`);
    if (entry.error) errorParts.push(String(entry.error));
    const statusTitle = errorParts.length ? ` title="${escapeHtml(errorParts.join(" | "))}"` : "";
    return `
      <tr class="${rowClassName} table-row-clickable" data-table-name="${escapeHtml(tableName)}">
        <td>
          <button
            class="table-open-link"
            type="button"
            data-table-open="${escapeHtml(tableName)}"
            onclick="if(window.__dlsOpen){window.__dlsOpen(decodeURIComponent('${openPayload}'));} return false;"
          >
            ${escapeHtml(tableName)}
          </button>
        </td>
        <td>${dlRowCount}</td>
        <td>${dlFieldCount}</td>
        <td>${dlLastUpdate}</td>
        <td>${fbRowCount}</td>
        <td>${fbFieldCount}</td>
        <td>${fbLastUpdate}</td>
        <td><span class="${compareClass(compareStatus)}">${compareLabel(compareStatus)}</span></td>
        <td class="${statusClass(status, timeoutTier)}"${statusTitle}>${escapeHtml(statusTextValue)}</td>
      </tr>
    `;
  });
  tableBody.innerHTML = rows.join("");
  return rowsData.length;
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function centerDetailWindow() {
  if (!tableDetailWindow) return;
  const viewportWidth = window.innerWidth;
  const viewportHeight = window.innerHeight;
  const rect = tableDetailWindow.getBoundingClientRect();
  const left = Math.max(12, Math.round((viewportWidth - rect.width) / 2));
  const top = Math.max(16, Math.round((viewportHeight - rect.height) / 2));
  tableDetailWindow.style.left = `${left}px`;
  tableDetailWindow.style.top = `${top}px`;
}

function beginDetailDrag(pointerId, clientX, clientY) {
  if (!tableDetailWindow) return;
  const rect = tableDetailWindow.getBoundingClientRect();
  detailModalDrag.dragging = true;
  detailModalDrag.pointerId = pointerId;
  detailModalDrag.startClientX = clientX;
  detailModalDrag.startClientY = clientY;
  detailModalDrag.startLeft = rect.left;
  detailModalDrag.startTop = rect.top;
}

function moveDetailDrag(clientX, clientY) {
  if (!detailModalDrag.dragging || !tableDetailWindow) return;
  const dx = clientX - detailModalDrag.startClientX;
  const dy = clientY - detailModalDrag.startClientY;
  const viewportWidth = window.innerWidth;
  const viewportHeight = window.innerHeight;
  const rect = tableDetailWindow.getBoundingClientRect();
  const maxLeft = Math.max(12, viewportWidth - rect.width - 12);
  const maxTop = Math.max(16, viewportHeight - rect.height - 12);
  const nextLeft = clamp(detailModalDrag.startLeft + dx, 12, maxLeft);
  const nextTop = clamp(detailModalDrag.startTop + dy, 16, maxTop);
  tableDetailWindow.style.left = `${Math.round(nextLeft)}px`;
  tableDetailWindow.style.top = `${Math.round(nextTop)}px`;
}

function endDetailDrag() {
  detailModalDrag.dragging = false;
  detailModalDrag.pointerId = null;
}

function setDetailModalVisible(visible) {
  if (!tableDetailModal) return;
  const isVisible = Boolean(visible);
  tableDetailModal.style.display = isVisible ? "flex" : "none";
  tableDetailModal.setAttribute("aria-hidden", isVisible ? "false" : "true");
  if (isVisible) {
    centerDetailWindow();
  } else {
    endDetailDrag();
  }
  if (isVisible && tableDetailCloseBtn) {
    window.setTimeout(() => tableDetailCloseBtn.focus(), 0);
  }
}

function setDetailLoadState(message, tone = "info") {
  if (!tableDetailLoadState) return;
  tableDetailLoadState.textContent = String(message || "");
  tableDetailLoadState.classList.remove("is-ok", "is-error");
  if (tone === "ok") {
    tableDetailLoadState.classList.add("is-ok");
  } else if (tone === "error") {
    tableDetailLoadState.classList.add("is-error");
  }
}

function fieldDiffLabel(status) {
  if (status === "missing_in_fabric") return "fehlt in Fabric";
  if (status === "missing_in_datalake") return "fehlt in DataLake";
  if (status === "type_mismatch") return "technisch verschieden";
  return "gleich";
}

function updateDetailSyncButton() {
  if (tableSyncAddBtn) {
    tableSyncAddBtn.disabled = !activeDetailTable || activeDetailSyncSelected;
  }
  if (tableSyncRemoveBtn) {
    tableSyncRemoveBtn.disabled = !activeDetailTable || !activeDetailSyncSelected;
  }
}

function metricDiffClass(deltaLabel) {
  const text = String(deltaLabel || "").trim().toLowerCase();
  if (text === "0" || text === "0s") return "metric-diff-ok";
  if (!text || text === "-" || text === "n/a") return "";
  return "metric-diff-warn";
}

function safeMetricNumber(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "n/a";
  return formatNumber(value);
}

function safeMetricTimestamp(value) {
  if (value === null || value === undefined || String(value).trim() === "") return "n/a";
  return formatDateTime(value);
}

function renderMetricMatrix(table) {
  const dlRows = table?.row_count;
  const fbRows = table?.fabric_row_count;
  const dlFields = table?.field_count;
  const fbFields = table?.fabric_field_count;
  const dlTs = table?.last_update;
  const fbTs = table?.fabric_last_update;
  const fieldDelta = numericDeltaLabel(dlFields, fbFields);
  const rowDelta = numericDeltaLabel(dlRows, fbRows);
  const tsDelta = timestampDeltaLabel(dlTs, fbTs);
  return `
    <table class="datalake-metric-table">
      <thead>
        <tr>
          <th>Merkmal</th>
          <th>DataLake</th>
          <th>Fabric</th>
          <th>Differenz</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>Anzahl Felder</td>
          <td>${escapeHtml(safeMetricNumber(dlFields))}</td>
          <td>${escapeHtml(safeMetricNumber(fbFields))}</td>
          <td class="metric-diff-cell ${metricDiffClass(fieldDelta)}">${escapeHtml(fieldDelta)}</td>
        </tr>
        <tr>
          <td>Anzahl Datensätze</td>
          <td>${escapeHtml(safeMetricNumber(dlRows))}</td>
          <td>${escapeHtml(safeMetricNumber(fbRows))}</td>
          <td class="metric-diff-cell ${metricDiffClass(rowDelta)}">${escapeHtml(rowDelta)}</td>
        </tr>
        <tr>
          <td>Last Update (Timestamp)</td>
          <td>${escapeHtml(safeMetricTimestamp(dlTs))}</td>
          <td>${escapeHtml(safeMetricTimestamp(fbTs))}</td>
          <td class="metric-diff-cell ${metricDiffClass(tsDelta)}">${escapeHtml(tsDelta)}</td>
        </tr>
      </tbody>
    </table>
  `;
}

function renderDetailLoading(tableName) {
  if (tableDetailTitle) tableDetailTitle.textContent = `Tabellendetail: ${tableName}`;
  setDetailLoadState(`Tabelle ${tableName} wird abgefragt ...`, "info");
  if (tableDetailMeta) {
    tableDetailMeta.innerHTML =
      '<table class="datalake-metric-table"><tbody><tr><td colspan="4">Lade Vergleichsdaten ...</td></tr></tbody></table>';
  }
  if (tableDiffSummary) tableDiffSummary.textContent = "Lade Feldvergleich ...";
  if (tableDetailDiffBody) {
    tableDetailDiffBody.innerHTML = '<tr><td colspan="4">Lade Felddetails ...</td></tr>';
  }
  activeDetailSyncSelected = false;
  updateDetailSyncButton();
}

function renderDetailPayload(payload) {
  const table = payload?.table || {};
  const tableName = String(table.table_name || activeDetailTable || "-");
  if (tableDetailTitle) tableDetailTitle.textContent = `Tabellendetail: ${tableName}`;

  if (tableDetailMeta) tableDetailMeta.innerHTML = renderMetricMatrix(table);

  const differences = Array.isArray(payload?.field_differences) ? payload.field_differences : [];
  const statusCounts = payload?.field_status_counts || {};
  const datalakeFieldCount = Number(payload?.field_counts?.datalake ?? table.field_count ?? 0);
  const fabricFieldCount = Number(payload?.field_counts?.fabric ?? table.fabric_field_count ?? 0);
  const fabricError = String(payload?.fabric_error || "").trim();
  if (fabricError) {
    setDetailLoadState(`Details geladen, aber mit Fabric-Fehler: ${fabricError}`, "error");
  } else {
    setDetailLoadState("Details erfolgreich geladen.", "ok");
  }
  if (tableDiffSummary) {
    if (fabricError) {
      tableDiffSummary.textContent = `Fabric Fehler: ${fabricError}`;
    } else {
      const missing = Number(statusCounts?.missing || 0);
      const mismatch = Number(statusCounts?.type_mismatch || 0);
      const match = Number(statusCounts?.match || 0);
      tableDiffSummary.textContent =
        `Fehlend: ${formatNumber(missing)} · Typ verschieden: ${formatNumber(mismatch)} · Gleich: ${formatNumber(match)} · DL: ${formatNumber(datalakeFieldCount)} · FB: ${formatNumber(fabricFieldCount)}`;
    }
  }

  if (!tableDetailDiffBody) return;
  if (fabricError) {
    tableDetailDiffBody.innerHTML = `<tr><td colspan="4">${escapeHtml(fabricError)}</td></tr>`;
  } else if (!differences.length) {
    tableDetailDiffBody.innerHTML = '<tr><td colspan="4">Keine Feldabweichungen gefunden.</td></tr>';
  } else {
    const priority = {
      missing_in_fabric: 0,
      missing_in_datalake: 0,
      type_mismatch: 1,
      match: 2,
    };
    const sorted = [...differences].sort((a, b) => {
      const sa = String(a?.status || "match");
      const sb = String(b?.status || "match");
      const pa = priority[sa] ?? 9;
      const pb = priority[sb] ?? 9;
      if (pa !== pb) return pa - pb;
      return String(a?.column_name || "").localeCompare(String(b?.column_name || ""));
    });
    tableDetailDiffBody.innerHTML = sorted
      .map((item) => {
        const status = String(item.status || "match").toLowerCase();
        return `
          <tr class="field-row-${escapeHtml(status)}">
            <td>${escapeHtml(String(item.column_name || "-"))}</td>
            <td>${escapeHtml(String(item.datalake_type || "-"))}</td>
            <td>${escapeHtml(String(item.fabric_type || "-"))}</td>
            <td><span class="field-status-${escapeHtml(status)}">${escapeHtml(fieldDiffLabel(status))}</span></td>
          </tr>
        `;
      })
      .join("");
  }

  activeDetailSyncSelected = Boolean(payload?.sync_selected);
  updateDetailSyncButton();
}

async function openTableDetail(tableName) {
  activeDetailTable = String(tableName || "").trim().toLowerCase();
  if (!activeDetailTable) return;
  if (statusText) {
    statusText.textContent = `${envLabel(currentEnv)}: Detail ${activeDetailTable} wird geöffnet ...`;
  }
  setDetailModalVisible(true);
  renderDetailLoading(activeDetailTable);
  try {
    const payload = await fetchTableDetail(activeDetailTable);
    renderDetailPayload(payload);
  } catch (error) {
    setDetailLoadState(`Fehler: ${error.message}`, "error");
    if (tableDetailMeta) tableDetailMeta.innerHTML = `<span class="status-error">${escapeHtml(error.message)}</span>`;
    if (tableDetailDiffBody) {
      tableDetailDiffBody.innerHTML = '<tr><td colspan="4">Felddetails konnten nicht geladen werden.</td></tr>';
    }
    if (tableDiffSummary) tableDiffSummary.textContent = "Fehler";
  }
}

function closeTableDetail() {
  activeDetailTable = "";
  setDetailModalVisible(false);
}

window.__dlsOpen = (tableName) => {
  openTableDetail(String(tableName || "").trim());
};

function renderSnapshot(snapshot) {
  latestSnapshot[currentEnv] = snapshot;
  const completed = snapshot?.completed_tables ?? 0;
  const total = snapshot?.total_tables ?? 0;
  const running = Boolean(snapshot?.running);
  const failed = snapshot?.error_tables ?? 0;
  const phase = String(snapshot?.phase || "").trim();
  const phaseDetail = String(snapshot?.phase_detail || "").trim();
  const activeTable = String(snapshot?.current_table || "").trim();
  const visible = renderTables(snapshot?.tables || [], activeTable);

  if (!running && visible === 0) {
    statusText.textContent = `${envLabel(currentEnv)}: Kein Snapshot vorhanden.`;
    progressText.textContent = "Bitte 'Neu laden' starten.";
    manualStatusOverlay = false;
    updateOverlayFromSnapshot(snapshot);
    stopPolling();
    return;
  }

  if (running) {
    statusText.textContent = `${envLabel(currentEnv)}: Aktualisierung läuft ...`;
  } else if (total > 0 && completed < total) {
    statusText.textContent = `${envLabel(currentEnv)}: Aktualisierung pausiert.`;
  } else {
    statusText.textContent = `${envLabel(currentEnv)}: Tabelleninventar geladen.`;
  }

  progressText.textContent = `Fortschritt: ${completed}/${total} · Fehler: ${failed}`;

  const detailText = phaseDetail || phaseFallback(phase);
  if (detailText) {
    const progressKey = running
      ? `${phase}|${detailText}|${activeTable}|${completed}|${total}`
      : `done|${detailText}|${snapshot?.finished_at_utc || ""}|${completed}|${failed}`;
    addStatusLog(detailText, progressKey);
  }

  updateOverlayFromSnapshot(snapshot);

  if (running) {
    ensurePolling();
  } else {
    stopPolling();
  }
}

function ensurePolling() {
  if (pollTimer !== null) return;
  pollTimer = window.setInterval(async () => {
    try {
      const snapshot = await fetchSnapshot({ autostart: false });
      renderSnapshot(snapshot);
    } catch (error) {
      statusText.textContent = `Fehler beim Polling: ${error.message}`;
      stopPolling();
    }
  }, POLL_MS);
}

function stopPolling() {
  if (pollTimer === null) return;
  window.clearInterval(pollTimer);
  pollTimer = null;
}

async function loadEnvironment(env, { force = false } = {}) {
  setEnv(env);
  if (force) {
    statusHistory[env] = [];
    lastLogKey[env] = "";
    renderStatusLog();
    addStatusLog("Neu laden ausgelöst.", `manual-refresh-${Date.now()}`);
    manualStatusOverlay = true;
    updateOverlayFromSnapshot(latestSnapshot[env]);
  }
  statusText.textContent = `${envLabel(env)}: starte Laden ...`;
  progressText.textContent = "";
  tableBody.innerHTML = '<tr><td colspan="9">Lade Tabellen ...</td></tr>';
  try {
    let snapshot;
    if (force) {
      snapshot = await refreshTables({ force: true });
    } else {
      snapshot = await fetchSnapshot({ autostart: false });
    }
    if (snapshot?.running && (snapshot?.total_tables ?? 0) === 0) {
      await new Promise((resolve) => window.setTimeout(resolve, 250));
      snapshot = await fetchSnapshot({ autostart: false });
    }
    renderSnapshot(snapshot);
  } catch (error) {
    statusText.textContent = `Fehler: ${error.message}`;
    progressText.textContent = "";
    stopPolling();
    manualStatusOverlay = false;
    updateOverlayFromSnapshot(latestSnapshot[currentEnv]);
    tableBody.innerHTML = '<tr><td colspan="9">Tabellen konnten nicht geladen werden.</td></tr>';
  }
}

if (envToggle) {
  envToggle.addEventListener("click", () => {
    const next = currentEnv === "live" ? "tst" : "live";
    closeTableDetail();
    loadEnvironment(next, { force: true });
  });
}

if (refreshBtn) {
  refreshBtn.addEventListener("click", () => {
    closeTableDetail();
    loadEnvironment(currentEnv, { force: true });
  });
}

if (openStatusBtn) {
  openStatusBtn.addEventListener("click", () => {
    manualStatusOverlay = true;
    updateOverlayFromSnapshot(latestSnapshot[currentEnv]);
  });
}

if (closeStatusBtn) {
  closeStatusBtn.addEventListener("click", () => {
    if (isAutoOverlayActive(latestSnapshot[currentEnv])) {
      return;
    }
    manualStatusOverlay = false;
    updateOverlayFromSnapshot(latestSnapshot[currentEnv]);
  });
}

if (tableBody) {
  tableBody.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : event.target?.parentElement;
    if (!target) return;

    const explicitOpen = target.closest("[data-table-open]");
    if (explicitOpen) {
      const tableName = String(explicitOpen.getAttribute("data-table-open") || "").trim();
      if (tableName) {
        if (statusText) {
          statusText.textContent = `${envLabel(currentEnv)}: Klick auf ${tableName}`;
        }
        openTableDetail(tableName);
      }
      return;
    }

    const row = target.closest("tr[data-table-name]");
    if (!row) return;
    const tableName = String(row.dataset.tableName || "").trim();
    if (!tableName) return;
    if (statusText) {
      statusText.textContent = `${envLabel(currentEnv)}: Klick auf ${tableName}`;
    }
    openTableDetail(tableName);
  });
}

if (tableDetailCloseBtn) {
  tableDetailCloseBtn.addEventListener("click", () => {
    closeTableDetail();
  });
}

if (tableDetailDragHandle) {
  tableDetailDragHandle.addEventListener("pointerdown", (event) => {
    if (event.button !== 0) return;
    const target = event.target instanceof Element ? event.target : null;
    if (target && target.closest("button, a, input, select, textarea")) {
      return;
    }
    beginDetailDrag(event.pointerId, event.clientX, event.clientY);
    tableDetailDragHandle.setPointerCapture(event.pointerId);
    event.preventDefault();
  });

  tableDetailDragHandle.addEventListener("pointermove", (event) => {
    if (!detailModalDrag.dragging || detailModalDrag.pointerId !== event.pointerId) return;
    moveDetailDrag(event.clientX, event.clientY);
    event.preventDefault();
  });

  tableDetailDragHandle.addEventListener("pointerup", (event) => {
    if (detailModalDrag.pointerId !== event.pointerId) return;
    endDetailDrag();
    tableDetailDragHandle.releasePointerCapture(event.pointerId);
  });

  tableDetailDragHandle.addEventListener("pointercancel", (event) => {
    if (detailModalDrag.pointerId !== event.pointerId) return;
    endDetailDrag();
    tableDetailDragHandle.releasePointerCapture(event.pointerId);
  });
}

window.addEventListener("resize", () => {
  if (tableDetailModal && tableDetailModal.style.display !== "none") {
    centerDetailWindow();
  }
});

async function applySyncSelection(enabled) {
  if (!activeDetailTable) return;
  if (tableSyncAddBtn) tableSyncAddBtn.disabled = true;
  if (tableSyncRemoveBtn) tableSyncRemoveBtn.disabled = true;
  try {
    const payload = await setTableSyncSelection(activeDetailTable, enabled);
    activeDetailSyncSelected = Boolean(payload?.sync_selected);
    updateDetailSyncButton();

    const snapshot = latestSnapshot[currentEnv];
    if (snapshot && Array.isArray(snapshot.tables)) {
      const row = snapshot.tables.find((item) => String(item?.table_name || "") === activeDetailTable);
      if (row) {
        row.sync_selected = activeDetailSyncSelected;
      }
    }
    setDetailLoadState(
      activeDetailSyncSelected
        ? "Tabelle ist in der Synchronisation aufgenommen."
        : "Tabelle wurde aus der Synchronisation entfernt.",
      "ok"
    );
  } catch (error) {
    if (tableDiffSummary) {
      tableDiffSummary.textContent = `Fehler: ${error.message}`;
    }
    setDetailLoadState(`Fehler beim Speichern: ${error.message}`, "error");
  } finally {
    updateDetailSyncButton();
  }
}

if (tableSyncAddBtn) {
  tableSyncAddBtn.addEventListener("click", () => {
    applySyncSelection(true);
  });
}

if (tableSyncRemoveBtn) {
  tableSyncRemoveBtn.addEventListener("click", () => {
    applySyncSelection(false);
  });
}

window.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") return;
  if (!progressOverlay?.hidden && !isAutoOverlayActive(latestSnapshot[currentEnv])) {
    manualStatusOverlay = false;
    updateOverlayFromSnapshot(latestSnapshot[currentEnv]);
  }
});

loadEnvironment("live", { force: false });
