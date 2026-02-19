const envSwitch = document.getElementById("envSwitch");
const envToggle = document.getElementById("envToggle");
const rsrdEnvSwitch = document.getElementById("rsrdEnvSwitch");
const rsrdEnvToggle = document.getElementById("rsrdEnvToggle");
const backBtn = document.getElementById("rsrd2BackBtn");
const compareTitle = document.getElementById("compareTitle");
const compareSubtitle = document.getElementById("compareSubtitle");
const compareStatus = document.getElementById("compareStatus");
const compareTableBody = document.getElementById("compareTableBody");
const xmlBtn = document.getElementById("rsrd2XmlBtn");
const documentsWrap = document.getElementById("rsrd2Documents");
const documentsCount = document.getElementById("rsrd2DocumentsCount");
const documentsBody = document.getElementById("rsrd2DocumentsBody");

const resolveEnvValue = (value) => (value && value.toUpperCase() === "TEST" ? "TEST" : "LIVE");
let currentEnv = resolveEnvValue(window.localStorage.getItem("sparepart.env") || "LIVE");
let currentRsrdEnv = resolveEnvValue(window.localStorage.getItem("sparepart.rsrd_env") || currentEnv);
const getErpEnvParam = () => currentEnv.toLowerCase();
const getRsrdEnvParam = () => currentRsrdEnv.toLowerCase();
const stripTrailingSlash = (value) => String(value || "").replace(/\/+$/, "");
const runtimeApiConfig = window.__SPAREPART_API_CONFIG__ || {};
const rsrd2BaseUrl = stripTrailingSlash(runtimeApiConfig.RSRD2_API_BASE_URL || "");
const rsrd2Api = (path) => {
  if (!String(path || "").startsWith("/")) return String(path || "");
  return rsrd2BaseUrl ? `${rsrd2BaseUrl}${path}` : path;
};
const appendQueryParam = (url, key, value) => {
  const joiner = url.includes("?") ? "&" : "?";
  return `${url}${joiner}${encodeURIComponent(key)}=${encodeURIComponent(value)}`;
};
const withRsrd2Env = (url) =>
  appendQueryParam(appendQueryParam(rsrd2Api(url), "env", getErpEnvParam()), "rsrd_env", getRsrdEnvParam());

const formatEnvBadge = (envValue) => {
  const normalized = resolveEnvValue(envValue);
  const label = normalized === "TEST" ? "TST" : "PRD";
  const cls = normalized === "TEST" ? "rsrd2-env-chip--tst" : "rsrd2-env-chip--prd";
  return `<span class="rsrd2-env-chip ${cls}">${label}</span>`;
};

const updateEnvSwitch = () => {
  if (envSwitch) envSwitch.dataset.env = getErpEnvParam();
  if (rsrdEnvSwitch) rsrdEnvSwitch.dataset.env = getRsrdEnvParam();
  if (compareSubtitle) {
    const erpBadge = formatEnvBadge(currentEnv);
    const rsrdBadge = formatEnvBadge(currentRsrdEnv);
    compareSubtitle.innerHTML = `ERP - ${erpBadge} / RSRD - ${rsrdBadge}`;
  }
};

const parseParams = () => {
  const params = new URLSearchParams(window.location.search);
  return {
    sern: params.get("sern") || "",
  };
};

const formatValue = (value) => {
  if (value === null || value === undefined || value === "") return "-";
  if (Array.isArray(value)) return value.join(", ");
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
};

const asText = (value) => {
  if (value === null || value === undefined || value === "") return "-";
  if (Array.isArray(value)) return value.map((item) => asText(item)).join(", ");
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
};

const pickField = (doc, keys) => {
  if (!doc || typeof doc !== "object") return "";
  for (const key of keys) {
    const value = doc[key];
    if (value !== null && value !== undefined && value !== "") return value;
  }
  return "";
};

const renderDocuments = (documents) => {
  if (!documentsBody) return;
  documentsBody.innerHTML = "";
  if (!Array.isArray(documents) || documents.length === 0) {
    documentsBody.innerHTML = `<div class="rsrd2-documents-empty">Keine Dokumente hinterlegt.</div>`;
    if (documentsCount) documentsCount.textContent = "(0)";
    return;
  }
  if (documentsCount) documentsCount.textContent = `(${documents.length})`;
  const table = document.createElement("table");
  table.className = "ids-data-table rsrd2-documents-table";
  table.innerHTML = `
    <thead>
      <tr>
        <th>Dokument</th>
        <th>ID</th>
        <th>Typ</th>
        <th>Sprache</th>
        <th>Link/Datei</th>
      </tr>
    </thead>
    <tbody></tbody>
  `;
  const body = table.querySelector("tbody");
  documents.forEach((doc) => {
    const tr = document.createElement("tr");
    const name = pickField(doc, [
      "DocumentName",
      "DocumentTitle",
      "Name",
      "Title",
      "DocumentDescription",
      "Description",
    ]);
    const docId = pickField(doc, ["DocumentID", "DocumentId", "ID", "Id", "DocumentNumber"]);
    const docType = pickField(doc, ["DocumentCategory", "Category", "DocumentType", "Type"]);
    const language = pickField(doc, ["DocumentLanguage", "Language", "Lang"]);
    const source = pickField(doc, [
      "DocumentURL",
      "DocumentUrl",
      "URL",
      "Url",
      "Link",
      "DocumentFileName",
      "FileName",
      "Filename",
      "Path",
    ]);
    [name, docId, docType, language, source].forEach((value) => {
      const td = document.createElement("td");
      td.textContent = asText(value);
      tr.appendChild(td);
    });
    body.appendChild(tr);
  });
  documentsBody.appendChild(table);
};

const renderCompare = (diffs) => {
  compareTableBody.innerHTML = "";
  if (!diffs.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 4;
    cell.textContent = "Keine Unterschiede gefunden.";
    row.appendChild(cell);
    compareTableBody.appendChild(row);
    return;
  }
  const filtered = [];
  const seenEqual = new Set();
  diffs.forEach((diff) => {
    const field = diff.erp_field || diff.field || diff.rsrd_field || "n/a";
    if (diff.equal && seenEqual.has(field)) {
      return;
    }
    if (diff.equal) seenEqual.add(field);
    filtered.push(diff);
  });
  const shortUpload = (value) => {
    if (!value) return "";
    const withoutComputed = value.replace("computed:false", "").trim();
    const dotted = withoutComputed.split("·").pop() || withoutComputed;
    const last = dotted.split("/").pop() || dotted;
    return last.replace(/^xsd:/, "").trim();
  };
  filtered.forEach((diff) => {
    const tr = document.createElement("tr");
    tr.classList.add(diff.equal ? "rsrd2-row-equal" : "rsrd2-row-diff");
    const field = diff.erp_field || diff.field || diff.rsrd_field || "n/a";
    const erpValue = formatValue(diff.erp);
    const rsrdValue = formatValue(diff.rsrd);
    const status = diff.equal ? "gleich" : "abweichend";
    const uploadField = diff.upload_field ? ` · ${shortUpload(diff.upload_field)}` : "";
    tr.innerHTML = `
      <td>${field}<span class="rsrd2-upload-field">${uploadField}</span></td>
      <td>${erpValue}</td>
      <td>${rsrdValue}</td>
      <td>${status}</td>
    `;
    compareTableBody.appendChild(tr);
  });
};

const openXmlPopupWindow = ({ onUploadTst, onUploadPrd }) => {
  const win = window.open("", "_blank", "width=980,height=720,scrollbars=yes");
  if (!win) return null;
  const doc = win.document;
  doc.open();
  doc.write(`<!DOCTYPE html>
  <html lang="de">
    <head>
      <meta charset="UTF-8" />
      <title>RSRD2 Upload</title>
      <style>
        body { font-family: "Courier New", monospace; margin: 0; background: #f6f7f9; color: #101820; }
        .wrap { padding: 20px; }
        h1 { font-size: 18px; margin: 0 0 6px; }
        .meta { font-size: 12px; color: #5b6472; margin-bottom: 16px; }
        .section { margin-bottom: 20px; }
        .actions { display: flex; gap: 10px; flex-wrap: wrap; margin: 12px 0 18px; }
        .btn { padding: 8px 14px; border-radius: 4px; border: 1px solid #d8dde6; background: #fff; cursor: pointer; }
        .btn-primary { background: #0072ed; border-color: #0072ed; color: #fff; }
        .status { font-size: 12px; color: #3e4653; margin-top: 8px; }
        .label { font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em; color: #3e4653; }
        pre { background: #fff; border: 1px solid #d8dde6; padding: 12px; overflow: auto; max-height: 420px; }
      </style>
    </head>
    <body>
      <div class="wrap">
        <h1 id="popupTitle"></h1>
        <div class="meta" id="popupMeta"></div>
        <div class="actions">
          <button class="btn" id="popupUploadTstBtn" type="button">Upload TST</button>
          <button class="btn btn-primary" id="popupUploadPrdBtn" type="button">Upload PRD</button>
        </div>
        <div class="status" id="popupStatus"></div>
        <div class="section">
          <div class="label">XML</div>
          <pre id="popupXml"></pre>
        </div>
        <div class="section">
          <div class="label">RSRD Antwort</div>
          <pre id="popupResponse"></pre>
        </div>
      </div>
    </body>
  </html>`);
  doc.close();
  const tstBtn = doc.getElementById("popupUploadTstBtn");
  const prdBtn = doc.getElementById("popupUploadPrdBtn");
  if (tstBtn && typeof onUploadTst === "function") {
    tstBtn.addEventListener("click", onUploadTst);
  }
  if (prdBtn && typeof onUploadPrd === "function") {
    prdBtn.addEventListener("click", onUploadPrd);
  }
  return { win, doc };
};

const formatXml = (xml) => {
  if (!xml || typeof xml !== "string") return "";
  const trimmed = xml.trim();
  if (!trimmed) return "";
  const withBreaks = trimmed.replace(/></g, ">\n<");
  const lines = withBreaks.split("\n");
  let indent = 0;
  return lines
    .map((line) => {
      const isClosing = /^<\//.test(line);
      const isOpening = /^<[^!?/][^>]*>$/.test(line) && !line.endsWith("/>");
      if (isClosing) indent = Math.max(indent - 1, 0);
      const pad = "  ".repeat(indent);
      if (isOpening) indent += 1;
      return `${pad}${line}`;
    })
    .join("\n");
};

const formatXmlMaybe = (text) => {
  if (!text || typeof text !== "string") return "";
  const trimmed = text.trim();
  if (!trimmed) return "";
  if (trimmed.startsWith("<")) return formatXml(trimmed);
  return text;
};

const formatResponse = (text, status) => {
  if (!text || typeof text !== "string") return "Keine Antwort.";
  const trimmed = text.trim();
  if (!trimmed) return "Keine Antwort.";
  const isXml = trimmed.startsWith("<");
  if (!isXml) return text;
  const hasFault =
    /<\\w*:Fault\\b/i.test(trimmed) ||
    /<Fault\\b/i.test(trimmed) ||
    /<faultcode\\b/i.test(trimmed) ||
    /<faultstring\\b/i.test(trimmed) ||
    /RollingStockDatasetFailed/i.test(trimmed);
  const isOkStatus = status && Number(status) >= 200 && Number(status) < 300;
  if (hasFault || !isOkStatus) return formatXml(trimmed);
  if (/UploadWagonDataResponse/i.test(trimmed)) return "OK";
  return formatXml(trimmed);
};

const openXmlPopup = ({
  win,
  title,
  erpLabel,
  rsrdLabel,
  xml,
  responseText,
  responseStatus,
  requestUrl,
  upload,
  statusText,
}) => {
  const doc = win?.document;
  if (!doc) return;
  doc.getElementById("popupTitle").textContent = title;
  const metaParts = [];
  if (erpLabel) metaParts.push(`ERP: ${erpLabel}`);
  if (rsrdLabel) metaParts.push(`RSRD: ${rsrdLabel}`);
  if (upload) metaParts.push("UPLOAD: ja");
  if (requestUrl) metaParts.push(`URL: ${requestUrl}`);
  if (responseStatus !== null && responseStatus !== undefined) metaParts.push(`HTTP: ${responseStatus}`);
  doc.getElementById("popupMeta").textContent = metaParts.join(" · ");
  const statusEl = doc.getElementById("popupStatus");
  if (statusEl) statusEl.textContent = statusText || "";
  doc.getElementById("popupXml").textContent = formatXml(xml || "");
  doc.getElementById("popupResponse").textContent = formatResponse(responseText || "", responseStatus);
};

const requestUploadXml = async ({ env, upload, popup }) => {
  const { sern } = parseParams();
  if (!sern) {
    compareStatus.textContent = "Keine Wagennummer angegeben.";
    renderDocuments([]);
    return;
  }
  const digits = sern.replace(/\D/g, "");
  const wagon = digits || sern;
  const erpEnv = getErpEnvParam();
  const rsrdEnv = env;
  const actionLabel = upload ? `Upload ${rsrdEnv.toUpperCase()}` : "XML erstellen";
  openXmlPopup({
    win: popup.win,
    title: `${actionLabel} · ${sern}`,
    erpLabel: erpEnv.toUpperCase(),
    rsrdLabel: rsrdEnv.toUpperCase(),
    xml: "Bitte warten ...",
    responseText: "",
    responseStatus: null,
    requestUrl: null,
    upload,
    statusText: upload ? "Sende an RSRD ..." : "Erzeuge XML ...",
  });
  compareStatus.textContent = `${actionLabel} ...`;
  try {
    const url = appendQueryParam(
      appendQueryParam(rsrd2Api("/api/rsrd2/upload_xml"), "env", erpEnv),
      "rsrd_env",
      rsrdEnv,
    );
    const resp = await fetch(
      `${url}&upload=${upload ? "true" : "false"}`,
      {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ wagon }),
      },
    );
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || "Upload fehlgeschlagen.");
    }
    const data = await resp.json().catch(() => ({}));
    openXmlPopup({
      win: popup.win,
      title: `${actionLabel} · ${sern}`,
      erpLabel: (data.erp_env || erpEnv).toUpperCase(),
      rsrdLabel: (data.rsrd_env || rsrdEnv).toUpperCase(),
      xml: data.xml || "",
      responseText: data.response_text || "",
      responseStatus: data.response_status,
      requestUrl: data.request_url,
      upload,
      statusText: upload ? "Upload abgeschlossen." : "XML erstellt.",
    });
    compareStatus.textContent = `${actionLabel} abgeschlossen.`;
  } catch (err) {
    compareStatus.textContent = err.message || "Upload fehlgeschlagen.";
    openXmlPopup({
      win: popup.win,
      title: `${actionLabel} · ${sern}`,
      erpLabel: erpEnv.toUpperCase(),
      rsrdLabel: rsrdEnv.toUpperCase(),
      xml: "",
      responseText: err.message || "Upload fehlgeschlagen.",
      responseStatus: null,
      requestUrl: null,
      upload,
      statusText: err.message || "Upload fehlgeschlagen.",
    });
  }
};

const loadCompare = async () => {
  const { sern } = parseParams();
  if (!sern) {
    compareStatus.textContent = "Keine Wagennummer angegeben.";
    return;
  }
  const digits = sern.replace(/\\D/g, "");
  const querySern = digits || sern;
  compareTitle.textContent = `Vergleich Wagen ${sern}`;
  updateEnvSwitch();
  compareStatus.textContent = "Lade Vergleich ...";
  try {
    const resp = await fetch(withRsrd2Env("/api/rsrd2/compare?create_upload=false&include_all=true"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ wagons: [querySern] }),
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || "Vergleich fehlgeschlagen.");
    }
    const data = await resp.json().catch(() => ({}));
    const row = Array.isArray(data.rows) ? data.rows[0] : null;
    if (!row) {
      compareStatus.textContent = "Keine Daten gefunden.";
      renderCompare([]);
      return;
    }
    if (row.rsrd_missing) {
      compareStatus.textContent = "RSRD2-Daten fehlen für diesen Wagen.";
      renderCompare([]);
      renderDocuments([]);
      return;
    }
    const diffs = Array.isArray(row.differences) ? row.differences : [];
    const diffCount = diffs.filter((diff) => !diff.equal).length;
    compareStatus.textContent = `Abweichungen: ${diffCount} (Gesamtfelder: ${diffs.length})`;
    renderCompare(diffs);
    renderDocuments(row.documents || []);
  } catch (err) {
    compareStatus.textContent = err.message || "Vergleich fehlgeschlagen.";
    renderCompare([]);
    renderDocuments([]);
  }
};

if (envToggle) {
  envToggle.addEventListener("click", () => {
    currentEnv = currentEnv === "LIVE" ? "TEST" : "LIVE";
    window.localStorage.setItem("sparepart.env", currentEnv);
    updateEnvSwitch();
    loadCompare();
  });
}
if (rsrdEnvToggle) {
  rsrdEnvToggle.addEventListener("click", () => {
    currentRsrdEnv = currentRsrdEnv === "LIVE" ? "TEST" : "LIVE";
    window.localStorage.setItem("sparepart.rsrd_env", currentRsrdEnv);
    updateEnvSwitch();
    loadCompare();
  });
}

if (backBtn) {
  backBtn.addEventListener("click", () => {
    if (window.history.length > 1) {
      window.history.back();
    } else {
      window.location.href = "rsrd2.html";
    }
  });
}

updateEnvSwitch();
loadCompare();

const initXmlPopup = () => {
  const popup = openXmlPopupWindow({
    onUploadTst: () => requestUploadXml({ env: "tst", upload: true, popup }),
    onUploadPrd: () => requestUploadXml({ env: "prd", upload: true, popup }),
  });
  if (!popup) {
    compareStatus.textContent = "Popup blockiert. Bitte Popups erlauben.";
    return;
  }
  requestUploadXml({ env: getRsrdEnvParam(), upload: false, popup });
};

if (xmlBtn) {
  xmlBtn.addEventListener("click", () => {
    initXmlPopup();
  });
}
