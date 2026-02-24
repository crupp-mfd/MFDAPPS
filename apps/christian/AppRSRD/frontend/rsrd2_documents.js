const docsBackBtn = document.getElementById("rsrd2DocsBackBtn");
const docsFileInput = document.getElementById("rsrd2DocsFileInput");
const docsPickBtn = document.getElementById("rsrd2DocsPickBtn");
const docsUploadBtn = document.getElementById("rsrd2DocsUploadBtn");
const docsReloadBtn = document.getElementById("rsrd2DocsReloadBtn");
const docsSelection = document.getElementById("rsrd2DocsSelection");
const docsSelectionList = document.getElementById("rsrd2DocsSelectionList");
const docsStatus = document.getElementById("rsrd2DocsStatus");
const docsTableBody = document.getElementById("rsrd2DocsTableBody");

const runtimeApiConfig = window.__SPAREPART_API_CONFIG__ || {};
const rsrd2BaseUrl = String(runtimeApiConfig.RSRD2_API_BASE_URL || "").replace(/\/+$/, "");
const rsrd2Api = (path) => {
  if (!String(path || "").startsWith("/")) return String(path || "");
  return rsrd2BaseUrl ? `${rsrd2BaseUrl}${path}` : path;
};
const resolveEnvValue = (value) => (value && value.toUpperCase() === "TEST" ? "TEST" : "LIVE");
const resolveEnvParam = (value) => {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) return null;
  if (["test", "tst", "t"].includes(normalized)) return "TEST";
  if (["live", "prd", "p", "l"].includes(normalized)) return "LIVE";
  return null;
};
let currentEnv = resolveEnvValue(window.localStorage.getItem("sparepart.env") || "LIVE");
let currentRsrdEnv = resolveEnvValue(window.localStorage.getItem("sparepart.rsrd_env") || currentEnv);
{
  const params = new URLSearchParams(window.location.search);
  const erpFromUrl = resolveEnvParam(params.get("env"));
  const rsrdFromUrl = resolveEnvParam(params.get("rsrd_env"));
  if (erpFromUrl) {
    currentEnv = erpFromUrl;
    window.localStorage.setItem("sparepart.env", currentEnv);
  }
  if (rsrdFromUrl) {
    currentRsrdEnv = rsrdFromUrl;
    window.localStorage.setItem("sparepart.rsrd_env", currentRsrdEnv);
  }
}
const getErpEnvParam = () => currentEnv.toLowerCase();
const getRsrdEnvParam = () => currentRsrdEnv.toLowerCase();
const appendEnvParam = (url) => {
  const env = getErpEnvParam();
  const joiner = url.includes("?") ? "&" : "?";
  return `${url}${joiner}env=${encodeURIComponent(env)}`;
};

let docsState = [];
let baureiheOptions = [];
let wagenTypOptions = [];

const renderSelectedFiles = () => {
  const files = docsFileInput?.files ? Array.from(docsFileInput.files) : [];
  if (docsSelection) {
    if (!files.length) {
      docsSelection.textContent = "Keine Datei ausgewählt.";
    } else if (files.length === 1) {
      docsSelection.textContent = `1 Datei ausgewählt: ${files[0].name}`;
    } else {
      docsSelection.textContent = `${files.length} Dateien ausgewählt.`;
    }
  }
  if (docsSelectionList) {
    docsSelectionList.innerHTML = files
      .slice(0, 8)
      .map((file) => `<span class="rsrd2-doc-chip">${esc(file.name)}</span>`)
      .join("");
    if (files.length > 8) {
      docsSelectionList.insertAdjacentHTML(
        "beforeend",
        `<span class="rsrd2-doc-chip">+${files.length - 8} weitere</span>`,
      );
    }
  }
};

const setStatus = (message, type = "info") => {
  if (!docsStatus) return;
  docsStatus.textContent = message || "";
  docsStatus.classList.remove("rsrd2-status--error", "rsrd2-status--success");
  if (type === "error") docsStatus.classList.add("rsrd2-status--error");
  if (type === "success") docsStatus.classList.add("rsrd2-status--success");
};

const esc = (value) =>
  String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");

const formatBytes = (size) => {
  const num = Number(size || 0);
  if (!Number.isFinite(num) || num <= 0) return "-";
  if (num < 1024) return `${num} B`;
  if (num < 1024 * 1024) return `${(num / 1024).toFixed(1)} KB`;
  return `${(num / (1024 * 1024)).toFixed(1)} MB`;
};

const normalizeList = (values) => {
  if (!Array.isArray(values)) return [];
  const out = [];
  const seen = new Set();
  for (const item of values) {
    const value = String(item || "").trim();
    if (!value) continue;
    const key = value.toUpperCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(value);
  }
  return out;
};

const renderMultiSelectOptions = (baseOptions, selectedValues) => {
  const selected = normalizeList(selectedValues);
  const merged = normalizeList(baseOptions);
  const mergedKeys = new Set(merged.map((item) => item.toUpperCase()));
  selected.forEach((value) => {
    if (!mergedKeys.has(value.toUpperCase())) merged.push(value);
  });
  if (!merged.length) {
    return `<option value="" disabled>Keine Werte verfügbar</option>`;
  }
  const selectedSet = new Set(selected.map((item) => item.toUpperCase()));
  return merged
    .map((value) => {
      const isSelected = selectedSet.has(value.toUpperCase());
      return `<option value="${esc(value)}"${isSelected ? " selected" : ""}>${esc(value)}</option>`;
    })
    .join("");
};

const renderDocuments = () => {
  if (!docsTableBody) return;
  docsTableBody.innerHTML = "";
  if (!docsState.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 5;
    td.textContent = "Noch keine Dokumente vorhanden.";
    tr.appendChild(td);
    docsTableBody.appendChild(tr);
    return;
  }

  docsState.forEach((doc) => {
    const tr = document.createElement("tr");
    tr.dataset.docId = String(doc.id);
    tr.innerHTML = `
      <td>
        <div class="rsrd2-doc-name">${esc(doc.display_name || doc.original_name || `Dokument ${doc.id}`)}</div>
        <a class="rsrd2-doc-download" href="${esc(appendEnvParam(rsrd2Api(`/api/rsrd2/documents/${doc.id}/download`)))}">Download</a>
      </td>
      <td>
        <div>${esc(doc.uploaded_at || "-")}</div>
        <div class="rsrd2-doc-size">${esc(formatBytes(doc.size_bytes))}</div>
      </td>
      <td>
        <select class="rsrd2-doc-multiselect" data-field="baureihen" multiple size="7">
          ${renderMultiSelectOptions(baureiheOptions, doc.baureihen)}
        </select>
      </td>
      <td>
        <select class="rsrd2-doc-multiselect" data-field="wagen_typen" multiple size="7">
          ${renderMultiSelectOptions(wagenTypOptions, doc.wagen_typen)}
        </select>
      </td>
      <td>
        <div class="rsrd2-doc-actions">
          <button type="button" class="ids-button tertiary" data-action="save">Zuordnung speichern</button>
          <button type="button" class="ids-button tertiary" data-action="delete">Löschen</button>
        </div>
      </td>
    `;
    docsTableBody.appendChild(tr);
  });
};

const loadDocuments = async () => {
  setStatus("Lade Dokumente ...");
  try {
    const resp = await fetch(appendEnvParam(rsrd2Api("/api/rsrd2/documents")));
    if (!resp.ok) throw new Error(await resp.text());
    const data = await resp.json().catch(() => ({}));
    docsState = Array.isArray(data.documents) ? data.documents : [];
    baureiheOptions = normalizeList(data.baureihe_options);
    wagenTypOptions = normalizeList(data.wagen_typ_options);
    renderDocuments();
    setStatus(`Dokumente geladen: ${docsState.length}.`, "success");
  } catch (error) {
    setStatus(error.message || "Dokumente konnten nicht geladen werden.", "error");
  }
};

const uploadDocuments = async () => {
  const files = docsFileInput?.files ? Array.from(docsFileInput.files) : [];
  if (!files.length) {
    setStatus("Bitte mindestens eine Datei auswählen.", "error");
    return;
  }
  let uploaded = 0;
  setStatus(`Upload läuft (${files.length} Datei(en)) ...`);
  for (const file of files) {
    try {
      const resp = await fetch(appendEnvParam(rsrd2Api(`/api/rsrd2/documents/upload?name=${encodeURIComponent(file.name)}`)), {
        method: "POST",
        headers: {
          "Content-Type": file.type || "application/octet-stream",
          "X-File-Name": encodeURIComponent(file.name),
        },
        body: file,
      });
      if (!resp.ok) throw new Error(await resp.text());
      uploaded += 1;
    } catch (error) {
      setStatus(`Fehler bei ${file.name}: ${error.message || "Upload fehlgeschlagen."}`, "error");
      return;
    }
  }
  if (docsFileInput) docsFileInput.value = "";
  renderSelectedFiles();
  setStatus(`Upload abgeschlossen: ${uploaded} Datei(en).`, "success");
  await loadDocuments();
};

const saveAssignments = async (docId, row) => {
  const readSelected = (selector) =>
    Array.from(row.querySelector(selector)?.selectedOptions || [])
      .map((option) => String(option.value || "").trim())
      .filter(Boolean);
  const payload = {
    baureihen: readSelected('[data-field="baureihen"]'),
    wagen_typen: readSelected('[data-field="wagen_typen"]'),
  };
  setStatus(`Speichere Zuordnung für Dokument ${docId} ...`);
  try {
    const resp = await fetch(appendEnvParam(rsrd2Api(`/api/rsrd2/documents/${encodeURIComponent(docId)}/assignments`)), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) throw new Error(await resp.text());
    setStatus(`Zuordnung gespeichert (Dokument ${docId}).`, "success");
    await loadDocuments();
  } catch (error) {
    setStatus(error.message || "Zuordnung konnte nicht gespeichert werden.", "error");
  }
};

const deleteDocument = async (docId) => {
  const confirmed = window.confirm(`Dokument ${docId} wirklich löschen?`);
  if (!confirmed) return;
  setStatus(`Lösche Dokument ${docId} ...`);
  try {
    const resp = await fetch(appendEnvParam(rsrd2Api(`/api/rsrd2/documents/${encodeURIComponent(docId)}`)), {
      method: "DELETE",
    });
    if (!resp.ok) throw new Error(await resp.text());
    setStatus(`Dokument ${docId} gelöscht.`, "success");
    await loadDocuments();
  } catch (error) {
    setStatus(error.message || "Dokument konnte nicht gelöscht werden.", "error");
  }
};

if (docsBackBtn) {
  docsBackBtn.addEventListener("click", () => {
    const params = new URLSearchParams({
      module: "rsrd2",
      env: getErpEnvParam(),
      rsrd_env: getRsrdEnvParam(),
    });
    window.location.href = `rsrd2.html?${params.toString()}`;
  });
}
if (docsPickBtn && docsFileInput) {
  docsPickBtn.addEventListener("click", () => {
    docsFileInput.click();
  });
}
if (docsFileInput) {
  docsFileInput.addEventListener("change", () => {
    renderSelectedFiles();
  });
}
if (docsUploadBtn) {
  docsUploadBtn.addEventListener("click", () => {
    uploadDocuments();
  });
}
if (docsReloadBtn) {
  docsReloadBtn.addEventListener("click", () => {
    loadDocuments();
  });
}
if (docsTableBody) {
  docsTableBody.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-action]");
    if (!button) return;
    const row = button.closest("tr[data-doc-id]");
    if (!row) return;
    const docId = row.dataset.docId;
    if (!docId) return;
    const action = button.dataset.action;
    if (action === "save") {
      saveAssignments(docId, row);
      return;
    }
    if (action === "delete") {
      deleteDocument(docId);
    }
  });
}

renderSelectedFiles();
loadDocuments();
