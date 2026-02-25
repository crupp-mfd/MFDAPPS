const statusText = document.getElementById("statusText");
const loaderSubtitle = document.getElementById("loaderSubtitle");
const progressBar = document.getElementById("progressBar");
const progressWrapper = document.getElementById("progressWrapper");
const progressDetail = document.getElementById("progressDetail");
const loaderPanel = document.getElementById("loaderPanel");
const loaderOverlay = document.getElementById("loaderOverlay");
const loaderIo = document.getElementById("loaderIo");
const loaderSource = document.getElementById("loaderSource");
const loaderTarget = document.getElementById("loaderTarget");
const loaderSteps = document.getElementById("loaderSteps");
const loaderStepsList = document.getElementById("loaderStepsList");
const loaderStepImageWrap = document.getElementById("loaderStepImageWrap");
const loaderStepImage = document.getElementById("loaderStepImage");
const loaderRandomImage = document.getElementById("loaderRandomImage");
const loaderRandomImageWrap = document.getElementById("loaderRandomImageWrap");
const loaderSpinner = document.getElementById("loaderSpinner");
const loaderCloseBtn = document.getElementById("loaderCloseBtn");
const loaderMessageBox = document.getElementById("loaderMessageBox");
const loaderActionBtn = document.getElementById("loaderActionBtn");
const loaderDownloadLink = document.getElementById("loaderDownloadLink");
const mainMenuBtn = document.getElementById("mainMenuBtn");
const mainMenu = document.getElementById("mainMenu");
const envSwitch = document.getElementById("envSwitch");
const envToggle = document.getElementById("envToggle");
const rsrdEnvSwitch = document.getElementById("rsrdEnvSwitch");
const rsrdEnvToggle = document.getElementById("rsrdEnvToggle");
const moduleTitle = document.getElementById("moduleTitle");
const moduleSubtitle = document.getElementById("moduleSubtitle");
const sparepartModule = document.getElementById("sparepartModule");
const rsrd2Module = document.getElementById("rsrd2Module");
const menuItems = document.querySelectorAll(".menu-item");
const headerSearch = document.getElementById("headerSearch");
const rsrd2LoadErpBtn = document.getElementById("rsrd2LoadErpBtn");
const rsrd2LoadErpFullBtn = document.getElementById("rsrd2LoadErpFullBtn");
const rsrd2FetchJsonBtn = document.getElementById("rsrd2FetchJsonBtn");
const rsrd2ProcessJsonBtn = document.getElementById("rsrd2ProcessJsonBtn");
const rsrd2CompareBtn = document.getElementById("rsrd2CompareBtn");
const rsrd2Status = document.getElementById("rsrd2Status");
const rsrd2Log = document.getElementById("rsrd2Log");
const rsrd2TableBody = document.getElementById("rsrd2TableBody");
const rsrd2CountInfo = document.getElementById("rsrd2CountInfo");
const rsrd2PaginationInfo = document.getElementById("rsrd2PaginationInfo");
const rsrd2PrevPage = document.getElementById("rsrd2PrevPage");
const rsrd2NextPage = document.getElementById("rsrd2NextPage");
const rsrd2PageSizeSelect = document.getElementById("rsrd2PageSizeSelect");
const rsrd2FilterSern = document.getElementById("rsrd2FilterSern");
const rsrd2FilterSernOptions = document.getElementById("rsrd2FilterSernOptions");
const rsrd2FilterBaureihe = document.getElementById("rsrd2FilterBaureihe");
const rsrd2FilterBaureiheOptions = document.getElementById("rsrd2FilterBaureiheOptions");
const rsrd2FilterHalter = document.getElementById("rsrd2FilterHalter");
const rsrd2FilterHalterOptions = document.getElementById("rsrd2FilterHalterOptions");
const rsrd2FilterUic = document.getElementById("rsrd2FilterUic");
const rsrd2FilterUicOptions = document.getElementById("rsrd2FilterUicOptions");
const rsrd2FilterStatus = document.getElementById("rsrd2FilterStatus");
const rsrd2GoBtn = document.getElementById("rsrd2GoBtn");
const rsrd2BulkData = document.getElementById("rsrd2BulkData");
const rsrd2BulkDataBtn = document.getElementById("rsrd2BulkDataBtn");
const rsrd2BulkKm = document.getElementById("rsrd2BulkKm");
const rsrd2BulkKmBtn = document.getElementById("rsrd2BulkKmBtn");
const rsrd2BulkDocs = document.getElementById("rsrd2BulkDocs");
const rsrd2BulkDocsBtn = document.getElementById("rsrd2BulkDocsBtn");
const rsrd2BulkTransfer = document.getElementById("rsrd2BulkTransfer");
const rsrd2BulkTransferBtn = document.getElementById("rsrd2BulkTransferBtn");
const rsrd2ClearFiltersBtn = document.getElementById("rsrd2ClearFiltersBtn");
const rsrd2UploadSelectedBtn = document.getElementById("rsrd2UploadSelectedBtn");
const tablePanel = document.getElementById("tablePanel");
const tableHead = document.getElementById("tableHead");
const tableBody = document.getElementById("tableBody");
const countInfo = document.getElementById("countInfo");
const cacheStatusInfo = document.getElementById("cacheStatusInfo");
const exportSelectionBtn = document.getElementById("exportSelectionBtn");
const paginationInfo = document.getElementById("paginationInfo");
const prevBtn = document.getElementById("prevPage");
const nextBtn = document.getElementById("nextPage");
const rollbackFromDbBtn = document.getElementById("rollbackFromDbBtn");
const reloadBtn = document.getElementById("reloadBtn");
// BEGIN WAGON RENNUMBERING
const wagonRenumberBtn = document.getElementById("wagonRenumberBtn");
// END WAGON RENNUMBERING
const mos170AddPropBtn = document.getElementById("mos170AddPropBtn");
const cms100MwnoBtn = document.getElementById("cms100MwnoBtn");
const mos100ChgSernBtn = document.getElementById("mos100ChgSernBtn");
const mos180ApproveBtn = document.getElementById("mos180ApproveBtn");
const mos050MontageBtn = document.getElementById("mos050MontageBtn");
const crs335UpdBtn = document.getElementById("crs335UpdBtn");
const sts046DelBtn = document.getElementById("sts046DelBtn");
const sts046AddBtn = document.getElementById("sts046AddBtn");
const mms240UpdBtn = document.getElementById("mms240UpdBtn");
const cusextAddBtn = document.getElementById("cusextAddBtn");
const pageSizeSelect = document.getElementById("pageSizeSelect");
const wagonsView = document.getElementById("wagonsView");
const objstrkView = document.getElementById("objstrkView");
const swapView = document.getElementById("swapView");
const objstrkHead = document.getElementById("objstrkHead");
const objstrkBody = document.getElementById("objstrkBody");
const objstrkMeta = document.getElementById("objstrkMeta");
const renumberFields = document.getElementById("renumberFields");
const renumberItnoInput = document.getElementById("renumberItno");
const renumberItnoOptions = document.getElementById("renumberItnoOptions");
const renumberSernInput = document.getElementById("renumberSern");
const renumberDateInput = document.getElementById("renumberDate");
const renumberTypeSelect = document.getElementById("renumberType");
const renumberSernHint = document.getElementById("renumberSernHint");
const renumberExecuteBtn = document.getElementById("renumberExecuteBtn");
const renumberInstallBtn = document.getElementById("renumberInstallBtn");
const backToWagonsBtn = document.getElementById("backToWagonsBtn");
const collapseAllBtn = document.getElementById("collapseAllBtn");
const expandAllBtn = document.getElementById("expandAllBtn");
const openSwapFromWagonsBtn = document.getElementById("openSwapDbFromWagons");
const openSwapFromObjBtn = document.getElementById("openSwapDbFromObj");
const swapTableBody = document.getElementById("swapTableBody");
const swapCountInfo = document.getElementById("swapCountInfo");
const swapBackBtn = document.getElementById("swapBackBtn");
const swapExecuteBtn = document.getElementById("swapExecuteBtn");
const swapSelectAllBtn = document.getElementById("swapSelectAllBtn");
const swapClearSelectionBtn = document.getElementById("swapClearSelectionBtn");
const swapPrevPageBtn = document.getElementById("swapPrevPage");
const swapNextPageBtn = document.getElementById("swapNextPage");
const renumberButtons = [
  renumberExecuteBtn,
  renumberInstallBtn,
  wagonRenumberBtn,
  mos170AddPropBtn,
  cms100MwnoBtn,
  mos100ChgSernBtn,
  mos180ApproveBtn,
  mos050MontageBtn,
  crs335UpdBtn,
  sts046DelBtn,
  sts046AddBtn,
  mms240UpdBtn,
  cusextAddBtn,
  rollbackFromDbBtn,
];
const swapPaginationInfo = document.getElementById("swapPaginationInfo");
const partsModal = document.getElementById("partsModal");
const partsModalContent = partsModal ? partsModal.querySelector(".modal-inner") : null;
const partsModalHeader = partsModal ? partsModal.querySelector(".modal-header") : null;
const partsResizeHandle = document.getElementById("partsResizeHandle");
const closePartsModalBtn = document.getElementById("closePartsModalBtn");
const partsModalSubtitle = document.getElementById("partsModalSubtitle");
const partsSearchForm = document.getElementById("partsSearchForm");
const partsFilterType = document.getElementById("partsFilterType");
const partsFilterItem = document.getElementById("partsFilterItem");
const partsFilterSerial = document.getElementById("partsFilterSerial");
const partsFilterFacility = document.getElementById("partsFilterFacility");
const partsFilterBin = document.getElementById("partsFilterBin");
const partsTypeOptions = document.getElementById("partsTypeOptions");
const partsItemOptions = document.getElementById("partsItemOptions");
const partsSerialOptions = document.getElementById("partsSerialOptions");
const partsFacilityOptions = document.getElementById("partsFacilityOptions");
const partsBinOptions = document.getElementById("partsBinOptions");
const partsClearFiltersBtn = document.getElementById("partsClearFiltersBtn");
const partsPrevPageBtn = document.getElementById("partsPrevPage");
const partsNextPageBtn = document.getElementById("partsNextPage");
const partsPaginationInfo = document.getElementById("partsPaginationInfo");
const partsResultsBody = document.getElementById("partsResultsBody");
const serialFilterInput = document.getElementById("serialFilter");
const itemFilterInput = document.getElementById("itemFilter");
const customerFilterInput = document.getElementById("customerFilter");
const customerNameFilterInput = document.getElementById("customerNameFilter");
const typeFilterInput = document.getElementById("typeFilter");
const fulltextInput = document.getElementById("fulltextSearch");
const facilityFilterInput = document.getElementById("facilityFilter");
const binFilterInput = document.getElementById("binFilter");
const clearFiltersBtn = document.getElementById("clearFiltersBtn");
const customerNumbersList = document.getElementById("customerNumbers");
const customerNamesList = document.getElementById("customerNames");
const itemNumbersList = document.getElementById("itemNumbers");
const typeOptionsList = document.getElementById("typeOptions");
const serialOptionsList = document.getElementById("serialOptions");
const facilityOptionsList = document.getElementById("facilityOptions");
const binOptionsList = document.getElementById("binOptions");
const aStatFilterInput = document.getElementById("aStatFilter");
const aStatOptionsList = document.getElementById("aStatOptions");
const aItnoFilterInput = document.getElementById("aItnoFilter");
const aItnoOptionsList = document.getElementById("aItnoOptions");
const aSernFilterInput = document.getElementById("aSernFilter");
const aSernOptionsList = document.getElementById("aSernOptions");
const aAliiFilterInput = document.getElementById("aAliiFilter");
const aAliiOptionsList = document.getElementById("aAliiOptions");
const aEqtpFilterInput = document.getElementById("aEqtpFilter");
const aEqtpOptionsList = document.getElementById("aEqtpOptions");
const bWhloFilterInput = document.getElementById("bWhloFilter");
const bWhloOptionsList = document.getElementById("bWhloOptions");
const bWhslFilterInput = document.getElementById("bWhslFilter");
const bWhslOptionsList = document.getElementById("bWhslOptions");
const bFaciFilterInput = document.getElementById("bFaciFilter");
const bFaciOptionsList = document.getElementById("bFaciOptions");
const cMtrlFilterInput = document.getElementById("cMtrlFilter");
const cMtrlOptionsList = document.getElementById("cMtrlOptions");
const cSernFilterInput = document.getElementById("cSernFilter");
const cSernOptionsList = document.getElementById("cSernOptions");
const wItnoFilterInput = document.getElementById("wItnoFilter");
const wItnoOptionsList = document.getElementById("wItnoOptions");
const wSernFilterInput = document.getElementById("wSernFilter");
const wSernOptionsList = document.getElementById("wSernOptions");
const newItnoInput = document.getElementById("newItnoInput");
const newSernInput = document.getElementById("newSernInput");
const excelUploadInput = document.getElementById("excelUploadInput");
const excelUploadInfo = document.getElementById("excelUploadInfo");
const ausbauDelayInput = document.getElementById("ausbauDelayInput");
const einbauDelayInput = document.getElementById("einbauDelayInput");
const teilenummerGoBtn = document.getElementById("teilenummerGoBtn");
const apiLogBtn = document.getElementById("apiLogBtn");
const apiLogModal = document.getElementById("apiLogModal");
const apiLogContent = document.getElementById("apiLogContent");
const closeApiLogBtn = document.getElementById("closeApiLogBtn");
const clearApiLogBtn = document.getElementById("clearApiLogBtn");
const exportApiLogCsvBtn = document.getElementById("exportApiLogCsvBtn");

const DEFAULT_PAGE_SIZE = 25;
const CHUNK_SIZE = 150;
const MIN_OVERLAY_MS = 900;
const MIN_JOB_OVERLAY_MS = 1500; // New constant for job overlay
const SINGLE_STEP_RETRY_MS = 3000;
const RSRD2_LOG_LIMIT = 200;
const RSRD2_JOB_POLL_MS = 1500;

// ... existing code ...

const hideOverlay = (jobFinished = false) => {
  if (overlayPinnedOpen) return;
  const elapsed = Date.now() - overlayShownAt;
  const minDuration = jobFinished ? MIN_JOB_OVERLAY_MS : MIN_OVERLAY_MS;
  const remaining = Math.max(0, minDuration - elapsed);
  window.setTimeout(() => {
    if (overlayPinnedOpen) return;
    loaderOverlay.classList.add("hidden");
    setOverlayPinned(false);
    setOverlaySpinner(false);
    setOverlayMessage("");
    setOverlayActionVisible(false);
    setOverlayDownloadLink("", "");
    setOverlayProgressVisible(true);
    setRandomOverlayImage(false);
    clearOverlayContext();
  }, remaining);
};

const COLUMN_LABELS_BY_MODULE = {
  sparepart: {
    "SERIENNUMMER": "Seriennummer",
    "BAUREIHE": "Modellreihe",
    "WAGEN-TYP": "Modell-Typ",
    "KUNDEN-NUMMER": "Kd.Nr.",
    "KUNDEN-NAME": "Kundenname",
    "LAGERORT": "StOrt",
    "LAGERPLATZ": "LgOrt",
    "OBJSTRK": "ObjStrk",
  },
  wagenumbau: {
    "SERIENNUMMER": "Seriennummer",
    "BAUREIHE": "Modellreihe",
    "WAGEN-TYP": "Modell-Typ",
    "KUNDEN-NUMMER": "Kd.Nr.",
    "KUNDEN-NAME": "Kundenname",
    "LAGERORT": "StOrt",
    "LAGERPLATZ": "LgOrt",
    "OBJSTRK": "ObjStrk",
  },
  teilenummer: {
    "A_STAT": "STAT",
    "A_ITNO": "ITNO",
    "A_SERN": "SERN",
    "X_EINBAU": "EINBAU",
    "X_AUSBAU_NEU": "AUSBAU NEU",
    "X_EINBAU_NEU": "EINBAU NEU",
    "A_ALII": "ALII",
    "A_EQTP": "EQTP",
    "B_WHLO": "WHLO",
    "B_WHSL": "WHSL",
    "B_FACI": "FACI",
    "C_CFGL": "CFGL",
    "C_MTRL": "HITN",
    "C_SERN": "HSER",
    "W_ITNO": "W_ITNO",
    "W_SERN": "W_SERN",
  },
};
const COLUMN_ORDER_BY_MODULE = {
  sparepart: [
    "SERIENNUMMER",
    "BAUREIHE",
    "WAGEN-TYP",
    "KUNDEN-NUMMER",
    "KUNDEN-NAME",
    "LAGERORT",
    "LAGERPLATZ",
    "OBJSTRK",
  ],
  wagenumbau: [
    "SERIENNUMMER",
    "BAUREIHE",
    "WAGEN-TYP",
    "KUNDEN-NUMMER",
    "KUNDEN-NAME",
    "LAGERORT",
    "LAGERPLATZ",
    "OBJSTRK",
  ],
  teilenummer: [
    "A_ITNO",
    "A_SERN",
    "X_EINBAU",
    "X_AUSBAU_NEU",
    "X_EINBAU_NEU",
    "A_ALII",
    "A_EQTP",
    "B_WHLO",
    "B_WHSL",
    "B_FACI",
    "C_CFGL",
    "C_MTRL",
    "C_SERN",
    "W_ITNO",
    "W_SERN",
  ],
};
const getColumnOrder = () => COLUMN_ORDER_BY_MODULE[currentModule] || COLUMN_ORDER_BY_MODULE.sparepart;
const getColumnLabels = () => COLUMN_LABELS_BY_MODULE[currentModule] || COLUMN_LABELS_BY_MODULE.sparepart;
const getFilterFields = () => FILTER_FIELDS_BY_MODULE[currentModule] || FILTER_FIELDS_BY_MODULE.sparepart;
const getAllFilterFields = () => Object.values(FILTER_FIELDS_BY_MODULE).flat();

const OBJSTRK_COLUMNS_BY_MODULE = {
  sparepart: ["MFGL", "TX40", "ITDS", "ITNO", "SER2", "MVA1", "ERSATZ_ITNO", "ERSATZ_SERN", "PARTS"],
  wagenumbau: ["MFGL", "TX40", "ITDS", "ITNO", "SER2"],
};
const OBJSTRK_SEARCH_COLUMNS = ["TX40", "ITDS"];
const OBJSTRK_FIELD_SOURCES = {
  MFGL: ["CFGL", "MFGL"],
};
const OBJSTRK_LABELS = {
  MFGL: "Position",
  TX40: "BaugruppeID",
  ITDS: "Baugruppe",
  ITNO: "TeileNr",
  SER2: "Seriennummer",
  MVA1: "Kilometer",
  ERSATZ_ITNO: "Neu-ArtNr",
  ERSATZ_SERN: "Neu-SerNr",
  PARTS: "Tausch",
};

const FILTER_FIELDS_BY_MODULE = {
  sparepart: [
    { key: "KUNDEN-NUMMER", input: customerFilterInput, list: customerNumbersList },
    { key: "KUNDEN-NAME", input: customerNameFilterInput, list: customerNamesList },
    { key: "BAUREIHE", input: itemFilterInput, list: itemNumbersList },
    { key: "WAGEN-TYP", input: typeFilterInput, list: typeOptionsList },
    { key: "SERIENNUMMER", input: serialFilterInput, list: serialOptionsList },
    { key: "LAGERORT", input: facilityFilterInput, list: facilityOptionsList },
    { key: "LAGERPLATZ", input: binFilterInput, list: binOptionsList },
  ],
  wagenumbau: [
    { key: "KUNDEN-NUMMER", input: customerFilterInput, list: customerNumbersList },
    { key: "KUNDEN-NAME", input: customerNameFilterInput, list: customerNamesList },
    { key: "BAUREIHE", input: itemFilterInput, list: itemNumbersList },
    { key: "WAGEN-TYP", input: typeFilterInput, list: typeOptionsList },
    { key: "SERIENNUMMER", input: serialFilterInput, list: serialOptionsList },
    { key: "LAGERORT", input: facilityFilterInput, list: facilityOptionsList },
    { key: "LAGERPLATZ", input: binFilterInput, list: binOptionsList },
  ],
  teilenummer: [
    { key: "A_STAT", input: aStatFilterInput, list: aStatOptionsList },
    { key: "A_ITNO", input: aItnoFilterInput, list: aItnoOptionsList },
    { key: "A_SERN", input: aSernFilterInput, list: aSernOptionsList },
    { key: "A_ALII", input: aAliiFilterInput, list: aAliiOptionsList },
    { key: "A_EQTP", input: aEqtpFilterInput, list: aEqtpOptionsList },
    { key: "B_WHLO", input: bWhloFilterInput, list: bWhloOptionsList },
    { key: "B_WHSL", input: bWhslFilterInput, list: bWhslOptionsList },
    { key: "B_FACI", input: bFaciFilterInput, list: bFaciOptionsList },
    { key: "C_MTRL", input: cMtrlFilterInput, list: cMtrlOptionsList },
    { key: "C_SERN", input: cSernFilterInput, list: cSernOptionsList },
    { key: "W_ITNO", input: wItnoFilterInput, list: wItnoOptionsList },
    { key: "W_SERN", input: wSernFilterInput, list: wSernOptionsList },
  ],
};

const resolveEnvValue = (value) => {
  const normalized = String(value || "").trim().toUpperCase();
  if (normalized === "TEST" || normalized === "TST") return "TEST";
  if (normalized === "LIVE" || normalized === "PRD" || normalized === "PROD") return "LIVE";
  return "LIVE";
};
let currentEnv = resolveEnvValue(window.localStorage.getItem("sparepart.env") || "LIVE");
let currentRsrdEnv = resolveEnvValue(window.localStorage.getItem("sparepart.rsrd_env") || currentEnv);
const persistEnvPreferences = () => {
  window.localStorage.setItem("sparepart.env", currentEnv);
  window.localStorage.setItem("sparepart.rsrd_env", currentRsrdEnv);
};
persistEnvPreferences();

const getErpEnvParam = () => currentEnv.toLowerCase();
const getRsrdEnvParam = () => currentRsrdEnv.toLowerCase();

const stripTrailingSlash = (value) => String(value || "").replace(/\/+$/, "");
const runtimeApiConfig = window.__SPAREPART_API_CONFIG__ || {};
const API_BASE_URLS = {
  core: stripTrailingSlash(runtimeApiConfig.CORE_API_BASE_URL || ""),
  rsrd2: stripTrailingSlash(runtimeApiConfig.RSRD2_API_BASE_URL || ""),
  goldenview: stripTrailingSlash(runtimeApiConfig.GOLDENVIEW_API_BASE_URL || ""),
};

const withServiceBase = (service, path) => {
  const targetPath = String(path || "");
  if (!targetPath.startsWith("/")) return targetPath;
  const base = API_BASE_URLS[service] || "";
  if (!base) return targetPath;
  return `${base}${targetPath}`;
};

const coreApi = (path) => withServiceBase("core", path);
const rsrd2Api = (path) => withServiceBase("rsrd2", path);
const goldenviewApi = (path) => withServiceBase("goldenview", path);

const appendQueryParam = (url, key, value) => {
  const joiner = url.includes("?") ? "&" : "?";
  return `${url}${joiner}${encodeURIComponent(key)}=${encodeURIComponent(value)}`;
};

const withEnv = (url) => appendQueryParam(coreApi(url), "env", getErpEnvParam());
const withErpEnv = (url) => appendQueryParam(rsrd2Api(url), "env", getErpEnvParam());
const withRsrdEnv = (url) => appendQueryParam(rsrd2Api(url), "env", getRsrdEnvParam());
const withRsrd2Env = (url) =>
  appendQueryParam(appendQueryParam(rsrd2Api(url), "env", getErpEnvParam()), "rsrd_env", getRsrdEnvParam());
const MODULE_META = {
  sparepart: {
    title: "MFD Automation",
    subtitle: "MFDApps · Objektstrukturtausch",
    showSearch: true,
    loaderSubtitle: "Objektstrukturtausch",
  },
  teilenummer: {
    title: "MFD Automation",
    subtitle: "MFDApps · Teilenummer ändern",
    showSearch: true,
    loaderSubtitle: "Teilenummer ändern",
  },
  wagenumbau: {
    title: "MFD Automation",
    subtitle: "MFDApps · Wagenumbau",
    showSearch: true,
    loaderSubtitle: "Wagenumbau",
  },
  rsrd2: {
    title: "MFD Rail Automatisation",
    subtitle: "RSRD2 Sync",
    showSearch: false,
    loaderSubtitle: "RSRD2 Sync",
  },
};
const WAGON_MODULES = {
  sparepart: {
    table: "wagons",
    metaKey: "wagons",
    reloadUrl: "/api/reload",
    reloadOnStart: false,
    includeSpareparts: true,
  },
  teilenummer: {
    table: "TEILENUMMER",
    metaKey: "teilenummer",
    reloadUrl: "/api/teilenummer/reload",
    reloadOnStart: false,
    includeSpareparts: false,
  },
  wagenumbau: {
    table: "Wagenumbau_Wagons",
    metaKey: "wagenumbau_wagons",
    reloadUrl: "/api/reload",
    reloadOnStart: false,
    includeSpareparts: false,
  },
};
const getObjStrkColumns = () => OBJSTRK_COLUMNS_BY_MODULE[currentModule] || OBJSTRK_COLUMNS_BY_MODULE.sparepart;
const escapeHtml = (value) =>
  String(value).replace(/[&<>"']/g, (ch) =>
    ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    }[ch] || ch),
  );

const toTeilenummerKey = (itno, sern) =>
  `${String(itno || "").trim().toUpperCase()}||${String(sern || "").trim().toUpperCase()}`;

const formatYyyyMmDd = (value) => {
  const digits = String(value ?? "").replace(/\D/g, "");
  if (digits.length < 8) return "";
  return `${digits.slice(6, 8)}.${digits.slice(4, 6)}.${digits.slice(0, 4)}`;
};

const formatHhMmSs = (value) => {
  const digits = String(value ?? "").replace(/\D/g, "");
  if (!digits) return "";
  const padded = digits.padStart(6, "0").slice(0, 6);
  return `${padded.slice(0, 2)}:${padded.slice(2, 4)}:${padded.slice(4, 6)}`;
};

const getTeilenummerDateTimeValue = (row, key) => {
  if (!row || currentModule !== "teilenummer") return { date: "", time: "" };
  const isRealtimeDelay = (value) => {
    const text = String(value ?? "").trim();
    if (!text) return true;
    const num = Number.parseInt(text, 10);
    return Number.isFinite(num) && num === 0;
  };
  if (key === "X_EINBAU") {
    return { date: formatYyyyMmDd(row.RGDT), time: formatHhMmSs(row.RGTM) };
  }
  if (key === "X_AUSBAU_NEU") {
    if (isRealtimeDelay(row.OUT_DELAY_MIN)) return { date: "Timestamp", time: "" };
    return { date: formatYyyyMmDd(row.PLAN_OUT_DATE), time: formatHhMmSs(row.PLAN_OUT_TIME) };
  }
  if (key === "X_EINBAU_NEU") {
    if (isRealtimeDelay(row.IN_DELAY_MIN)) return { date: "Timestamp", time: "" };
    return { date: formatYyyyMmDd(row.PLAN_IN_DATE), time: formatHhMmSs(row.PLAN_IN_TIME) };
  }
  return { date: "", time: "" };
};

const updateExcelUploadInfo = () => {
  if (!excelUploadInfo) return;
  const selectedFile = excelUploadInput?.files?.[0]?.name || "";
  const rawLabel = String(selectedFile || teilenummerExcelFilename || "-").trim() || "-";
  let shortLabel = rawLabel;
  if (rawLabel !== "-" && rawLabel.length > 25) {
    shortLabel = `${rawLabel.slice(0, 25)}...`;
  }
  excelUploadInfo.textContent = shortLabel;
  excelUploadInfo.title = rawLabel;
};

const showTeilenummerExcelValidationPopup = (payload, hasError = false) => {
  const checks = Array.isArray(payload?.checks) ? payload.checks : [];
  const validation = payload?.validation || {};
  const checked = Number(validation.checked || checks.length || 0);
  const ok = Number(validation.ok || checks.filter((entry) => entry?.allowed).length || 0);
  const failed = Number(validation.failed || Math.max(0, checked - ok));
  const isWarningCheck = (entry) => String(entry?.message || "").trim().toUpperCase().startsWith("WARN:");
  const warningChecks = checks.filter((entry) => isWarningCheck(entry));
  const warningCount = warningChecks.length;
  const lines = [];
  const formatCheckLine = (entry) => {
    const row = entry?.row ?? "-";
    const oldPair = `${entry?.old_itno || "-"} / ${entry?.old_sern || "-"}`;
    const newPair = `${entry?.new_itno || "-"} / ${entry?.new_sern || "-"}`;
    const hitn = entry?.hitn || "-";
    const status = isWarningCheck(entry) ? "WARN" : (entry?.allowed ? "OK" : "NOK");
    const message = entry?.message || "-";
    return `Zeile ${row} | ALT ${oldPair} -> NEU ${newPair} | HITN ${hitn} | ${status} | ${message}`;
  };
  if (hasError) {
    lines.push(`Excel-Validierung mit Fehlern: geprüft=${checked}, ok=${ok}, warnungen=${warningCount}, fehler=${failed}`);
    const failedChecks = checks.filter((entry) => !entry?.allowed);
    if (failedChecks.length) {
      lines.push("");
      lines.push("Prüflog:");
      failedChecks.forEach((entry) => lines.push(formatCheckLine(entry)));
    }
    const errors = Array.isArray(payload?.errors) ? payload.errors : [];
    if (errors.length) {
      lines.push("");
      lines.push("Fehler:");
      errors.forEach((entry) => lines.push(String(entry || "")));
    } else if (failedChecks.length) {
      failedChecks
        .filter((entry) => !entry?.allowed)
        .forEach((entry) => lines.push(`Zeile ${entry?.row ?? "-"}: ${entry?.message || "Fehler"}`));
    }
  } else {
    lines.push(`Excel wurde geprüft. geprüft=${checked}, warnungen=${warningCount}, fehler=${failed}`);
    if (warningChecks.length) {
      lines.push("");
      lines.push("Warnungen (werden im Lauf per MOS450MI/AddComponent angelegt):");
      warningChecks.forEach((entry) => lines.push(formatCheckLine(entry)));
    }
  }
  setOverlayMessage(lines.join("\n"));
  setOverlaySpinner(false);
  setOverlayProgressVisible(false);
  setOverlayPinned(true);
  setOverlayActionVisible(false);
};

const parseDelayMinutes = (rawValue) => {
  const text = String(rawValue ?? "").trim();
  if (!text) return null;
  const num = Number.parseInt(text, 10);
  if (!Number.isFinite(num) || num < 0) {
    throw new Error("Verzögerung muss eine nicht-negative ganze Zahl sein.");
  }
  return num;
};

const currentDelayPayload = () => ({
  out_delay_min: parseDelayMinutes(ausbauDelayInput?.value),
  in_delay_min: parseDelayMinutes(einbauDelayInput?.value),
});

const buildExportTimestamp = () => {
  const now = new Date();
  const pad = (value) => String(value).padStart(2, "0");
  return `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
};

const buildTeilenummerExportRows = () => {
  const columnOrder = getColumnOrder();
  const labels = getColumnLabels();
  const columns = columnOrder.map((key) => labels[key] || key);
  const rows = wagons.map((wagon) => {
    const out = {};
    out.__W_STAT = wagon.W_STAT ?? "";
    out.__A_STAT = wagon.A_STAT ?? "";
    columnOrder.forEach((key, index) => {
      const label = columns[index];
      if (key === "X_EINBAU" || key === "X_AUSBAU_NEU" || key === "X_EINBAU_NEU") {
        const dt = getTeilenummerDateTimeValue(wagon, key);
        out[label] = dt.time ? `${dt.date || "-"} ${dt.time}` : (dt.date || "-");
      } else {
        out[label] = wagon[key] ?? "";
      }
    });
    return out;
  });
  return { columns, rows };
};

const exportTeilenummerSelection = async () => {
  if (currentModule !== "teilenummer") return;
  if (!wagons.length) {
    showError("Keine Datensätze für Export vorhanden.");
    return;
  }
  setStatus("Excel wird erstellt ...");
  showOverlay({ showRandomImage: false, showProgress: false });
  setOverlaySpinner(true);
  setOverlayMessage("Excel wird erstellt ...");
  setOverlayProgressVisible(false);
  setOverlayActionVisible(false);
  setOverlayDownloadLink("", "");
  try {
    const payload = buildTeilenummerExportRows();
    const envLabel = currentDataEnvLabel();
    const filename = `Wagenuebersicht_${envLabel}_${buildExportTimestamp()}.xlsx`;
    const response = await fetch(withEnv("/api/teilenummer/export_xlsx"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        filename,
        columns: payload.columns,
        rows: payload.rows,
      }),
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(text || "Excel-Export fehlgeschlagen.");
    }
    const blob = await response.blob();
    if (excelExportObjectUrl) {
      URL.revokeObjectURL(excelExportObjectUrl);
    }
    excelExportObjectUrl = URL.createObjectURL(blob);
    setStatus("Excel wurde erstellt.");
    setOverlaySpinner(false);
    setOverlayMessage("Excel wurde erstellt.");
    setOverlayProgressVisible(false);
    setOverlayDownloadLink(excelExportObjectUrl, "Download Excel", filename);
    setOverlayPinned(true);
    setOverlayActionVisible(false);
  } catch (error) {
    setOverlaySpinner(false);
    setOverlayMessage(error.message || "Excel-Export fehlgeschlagen.");
    setOverlayProgressVisible(false);
    setOverlayPinned(true);
    setOverlayActionVisible(false);
    showError(error.message || "Excel-Export fehlgeschlagen.");
  }
};

let envMeta = null;
let envMetaPromise = null;
const expectedMetaEnv = () => (getErpEnvParam() === "live" ? "prd" : "tst");
const hasCompassSource = (meta) => Boolean(String(meta?.urls?.compass || "").trim());
const currentDataEnvLabel = () => (getErpEnvParam() === "live" ? "PRD" : "TST");

const updateCountInfo = () => {
  if (!countInfo) return;
  const sourceLabel = currentDataEnvLabel();
  if (!allWagons.length) {
    countInfo.textContent =
      `Keine Datenvorhanden (Quelle: ${sourceLabel}). Bitte drücken Sie "Daten aus PRD / TST aktualisieren".`;
    return;
  }
  const suffix = teilenummerExcelMappings.size ? " · Excel-Filter aktiv" : "";
  countInfo.textContent = `${wagons.length} von ${allWagons.length} Datensätzen (Quelle: ${sourceLabel})${suffix}`;
};

const formatCacheStatusTime = (value) => {
  if (!value) return "Zeit unbekannt";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat("de-CH", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(parsed);
};

const formatCacheStatusRow = (label, payload) => {
  if (!payload || payload.source === "missing") return `Datenstand ${label}: keine Daten`;
  const rowCount = Number(payload.row_count || 0).toLocaleString("de-CH");
  if (!payload.loaded_at) return `Datenstand ${label}: ${rowCount} Datensätze (Zeit unbekannt)`;
  return `Datenstand ${label}: ${formatCacheStatusTime(payload.loaded_at)} (${rowCount} Datensätze)`;
};

const refreshCacheStatusInfo = async () => {
  if (!cacheStatusInfo) return;
  const table = getWagonModuleConfig(currentModule).table || WAGON_MODULES.sparepart.table;
  const currentLabel = currentDataEnvLabel();
  cacheStatusInfo.textContent = `Datenstand ${currentLabel} wird geladen ...`;
  try {
    const payload = await fetchJSON(withEnv(`/api/meta/cache_status?table=${encodeURIComponent(table)}`));
    const currentPayload = currentLabel === "PRD" ? payload?.prd : payload?.tst;
    cacheStatusInfo.textContent = formatCacheStatusRow(currentLabel, currentPayload);
  } catch (error) {
    console.error("Konnte Cache-Status nicht laden", error);
    cacheStatusInfo.textContent = `Datenstand ${currentLabel} derzeit nicht verfügbar.`;
  }
};

const refreshEnvMeta = async () => {
  if (!envMetaPromise) {
    envMetaPromise = fetchJSON(withEnv("/api/meta/targets"))
      .then((data) => {
        envMeta = data || null;
        return envMeta;
      })
      .catch((error) => {
        console.error("Konnte Metadaten nicht laden", error);
        envMeta = null;
        return null;
      })
      .finally(() => {
        envMetaPromise = null;
      });
  }
  return envMetaPromise;
};

const ensureEnvMeta = async () => {
  if (envMeta && envMeta.env === expectedMetaEnv()) return envMeta;
  return refreshEnvMeta();
};

const formatOverlayValue = (value) => {
  if (Array.isArray(value)) {
    return value.filter(Boolean).join(" + ");
  }
  return value || "";
};

const buildSqliteUrl = (tableName) => {
  const base = envMeta?.urls?.sqlite || "";
  if (!base) return "";
  return tableName ? `${base}#${tableName}` : base;
};

const buildSqliteUrlFor = (meta, tableName) => {
  const base = meta?.urls?.sqlite || "";
  if (!base) return "";
  return tableName ? `${base}#${tableName}` : base;
};

let rsrd2Meta = {
  erp: null,
  rsrd: null,
  erpEnv: null,
  rsrdEnv: null,
  promise: null,
};

const fetchEnvMetaFor = async (envParam) => {
  const url = appendQueryParam(coreApi("/api/meta/targets"), "env", envParam);
  return fetchJSON(url);
};

const ensureRsrd2Meta = async () => {
  const erpEnv = getErpEnvParam();
  const rsrdEnv = getRsrdEnvParam();
  if (
    rsrd2Meta.erp &&
    rsrd2Meta.rsrd &&
    rsrd2Meta.erpEnv === erpEnv &&
    rsrd2Meta.rsrdEnv === rsrdEnv
  ) {
    return rsrd2Meta;
  }
  if (rsrd2Meta.promise) return rsrd2Meta.promise;
  rsrd2Meta.promise = Promise.all([fetchEnvMetaFor(erpEnv), fetchEnvMetaFor(rsrdEnv)])
    .then(([erp, rsrd]) => {
      rsrd2Meta = {
        erp,
        rsrd,
        erpEnv,
        rsrdEnv,
        promise: null,
      };
      return rsrd2Meta;
    })
    .catch((error) => {
      console.error("Konnte RSRD2 Metadaten nicht laden", error);
      rsrd2Meta = { erp: null, rsrd: null, erpEnv, rsrdEnv, promise: null };
      return rsrd2Meta;
    })
    .finally(() => {
      rsrd2Meta.promise = null;
    });
  return rsrd2Meta.promise;
};

const getWagonModuleConfig = (module) => WAGON_MODULES[module] || WAGON_MODULES.sparepart;
const getWagonTableMeta = (module) => envMeta?.tables?.[getWagonModuleConfig(module).metaKey];
const withTable = (url, tableName) =>
  `${url}${url.includes("?") ? "&" : "?"}table=${encodeURIComponent(tableName)}`;

const setOverlayContext = ({ source, target }) => {
  if (!loaderSource || !loaderTarget || !loaderIo) return;
  const sourceText = formatOverlayValue(source);
  const targetText = formatOverlayValue(target);
  loaderSource.textContent = sourceText || "-";
  loaderTarget.textContent = targetText || "-";
  loaderIo.classList.toggle("hidden", !sourceText && !targetText);
};

const clearOverlayContext = () => {
  setOverlayContext({ source: "", target: "" });
};

let allWagons = [];
let wagons = [];
let currentPage = 1;
let totalRows = 0;
let pageSize = DEFAULT_PAGE_SIZE;
let objStrkRows = [];
let objStrkRoots = [];
let objStrkNodesById = new Map();
const objStrkExpanded = new Set();
let objStrkSearchText = "";
let swapRows = [];
let swapPage = 1;
let swapSelected = new Set();
const getInitialModule = () => {
  const params = new URLSearchParams(window.location.search);
  const paramModule = params.get("module");
  const bodyModule = document.body?.dataset?.defaultModule;
  if (paramModule === "rsrd2" || paramModule === "wagenumbau" || paramModule === "teilenummer") return paramModule;
  if (bodyModule === "rsrd2" || bodyModule === "wagenumbau" || bodyModule === "teilenummer") return bodyModule;
  return "sparepart";
};
let currentModule = getInitialModule();
let currentView = "wagons";
let rsrd2Loaded = false;
let rsrd2OverviewLoaded = false;
let rsrd2OverviewPage = 1;
let rsrd2OverviewPageSize = 25;
let rsrd2OverviewTotal = 0;
let rsrd2OverviewRows = [];
let rsrd2UploadSelected = new Set();
let rsrd2FilterTimer = null;
let currentPartsEqtp = "";
let currentPartsContext = "";
let sparePartsRows = [];
let sparePartsPage = 1;
let sparePartsOptions = { item: [], serial: [], facility: [], bin: [] };
let partsDragState = null;
let partsResizeState = null;
let partsFilterTimer = null;
let renumberCheckTimer = null;
let renumberJobId = null;
let renumberJobTimer = null;
let renumberResultCount = 0;
let renumberJobMode = null;
let renumberSequence = null;
let renumberSequenceIndex = -1;
let renumberSequenceTransition = false;
let singleStepLoopActive = false;
let lastRenumberJobStatus = null;
let lastRenumberJobError = null;
let currentWagonItem = "";
let currentWagonSerial = "";
let currentPartsNodeId = null;
let currentPartsNode = null;
let currentOriginalItno = "";
let currentOriginalSern = "";
let sparePartUser = window.localStorage.getItem("sparepart.user") || "";
let teilenummerLogCount = 0;
let teilenummerExcelMappings = new Map();
let teilenummerExcelFilename = "";
const rsrd2JobOffsets = {};
const rsrd2JobStartTimes = {};

let overlayShownAt = 0;
let overlayPinnedOpen = false;
let excelExportObjectUrl = "";

const disableAllRenumberButtons = () => {
  renumberButtons.forEach((button) => {
    if (button) button.disabled = true;
  });
};

const enableAllRenumberButtons = () => {
  renumberButtons.forEach((button) => {
    if (button) button.disabled = false;
  });
};

const setRandomOverlayImage = (enabled) => {
  if (!loaderRandomImage || !loaderRandomImageWrap) return;
  loaderRandomImageWrap.classList.toggle("hidden", !enabled);
  if (enabled) {
    const imageIndex = Math.floor(Math.random() * 6) + 1;
    loaderRandomImage.src = `bilder/${imageIndex}-Bild.png`;
    loaderRandomImage.alt = `Statusbild ${imageIndex}`;
  } else {
    loaderRandomImage.removeAttribute("src");
    loaderRandomImage.alt = "";
  }
};

const setOverlaySpinner = (enabled) => {
  if (!loaderSpinner) return;
  loaderSpinner.classList.toggle("hidden", !enabled);
};

const setOverlayPinned = (enabled) => {
  overlayPinnedOpen = Boolean(enabled);
  if (loaderCloseBtn) {
    loaderCloseBtn.classList.toggle("hidden", !overlayPinnedOpen);
  }
};

const setOverlayMessage = (text) => {
  if (!loaderMessageBox) return;
  const value = String(text || "").trim();
  loaderMessageBox.textContent = value;
  loaderMessageBox.classList.toggle("hidden", !value);
};

const setOverlayActionVisible = (visible) => {
  if (!loaderActionBtn) return;
  loaderActionBtn.classList.toggle("hidden", !visible);
};

const setOverlayProgressVisible = (visible) => {
  if (progressWrapper) progressWrapper.classList.toggle("hidden", !visible);
  if (progressDetail) progressDetail.classList.toggle("hidden", !visible);
};

const setOverlayDownloadLink = (url, label, filename = "") => {
  if (!loaderDownloadLink) return;
  const hasUrl = Boolean(url);
  loaderDownloadLink.classList.toggle("hidden", !hasUrl);
  if (!hasUrl) {
    loaderDownloadLink.removeAttribute("href");
    loaderDownloadLink.removeAttribute("download");
    loaderDownloadLink.textContent = "Download Excel";
    return;
  }
  loaderDownloadLink.href = url;
  if (filename) loaderDownloadLink.setAttribute("download", filename);
  loaderDownloadLink.textContent = label || "Download Excel";
};

const setTeilenummerRandomImage = () => {
  if (!loaderRandomImage || !loaderRandomImageWrap) return;
  const imageIndex = Math.floor(Math.random() * 6) + 1;
  loaderRandomImageWrap.classList.remove("hidden");
  loaderRandomImage.onerror = () => {
    loaderRandomImage.onerror = null;
    loaderRandomImage.src = `bilder/${imageIndex}-Bild.png`;
  };
  loaderRandomImage.src = `bilder/${imageIndex}-Bild.jpg`;
  loaderRandomImage.alt = `Statusbild ${imageIndex}`;
};

const showOverlay = ({ showRandomImage = true, showProgress = true } = {}) => {
  overlayShownAt = Date.now();
  setOverlayPinned(false);
  loaderOverlay.classList.remove("hidden");
  setOverlayMessage("");
  setOverlayActionVisible(false);
  setOverlayDownloadLink("", "");
  setOverlayProgressVisible(showProgress);
  const shouldShowImage = showRandomImage && !renumberSequence;
  setRandomOverlayImage(shouldShowImage);
  setOverlaySpinner(!shouldShowImage);
};

const showOverlayWithImage = (src, alt, statusMessage) => {
  overlayShownAt = Date.now();
  setOverlayPinned(false);
  loaderOverlay.classList.remove("hidden");
  setOverlayMessage("");
  setOverlayActionVisible(false);
  setOverlayDownloadLink("", "");
  setOverlayProgressVisible(true);
  setOverlaySpinner(false);
  if (loaderRandomImageWrap && loaderRandomImage) {
    loaderRandomImageWrap.classList.remove("hidden");
    loaderRandomImage.src = src;
    loaderRandomImage.alt = alt;
  }
  if (statusMessage) {
    setStatus(statusMessage);
  }
};



const setIndeterminate = (enabled) => {
  progressBar.classList.toggle("indeterminate", enabled);
  if (enabled) {
    progressDetail.textContent = "";
  }
};

const setStatus = (text) => {
  statusText.textContent = text;
};

const updateProgress = (value, total) => {
  setIndeterminate(false);
  const safeTotal = Math.max(total, 1);
  const percent = Math.min(100, Math.round((value / safeTotal) * 100));
  progressBar.style.width = `${percent}%`;
  const stepInfo = getSequenceStepInfo();
  progressDetail.textContent = stepInfo
    ? `${stepInfo.index}/${stepInfo.total} - ${value} / ${total}`
    : `${value} / ${total}`;
};

const resetProgress = () => {
  if (!progressBar || !progressDetail) return;
  progressBar.style.width = "0%";
  progressDetail.textContent = "0 / 0";
};

const buildRenumberSequence = () => [
  { label: "MOS125 Ausbau", endpoint: "/api/renumber/run", mode: "out" },
  { label: "WAGEN UMNUMMERIEREN", endpoint: "/api/renumber/wagon", mode: "wagon_renumber" },
  { label: "MOS170 AddProp", endpoint: "/api/renumber/mos170", mode: "mos170" },
  { label: "MOS170 PLPN", endpoint: "/api/renumber/mos170/plpn", mode: "mos170_plpn" },
  { label: "MOS100 Chg_SERN", endpoint: "/api/renumber/mos100", mode: "mos100" },
  { label: "MOS180 Approve", endpoint: "/api/renumber/mos180", mode: "mos180" },
  { label: "MOS050 Montage", endpoint: "/api/renumber/mos050", mode: "mos050" },
  { label: "STS046 DelGenItem", endpoint: "/api/renumber/sts046", mode: "sts046" },
  { label: "STS046 AddGenItem", endpoint: "/api/renumber/sts046/add", mode: "sts046_add" },
  { label: "MMS240 Upd", endpoint: "/api/renumber/mms240", mode: "mms240" },
  { label: "CUSEXT AddFieldValue", endpoint: "/api/renumber/cusext", mode: "cusext" },
  { label: "MOS125 Einbau", endpoint: "/api/renumber/install", mode: "in" },
];

const getSingleRenumberStep = (mode) => {
  const sequence = buildRenumberSequence();
  const step = sequence.find((entry) => entry.mode === mode);
  if (step) return step;
  if (mode === "rollback") {
    return { label: "Roll-Back", endpoint: "/api/renumber/rollback", mode: "rollback" };
  }
  return null;
};

const buildWagonRenumberSequence = () => [
  { label: "MOS170 AddProp", endpoint: "/api/renumber/mos170", mode: "mos170" },
  { label: "MOS170 PLPN", endpoint: "/api/renumber/mos170/plpn", mode: "mos170_plpn" },
  { label: "MOS100 Chg_SERN", endpoint: "/api/renumber/mos100", mode: "mos100" },
  { label: "MOS180 Approve", endpoint: "/api/renumber/mos180", mode: "mos180" },
  { label: "CRS335 Upd", endpoint: "/api/renumber/crs335", mode: "crs335" },
];

const getSequenceStepInfo = () => {
  if (!renumberSequence || renumberSequenceIndex < 0) return null;
  const step = renumberSequence[renumberSequenceIndex];
  if (!step) return null;
  return { index: renumberSequenceIndex + 1, total: renumberSequence.length, label: step.label };
};

const formatSequenceStatus = (stepInfo, detail) => {
  if (!stepInfo) return detail;
  const prefix = `Schritt ${stepInfo.index}/${stepInfo.total}: ${stepInfo.label}`;
  if (!detail) return prefix;
  return `${prefix} - ${detail}`;
};

const formatSequenceStatusLine = (stepInfo, processed, total) => {
  if (!stepInfo) return "";
  const base = `Schritt ${stepInfo.index}/${stepInfo.total}: ${stepInfo.label}`;
  if (total > 0) {
    return `${base} · ${processed}/${total}`;
  }
  return base;
};

const renderSequenceSteps = () => {
  if (!loaderSteps || !loaderStepsList) return;
  if (!renumberSequence || !renumberSequence.length) {
    loaderSteps.classList.add("hidden");
    loaderStepsList.innerHTML = "";
    if (loaderStepImageWrap) {
      loaderStepImageWrap.classList.add("hidden");
      loaderStepImageWrap.style.marginTop = "0px";
    }
    if (loaderStepImage) {
      loaderStepImage.removeAttribute("src");
      loaderStepImage.alt = "";
    }
    return;
  }
  loaderSteps.classList.remove("hidden");
  const activeIndex = Math.max(renumberSequenceIndex, 0);
  loaderStepsList.innerHTML = renumberSequence
    .map((step, index) => {
      const isActive = index === activeIndex;
      const isDone = index < activeIndex;
      const classes = ["loader-step"];
      if (isActive) classes.push("loader-step--active");
      if (isDone) classes.push("loader-step--done");
      const indexLabel = String(index + 1).padStart(2, "0");
      return `
        <li class="${classes.join(" ")}">
          <span class="loader-step-index">${indexLabel}</span>
          <span class="loader-step-dot"></span>
          <span>${step.label}</span>
        </li>`;
    })
    .join("");
  if (loaderStepImageWrap && loaderStepImage) {
    const imageIndex = Math.min(Math.floor(activeIndex / 2) + 1, 6);
    loaderStepImage.src = `bilder/${imageIndex}-Bild.png`;
    loaderStepImage.alt = `Schritt ${imageIndex}: ${renumberSequence[activeIndex]?.label || ""}`;
    loaderStepImageWrap.classList.remove("hidden");
    loaderStepImageWrap.style.marginTop = "0px";
  }
};

const startRenumberSequenceStep = async (index) => {
  if (!renumberSequence || index < 0 || index >= renumberSequence.length) return;
  const step = renumberSequence[index];
  renumberSequenceIndex = index;
  renumberJobMode = step.mode;
  renderSequenceSteps();
  renumberResultCount = 0;
  resetProgress();
  setIndeterminate(true);
  setStatus(formatSequenceStatus(getSequenceStepInfo(), "wird gestartet"));
  const runResp = await fetch(withEnv(step.endpoint), { method: "POST" });
  if (!runResp.ok) {
    const text = await runResp.text();
    throw new Error(text || `${step.label} fehlgeschlagen`);
  }
  const runData = await runResp.json();
  renumberJobId = runData.job_id || null;
  if (!renumberJobId) {
    throw new Error("Kein Job-ID erhalten.");
  }
  if (!renumberJobTimer) {
    renumberJobTimer = window.setInterval(pollRenumberJob, 700);
  }
  if (!renumberSequenceTransition) {
    await pollRenumberJob();
  }
};

const startRenumberSequenceWithSteps = async (steps, subtitle) => {
  renumberSequence = steps;
  renumberSequenceIndex = -1;
  renumberSequenceTransition = false;
  if (loaderSubtitle) {
    loaderSubtitle.textContent = subtitle || "Wagenumbau Ablauf";
  }
  renderSequenceSteps();
  if (renumberJobTimer) {
    clearInterval(renumberJobTimer);
    renumberJobTimer = null;
  }
  await startRenumberSequenceStep(0);
};

const startRenumberSequence = async () => {
  await startRenumberSequenceWithSteps(buildRenumberSequence(), "Wagenumbau Ablauf");
};
const stopRenumberSequence = () => {
  renumberSequence = null;
  renumberSequenceIndex = -1;
  renumberSequenceTransition = false;
  renderSequenceSteps();
};

const fetchJSON = async (url, options) => {
  const resp = await fetch(url, options);
  if (!resp.ok) {
    throw new Error(await parseErrorResponse(resp));
  }
  return resp.json();
};

const sleep = (ms) => new Promise((resolve) => window.setTimeout(resolve, ms));

const fetchRenumberPending = async (mode) => {
  const data = await fetchJSON(withEnv(`/api/renumber/pending?mode=${encodeURIComponent(mode)}`));
  return Number(data.pending || 0);
};

const waitForRenumberJobIdle = async () => {
  while (renumberJobId) {
    await sleep(200);
  }
  return { status: lastRenumberJobStatus, error: lastRenumberJobError };
};

const ensureSparePartUser = () => {
  if (sparePartUser) return sparePartUser;
  const input = window.prompt("Bitte Benutzerkennung für Teiletausch eingeben:", "");
  sparePartUser = (input || "UNBEKANNT").trim() || "UNBEKANNT";
  window.localStorage.setItem("sparepart.user", sparePartUser);
  return sparePartUser;
};

const showWagonsView = () => {
  if (wagonsView) wagonsView.classList.remove("hidden");
  if (objstrkView) objstrkView.classList.add("hidden");
  if (swapView) swapView.classList.add("hidden");
  currentView = "wagons";
};

const showObjStrkView = () => {
  if (wagonsView) wagonsView.classList.add("hidden");
  if (objstrkView) objstrkView.classList.remove("hidden");
  if (swapView) swapView.classList.add("hidden");
  currentView = "obj";
};

const showSwapDbView = () => {
  if (wagonsView) wagonsView.classList.add("hidden");
  if (objstrkView) objstrkView.classList.add("hidden");
  if (swapView) swapView.classList.remove("hidden");
  currentView = "swap";
};

const applyModuleVisibility = () => {
  const showSparepartModule =
    currentModule === "sparepart" || currentModule === "wagenumbau" || currentModule === "teilenummer";
  if (sparepartModule) {
    sparepartModule.classList.toggle("hidden", !showSparepartModule);
  }
  if (rsrd2Module) {
    rsrd2Module.classList.toggle("hidden", currentModule !== "rsrd2");
  }
};

const resolveModule = (nextModule) => {
  if (nextModule === "rsrd2") return "rsrd2";
  if (nextModule === "wagenumbau") return "wagenumbau";
  if (nextModule === "teilenummer") return "teilenummer";
  return "sparepart";
};

const reloadWagonTable = async (config) => {
  const table = config.table || WAGON_MODULES.sparepart.table;
  const resp = await fetch(withEnv(withTable(config.reloadUrl, table)), { method: "POST" });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || "Reload fehlgeschlagen");
  }
  return resp;
};

const startModuleWorkflow = async ({ skipReload = false } = {}) => {
  if (currentModule === "rsrd2") {
    loadRsrd2Wagons();
    loadRsrd2Overview();
    return;
  }
  if (currentModule === "teilenummer") {
    resetTeilenummerGo();
  }
  const config = getWagonModuleConfig(currentModule);
  let reloadError = null;
  try {
    await ensureEnvMeta();
    refreshCacheStatusInfo();
    showOverlay({ showRandomImage: true });
    if (config.reloadOnStart && !skipReload) {
      setOverlayContext({
        source: envMeta?.urls?.compass,
        target: buildSqliteUrl(getWagonTableMeta(currentModule)),
      });
      setStatus("Datenbank wird neu geladen ...");
      setIndeterminate(true);
      if (!hasCompassSource(envMeta)) {
        reloadError = new Error(
          "Infor-Compass-Verbindung fehlt. Versuche vorhandenen SQLite-Cache zu laden.",
        );
      } else {
        try {
          await reloadWagonTable(config);
        } catch (error) {
          reloadError =
            error instanceof Error ? error : new Error(String(error || "Reload fehlgeschlagen"));
          setStatus("Reload fehlgeschlagen. Versuche vorhandenen SQLite-Cache ...");
        }
      }
    }
    setOverlayContext({
      source: buildSqliteUrl(getWagonTableMeta(currentModule)),
      target: window.location.origin,
    });
    await loadWagons(config.table);
    if (reloadError && totalRows > 0) {
      setStatus("Cache geladen (ohne frischen Datalake-Reload).");
    }
    showTable();
  } catch (error) {
    console.error(error);
    const primaryMessage = error?.message || "Unbekannter Fehler";
    if (reloadError) {
      showError(`${primaryMessage} (Reload-Hinweis: ${reloadError.message})`);
      return;
    }
    showError(primaryMessage);
  }
};

const setModule = (nextModule, force = false) => {
  const resolved = resolveModule(nextModule);
  if (!force && resolved === currentModule) return;
  currentModule = resolved;
  const meta = MODULE_META[resolved] || MODULE_META.sparepart;
  if (moduleTitle) {
    moduleTitle.textContent = meta.title;
  }
  if (moduleSubtitle) {
    moduleSubtitle.textContent = meta.subtitle;
  }
  if (loaderSubtitle && meta.loaderSubtitle) {
    loaderSubtitle.textContent = meta.loaderSubtitle;
  }
  if (headerSearch) {
    headerSearch.classList.toggle("hidden", !meta.showSearch);
  }
  applyModuleVisibility();
  if (resolved === "sparepart" || resolved === "wagenumbau" || resolved === "teilenummer") {
    showWagonsView();
  }
  if (openSwapFromWagonsBtn) {
    openSwapFromWagonsBtn.classList.toggle("hidden", resolved === "wagenumbau");
  }
  if (openSwapFromObjBtn) {
    openSwapFromObjBtn.classList.toggle("hidden", resolved === "wagenumbau");
  }
  if (collapseAllBtn) {
    collapseAllBtn.classList.toggle("hidden", resolved === "wagenumbau");
  }
  if (expandAllBtn) {
    expandAllBtn.classList.toggle("hidden", resolved === "wagenumbau");
  }
  if (renumberFields) {
    renumberFields.classList.toggle("hidden", resolved !== "wagenumbau");
  }
  if (resolved !== "wagenumbau") {
    resetRenumberSernStatus();
  }
  startModuleWorkflow();
  menuItems.forEach((btn) => {
    btn.classList.toggle("menu-item--active", btn.dataset.module === resolved);
  });
};

const formatEnvBadge = (envValue) => {
  const normalized = resolveEnvValue(envValue);
  const label = normalized === "TEST" ? "TST" : "PRD";
  const cls = normalized === "TEST" ? "rsrd2-env-chip--tst" : "rsrd2-env-chip--prd";
  return `<span class="rsrd2-env-chip ${cls}">${label}</span>`;
};

const updateRsrd2OverviewTitle = () => {
  const title = document.getElementById("rsrd2OverviewTitle");
  if (!title) return;
  const erpBadge = formatEnvBadge(currentEnv);
  const rsrdBadge = formatEnvBadge(currentRsrdEnv);
  title.innerHTML = `Wagenübersicht - ERP - ${erpBadge} / RSRD - ${rsrdBadge}`;
};

const updateEnvSwitch = () => {
  if (envSwitch) {
    envSwitch.dataset.env = getErpEnvParam();
  }
  if (rsrdEnvSwitch) {
    rsrdEnvSwitch.dataset.env = getRsrdEnvParam();
  }
  updateRsrd2OverviewTitle();
};

const setEnvironment = (envValue) => {
  const normalized = resolveEnvValue(envValue);
  if (normalized === currentEnv) return;
  currentEnv = normalized;
  persistEnvPreferences();
  envMeta = null;
  rsrd2Meta = { erp: null, rsrd: null, erpEnv: null, rsrdEnv: null, promise: null };
  swapSelected.clear();
  swapRows = [];
  swapPage = 1;
  currentPage = 1;
  updateEnvSwitch();
  startModuleWorkflow();
};

const setRsrdEnvironment = (envValue) => {
  const normalized = resolveEnvValue(envValue);
  if (normalized === currentRsrdEnv) return;
  currentRsrdEnv = normalized;
  persistEnvPreferences();
  rsrd2Meta = { erp: null, rsrd: null, erpEnv: null, rsrdEnv: null, promise: null };
  updateEnvSwitch();
  rsrd2Loaded = false;
  rsrd2OverviewLoaded = false;
  rsrd2OverviewPage = 1;
  loadRsrd2Overview(true);
  loadRsrd2Wagons(true);
};

const buildObjStrkHierarchy = (rows) => {
  const map = new Map();
  const nodes = rows.map((row, index) => {
    const id = (row.__id || row.SER2 || "").trim() || `node-${index}`;
    const parentId = (row.__parentId || "").trim() || null;
    const node = { id, parentId, data: row, children: [] };
    map.set(id, node);
    return node;
  });
  const roots = [];
  nodes.forEach((node) => {
    if (node.parentId && map.has(node.parentId)) {
      map.get(node.parentId).children.push(node);
    } else {
      roots.push(node);
    }
  });
  const sortNodes = (list) => {
    list.sort((a, b) => (a.data.MFGL || "").localeCompare(b.data.MFGL || ""));
    list.forEach((node) => sortNodes(node.children));
  };
  sortNodes(roots);
  return { roots, map };
};

const matchesObjStrkSearch = (node) => {
  if (!objStrkSearchText) return true;
  return OBJSTRK_SEARCH_COLUMNS.some((column) =>
    (node.data[column] || "").toLowerCase().includes(objStrkSearchText)
  );
};

const flattenExpandedObjNodes = (nodes, level = 0, acc = []) => {
  nodes.forEach((node) => {
    acc.push({ node, level });
    if (objStrkExpanded.has(node.id) && node.children.length) {
      flattenExpandedObjNodes(node.children, level + 1, acc);
    }
  });
  return acc;
};

const collectSearchRows = (node, level) => {
  let rows = [];
  node.children.forEach((child) => {
    rows = rows.concat(collectSearchRows(child, level + 1));
  });
  const matches = matchesObjStrkSearch(node);
  if (matches || rows.length) {
    return [{ node, level }, ...rows];
  }
  return [];
};

const getObjStrkDisplayRows = () => {
  if (!objStrkRoots.length) return [];
  if (!objStrkSearchText) {
    return flattenExpandedObjNodes(objStrkRoots);
  }
  let rows = [];
  objStrkRoots.forEach((root) => {
    rows = rows.concat(collectSearchRows(root, 0));
  });
  return rows;
};

const expandAllObjNodes = () => {
  objStrkNodesById.forEach((node) => {
    if (node.children.length) {
      objStrkExpanded.add(node.id);
    }
  });
};

const collapseAllObjNodes = () => {
  objStrkExpanded.clear();
};

const updateObjStrkSearch = () => {
  if (!fulltextInput) return;
  objStrkSearchText = fulltextInput.value.trim().toLowerCase();
  if (objStrkSearchText) {
    expandAllObjNodes();
  }
  if (objStrkRoots.length) {
    renderObjStrkTable();
  }
};

const resetRenumberSernStatus = (message = "", isError = false) => {
  if (renumberSernHint) {
    renumberSernHint.textContent = message;
    renumberSernHint.classList.toggle("objstrk-field-hint--error", isError);
    renumberSernHint.classList.toggle("objstrk-field-hint--ok", !isError && Boolean(message));
  }
  if (renumberSernInput) {
    renumberSernInput.classList.toggle("objstrk-input--error", isError);
  }
};

const checkRenumberSerial = async () => {
  if (currentModule !== "wagenumbau" || !renumberSernInput) return;
  const value = renumberSernInput.value.trim();
  if (!value) {
    resetRenumberSernStatus();
    return;
  }
  try {
    const table = getWagonModuleConfig("wagenumbau").table;
    const url = withEnv(withTable(`/api/wagons/exists?sern=${encodeURIComponent(value)}`, table));
    const data = await fetchJSON(url);
    if (data.exists) {
      resetRenumberSernStatus("Seriennummer vergeben.", true);
    } else {
      resetRenumberSernStatus("Seriennummer frei.", false);
    }
  } catch (error) {
    console.error(error);
    resetRenumberSernStatus("Prüfung fehlgeschlagen.", true);
  }
};

const getTodayDateInputValue = () => {
  const now = new Date();
  const offsetMs = now.getTimezoneOffset() * 60000;
  return new Date(now.getTime() - offsetMs).toISOString().slice(0, 10);
};

const updateRenumberItnoOptions = () => {
  if (currentModule !== "wagenumbau" || !renumberItnoOptions) return;
  const values = Array.from(
    new Set(
      allWagons
        .map((row) => (row["BAUREIHE"] || "").trim())
        .filter((value) => value),
    ),
  ).sort((a, b) => a.localeCompare(b));
  renumberItnoOptions.innerHTML = values.map((value) => `<option value="${value}"></option>`).join("");
};

const getRenumberPayload = () => ({
  new_baureihe: renumberItnoInput ? renumberItnoInput.value.trim() : "",
  new_sern: renumberSernInput ? renumberSernInput.value.trim() : "",
  umbau_datum: renumberDateInput ? renumberDateInput.value : "",
  umbau_art: renumberTypeSelect ? renumberTypeSelect.value : "",
});

const isStatusSuccess = (value) => {
  const normalized = String(value || "").toLowerCase();
  return normalized.includes("ok") || normalized.includes("success");
};

const isStatusError = (value) => {
  const normalized = String(value || "").toLowerCase();
  return normalized.includes("err") || normalized.includes("fail");
};

const applyRenumberResults = (results) => {
  if (!Array.isArray(results) || !results.length) return;
  const buckets = new Map();
  const ser2Buckets = new Map();
  const itnoBuckets = new Map();
  const used = new Set();
  const normalizeKeyValue = (value) =>
    value === null || value === undefined ? "" : String(value).trim();
  const makeKey = (cfgl, itno, ser2) =>
    `${normalizeKeyValue(cfgl)}||${normalizeKeyValue(itno)}||${normalizeKeyValue(ser2)}`;
  const makeCfgrSer2Key = (cfgr, ser2) =>
    `${normalizeKeyValue(cfgr)}||${normalizeKeyValue(ser2)}`;
  const makeCfgrItnoKey = (cfgr, itno) =>
    `${normalizeKeyValue(cfgr)}||${normalizeKeyValue(itno)}`;
  const pushBucket = (map, key, row) => {
    const list = map.get(key) || [];
    list.push(row);
    map.set(key, list);
  };
  const takeRow = (map, key) => {
    const list = map.get(key);
    if (!list || !list.length) return null;
    while (list.length) {
      const candidate = list.shift();
      if (!used.has(candidate)) {
        used.add(candidate);
        return candidate;
      }
    }
    return null;
  };
  objStrkRows.forEach((row) => {
    const cfgl = normalizeKeyValue(row.MFGL);
    const itno = normalizeKeyValue(row.ITNO);
    const ser2 = normalizeKeyValue(row.SER2);
    pushBucket(buckets, makeKey(cfgl, itno, ser2), row);
    if (ser2) {
      pushBucket(ser2Buckets, makeCfgrSer2Key(cfgl, ser2), row);
    }
    if (itno) {
      pushBucket(itnoBuckets, makeCfgrItnoKey(cfgl, itno), row);
    }
  });
  results.forEach((entry) => {
    const cfgr = normalizeKeyValue(entry.cfgr);
    const itno = normalizeKeyValue(entry.itno);
    const ser2 = normalizeKeyValue(entry.ser2);
    let row = takeRow(buckets, makeKey(cfgr, itno, ser2));
    if (!row && ser2) {
      row = takeRow(ser2Buckets, makeCfgrSer2Key(cfgr, ser2));
    }
    if (!row && itno) {
      row = takeRow(itnoBuckets, makeCfgrItnoKey(cfgr, itno));
    }
    if (!row) return;
    if ("out" in entry) {
      row.OUT = entry.out || "";
    }
    if ("in" in entry) {
      row.IN = entry.in || "";
    }
    if ("rollback" in entry) {
      row.ROLLBACK = entry.rollback || "";
    }
  });
};

const pollRenumberJob = async () => {
  if (!renumberJobId) return;
  if (renumberSequenceTransition) return;
  try {
    const job = await fetchJSON(rsrd2Api(`/api/rsrd2/jobs/${renumberJobId}`));
    const isSingleStepLoop = singleStepLoopActive && !renumberSequence;
    const total = Number(job.total || 0);
    const processed = Number(job.processed || 0);
    const jobType = job.type || renumberJobMode || "renumber_run";
    const isMos170Job = jobType === "mos170_addprop" || renumberJobMode === "mos170";
    const isCms100Job = jobType === "mos170_plpn" || renumberJobMode === "mos170_plpn";
    const isMos100Job = jobType === "ips_mos100_chgsern" || renumberJobMode === "mos100";
    const isWagonRenumberJob = jobType === "wagon_renumber" || renumberJobMode === "wagon_renumber";
    const isMos180Job = jobType === "mos180_approve" || renumberJobMode === "mos180";
    const isMos050Job = jobType === "mos050_montage" || renumberJobMode === "mos050";
    const isCrs335Job = jobType === "crs335_updctrlobj" || renumberJobMode === "crs335";
    const isSts046DelJob = jobType === "sts046_delgenitem" || renumberJobMode === "sts046";
    const isSts046AddJob = jobType === "sts046_addgenitem" || renumberJobMode === "sts046_add";
    const isMms240Job = jobType === "mms240_upd" || renumberJobMode === "mms240";
    const isCusextJob = jobType === "cusext_addfieldvalue" || renumberJobMode === "cusext";
    const isTeilenummerJob = jobType === "teilenummer_run" || renumberJobMode === "teilenummer";
    const isRollbackJob = jobType === "renumber_rollback" || renumberJobMode === "rollback";
    const actionLabel =
      isTeilenummerJob
        ? "Teilenummer"
        :
      isWagonRenumberJob
        ? "Wagen umnummerieren"
        : isMos170Job
          ? "MOS170"
          : isCms100Job
            ? "MOS170 PLPN"
            : isMos100Job
              ? "MOS100"
              : isMos180Job
                ? "MOS180"
                : isMos050Job
                  ? "MOS050"
                  : isCrs335Job
                    ? "CRS335"
                    : isSts046DelJob || isSts046AddJob
                      ? "STS046"
                      : isMms240Job
                        ? "MMS240"
                        : isCusextJob
                          ? "CUSEXT"
                        : jobType === "renumber_install" || renumberJobMode === "in"
                          ? "Einbau"
                          : isRollbackJob
                            ? "Roll-Back"
                            : "Ausbau";
    const logs = Array.isArray(job.logs) ? job.logs : [];
    const lastLog = logs.length ? logs[logs.length - 1] : "";
    if (isTeilenummerJob && logs.length > teilenummerLogCount) {
      const newLogs = logs.slice(teilenummerLogCount);
      newLogs.forEach((entry) => {
        if (typeof entry === "string" && entry.includes("abgeschlossen")) {
          setTeilenummerRandomImage();
        }
      });
      teilenummerLogCount = logs.length;
    }
    const stepInfo = getSequenceStepInfo();
    const statusMessage = renumberSequence
      ? formatSequenceStatusLine(stepInfo, processed, total)
      : lastLog
        ? formatSequenceStatus(stepInfo, lastLog)
        : total > 0
          ? formatSequenceStatus(stepInfo, `${actionLabel} ${processed} / ${total}`)
          : formatSequenceStatus(stepInfo, `${actionLabel} läuft ...`);
    if (statusMessage) {
      setStatus(statusMessage);
    }
    if (total > 0) {
      updateProgress(processed, total);
    }
    const results = Array.isArray(job.results) ? job.results : [];
    if (results.length > renumberResultCount) {
      const newResults = results.slice(renumberResultCount);
      renumberResultCount = results.length;
      applyRenumberResults(newResults);
      renderObjStrkTable();
    }
    if (job.status === "success") {
      lastRenumberJobStatus = "success";
      lastRenumberJobError = null;
      if (isSingleStepLoop) {
        clearInterval(renumberJobTimer);
        renumberJobTimer = null;
        renumberJobId = null;
        return;
      }
      if (renumberSequence && renumberSequenceIndex < renumberSequence.length - 1) {
        renumberSequenceTransition = true;
        try {
          await startRenumberSequenceStep(renumberSequenceIndex + 1);
        } catch (error) {
          console.error(error);
          stopRenumberSequence();
          clearInterval(renumberJobTimer);
          renumberJobTimer = null;
          renumberJobId = null;
          renumberJobMode = null;
          hideOverlay(true); // Job finished with error during sequence transition
          enableAllRenumberButtons();
        } finally {
          renumberSequenceTransition = false;
        }
        return;
      }
      stopRenumberSequence();
      clearInterval(renumberJobTimer);
      renumberJobTimer = null;
      renumberJobId = null;
      renumberJobMode = null;
      hideOverlay(true); // Job finished successfully
      enableAllRenumberButtons();
      setTeilenummerControlsDisabled(false);
      if (isTeilenummerJob) {
        resetTeilenummerGo();
        try {
          await loadWagons(WAGON_MODULES.teilenummer.table);
          applyFilters();
        } catch (error) {
          console.error(error);
        }
      }
    } else if (job.status === "error") {
      lastRenumberJobStatus = "error";
      lastRenumberJobError = job.error || "Unbekannter Fehler";
      if (isSingleStepLoop) {
        clearInterval(renumberJobTimer);
        renumberJobTimer = null;
        renumberJobId = null;
        return;
      }
      stopRenumberSequence();
      clearInterval(renumberJobTimer);
      renumberJobTimer = null;
      renumberJobId = null;
      renumberJobMode = null;
      hideOverlay(true); // Job finished with error
      enableAllRenumberButtons();
      setTeilenummerControlsDisabled(false);
    }
  } catch (error) {
    console.error(error);
    const isSingleStepLoop = singleStepLoopActive && !renumberSequence;
    lastRenumberJobStatus = "error";
    lastRenumberJobError = error.message || "Statusabfrage fehlgeschlagen.";
    if (isSingleStepLoop) {
      clearInterval(renumberJobTimer);
      renumberJobTimer = null;
      renumberJobId = null;
      return;
    }
    stopRenumberSequence();
    clearInterval(renumberJobTimer);
    renumberJobTimer = null;
    renumberJobId = null;
    renumberJobMode = null;
    hideOverlay(true); // Polling failed due to error
    enableAllRenumberButtons();
  }
};


const loadWagons = async (tableName) => {
  const table = tableName || WAGON_MODULES.sparepart.table;
  setStatus("VERBINDE ...");
  setIndeterminate(true);
  const countPayload = await fetchJSON(withEnv(withTable("/api/wagons/count", table)));
  totalRows = countPayload.total;
  updateProgress(0, totalRows);

  setStatus("LADE DATEN VON M3");
  wagons = [];
  let fetched = 0;

  while (fetched < totalRows) {
    const url = withEnv(
      withTable(`/api/wagons/chunk?offset=${fetched}&limit=${CHUNK_SIZE}`, table),
    );
    const chunk = await fetchJSON(url);
    wagons = wagons.concat(chunk.rows);
    fetched += chunk.returned;
    updateProgress(fetched, totalRows);
  }

  allWagons = wagons.slice();
  populateDatalists();

  if (totalRows === 0) {
    setStatus("Keine Datensätze gefunden.");
  } else {
    setStatus("Ladevorgang abgeschlossen.");
  }
};

const populateDatalists = () => {
  getFilterFields().forEach(({ key, list }) => {
    if (!list) return;
    const values = Array.from(
      new Set(
        allWagons
          .map((row) => (row[key] || "").trim())
          .filter((value) => value.length > 0)
      )
    ).sort((a, b) => a.localeCompare(b));
    list.innerHTML = values.map((value) => `<option value="${value}"></option>`).join("");
  });
};

const normalizeObjStrkRows = (payload) => {
  const records =
    payload?.response?.MIRecord ??
    payload?.response?.MIRecords ??
    payload?.response?.MIResponse ??
    [];
  const recordArray = Array.isArray(records)
    ? records
    : records
      ? [records]
      : [];
  return recordArray.map((record) => {
    const row = {};
    const nameValues = record?.NameValue || record?.nameValue || [];
    const pairArray = Array.isArray(nameValues) ? nameValues : [nameValues];
    pairArray.forEach((entry) => {
      if (!entry) return;
      const name = entry.Name || entry.name;
      if (!name) return;
      row[name] = entry.Value ?? entry.value ?? "";
    });
    return row;
  });
};


const renderTableHead = () => {
  tableHead.innerHTML = "";
  const headerRow = document.createElement("tr");
  const columnOrder = getColumnOrder();
  const columnLabels = getColumnLabels();
  columnOrder.forEach((key) => {
    const th = document.createElement("th");
    th.textContent = columnLabels[key] || key;
    if (key === "OBJSTRK") th.classList.add("objstrk-col");
    headerRow.appendChild(th);
  });
  if (currentModule === "teilenummer") {
    const th = document.createElement("th");
    th.classList.add("mark-col");
    const checkAllBtn = document.createElement("button");
    checkAllBtn.type = "button";
    checkAllBtn.className = "check-inline-btn";
    checkAllBtn.textContent = "Check-All";
    checkAllBtn.addEventListener("click", () => runCheckToggle(true));

    const checkNoneBtn = document.createElement("button");
    checkNoneBtn.type = "button";
    checkNoneBtn.className = "check-inline-btn";
    checkNoneBtn.textContent = "Check-None";
    checkNoneBtn.addEventListener("click", () => runCheckToggle(false));

    th.appendChild(checkAllBtn);
    th.appendChild(checkNoneBtn);
    headerRow.appendChild(th);
  }
  tableHead.appendChild(headerRow);
};

const renderObjStrkHead = () => {
  if (!objstrkHead) return;
  objstrkHead.innerHTML = "";
  const row = document.createElement("tr");
  getObjStrkColumns().forEach((column) => {
    const th = document.createElement("th");
    th.textContent = OBJSTRK_LABELS[column] || column;
     if (column === "PARTS") th.classList.add("objstrk-col-action");
    row.appendChild(th);
  });
  objstrkHead.appendChild(row);
};

const applyFilters = () => {
  const fulltext = fulltextInput?.value.trim().toLowerCase() || "";
  const filters = getFilterFields()
    .map(({ key, input }) => ({
      key,
      value: input ? input.value.trim().toLowerCase() : "",
    }))
    .filter(({ value }) => value.length > 0);
  const columnOrder = getColumnOrder();

  wagons = allWagons.filter((row) => {
    const filterMatch = filters.every(({ key, value }) =>
      (row[key] || "").toLowerCase().includes(value),
    );
    const fulltextMatch =
      !fulltext || columnOrder.some((key) => (row[key] || "").toLowerCase().includes(fulltext));
    return filterMatch && fulltextMatch;
  });

  if (currentModule === "teilenummer" && teilenummerExcelMappings.size) {
    wagons = wagons.filter((row) => teilenummerExcelMappings.has(toTeilenummerKey(row.A_ITNO, row.A_SERN)));
  }

  if (currentModule === "teilenummer") {
    wagons.sort((a, b) => {
      const left = String(a?.W_SERN || "");
      const right = String(b?.W_SERN || "");
      return left.localeCompare(right, "de-CH", { numeric: true, sensitivity: "base" });
    });
  }

  currentPage = 1;
  updateCountInfo();
  renderPage();
};

const resetTeilenummerGo = () => {
  if (!teilenummerGoBtn) return;
  teilenummerGoBtn.textContent = "GO (0)";
  teilenummerGoBtn.dataset.count = "0";
  teilenummerGoBtn.classList.remove("go-cta--active");
  teilenummerGoBtn.classList.add("go-cta--idle");
};

const showTeilenummerGo = (count) => {
  if (!teilenummerGoBtn) return;
  const safeCount = Number.isFinite(count) ? count : Number(count) || 0;
  teilenummerGoBtn.textContent = `GO (${safeCount})`;
  teilenummerGoBtn.dataset.count = String(safeCount);
  if (safeCount > 0) {
    teilenummerGoBtn.classList.remove("go-cta--idle");
    teilenummerGoBtn.classList.add("go-cta--active");
  } else {
    teilenummerGoBtn.classList.remove("go-cta--active");
    teilenummerGoBtn.classList.add("go-cta--idle");
  }
};

const formatApiLogEntry = (entry) => {
  if (!entry || typeof entry !== "object") return String(entry ?? "");
  const ts = entry.ts || "";
  const action = entry.action || "";
  const ok = entry.ok === true ? "OK" : entry.ok === false ? "NOK" : "";
  const error = entry.error || "";
  const wagon = entry.wagon || {};
  const itno = wagon.itno || "";
  const sern = wagon.sern || "";
  const reqUrl = (entry.request || {}).url || "";
  return `${ts} | ${action} | ${ok} | ${itno}/${sern}${error ? ` | ${error}` : ""}${reqUrl ? `\n${reqUrl}` : ""}`;
};

const renderApiLogContent = (entries) => {
  if (!apiLogContent) return;
  if (!Array.isArray(entries) || !entries.length) {
    apiLogContent.textContent = "API-Log ist leer.";
    return;
  }
  apiLogContent.textContent = entries.map(formatApiLogEntry).join("\n\n");
};

const loadApiLog = async () => {
  if (!apiLogContent) return;
  apiLogContent.textContent = "Lade API-Log ...";
  try {
    const data = await fetchJSON(withEnv("/api/teilenummer/api_log?limit=1500"));
    renderApiLogContent(data?.entries || []);
  } catch (error) {
    apiLogContent.textContent = error.message || "API-Log konnte nicht geladen werden.";
  }
};

const showApiLogModal = async () => {
  if (!apiLogModal) return;
  if (apiLogContent) {
    apiLogContent.textContent = "Lade API-Log ...";
  }
  apiLogModal.classList.remove("hidden");
  apiLogModal.setAttribute("aria-hidden", "false");
  await loadApiLog();
};

const hideApiLogModal = () => {
  if (!apiLogModal) return;
  apiLogModal.classList.add("hidden");
  apiLogModal.setAttribute("aria-hidden", "true");
};

window.__openApiLogModal = () => {
  void showApiLogModal();
};

const setTeilenummerControlsDisabled = (disabled) => {
  if (currentModule !== "teilenummer") return;
  if (openSwapFromWagonsBtn) openSwapFromWagonsBtn.disabled = disabled;
  if (teilenummerGoBtn) teilenummerGoBtn.disabled = disabled;
};

const runCheckToggle = async (checked) => {
  if (currentModule !== "teilenummer") return;
  if (!wagons.length) return;
  setStatus(checked ? "Markiere Datensätze ..." : "Entferne Markierungen ...");
  showOverlay({ showRandomImage: false, showProgress: false });
  setOverlayProgressVisible(false);
  try {
    const delayPayload = currentDelayPayload();
    for (const row of wagons) {
      await fetchJSON(withEnv("/api/teilenummer/check"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          A_BIRT: row.A_BIRT ?? "",
          A_ITNO: row.A_ITNO ?? "",
          A_SERN: row.A_SERN ?? "",
          checked,
          ...delayPayload,
        }),
      });
      row.CHECKED = checked ? "1" : "";
    }
    renderPage();
  } catch (error) {
    showError(error.message || "Markierung fehlgeschlagen");
  } finally {
    hideOverlay();
  }
};

const loadTeilenummerExcelMappings = async () => {
  if (!excelUploadInput || !excelUploadInput.files || !excelUploadInput.files[0]) {
    teilenummerExcelMappings = new Map();
    teilenummerExcelFilename = "";
    updateExcelUploadInfo();
    applyFilters();
    return;
  }
  const file = excelUploadInput.files[0];
  setStatus("Excel wird geprüft ...");
  showOverlay({ showRandomImage: false, showProgress: false });
  setOverlayMessage("");
  setOverlaySpinner(true);
  setOverlayProgressVisible(false);
  setOverlayActionVisible(false);
  let keepOverlayOpen = false;
  try {
    const toBase64 = (arrayBuffer) => {
      let binary = "";
      const bytes = new Uint8Array(arrayBuffer);
      const chunkSize = 0x8000;
      for (let i = 0; i < bytes.length; i += chunkSize) {
        const chunk = bytes.subarray(i, i + chunkSize);
        binary += String.fromCharCode(...chunk);
      }
      return btoa(binary);
    };
    const contentBuffer = await file.arrayBuffer();
    const response = await fetch(withEnv("/api/teilenummer/excel/validate"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        filename: file.name || "",
        content_b64: toBase64(contentBuffer),
        ...currentDelayPayload(),
      }),
    });
    const responseText = await response.text();
    let responsePayload = null;
    if (responseText) {
      try {
        responsePayload = JSON.parse(responseText);
      } catch {
        responsePayload = null;
      }
    }
    if (!response.ok) {
      let errorMessage = "Excel-Validierung fehlgeschlagen.";
      const detail = responsePayload?.detail;
      if (typeof detail === "string" && detail.trim()) {
        errorMessage = detail;
      } else if (detail && typeof detail === "object") {
        if (detail.message) errorMessage = String(detail.message);
        showTeilenummerExcelValidationPopup(detail, true);
        keepOverlayOpen = true;
      } else if (responseText) {
        errorMessage = responseText;
      }
      throw new Error(errorMessage);
    }
    const payload = responsePayload || {};
    showTeilenummerExcelValidationPopup(payload, false);
    keepOverlayOpen = true;
    const mappings = Array.isArray(payload.mappings) ? payload.mappings : [];
    const mappingMap = new Map();
    mappings.forEach((entry) => {
      const key = toTeilenummerKey(entry.old_itno, entry.old_sern);
      mappingMap.set(
        key,
        {
          old_itno: String(entry.old_itno || "").trim(),
          old_sern: String(entry.old_sern || "").trim(),
          new_itno: String(entry.new_itno || "").trim(),
          new_sern: String(entry.new_sern || "").trim(),
        },
      );
      const row = allWagons.find((item) => toTeilenummerKey(item.A_ITNO, item.A_SERN) === key);
      if (row) {
        row.PLAN_OUT_DATE = String(entry.PLAN_OUT_DATE || "");
        row.PLAN_OUT_TIME = String(entry.PLAN_OUT_TIME || "");
        row.PLAN_IN_DATE = String(entry.PLAN_IN_DATE || "");
        row.PLAN_IN_TIME = String(entry.PLAN_IN_TIME || "");
        row.OUT_DELAY_MIN = String(entry.out_delay_min || "");
        row.IN_DELAY_MIN = String(entry.in_delay_min || "");
      }
    });
    teilenummerExcelMappings = mappingMap;
    teilenummerExcelFilename = payload.filename || file.name || "";
    updateExcelUploadInfo();
    applyFilters();
  } catch (error) {
    teilenummerExcelMappings = new Map();
    teilenummerExcelFilename = "";
    updateExcelUploadInfo();
    applyFilters();
    if (!keepOverlayOpen) {
      setStatus(error.message || "Excel-Validierung fehlgeschlagen.");
      setOverlayPinned(true);
      setOverlaySpinner(false);
      setOverlayProgressVisible(false);
      setOverlayActionVisible(false);
      setOverlayMessage(error.message || "Excel-Validierung fehlgeschlagen.");
      keepOverlayOpen = true;
    }
  } finally {
    if (!keepOverlayOpen) {
      hideOverlay();
    }
  }
};

const prepareTeilenummerTausch = async () => {
  if (currentModule !== "teilenummer") return;
  const newItno = newItnoInput?.value.trim() || "";
  const newSern = newSernInput?.value.trim() || "";
  const mappings = Array.from(teilenummerExcelMappings.values());
  const hasCheckedRows = allWagons.some((row) => String(row.CHECKED || "").trim() === "1");
  if (!hasCheckedRows) {
    setStatus("Keine Position markiert. Leere Tausch-DB ...");
    showOverlay({ showRandomImage: false, showProgress: false });
    setOverlaySpinner(true);
    setOverlayProgressVisible(false);
    setOverlayActionVisible(false);
    try {
      const data = await fetchJSON(withEnv("/api/teilenummer/clear_tausch"), { method: "POST" });
      resetTeilenummerGo();
      setOverlaySpinner(false);
      setOverlayPinned(true);
      setOverlayMessage(
        `Keine Position markiert.\nTausch-DB wurde geleert (${Number(data?.cleared || 0)} Einträge).`,
      );
    } catch (error) {
      setOverlaySpinner(false);
      setOverlayPinned(true);
      setOverlayMessage(error.message || "Tausch-DB konnte nicht geleert werden.");
    }
    return;
  }
  const checkedRows = allWagons.filter((row) => String(row.CHECKED || "").trim() === "1");
  if (newSern && checkedRows.length > 1 && !mappings.length) {
    window.alert("Mit Neue SERN ist nur eine markierte Position erlaubt.");
    return;
  }
  if (!newItno && !mappings.length) {
    window.alert("Bitte eine neue ITNO eingeben.");
    return;
  }
  setStatus("Erstelle Tauschdaten ...");
  showOverlay({ showRandomImage: false });
  setIndeterminate(true);
  try {
    const data = await fetchJSON(withEnv("/api/teilenummer/prepare"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        new_itno: newItno,
        new_sern: newSern,
        mappings,
        ...currentDelayPayload(),
      }),
    });
    showTeilenummerGo(Number(data.count || 0));
  } catch (error) {
    showError(error.message || "Tauschdaten konnten nicht erstellt werden.");
  } finally {
    hideOverlay();
  }
};

const runTeilenummerGo = async () => {
  if (currentModule !== "teilenummer") return;
  const goCount = Number(teilenummerGoBtn?.dataset?.count || "0");
  if (!Number.isFinite(goCount) || goCount <= 0) {
    window.alert("Bitte zuerst 'Jetzt tauschen' ausführen.");
    return;
  }
  let modeInfo = { env: getErpEnvParam(), dry_run: false, mode: "LIVE" };
  try {
    modeInfo = await fetchJSON(withEnv("/api/teilenummer/mode"));
  } catch (error) {
    console.warn("Konnte Betriebsmodus nicht vorab laden", error);
  }
  const modeEnv = String(modeInfo?.env || getErpEnvParam()).toUpperCase();
  const modeLabel = modeInfo?.dry_run ? "DRYRUN" : "LIVE";
  if (modeInfo?.dry_run) {
    const proceed = window.confirm(
      `Achtung: ${modeEnv} läuft aktuell im DRYRUN.\n\nTausch trotzdem starten?`,
    );
    if (!proceed) return;
  }
  setTeilenummerControlsDisabled(true);
  setStatus(`Teilenummer-Ablauf startet (${modeEnv} · ${modeLabel}) ...`);
  showOverlay({ showRandomImage: false });
  setIndeterminate(true);
  teilenummerLogCount = 0;
  setTeilenummerRandomImage();
  try {
    const runResp = await fetch(withEnv("/api/teilenummer/run"), { method: "POST" });
    if (!runResp.ok) {
      const text = await runResp.text();
      throw new Error(text || "Teilenummer-Ablauf fehlgeschlagen");
    }
    const runData = await runResp.json();
    const runEnv = String(runData?.env || modeEnv).toUpperCase();
    const runModeLabel = runData?.dry_run ? "DRYRUN" : "LIVE";
    setStatus(`Teilenummer-Ablauf läuft (${runEnv} · ${runModeLabel}) ...`);
    renumberJobId = runData.job_id || null;
    renumberJobMode = "teilenummer";
    renumberResultCount = 0;
    if (!renumberJobId) {
      throw new Error("Kein Job-ID erhalten.");
    }
    if (renumberJobTimer) {
      clearInterval(renumberJobTimer);
    }
    renumberJobTimer = window.setInterval(pollRenumberJob, 700);
    await pollRenumberJob();
  } catch (error) {
    console.error(error);
    showError(error.message || "Teilenummer-Ablauf fehlgeschlagen.");
    setTeilenummerControlsDisabled(false);
    hideOverlay(true);
  }
};


const renderPage = () => {
  tableBody.innerHTML = "";
  const columnOrder = getColumnOrder();
  const extraColumns = currentModule === "teilenummer" ? 1 : 0;
  if (!wagons.length) {
    const message = allWagons.length
      ? "Keine Treffer für die aktuellen Filter."
      : 'Keine Datenvorhanden. Bitte drücken Sie "Daten aus PRD / TST aktualisieren".';
    tableBody.innerHTML = `<tr><td colspan="${columnOrder.length + extraColumns}">${message}</td></tr>`;
    return;
  }

  const maxPage = Math.max(1, Math.ceil(wagons.length / pageSize));
  currentPage = Math.min(currentPage, maxPage);

  const start = (currentPage - 1) * pageSize;
  const rows = wagons.slice(start, start + pageSize);

  rows.forEach((wagon) => {
    const tr = document.createElement("tr");
    const teilenummerStatus = String(wagon.A_STAT || "").trim();
    const wagonStatus = String(wagon.W_STAT || "").trim();
    const hasTeilenummerWarningStatus =
      currentModule === "teilenummer" && Boolean(teilenummerStatus) && teilenummerStatus !== "20";
    const hasWagonWarningStatus =
      currentModule === "teilenummer" && Boolean(wagonStatus) && wagonStatus !== "20";
    if (hasTeilenummerWarningStatus) {
      tr.classList.add("teilenummer-row-status-warning");
    }
    columnOrder.forEach((key) => {
      const td = document.createElement("td");
      if (key === "OBJSTRK") {
        td.classList.add("objstrk-col");
        const button = document.createElement("button");
        button.type = "button";
        button.className = "objstrk-btn";
        button.title = "Objektstruktur";
        button.setAttribute("aria-label", "Objektstruktur");
        button.dataset.mtrl = wagon["BAUREIHE"] ?? "";
        button.dataset.sern = wagon["SERIENNUMMER"] ?? "";
        button.innerHTML = `
          <svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true" focusable="false">
            <path d="M22.7 19.3l-6.1-6.1c.9-2.3.4-5-1.5-6.9-1.9-1.9-4.6-2.4-6.9-1.5l3 3-2.1 2.1-3-3c-.9 2.3-.4 5 1.5 6.9 1.9 1.9 4.6 2.4 6.9 1.5l6.1 6.1c.4.4 1 .4 1.4 0l.7-.7c.4-.4.4-1 0-1.4z"></path>
          </svg>
        `;
        td.appendChild(button);
      } else {
        let cellValue = wagon[key] ?? "";
        const isTeilenummerDateColumn =
          currentModule === "teilenummer" &&
          (key === "X_EINBAU" || key === "X_AUSBAU_NEU" || key === "X_EINBAU_NEU");
        if (isTeilenummerDateColumn) {
          const dt = getTeilenummerDateTimeValue(wagon, key);
          cellValue = dt.date || "-";
          td.textContent = cellValue;
          if (dt.time) {
            const timeLine = document.createElement("small");
            timeLine.className = "datetime-subline";
            timeLine.textContent = dt.time;
            td.appendChild(document.createElement("br"));
            td.appendChild(timeLine);
          }
          tr.appendChild(td);
          return;
        }
        if (currentModule === "teilenummer") {
          if (key === "A_ALII" && cellValue) {
            cellValue = String(cellValue).slice(0, 15);
          }
          if (key === "B_WHSL" && cellValue) {
            cellValue = String(cellValue);
          }
        }
        td.textContent = cellValue;
        if (hasWagonWarningStatus && (key === "W_ITNO" || key === "W_SERN")) {
          td.classList.add("wagon-status-warning-cell");
        }
        if (hasWagonWarningStatus && key === "W_ITNO") {
          const wagonStatusLine = document.createElement("small");
          wagonStatusLine.className = "status-warning-line";
          wagonStatusLine.textContent = `Status: ${wagonStatus}`;
          td.appendChild(document.createElement("br"));
          td.appendChild(wagonStatusLine);
        }
        if (hasTeilenummerWarningStatus && key === "A_ITNO") {
          const statusLine = document.createElement("small");
          statusLine.className = "status-warning-line";
          statusLine.textContent = `Status: ${teilenummerStatus}`;
          td.appendChild(document.createElement("br"));
          td.appendChild(statusLine);
        }
        if (currentModule === "teilenummer" && (key === "A_ITNO" || key === "A_SERN")) {
          const mapping = teilenummerExcelMappings.get(toTeilenummerKey(wagon.A_ITNO, wagon.A_SERN));
          if (mapping) {
            const mappedValue = key === "A_ITNO" ? mapping.new_itno : mapping.new_sern;
            if (mappedValue && String(mappedValue) !== String(cellValue || "")) {
              const mappedLine = document.createElement("small");
              mappedLine.className = "mapping-line";
              mappedLine.textContent = mappedValue;
              td.appendChild(mappedLine);
            }
          }
        }
      }
      tr.appendChild(td);
    });
    if (currentModule === "teilenummer") {
      const td = document.createElement("td");
      td.classList.add("mark-col");
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.className = "mark-checkbox";
      checkbox.checked = String(wagon.CHECKED || "").trim() === "1";
      checkbox.addEventListener("change", async () => {
        try {
          const delayPayload = currentDelayPayload();
          await fetchJSON(withEnv("/api/teilenummer/check"), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              A_BIRT: wagon.A_BIRT ?? "",
              A_ITNO: wagon.A_ITNO ?? "",
              A_SERN: wagon.A_SERN ?? "",
              checked: checkbox.checked,
              ...delayPayload,
            }),
          });
          wagon.CHECKED = checkbox.checked ? "1" : "";
        } catch (error) {
          checkbox.checked = !checkbox.checked;
          showError(error.message || "Markierung fehlgeschlagen");
        }
      });
      td.appendChild(checkbox);
      tr.appendChild(td);
    }
    tableBody.appendChild(tr);
  });

  paginationInfo.textContent = `Seite ${currentPage} / ${maxPage}`;
  prevBtn.disabled = currentPage <= 1;
  nextBtn.disabled = currentPage >= maxPage;
};

const renderObjStrkTable = () => {
  if (!objstrkBody) return;
  objstrkBody.innerHTML = "";
  const columns = getObjStrkColumns();
  const rows = getObjStrkDisplayRows();
  if (!rows.length) {
    objstrkBody.innerHTML = `<tr><td colspan="${columns.length}">Keine Objektstruktur gefunden.</td></tr>`;
    return;
  }
  rows.forEach(({ node, level }) => {
    const tr = document.createElement("tr");
    if (currentModule === "wagenumbau") {
      const rollbackStatus = node.data.ROLLBACK || "";
      const inStatus = node.data.IN || "";
      const outStatus = node.data.OUT || "";
      if (rollbackStatus) {
        if (isStatusSuccess(rollbackStatus)) {
          tr.classList.add("objstrk-row-install-success");
        } else if (isStatusError(rollbackStatus)) {
          tr.classList.add("objstrk-row-error");
        }
      } else if (inStatus) {
        if (isStatusSuccess(inStatus)) {
          tr.classList.add("objstrk-row-install-success");
        } else if (isStatusError(inStatus)) {
          tr.classList.add("objstrk-row-error");
        }
      } else if (outStatus) {
        if (isStatusSuccess(outStatus)) {
          tr.classList.add("objstrk-row-success");
        } else if (isStatusError(outStatus)) {
          tr.classList.add("objstrk-row-error");
        }
      }
    }
    columns.forEach((column, index) => {
      const td = document.createElement("td");
      const value = node.data[column] ?? "";
      if (column === "PARTS") {
        td.classList.add("objstrk-col-action");
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "objstrk-btn";
        btn.title = "Tausch";
        btn.setAttribute("aria-label", "Tausch");
        btn.dataset.eqtp = node.data.EQTP || "";
        btn.dataset.context = `${node.data.ITDS || ""} (${node.data.ITNO || ""})`;
        btn.dataset.originalItno = node.data.ITNO || "";
        btn.dataset.originalSern = node.data.SER2 || "";
        btn.dataset.nodeId = node.id;
        const hasReplacement = Boolean(
          (node.data.ERSATZ_ITNO && node.data.ERSATZ_ITNO.trim()) ||
          (node.data.ERSATZ_SERN && node.data.ERSATZ_SERN.trim())
        );
        if (hasReplacement) {
          btn.dataset.action = "remove";
          btn.classList.add("objstrk-btn--danger");
          btn.title = "Auswahl löschen";
          btn.setAttribute("aria-label", "Auswahl löschen");
          btn.textContent = "X";
        } else {
          btn.dataset.action = "select";
          btn.innerHTML = `
            <svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true" focusable="false">
              <path d="M22.7 19.3l-6.1-6.1c.9-2.3.4-5-1.5-6.9-1.9-1.9-4.6-2.4-6.9-1.5l3 3-2.1 2.1-3-3c-.9 2.3-.4 5 1.5 6.9 1.9 1.9 4.6 2.4 6.9 1.5l6.1 6.1c.4.4 1 .4 1.4 0l.7-.7c.4-.4.4-1 0-1.4z"></path>
            </svg>
          `;
        }
        td.appendChild(btn);
      } else if (index === 0) {
        const cell = document.createElement("div");
        cell.className = "objstrk-cell";
        cell.style.setProperty("--objstrk-level", String(level));
        const showToggle = !objStrkSearchText && node.children.length;
        if (showToggle) {
          const toggle = document.createElement("button");
          toggle.type = "button";
          toggle.className = "objstrk-toggle";
          toggle.dataset.nodeId = node.id;
          toggle.setAttribute("aria-label", objStrkExpanded.has(node.id) ? "Einklappen" : "Ausklappen");
          toggle.textContent = objStrkExpanded.has(node.id) ? "−" : "+";
          cell.appendChild(toggle);
        } else {
          const spacer = document.createElement("span");
          spacer.className = "objstrk-spacer";
          cell.appendChild(spacer);
        }
        const text = document.createElement("span");
        text.textContent = value;
        cell.appendChild(text);
        td.appendChild(cell);
      } else {
        td.textContent = value;
      }
      tr.appendChild(td);
    });
    objstrkBody.appendChild(tr);
  });
};

const getSwapKey = (row) =>
  String(row.ID ?? `${row.WAGEN_ITNO || ""}::${row.WAGEN_SERN || ""}::${row.ORIGINAL_ITNO || ""}::${row.ORIGINAL_SERN || ""}`);

const findSwapRowByKey = (key) => swapRows.find((row) => getSwapKey(row) === key);

const renderSwapTable = () => {
  if (!swapTableBody) return;
  if (!swapRows.length) {
    swapTableBody.innerHTML = '<tr><td colspan="10">Keine offenen Tauschdatensätze.</td></tr>';
    if (swapPaginationInfo) swapPaginationInfo.textContent = "Seite 1 / 1";
    if (swapPrevPageBtn) swapPrevPageBtn.disabled = true;
    if (swapNextPageBtn) swapNextPageBtn.disabled = true;
  } else {
    const totalPages = Math.max(1, Math.ceil(swapRows.length / SWAP_PAGE_SIZE));
    swapPage = Math.min(Math.max(1, swapPage), totalPages);
    const start = (swapPage - 1) * SWAP_PAGE_SIZE;
    const rows = swapRows.slice(start, start + SWAP_PAGE_SIZE);
    swapTableBody.innerHTML = rows
      .map((row) => {
        const key = getSwapKey(row);
        const isSelected = swapSelected.has(key);
        return `
          <tr>
            <td>${row.WAGEN_ITNO || ""}</td>
            <td>${row.WAGEN_SERN || ""}</td>
            <td>${row.ORIGINAL_ITNO || ""}</td>
            <td>${row.ORIGINAL_SERN || ""}</td>
            <td>${row.ERSATZ_ITNO || ""}</td>
            <td>${row.ERSATZ_SERN || ""}</td>
            <td>${row.USER || ""}</td>
            <td>${row.TIMESTAMP || ""}</td>
            <td class="swap-action-cell">
              <button class="swap-check-btn ${isSelected ? "swap-check-btn--active" : ""}" data-key="${key}" title="${
                isSelected ? "Markierung entfernen" : "Für Tausch markieren"
              }">${isSelected ? "J" : "N"}</button>
            </td>
            <td>
              <button class="swap-delete-btn" data-key="${key}" data-wagen="${row.WAGEN_ITNO || ""}" data-sern="${
          row.WAGEN_SERN || ""
        }" data-original-itno="${row.ORIGINAL_ITNO || ""}" data-original-sern="${row.ORIGINAL_SERN || ""}" title="Datensatz löschen">X</button>
            </td>
          </tr>
        `;
      })
      .join("");
    if (swapPaginationInfo) {
      swapPaginationInfo.textContent = `Seite ${swapPage} / ${totalPages}`;
    }
    if (swapPrevPageBtn) swapPrevPageBtn.disabled = swapPage <= 1;
    if (swapNextPageBtn) swapNextPageBtn.disabled = swapPage >= totalPages;
  }
  if (swapCountInfo) {
    swapCountInfo.textContent = swapRows.length
      ? `${swapRows.length} offene Tauschdatensätze`
      : "Keine offenen Tauschdatensätze";
  }
};

const renderPartsTable = () => {
  if (!partsResultsBody) return;
  if (!sparePartsRows.length) {
    partsResultsBody.innerHTML = '<tr><td colspan="6">Keine Treffer gefunden.</td></tr>';
    if (partsPaginationInfo) partsPaginationInfo.textContent = "Seite 1 / 1";
    if (partsPrevPageBtn) partsPrevPageBtn.disabled = true;
    if (partsNextPageBtn) partsNextPageBtn.disabled = true;
    return;
  }
  const totalPages = Math.max(1, Math.ceil(sparePartsRows.length / PARTS_PAGE_SIZE));
  sparePartsPage = Math.min(sparePartsPage, totalPages);
  const start = (sparePartsPage - 1) * PARTS_PAGE_SIZE;
  const rows = sparePartsRows.slice(start, start + PARTS_PAGE_SIZE);
  partsResultsBody.innerHTML = rows
    .map(
      (row, idx) => `
      <tr>
        <td>${row["WAGEN-TYP"] || ""}</td>
        <td>${row["BAUREIHE"] || ""}</td>
        <td>${row["SERIENNUMMER"] || ""}</td>
        <td>${row["LAGERORT"] || ""}</td>
        <td>${row["LAGERPLATZ"] || ""}</td>
        <td><button class="parts-select-btn" data-row-index="${start + idx}" data-part-id="${row.ID || ""}">&#x276F;</button></td>
      </tr>`
    )
    .join("");
  if (partsPaginationInfo) partsPaginationInfo.textContent = `Seite ${sparePartsPage} / ${totalPages}`;
  if (partsPrevPageBtn) partsPrevPageBtn.disabled = sparePartsPage <= 1;
  if (partsNextPageBtn) partsNextPageBtn.disabled = sparePartsPage >= totalPages;
};

const getPartsFilterValues = () => ({
  typ: (partsFilterType?.value || "").trim(),
  item: (partsFilterItem?.value || "").trim(),
  serial: (partsFilterSerial?.value || "").trim(),
  facility: (partsFilterFacility?.value || "").trim(),
  bin: (partsFilterBin?.value || "").trim(),
});

const clearPartsFilters = (shouldReload = true) => {
  [partsFilterType, partsFilterItem, partsFilterSerial, partsFilterFacility, partsFilterBin].forEach((input) => {
    if (input) input.value = "";
  });
  sparePartsPage = 1;
  if (shouldReload) {
    loadSpareParts();
  }
};

const loadSpareParts = async () => {
  if (!currentPartsEqtp) {
    sparePartsRows = [];
    renderPartsTable();
    return;
  }
  const params = new URLSearchParams({ eqtp: currentPartsEqtp });
  const { typ, item, serial, facility, bin } = getPartsFilterValues();
  if (typ) params.append("type", typ);
  if (item) params.append("item", item);
  if (serial) params.append("serial", serial);
  if (facility) params.append("facility", facility);
  if (bin) params.append("bin", bin);
  params.append("limit", "200");
  const data = await fetchJSON(withEnv(`/api/spareparts/search?${params.toString()}`));
  sparePartsRows = data.rows || data;
  sparePartsPage = 1;
  renderPartsTable();
};

const setDatalistOptions = (datalist, values) => {
  if (!datalist) return;
  datalist.innerHTML = values.map((value) => `<option value="${value}"></option>`).join("");
};

const loadPartsFilterOptions = async () => {
  if (!currentPartsEqtp) return;
  const data = await fetchJSON(withEnv(`/api/spareparts/filters?eqtp=${currentPartsEqtp}`));
  sparePartsOptions = {
    typ: data.types || [],
    item: data.items || [],
    serial: data.serials || [],
    facility: data.facilities || [],
    bin: data.bins || [],
  };
  setDatalistOptions(partsTypeOptions, sparePartsOptions.typ);
  setDatalistOptions(partsItemOptions, sparePartsOptions.item);
  setDatalistOptions(partsSerialOptions, sparePartsOptions.serial);
  setDatalistOptions(partsFacilityOptions, sparePartsOptions.facility);
  setDatalistOptions(partsBinOptions, sparePartsOptions.bin);
};

const schedulePartsReload = () => {
  if (partsFilterTimer) {
    window.clearTimeout(partsFilterTimer);
  }
  partsFilterTimer = window.setTimeout(() => {
    sparePartsPage = 1;
    loadSpareParts();
  }, 200);
};

const setPartsModalGeometry = () => {
  if (!partsModalContent) return;
  const width = Math.min(1080, Math.max(420, window.innerWidth - 40));
  const height = Math.min(640, Math.max(320, window.innerHeight - 40));
  const left = Math.max(20, (window.innerWidth - width) / 2);
  const top = Math.max(20, (window.innerHeight - height) / 2);
  partsModalContent.style.width = `${width}px`;
  partsModalContent.style.height = `${height}px`;
  partsModalContent.style.left = `${left}px`;
  partsModalContent.style.top = `${top}px`;
};

const clampPartsModalPosition = () => {
  if (!partsModalContent) return;
  const rect = partsModalContent.getBoundingClientRect();
  const maxLeft = window.innerWidth - 40;
  const maxTop = window.innerHeight - 40;
  const width = rect.width;
  const height = rect.height;
  const left = Math.min(Math.max(10, rect.left), Math.max(10, maxLeft - width));
  const top = Math.min(Math.max(10, rect.top), Math.max(10, maxTop - height));
  partsModalContent.style.left = `${left}px`;
  partsModalContent.style.top = `${top}px`;
  const maxWidth = window.innerWidth - left - 20;
  const maxHeight = window.innerHeight - top - 20;
  partsModalContent.style.width = `${Math.min(width, maxWidth)}px`;
  partsModalContent.style.height = `${Math.min(height, maxHeight)}px`;
};

const openPartsModal = async (eqtp, contextText) => {
  if (!partsModal) return;
  currentPartsEqtp = eqtp || "";
  currentPartsContext = contextText || "";
  if (partsModalSubtitle) {
    partsModalSubtitle.textContent = eqtp
      ? `Teileart: ${eqtp}${contextText ? ` · ${contextText}` : ""}`
      : contextText || "Keine Teileart verfügbar";
  }
  clearPartsFilters(false);
  sparePartsRows = [];
  renderPartsTable();
  setPartsModalGeometry();
  partsModal.classList.remove("hidden");
  if (currentPartsEqtp) {
    await loadSpareParts();
  }
};

const closePartsModal = () => {
  if (!partsModal) return;
  partsModal.classList.add("hidden");
  if (partsFilterTimer) {
    window.clearTimeout(partsFilterTimer);
    partsFilterTimer = null;
  }
  currentPartsNodeId = null;
  currentPartsNode = null;
  currentOriginalItno = "";
  currentOriginalSern = "";
};

const loadExistingSelections = async (mtrl, sern) => {
  try {
    const data = await fetchJSON(
      withEnv(`/api/spareparts/selections?mtrl=${encodeURIComponent(mtrl)}&sern=${encodeURIComponent(sern)}`)
    );
    if (!Array.isArray(data.rows)) return;
    const map = new Map(
      data.rows.map((row) => [
        `${row.ORIGINAL_ITNO}::${row.ORIGINAL_SERN}`,
        { ersatzItno: row.ERSATZ_ITNO, ersatzSern: row.ERSATZ_SERN },
      ])
    );
    objStrkRows.forEach((row) => {
      const key = `${row.ITNO || ""}::${row.SER2 || ""}`;
      const replacement = map.get(key);
      if (replacement) {
        row.ERSATZ_ITNO = replacement.ersatzItno;
        row.ERSATZ_SERN = replacement.ersatzSern;
        const node = objStrkNodesById.get(row.__id);
        if (node) {
          node.data.ERSATZ_ITNO = replacement.ersatzItno;
          node.data.ERSATZ_SERN = replacement.ersatzSern;
        }
      }
    });
    renderObjStrkTable();
  } catch (error) {
    console.error("Konnte Ersatzteilzuordnungen nicht laden", error);
  }
  return undefined;
};

const startPartsDrag = (event) => {
  if (!partsModalContent || event.button !== 0) return;
  if (event.target instanceof Element && event.target.closest("button")) return;
  event.preventDefault();
  const rect = partsModalContent.getBoundingClientRect();
  partsDragState = {
    offsetX: event.clientX - rect.left,
    offsetY: event.clientY - rect.top,
  };
  document.addEventListener("mousemove", onPartsDrag);
  document.addEventListener("mouseup", stopPartsDrag);
};

const onPartsDrag = (event) => {
  if (!partsModalContent || !partsDragState) return;
  const left = event.clientX - partsDragState.offsetX;
  const top = event.clientY - partsDragState.offsetY;
  partsModalContent.style.left = `${Math.max(10, Math.min(left, window.innerWidth - 20))}px`;
  partsModalContent.style.top = `${Math.max(10, Math.min(top, window.innerHeight - 20))}px`;
};

const stopPartsDrag = () => {
  document.removeEventListener("mousemove", onPartsDrag);
  document.removeEventListener("mouseup", stopPartsDrag);
  partsDragState = null;
};

const startPartsResize = (event) => {
  if (!partsModalContent || event.button !== 0) return;
  event.preventDefault();
  const rect = partsModalContent.getBoundingClientRect();
  partsResizeState = {
    startX: event.clientX,
    startY: event.clientY,
    startWidth: rect.width,
    startHeight: rect.height,
  };
  document.addEventListener("mousemove", onPartsResize);
  document.addEventListener("mouseup", stopPartsResize);
};

const onPartsResize = (event) => {
  if (!partsModalContent || !partsResizeState) return;
  const minWidth = 360;
  const minHeight = 260;
  const deltaX = event.clientX - partsResizeState.startX;
  const deltaY = event.clientY - partsResizeState.startY;
  let newWidth = partsResizeState.startWidth + deltaX;
  let newHeight = partsResizeState.startHeight + deltaY;
  newWidth = Math.min(Math.max(newWidth, minWidth), window.innerWidth - 20);
  newHeight = Math.min(Math.max(newHeight, minHeight), window.innerHeight - 20);
  partsModalContent.style.width = `${newWidth}px`;
  partsModalContent.style.height = `${newHeight}px`;
};

const stopPartsResize = () => {
  document.removeEventListener("mousemove", onPartsResize);
  document.removeEventListener("mouseup", stopPartsResize);
  partsResizeState = null;
};

const submitReplacementSelection = async (rowIndex) => {
  if (
    !currentPartsNode ||
    !currentWagonItem ||
    !currentWagonSerial ||
    !currentOriginalItno ||
    !currentOriginalSern
  ) {
    return;
  }
  const row = sparePartsRows[rowIndex];
  if (!row) return;
  const ersatzItno = row.ITNO || row["BAUREIHE"] || "";
  const ersatzSern = row["SERIENNUMMER"] || "";
  const user = ensureSparePartUser();
  const payload = {
    WAGEN_ITNO: currentWagonItem,
    WAGEN_SERN: currentWagonSerial,
    ORIGINAL_ITNO: currentOriginalItno,
    ORIGINAL_SERN: currentOriginalSern,
    ERSATZ_ITNO: ersatzItno,
    ERSATZ_SERN: ersatzSern,
    USER: user,
    UPLOAD: "N",
  };
  try {
    const resp = await fetch(withEnv("/api/spareparts/select"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || "Auswahl konnte nicht gespeichert werden");
    }
    currentPartsNode.data.ERSATZ_ITNO = ersatzItno;
    currentPartsNode.data.ERSATZ_SERN = ersatzSern;
    renderObjStrkTable();
    closePartsModal();
  } catch (error) {
    console.error(error);
    window.alert(error.message || "Fehler beim Speichern der Auswahl");
  }
};

const removeReplacementSelection = async (nodeId, originalItno, originalSern) => {
  if (!nodeId || !currentWagonItem || !currentWagonSerial || !originalItno || !originalSern) {
    return;
  }
  const payload = {
    WAGEN_ITNO: currentWagonItem,
    WAGEN_SERN: currentWagonSerial,
    ORIGINAL_ITNO: originalItno,
    ORIGINAL_SERN: originalSern,
  };
  try {
    const resp = await fetch(withEnv("/api/spareparts/select"), {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || "Auswahl konnte nicht gelöscht werden");
    }
    const node = objStrkNodesById.get(nodeId);
    if (node) {
      node.data.ERSATZ_ITNO = "";
      node.data.ERSATZ_SERN = "";
    }
    const row = objStrkRows.find((entry) => entry.__id === nodeId);
    if (row) {
      row.ERSATZ_ITNO = "";
      row.ERSATZ_SERN = "";
    }
    renderObjStrkTable();
  } catch (error) {
    console.error(error);
    window.alert(error.message || "Fehler beim Löschen der Auswahl");
  }
};

const loadSwapData = async () => {
  await ensureEnvMeta();
  setOverlayContext({
    source: buildSqliteUrl(envMeta?.tables?.sparepart_swaps),
    target: window.location.origin,
  });
  showOverlay({ showRandomImage: true });
  setStatus("LADE TAUSCHDATEN ...");
  setIndeterminate(true);
  try {
    const data = await fetchJSON(withEnv("/api/spareparts/swaps?upload=N"));
    const rows = Array.isArray(data.rows) ? data.rows : [];
    const retained = new Set();
    rows.forEach((row) => {
      const key = getSwapKey(row);
      if (swapSelected.has(key)) retained.add(key);
    });
    swapSelected = retained;
    swapRows = rows;
    swapPage = 1;
    renderSwapTable();
    showSwapDbView();
    hideOverlay();
  } catch (error) {
    console.error(error);
    showError(error.message || "Tauschdaten konnten nicht geladen werden");
  }
};

const formatDateTime = (value) => {
  if (!value) return "";
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? value : date.toLocaleString("de-DE");
};

const appendRsrd2Log = (message, variant = "info") => {
  if (!rsrd2Log || !message) return;
  const entry = document.createElement("div");
  entry.className = `rsrd2-log-entry rsrd2-log-entry--${variant}`;
  const timeSpan = document.createElement("span");
  timeSpan.className = "rsrd2-log-time";
  timeSpan.textContent = new Date().toLocaleTimeString("de-DE", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  const messageSpan = document.createElement("span");
  messageSpan.className = "rsrd2-log-message";
  messageSpan.textContent = message;
  entry.append(timeSpan, messageSpan);
  rsrd2Log.append(entry);
  rsrd2Log.classList.remove("rsrd2-log-empty");
  while (rsrd2Log.childElementCount > RSRD2_LOG_LIMIT) {
    rsrd2Log.removeChild(rsrd2Log.firstElementChild);
  }
  rsrd2Log.scrollTop = rsrd2Log.scrollHeight;
};

const pruneLastRsrd2InfoIfMatch = (message) => {
  if (!rsrd2Log || !message) return;
  const lastEntry = rsrd2Log.lastElementChild;
  if (!lastEntry) return;
  if (!lastEntry.classList.contains("rsrd2-log-entry--info")) return;
  const lastMessage = lastEntry.querySelector(".rsrd2-log-message");
  if (!lastMessage) return;
  if (lastMessage.textContent === message) {
    rsrd2Log.removeChild(lastEntry);
  }
};

const clearRsrd2Log = () => {
  if (!rsrd2Log) return;
  rsrd2Log.innerHTML = "";
  rsrd2Log.classList.add("rsrd2-log-empty");
};

const parseErrorResponse = async (resp) => {
  const text = await resp.text();
  try {
    const parsed = JSON.parse(text);
    if (parsed && parsed.detail) return parsed.detail;
  } catch {
    // ignore JSON parse errors
  }
  if (text) return text;
  return resp.statusText || "Unbekannter Fehler";
};

const loadRsrd2Wagons = async (force = false) => {
  if (!rsrd2Status) return;
  if (rsrd2Loaded && !force) return;
  rsrd2Status.textContent = "Lade Wagennummern aus dem Cache ...";
  try {
    const data = await fetchJSON(withRsrdEnv("/api/rsrd2/wagons?limit=1"));
    const count = data.total || 0;
    const summary = count
      ? `${count} Datensätze vorhanden (Stand ${formatDateTime(new Date().toISOString())})`
      : "Noch keine Wagennummern geladen.";
    rsrd2Status.textContent = summary;
    if (force) {
      appendRsrd2Log(
        count ? `Cache aktualisiert: ${count} Wagennummern verfügbar.` : "Cache aktualisiert: Keine Wagennummern vorhanden.",
        "info",
      );
    }
    rsrd2Loaded = true;
  } catch (error) {
    console.error(error);
    rsrd2Status.textContent = error.message || "Abruf fehlgeschlagen.";
    appendRsrd2Log(error.message || "Abruf fehlgeschlagen.", "error");
  }
};

const rsrd2Filters = () => ({
  sern: (rsrd2FilterSern?.value || "").trim(),
  baureihe: (rsrd2FilterBaureihe?.value || "").trim(),
  halter: (rsrd2FilterHalter?.value || "").trim(),
  uic: (rsrd2FilterUic?.value || "").trim(),
  status: (rsrd2FilterStatus?.value || "").trim(),
});

const buildRsrd2OverviewUrl = () => {
  const offset = (rsrd2OverviewPage - 1) * rsrd2OverviewPageSize;
  const params = new URLSearchParams({
    limit: String(rsrd2OverviewPageSize),
    offset: String(offset),
  });
  const filters = rsrd2Filters();
  if (filters.sern) params.set("sern", filters.sern);
  if (filters.baureihe) params.set("baureihe", filters.baureihe);
  if (filters.halter) params.set("halter", filters.halter);
  if (filters.uic) params.set("uic", filters.uic);
  if (filters.status) params.set("status", filters.status);
  return withRsrd2Env(`/api/rsrd2/overview?${params.toString()}`);
};

const normalizeSyncEnv = (value) => {
  const normalized = String(value || "").trim().toUpperCase();
  if (normalized === "T" || normalized === "P") return normalized;
  return "N";
};

const applySyncEnvClass = (select, value) => {
  if (!select) return;
  const normalized = normalizeSyncEnv(value).toLowerCase();
  select.classList.remove("rsrd2-env-select--n", "rsrd2-env-select--t", "rsrd2-env-select--p");
  select.classList.add(`rsrd2-env-select--${normalized}`);
};

const createSyncEnvSelect = ({ wagonNumber, kind, value }) => {
  const select = document.createElement("select");
  select.className = "rsrd2-env-select";
  [
    { value: "N", label: "N" },
    { value: "T", label: "T" },
    { value: "P", label: "P" },
  ].forEach((option) => {
    const opt = document.createElement("option");
    opt.value = option.value;
    opt.textContent = option.label;
    select.appendChild(opt);
  });
  const initialValue = normalizeSyncEnv(value);
  select.value = initialValue;
  applySyncEnvClass(select, initialValue);
  select.addEventListener("change", async () => {
    const nextValue = normalizeSyncEnv(select.value);
    const previousValue = normalizeSyncEnv(value);
    applySyncEnvClass(select, nextValue);
    try {
      const resp = await fetch(withErpEnv("/api/rsrd2/sync_env"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ wagon: wagonNumber, kind, value: nextValue }),
      });
      if (!resp.ok) {
        const message = await parseErrorResponse(resp);
        throw new Error(message || "Sync-Auswahl konnte nicht gespeichert werden.");
      }
      value = nextValue;
    } catch (error) {
      select.value = previousValue;
      applySyncEnvClass(select, previousValue);
      appendRsrd2Log(error.message || "Sync-Auswahl konnte nicht gespeichert werden.", "error");
    }
  });
  return select;
};

const updateGoButtonState = () => {
  if (!rsrd2GoBtn) return;
  const hasSelection = rsrd2UploadSelected.size > 0;
  rsrd2GoBtn.classList.toggle("rsrd2-go-btn--armed", hasSelection);
  rsrd2GoBtn.classList.toggle("rsrd2-go-btn--idle", !hasSelection);
};

const initBulkSelect = (select) => {
  if (!select) return;
  const value = normalizeSyncEnv(select.value);
  select.value = value;
  applySyncEnvClass(select, value);
  select.addEventListener("change", () => {
    applySyncEnvClass(select, normalizeSyncEnv(select.value));
  });
};

const normalizeTransferValue = (value) => {
  const normalized = String(value || "").trim().toUpperCase();
  return normalized === "J" ? "J" : "N";
};

const bulkSetTransfer = async ({ select }) => {
  if (!select || !rsrd2Status) return;
  const value = normalizeTransferValue(select.value);
  const filters = rsrd2Filters();
  const startText = `Setze 1x Übertragen auf ${value} (gefilterte Wagen) ...`;
  rsrd2Status.textContent = startText;
  appendRsrd2Log(startText, "info");
  try {
    const resp = await fetch(withRsrd2Env("/api/rsrd2/one_time_transfer_bulk"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value, filters }),
    });
    if (!resp.ok) {
      const message = await parseErrorResponse(resp);
      throw new Error(message || "Bulk-Update fehlgeschlagen.");
    }
    const data = await resp.json().catch(() => ({}));
    const count = data.total ?? 0;
    const summary = `Bulk-Update 1x Übertragen: ${count} Wagen gesetzt.`;
    rsrd2Status.textContent = summary;
    appendRsrd2Log(summary, "success");
    rsrd2OverviewLoaded = false;
    loadRsrd2Overview(true);
  } catch (error) {
    const message = error.message || "Bulk-Update fehlgeschlagen.";
    rsrd2Status.textContent = message;
    appendRsrd2Log(message, "error");
  }
};

const bulkSetSyncEnv = async ({ kind, select }) => {
  if (!select || !rsrd2Status) return;
  const value = normalizeSyncEnv(select.value);
  const filters = rsrd2Filters();
  const startText = `Setze ${kind.toUpperCase()} auf ${value} (gefilterte Wagen) ...`;
  rsrd2Status.textContent = startText;
  appendRsrd2Log(startText, "info");
  try {
    const resp = await fetch(withRsrd2Env("/api/rsrd2/sync_env_bulk"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ kind, value, filters }),
    });
    if (!resp.ok) {
      const message = await parseErrorResponse(resp);
      throw new Error(message || "Bulk-Update fehlgeschlagen.");
    }
    const data = await resp.json().catch(() => ({}));
    const count = data.total ?? 0;
    const summary = `Bulk-Update ${kind.toUpperCase()}: ${count} Wagen gesetzt.`;
    rsrd2Status.textContent = summary;
    appendRsrd2Log(summary, "success");
    rsrd2OverviewLoaded = false;
    loadRsrd2Overview(true);
  } catch (error) {
    const message = error.message || "Bulk-Update fehlgeschlagen.";
    rsrd2Status.textContent = message;
    appendRsrd2Log(message, "error");
  }
};

const renderRsrd2Overview = () => {
  if (!rsrd2TableBody) return;
  rsrd2TableBody.innerHTML = "";
  if (!rsrd2OverviewRows.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 9;
    cell.textContent = "Keine Daten gefunden.";
    row.appendChild(cell);
    rsrd2TableBody.appendChild(row);
  } else {
    rsrd2OverviewRows.forEach((row) => {
      const tr = document.createElement("tr");
      const wagonNumber = row.wagon_number || "-";
      const wagonLabel = formatWagonSerial(wagonNumber);

      const rsrdOk = row.rsrd_present !== undefined ? Boolean(row.rsrd_present) : Boolean(row.rsrd_wagon_id);
      tr.innerHTML = `
        <td><span class="rsrd2-dot ${rsrdOk ? "rsrd2-dot--ok" : "rsrd2-dot--missing"}"></span>${wagonLabel}</td>
        <td>${row.baureihe || "-"}</td>
        <td>${row.wagen_typ || "-"}</td>
        <td>${row.halter_code || "-"}</td>
      `;

      const syncDataCell = document.createElement("td");
      syncDataCell.appendChild(
        createSyncEnvSelect({ wagonNumber, kind: "data", value: row.sync_data_env }),
      );
      tr.appendChild(syncDataCell);

      const syncKmCell = document.createElement("td");
      syncKmCell.appendChild(
        createSyncEnvSelect({ wagonNumber, kind: "km", value: row.sync_km_env }),
      );
      tr.appendChild(syncKmCell);

      const syncDocsCell = document.createElement("td");
      syncDocsCell.appendChild(
        createSyncEnvSelect({ wagonNumber, kind: "docs", value: row.sync_docs_env }),
      );
      tr.appendChild(syncDocsCell);

      const uploadCell = document.createElement("td");
      const uploadWrap = document.createElement("label");
      uploadWrap.className = "rsrd2-check-wrap";
      const uploadCheckbox = document.createElement("input");
      uploadCheckbox.type = "checkbox";
      uploadCheckbox.className = "rsrd2-checkbox";
      uploadCheckbox.setAttribute("aria-label", `1x Übertragen ${wagonNumber}`);
      const transferEnabled = ["1", "true", "y", "j"].includes(String(row.one_time_transfer || "").toLowerCase());
      uploadCheckbox.checked = transferEnabled;
      if (transferEnabled) {
        rsrd2UploadSelected.add(wagonNumber);
      } else {
        rsrd2UploadSelected.delete(wagonNumber);
      }
      updateGoButtonState();
      uploadCheckbox.addEventListener("change", async () => {
        const enabled = uploadCheckbox.checked;
        try {
          const resp = await fetch(withErpEnv("/api/rsrd2/one_time_transfer"), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ wagon: wagonNumber, enabled }),
          });
          if (!resp.ok) {
            const message = await parseErrorResponse(resp);
            throw new Error(message || "Auswahl konnte nicht gespeichert werden.");
          }
          if (enabled) {
            rsrd2UploadSelected.add(wagonNumber);
          } else {
            rsrd2UploadSelected.delete(wagonNumber);
          }
          updateGoButtonState();
        } catch (error) {
          uploadCheckbox.checked = !enabled;
          appendRsrd2Log(error.message || "Auswahl konnte nicht gespeichert werden.", "error");
        }
      });
      uploadWrap.appendChild(uploadCheckbox);
      uploadCell.appendChild(uploadWrap);
      tr.appendChild(uploadCell);

      const compareCell = document.createElement("td");
      const compareLink = document.createElement("a");
      compareLink.className = "rsrd2-compare-btn";
      compareLink.href = `/rsrd2_compare.html?sern=${encodeURIComponent(wagonNumber)}`;
      compareLink.title = "Vergleichen";
      compareLink.setAttribute("aria-label", "Vergleichen");
      compareLink.innerHTML = `<img src="bilder/rsrd2-compare.svg" alt="" />`;
      compareCell.appendChild(compareLink);
      tr.appendChild(compareCell);

      rsrd2TableBody.appendChild(tr);
    });
  }

  if (rsrd2CountInfo) {
    rsrd2CountInfo.textContent = `${rsrd2OverviewTotal} Datensätze`;
  }
  if (rsrd2PaginationInfo) {
    const totalPages = Math.max(1, Math.ceil(rsrd2OverviewTotal / rsrd2OverviewPageSize));
    rsrd2PaginationInfo.textContent = `Seite ${rsrd2OverviewPage} / ${totalPages}`;
  }
  if (rsrd2PrevPage) rsrd2PrevPage.disabled = rsrd2OverviewPage <= 1;
  if (rsrd2NextPage) {
    const totalPages = Math.max(1, Math.ceil(rsrd2OverviewTotal / rsrd2OverviewPageSize));
    rsrd2NextPage.disabled = rsrd2OverviewPage >= totalPages;
  }
};

const loadRsrd2Overview = async (force = false) => {
  if (!rsrd2TableBody || !rsrd2CountInfo) return;
  if (rsrd2OverviewLoaded && !force) return;
  try {
    const data = await fetchJSON(buildRsrd2OverviewUrl());
    rsrd2OverviewRows = Array.isArray(data.rows) ? data.rows : [];
    rsrd2OverviewTotal = data.total || 0;
    rsrd2OverviewLoaded = true;
    renderRsrd2Overview();
  } catch (error) {
    console.error(error);
    rsrd2OverviewRows = [];
    rsrd2OverviewTotal = 0;
    renderRsrd2Overview();
    appendRsrd2Log(error.message || "Übersicht konnte nicht geladen werden.", "error");
  }
};

const ensureRsrdLoaderSubtitle = () => {
  if (loaderSubtitle && MODULE_META.rsrd2?.loaderSubtitle) {
    loaderSubtitle.textContent = MODULE_META.rsrd2.loaderSubtitle;
  }
};

const runRsrdAction = async ({ url, startMessage, successMessage, onSuccess }) => {
  if (!rsrd2Status) return;
  const startText = startMessage || "Operation wird ausgeführt ...";
  rsrd2Status.textContent = startText;
  appendRsrd2Log(startText, "info");
  ensureRsrdLoaderSubtitle();
  showOverlayWithImage("bilder/RSRD-1.svg", "RSRD2", startText);
  setIndeterminate(true);
  try {
    const resp = await fetch(url, { method: "POST" });
    if (!resp.ok) {
      const message = await parseErrorResponse(resp);
      throw new Error(message || "Aktion fehlgeschlagen.");
    }
    const data = await resp.json().catch(() => ({}));
    if (successMessage) {
      const finalMessage = typeof successMessage === "function" ? successMessage(data) : successMessage;
      rsrd2Status.textContent = finalMessage;
      appendRsrd2Log(finalMessage, "success");
    }
    if (Array.isArray(data?.logs)) {
      data.logs.forEach((entry) => appendRsrd2Log(entry, "info"));
    }
    if (onSuccess) await onSuccess(data);
  } catch (error) {
    const fallback = error.message || "Aktion fehlgeschlagen.";
    rsrd2Status.textContent = fallback;
    appendRsrd2Log(fallback, "error");
    throw error;
  } finally {
    hideOverlay();
  }
};

const watchRsrdJob = (jobId, successBuilder) =>
  new Promise((resolve, reject) => {
    if (!jobId) {
      reject(new Error("Job-ID fehlt."));
      return;
    }
    const poll = async () => {
      try {
        const resp = await fetch(rsrd2Api(`/api/rsrd2/jobs/${encodeURIComponent(jobId)}`));
        if (!resp.ok) {
          const message = await parseErrorResponse(resp);
          throw new Error(message || "Statusabfrage fehlgeschlagen.");
        }
        const data = await resp.json();
        const logs = Array.isArray(data.logs) ? data.logs : [];
        const last = rsrd2JobOffsets[jobId] || 0;
        for (let idx = last; idx < logs.length; idx += 1) {
          appendRsrd2Log(logs[idx], "info");
        }
        rsrd2JobOffsets[jobId] = logs.length;
        if (data.status === "running") {
          const startedAt = rsrd2JobStartTimes[jobId] || Date.now();
          rsrd2JobStartTimes[jobId] = startedAt;
          const elapsedSec = Math.max(0, Math.floor((Date.now() - startedAt) / 1000));
          const mins = String(Math.floor(elapsedSec / 60)).padStart(2, "0");
          const secs = String(elapsedSec % 60).padStart(2, "0");
          const runningMessage = logs.length
            ? logs[logs.length - 1]
            : `RSRD2 läuft ... (${mins}:${secs})`;
          if (rsrd2Status) rsrd2Status.textContent = runningMessage;
          setStatus(runningMessage);
          window.setTimeout(poll, RSRD2_JOB_POLL_MS);
          return;
        }
        delete rsrd2JobOffsets[jobId];
        delete rsrd2JobStartTimes[jobId];
        if (data.status === "success") {
          const summary =
            typeof successBuilder === "function" ? successBuilder(data.result || {}, data) : "Aktion abgeschlossen.";
          if (summary) {
            pruneLastRsrd2InfoIfMatch(summary);
            appendRsrd2Log(summary, "success");
            if (rsrd2Status) rsrd2Status.textContent = summary;
            setStatus(summary);
          }
          resolve(data);
        } else {
          const message = data.error || "Aktion fehlgeschlagen.";
          appendRsrd2Log(message, "error");
          if (rsrd2Status) rsrd2Status.textContent = message;
          reject(new Error(message));
        }
      } catch (error) {
        appendRsrd2Log(error.message || "Statusabfrage fehlgeschlagen.", "error");
        if (rsrd2Status) rsrd2Status.textContent = error.message || "Statusabfrage fehlgeschlagen.";
        delete rsrd2JobOffsets[jobId];
        delete rsrd2JobStartTimes[jobId];
        reject(error);
      }
    };
    poll();
  });

const startRsrdJob = async ({ url, startMessage, successBuilder, afterSuccess }) => {
  if (!rsrd2Status) return;
  const startText = startMessage || "Vorgang gestartet ...";
  rsrd2Status.textContent = startText;
  appendRsrd2Log(startText, "info");
  ensureRsrdLoaderSubtitle();
  showOverlayWithImage("bilder/RSRD-1.svg", "RSRD2", startText);
  setIndeterminate(true);
  try {
    const resp = await fetch(url, { method: "POST" });
    if (!resp.ok) {
      const message = await parseErrorResponse(resp);
      throw new Error(message || "Aktion fehlgeschlagen.");
    }
    const data = await resp.json().catch(() => ({}));
    const jobId = data.job_id;
    if (jobId) {
      rsrd2JobOffsets[jobId] = 0;
      rsrd2JobStartTimes[jobId] = Date.now();
      const startedMessage = `Job gestartet (${jobId}).`;
      appendRsrd2Log(startedMessage, "info");
      if (rsrd2Status) rsrd2Status.textContent = startedMessage;
      setStatus(startedMessage);
    }
    await watchRsrdJob(jobId, successBuilder);
    if (afterSuccess) {
      await afterSuccess();
    }
  } catch (error) {
    const fallback = error.message || "Aktion fehlgeschlagen.";
    rsrd2Status.textContent = fallback;
    appendRsrd2Log(fallback, "error");
  } finally {
    hideOverlay();
  }
};

const loadErpWagons = async () => {
  const meta = await ensureRsrd2Meta();
  const erpMeta = meta?.erp;
  setOverlayContext({
    source: erpMeta?.urls?.compass,
    target: buildSqliteUrlFor(erpMeta, erpMeta?.tables?.rsrd_erp_numbers),
  });
  return startRsrdJob({
    url: withErpEnv("/api/rsrd2/load_erp"),
    startMessage: "Lade Wagennummern aus dem ERP ...",
    successBuilder: (result) => {
      rsrd2Loaded = false;
      return `ERP-Wagennummern geladen: ${result?.count_wagons ?? 0}.`;
    },
  });
};

const loadErpAttributes = async () => {
  const meta = await ensureRsrd2Meta();
  const erpMeta = meta?.erp;
  setOverlayContext({
    source: erpMeta?.urls?.compass,
    target: buildSqliteUrlFor(erpMeta, erpMeta?.tables?.rsrd_erp_full),
  });
  return startRsrdJob({
    url: withErpEnv("/api/rsrd2/load_erp_full"),
    startMessage: "Lade Wagenattribute aus dem ERP ...",
    successBuilder: (result) => {
      rsrd2OverviewLoaded = false;
      return `ERP-Wagenattribute geladen: ${result?.count_full ?? 0} Datensätze.`;
    },
    afterSuccess: async () => {
      rsrd2OverviewPage = 1;
      await loadRsrd2Overview(true);
    },
  });
};

const fetchRsrdJson = async () => {
  const meta = await ensureRsrd2Meta();
  const rsrdMeta = meta?.rsrd;
  setOverlayContext({
    source: rsrdMeta?.urls?.rsrd_wsdl,
    target: [
      buildSqliteUrlFor(rsrdMeta, rsrdMeta?.tables?.rsrd?.wagons),
      buildSqliteUrlFor(rsrdMeta, rsrdMeta?.tables?.rsrd?.json),
    ],
  });
  return runRsrdAction({
    url: withRsrd2Env("/api/rsrd2/fetch_json"),
    startMessage: "JSON aus RSRD wird geladen ...",
    successMessage: (data) => `JSON geladen für ${data?.staged ?? 0} Wagen.`,
    onSuccess: async () => {
      rsrd2Loaded = false;
      await loadRsrd2Wagons(true);
    },
  });
};

const processRsrdJson = async () => {
  const meta = await ensureRsrd2Meta();
  const rsrdMeta = meta?.rsrd;
  setOverlayContext({
    source: buildSqliteUrlFor(rsrdMeta, rsrdMeta?.tables?.rsrd?.json),
    target: buildSqliteUrlFor(rsrdMeta, rsrdMeta?.tables?.rsrd?.detail),
  });
  return runRsrdAction({
    url: withRsrd2Env("/api/rsrd2/process_json"),
    startMessage: "Verarbeite JSON in strukturierte Daten ...",
    successMessage: (data) => `JSON verarbeitet für ${data?.processed ?? 0} Wagen.`,
  });
};

const formatDiffValue = (value) => {
  if (value === null || value === undefined || value === "") return "leer";
  if (Array.isArray(value)) return value.join(", ");
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
};

const formatWagonSerial = (value) => {
  if (!value) return "-";
  const digits = String(value).replace(/\\D/g, "");
  if (digits.length === 12) {
    return `${digits.slice(0, 2)} ${digits.slice(2, 4)} ${digits.slice(4, 8)} ${digits.slice(8, 11)}-${digits.slice(11)}`;
  }
  return String(value);
};

const extractRsrdFieldName = (uploadField, fallback) => {
  if (!uploadField) return fallback || "n/a";
  const parts = uploadField.split("/");
  let last = parts[parts.length - 1] || "";
  if (last.includes("(")) {
    last = last.split("(")[0];
  }
  return last || fallback || "n/a";
};

const DEBUG_RSRD_COMPARE_WAGONS = ["378445949472"];

const compareRsrdData = async () => {
  if (!rsrd2Status) return;
  const startText = "Prüfung läuft ...";
  rsrd2Status.textContent = startText;
  clearRsrd2Log();
  appendRsrd2Log(startText, "info");
  ensureRsrdLoaderSubtitle();
  const meta = await ensureRsrd2Meta();
  const erpMeta = meta?.erp;
  const rsrdMeta = meta?.rsrd;
  setOverlayContext({
    source: [
      buildSqliteUrlFor(erpMeta, erpMeta?.tables?.rsrd_erp_full),
      buildSqliteUrlFor(rsrdMeta, rsrdMeta?.tables?.rsrd?.detail),
    ],
    target: buildSqliteUrlFor(rsrdMeta, rsrdMeta?.tables?.rsrd_upload),
  });
  showOverlay();
  setStatus(startText);
  setIndeterminate(true);
  try {
    const resp = await fetch(withRsrd2Env("/api/rsrd2/compare?create_upload=true"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ wagons: DEBUG_RSRD_COMPARE_WAGONS }),
    });
    if (!resp.ok) {
      const message = await parseErrorResponse(resp);
      throw new Error(message || "Prüfung fehlgeschlagen.");
    }
    const data = await resp.json().catch(() => ({}));
    const rows = Array.isArray(data.rows) ? data.rows : [];
    const withDiffs = rows.filter((row) => (row.diff_count || 0) > 0);
    const missing = rows.filter((row) => row.rsrd_missing);

    rows.forEach((row) => {
      const wagon = row.wagon_number_freight || "unbekannt";
      if (row.rsrd_missing) {
        appendRsrd2Log(`Wagen ${wagon}: keine RSRD-Daten gefunden.`, "error");
        return;
      }
      const diffs = Array.isArray(row.differences) ? row.differences : [];
      const diffCount = diffs.filter((diff) => !diff.equal).length;
      appendRsrd2Log(`Wagen ${wagon}: ${diffCount} Abweichungen (Detailvergleich).`, "info");
      diffs.forEach((diff) => {
        if (diff.equal) return;
        const erpValue = formatDiffValue(diff.erp);
        const rsrdValue = formatDiffValue(diff.rsrd);
        const erpField = diff.erp_field || diff.field || "n/a";
        const uploadRequirement = diff.upload_requirement || "n/a";
        const rsrdFieldName = extractRsrdFieldName(diff.upload_field, diff.rsrd_field || diff.field);
        appendRsrd2Log(
          `ERP:${erpField}=${erpValue} | RSRD:${rsrdFieldName}=${rsrdValue} | UL: ${uploadRequirement}`,
          "error",
        );
      });
    });

    const summary = `Prüfung abgeschlossen: ${rows.length} Wagen geprüft, ${withDiffs.length} mit Abweichungen, ${data.created ?? 0} Neuanlagen erstellt.`;
    rsrd2Status.textContent = summary;
    appendRsrd2Log(summary, missing.length || withDiffs.length ? "success" : "info");
  } catch (error) {
    const fallback = error.message || "Prüfung fehlgeschlagen.";
    rsrd2Status.textContent = fallback;
    appendRsrd2Log(fallback, "error");
  } finally {
    hideOverlay();
  }
};

const uploadSelectedRsrd2 = async () => {
  if (!rsrd2Status) return;
  const wagons = Array.from(rsrd2UploadSelected);
  if (!wagons.length) {
    rsrd2Status.textContent = "Keine Wagen für Neuanlage markiert.";
    return;
  }
  const startText = `Neuanlage wird vorbereitet (${wagons.length} Wagen) ...`;
  rsrd2Status.textContent = startText;
  appendRsrd2Log(startText, "info");
  ensureRsrdLoaderSubtitle();
  const meta = await ensureRsrd2Meta();
  const erpMeta = meta?.erp;
  const rsrdMeta = meta?.rsrd;
  setOverlayContext({
    source: buildSqliteUrlFor(erpMeta, erpMeta?.tables?.rsrd_erp_full),
    target: buildSqliteUrlFor(rsrdMeta, rsrdMeta?.tables?.rsrd_upload),
  });
  showOverlay();
  setStatus(startText);
  setIndeterminate(true);
  try {
    const resp = await fetch(withRsrd2Env("/api/rsrd2/compare?create_upload=true&include_all=false"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ wagons }),
    });
    if (!resp.ok) {
      const message = await parseErrorResponse(resp);
      throw new Error(message || "Neuanlage fehlgeschlagen.");
    }
    const data = await resp.json().catch(() => ({}));
    const created = data.created ?? 0;
    const summary = `Neuanlage vorbereitet: ${created} Wagen.`;
    rsrd2Status.textContent = summary;
    appendRsrd2Log(summary, "success");
    rsrd2UploadSelected.clear();
    updateGoButtonState();
    renderRsrd2Overview();
  } catch (error) {
    const message = error.message || "Neuanlage fehlgeschlagen.";
    rsrd2Status.textContent = message;
    appendRsrd2Log(message, "error");
  } finally {
    hideOverlay();
  }
};

const syncRsrd2All = async () => {
  if (!rsrd2Status) return;
  rsrd2Status.textContent = "Starte RSRD2-Synchronisation ...";
  appendRsrd2Log("Starte RSRD2-Synchronisation ...", "info");
  ensureRsrdLoaderSubtitle();
  const meta = await ensureRsrd2Meta();
  const erpMeta = meta?.erp;
  const rsrdMeta = meta?.rsrd;
  setOverlayContext({
    source: [
      rsrdMeta?.urls?.rsrd_wsdl,
      buildSqliteUrlFor(erpMeta, erpMeta?.tables?.rsrd_erp_numbers),
    ],
    target: [
      buildSqliteUrlFor(rsrdMeta, rsrdMeta?.tables?.rsrd?.json),
      buildSqliteUrlFor(rsrdMeta, rsrdMeta?.tables?.rsrd?.detail),
    ],
  });
  showOverlay();
  setStatus("RSRD2-Daten werden geladen ...");
  setIndeterminate(true);
  try {
    const resp = await fetch(withRsrd2Env("/api/rsrd2/sync_all"), { method: "POST" });
    if (!resp.ok) {
      const message = await parseErrorResponse(resp);
      throw new Error(message || "Synchronisation fehlgeschlagen");
    }
    const payload = await resp.json().catch(() => ({}));
    const synced = payload.synced ?? 0;
    const finalText = `RSRD2-Abruf abgeschlossen (${synced} Wagennummern).`;
    rsrd2Status.textContent = finalText;
    appendRsrd2Log(finalText, "success");
    rsrd2Loaded = false;
    await loadRsrd2Wagons(true);
  } catch (error) {
    console.error(error);
    const fail = error.message || "Synchronisation fehlgeschlagen.";
    rsrd2Status.textContent = fail;
    appendRsrd2Log(fail, "error");
  } finally {
    hideOverlay();
  }
};

const toggleSwapSelection = (key) => {
  if (!key) return;
  if (swapSelected.has(key)) {
    swapSelected.delete(key);
  } else {
    swapSelected.add(key);
  }
  renderSwapTable();
};

const markAllSwapRecords = () => {
  swapSelected = new Set(swapRows.map((row) => getSwapKey(row)));
  renderSwapTable();
};

const clearAllSwapSelections = () => {
  swapSelected.clear();
  renderSwapTable();
};

const deleteSwapRecord = async (key) => {
  if (!key) return;
  const row = findSwapRowByKey(key);
  if (!row) return;
  const payload = {
    WAGEN_ITNO: row.WAGEN_ITNO || "",
    WAGEN_SERN: row.WAGEN_SERN || "",
    ORIGINAL_ITNO: row.ORIGINAL_ITNO || "",
    ORIGINAL_SERN: row.ORIGINAL_SERN || "",
  };
  try {
    const resp = await fetch(withEnv("/api/spareparts/select"), {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || "Datensatz konnte nicht gelöscht werden");
    }
    swapRows = swapRows.filter((row) => getSwapKey(row) !== key);
    swapSelected.delete(key);
    renderSwapTable();
  } catch (error) {
    console.error(error);
    window.alert(error.message || "Fehler beim Löschen des Datensatzes");
  }
};

const updateObjStrkMeta = (count, mtrl, sern) => {
  if (!objstrkMeta) return;
  if (!count) {
    objstrkMeta.textContent = "Keine Positionen gefunden";
    return;
  }
  objstrkMeta.textContent = `${count} Positionen · BAUREIHE: ${mtrl} · SERIENNUMMER: ${sern}`;
};

const initPaginationControls = () => {
  prevBtn.addEventListener("click", () => {
    if (currentPage > 1) {
      currentPage -= 1;
      renderPage();
    }
  });
  nextBtn.addEventListener("click", () => {
    const maxPage = Math.max(1, Math.ceil(wagons.length / pageSize));
    if (currentPage < maxPage) {
      currentPage += 1;
      renderPage();
    }
  });
};

const initPageSizeControl = () => {
  const storedSize = Number(window.localStorage.getItem("sparepart.pageSize"));
  if ([25, 50].includes(storedSize)) {
    pageSize = storedSize;
  } else if (storedSize === 10) {
    pageSize = 25;
    window.localStorage.removeItem("sparepart.pageSize");
  }

  if (!pageSizeSelect) return;

  pageSizeSelect.value = String(pageSize);
  pageSizeSelect.addEventListener("change", () => {
    const nextSize = Number(pageSizeSelect.value);
    if (![25, 50].includes(nextSize)) return;
    pageSize = nextSize;
    window.localStorage.setItem("sparepart.pageSize", String(pageSize));
    currentPage = 1;
    renderPage();
  });
};


const showTable = () => {
  hideOverlay();
  showWagonsView();
  tablePanel.classList.remove("hidden");
  renderTableHead();
  applyFilters();
};

const showError = (message) => {
  showOverlay();
  progressDetail.innerHTML = `<div class="error-banner">${message}</div>`;
};

const handleObjStrkResponse = (payload, { mtrl, sern }) => {
  const rows = normalizeObjStrkRows(payload);
  const columns = getObjStrkColumns();
  const isFlat = currentModule === "wagenumbau";
  objStrkRows = rows.map((row, index) => {
    const normalized = {};
    columns.forEach((column) => {
      const sources = OBJSTRK_FIELD_SOURCES[column];
      if (sources) {
        let found = "";
        for (const source of sources) {
          const value = row[source];
          if (value !== null && value !== undefined && value !== "") {
            found = value;
            break;
          }
        }
        normalized[column] = found;
      } else {
        normalized[column] = row[column] ?? "";
      }
    });
    normalized.EQTP = row.EQTP ?? row["TEILEART"] ?? "";
    const id = (row.SER2 || "").trim() || `node-${index}`;
    const parentId = isFlat ? "" : (row.SERN || "").trim() || "";
    normalized.__id = id;
    normalized.__parentId = parentId;
    return normalized;
  });
  const count = objStrkRows.length;
  updateObjStrkMeta(count, mtrl, sern);
  if (currentModule === "wagenumbau") {
    updateRenumberItnoOptions();
    if (renumberSernInput) {
      renumberSernInput.value = (sern || "").trim();
      checkRenumberSerial();
    }
    if (renumberDateInput) {
      renumberDateInput.value = getTodayDateInputValue();
    }
  }
  const { roots, map } = buildObjStrkHierarchy(objStrkRows);
  objStrkRoots = roots;
  objStrkNodesById = map;
  collapseAllObjNodes();
  expandAllObjNodes();
  objStrkSearchText = fulltextInput ? fulltextInput.value.trim().toLowerCase() : "";
  if (objStrkSearchText) {
    expandAllObjNodes();
  }
  renderObjStrkHead();
  renderObjStrkTable();
  loadExistingSelections(mtrl, sern).finally(() => {
    showObjStrkView();
    hideOverlay();
  });
};

const loadObjStrk = async (mtrl, sern) => {
  currentWagonItem = mtrl;
  currentWagonSerial = sern;
  await ensureEnvMeta();
  setOverlayContext({
    source: envMeta?.urls?.mi,
    target: window.location.origin,
  });
  showOverlay();
  setStatus("LADE OBJEKTSTRUKTUR ...");
  setIndeterminate(true);
  try {
    const storeParam =
      currentModule === "wagenumbau" ? "&store_table=RENUMBER_WAGON" : "";
    const payload = await fetchJSON(
      withEnv(
        `/api/objstrk?mtrl=${encodeURIComponent(mtrl)}&sern=${encodeURIComponent(sern)}${storeParam}`,
      ),
    );
    handleObjStrkResponse(payload, { mtrl, sern });
  } catch (error) {
    console.error(error);
    showError(error.message || "Objektstruktur konnte nicht geladen werden");
  }
};

const loadObjStrkFromRenumber = async () => {
  await ensureEnvMeta();
  setOverlayContext({
    source: buildSqliteUrl(envMeta?.tables?.renumber_wagon),
    target: window.location.origin,
  });
  showOverlay();
  setStatus("LADE OBJEKTSTRUKTUR AUS RENUMBER_WAGON ...");
  setIndeterminate(true);
  try {
    const payload = await fetchJSON(withEnv("/api/renumber/objstrk"));
    const mtrl = (payload?.wagon_itno || currentWagonItem || "").trim();
    const sern = (payload?.wagon_sern || currentWagonSerial || "").trim();
    currentWagonItem = mtrl;
    currentWagonSerial = sern;
    handleObjStrkResponse(payload, { mtrl, sern });
  } catch (error) {
    console.error(error);
    showError(error.message || "Objektstruktur konnte nicht geladen werden");
  }
};

const resolveRollbackSerial = () => {
  const fromFilter = serialFilterInput ? serialFilterInput.value.trim() : "";
  if (fromFilter) return fromFilter;
  const fallback = currentWagonSerial || "";
  return window.prompt("Seriennummer für Rollback eingeben:", fallback) || "";
};

const startRollbackFromDb = async () => {
  if (currentModule !== "wagenumbau") return;
  if (renumberJobId) {
    window.alert("Ein anderer Prozess läuft bereits.");
    return;
  }
  const hisn = resolveRollbackSerial().trim();
  if (!hisn) {
    window.alert("Seriennummer fehlt.");
    return;
  }
  disableAllRenumberButtons();
  try {
    await ensureEnvMeta();
    setOverlayContext({
      source: envMeta?.urls?.compass,
      target: [buildSqliteUrl(envMeta?.tables?.renumber_wagon), envMeta?.urls?.mi],
    });
    showOverlay();
    setStatus(`Importiere MROUHI für ${hisn} und starte Rollback ...`);
    setIndeterminate(true);
    const resp = await fetch(
      withEnv(`/api/renumber/rollback_from_mrouhi?hisn=${encodeURIComponent(hisn)}`),
      {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ hisn }),
      }
    );
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text || "Rollback aus DB fehlgeschlagen");
    }
    const data = await resp.json();
    renumberJobId = data.job_id || null;
    renumberJobMode = "rollback";
    renumberResultCount = 0;
    if (!renumberJobId) {
      throw new Error("Keine Job-ID erhalten.");
    }
    setIndeterminate(false);
    resetProgress();
    if (renumberJobTimer) {
      clearInterval(renumberJobTimer);
    }
    renumberJobTimer = window.setInterval(pollRenumberJob, 700);
    await pollRenumberJob();
  } catch (error) {
    console.error(error);
    window.alert(error.message || "Rollback aus DB fehlgeschlagen");
    enableAllRenumberButtons();
    renumberJobMode = null;
    if (!renumberJobId) {
      hideOverlay(true);
    }
  }
};

const reloadData = async () => {
  reloadBtn.disabled = true;
  const originalText = reloadBtn.textContent;
  reloadBtn.textContent = "Lädt ...";
  try {
    await ensureEnvMeta();
    if (!hasCompassSource(envMeta)) {
      throw new Error(
        "Infor-Compass-Verbindung fehlt. Reload aus dem Datalake ist nicht möglich.",
      );
    }
    const config = getWagonModuleConfig(currentModule);
    const targets = [buildSqliteUrl(getWagonTableMeta(currentModule))];
    if (config.includeSpareparts) {
      targets.push(buildSqliteUrl(envMeta?.tables?.spareparts));
    }
    setOverlayContext({
      source: envMeta?.urls?.compass,
      target: targets,
    });
    showOverlay({ showRandomImage: true });
    setStatus("Datenbank wird neu geladen ...");
    setIndeterminate(true);
    await reloadWagonTable(config);
    await startModuleWorkflow({ skipReload: true });
  } catch (error) {
    console.error(error);
    showError(error.message || "Neu laden fehlgeschlagen");
  } finally {
    reloadBtn.disabled = false;
    reloadBtn.textContent = originalText;
  }
};

initPaginationControls();
initPageSizeControl();
reloadBtn.addEventListener("click", reloadData);
if (rollbackFromDbBtn) {
  rollbackFromDbBtn.addEventListener("click", () => {
    startRollbackFromDb();
  });
}
// BEGIN WAGON RENNUMBERING
if (wagonRenumberBtn) {
  wagonRenumberBtn.addEventListener("click", async () => {
    if (renumberSequence) return;
    wagonRenumberBtn.disabled = true;
    if (renumberExecuteBtn) renumberExecuteBtn.disabled = true;
    if (renumberInstallBtn) renumberInstallBtn.disabled = true;
    if (mos170AddPropBtn) mos170AddPropBtn.disabled = true;
    if (cms100MwnoBtn) cms100MwnoBtn.disabled = true;
    if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = true;
    if (mos180ApproveBtn) mos180ApproveBtn.disabled = true;
    if (mos050MontageBtn) mos050MontageBtn.disabled = true;
    if (crs335UpdBtn) crs335UpdBtn.disabled = true;
    if (sts046DelBtn) sts046DelBtn.disabled = true;
    if (sts046AddBtn) sts046AddBtn.disabled = true;
    if (mms240UpdBtn) mms240UpdBtn.disabled = true;
    if (cusextAddBtn) cusextAddBtn.disabled = true;
    renumberJobMode = "wagon_renumber";
    try {
      await ensureEnvMeta();
      setOverlayContext({
        source: envMeta?.urls?.mi,
        target: window.location.origin,
      });
      showOverlay();
      setStatus("Wagen wird umnummeriert ...");
      setIndeterminate(true);
      await startRenumberSequenceWithSteps(buildWagonRenumberSequence(), "Wagen umbenennen");
    } catch (error) {
      console.error(error);
      wagonRenumberBtn.disabled = false;
      if (renumberExecuteBtn) renumberExecuteBtn.disabled = false;
      if (renumberInstallBtn) renumberInstallBtn.disabled = false;
      if (mos170AddPropBtn) mos170AddPropBtn.disabled = false;
      if (cms100MwnoBtn) cms100MwnoBtn.disabled = false;
      if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = false;
      if (mos180ApproveBtn) mos180ApproveBtn.disabled = false;
      if (mos050MontageBtn) mos050MontageBtn.disabled = false;
      if (crs335UpdBtn) crs335UpdBtn.disabled = false;
      if (sts046DelBtn) sts046DelBtn.disabled = false;
      if (sts046AddBtn) sts046AddBtn.disabled = false;
      if (mms240UpdBtn) mms240UpdBtn.disabled = false;
      if (cusextAddBtn) cusextAddBtn.disabled = false;
      renumberJobMode = null;
    } finally {
      if (!renumberJobId) {
        hideOverlay();
      }
    }
  });
}
// END WAGON RENNUMBERING
if (mos170AddPropBtn) {
  mos170AddPropBtn.addEventListener("click", async () => {
    mos170AddPropBtn.disabled = true;
    if (renumberExecuteBtn) renumberExecuteBtn.disabled = true;
    if (renumberInstallBtn) renumberInstallBtn.disabled = true;
    if (wagonRenumberBtn) wagonRenumberBtn.disabled = true;
    if (cms100MwnoBtn) cms100MwnoBtn.disabled = true;
    if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = true;
    if (mos180ApproveBtn) mos180ApproveBtn.disabled = true;
    if (mos050MontageBtn) mos050MontageBtn.disabled = true;
    if (crs335UpdBtn) crs335UpdBtn.disabled = true;
    if (sts046DelBtn) sts046DelBtn.disabled = true;
    if (sts046AddBtn) sts046AddBtn.disabled = true;
    if (mms240UpdBtn) mms240UpdBtn.disabled = true;
    if (cusextAddBtn) cusextAddBtn.disabled = true;
    renumberJobMode = "mos170";
    try {
      await ensureEnvMeta();
      setOverlayContext({
        source: envMeta?.urls?.mi,
        target: window.location.origin,
      });
      showOverlay();
      setStatus("MOS170MI AddProp laeuft ...");
      setIndeterminate(true);
      const runResp = await fetch(withEnv("/api/renumber/mos170"), { method: "POST" });
      if (!runResp.ok) {
        const text = await runResp.text();
        throw new Error(text || "MOS170MI AddProp fehlgeschlagen");
      }
      const runData = await runResp.json();
      renumberJobId = runData.job_id || null;
      renumberResultCount = 0;
      if (!renumberJobId) {
        throw new Error("Kein Job-ID erhalten.");
      }
      setIndeterminate(false);
      updateProgress(0, 1);
      if (renumberJobTimer) {
        clearInterval(renumberJobTimer);
      }
      renumberJobTimer = window.setInterval(pollRenumberJob, 700);
      await pollRenumberJob();
    } catch (error) {
      console.error(error);
      window.alert(error.message || "MOS170MI AddProp fehlgeschlagen");
      mos170AddPropBtn.disabled = false;
      if (renumberExecuteBtn) renumberExecuteBtn.disabled = false;
      if (renumberInstallBtn) renumberInstallBtn.disabled = false;
      if (wagonRenumberBtn) wagonRenumberBtn.disabled = false;
      if (cms100MwnoBtn) cms100MwnoBtn.disabled = false;
      if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = false;
      if (mos180ApproveBtn) mos180ApproveBtn.disabled = false;
      if (mos050MontageBtn) mos050MontageBtn.disabled = false;
      if (crs335UpdBtn) crs335UpdBtn.disabled = false;
      if (sts046DelBtn) sts046DelBtn.disabled = false;
      if (sts046AddBtn) sts046AddBtn.disabled = false;
      if (mms240UpdBtn) mms240UpdBtn.disabled = false;
      if (cusextAddBtn) cusextAddBtn.disabled = false;
      renumberJobMode = null;
    } finally {
      if (!renumberJobId) {
        hideOverlay();
      }
    }
  });
}
if (cms100MwnoBtn) {
  cms100MwnoBtn.addEventListener("click", async () => {
    cms100MwnoBtn.disabled = true;
    if (renumberExecuteBtn) renumberExecuteBtn.disabled = true;
    if (renumberInstallBtn) renumberInstallBtn.disabled = true;
    if (wagonRenumberBtn) wagonRenumberBtn.disabled = true;
    if (mos170AddPropBtn) mos170AddPropBtn.disabled = true;
    if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = true;
    if (mos180ApproveBtn) mos180ApproveBtn.disabled = true;
    if (mos050MontageBtn) mos050MontageBtn.disabled = true;
    if (crs335UpdBtn) crs335UpdBtn.disabled = true;
    if (sts046DelBtn) sts046DelBtn.disabled = true;
    if (sts046AddBtn) sts046AddBtn.disabled = true;
    if (mms240UpdBtn) mms240UpdBtn.disabled = true;
    if (cusextAddBtn) cusextAddBtn.disabled = true;
    renumberJobMode = "mos170_plpn";
    try {
      await ensureEnvMeta();
      setOverlayContext({
        source: envMeta?.urls?.mi,
        target: window.location.origin,
      });
      showOverlay();
      setStatus("MOS170 PLPN laeuft ...");
      setIndeterminate(true);
      const runResp = await fetch(withEnv("/api/renumber/mos170/plpn"), { method: "POST" });
      if (!runResp.ok) {
        const text = await runResp.text();
        throw new Error(text || "MOS170 PLPN fehlgeschlagen");
      }
      const runData = await runResp.json();
      renumberJobId = runData.job_id || null;
      renumberResultCount = 0;
      if (!renumberJobId) {
        throw new Error("Kein Job-ID erhalten.");
      }
      setIndeterminate(false);
      updateProgress(0, 1);
      if (renumberJobTimer) {
        clearInterval(renumberJobTimer);
      }
      renumberJobTimer = window.setInterval(pollRenumberJob, 700);
      await pollRenumberJob();
    } catch (error) {
      console.error(error);
      window.alert(error.message || "MOS170 PLPN fehlgeschlagen");
      cms100MwnoBtn.disabled = false;
      if (renumberExecuteBtn) renumberExecuteBtn.disabled = false;
      if (renumberInstallBtn) renumberInstallBtn.disabled = false;
      if (wagonRenumberBtn) wagonRenumberBtn.disabled = false;
      if (mos170AddPropBtn) mos170AddPropBtn.disabled = false;
      if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = false;
      if (mos180ApproveBtn) mos180ApproveBtn.disabled = false;
      if (mos050MontageBtn) mos050MontageBtn.disabled = false;
      if (crs335UpdBtn) crs335UpdBtn.disabled = false;
      if (sts046DelBtn) sts046DelBtn.disabled = false;
      if (sts046AddBtn) sts046AddBtn.disabled = false;
      if (mms240UpdBtn) mms240UpdBtn.disabled = false;
      if (cusextAddBtn) cusextAddBtn.disabled = false;
      renumberJobMode = null;
    } finally {
      if (!renumberJobId) {
        hideOverlay();
      }
    }
  });
}
if (mos100ChgSernBtn) {
  mos100ChgSernBtn.addEventListener("click", async () => {
    mos100ChgSernBtn.disabled = true;
    if (renumberExecuteBtn) renumberExecuteBtn.disabled = true;
    if (renumberInstallBtn) renumberInstallBtn.disabled = true;
    if (wagonRenumberBtn) wagonRenumberBtn.disabled = true;
    if (mos170AddPropBtn) mos170AddPropBtn.disabled = true;
    if (cms100MwnoBtn) cms100MwnoBtn.disabled = true;
    if (mos180ApproveBtn) mos180ApproveBtn.disabled = true;
    if (mos050MontageBtn) mos050MontageBtn.disabled = true;
    if (crs335UpdBtn) crs335UpdBtn.disabled = true;
    if (sts046DelBtn) sts046DelBtn.disabled = true;
    if (sts046AddBtn) sts046AddBtn.disabled = true;
    if (mms240UpdBtn) mms240UpdBtn.disabled = true;
    if (cusextAddBtn) cusextAddBtn.disabled = true;
    renumberJobMode = "mos100";
    try {
      await ensureEnvMeta();
      setOverlayContext({
        source: envMeta?.urls?.mi,
        target: window.location.origin,
      });
      showOverlay();
      setStatus("MOS100 Chg_SERN laeuft ...");
      setIndeterminate(true);
      const runResp = await fetch(withEnv("/api/renumber/mos100"), { method: "POST" });
      if (!runResp.ok) {
        const text = await runResp.text();
        throw new Error(text || "MOS100 Chg_SERN fehlgeschlagen");
      }
      const runData = await runResp.json();
      renumberJobId = runData.job_id || null;
      renumberResultCount = 0;
      if (!renumberJobId) {
        throw new Error("Kein Job-ID erhalten.");
      }
      setIndeterminate(false);
      updateProgress(0, 1);
      if (renumberJobTimer) {
        clearInterval(renumberJobTimer);
      }
      renumberJobTimer = window.setInterval(pollRenumberJob, 700);
      await pollRenumberJob();
    } catch (error) {
      console.error(error);
      window.alert(error.message || "MOS100 Chg_SERN fehlgeschlagen");
      mos100ChgSernBtn.disabled = false;
      if (renumberExecuteBtn) renumberExecuteBtn.disabled = false;
      if (renumberInstallBtn) renumberInstallBtn.disabled = false;
      if (wagonRenumberBtn) wagonRenumberBtn.disabled = false;
      if (mos170AddPropBtn) mos170AddPropBtn.disabled = false;
      if (cms100MwnoBtn) cms100MwnoBtn.disabled = false;
      if (mos180ApproveBtn) mos180ApproveBtn.disabled = false;
      if (mos050MontageBtn) mos050MontageBtn.disabled = false;
      if (crs335UpdBtn) crs335UpdBtn.disabled = false;
      if (sts046DelBtn) sts046DelBtn.disabled = false;
      if (sts046AddBtn) sts046AddBtn.disabled = false;
      if (mms240UpdBtn) mms240UpdBtn.disabled = false;
      if (cusextAddBtn) cusextAddBtn.disabled = false;
      renumberJobMode = null;
    } finally {
      if (!renumberJobId) {
        hideOverlay();
      }
    }
  });
}
if (mos180ApproveBtn) {
  mos180ApproveBtn.addEventListener("click", async () => {
    mos180ApproveBtn.disabled = true;
    if (renumberExecuteBtn) renumberExecuteBtn.disabled = true;
    if (renumberInstallBtn) renumberInstallBtn.disabled = true;
    if (wagonRenumberBtn) wagonRenumberBtn.disabled = true;
    if (mos170AddPropBtn) mos170AddPropBtn.disabled = true;
    if (cms100MwnoBtn) cms100MwnoBtn.disabled = true;
    if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = true;
    if (mos050MontageBtn) mos050MontageBtn.disabled = true;
    if (crs335UpdBtn) crs335UpdBtn.disabled = true;
    if (sts046DelBtn) sts046DelBtn.disabled = true;
    if (sts046AddBtn) sts046AddBtn.disabled = true;
    if (mms240UpdBtn) mms240UpdBtn.disabled = true;
    if (cusextAddBtn) cusextAddBtn.disabled = true;
    renumberJobMode = "mos180";
    try {
      await ensureEnvMeta();
      setOverlayContext({
        source: envMeta?.urls?.mi,
        target: window.location.origin,
      });
      showOverlay();
      setStatus("MOS180MI Approve laeuft ...");
      setIndeterminate(true);
      const runResp = await fetch(withEnv("/api/renumber/mos180"), { method: "POST" });
      if (!runResp.ok) {
        const text = await runResp.text();
        throw new Error(text || "MOS180MI Approve fehlgeschlagen");
      }
      const runData = await runResp.json();
      renumberJobId = runData.job_id || null;
      renumberResultCount = 0;
      if (!renumberJobId) {
        throw new Error("Kein Job-ID erhalten.");
      }
      setIndeterminate(false);
      updateProgress(0, 1);
      if (renumberJobTimer) {
        clearInterval(renumberJobTimer);
      }
      renumberJobTimer = window.setInterval(pollRenumberJob, 700);
      await pollRenumberJob();
    } catch (error) {
      console.error(error);
      window.alert(error.message || "MOS180MI Approve fehlgeschlagen");
      mos180ApproveBtn.disabled = false;
      if (renumberExecuteBtn) renumberExecuteBtn.disabled = false;
      if (renumberInstallBtn) renumberInstallBtn.disabled = false;
      if (wagonRenumberBtn) wagonRenumberBtn.disabled = false;
      if (mos170AddPropBtn) mos170AddPropBtn.disabled = false;
      if (cms100MwnoBtn) cms100MwnoBtn.disabled = false;
      if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = false;
      if (mos050MontageBtn) mos050MontageBtn.disabled = false;
      if (crs335UpdBtn) crs335UpdBtn.disabled = false;
      if (sts046DelBtn) sts046DelBtn.disabled = false;
      if (sts046AddBtn) sts046AddBtn.disabled = false;
      if (mms240UpdBtn) mms240UpdBtn.disabled = false;
      if (cusextAddBtn) cusextAddBtn.disabled = false;
      renumberJobMode = null;
    } finally {
      if (!renumberJobId) {
        hideOverlay();
      }
    }
  });
}
if (mos050MontageBtn) {
  mos050MontageBtn.addEventListener("click", async () => {
    mos050MontageBtn.disabled = true;
    if (renumberExecuteBtn) renumberExecuteBtn.disabled = true;
    if (renumberInstallBtn) renumberInstallBtn.disabled = true;
    if (wagonRenumberBtn) wagonRenumberBtn.disabled = true;
    if (mos170AddPropBtn) mos170AddPropBtn.disabled = true;
    if (cms100MwnoBtn) cms100MwnoBtn.disabled = true;
    if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = true;
    if (mos180ApproveBtn) mos180ApproveBtn.disabled = true;
    if (crs335UpdBtn) crs335UpdBtn.disabled = true;
    if (mms240UpdBtn) mms240UpdBtn.disabled = true;
    if (cusextAddBtn) cusextAddBtn.disabled = true;
    renumberJobMode = "mos050";
    try {
      await ensureEnvMeta();
      setOverlayContext({
        source: envMeta?.urls?.mi,
        target: window.location.origin,
      });
      showOverlay();
      setStatus("MOS050 Montage laeuft ...");
      setIndeterminate(true);
      const runResp = await fetch(withEnv("/api/renumber/mos050"), { method: "POST" });
      if (!runResp.ok) {
        const text = await runResp.text();
        throw new Error(text || "MOS050 Montage fehlgeschlagen");
      }
      const runData = await runResp.json();
      renumberJobId = runData.job_id || null;
      renumberResultCount = 0;
      if (!renumberJobId) {
        throw new Error("Kein Job-ID erhalten.");
      }
      setIndeterminate(false);
      updateProgress(0, 1);
      if (renumberJobTimer) {
        clearInterval(renumberJobTimer);
      }
      renumberJobTimer = window.setInterval(pollRenumberJob, 700);
      await pollRenumberJob();
    } catch (error) {
      console.error(error);
      window.alert(error.message || "MOS050 Montage fehlgeschlagen");
      mos050MontageBtn.disabled = false;
      if (renumberExecuteBtn) renumberExecuteBtn.disabled = false;
      if (renumberInstallBtn) renumberInstallBtn.disabled = false;
      if (wagonRenumberBtn) wagonRenumberBtn.disabled = false;
      if (mos170AddPropBtn) mos170AddPropBtn.disabled = false;
      if (cms100MwnoBtn) cms100MwnoBtn.disabled = false;
      if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = false;
      if (mos180ApproveBtn) mos180ApproveBtn.disabled = false;
      if (crs335UpdBtn) crs335UpdBtn.disabled = false;
      if (mms240UpdBtn) mms240UpdBtn.disabled = false;
      if (cusextAddBtn) cusextAddBtn.disabled = false;
      renumberJobMode = null;
    } finally {
      if (!renumberJobId) {
        hideOverlay();
      }
    }
  });
}
if (crs335UpdBtn) {
  crs335UpdBtn.addEventListener("click", async () => {
    crs335UpdBtn.disabled = true;
    if (renumberExecuteBtn) renumberExecuteBtn.disabled = true;
    if (renumberInstallBtn) renumberInstallBtn.disabled = true;
    if (wagonRenumberBtn) wagonRenumberBtn.disabled = true;
    if (mos170AddPropBtn) mos170AddPropBtn.disabled = true;
    if (cms100MwnoBtn) cms100MwnoBtn.disabled = true;
    if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = true;
    if (mos180ApproveBtn) mos180ApproveBtn.disabled = true;
    if (mos050MontageBtn) mos050MontageBtn.disabled = true;
    if (sts046DelBtn) sts046DelBtn.disabled = true;
    if (mms240UpdBtn) mms240UpdBtn.disabled = true;
    if (cusextAddBtn) cusextAddBtn.disabled = true;
    renumberJobMode = "crs335";
    try {
      await ensureEnvMeta();
      setOverlayContext({
        source: envMeta?.urls?.mi,
        target: window.location.origin,
      });
      showOverlay();
      setStatus("CRS335MI wird ausgefuehrt ...");
      setIndeterminate(true);
      const runResp = await fetch(withEnv("/api/renumber/crs335"), { method: "POST" });
      if (!runResp.ok) {
        const text = await runResp.text();
        throw new Error(text || "CRS335MI fehlgeschlagen");
      }
      const runData = await runResp.json();
      renumberJobId = runData.job_id || null;
      renumberResultCount = 0;
      if (!renumberJobId) {
        throw new Error("Kein Job-ID erhalten.");
      }
      setIndeterminate(false);
      updateProgress(0, 1);
      if (renumberJobTimer) {
        clearInterval(renumberJobTimer);
      }
      renumberJobTimer = window.setInterval(pollRenumberJob, 700);
      await pollRenumberJob();
    } catch (error) {
      console.error(error);
      window.alert(error.message || "CRS335MI fehlgeschlagen");
      crs335UpdBtn.disabled = false;
      if (renumberExecuteBtn) renumberExecuteBtn.disabled = false;
      if (renumberInstallBtn) renumberInstallBtn.disabled = false;
      if (wagonRenumberBtn) wagonRenumberBtn.disabled = false;
      if (mos170AddPropBtn) mos170AddPropBtn.disabled = false;
      if (cms100MwnoBtn) cms100MwnoBtn.disabled = false;
      if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = false;
      if (mos180ApproveBtn) mos180ApproveBtn.disabled = false;
      if (mos050MontageBtn) mos050MontageBtn.disabled = false;
      if (sts046DelBtn) sts046DelBtn.disabled = false;
      if (mms240UpdBtn) mms240UpdBtn.disabled = false;
      if (cusextAddBtn) cusextAddBtn.disabled = false;
      renumberJobMode = null;
    } finally {
      if (!renumberJobId) {
        hideOverlay();
      }
    }
  });
}
if (sts046DelBtn) {
  sts046DelBtn.addEventListener("click", async () => {
    sts046DelBtn.disabled = true;
    if (renumberExecuteBtn) renumberExecuteBtn.disabled = true;
    if (renumberInstallBtn) renumberInstallBtn.disabled = true;
    if (wagonRenumberBtn) wagonRenumberBtn.disabled = true;
    if (mos170AddPropBtn) mos170AddPropBtn.disabled = true;
    if (cms100MwnoBtn) cms100MwnoBtn.disabled = true;
    if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = true;
    if (mos180ApproveBtn) mos180ApproveBtn.disabled = true;
    if (mos050MontageBtn) mos050MontageBtn.disabled = true;
    if (crs335UpdBtn) crs335UpdBtn.disabled = true;
    if (sts046AddBtn) sts046AddBtn.disabled = true;
    if (mms240UpdBtn) mms240UpdBtn.disabled = true;
    if (cusextAddBtn) cusextAddBtn.disabled = true;
    renumberJobMode = "sts046";
    try {
      await ensureEnvMeta();
      setOverlayContext({
        source: envMeta?.urls?.mi,
        target: window.location.origin,
      });
      showOverlay();
      setStatus("STS046MI wird ausgefuehrt ...");
      setIndeterminate(true);
      const runResp = await fetch(withEnv("/api/renumber/sts046"), { method: "POST" });
      if (!runResp.ok) {
        const text = await runResp.text();
        throw new Error(text || "STS046MI fehlgeschlagen");
      }
      const runData = await runResp.json();
      renumberJobId = runData.job_id || null;
      renumberResultCount = 0;
      if (!renumberJobId) {
        throw new Error("Kein Job-ID erhalten.");
      }
      setIndeterminate(false);
      updateProgress(0, 1);
      if (renumberJobTimer) {
        clearInterval(renumberJobTimer);
      }
      renumberJobTimer = window.setInterval(pollRenumberJob, 700);
      await pollRenumberJob();
    } catch (error) {
      console.error(error);
      window.alert(error.message || "STS046MI fehlgeschlagen");
      sts046DelBtn.disabled = false;
      if (renumberExecuteBtn) renumberExecuteBtn.disabled = false;
      if (renumberInstallBtn) renumberInstallBtn.disabled = false;
      if (wagonRenumberBtn) wagonRenumberBtn.disabled = false;
      if (mos170AddPropBtn) mos170AddPropBtn.disabled = false;
      if (cms100MwnoBtn) cms100MwnoBtn.disabled = false;
      if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = false;
      if (mos180ApproveBtn) mos180ApproveBtn.disabled = false;
      if (mos050MontageBtn) mos050MontageBtn.disabled = false;
      if (crs335UpdBtn) crs335UpdBtn.disabled = false;
      if (sts046AddBtn) sts046AddBtn.disabled = false;
      if (mms240UpdBtn) mms240UpdBtn.disabled = false;
      if (cusextAddBtn) cusextAddBtn.disabled = false;
      renumberJobMode = null;
    } finally {
      if (!renumberJobId) {
        hideOverlay();
      }
    }
  });
}
if (sts046AddBtn) {
  sts046AddBtn.addEventListener("click", async () => {
    sts046AddBtn.disabled = true;
    if (renumberExecuteBtn) renumberExecuteBtn.disabled = true;
    if (renumberInstallBtn) renumberInstallBtn.disabled = true;
    if (wagonRenumberBtn) wagonRenumberBtn.disabled = true;
    if (mos170AddPropBtn) mos170AddPropBtn.disabled = true;
    if (cms100MwnoBtn) cms100MwnoBtn.disabled = true;
    if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = true;
    if (mos180ApproveBtn) mos180ApproveBtn.disabled = true;
    if (mos050MontageBtn) mos050MontageBtn.disabled = true;
    if (crs335UpdBtn) crs335UpdBtn.disabled = true;
    if (sts046DelBtn) sts046DelBtn.disabled = true;
    if (mms240UpdBtn) mms240UpdBtn.disabled = true;
    if (cusextAddBtn) cusextAddBtn.disabled = true;
    renumberJobMode = "sts046_add";
    try {
      await ensureEnvMeta();
      setOverlayContext({
        source: envMeta?.urls?.mi,
        target: window.location.origin,
      });
      showOverlay();
      setStatus("STS046MI (Add) wird ausgefuehrt ...");
      setIndeterminate(true);
      const runResp = await fetch(withEnv("/api/renumber/sts046/add"), { method: "POST" });
      if (!runResp.ok) {
        const text = await runResp.text();
        throw new Error(text || "STS046MI Add fehlgeschlagen");
      }
      const runData = await runResp.json();
      renumberJobId = runData.job_id || null;
      renumberResultCount = 0;
      if (!renumberJobId) {
        throw new Error("Kein Job-ID erhalten.");
      }
      setIndeterminate(false);
      updateProgress(0, 1);
      if (renumberJobTimer) {
        clearInterval(renumberJobTimer);
      }
      renumberJobTimer = window.setInterval(pollRenumberJob, 700);
      await pollRenumberJob();
    } catch (error) {
      console.error(error);
      window.alert(error.message || "STS046MI Add fehlgeschlagen");
      sts046AddBtn.disabled = false;
      if (renumberExecuteBtn) renumberExecuteBtn.disabled = false;
      if (renumberInstallBtn) renumberInstallBtn.disabled = false;
      if (wagonRenumberBtn) wagonRenumberBtn.disabled = false;
      if (mos170AddPropBtn) mos170AddPropBtn.disabled = false;
      if (cms100MwnoBtn) cms100MwnoBtn.disabled = false;
      if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = false;
      if (mos180ApproveBtn) mos180ApproveBtn.disabled = false;
      if (mos050MontageBtn) mos050MontageBtn.disabled = false;
      if (crs335UpdBtn) crs335UpdBtn.disabled = false;
      if (sts046DelBtn) sts046DelBtn.disabled = false;
      if (mms240UpdBtn) mms240UpdBtn.disabled = false;
      if (cusextAddBtn) cusextAddBtn.disabled = false;
      renumberJobMode = null;
    } finally {
      if (!renumberJobId) {
        hideOverlay();
      }
    }
  });
}
if (mms240UpdBtn) {
  mms240UpdBtn.addEventListener("click", async () => {
    mms240UpdBtn.disabled = true;
    if (renumberExecuteBtn) renumberExecuteBtn.disabled = true;
    if (renumberInstallBtn) renumberInstallBtn.disabled = true;
    if (wagonRenumberBtn) wagonRenumberBtn.disabled = true;
    if (mos170AddPropBtn) mos170AddPropBtn.disabled = true;
    if (cms100MwnoBtn) cms100MwnoBtn.disabled = true;
    if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = true;
    if (mos180ApproveBtn) mos180ApproveBtn.disabled = true;
    if (mos050MontageBtn) mos050MontageBtn.disabled = true;
    if (crs335UpdBtn) crs335UpdBtn.disabled = true;
    if (sts046DelBtn) sts046DelBtn.disabled = true;
    if (sts046AddBtn) sts046AddBtn.disabled = true;
    if (cusextAddBtn) cusextAddBtn.disabled = true;
    renumberJobMode = "mms240";
    try {
      await ensureEnvMeta();
      setOverlayContext({
        source: envMeta?.urls?.mi,
        target: window.location.origin,
      });
      showOverlay();
      setStatus("MMS240MI wird ausgefuehrt ...");
      setIndeterminate(true);
      const runResp = await fetch(withEnv("/api/renumber/mms240"), { method: "POST" });
      if (!runResp.ok) {
        const text = await runResp.text();
        throw new Error(text || "MMS240MI fehlgeschlagen");
      }
      const runData = await runResp.json();
      renumberJobId = runData.job_id || null;
      renumberResultCount = 0;
      if (!renumberJobId) {
        throw new Error("Kein Job-ID erhalten.");
      }
      setIndeterminate(false);
      updateProgress(0, 1);
      if (renumberJobTimer) {
        clearInterval(renumberJobTimer);
      }
      renumberJobTimer = window.setInterval(pollRenumberJob, 700);
      await pollRenumberJob();
    } catch (error) {
      console.error(error);
      window.alert(error.message || "MMS240MI fehlgeschlagen");
      mms240UpdBtn.disabled = false;
      if (renumberExecuteBtn) renumberExecuteBtn.disabled = false;
      if (renumberInstallBtn) renumberInstallBtn.disabled = false;
      if (wagonRenumberBtn) wagonRenumberBtn.disabled = false;
      if (mos170AddPropBtn) mos170AddPropBtn.disabled = false;
      if (cms100MwnoBtn) cms100MwnoBtn.disabled = false;
      if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = false;
      if (mos180ApproveBtn) mos180ApproveBtn.disabled = false;
      if (mos050MontageBtn) mos050MontageBtn.disabled = false;
      if (crs335UpdBtn) crs335UpdBtn.disabled = false;
      if (sts046DelBtn) sts046DelBtn.disabled = false;
      if (sts046AddBtn) sts046AddBtn.disabled = false;
      if (cusextAddBtn) cusextAddBtn.disabled = false;
      renumberJobMode = null;
    } finally {
      if (!renumberJobId) {
        hideOverlay();
      }
    }
  });
}
if (cusextAddBtn) {
  cusextAddBtn.addEventListener("click", async () => {
    cusextAddBtn.disabled = true;
    if (renumberExecuteBtn) renumberExecuteBtn.disabled = true;
    if (renumberInstallBtn) renumberInstallBtn.disabled = true;
    if (wagonRenumberBtn) wagonRenumberBtn.disabled = true;
    if (mos170AddPropBtn) mos170AddPropBtn.disabled = true;
    if (cms100MwnoBtn) cms100MwnoBtn.disabled = true;
    if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = true;
    if (mos180ApproveBtn) mos180ApproveBtn.disabled = true;
    if (mos050MontageBtn) mos050MontageBtn.disabled = true;
    if (crs335UpdBtn) crs335UpdBtn.disabled = true;
    if (sts046DelBtn) sts046DelBtn.disabled = true;
    if (sts046AddBtn) sts046AddBtn.disabled = true;
    if (mms240UpdBtn) mms240UpdBtn.disabled = true;
    renumberJobMode = "cusext";
    try {
      await ensureEnvMeta();
      setOverlayContext({
        source: envMeta?.urls?.mi,
        target: window.location.origin,
      });
      showOverlay();
      setStatus("CUSEXTMI wird ausgefuehrt ...");
      setIndeterminate(true);
      const runResp = await fetch(withEnv("/api/renumber/cusext"), { method: "POST" });
      if (!runResp.ok) {
        const text = await runResp.text();
        throw new Error(text || "CUSEXTMI fehlgeschlagen");
      }
      const runData = await runResp.json();
      renumberJobId = runData.job_id || null;
      renumberResultCount = 0;
      if (!renumberJobId) {
        throw new Error("Kein Job-ID erhalten.");
      }
      setIndeterminate(false);
      updateProgress(0, 1);
      if (renumberJobTimer) {
        clearInterval(renumberJobTimer);
      }
      renumberJobTimer = window.setInterval(pollRenumberJob, 700);
      await pollRenumberJob();
    } catch (error) {
      console.error(error);
      window.alert(error.message || "CUSEXTMI fehlgeschlagen");
      cusextAddBtn.disabled = false;
      if (renumberExecuteBtn) renumberExecuteBtn.disabled = false;
      if (renumberInstallBtn) renumberInstallBtn.disabled = false;
      if (wagonRenumberBtn) wagonRenumberBtn.disabled = false;
      if (mos170AddPropBtn) mos170AddPropBtn.disabled = false;
      if (cms100MwnoBtn) cms100MwnoBtn.disabled = false;
      if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = false;
      if (mos180ApproveBtn) mos180ApproveBtn.disabled = false;
      if (mos050MontageBtn) mos050MontageBtn.disabled = false;
      if (crs335UpdBtn) crs335UpdBtn.disabled = false;
      if (sts046DelBtn) sts046DelBtn.disabled = false;
      if (sts046AddBtn) sts046AddBtn.disabled = false;
      if (mms240UpdBtn) mms240UpdBtn.disabled = false;
      renumberJobMode = null;
    } finally {
      if (!renumberJobId) {
        hideOverlay();
      }
    }
  });
}
if (fulltextInput) {
  fulltextInput.addEventListener("input", () => {
    applyFilters();
    updateObjStrkSearch();
  });
}
if (renumberSernInput) {
  renumberSernInput.addEventListener("input", () => {
    if (renumberCheckTimer) {
      window.clearTimeout(renumberCheckTimer);
    }
    renumberCheckTimer = window.setTimeout(checkRenumberSerial, 400);
  });
  renumberSernInput.addEventListener("blur", checkRenumberSerial);
}
const renumberSteps = document.getElementById("renumberSteps");

const startSingleRenumberStep = async (mode) => {
  if (currentModule !== "wagenumbau" || !mode) return;
  if (renumberJobId) {
    window.alert("Ein anderer Prozess läuft bereits.");
    return;
  }

  disableAllRenumberButtons(); // Disable all buttons at the start of the process

  const payload = getRenumberPayload();
  if (
    mode !== "rollback" &&
    (!payload.new_baureihe || !payload.new_sern || !payload.umbau_datum || !payload.umbau_art)
  ) {
    window.alert("Bitte alle Umbaufelder ausfüllen.");
    enableAllRenumberButtons(); // Re-enable buttons if validation fails
    return;
  }

  try {
    await ensureEnvMeta();
    const needsUpdate = ["out", "wagon_renumber", "mos170"].includes(mode);
    if (needsUpdate) {
      // Only reset/update state for steps that start the chain.
      const updateResp = await fetch(withEnv("/api/renumber/update"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!updateResp.ok) {
        const text = await updateResp.text();
        throw new Error(text || "Vorbereitung für den Einzelschritt fehlgeschlagen.");
      }
    }

    const step = getSingleRenumberStep(mode);
    if (!step) {
      throw new Error(`Schritt '${mode}' nicht gefunden.`);
    }
    if (mode === "rollback") {
      objStrkRows.forEach((row) => {
        row.ROLLBACK = "";
      });
    }

    setOverlayContext({
      source: envMeta?.urls?.mi,
      target: window.location.origin,
    });
    showOverlay();
    singleStepLoopActive = true;
    let pending = 0;
    do {
      lastRenumberJobStatus = null;
      lastRenumberJobError = null;
      renumberJobMode = step.mode;
      renumberResultCount = 0;
      resetProgress();
      setIndeterminate(true);
      setStatus(`Starte Einzelschritt: ${step.label}`);

      const runResp = await fetch(withEnv(step.endpoint), { method: "POST" });
      if (!runResp.ok) {
        const text = await runResp.text();
        throw new Error(text || `${step.label} fehlgeschlagen`);
      }
      const runData = await runResp.json();
      renumberJobId = runData.job_id || null;
      if (!renumberJobId) {
        throw new Error("Keine Job-ID für den Einzelschritt erhalten.");
      }
      if (renumberJobTimer) {
        clearInterval(renumberJobTimer);
      }
      renumberJobTimer = window.setInterval(pollRenumberJob, 700);
      await pollRenumberJob();

      const completion = await waitForRenumberJobIdle();
      if (completion.status === "error") {
        throw new Error(completion.error || `${step.label} fehlgeschlagen`);
      }
      pending = await fetchRenumberPending(step.mode);
      if (pending > 0) {
        setStatus(`Warte ${SINGLE_STEP_RETRY_MS / 1000} Sekunden ... (${pending} offen)`);
        setIndeterminate(true);
        await sleep(SINGLE_STEP_RETRY_MS);
      }
    } while (pending > 0);

    hideOverlay(true);
    enableAllRenumberButtons();
  } catch (error) {
    console.error(error);
    window.alert(error.message || "Einzelschritt fehlgeschlagen");
    hideOverlay(true); // Job finished with error
    enableAllRenumberButtons(); // Re-enable buttons on error
  } finally {
    singleStepLoopActive = false;
    renumberJobId = null;
    renumberJobMode = null;
    lastRenumberJobStatus = null;
    lastRenumberJobError = null;
    if (renumberJobTimer) {
      clearInterval(renumberJobTimer);
      renumberJobTimer = null;
    }
  }
};

if (renumberSteps) {
  renumberSteps.addEventListener("click", (event) => {
    const button = event.target.closest("button");
    if (button && button.dataset.mode) {
      startSingleRenumberStep(button.dataset.mode);
    }
  });
}

if (renumberExecuteBtn) {
  renumberExecuteBtn.addEventListener("click", async () => {
    if (currentModule !== "wagenumbau") return;
    if (renumberSequence) return;
    await checkRenumberSerial();
    const hasError = renumberSernInput?.classList.contains("objstrk-input--error");
    if (hasError) {
      window.alert("Neue Seriennummer existiert bereits.");
      return;
    }
    const payload = getRenumberPayload();
    if (!payload.new_baureihe || !payload.new_sern || !payload.umbau_datum || !payload.umbau_art) {
      window.alert("Bitte alle Umbaufelder ausfüllen.");
      return;
    }
    disableAllRenumberButtons(); // Disable all buttons at the start of the process
    try {
      const resp = await fetch(withEnv("/api/renumber/update"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text || "Umbau konnte nicht gespeichert werden");
      }
      objStrkRows.forEach((row) => {
        row.NEW_BAUREIHE = payload.new_baureihe;
        row.NEW_SERN = payload.new_sern;
        row.UMBAU_DATUM = payload.umbau_datum;
        row.UMBAU_ART = payload.umbau_art;
        row.OUT = "";
        row.IN = "";
        row.ROLLBACK = "";
      });
      setOverlayContext({
        source: envMeta?.urls?.mi,
        target: window.location.origin,
      });
      showOverlay({ showRandomImage: false });
      setStatus("Starte Wagenumbau-Ablauf ...");
      setIndeterminate(true);
      await startRenumberSequence();
    } catch (error) {
      console.error(error);
      window.alert(error.message || "Umbau speichern fehlgeschlagen");
      enableAllRenumberButtons(); // Re-enable buttons on error
      renumberJobMode = null;
      stopRenumberSequence();
    } finally {
      if (!renumberJobId) {
        hideOverlay(true); // Call hideOverlay with true when job finishes
      }
    }
  });
}
if (renumberInstallBtn) {
  renumberInstallBtn.addEventListener("click", async () => {
    if (currentModule !== "wagenumbau") return;
    renumberInstallBtn.disabled = true;
    if (renumberExecuteBtn) renumberExecuteBtn.disabled = true;
    if (wagonRenumberBtn) wagonRenumberBtn.disabled = true;
    if (mos170AddPropBtn) mos170AddPropBtn.disabled = true;
    if (cms100MwnoBtn) cms100MwnoBtn.disabled = true;
    if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = true;
    if (mos180ApproveBtn) mos180ApproveBtn.disabled = true;
    if (mos050MontageBtn) mos050MontageBtn.disabled = true;
    if (crs335UpdBtn) crs335UpdBtn.disabled = true;
    if (sts046DelBtn) sts046DelBtn.disabled = true;
    if (sts046AddBtn) sts046AddBtn.disabled = true;
    if (mms240UpdBtn) mms240UpdBtn.disabled = true;
    if (cusextAddBtn) cusextAddBtn.disabled = true;
    renumberJobMode = "in";
    try {
      objStrkRows.forEach((row) => {
        row.IN = "";
      });
      setOverlayContext({
        source: envMeta?.urls?.mi,
        target: window.location.origin,
      });
      showOverlay();
      setStatus("Führe Einbauten über MOS125MI aus ...");
      setIndeterminate(true);
      const runResp = await fetch(withEnv("/api/renumber/install"), { method: "POST" });
      if (!runResp.ok) {
        const text = await runResp.text();
        throw new Error(text || "Einbau fehlgeschlagen");
      }
      const runData = await runResp.json();
      renumberJobId = runData.job_id || null;
      renumberResultCount = 0;
      if (!renumberJobId) {
        throw new Error("Kein Job-ID erhalten.");
      }
      setIndeterminate(false);
      updateProgress(0, 1);
      if (renumberJobTimer) {
        clearInterval(renumberJobTimer);
      }
      renumberJobTimer = window.setInterval(pollRenumberJob, 700);
      await pollRenumberJob();
    } catch (error) {
      console.error(error);
      window.alert(error.message || "Einbau speichern fehlgeschlagen");
      renumberInstallBtn.disabled = false;
      if (renumberExecuteBtn) renumberExecuteBtn.disabled = false;
      if (wagonRenumberBtn) wagonRenumberBtn.disabled = false;
      if (mos170AddPropBtn) mos170AddPropBtn.disabled = false;
      if (cms100MwnoBtn) cms100MwnoBtn.disabled = false;
      if (mos100ChgSernBtn) mos100ChgSernBtn.disabled = false;
      if (mos180ApproveBtn) mos180ApproveBtn.disabled = false;
      if (mos050MontageBtn) mos050MontageBtn.disabled = false;
      if (crs335UpdBtn) crs335UpdBtn.disabled = false;
      if (sts046DelBtn) sts046DelBtn.disabled = false;
      if (sts046AddBtn) sts046AddBtn.disabled = false;
      if (mms240UpdBtn) mms240UpdBtn.disabled = false;
      if (cusextAddBtn) cusextAddBtn.disabled = false;
      renumberJobMode = null;
    } finally {
      if (!renumberJobId) {
        hideOverlay();
      }
    }
  });
}
getAllFilterFields().forEach(({ input }) => {
  if (input) {
    input.addEventListener("input", applyFilters);
  }
});
if (clearFiltersBtn) {
  clearFiltersBtn.addEventListener("click", () => {
    getAllFilterFields().forEach(({ input }) => {
      if (input) input.value = "";
    });
    if (fulltextInput) fulltextInput.value = "";
    applyFilters();
    updateObjStrkSearch();
  });
}
if (excelUploadInput) {
  excelUploadInput.addEventListener("change", () => {
    updateExcelUploadInfo();
    loadTeilenummerExcelMappings();
  });
}
if (backToWagonsBtn) {
  backToWagonsBtn.addEventListener("click", () => {
    showWagonsView();
  });
}
if (collapseAllBtn) {
  collapseAllBtn.addEventListener("click", () => {
    collapseAllObjNodes();
    renderObjStrkTable();
  });
}
if (expandAllBtn) {
  expandAllBtn.addEventListener("click", () => {
    expandAllObjNodes();
    renderObjStrkTable();
  });
}
if (mainMenuBtn && mainMenu) {
  mainMenuBtn.addEventListener("click", () => {
    mainMenu.classList.toggle("hidden");
  });
  document.addEventListener("click", (event) => {
    if (!(event.target instanceof Element)) return;
    if (!mainMenuBtn.contains(event.target) && !mainMenu.contains(event.target)) {
      mainMenu.classList.add("hidden");
    }
  });
  menuItems.forEach((item) => {
    item.addEventListener("click", () => {
      mainMenu.classList.add("hidden");
      const targetPage = item.dataset.page;
      if (targetPage) {
        window.location.href = targetPage;
        return;
      }
      setModule(item.dataset.module || "sparepart");
    });
  });
}
if (envToggle) {
  envToggle.addEventListener("click", () => {
    setEnvironment(currentEnv === "LIVE" ? "TEST" : "LIVE");
  });
}
if (rsrdEnvToggle) {
  rsrdEnvToggle.addEventListener("click", () => {
    setRsrdEnvironment(currentRsrdEnv === "LIVE" ? "TEST" : "LIVE");
  });
}
const openSwapDb = () => {
  if (currentModule === "teilenummer") {
    prepareTeilenummerTausch();
    return;
  }
  loadSwapData();
};
if (openSwapFromWagonsBtn) {
  openSwapFromWagonsBtn.addEventListener("click", openSwapDb);
}
if (teilenummerGoBtn) {
  teilenummerGoBtn.addEventListener("click", runTeilenummerGo);
}
if (openSwapFromObjBtn) {
  openSwapFromObjBtn.addEventListener("click", openSwapDb);
}
if (tableBody) {
  tableBody.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const button = target ? target.closest(".objstrk-btn") : null;
    if (!button) return;
    const mtrl = button.dataset.mtrl || "";
    const sern = button.dataset.sern || "";
    if (!mtrl || !sern) return;
    loadObjStrk(mtrl, sern);
  });
}
if (swapBackBtn) {
  swapBackBtn.addEventListener("click", () => {
    showWagonsView();
  });
}
if (swapExecuteBtn) {
  swapExecuteBtn.addEventListener("click", () => {
    window.alert("Tauschdurchführung befindet sich noch in Arbeit.");
  });
}
if (swapSelectAllBtn) {
  swapSelectAllBtn.addEventListener("click", markAllSwapRecords);
}
if (swapClearSelectionBtn) {
  swapClearSelectionBtn.addEventListener("click", clearAllSwapSelections);
}
if (rsrd2LoadErpBtn) {
  rsrd2LoadErpBtn.addEventListener("click", () => {
    loadErpWagons();
  });
}

if (rsrd2LoadErpFullBtn) {
  rsrd2LoadErpFullBtn.addEventListener("click", () => {
    loadErpAttributes();
  });
}
if (rsrd2FetchJsonBtn) {
  rsrd2FetchJsonBtn.addEventListener("click", () => {
    fetchRsrdJson();
  });
}
if (rsrd2ProcessJsonBtn) {
  rsrd2ProcessJsonBtn.addEventListener("click", () => {
    processRsrdJson();
  });
}
if (rsrd2CompareBtn) {
  rsrd2CompareBtn.addEventListener("click", () => {
    compareRsrdData();
  });
}
if (rsrd2UploadSelectedBtn) {
  rsrd2UploadSelectedBtn.addEventListener("click", () => {
    uploadSelectedRsrd2();
  });
}
if (rsrd2PrevPage) {
  rsrd2PrevPage.addEventListener("click", () => {
    rsrd2OverviewPage = Math.max(1, rsrd2OverviewPage - 1);
    rsrd2OverviewLoaded = false;
    loadRsrd2Overview(true);
  });
}
if (rsrd2NextPage) {
  rsrd2NextPage.addEventListener("click", () => {
    rsrd2OverviewPage += 1;
    rsrd2OverviewLoaded = false;
    loadRsrd2Overview(true);
  });
}
if (rsrd2PageSizeSelect) {
  rsrd2PageSizeSelect.addEventListener("change", () => {
    rsrd2OverviewPageSize = Number(rsrd2PageSizeSelect.value) || 25;
    rsrd2OverviewPage = 1;
    rsrd2OverviewLoaded = false;
    loadRsrd2Overview(true);
  });
}
const handleRsrd2Filter = () => {
  if (rsrd2FilterTimer) window.clearTimeout(rsrd2FilterTimer);
  rsrd2FilterTimer = window.setTimeout(() => {
    rsrd2OverviewPage = 1;
    rsrd2OverviewLoaded = false;
    loadRsrd2Overview(true);
  }, 250);
};

const updateRsrd2Suggestions = async ({ field, value, target }) => {
  if (!target) return;
  const query = (value || "").trim();
  if (!query) {
    target.innerHTML = "";
    return;
  }
  try {
    const resp = await fetch(withErpEnv(`/api/rsrd2/suggestions?field=${encodeURIComponent(field)}&q=${encodeURIComponent(query)}`));
    if (!resp.ok) return;
    const data = await resp.json().catch(() => ({}));
    const values = Array.isArray(data.values) ? data.values : [];
    target.innerHTML = values.map((item) => `<option value="${String(item).replace(/"/g, "&quot;")}"></option>`).join("");
  } catch (error) {
    target.innerHTML = "";
  }
};
if (rsrd2FilterSern) rsrd2FilterSern.addEventListener("input", handleRsrd2Filter);
if (rsrd2FilterBaureihe) rsrd2FilterBaureihe.addEventListener("input", handleRsrd2Filter);
if (rsrd2FilterHalter) rsrd2FilterHalter.addEventListener("input", handleRsrd2Filter);
if (rsrd2FilterUic) rsrd2FilterUic.addEventListener("input", handleRsrd2Filter);
if (rsrd2FilterStatus) rsrd2FilterStatus.addEventListener("change", handleRsrd2Filter);
if (rsrd2FilterSern) {
  rsrd2FilterSern.addEventListener("input", (event) =>
    updateRsrd2Suggestions({ field: "sern", value: event.target.value, target: rsrd2FilterSernOptions }),
  );
}
if (rsrd2FilterBaureihe) {
  rsrd2FilterBaureihe.addEventListener("input", (event) =>
    updateRsrd2Suggestions({ field: "baureihe", value: event.target.value, target: rsrd2FilterBaureiheOptions }),
  );
}
if (rsrd2FilterHalter) {
  rsrd2FilterHalter.addEventListener("input", (event) =>
    updateRsrd2Suggestions({ field: "halter", value: event.target.value, target: rsrd2FilterHalterOptions }),
  );
}
if (rsrd2FilterUic) {
  rsrd2FilterUic.addEventListener("input", (event) =>
    updateRsrd2Suggestions({ field: "wagen_typ", value: event.target.value, target: rsrd2FilterUicOptions }),
  );
}
if (rsrd2GoBtn) {
  rsrd2GoBtn.addEventListener("click", () => {
    uploadSelectedRsrd2();
  });
  updateGoButtonState();
}
initBulkSelect(rsrd2BulkData);
initBulkSelect(rsrd2BulkKm);
initBulkSelect(rsrd2BulkDocs);
if (rsrd2BulkDataBtn) {
  rsrd2BulkDataBtn.addEventListener("click", () => bulkSetSyncEnv({ kind: "data", select: rsrd2BulkData }));
}
if (rsrd2BulkKmBtn) {
  rsrd2BulkKmBtn.addEventListener("click", () => bulkSetSyncEnv({ kind: "km", select: rsrd2BulkKm }));
}
if (rsrd2BulkDocsBtn) {
  rsrd2BulkDocsBtn.addEventListener("click", () => bulkSetSyncEnv({ kind: "docs", select: rsrd2BulkDocs }));
}
if (rsrd2BulkTransferBtn) {
  rsrd2BulkTransferBtn.addEventListener("click", () => bulkSetTransfer({ select: rsrd2BulkTransfer }));
}
if (rsrd2ClearFiltersBtn) {
  rsrd2ClearFiltersBtn.addEventListener("click", () => {
    if (rsrd2FilterSern) rsrd2FilterSern.value = "";
    if (rsrd2FilterBaureihe) rsrd2FilterBaureihe.value = "";
    if (rsrd2FilterHalter) rsrd2FilterHalter.value = "";
    if (rsrd2FilterUic) rsrd2FilterUic.value = "";
    if (rsrd2FilterStatus) rsrd2FilterStatus.value = "";
    if (rsrd2FilterSernOptions) rsrd2FilterSernOptions.innerHTML = "";
    if (rsrd2FilterBaureiheOptions) rsrd2FilterBaureiheOptions.innerHTML = "";
    if (rsrd2FilterHalterOptions) rsrd2FilterHalterOptions.innerHTML = "";
    if (rsrd2FilterUicOptions) rsrd2FilterUicOptions.innerHTML = "";
    rsrd2OverviewPage = 1;
    rsrd2OverviewLoaded = false;
    loadRsrd2Overview(true);
  });
}
if (swapPrevPageBtn) {
  swapPrevPageBtn.addEventListener("click", () => {
    if (swapPage > 1) {
      swapPage -= 1;
      renderSwapTable();
    }
  });
}
if (swapNextPageBtn) {
  swapNextPageBtn.addEventListener("click", () => {
    const maxPage = Math.max(1, Math.ceil(swapRows.length / SWAP_PAGE_SIZE));
    if (swapPage < maxPage) {
      swapPage += 1;
      renderSwapTable();
    }
  });
}
if (swapTableBody) {
  swapTableBody.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    if (!target) return;
    const checkBtn = target.closest(".swap-check-btn");
    if (checkBtn) {
      toggleSwapSelection(checkBtn.dataset.key || "");
      return;
    }
    const deleteBtn = target.closest(".swap-delete-btn");
    if (deleteBtn) {
      deleteSwapRecord(deleteBtn.dataset.key || "");
    }
  });
}
if (objstrkBody) {
  objstrkBody.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const toggle = target ? target.closest(".objstrk-toggle") : null;
    if (toggle) {
      const nodeId = toggle.dataset.nodeId;
      if (!nodeId) return;
      if (objStrkExpanded.has(nodeId)) {
        objStrkExpanded.delete(nodeId);
      } else {
        objStrkExpanded.add(nodeId);
      }
      renderObjStrkTable();
      return;
    }
    const partsBtn = target ? target.closest(".objstrk-btn") : null;
    if (partsBtn && partsBtn.dataset.eqtp !== undefined && !partsBtn.dataset.mtrl) {
      const action = partsBtn.dataset.action || "select";
      const nodeId = partsBtn.dataset.nodeId || null;
      if (action === "remove" && nodeId) {
        removeReplacementSelection(nodeId, partsBtn.dataset.originalItno || "", partsBtn.dataset.originalSern || "");
        return;
      }
      currentPartsNodeId = nodeId;
      currentPartsNode = currentPartsNodeId ? objStrkNodesById.get(currentPartsNodeId) : null;
      currentOriginalItno = partsBtn.dataset.originalItno || "";
      currentOriginalSern = partsBtn.dataset.originalSern || "";
      openPartsModal(partsBtn.dataset.eqtp || "", partsBtn.dataset.context || "");
    }
  });
}
if (closePartsModalBtn) {
  closePartsModalBtn.addEventListener("click", closePartsModal);
}
if (partsModal) {
  partsModal.addEventListener("click", (event) => {
    if (event.target === partsModal) {
      closePartsModal();
    }
  });
  window.addEventListener("resize", () => {
    if (!partsModal.classList.contains("hidden")) {
      clampPartsModalPosition();
    }
  });
}
if (partsSearchForm) {
  partsSearchForm.addEventListener("submit", (event) => {
    event.preventDefault();
    loadSpareParts();
  });
}
[
  partsFilterType,
  partsFilterItem,
  partsFilterSerial,
  partsFilterFacility,
  partsFilterBin,
].forEach((input) => {
  if (input) {
    input.addEventListener("input", schedulePartsReload);
  }
});
if (partsClearFiltersBtn) {
  partsClearFiltersBtn.addEventListener("click", () => clearPartsFilters(true));
}
if (partsModalHeader) {
  partsModalHeader.addEventListener("mousedown", startPartsDrag);
}
if (partsResizeHandle) {
  partsResizeHandle.addEventListener("mousedown", startPartsResize);
}
if (partsPrevPageBtn) {
  partsPrevPageBtn.addEventListener("click", () => {
    if (sparePartsPage > 1) {
      sparePartsPage -= 1;
      renderPartsTable();
    }
  });
}
if (partsNextPageBtn) {
  partsNextPageBtn.addEventListener("click", () => {
    const totalPages = Math.max(1, Math.ceil(sparePartsRows.length / PARTS_PAGE_SIZE));
    if (sparePartsPage < totalPages) {
      sparePartsPage += 1;
      renderPartsTable();
    }
  });
}
if (partsResultsBody) {
  partsResultsBody.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const selectBtn = target ? target.closest(".parts-select-btn") : null;
    if (selectBtn) {
      const rowIndex = Number(selectBtn.dataset.rowIndex);
      submitReplacementSelection(rowIndex);
    }
  });
}
if (loaderCloseBtn) {
  loaderCloseBtn.addEventListener("click", () => {
    setOverlayPinned(false);
    loaderOverlay.classList.add("hidden");
    setOverlaySpinner(false);
    setOverlayMessage("");
    setOverlayActionVisible(false);
    setOverlayDownloadLink("", "");
    setRandomOverlayImage(false);
    clearOverlayContext();
  });
}
if (loaderActionBtn) {
  loaderActionBtn.addEventListener("click", () => {
    setOverlayPinned(false);
    loaderOverlay.classList.add("hidden");
    setOverlaySpinner(false);
    setOverlayMessage("");
    setOverlayActionVisible(false);
    setOverlayDownloadLink("", "");
    setRandomOverlayImage(false);
    clearOverlayContext();
  });
}
if (exportSelectionBtn) {
  exportSelectionBtn.addEventListener("click", () => {
    exportTeilenummerSelection();
  });
}
if (apiLogBtn) {
  apiLogBtn.addEventListener("click", () => {
    window.open(withEnv("/api/teilenummer/api_log/view"), "_blank", "noopener");
  });
}
if (closeApiLogBtn) {
  closeApiLogBtn.addEventListener("click", () => {
    hideApiLogModal();
  });
}
if (apiLogModal) {
  apiLogModal.addEventListener("click", (event) => {
    if (event.target === apiLogModal) {
      hideApiLogModal();
    }
  });
}
if (clearApiLogBtn) {
  clearApiLogBtn.addEventListener("click", async () => {
    try {
      await fetchJSON(withEnv("/api/teilenummer/api_log/clear"), { method: "POST" });
      await loadApiLog();
    } catch (error) {
      if (apiLogContent) {
        apiLogContent.textContent = error.message || "API-Log konnte nicht gelöscht werden.";
      }
    }
  });
}
if (exportApiLogCsvBtn) {
  exportApiLogCsvBtn.addEventListener("click", async () => {
    try {
      const response = await fetch(withEnv("/api/teilenummer/api_log.csv"));
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || "CSV-Export fehlgeschlagen.");
      }
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `API_${buildExportTimestamp()}.csv`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    } catch (error) {
      if (apiLogContent) {
        apiLogContent.textContent = error.message || "CSV-Export fehlgeschlagen.";
      }
    }
  });
}
updateExcelUploadInfo();
setModule(currentModule, true);
updateEnvSwitch();
