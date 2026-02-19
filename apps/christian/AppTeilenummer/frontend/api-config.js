(function initMFDAppsApiConfig() {
  const legacy = window.__SPAREPART_API_CONFIG__ || {};
  const current = window.__MFDAPPS_API_CONFIG__ || {};
  const merged = {
    CORE_API_BASE_URL: current.CORE_API_BASE_URL || legacy.CORE_API_BASE_URL || "",
    RSRD2_API_BASE_URL: current.RSRD2_API_BASE_URL || legacy.RSRD2_API_BASE_URL || "",
    GOLDENVIEW_API_BASE_URL: current.GOLDENVIEW_API_BASE_URL || legacy.GOLDENVIEW_API_BASE_URL || "",
  };
  window.__MFDAPPS_API_CONFIG__ = merged;
  // Legacy compatibility for existing frontend scripts.
  window.__SPAREPART_API_CONFIG__ = merged;
})();
