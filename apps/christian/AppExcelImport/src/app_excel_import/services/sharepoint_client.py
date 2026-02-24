from __future__ import annotations

import base64
from dataclasses import dataclass
import os
import time
from urllib.parse import quote, unquote, urlsplit

import requests

from app_excel_import.config import ExcelImportSettings


GRAPH_SCOPE = "https://graph.microsoft.com/.default"
GRAPH_RESOURCE = "https://graph.microsoft.com"


@dataclass(frozen=True)
class SharePointWorkbookDownload:
    source_name: str
    content: bytes


class SharePointGraphClient:
    def __init__(self, settings: ExcelImportSettings) -> None:
        self.settings = settings
        self._access_token: str | None = None
        self._access_token_expires_at_utc: float = 0.0

    def is_configured(self) -> bool:
        site_configured = bool(self.settings.sharepoint_site_hostname and self.settings.sharepoint_site_path)
        auth_configured = self.settings.sharepoint_use_managed_identity or self._has_client_credentials()
        return site_configured and auth_configured

    def download_workbook(
        self,
        workbook_path: str,
        site_hostname: str | None = None,
        site_path: str | None = None,
    ) -> SharePointWorkbookDownload:
        raw_workbook = workbook_path.strip()
        if not raw_workbook:
            raise ValueError("SharePoint workbook_path must not be empty.")

        access_token = self._get_access_token()
        if raw_workbook.lower().startswith(("http://", "https://")):
            parsed_direct = self._parse_direct_file_url(raw_workbook)
            if parsed_direct is not None:
                parsed_hostname, parsed_site_path, parsed_workbook_path = parsed_direct
                site_id = self._resolve_site_id(
                    access_token=access_token,
                    hostname=parsed_hostname,
                    site_path=parsed_site_path,
                )
                content = self._download_file_content(
                    access_token=access_token,
                    site_id=site_id,
                    workbook_path=parsed_workbook_path,
                )
                return SharePointWorkbookDownload(source_name=raw_workbook, content=content)

            content = self._download_file_from_share_url(access_token=access_token, share_url=raw_workbook)
            return SharePointWorkbookDownload(source_name=raw_workbook, content=content)

        normalized_workbook = raw_workbook.strip("/")
        resolved_hostname = self._normalize_hostname(site_hostname or self.settings.sharepoint_site_hostname)
        resolved_site_path = self._normalize_site_path(site_path or self.settings.sharepoint_site_path)
        if not resolved_hostname:
            raise ValueError("SharePoint hostname is missing. Set SHAREPOINT_SITE_HOSTNAME or provide site_hostname.")
        if not resolved_site_path:
            raise ValueError("SharePoint site path is missing. Set SHAREPOINT_SITE_PATH or provide site_path.")

        site_id = self._resolve_site_id(access_token=access_token, hostname=resolved_hostname, site_path=resolved_site_path)
        content = self._download_file_content(access_token=access_token, site_id=site_id, workbook_path=normalized_workbook)
        source_name = f"sharepoint://{resolved_hostname}/sites/{resolved_site_path}/{normalized_workbook}"
        return SharePointWorkbookDownload(source_name=source_name, content=content)

    def _normalize_site_path(self, raw: str | None) -> str:
        if raw is None:
            return ""
        text = raw.strip().strip("/")
        if text.startswith("sites/"):
            return text[len("sites/") :]
        return text

    def _normalize_hostname(self, raw: str | None) -> str:
        if raw is None:
            return ""
        text = raw.strip()
        if "://" in text:
            text = urlsplit(text).netloc
        return text.strip().strip("/")

    def _parse_direct_file_url(self, file_url: str) -> tuple[str, str, str] | None:
        parsed = urlsplit(file_url)
        if parsed.scheme.lower() not in {"http", "https"}:
            return None

        hostname = parsed.netloc.strip().strip("/")
        decoded_path = unquote(parsed.path).strip("/")
        if not hostname or not decoded_path:
            return None

        segments = [segment for segment in decoded_path.split("/") if segment]
        if len(segments) < 4:
            return None
        if segments[0] not in {"sites", "teams"}:
            return None

        configured_site_path = self._normalize_site_path(self.settings.sharepoint_site_path)
        if configured_site_path:
            configured_parts = [part for part in configured_site_path.split("/") if part]
            configured_prefix = [segments[0], *configured_parts]
            if len(segments) > len(configured_prefix) and segments[: len(configured_prefix)] == configured_prefix:
                workbook_path = "/".join(segments[len(configured_prefix) :])
                if workbook_path:
                    return hostname, configured_site_path, workbook_path

        site_path = segments[1]
        workbook_path = "/".join(segments[2:])
        if not workbook_path:
            return None
        return hostname, site_path, workbook_path

    def _has_client_credentials(self) -> bool:
        return bool(
            self.settings.sharepoint_tenant_id and self.settings.sharepoint_client_id and self.settings.sharepoint_client_secret
        )

    def _get_access_token(self) -> str:
        now = time.time()
        if self._access_token and now < self._access_token_expires_at_utc - 60:
            return self._access_token

        errors: list[str] = []

        if self.settings.sharepoint_use_managed_identity:
            try:
                token, expires_in = self._get_managed_identity_token()
                self._cache_token(token=token, expires_in=expires_in)
                return token
            except Exception as exc:  # pragma: no cover - environment specific
                errors.append(f"managed identity: {exc}")

        if self._has_client_credentials():
            try:
                token, expires_in = self._get_client_credentials_token()
                self._cache_token(token=token, expires_in=expires_in)
                return token
            except Exception as exc:
                errors.append(f"client credentials: {exc}")

        error_suffix = "; ".join(errors) if errors else "no authentication mode configured"
        raise RuntimeError(f"Failed to acquire SharePoint access token ({error_suffix}).")

    def _cache_token(self, token: str, expires_in: int) -> None:
        self._access_token = token
        self._access_token_expires_at_utc = time.time() + max(60, expires_in)

    def _get_client_credentials_token(self) -> tuple[str, int]:
        tenant_id = (self.settings.sharepoint_tenant_id or "").strip()
        client_id = (self.settings.sharepoint_client_id or "").strip()
        client_secret = (self.settings.sharepoint_client_secret or "").strip()
        if not tenant_id or not client_id or not client_secret:
            raise ValueError("tenant_id, client_id or client_secret missing")

        token_url = f"https://login.microsoftonline.com/{quote(tenant_id, safe='')}/oauth2/v2.0/token"
        response = requests.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": GRAPH_SCOPE,
            },
            timeout=self.settings.sharepoint_request_timeout,
        )
        payload = self._expect_json(response, action="token request")
        access_token = str(payload.get("access_token", "")).strip()
        if not access_token:
            raise RuntimeError("token response did not contain access_token")
        expires_in = int(payload.get("expires_in", 3600))
        return access_token, expires_in

    def _get_managed_identity_token(self) -> tuple[str, int]:
        identity_endpoint = (os.environ.get("IDENTITY_ENDPOINT") or "").strip()
        identity_header = (os.environ.get("IDENTITY_HEADER") or "").strip()
        timeout_seconds = min(3, self.settings.sharepoint_request_timeout)

        params: dict[str, str] = {"resource": GRAPH_RESOURCE}
        mi_client_id = (self.settings.sharepoint_managed_identity_client_id or "").strip()
        if mi_client_id:
            params["client_id"] = mi_client_id

        if identity_endpoint and identity_header:
            params["api-version"] = "2019-08-01"
            headers = {"X-IDENTITY-HEADER": identity_header}
            response = requests.get(
                identity_endpoint,
                headers=headers,
                params=params,
                timeout=timeout_seconds,
            )
        else:  # fallback for IMDS-enabled environments
            params["api-version"] = "2018-02-01"
            response = requests.get(
                "http://169.254.169.254/metadata/identity/oauth2/token",
                headers={"Metadata": "true"},
                params=params,
                timeout=timeout_seconds,
            )

        payload = self._expect_json(response, action="managed identity token request")
        access_token = str(payload.get("access_token", "")).strip()
        if not access_token:
            raise RuntimeError("managed identity response did not contain access_token")
        expires_in = int(payload.get("expires_in", 3600))
        return access_token, expires_in

    def _resolve_site_id(self, access_token: str, hostname: str, site_path: str) -> str:
        encoded_site_path = quote(site_path, safe="/")
        url = f"{self.settings.sharepoint_graph_base_url}/sites/{hostname}:/sites/{encoded_site_path}"
        payload = self._graph_get_json(url=url, access_token=access_token, action="resolve site")
        site_id = str(payload.get("id", "")).strip()
        if not site_id:
            raise RuntimeError("site resolution did not return site id")
        return site_id

    def _download_file_content(self, access_token: str, site_id: str, workbook_path: str) -> bytes:
        encoded_workbook_path = quote(workbook_path, safe="/")
        url = f"{self.settings.sharepoint_graph_base_url}/sites/{site_id}/drive/root:/{encoded_workbook_path}:/content"
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=self.settings.sharepoint_request_timeout,
            allow_redirects=True,
        )
        if response.status_code >= 400:
            self._raise_graph_error(response=response, action="download workbook")
        if not response.content:
            raise RuntimeError("downloaded workbook is empty")
        return response.content

    def _download_file_from_share_url(self, access_token: str, share_url: str) -> bytes:
        encoded_share = self._encode_share_url(share_url)
        url = f"{self.settings.sharepoint_graph_base_url}/shares/{encoded_share}/driveItem/content"
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=self.settings.sharepoint_request_timeout,
            allow_redirects=True,
        )
        if response.status_code >= 400:
            self._raise_graph_error(response=response, action="download workbook from share url")
        if not response.content:
            raise RuntimeError("downloaded workbook from share url is empty")
        return response.content

    def _encode_share_url(self, share_url: str) -> str:
        encoded = base64.urlsafe_b64encode(share_url.encode("utf-8")).decode("utf-8").rstrip("=")
        return f"u!{encoded}"

    def _graph_get_json(self, url: str, access_token: str, action: str) -> dict:
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=self.settings.sharepoint_request_timeout,
        )
        return self._expect_json(response=response, action=action)

    def _expect_json(self, response: requests.Response, action: str) -> dict:
        if response.status_code >= 400:
            self._raise_graph_error(response=response, action=action)
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"{action} returned non-JSON response (status {response.status_code}).") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"{action} returned unexpected payload type: {type(payload).__name__}.")
        return payload

    def _raise_graph_error(self, response: requests.Response, action: str) -> None:
        detail = response.text.strip()
        try:
            payload = response.json()
            if isinstance(payload, dict):
                error_message = ""
                raw_error = payload.get("error")
                if isinstance(raw_error, dict):
                    error_message = str(raw_error.get("message") or "").strip()
                elif isinstance(raw_error, str):
                    error_message = raw_error.strip()
                detail = str(payload.get("error_description") or "").strip() or error_message or detail
        except Exception:
            pass
        raise RuntimeError(f"{action} failed with status {response.status_code}: {detail}")
