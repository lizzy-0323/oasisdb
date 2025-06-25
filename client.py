"""OasisDB Python SDK

This module provides a thin, high-level wrapper around the OasisDB HTTP API so that
users can interact with the database easily from Python code.

Example
-------
>>> from client import OasisDBClient
>>> client = OasisDBClient()
>>> client.health_check()
True

All methods raise `OasisDBError` (a subclass of `RuntimeError`) when the server
returns a non-successful status code.
"""
from __future__ import annotations

import logging
from typing import Any, Mapping, MutableMapping, Optional, Sequence, Iterable, Dict, List

import requests

__all__ = [
    "OasisDBClient",
    "OasisDBError",
]

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class OasisDBError(RuntimeError):
    """Represents an error returned by the OasisDB server."""

    def __init__(self, status_code: int, message: Optional[str] = None):
        self.status_code = status_code
        super().__init__(message or f"HTTP {status_code}")


class OasisDBClient:
    """High-level HTTP client for OasisDB.

    Parameters
    ----------
    base_url:
        Base URL where the OasisDB HTTP server is reachable. The default is
        ``http://localhost:8080``.
    session:
        Optional *requests* session. If ``None`` a new :class:`requests.Session`
        will be created.
    timeout:
        Default timeout (in seconds) applied to every request unless explicitly
        overridden.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        *,
        session: Optional[requests.Session] = None,
        timeout: Optional[float] | tuple[float, float] = 30,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session: requests.Session = session or requests.Session()
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------
    def _url(self, path: str) -> str:
        """Return full URL for *path* (which must start with '/')."""
        return f"{self.base_url}{path}"

    def _request(self, method: str, path: str, **kwargs: Any):
        url = self._url(path)
        if "timeout" not in kwargs and self._timeout is not None:
            kwargs["timeout"] = self._timeout

        logger.debug("%s %s", method.upper(), url)
        response = self.session.request(method, url, **kwargs)

        if response.status_code >= 400:
            raise OasisDBError(response.status_code, response.text)

        if not response.content:
            return None

        try:
            return response.json()
        except ValueError:
            return response.text

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------
    # System / health ---------------------------------------------------
    def health_check(self) -> bool:
        """Ping the root endpoint and return True if server replies."""
        return self._request("GET", "/") == {"status": "ok"}

    # Collections -------------------------------------------------------
    def create_collection(
        self,
        name: str,
        dimension: int,
        *,
        index_type: str = "hnsw",
        parameters: Optional[Mapping[str, str]] = None,
    ) -> Dict[str, Any]:
        payload = {
            "name": name,
            "dimension": dimension,
            "index_type": index_type,
            "parameters": parameters or {},
        }
        return self._request("POST", "/v1/collections", json=payload)

    def get_collection(self, name: str) -> Dict[str, Any]:
        return self._request("GET", f"/v1/collections/{name}")

    def list_collections(self) -> List[Dict[str, Any]]:
        return self._request("GET", "/v1/collections")

    def delete_collection(self, name: str) -> None:
        self._request("DELETE", f"/v1/collections/{name}")

    # Documents ---------------------------------------------------------
    def upsert_document(
        self,
        collection: str,
        *,
        doc_id: str,
        vector: Sequence[float],
        parameters: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload = {
            "id": doc_id,
            "vector": list(vector),
            "parameters": parameters or {},
        }
        return self._request(
            "POST", f"/v1/collections/{collection}/documents", json=payload
        )

    def batch_upsert_documents(
        self,
        collection: str,
        documents: Iterable[Mapping[str, Any]],
    ) -> None:
        docs = []
        for doc in documents:
            if "id" not in doc or "vector" not in doc:
                raise ValueError("Each document must contain 'id' and 'vector'.")
            docs.append(doc)
        self._request(
            "POST", f"/v1/collections/{collection}/documents/batchupsert", json={"documents": docs}
        )

    def get_document(self, collection: str, doc_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/v1/collections/{collection}/documents/{doc_id}")

    def delete_document(self, collection: str, doc_id: str) -> None:
        self._request("DELETE", f"/v1/collections/{collection}/documents/{doc_id}")

    # Index building ----------------------------------------------------
    def build_index(self, collection: str, documents: Iterable[Mapping[str, Any]]) -> None:
        self._request(
            "POST", f"/v1/collections/{collection}/buildindex", json={"documents": list(documents)}
        )

    def set_params(
        self,
        collection: str,
        parameters: Mapping[str, Any],
    ) -> None:
        """Set search/index parameters for a collection.

        Parameters
        ----------
        collection:
            Target collection name.
        parameters:
            Dictionary of parameter name → value pairs (e.g. ``{"efsearch": 128}``).
        """
        payload = {"parameters": parameters}
        self._request(
            "POST",
            f"/v1/collections/{collection}/documents/setparams",
            json=payload,
        )

    # Search ------------------------------------------------------------
    def search_vectors(
        self,
        collection: str,
        vector: Sequence[float],
        *,
        limit: int = 10,
    ) -> Dict[str, Any]:
        payload = {"vector": list(vector), "limit": limit}
        return self._request(
            "POST", f"/v1/collections/{collection}/vectors/search", json=payload
        )

    def search_documents(
        self,
        collection: str,
        vector: Sequence[float],
        *,
        limit: int = 10,
        filter: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload: MutableMapping[str, Any] = {"vector": list(vector), "limit": limit}
        if filter:
            payload["filter"] = filter
        return self._request(
            "POST", f"/v1/collections/{collection}/documents/search", json=payload
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "OasisDBClient":
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
