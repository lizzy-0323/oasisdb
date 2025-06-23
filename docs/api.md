# OasisDB Python SDK API Reference

This document is generated from the `OasisDBClient` class implemented in `client.py`. It lists all public methods and explains how to use them so that developers can get started quickly.

---

## Quick Start

```python
from client import OasisDBClient

client = OasisDBClient()
print(client.health_check())  # Should print True when the server is reachable
```

---

## `OasisDBClient` Constructor

```python
OasisDBClient(
    base_url: str = "http://localhost:8080",
    *,
    session: Optional[requests.Session] = None,
    timeout: Optional[float | tuple[float, float]] = 30,
)
```

Parameter description:

| Parameter | Type | Default | Description |
| --------- | ---- | ------- | ----------- |
| `base_url` | `str` | `"http://localhost:8080"` | Root URL of the OasisDB HTTP service |
| `session` | `requests.Session \| None` | `None` | Optional shared HTTP session |
| `timeout` | `float \| (float, float) \| None` | `30` | Request timeout in seconds; pass `None` for no limit |

---

## Method Overview

| Method | Return | Description |
| ------ | ------ | ----------- |
| `health_check()` | `bool` | Check whether the server is alive |
| `create_collection(name, dimension, *, index_type="hnsw", parameters=None)` | `dict` | Create a vector collection |
| `get_collection(name)` | `dict` | Get collection details |
| `list_collections()` | `list[dict]` | List all collections |
| `delete_collection(name)` | `None` | Delete a collection |
| `upsert_document(collection, *, doc_id, vector, parameters=None)` | `dict` | Insert or update a single document |
| `batch_upsert_documents(collection, documents)` | `None` | Insert/update multiple documents |
| `get_document(collection, doc_id)` | `dict` | Get a single document |
| `delete_document(collection, doc_id)` | `None` | Delete a single document |
| `build_index(collection, documents)` | `None` | Build index offline |
| `search_vectors(collection, vector, *, limit=10)` | `dict` | Return vector-only nearest-neighbor results |
| `search_documents(collection, vector, *, limit=10, filter=None)` | `dict` | Return document results with optional filter |

Detailed explanations, parameters and examples for each method are provided below.

---

### `health_check()`

* **HTTP call**: `GET /`
* **Return**: `True` if the server returns `{"status": "ok"}`.

```python
client.health_check()  # True / False
```

---

### `create_collection()`

```python
create_collection(
    name: str,
    dimension: int,
    *,
    index_type: str = "hnsw",
    parameters: Mapping[str, str] | None = None,
) -> dict
```

Explanation:
1. `name`: collection name, unique.
2. `dimension`: vector dimension.
3. `index_type`: index type, currently supports `"hnsw"`.
4. `parameters`: index-specific parameter dictionary.

Example:

```python
client.create_collection("movies", 768, index_type="hnsw", parameters={"efConstruction": "200"})
```

---

### `get_collection()` / `list_collections()` / `delete_collection()`

* `get_collection(name)`: `GET /v1/collections/{name}`
* `list_collections()`: `GET /v1/collections`
* `delete_collection(name)`: `DELETE /v1/collections/{name}`

```python
info = client.get_collection("movies")
all_cols = client.list_collections()
client.delete_collection("movies")
```

---

### `upsert_document()`

```python
upsert_document(
    collection: str,
    *,
    doc_id: str,
    vector: Sequence[float],
    parameters: Mapping[str, Any] | None = None,
) -> dict
```

Insert or update a single document in the specified collection.

Example:

```python
client.upsert_document(
    "movies",
    doc_id="tt0111161",
    vector=[0.12, 0.98, ...],
    parameters={"title": "The Shawshank Redemption"},
)
```

---

### `batch_upsert_documents()`

```python
batch_upsert_documents(collection: str, documents: Iterable[Mapping[str, Any]]) -> None
```

Insert or update multiple documents at once. Each element in `documents` must contain `id` and `vector`; other fields are optional.

---

### `get_document()` / `delete_document()`

* `get_document(collection, doc_id)`: `GET /v1/collections/{collection}/documents/{id}`
* `delete_document(collection, doc_id)`: `DELETE /v1/collections/{collection}/documents/{id}`

---

### `build_index()`

```python
build_index(collection: str, documents: Iterable[Mapping[str, Any]]) -> None
```

Build the index on the server side offline; useful when you import a large dataset and then build the index in one shot.

---

### `search_vectors()`

```python
search_vectors(collection: str, vector: Sequence[float], *, limit: int = 10) -> dict
```

Return only similarity scores of vectors without document metadata.

---

### `search_documents()`

```python
search_documents(
    collection: str,
    vector: Sequence[float],
    *,
    limit: int = 10,
    filter: Mapping[str, Any] | None = None,
) -> dict
```

Return matching documents and scores. You can pass a `filter` in JSON-Schema style to refine results.

Example:

```python
results = client.search_documents(
    "movies",
    query_vector,
    limit=5,
    filter={"genre": "Drama"},
)
for hit in results["hits"]:
    print(hit["id"], hit["score"])
```

---

## Error Handling

All methods raise `OasisDBError` when the server returns 4xx or 5xx.

```python
from client import OasisDBError

try:
    client.get_collection("non-exist")
except OasisDBError as e:
    print(e.status_code, str(e))
```

---

## Resource Management

`OasisDBClient` implements the context-manager protocol, so you can use `with` to ensure the HTTP connection is closed properly:

```python
with OasisDBClient() as client:
    client.create_collection("demo", 128)
```

That’s all! You now know every public API in the Python SDK—happy vectorizing!
