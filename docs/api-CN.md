# OasisDB Python SDK API 参考

本文档基于 `client.py` 中提供的 `OasisDBClient` 类，列举并说明全部公开接口，方便开发者快速上手。

---

## 快速开始

```python
from client import OasisDBClient

client = OasisDBClient()
print(client.health_check())  # 应输出 True 当服务器可用时
```

---

## `OasisDBClient` 构造函数

```python
OasisDBClient(
    base_url: str = "http://localhost:8080",
    *,
    session: Optional[requests.Session] = None,
    timeout: Optional[float | tuple[float, float]] = 30,
)
```

参数说明：

| 参数 | 类型 | 默认值 | 说明 |
| ---- | ---- | ------ | ---- |
| `base_url` | `str` | `"http://localhost:8080"` | OasisDB HTTP 服务的根地址 |
| `session` | `requests.Session \| None` | `None` | 可选，共享的 HTTP 会话 |
| `timeout` | `float \| (float, float) \| None` | `30` | 单个请求的超时时间（秒），传 `None` 表示不限制 |

---

## 方法一览

| 方法 | 返回值 | 描述 |
| ---- | ------ | ---- |
| `health_check()` | `bool` | 检查服务器是否可用 |
| `create_collection(name, dimension, *, index_type="hnsw", parameters=None)` | `dict` | 创建向量集合 |
| `get_collection(name)` | `dict` | 查询集合详情 |
| `list_collections()` | `list[dict]` | 列出全部集合 |
| `delete_collection(name)` | `None` | 删除集合 |
| `upsert_document(collection, *, doc_id, vector, parameters=None)` | `dict` | 插入或更新单条文档 |
| `batch_upsert_documents(collection, documents)` | `None` | 批量插入/更新文档 |
| `get_document(collection, doc_id)` | `dict` | 查询单条文档 |
| `delete_document(collection, doc_id)` | `None` | 删除单条文档 |
| `build_index(collection, documents)` | `None` | 离线构建索引 |
| `set_params(collection, parameters)` | `None` | 调整索引/搜索参数 |
| `search_vectors(collection, vector, *, limit=10)` | `dict` | 仅返回向量近邻结果 |
| `search_documents(collection, vector, *, limit=10, filter=None)` | `dict` | 返回文档近邻结果，可附带过滤条件 |

下文详细介绍每个方法的用途、参数与示例。

---

### `health_check()`

- **HTTP 调用**：`GET /`
- **返回**：`True` 表示服务器返回 `{"status": "ok"}`。

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

说明：
1. `name`：集合名称，唯一。
2. `dimension`：向量维度。
3. `index_type`：索引类型，目前支持 `"hnsw"`。
4. `parameters`：索引参数字典，可根据索引类型调整。

示例：

```python
client.create_collection("movies", 768, index_type="hnsw", parameters={"efConstruction": "200"})
```

---

### `get_collection()` / `list_collections()` / `delete_collection()`

- `get_collection(name)`：`GET /v1/collections/{name}`
- `list_collections()`：`GET /v1/collections`
- `delete_collection(name)`：`DELETE /v1/collections/{name}`

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

向指定集合写入或更新一条文档。

示例：

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

一次写入多条文档，`documents` 中的每个元素必须包含 `id` 与 `vector` 字段，其余字段可选。

---

### `get_document()` / `delete_document()`

- `get_document(collection, doc_id)`：`GET /v1/collections/{collection}/documents/{id}`
- `delete_document(collection, doc_id)`：`DELETE /v1/collections/{collection}/documents/{id}`

---

### `build_index()`

```python
build_index(collection: str, documents: Iterable[Mapping[str, Any]]) -> None
```

在服务器端离线构建索引，适用于一次性导入大量数据后统一建立索引的场景。

---

---

### `set_params()`

```python
set_params(collection: str, parameters: Mapping[str, Any]) -> None
```

为指定集合设置运行时搜索或索引参数，例如 ``{"efsearch": 128}``。

示例：

```python
client.set_params("movies", {"efsearch": 128})
```

---

### `search_vectors()`

```python
search_vectors(collection: str, vector: Sequence[float], *, limit: int = 10) -> dict
```

仅返回向量与目标集合中向量的相似度结果，不包含文档元数据。

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

同时返回匹配文档及其分数，可通过 `filter` 传入字段过滤条件（JSON Schema 形式）。

示例：

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

## 错误处理

所有接口在服务器返回 4xx / 5xx 时会抛出 `OasisDBError`。

```python
from client import OasisDBError

try:
    client.get_collection("non-exist")
except OasisDBError as e:
    print(e.status_code, str(e))
```

---

## 资源管理

`OasisDBClient` 实现了上下文管理协议，可使用 `with` 关键字确保 HTTP 连接正确关闭：

```python
with OasisDBClient() as client:
    client.create_collection("demo", 128)
```

