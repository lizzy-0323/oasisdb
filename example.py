"""Example script for OasisDB Python SDK.

Run this after OasisDB server has started (default address: http://localhost:8080).
It will:
  1. Perform a health-check.
  2. Create a collection called 'demo'.
  3. Upsert a few documents.
  4. Perform a vector and a document search.
  5. List existing collections.
  6. Clean up by deleting the collection.

Usage
-----
$ python example.py
"""
from __future__ import annotations

import random
import sys
from typing import List

from client import OasisDBClient, OasisDBError


def random_vector(dim: int) -> List[float]:
    return [random.random() for _ in range(dim)]


def main() -> None:
    client = OasisDBClient()
    try:
        # 1. Health check
        ok = client.health_check()
        print("Health check:", "OK" if ok else "FAILED")
        if not ok:
            sys.exit(1)

        # 2. Create collection
        coll = client.create_collection("demo", dimension=3)
        print("Created collection:", coll)

        # 3. Upsert documents
        docs = [
            {"id": f"{i}", "vector": random_vector(3)}
            for i in range(5)
        ]
        client.batch_upsert_documents("demo", docs)
        print("Upserted", len(docs), "documents")

        # 4a. Vector search
        query_vec = random_vector(3)
        vec_results = client.search_vectors("demo", query_vec, limit=3)
        print("Vector search results:", vec_results)

        # 4b. Document search with filter
        doc_results = client.search_documents(
            "demo", query_vec, limit=3
        )
        print("Document search results:", doc_results)

        # 5. List collections
        colls = client.list_collections()
        print("Existing collections:", colls)

    except OasisDBError as e:
        print("Server returned error:", e)
    finally:
        # 6. Clean up
        try:
            client.delete_collection("demo")
            print("Deleted collection 'demo'")
        except OasisDBError:
            pass
        client.close()


if __name__ == "__main__":
    main()
