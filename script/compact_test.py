#!/usr/bin/env python3
"""
Compact Test for OasisDB

This script creates random documents to trigger compact operations
and monitors for any failures in get collection operations.
"""

import sys
import os
import time
import random
import threading
import numpy as np
from typing import Dict, Any

# Add client SDK to path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "client-sdk", "python"))

from client import OasisDBClient


class CompactTester:
    def __init__(
        self, host: str = "http://localhost:8080", collection_name: str = "compact_test"
    ):
        self.client = OasisDBClient(host)
        self.collection_name = collection_name
        self.dimension = 128
        self.is_running = True
        self.errors = []
        self.stats = {
            "documents_inserted": 0,
            "get_collection_success": 0,
            "get_collection_failures": 0,
            "compact_triggers": 0,
        }

    def generate_random_document(self, doc_id: str) -> Dict[str, Any]:
        """Generate a random document with vector and metadata"""
        vector = np.random.normal(0, 1, self.dimension).astype(np.float32).tolist()

        return {
            "id": doc_id,
            "vector": vector,
            "parameters": {
                "category": random.choice(
                    ["electronics", "books", "clothing", "toys", "sports"]
                ),
                "price": round(random.uniform(10.0, 1000.0), 2),
                "rating": round(random.uniform(1.0, 5.0), 1),
                "description": f"Random item {doc_id} with {random.randint(5, 50)} features",
            },
            "dimension": self.dimension,
        }

    def setup_collection(self):
        """Setup test collection"""
        try:
            print(f"🔧 Setting up collection: {self.collection_name}")

            # Try to delete existing collection
            try:
                self.client.delete_collection(self.collection_name)
                print(f"✅ Deleted existing collection")
            except:
                pass  # Collection might not exist

            # Create new collection
            self.client.create_collection(
                name=self.collection_name, dimension=self.dimension, index_type="hnsw"
            )
            print(
                f"✅ Created collection: {self.collection_name} (dimension: {self.dimension})"
            )

            # Verify collection creation
            collection = self.client.get_collection(self.collection_name)
            print(f"✅ Collection verified: {collection}")

        except Exception as e:
            print(f"❌ Failed to setup collection: {e}")
            raise

    def monitor_collection_status(self):
        """Monitor collection status in background"""
        while self.is_running:
            try:
                start_time = time.time()
                collection = self.client.get_collection(self.collection_name)
                end_time = time.time()

                self.stats["get_collection_success"] += 1

                # Log success with timing
                print(
                    f"📊 Get collection success (took {end_time - start_time:.4f}s): {collection}"
                )

            except Exception as e:
                self.stats["get_collection_failures"] += 1
                error_msg = f"❌ Get collection failed: {e}"
                print(error_msg)
                self.errors.append(
                    {
                        "timestamp": time.time(),
                        "type": "get_collection_failure",
                        "error": str(e),
                    }
                )

            time.sleep(2)  # Check every 2 seconds

    def insert_documents_batch(self, batch_size: int = 1000):
        """Insert a batch of documents"""
        documents = []
        for i in range(batch_size):
            doc_id = f"doc_{self.stats['documents_inserted'] + i + 1:06d}"
            doc = self.generate_random_document(doc_id)
            documents.append(doc)

        try:
            start_time = time.time()
            self.client.batch_upsert_documents(self.collection_name, documents)
            end_time = time.time()

            self.stats["documents_inserted"] += batch_size
            print(
                f"📝 Inserted {batch_size} documents (took {end_time - start_time:.4f}s). Total: {self.stats['documents_inserted']}"
            )

            return True
        except Exception as e:
            error_msg = f"❌ Failed to insert documents: {e}"
            print(error_msg)
            self.errors.append(
                {"timestamp": time.time(), "type": "insert_failure", "error": str(e)}
            )
            return False

    def insert_single_document(self, doc_id: str):
        """Insert a single document"""
        doc = self.generate_random_document(doc_id)

        try:
            start_time = time.time()
            self.client.upsert_document(self.collection_name, doc)
            end_time = time.time()

            self.stats["documents_inserted"] += 1
            print(
                f"📝 Inserted document {doc_id} (took {end_time - start_time:.4f}s). Total: {self.stats['documents_inserted']}"
            )

            return True
        except Exception as e:
            error_msg = f"❌ Failed to insert document {doc_id}: {e}"
            print(error_msg)
            self.errors.append(
                {
                    "timestamp": time.time(),
                    "type": "single_insert_failure",
                    "error": str(e),
                    "doc_id": doc_id,
                }
            )
            return False

    def run_stress_test(self, duration_minutes: int = 10, batch_size: int = 50):
        """Run stress test to trigger compaction"""
        print(f"\n🚀 Starting stress test for {duration_minutes} minutes...")
        print(f"   Batch size: {batch_size}")
        print(
            f"   Target: Trigger LSM Tree compaction and monitor collection operations\n"
        )

        # Start monitoring thread
        monitor_thread = threading.Thread(
            target=self.monitor_collection_status, daemon=True
        )
        monitor_thread.start()

        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)

        batch_count = 0

        while time.time() < end_time and self.is_running:
            batch_count += 1
            print(f"\n--- Batch {batch_count} ---")

            # Insert batch of documents
            if self.insert_documents_batch(batch_size):
                # Wait a bit to let compaction potentially trigger
                time.sleep(1)

                # Try to perform some operations after insertion
                try:
                    # Test search to trigger more operations
                    random_vector = (
                        np.random.normal(0, 1, self.dimension)
                        .astype(np.float32)
                        .tolist()
                    )
                    results = self.client.search_vectors(
                        self.collection_name, random_vector, limit=5
                    )
                    print(
                        f"🔍 Search completed, found {len(results.get('ids', []))} results"
                    )
                except Exception as e:
                    print(f"⚠️ Search failed: {e}")
                    self.errors.append(
                        {
                            "timestamp": time.time(),
                            "type": "search_failure",
                            "error": str(e),
                        }
                    )

            # Print stats periodically
            if batch_count % 5 == 0:
                self.print_stats()

            time.sleep(0.5)  # Small delay between batches

        print(f"\n✅ Stress test completed after {duration_minutes} minutes")
        self.is_running = False

        # Final stats
        self.print_stats()
        self.print_errors()

    def print_stats(self):
        """Print current statistics"""
        print(f"\n📈 Current Statistics:")
        print(f"   Documents inserted: {self.stats['documents_inserted']}")
        print(f"   Get collection success: {self.stats['get_collection_success']}")
        print(f"   Get collection failures: {self.stats['get_collection_failures']}")
        print(f"   Total errors: {len(self.errors)}")

        if (
            self.stats["get_collection_success"] + self.stats["get_collection_failures"]
            > 0
        ):
            success_rate = (
                self.stats["get_collection_success"]
                / (
                    self.stats["get_collection_success"]
                    + self.stats["get_collection_failures"]
                )
                * 100
            )
            print(f"   Get collection success rate: {success_rate:.2f}%")

    def print_errors(self):
        """Print detailed error information"""
        if not self.errors:
            print("\n✅ No errors detected!")
            return

        print(f"\n❌ Errors detected ({len(self.errors)} total):")
        for i, error in enumerate(self.errors[-10:], 1):  # Show last 10 errors
            timestamp = time.strftime("%H:%M:%S", time.localtime(error["timestamp"]))
            print(f"   {i}. [{timestamp}] {error['type']}: {error['error']}")

        if len(self.errors) > 10:
            print(f"   ... and {len(self.errors) - 10} more errors")

    def cleanup(self):
        """Cleanup resources"""
        try:
            self.is_running = False
            print(f"\n🧹 Cleaning up...")
            # Optionally delete test collection
            # self.client.delete_collection(self.collection_name)
            # print(f"✅ Deleted test collection")
        except Exception as e:
            print(f"⚠️ Cleanup warning: {e}")


def main():
    """Main test execution"""
    print("🏁 OasisDB Compact Test Starting...")
    print("=" * 60)

    tester = CompactTester()

    try:
        # Setup
        tester.setup_collection()

        # Run stress test
        duration = 5  # Test for 5 minutes
        batch_size = 1000  # Insert 100 documents per batch

        tester.run_stress_test(duration_minutes=duration, batch_size=batch_size)

    except KeyboardInterrupt:
        print("\n⏹️ Test interrupted by user")
    except Exception as e:
        print(f"\n💥 Test failed with error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        tester.cleanup()

        # Final summary
        print("\n" + "=" * 60)
        print("📋 FINAL SUMMARY")
        print("=" * 60)
        tester.print_stats()
        tester.print_errors()

        if tester.stats["get_collection_failures"] > 0:
            print("\n🚨 ISSUE DETECTED: Get collection failures occurred!")
            print("   Check the OasisDB server logs for compact-related errors.")
        else:
            print("\n✅ No get collection failures detected during test.")


if __name__ == "__main__":
    main()
