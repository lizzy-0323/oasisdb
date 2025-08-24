#!/usr/bin/env python3
"""
Log Monitor for OasisDB

This script monitors the OasisDB log file for compact-related events
and potential issues with get collection operations.
"""

import time
import json
import re
import os
import threading
from collections import defaultdict, deque
from datetime import datetime


class LogMonitor:
    def __init__(self, log_file_path: str = "./oasisdb.log"):
        self.log_file_path = log_file_path
        self.is_running = True
        self.stats = defaultdict(int)
        self.compact_events = deque(maxlen=100)  # Keep last 100 compact events
        self.errors = deque(maxlen=50)  # Keep last 50 errors
        self.get_collection_events = deque(maxlen=100)

    def parse_log_line(self, line: str) -> dict:
        """Parse a JSON log line"""
        try:
            return json.loads(line.strip())
        except:
            # Handle non-JSON lines
            return {"raw": line.strip()}

    def analyze_log_entry(self, entry: dict):
        """Analyze a single log entry"""
        if "raw" in entry:
            return  # Skip non-JSON entries

        level = entry.get("level", "").upper()
        msg = entry.get("msg", "")
        timestamp = entry.get("ts", "")

        # Count log levels
        self.stats[f"log_level_{level.lower()}"] += 1

        # Track compact events
        if "compact" in msg.lower():
            self.compact_events.append(
                {
                    "timestamp": timestamp,
                    "level": level,
                    "message": msg,
                    "details": entry,
                }
            )
            self.stats["compact_events"] += 1

            # Specific compact event types
            if "starting" in msg.lower():
                self.stats["compact_started"] += 1
                print(f"🔧 COMPACT STARTED: {msg}")
            elif "completed" in msg.lower():
                self.stats["compact_completed"] += 1
                duration = entry.get("duration", "unknown")
                print(f"✅ COMPACT COMPLETED: {msg} (duration: {duration})")
            elif "trigger" in msg.lower():
                self.stats["compact_triggered"] += 1
                print(f"⚡ COMPACT TRIGGERED: {msg}")

        # Track collection-related events
        if "collection" in msg.lower():
            if "get" in msg.lower() or "GetCollection" in entry.get("caller", ""):
                self.get_collection_events.append(
                    {
                        "timestamp": timestamp,
                        "level": level,
                        "message": msg,
                        "details": entry,
                    }
                )

                if level == "ERROR":
                    self.stats["get_collection_errors"] += 1
                    print(f"❌ GET COLLECTION ERROR: {msg}")
                else:
                    self.stats["get_collection_success"] += 1

        # Track errors
        if level == "ERROR":
            self.errors.append(
                {"timestamp": timestamp, "message": msg, "details": entry}
            )
            self.stats["total_errors"] += 1
            print(f"🚨 ERROR: {msg}")

        # Track specific LSM Tree operations
        if (
            "lsm" in msg.lower()
            or "sstable" in msg.lower()
            or "memtable" in msg.lower()
        ):
            self.stats["lsm_operations"] += 1

            if level == "ERROR":
                print(f"🔴 LSM ERROR: {msg}")
            elif "debug" in level.lower():
                # Only print important debug messages
                if any(
                    keyword in msg.lower() for keyword in ["failed", "error", "panic"]
                ):
                    print(f"🟡 LSM DEBUG: {msg}")

    def tail_log_file(self):
        """Tail the log file and process new lines"""
        try:
            if not os.path.exists(self.log_file_path):
                print(f"⚠️ Log file not found: {self.log_file_path}")
                print("   Waiting for log file to be created...")

                # Wait for log file to be created
                while not os.path.exists(self.log_file_path) and self.is_running:
                    time.sleep(1)

                if not self.is_running:
                    return

                print(f"✅ Log file found: {self.log_file_path}")

            # Read existing content first
            with open(self.log_file_path, "r") as f:
                # Go to end of file
                f.seek(0, 2)

                print(f"📈 Starting to monitor log file: {self.log_file_path}")

                while self.is_running:
                    line = f.readline()
                    if line:
                        entry = self.parse_log_line(line)
                        self.analyze_log_entry(entry)
                    else:
                        time.sleep(0.1)  # Wait for new content

        except Exception as e:
            print(f"💥 Error monitoring log file: {e}")

    def print_stats(self):
        """Print current statistics"""
        print("\n" + "=" * 60)
        print("📊 LOG MONITOR STATISTICS")
        print("=" * 60)

        # General stats
        print("📈 General:")
        for key, value in sorted(self.stats.items()):
            if key.startswith("log_level_"):
                level = key.replace("log_level_", "").upper()
                print(f"   {level} logs: {value}")

        print(f"\n🔧 Compact Operations:")
        print(f"   Events: {self.stats.get('compact_events', 0)}")
        print(f"   Started: {self.stats.get('compact_started', 0)}")
        print(f"   Completed: {self.stats.get('compact_completed', 0)}")
        print(f"   Triggered: {self.stats.get('compact_triggered', 0)}")

        print(f"\n📋 Collection Operations:")
        print(
            f"   Get collection success: {self.stats.get('get_collection_success', 0)}"
        )
        print(f"   Get collection errors: {self.stats.get('get_collection_errors', 0)}")

        success = self.stats.get("get_collection_success", 0)
        errors = self.stats.get("get_collection_errors", 0)
        if success + errors > 0:
            success_rate = success / (success + errors) * 100
            print(f"   Success rate: {success_rate:.2f}%")

        print(f"\n🚨 Errors:")
        print(f"   Total errors: {self.stats.get('total_errors', 0)}")
        print(f"   LSM operations: {self.stats.get('lsm_operations', 0)}")

        # Recent events
        if self.compact_events:
            print(f"\n🔧 Recent Compact Events (last 5):")
            for event in list(self.compact_events)[-5:]:
                timestamp = event["timestamp"]
                msg = event["message"]
                print(f"   [{timestamp}] {msg}")

        if self.errors:
            print(f"\n❌ Recent Errors (last 3):")
            for error in list(self.errors)[-3:]:
                timestamp = error["timestamp"]
                msg = error["message"]
                print(f"   [{timestamp}] {msg}")

    def run_monitor(self):
        """Run the log monitor"""
        print("🔍 OasisDB Log Monitor Starting...")
        print("=" * 60)

        # Start log tailing in background
        tail_thread = threading.Thread(target=self.tail_log_file, daemon=True)
        tail_thread.start()

        # Print stats periodically
        try:
            while self.is_running:
                time.sleep(10)  # Print stats every 10 seconds
                self.print_stats()

        except KeyboardInterrupt:
            print("\n⏹️ Monitor stopped by user")
        finally:
            self.is_running = False

    def stop(self):
        """Stop the monitor"""
        self.is_running = False


def main():
    """Main execution"""
    import sys

    log_file = "./oasisdb.log"
    if len(sys.argv) > 1:
        log_file = sys.argv[1]

    monitor = LogMonitor(log_file)

    try:
        monitor.run_monitor()
    except Exception as e:
        print(f"💥 Monitor failed: {e}")
        import traceback

        traceback.print_exc()
    finally:
        monitor.print_stats()


if __name__ == "__main__":
    main()
