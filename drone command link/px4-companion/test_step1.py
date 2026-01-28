#!/usr/bin/env python3
"""
test_step1.py - Validation tests for SQLite gating

Tests:
1. Duplicate command protection
2. Agent restart safety
3. Database persistence
4. Concurrent command handling

Run this after installing Step 1.
"""

import json
import time
import sqlite3
import subprocess
import paho.mqtt.client as mqtt
from datetime import datetime

# Configuration
DRONE_ID = "drone-01"
MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
DB_PATH = "/var/lib/px4-agent/commands.db"
SERVICE_NAME = "px4-agent"

class TestRunner:
    def __init__(self):
        self.mqtt_client = None
        self.test_results = []
    
    def connect_mqtt(self):
        """Connect to MQTT broker."""
        print("🔌 Connecting to MQTT broker...")
        self.mqtt_client = mqtt.Client(client_id="test_client")
        self.mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        self.mqtt_client.loop_start()
        time.sleep(1)
        print("✅ Connected to MQTT\n")
    
    def send_command(self, cmd_id, cmd_type="RTL", params=None):
        """Send command via MQTT."""
        topic = f"drone/{DRONE_ID}/cmd"
        
        payload = {
            "cmd_id": cmd_id,
            "cmd": cmd_type,
            "params": params or {},
            "drone_id": DRONE_ID,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        
        print(f"📤 Sending command: {cmd_id} ({cmd_type})")
        result = self.mqtt_client.publish(topic, json.dumps(payload), qos=1)
        result.wait_for_publish()
        time.sleep(0.5)  # Give agent time to process
    
    def check_database(self, cmd_id):
        """Check if command exists in database."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute(
            "SELECT cmd_id, cmd_type, state FROM commands WHERE cmd_id = ?",
            (cmd_id,)
        )
        row = cursor.fetchone()
        conn.close()
        return row
    
    def get_db_stats(self):
        """Get database statistics."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute(
            "SELECT state, COUNT(*) as cnt FROM commands GROUP BY state"
        )
        stats = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()
        return stats
    
    def restart_agent(self):
        """Restart the agent service."""
        print("🔄 Restarting agent service...")
        subprocess.run(["systemctl", "restart", SERVICE_NAME], check=True)
        time.sleep(3)  # Wait for restart
        print("✅ Agent restarted\n")
    
    def test_1_duplicate_protection(self):
        """Test 1: Duplicate command should not execute twice."""
        print("=" * 60)
        print("TEST 1: Duplicate Command Protection")
        print("=" * 60)
        
        cmd_id = "test-duplicate-001"
        
        # Send command first time
        print("\n1️⃣ Sending command (first time)...")
        self.send_command(cmd_id, "RTL")
        
        row = self.check_database(cmd_id)
        if row:
            print(f"✅ Command stored: {row}")
        else:
            print("❌ Command NOT in database!")
            self.test_results.append(("Test 1", "FAIL", "Command not stored"))
            return
        
        # Send same command again
        print("\n2️⃣ Sending SAME command (duplicate)...")
        self.send_command(cmd_id, "RTL")
        
        # Check that it still exists only once
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute(
            "SELECT COUNT(*) FROM commands WHERE cmd_id = ?",
            (cmd_id,)
        )
        count = cursor.fetchone()[0]
        conn.close()
        
        if count == 1:
            print(f"✅ PASS: Command exists exactly once (count={count})")
            self.test_results.append(("Test 1", "PASS", "Duplicate blocked"))
        else:
            print(f"❌ FAIL: Command count = {count} (expected 1)")
            self.test_results.append(("Test 1", "FAIL", f"Count={count}"))
        
        print("")
    
    def test_2_restart_safety(self):
        """Test 2: Command should not re-execute after restart."""
        print("=" * 60)
        print("TEST 2: Agent Restart Safety")
        print("=" * 60)
        
        cmd_id = "test-restart-002"
        
        # Send command
        print("\n1️⃣ Sending command...")
        self.send_command(cmd_id, "LOITER")
        
        row = self.check_database(cmd_id)
        if not row:
            print("❌ Command not stored - cannot test restart!")
            self.test_results.append(("Test 2", "FAIL", "Setup failed"))
            return
        
        print(f"✅ Command stored: {row}")
        
        # Restart agent
        print("\n2️⃣ Restarting agent...")
        self.restart_agent()
        
        # Reconnect MQTT
        self.mqtt_client.loop_stop()
        time.sleep(1)
        self.connect_mqtt()
        
        # Check database again
        print("\n3️⃣ Checking database after restart...")
        row = self.check_database(cmd_id)
        
        if row:
            print(f"✅ PASS: Command still exists: {row}")
            self.test_results.append(("Test 2", "PASS", "Command persisted"))
        else:
            print("❌ FAIL: Command lost after restart!")
            self.test_results.append(("Test 2", "FAIL", "Command lost"))
        
        print("")
    
    def test_3_concurrent_commands(self):
        """Test 3: Multiple different commands."""
        print("=" * 60)
        print("TEST 3: Concurrent Commands")
        print("=" * 60)
        
        cmd_ids = [
            "test-concurrent-003a",
            "test-concurrent-003b",
            "test-concurrent-003c"
        ]
        
        print("\n1️⃣ Sending multiple commands rapidly...")
        for cmd_id in cmd_ids:
            self.send_command(cmd_id, "RTL")
        
        time.sleep(1)  # Wait for processing
        
        print("\n2️⃣ Checking database...")
        missing = []
        for cmd_id in cmd_ids:
            row = self.check_database(cmd_id)
            if row:
                print(f"✅ Found: {cmd_id}")
            else:
                print(f"❌ Missing: {cmd_id}")
                missing.append(cmd_id)
        
        if len(missing) == 0:
            print("\n✅ PASS: All commands stored")
            self.test_results.append(("Test 3", "PASS", "All commands stored"))
        else:
            print(f"\n❌ FAIL: Missing {len(missing)} commands")
            self.test_results.append(("Test 3", "FAIL", f"Missing {missing}"))
        
        print("")
    
    def test_4_database_stats(self):
        """Test 4: Database statistics."""
        print("=" * 60)
        print("TEST 4: Database Statistics")
        print("=" * 60)
        
        stats = self.get_db_stats()
        
        print("\n📊 Current database stats:")
        for state, count in stats.items():
            print(f"   {state}: {count}")
        
        if "RECEIVED" in stats and stats["RECEIVED"] > 0:
            print("\n✅ PASS: Database has RECEIVED commands")
            self.test_results.append(("Test 4", "PASS", "Stats valid"))
        else:
            print("\n⚠️  WARNING: No RECEIVED commands in database")
            self.test_results.append(("Test 4", "WARN", "No commands"))
        
        print("")
    
    def print_summary(self):
        """Print test summary."""
        print("=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)
        
        passed = sum(1 for _, result, _ in self.test_results if result == "PASS")
        failed = sum(1 for _, result, _ in self.test_results if result == "FAIL")
        warnings = sum(1 for _, result, _ in self.test_results if result == "WARN")
        
        for test_name, result, detail in self.test_results:
            status_icon = "✅" if result == "PASS" else "❌" if result == "FAIL" else "⚠️"
            print(f"{status_icon} {test_name}: {result} - {detail}")
        
        print("")
        print(f"Results: {passed} passed, {failed} failed, {warnings} warnings")
        
        if failed == 0:
            print("\n🎉 All critical tests passed!")
            print("✅ Step 1 validation complete - ready for Step 2")
        else:
            print("\n⚠️  Some tests failed - review logs before proceeding")
    
    def run_all_tests(self):
        """Run all validation tests."""
        print("\n")
        print("🧪 Starting Step 1 Validation Tests")
        print("")
        
        self.connect_mqtt()
        
        try:
            self.test_1_duplicate_protection()
            self.test_2_restart_safety()
            self.test_3_concurrent_commands()
            self.test_4_database_stats()
        finally:
            if self.mqtt_client:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
        
        self.print_summary()


def main():
    """Main test runner."""
    import sys
    import os
    
    # Check if running as root
    if os.geteuid() != 0:
        print("⚠️  Warning: Not running as root - some tests may fail")
        print("   Recommended: sudo python3 test_step1.py")
        print("")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(0)
    
    # Check if database exists
    if not os.path.exists(DB_PATH):
        print(f"❌ Error: Database not found at {DB_PATH}")
        print("   Run install_step1.sh first!")
        sys.exit(1)
    
    # Check if agent is running
    result = subprocess.run(
        ["systemctl", "is-active", SERVICE_NAME],
        capture_output=True,
        text=True
    )
    
    if result.stdout.strip() != "active":
        print(f"⚠️  Warning: {SERVICE_NAME} service is not running")
        print("   Start it with: sudo systemctl start px4-agent")
        sys.exit(1)
    
    # Run tests
    runner = TestRunner()
    runner.run_all_tests()


if __name__ == "__main__":
    main()
