#!/usr/bin/env python3
"""
agent_db.py - SQLite persistence layer for px4_agent

STEP 1: Minimal schema for command gating
- Commands cannot execute without SQLite record
- Duplicate protection via cmd_id PRIMARY KEY
- Restart safety via state persistence

Schema design principles:
- Start minimal (cmd_id, cmd_type, state, timestamps)
- No payload yet (keeps logic simple)
- No error taxonomy (comes in Step 4)
- Focus: gate execution, nothing more
"""

import sqlite3
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from contextlib import contextmanager

log = logging.getLogger("agent_db")

# State constants (Step 1: only RECEIVED)
STATE_RECEIVED = "RECEIVED"

class CommandDatabase:
    """
    SQLite database for command persistence and gating.
    
    Thread-safety: Use get_connection() context manager for all operations.
    Location: Must be persistent (/var/lib/px4-agent/commands.db)
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
        log.info(f"✅ Command database initialized: {db_path}")
    
    def _init_db(self):
        """Create tables if they don't exist."""
        with self.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS commands (
                    cmd_id TEXT PRIMARY KEY,
                    cmd_type TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_state_created 
                ON commands(state, created_at)
            """)
            conn.commit()
            log.info("Database schema ready")
    
    @contextmanager
    def get_connection(self):
        """Context manager for thread-safe database access."""
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def command_exists(self, cmd_id: str) -> bool:
        """
        Check if command already exists in database.
        
        This is the GATE: if True, command must NOT execute.
        """
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT 1 FROM commands WHERE cmd_id = ? LIMIT 1",
                (cmd_id,)
            )
            exists = cursor.fetchone() is not None
            if exists:
                log.info(f"🔒 Command {cmd_id} already exists - DUPLICATE BLOCKED")
            return exists
    
    def insert_command(self, cmd_id: str, cmd_type: str) -> bool:
        """
        Insert new command with state=RECEIVED.
        
        Returns:
            True if inserted successfully
            False if cmd_id already exists (duplicate)
        """
        now = int(datetime.utcnow().timestamp())
        
        try:
            with self.get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO commands (cmd_id, cmd_type, state, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (cmd_id, cmd_type, STATE_RECEIVED, now, now)
                )
                conn.commit()
                log.info(f"✅ Command {cmd_id} inserted with state={STATE_RECEIVED}")
                return True
        except sqlite3.IntegrityError:
            # Primary key violation - duplicate cmd_id
            log.warning(f"🔒 Command {cmd_id} already exists - INSERT blocked")
            return False
    
    def get_command_state(self, cmd_id: str) -> Optional[str]:
        """Get current state of a command."""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT state FROM commands WHERE cmd_id = ?",
                (cmd_id,)
            )
            row = cursor.fetchone()
            return row['state'] if row else None
    
    def update_state(self, cmd_id: str, new_state: str) -> bool:
        """
        Update command state.
        
        Returns True if update succeeded, False if cmd_id not found.
        """
        now = int(datetime.utcnow().timestamp())
        
        with self.get_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE commands 
                SET state = ?, updated_at = ?
                WHERE cmd_id = ?
                """,
                (new_state, now, cmd_id)
            )
            conn.commit()
            
            if cursor.rowcount > 0:
                log.info(f"✅ Command {cmd_id} state updated: {new_state}")
                return True
            else:
                log.warning(f"⚠️  Command {cmd_id} not found for state update")
                return False
    
    def get_command_info(self, cmd_id: str) -> Optional[Dict[str, Any]]:
        """Get full command record."""
        with self.get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT cmd_id, cmd_type, state, created_at, updated_at
                FROM commands
                WHERE cmd_id = ?
                """,
                (cmd_id,)
            )
            row = cursor.fetchone()
            if row:
                return {
                    'cmd_id': row['cmd_id'],
                    'cmd_type': row['cmd_type'],
                    'state': row['state'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
            return None
    
    def count_commands_by_state(self, state: str) -> int:
        """Count commands in a given state."""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) as cnt FROM commands WHERE state = ?",
                (state,)
            )
            return cursor.fetchone()['cnt']
    
    def get_stats(self) -> Dict[str, int]:
        """Get database statistics."""
        with self.get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT state, COUNT(*) as cnt
                FROM commands
                GROUP BY state
                """
            )
            stats = {row['state']: row['cnt'] for row in cursor.fetchall()}
            
            cursor = conn.execute("SELECT COUNT(*) as total FROM commands")
            stats['TOTAL'] = cursor.fetchone()['total']
            
            return stats


def test_database():
    """Test database operations."""
    import tempfile
    import os
    
    # Create temp database
    fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    
    try:
        print("Testing CommandDatabase...")
        
        db = CommandDatabase(db_path)
        
        # Test 1: Insert new command
        print("\n1. Testing insert...")
        success = db.insert_command("cmd-001", "RTL")
        assert success, "First insert should succeed"
        assert db.command_exists("cmd-001"), "Command should exist"
        
        # Test 2: Duplicate insert
        print("2. Testing duplicate protection...")
        success = db.insert_command("cmd-001", "RTL")
        assert not success, "Duplicate insert should fail"
        
        # Test 3: State retrieval
        print("3. Testing state retrieval...")
        state = db.get_command_state("cmd-001")
        assert state == STATE_RECEIVED, f"State should be RECEIVED, got {state}"
        
        # Test 4: Stats
        print("4. Testing stats...")
        stats = db.get_stats()
        print(f"   Stats: {stats}")
        assert stats['TOTAL'] == 1
        assert stats[STATE_RECEIVED] == 1
        
        print("\n✅ All tests passed!")
        
    finally:
        os.unlink(db_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    test_database()
