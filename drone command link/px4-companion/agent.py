#!/usr/bin/env python3
"""
px4_agent.py - Complete version with SQLite command gating (STEP 1)
Added: LAND, ARM, DISARM, TAKEOFF, fixed LOITER
NEW: SQLite gating to prevent duplicate execution
"""
import os
import time
import json
import sqlite3
import threading
import logging
from datetime import datetime
import paho.mqtt.client as mqtt
from pymavlink import mavutil

# ---------- config via env ----------
DRONE_ID = os.environ.get("DRONE_ID", "drone-01")
MQTT_HOST = os.environ.get("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "drone")
MQTT_PASS = os.environ.get("MQTT_PASS", "")
PX4_URL = os.environ.get("PX4_URL", "udp:127.0.0.1:14550")
DB_PATH = os.environ.get("DB_PATH", "/var/lib/px4_agent/agent.db")
HEARTBEAT_TIMEOUT = int(os.environ.get("HEARTBEAT_TIMEOUT", "10"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
ENABLE_CLOUD_LINK_CHECK = os.environ.get("ENABLE_CLOUD_LINK_CHECK", "false").lower() in ("true", "1", "yes")

# Try to read MQTT password from file if not in env
if not MQTT_PASS:
    try:
        for path in ['/etc/mqtt-creds/mosquitto_drone_credentials.txt', '/root/mosquitto_drone_credentials.txt']:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    for line in f:
                        if line.startswith(f'{MQTT_USER}:'):
                            MQTT_PASS = line.strip().split(':', 1)[1]
                            break
                if MQTT_PASS:
                    break
    except Exception as e:
        pass

if ENABLE_CLOUD_LINK_CHECK:
    log_msg = "🔒 Cloud link checking ENABLED - commands require heartbeat"
else:
    log_msg = "⚠️  Cloud link checking DISABLED - accepting all commands (local testing mode)"

# ---------- logging ----------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("px4_agent")

log.info("=" * 70)
log.info("🚀 PX4 Agent with SQLite Command Gating (STEP 1)")
log.info(log_msg)
log.info("=" * 70)
log.info(f"Configuration:")
log.info(f"  DRONE_ID: {DRONE_ID}")
log.info(f"  MQTT_HOST: {MQTT_HOST}")
log.info(f"  MQTT_PORT: {MQTT_PORT}")
log.info(f"  MQTT_USER: {MQTT_USER}")
log.info(f"  MQTT_PASS: {'***' if MQTT_PASS else 'NOT SET'}")
log.info(f"  PX4_URL: {PX4_URL}")
log.info(f"  DB_PATH: {DB_PATH}")
log.info("=" * 70)

# ---------- sqlite with thread-safe connection ----------
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Database lock for thread safety
db_lock = threading.Lock()

def get_db_connection():
    """Get thread-safe database connection"""
    return sqlite3.connect(DB_PATH, timeout=10.0)

def init_database():
    """Initialize database schema"""
    with db_lock:
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            
            # Commands table with PRIMARY KEY for duplicate protection
            cur.execute("""CREATE TABLE IF NOT EXISTS commands(
                cmd_id TEXT PRIMARY KEY,
                drone_id TEXT,
                payload TEXT,
                status TEXT,
                created_at TEXT,
                updated_at TEXT
            )""")
            
            # Events table for logging
            cur.execute("""CREATE TABLE IF NOT EXISTS events(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level TEXT,
                msg TEXT,
                meta TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )""")
            
            # Index for faster status queries
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_status 
                ON commands(status, created_at)""")
            
            conn.commit()
            log.info("✅ Database schema initialized")
            
        finally:
            conn.close()

# Initialize database on startup
init_database()

def command_exists(cmd_id):
    """
    STEP 1 GATE: Check if command already exists
    Returns True if command was already processed
    """
    with db_lock:
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM commands WHERE cmd_id = ? LIMIT 1", (cmd_id,))
            exists = cur.fetchone() is not None
            
            if exists:
                log.warning(f"🔒 DUPLICATE command detected: {cmd_id}")
            
            return exists
        finally:
            conn.close()

def db_insert_command(cmd_id, drone_id, payload_json, status="RECEIVED"):
    """
    Insert command into database with duplicate protection
    Returns True if inserted, False if duplicate
    """
    now = datetime.utcnow().isoformat() + "Z"
    
    with db_lock:
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO commands(cmd_id, drone_id, payload, status, created_at, updated_at) 
                   VALUES (?,?,?,?,?,?)""",
                (cmd_id, drone_id, json.dumps(payload_json), status, now, now)
            )
            conn.commit()
            log.info(f"✅ Command {cmd_id} inserted with status={status}")
            return True
            
        except sqlite3.IntegrityError:
            # PRIMARY KEY violation - duplicate cmd_id
            log.warning(f"🔒 Command {cmd_id} already exists - INSERT blocked")
            return False
            
        finally:
            conn.close()

def db_update_status(cmd_id, status):
    """Update command status"""
    now = datetime.utcnow().isoformat() + "Z"
    
    with db_lock:
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE commands SET status=?, updated_at=? WHERE cmd_id=?",
                (status, now, cmd_id)
            )
            conn.commit()
            log.info(f"✅ Command {cmd_id} status updated: {status}")
        finally:
            conn.close()

def db_get_command_status(cmd_id):
    """Get current status of command"""
    with db_lock:
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT status FROM commands WHERE cmd_id = ?", (cmd_id,))
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

def db_log_event(level, msg, meta=None):
    """Log event to database"""
    with db_lock:
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO events(level, msg, meta) VALUES (?,?,?)",
                (level, msg, json.dumps(meta or {}))
            )
            conn.commit()
        finally:
            conn.close()

def db_get_stats():
    """Get database statistics"""
    with db_lock:
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT status, COUNT(*) FROM commands GROUP BY status")
            stats = {row[0]: row[1] for row in cur.fetchall()}
            cur.execute("SELECT COUNT(*) FROM commands")
            stats['TOTAL'] = cur.fetchone()[0]
            return stats
        finally:
            conn.close()

# Log initial database stats
stats = db_get_stats()
log.info(f"📊 Database stats: {stats}")

# ---------- MAVLink ----------
log.info(f"Connecting to PX4 at {PX4_URL} ...")
mav = mavutil.mavlink_connection(PX4_URL)

# Wait for heartbeat to get target system/component IDs
log.info("Waiting for PX4 heartbeat...")
mav.wait_heartbeat(timeout=10)
log.info(f"✓ Heartbeat received: system={mav.target_system}, component={mav.target_component}")

if mav.target_system == 0:
    log.warning("⚠️  Target system is 0, setting to 1")
    mav.target_system = 1
if mav.target_component == 0:
    log.warning("⚠️  Target component is 0, setting to 1")
    mav.target_component = 1

def decode_custom_mode(custom_mode):
    """Decode PX4 custom mode"""
    main_mode = (custom_mode >> 16) & 0xFF
    sub_mode = (custom_mode >> 24) & 0xFF

    main_mode_names = {
        1: "MANUAL",
        2: "ALTCTL",
        3: "POSCTL",
        4: "AUTO",
        5: "ACRO",
        6: "OFFBOARD",
        7: "STABILIZED",
        8: "RATTITUDE"
    }

    auto_sub_mode_names = {
        1: "AUTO_READY",
        2: "AUTO_TAKEOFF",
        3: "AUTO_LOITER",
        4: "AUTO_MISSION",
        5: "AUTO_RTL",
        6: "AUTO_LAND",
        7: "AUTO_RTGS",
        8: "AUTO_FOLLOW_TARGET",
        9: "AUTO_PRECLAND"
    }

    main_name = main_mode_names.get(main_mode, f"UNKNOWN_MAIN_{main_mode}")

    if main_mode == 4:  # AUTO mode
        sub_name = auto_sub_mode_names.get(sub_mode, f"UNKNOWN_SUB_{sub_mode}")
        return f"{main_name}.{sub_name}"
    else:
        return main_name

def send_mavlink_command(cmd_payload):
    """
    Map high-level cmd to MAV_CMD* or set_mode.
    Return dict {success:bool, detail:str, mav_cmd:optional}

    Supported commands:
    - RTL: Return to launch
    - LAND: Land at current position
    - LOITER: Hold position (AUTO.LOITER mode)
    - ARM: Arm motors
    - DISARM: Disarm motors
    - TAKEOFF: Takeoff to altitude
    - SET_MODE: Set custom flight mode
    """
    cmd = cmd_payload.get("cmd")
    params = cmd_payload.get("params", {}) or {}

    try:
        # RTL - Return to Launch
        if cmd == "RTL":
            log.info(f"🏠 Sending RTL command")
            mav.mav.command_long_send(
                mav.target_system,
                mav.target_component,
                mavutil.mavlink.MAV_CMD_NAV_RETURN_TO_LAUNCH,
                0, 0, 0, 0, 0, 0, 0, 0
            )
            return {
                "success": True,
                "detail": "sent RTL",
                "mav_cmd": mavutil.mavlink.MAV_CMD_NAV_RETURN_TO_LAUNCH
            }

        # LAND - Land at current position
        if cmd == "LAND":
            log.info(f"🛬 Sending LAND command")
            mav.mav.command_long_send(
                mav.target_system,
                mav.target_component,
                mavutil.mavlink.MAV_CMD_NAV_LAND,
                0,              # confirmation
                0,              # abort altitude (0 = use default)
                0,              # land mode (0 = default)
                0,              # empty
                float('nan'),   # desired yaw angle (NaN = use current)
                0,              # latitude (0 = current position)
                0,              # longitude (0 = current position)
                0               # altitude (0 = ground level)
            )
            return {
                "success": True,
                "detail": "sent LAND",
                "mav_cmd": mavutil.mavlink.MAV_CMD_NAV_LAND
            }

        # LOITER/HOLD - Use SET_MODE to switch to hold mode
        if cmd in ("LOITER", "HOLD"):
            log.info(f"⏸️  Sending LOITER command (SET_MODE to AUTO.LOITER)")
            # PX4 HOLD mode: custom_mode with AUTO.LOITER
            # custom_mode = (sub_mode << 24) | (main_mode << 16)
            # sub=3 (LOITER), main=4 (AUTO)
            custom_mode = (3 << 24) | (4 << 16)

            mav.mav.set_mode_send(
                mav.target_system,
                mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED,
                custom_mode
            )
            return {
                "success": True,
                "detail": "set mode AUTO.LOITER",
                "mav_cmd": "SET_MODE_LOITER"
            }

        # ARM - Arm the motors
        if cmd == "ARM":
            log.info(f"⚡ Sending ARM command")
            mav.mav.command_long_send(
                mav.target_system,
                mav.target_component,
                mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM,
                0,  # confirmation
                1,  # param1: 1=arm, 0=disarm
                0,  # param2: force (0=normal, 21196=force)
                0, 0, 0, 0, 0  # unused params
            )
            return {
                "success": True,
                "detail": "sent ARM",
                "mav_cmd": mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM
            }

        # DISARM - Disarm the motors
        if cmd == "DISARM":
            log.info(f"🔌 Sending DISARM command")
            mav.mav.command_long_send(
                mav.target_system,
                mav.target_component,
                mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM,
                0,  # confirmation
                0,  # param1: 1=arm, 0=disarm
                0,  # param2: force (0=normal, 21196=force)
                0, 0, 0, 0, 0  # unused params
            )
            return {
                "success": True,
                "detail": "sent DISARM",
                "mav_cmd": mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM
            }

        # TAKEOFF - Takeoff to specified altitude
        if cmd == "TAKEOFF":
            altitude = params.get("altitude", 10.0)
            log.info(f"🚁 Sending TAKEOFF command to {altitude}m")

            mav.mav.command_long_send(
                mav.target_system,
                mav.target_component,
                mavutil.mavlink.MAV_CMD_NAV_TAKEOFF,
                0,              # confirmation
                0,              # param1: minimum pitch (degrees)
                0,              # param2: empty
                0,              # param3: empty
                float('nan'),   # param4: yaw angle (NaN = current)
                0,              # param5: latitude (0 = current)
                0,              # param6: longitude (0 = current)
                altitude        # param7: altitude (meters)
            )
            return {
                "success": True,
                "detail": f"sent TAKEOFF to {altitude}m",
                "mav_cmd": mavutil.mavlink.MAV_CMD_NAV_TAKEOFF
            }

        # SET_MODE - Set custom flight mode
        if cmd == "SET_MODE":
            mode = (params.get("mode") or "").upper()
            log.info(f"🎮 Sending SET_MODE command: {mode}")

            # Mode mapping for PX4
            # Base modes (main_mode)
            mode_map = {
                "MANUAL": 1 << 16,
                "ALTCTL": 2 << 16,
                "POSCTL": 3 << 16,
                "AUTO": 4 << 16,
                "ACRO": 5 << 16,
                "OFFBOARD": 6 << 16,
                "STABILIZED": 7 << 16,
                "RATTITUDE": 8 << 16,
                # AUTO sub-modes (combined)
                "AUTO.READY": (1 << 24) | (4 << 16),
                "AUTO.TAKEOFF": (2 << 24) | (4 << 16),
                "AUTO.LOITER": (3 << 24) | (4 << 16),
                "AUTO.MISSION": (4 << 24) | (4 << 16),
                "AUTO.RTL": (5 << 24) | (4 << 16),
                "AUTO.LAND": (6 << 24) | (4 << 16),
                "HOLD": (3 << 24) | (4 << 16),
            }

            if mode not in mode_map:
                log.warning(f"⚠️  Unknown mode: {mode}")
                return {
                    "success": False,
                    "detail": f"unknown mode {mode}"
                }

            custom_mode = mode_map[mode]
            mav.mav.set_mode_send(
                mav.target_system,
                mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED,
                custom_mode
            )
            return {
                "success": True,
                "detail": f"set_mode {mode}",
                "mav_cmd": "SET_MODE"
            }

        # Unknown command
        log.warning(f"⚠️  Unsupported command: {cmd}")
        return {
            "success": False,
            "detail": f"unsupported cmd {cmd}"
        }

    except Exception as e:
        log.error(f"❌ MAVLink send error: {e}")
        return {
            "success": False,
            "detail": f"mav send error: {e}"
        }


def wait_for_command_ack(timeout=15, target_command=None):
    """Wait for COMMAND_ACK from PX4"""
    start = time.time()
    log.info(f"⏳ Waiting for COMMAND_ACK (command={target_command}, timeout={timeout}s)...")

    ack_count = 0

    while time.time() - start < timeout:
        remaining = timeout - (time.time() - start)
        if remaining <= 0:
            break

        msg = mav.recv_match(type='COMMAND_ACK', blocking=True, timeout=0.1)

        if not msg:
            continue

        ack_count += 1

        try:
            cmd = int(msg.command)
            result = int(msg.result)

            log.info(f"📨 COMMAND_ACK #{ack_count}: command={cmd}, result={result}")

            if target_command is None or cmd == target_command:
                result_map = {
                    0: "MAV_RESULT_ACCEPTED",
                    1: "MAV_RESULT_TEMPORARILY_REJECTED",
                    2: "MAV_RESULT_DENIED",
                    3: "MAV_RESULT_UNSUPPORTED",
                    4: "MAV_RESULT_FAILED",
                    5: "MAV_RESULT_IN_PROGRESS",
                    6: "MAV_RESULT_CANCELLED"
                }
                result_text = result_map.get(result, f"UNKNOWN_{result}")
                success = (result == 0)
                elapsed = time.time() - start
                log.info(f"✅ COMMAND_ACK received after {elapsed:.3f}s: {result_text}")

                return (success, {
                    "detail": f"cmd={cmd} result={result_text}",
                    "result_code": result,
                    "result_text": result_text,
                    "command": cmd,
                    "time_to_ack": f"{elapsed:.3f}s",
                    "verification": "command_ack"
                })

        except Exception as e:
            log.warning(f"Error parsing COMMAND_ACK: {e}")
            continue

    elapsed = time.time() - start
    log.warning(f"⚠️ No COMMAND_ACK after {elapsed:.1f}s (saw {ack_count} other ACKs)")

    # Fallback: check mode
    msg = mav.recv_match(type='HEARTBEAT', blocking=True, timeout=1)
    if msg:
        mode_name = decode_custom_mode(msg.custom_mode)
        log.info(f"Current mode: {mode_name}")

        if "RTL" in mode_name:
            log.info("✅ Mode verification: RTL active despite no ACK")
            return (True, {
                "detail": f"no ACK but mode is {mode_name}",
                "result_code": 0,
                "result_text": "VERIFIED_BY_MODE",
                "verification": "mode_check",
                "current_mode": mode_name
            })

    return (False, {
        "detail": f"no COMMAND_ACK after {timeout}s",
        "result_code": -1,
        "result_text": "TIMEOUT",
        "verification": "failed",
        "acks_seen": ack_count
    })

# ---------- MQTT client ----------
client_id = f"px4-agent-{DRONE_ID}"
client = mqtt.Client(client_id=client_id, clean_session=False)

if MQTT_USER and MQTT_PASS:
    log.info(f"Setting MQTT credentials: user={MQTT_USER}")
    client.username_pw_set(MQTT_USER, MQTT_PASS)
else:
    log.warning("⚠️  MQTT credentials not set - connection may fail!")

cloud_link_ok = True
last_heartbeat = 0

def publish_ack(cmd_id, status, exec_result=None, reason=None):
    topic = f"drone/{DRONE_ID}/cmd_ack"
    msg = {
        "cmd_id": cmd_id,
        "drone_id": DRONE_ID,
        "status": status,
        "ts_utc": datetime.utcnow().isoformat() + "Z"
    }
    if exec_result is not None:
        msg["exec_result"] = exec_result
    if reason:
        msg["reason"] = reason
    client.publish(topic, json.dumps(msg), qos=1)
    log.info(f"📤 Published ACK: status={status}, cmd_id={cmd_id}")

def on_connect(client, userdata, flags, rc):
    log.info(f"🔌 MQTT on_connect called: rc={rc}")

    if rc != 0:
        rc_meanings = {
            1: "Connection refused - incorrect protocol version",
            2: "Connection refused - invalid client identifier",
            3: "Connection refused - server unavailable",
            4: "Connection refused - bad username or password",
            5: "Connection refused - not authorized"
        }
        error_msg = rc_meanings.get(rc, f"Unknown error code {rc}")
        log.error(f"❌ MQTT connection failed: {error_msg}")
        return

    log.info("✅ MQTT connected successfully, subscribing to topics...")

    # Subscribe to command topic
    cmd_topic = f"drone/{DRONE_ID}/cmd"
    log.info(f"📡 Subscribing to: {cmd_topic}")
    result, mid = client.subscribe(cmd_topic, qos=1)
    log.info(f"   Subscribe result: {result}, message_id: {mid}")

    if result == 0:
        log.info(f"✅ Successfully subscribed to {cmd_topic}")
    else:
        log.error(f"❌ Failed to subscribe to {cmd_topic}, result code: {result}")

    if ENABLE_CLOUD_LINK_CHECK:
        link_topic = f"drone/{DRONE_ID}/link"
        log.info(f"📡 Subscribing to: {link_topic}")
        result, mid = client.subscribe(link_topic, qos=1)
        log.info(f"   Subscribe result: {result}, message_id: {mid}")
        log.info(f"✅ Subscribed to heartbeat topic: {link_topic}")
    else:
        log.info("⚠️  Skipped /link subscription (cloud link checking disabled)")

def on_subscribe(client, userdata, mid, granted_qos):
    """Called when subscription is confirmed"""
    log.info(f"🎯 Subscription confirmed: mid={mid}, granted_qos={granted_qos}")

def on_disconnect(client, userdata, rc):
    """Called when disconnected from broker"""
    if rc != 0:
        log.warning(f"⚠️  Unexpected MQTT disconnect: rc={rc}")
    else:
        log.info("✅ MQTT disconnected cleanly")

def on_message(client, userdata, msg):
    """
    STEP 1 CHANGE: Added duplicate command checking before execution
    """
    log.info(f"🔔 on_message CALLED!")
    log.info(f"   Topic: {msg.topic}")
    log.info(f"   Payload length: {len(msg.payload)} bytes")
    log.info(f"   QoS: {msg.qos}")

    global last_heartbeat, cloud_link_ok
    topic = msg.topic

    try:
        payload = msg.payload.decode("utf-8")
        log.debug(f"   Decoded payload: {payload[:100]}...")
    except Exception as e:
        log.error(f"❌ Failed to decode payload: {e}")
        return

    try:
        j = json.loads(payload)
        log.debug(f"   Parsed JSON successfully")
    except Exception as e:
        log.warning(f"⚠️  Invalid JSON payload on {topic}: {e}")
        return

    # Handle heartbeat/link messages
    if topic.endswith("/link"):
        log.info("💓 Heartbeat message received")
        last_heartbeat = time.time()
        if not cloud_link_ok:
            cloud_link_ok = True
            log.info("☁️  Cloud link restored")
        return

    # Handle command messages with STEP 1 GATING
    if topic.endswith("/cmd"):
        log.info("🎮 Command message received!")
        cmd_id = j.get("cmd_id")
        drone_id = j.get("drone_id")
        cmd = j.get("cmd")

        log.info(f"   Command details:")
        log.info(f"     cmd_id: {cmd_id}")
        log.info(f"     drone_id: {drone_id}")
        log.info(f"     cmd: {cmd}")

        if drone_id != DRONE_ID:
            log.info(f"⚠️  Command for another drone ({drone_id} != {DRONE_ID}), ignoring")
            return

        # STEP 1 GATE: Check if command already exists
        if command_exists(cmd_id):
            log.warning(f"🔒 DUPLICATE command blocked: {cmd_id}")
            
            # Still send ACK (idempotent behavior)
            current_status = db_get_command_status(cmd_id)
            publish_ack(cmd_id, current_status or "DUPLICATE")
            
            db_log_event("WARN", "duplicate_command_blocked", {
                "cmd_id": cmd_id,
                "cmd": cmd
            })
            return

        # Cloud link check (if enabled)
        if ENABLE_CLOUD_LINK_CHECK:
            if time.time() - last_heartbeat > HEARTBEAT_TIMEOUT:
                cloud_link_ok = False

            if not cloud_link_ok:
                log.warning(f"⚠️  Cloud link down: rejecting cmd {cmd_id}")
                publish_ack(cmd_id, "NACK", reason="link_down")
                db_log_event("WARN", "rejected_cmd_link_down", {"cmd_id": cmd_id})
                return
        else:
            log.debug("Cloud link check disabled - accepting command")

        log.info(f"✅ Command accepted, processing: {cmd_id}")

        # STEP 1: Insert into database BEFORE execution
        success = db_insert_command(cmd_id, DRONE_ID, j, status="RECEIVED")
        if not success:
            log.error(f"❌ DB insert failed (race condition?) - command may have been processed")
            # Don't execute if insert failed
            return

        log.info(f"✅ Command stored in database")

        # Publish ACK
        publish_ack(cmd_id, "ACK")
        
        # Execute command in separate thread
        log.info(f"🚀 Starting command execution thread")
        threading.Thread(target=process_command, args=(j,), daemon=True).start()
    else:
        log.warning(f"⚠️  Message on unexpected topic: {topic}")

def process_command(j):
    """Execute command and send result"""
    cmd_id = j.get("cmd_id")
    log.info(f"⚙️  Processing command: {cmd_id}")

    try:
        res = send_mavlink_command(j)
        log.info(f"   MAVLink send result: {res}")

        if not res.get("success"):
            log.error(f"❌ MAVLink send failed: {res.get('detail')}")
            db_update_status(cmd_id, "FAILED")
            publish_ack(cmd_id, "NACK", exec_result=res, reason="send_failed")
            db_log_event("ERROR", "send_failed", {"cmd_id":cmd_id, "detail":res})
            return

        mav_cmd = res.get("mav_cmd")
        target = mav_cmd if isinstance(mav_cmd, int) else None

        acked, result_dict = wait_for_command_ack(timeout=15, target_command=target)

        if acked:
            log.info(f"✅ Command executed successfully: {cmd_id}")
            db_update_status(cmd_id, "EXECUTED")
            publish_ack(cmd_id, "EXECUTED", exec_result=result_dict)
            db_log_event("INFO", "cmd_executed", {"cmd_id": cmd_id, "result": result_dict})
        else:
            if result_dict.get("verification") == "mode_check":
                log.info(f"✅ Command verified by mode check: {cmd_id}")
                db_update_status(cmd_id, "EXECUTED")
                publish_ack(cmd_id, "EXECUTED", exec_result=result_dict)
                db_log_event("INFO", "cmd_executed_verified", {"cmd_id": cmd_id, "result": result_dict})
            else:
                log.warning(f"⚠️  Command failed (no ACK): {cmd_id}")
                db_update_status(cmd_id, "FAILED")
                publish_ack(cmd_id, "NACK", exec_result=result_dict, reason="no_ack")
                db_log_event("WARN", "no_command_ack", {"cmd_id": cmd_id, "result": result_dict})

    except Exception as e:
        log.exception(f"❌ Exception executing command {cmd_id}")
        db_update_status(cmd_id, "FAILED")
        publish_ack(cmd_id, "NACK", reason=str(e))
        db_log_event("ERROR", "exec_exception", {"cmd_id": cmd_id, "err": str(e)})

def heartbeat_watcher():
    """Monitor cloud link heartbeat"""
    global cloud_link_ok

    if not ENABLE_CLOUD_LINK_CHECK:
        log.info("Heartbeat watcher disabled (cloud link checking off)")
        return

    while True:
        if time.time() - last_heartbeat > HEARTBEAT_TIMEOUT:
            if cloud_link_ok:
                cloud_link_ok = False
                log.warning("💔 Heartbeat lost, marking cloud_link_ok = False")
                db_log_event("WARN", "heartbeat_lost", {"since": datetime.utcnow().isoformat()+"Z"})
        time.sleep(1)

# ---------- start ----------
log.info("🚀 Starting PX4 Agent with SQLite Gating...")
log.info(f"   Client ID: {client_id}")
log.info(f"   Connecting to MQTT broker: {MQTT_HOST}:{MQTT_PORT}")

client.on_connect = on_connect
client.on_subscribe = on_subscribe
client.on_disconnect = on_disconnect
client.on_message = on_message

try:
    log.info("📞 Calling client.connect()...")
    client.connect(MQTT_HOST, MQTT_PORT)
    log.info("✅ connect() returned successfully")
except Exception as e:
    log.error(f"❌ connect() failed: {e}")
    log.error(f"   Check that SSH tunnel is running and port {MQTT_PORT} is accessible")
    raise

threading.Thread(target=heartbeat_watcher, daemon=True).start()
log.info(f"✅ PX4 agent listening as {client_id} on mqtt {MQTT_HOST}:{MQTT_PORT}")
log.info(f"")
log.info(f"📋 Supported commands:")
log.info(f"   RTL      - Return to launch")
log.info(f"   LAND     - Land at current position")
log.info(f"   LOITER   - Hold position (AUTO.LOITER mode)")
log.info(f"   ARM      - Arm motors")
log.info(f"   DISARM   - Disarm motors")
log.info(f"   TAKEOFF  - Takeoff to altitude")
log.info(f"   SET_MODE - Set custom flight mode")
log.info(f"")
log.info(f"🔒 STEP 1: SQLite command gating ACTIVE")
log.info(f"   - Duplicate commands will be blocked")
log.info(f"   - Agent restart will not re-execute commands")
log.info(f"")
log.info("🔄 Starting MQTT loop_forever()...")
client.loop_forever()