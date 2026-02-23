#!/usr/bin/env python3
"""
px4_agent.py - Compatible with both Lambda and legacy payload formats
UPDATED: Accepts both formats:
  - Lambda: {target_id, action, command_id, params}
  - Legacy: {drone_id, cmd, cmd_id, params}
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
PX4_URL = os.environ.get("PX4_URL", "udpout:127.0.0.1:14550")
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
log.info("🚀 PX4 Agent - Lambda & Legacy Payload Compatible")
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
db_lock = threading.Lock()

def get_db_connection():
    return sqlite3.connect(DB_PATH, timeout=10.0)

def init_database():
    with db_lock:
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("""CREATE TABLE IF NOT EXISTS commands(
                cmd_id TEXT PRIMARY KEY,
                drone_id TEXT,
                payload TEXT,
                status TEXT,
                created_at TEXT,
                updated_at TEXT
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS events(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level TEXT,
                msg TEXT,
                meta TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_status
                ON commands(status, created_at)""")
            conn.commit()
            log.info("✅ Database schema initialized")
        finally:
            conn.close()

init_database()

def command_exists(cmd_id):
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
            log.warning(f"🔒 Command {cmd_id} already exists - INSERT blocked")
            return False
        finally:
            conn.close()

def db_update_status(cmd_id, status):
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

stats = db_get_stats()
log.info(f"📊 Database stats: {stats}")

# ---------- MAVLink ----------
log.info(f"Connecting to PX4 at {PX4_URL} ...")
mav = mavutil.mavlink_connection(PX4_URL)
log.info("Waiting for PX4 heartbeat...")
mav.wait_heartbeat(timeout=10)
log.info(f"✓ Heartbeat received: system={mav.target_system}, component={mav.target_component}")

if mav.target_system == 0:
    log.warning("⚠️  Target system is 0, setting to 1")
    mav.target_system = 1
if mav.target_component == 0:
    log.warning("⚠️  Target component is 0, setting to 1")
    mav.target_component = 1

def send_mavlink_command(cmd_payload):
    """
    Accepts both formats:
    - Legacy: {"cmd": "RTL", "params": {}}
    - Lambda: {"action": "rtl", "params": {}}
    """
    # Support both 'cmd' (legacy) and 'action' (Lambda)
    cmd = cmd_payload.get("cmd") or cmd_payload.get("action", "")
    cmd = cmd.upper()  # Normalize to uppercase
    params = cmd_payload.get("params", {}) or {}

    try:
        if cmd == "RTL":
            log.info(f"🏠 Sending RTL command")
            mav.mav.command_long_send(
                mav.target_system, mav.target_component,
                mavutil.mavlink.MAV_CMD_NAV_RETURN_TO_LAUNCH,
                0, 0, 0, 0, 0, 0, 0, 0
            )
            return {"success": True, "detail": "sent RTL",
                    "mav_cmd": mavutil.mavlink.MAV_CMD_NAV_RETURN_TO_LAUNCH}

        if cmd == "LAND":
            log.info(f"🛬 Sending LAND command")
            mav.mav.command_long_send(
                mav.target_system, mav.target_component,
                mavutil.mavlink.MAV_CMD_NAV_LAND,
                0, 0, 0, 0, float('nan'), 0, 0, 0
            )
            return {"success": True, "detail": "sent LAND",
                    "mav_cmd": mavutil.mavlink.MAV_CMD_NAV_LAND}

        if cmd in ("LOITER", "HOLD"):
            log.info(f"⏸️  Sending LOITER command")
            custom_mode = (3 << 24) | (4 << 16)
            mav.mav.set_mode_send(
                mav.target_system,
                mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED,
                custom_mode
            )
            return {"success": True, "detail": "set mode AUTO.LOITER",
                    "mav_cmd": "SET_MODE_LOITER"}

        if cmd == "ARM":
            log.info(f"⚡ Sending ARM command")
            mav.mav.command_long_send(
                mav.target_system, mav.target_component,
                mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM,
                0, 1, 0, 0, 0, 0, 0, 0
            )
            return {"success": True, "detail": "sent ARM",
                    "mav_cmd": mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM}

        if cmd == "DISARM":
            log.info(f"🔌 Sending DISARM command")
            mav.mav.command_long_send(
                mav.target_system, mav.target_component,
                mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM,
                0, 0, 0, 0, 0, 0, 0, 0
            )
            return {"success": True, "detail": "sent DISARM",
                    "mav_cmd": mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM}

        if cmd == "TAKEOFF":
            altitude = params.get("altitude", 10.0)
            log.info(f"🚁 Sending TAKEOFF to {altitude}m")
            mav.mav.command_long_send(
                mav.target_system, mav.target_component,
                mavutil.mavlink.MAV_CMD_NAV_TAKEOFF,
                0, 0, 0, 0, float('nan'), 0, 0, altitude
            )
            return {"success": True, "detail": f"sent TAKEOFF to {altitude}m",
                    "mav_cmd": mavutil.mavlink.MAV_CMD_NAV_TAKEOFF}

        log.warning(f"⚠️  Unsupported command: {cmd}")
        return {"success": False, "detail": f"unsupported cmd {cmd}"}

    except Exception as e:
        log.error(f"❌ MAVLink send error: {e}")
        return {"success": False, "detail": f"mav send error: {e}"}

def wait_for_command_ack(timeout=15, target_command=None):
    start = time.time()
    log.info(f"⏳ Waiting for COMMAND_ACK (timeout={timeout}s)...")
    
    while time.time() - start < timeout:
        msg = mav.recv_match(type='COMMAND_ACK', blocking=True, timeout=0.1)
        if not msg:
            continue
        
        try:
            cmd = int(msg.command)
            result = int(msg.result)
            if target_command is None or cmd == target_command:
                result_map = {
                    0: "ACCEPTED", 1: "TEMPORARILY_REJECTED", 2: "DENIED",
                    3: "UNSUPPORTED", 4: "FAILED", 5: "IN_PROGRESS", 6: "CANCELLED"
                }
                result_text = result_map.get(result, f"UNKNOWN_{result}")
                success = (result == 0)
                elapsed = time.time() - start
                log.info(f"✅ COMMAND_ACK: {result_text} after {elapsed:.3f}s")
                return (success, {
                    "detail": f"cmd={cmd} result={result_text}",
                    "result_code": result,
                    "result_text": result_text,
                    "verification": "command_ack"
                })
        except Exception as e:
            log.warning(f"Error parsing ACK: {e}")
            continue
    
    log.warning(f"⚠️ No COMMAND_ACK after {timeout}s")
    return (False, {
        "detail": f"no ACK after {timeout}s",
        "result_code": -1,
        "result_text": "TIMEOUT",
        "verification": "failed"
    })

# ---------- MQTT client ----------
client_id = f"px4-agent-{DRONE_ID}"
client = mqtt.Client(client_id=client_id, clean_session=False)

if MQTT_USER and MQTT_PASS:
    log.info(f"Setting MQTT credentials: user={MQTT_USER}")
    client.username_pw_set(MQTT_USER, MQTT_PASS)
else:
    log.warning("⚠️  MQTT credentials not set")

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
    log.info(f"🔌 MQTT on_connect: rc={rc}")
    if rc != 0:
        log.error(f"❌ Connection failed: rc={rc}")
        return
    
    log.info("✅ MQTT connected, subscribing...")
    cmd_topic = f"drone/{DRONE_ID}/cmd"
    client.subscribe(cmd_topic, qos=1)
    log.info(f"✅ Subscribed to {cmd_topic}")

def on_message(client, userdata, msg):
    """
    UPDATED: Accepts both Lambda and legacy payload formats
    """
    log.info(f"🔔 Message received on {msg.topic}")
    
    global last_heartbeat, cloud_link_ok
    topic = msg.topic

    try:
        payload = msg.payload.decode("utf-8")
        j = json.loads(payload)
        log.info(f"   Payload: {payload}")
    except Exception as e:
        log.error(f"❌ Invalid payload: {e}")
        return

    if topic.endswith("/link"):
        last_heartbeat = time.time()
        if not cloud_link_ok:
            cloud_link_ok = True
            log.info("☁️  Cloud link restored")
        return

    if topic.endswith("/cmd"):
        # ✅ Support both field name formats
        cmd_id = j.get("cmd_id") or j.get("command_id")
        drone_id = j.get("drone_id") or j.get("target_id")
        cmd = j.get("cmd") or j.get("action", "")
        cmd = cmd.upper()

        log.info(f"   cmd_id: {cmd_id}")
        log.info(f"   drone_id: {drone_id}")
        log.info(f"   cmd: {cmd}")
        log.info(f"   format: {'Lambda' if 'action' in j else 'Legacy'}")

        if not cmd_id or not drone_id:
            log.warning(f"⚠️  Missing required fields")
            return

        if drone_id != DRONE_ID:
            log.info(f"⚠️  Wrong drone ({drone_id} != {DRONE_ID})")
            return

        if command_exists(cmd_id):
            log.warning(f"🔒 DUPLICATE blocked: {cmd_id}")
            current_status = db_get_command_status(cmd_id)
            publish_ack(cmd_id, current_status or "DUPLICATE")
            return

        if ENABLE_CLOUD_LINK_CHECK and not cloud_link_ok:
            log.warning(f"⚠️  Cloud link down, rejecting {cmd_id}")
            publish_ack(cmd_id, "NACK", reason="link_down")
            return

        log.info(f"✅ Command accepted: {cmd_id}")

        success = db_insert_command(cmd_id, DRONE_ID, j, status="RECEIVED")
        if not success:
            log.error(f"❌ DB insert failed")
            return

        publish_ack(cmd_id, "ACK")
        
        threading.Thread(target=process_command, args=(j,), daemon=True).start()

def process_command(j):
    cmd_id = j.get("cmd_id") or j.get("command_id")
    log.info(f"⚙️  Processing: {cmd_id}")

    try:
        res = send_mavlink_command(j)
        
        if not res.get("success"):
            log.error(f"❌ MAVLink send failed")
            db_update_status(cmd_id, "FAILED")
            publish_ack(cmd_id, "NACK", exec_result=res, reason="send_failed")
            return

        mav_cmd = res.get("mav_cmd")
        target = mav_cmd if isinstance(mav_cmd, int) else None
        acked, result_dict = wait_for_command_ack(timeout=15, target_command=target)

        if acked:
            log.info(f"✅ Executed: {cmd_id}")
            db_update_status(cmd_id, "EXECUTED")
            publish_ack(cmd_id, "EXECUTED", exec_result=result_dict)
        else:
            log.warning(f"⚠️  Failed: {cmd_id}")
            db_update_status(cmd_id, "FAILED")
            publish_ack(cmd_id, "NACK", exec_result=result_dict, reason="no_ack")

    except Exception as e:
        log.exception(f"❌ Exception: {cmd_id}")
        db_update_status(cmd_id, "FAILED")
        publish_ack(cmd_id, "NACK", reason=str(e))

def heartbeat_watcher():
    global cloud_link_ok
    if not ENABLE_CLOUD_LINK_CHECK:
        return
    while True:
        if time.time() - last_heartbeat > HEARTBEAT_TIMEOUT:
            if cloud_link_ok:
                cloud_link_ok = False
                log.warning("💔 Heartbeat lost")
        time.sleep(1)

# ---------- start ----------
log.info("🚀 Starting PX4 Agent...")
log.info(f"   Client ID: {client_id}")
log.info(f"   Broker: {MQTT_HOST}:{MQTT_PORT}")

client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(MQTT_HOST, MQTT_PORT)
    log.info("✅ Connected to MQTT broker")
except Exception as e:
    log.error(f"❌ Connection failed: {e}")
    raise

threading.Thread(target=heartbeat_watcher, daemon=True).start()

log.info(f"")
log.info(f"📋 Supported commands: RTL, LAND, LOITER, ARM, DISARM, TAKEOFF")
log.info(f"📡 Accepts both payload formats:")
log.info(f"   Lambda: {{target_id, action, command_id, params}}")
log.info(f"   Legacy: {{drone_id, cmd, cmd_id, params}}")
log.info(f"🔒 SQLite command gating ACTIVE")
log.info(f"")
log.info("🔄 Starting MQTT loop...")
client.loop_forever()