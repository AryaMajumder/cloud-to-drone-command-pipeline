#!/usr/bin/env python3
"""
px4_agent.py — Merged agent

Combines:
  NEW: GCS heartbeat thread, udpin:0.0.0.0:14556, source_system=255,
       single MAVLink sender thread, correct TAKEOFF sequence (mode→arm→NAV_TAKEOFF),
       DO_REPOSITION for LOITER, AMSL altitude calculation.

  OLD: MQTT credentials (MQTT_USER/MQTT_PASS + credential file fallback),
       dual payload format (cmd_id/command_id, drone_id/target_id, cmd/action),
       duplicate command detection, per-connection thread-safe SQLite.

Supported commands: RTL, LAND, LOITER, HOLD, ARM, DISARM, TAKEOFF, SET_MODE
Payload formats accepted:
  Legacy: {"cmd_id": "...", "drone_id": "...", "cmd": "RTL", "params": {}}
  Lambda: {"command_id": "...", "target_id": "...", "action": "rtl", "params": {}}
"""

import os
import time
import json
import queue
import sqlite3
import threading
import logging
from datetime import datetime
import paho.mqtt.client as mqtt
from pymavlink import mavutil

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
DRONE_ID    = os.environ.get("DRONE_ID",    "drone-01")
MQTT_HOST   = os.environ.get("MQTT_HOST",   "127.0.0.1")
MQTT_PORT   = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER   = os.environ.get("MQTT_USER",   "drone")
MQTT_PASS   = os.environ.get("MQTT_PASS",   "")
PX4_URL     = os.environ.get("PX4_URL",     "udpin:0.0.0.0:14556")
DB_PATH     = os.environ.get("DB_PATH",     "/var/lib/px4_agent/agent.db")
HEARTBEAT_TIMEOUT     = int(os.environ.get("HEARTBEAT_TIMEOUT", "10"))
LOG_LEVEL             = os.environ.get("LOG_LEVEL",   "INFO")
ENABLE_CLOUD_LINK_CHECK = os.environ.get(
    "ENABLE_CLOUD_LINK_CHECK", "false").lower() in ("true", "1", "yes")

# Read MQTT password from credential file if not in env
if not MQTT_PASS:
    try:
        for path in [
            '/etc/mqtt-creds/mosquitto_drone_credentials.txt',
            '/root/mosquitto_drone_credentials.txt'
        ]:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    for line in f:
                        if line.startswith(f'{MQTT_USER}:'):
                            MQTT_PASS = line.strip().split(':', 1)[1]
                            break
                if MQTT_PASS:
                    break
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=LOG_LEVEL,
                    format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("px4_agent")

log.info("=" * 70)
log.info("PX4 Agent — Merged (cloud + PX4 fixes)")
log.info("Cloud link check: %s", "ENABLED" if ENABLE_CLOUD_LINK_CHECK else "DISABLED")
log.info("=" * 70)
log.info("DRONE_ID  : %s", DRONE_ID)
log.info("MQTT      : %s:%s  user=%s  pass=%s",
         MQTT_HOST, MQTT_PORT, MQTT_USER, "***" if MQTT_PASS else "NOT SET")
log.info("PX4_URL   : %s", PX4_URL)
log.info("DB_PATH   : %s", DB_PATH)
log.info("=" * 70)

# ─────────────────────────────────────────────────────────────────────────────
# SQLite — per-call connections, thread-safe via lock
# ─────────────────────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
db_lock = threading.Lock()

def _db():
    return sqlite3.connect(DB_PATH, timeout=10.0)

def _init_db():
    with db_lock:
        conn = _db()
        try:
            c = conn.cursor()
            c.execute("""CREATE TABLE IF NOT EXISTS commands(
                cmd_id     TEXT PRIMARY KEY,
                drone_id   TEXT,
                payload    TEXT,
                status     TEXT,
                created_at TEXT,
                updated_at TEXT
            )""")
            c.execute("""CREATE TABLE IF NOT EXISTS events(
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                level      TEXT,
                msg        TEXT,
                meta       TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )""")
            c.execute("""CREATE INDEX IF NOT EXISTS idx_status
                ON commands(status, created_at)""")
            conn.commit()
            log.info("DB initialised")
        finally:
            conn.close()

_init_db()

def command_exists(cmd_id):
    with db_lock:
        conn = _db()
        try:
            row = conn.execute(
                "SELECT 1 FROM commands WHERE cmd_id=? LIMIT 1", (cmd_id,)
            ).fetchone()
            if row:
                log.warning("DUPLICATE command: %s", cmd_id)
            return row is not None
        finally:
            conn.close()

def db_insert_command(cmd_id, drone_id, payload_json, status="RECEIVED"):
    now = datetime.utcnow().isoformat() + "Z"
    with db_lock:
        conn = _db()
        try:
            conn.execute(
                "INSERT INTO commands(cmd_id,drone_id,payload,status,created_at,updated_at)"
                " VALUES (?,?,?,?,?,?)",
                (cmd_id, drone_id, json.dumps(payload_json), status, now, now)
            )
            conn.commit()
            log.info("DB insert: %s  status=%s", cmd_id, status)
            return True
        except sqlite3.IntegrityError:
            log.warning("DB insert blocked (already exists): %s", cmd_id)
            return False
        finally:
            conn.close()

def db_update_status(cmd_id, status):
    now = datetime.utcnow().isoformat() + "Z"
    with db_lock:
        conn = _db()
        try:
            conn.execute(
                "UPDATE commands SET status=?,updated_at=? WHERE cmd_id=?",
                (status, now, cmd_id)
            )
            conn.commit()
            log.info("DB update: %s -> %s", cmd_id, status)
        finally:
            conn.close()

def db_get_status(cmd_id):
    with db_lock:
        conn = _db()
        try:
            row = conn.execute(
                "SELECT status FROM commands WHERE cmd_id=?", (cmd_id,)
            ).fetchone()
            return row[0] if row else None
        finally:
            conn.close()

def db_log_event(level, msg, meta=None):
    with db_lock:
        conn = _db()
        try:
            conn.execute(
                "INSERT INTO events(level,msg,meta) VALUES (?,?,?)",
                (level, msg, json.dumps(meta or {}))
            )
            conn.commit()
        finally:
            conn.close()

with db_lock:
    conn = _db()
    try:
        rows = conn.execute(
            "SELECT status, COUNT(*) FROM commands GROUP BY status"
        ).fetchall()
        log.info("DB stats: %s", {r[0]: r[1] for r in rows})
    finally:
        conn.close()

# ─────────────────────────────────────────────────────────────────────────────
# MAVLink connection
# ─────────────────────────────────────────────────────────────────────────────
log.info("Connecting to PX4 at %s ...", PX4_URL)
mav = mavutil.mavlink_connection(
    PX4_URL,
    source_system=255,
    source_component=190
)
log.info("Waiting for PX4 heartbeat ...")
mav.wait_heartbeat()
log.info("Heartbeat OK — target_system=%s target_component=%s",
         mav.target_system, mav.target_component)

if mav.target_system == 0:
    log.warning("target_system=0, forcing to 1")
    mav.target_system = 1
if mav.target_component == 0:
    mav.target_component = 1

# ─────────────────────────────────────────────────────────────────────────────
# Single MAVLink sender thread
# ALL mav.mav.* calls must be enqueued here to avoid C-level race conditions
# ─────────────────────────────────────────────────────────────────────────────
_mav_queue = queue.Queue()

def _mav_sender():
    while True:
        fn = _mav_queue.get()
        try:
            fn()
        except Exception as e:
            log.error("mav_sender error: %s", e)

threading.Thread(target=_mav_sender, daemon=True, name="mav-sender").start()

def mav_send(fn):
    """Enqueue a zero-argument callable onto the MAVLink sender thread."""
    _mav_queue.put(fn)

# ─────────────────────────────────────────────────────────────────────────────
# GCS heartbeat — prevents gcs_connection_lost=True in PX4
# Without this PX4 refuses to arm
# ─────────────────────────────────────────────────────────────────────────────
def _gcs_heartbeat_loop():
    log.info("GCS heartbeat thread started (1 Hz)")
    while True:
        mav_send(lambda: mav.mav.heartbeat_send(
            mavutil.mavlink.MAV_TYPE_GCS,
            mavutil.mavlink.MAV_AUTOPILOT_INVALID,
            0, 0, 0
        ))
        time.sleep(1)

threading.Thread(target=_gcs_heartbeat_loop, daemon=True, name="gcs-hb").start()

# ─────────────────────────────────────────────────────────────────────────────
# MAVLink helpers
# ─────────────────────────────────────────────────────────────────────────────
MAV_RESULT = {
    0: "ACCEPTED", 1: "TEMPORARILY_REJECTED", 2: "DENIED",
    3: "UNSUPPORTED", 4: "FAILED", 5: "IN_PROGRESS", 6: "CANCELLED",
}

def wait_ack(label, target_cmd=None, timeout=10):
    start = time.time()
    while time.time() - start < timeout:
        msg = mav.recv_match(type="COMMAND_ACK", blocking=True, timeout=0.2)
        if not msg:
            continue
        cmd    = int(msg.command)
        result = int(msg.result)
        text   = MAV_RESULT.get(result, f"UNKNOWN_{result}")
        log.info("ACK cmd=%s result=%s(%s)", cmd, result, text)
        if target_cmd is None or cmd == target_cmd:
            return result, text
        log.debug("(ACK for cmd=%s, still waiting for cmd=%s)", cmd, target_cmd)
    log.warning("ACK TIMEOUT for '%s' after %ss", label, timeout)
    return None, "TIMEOUT"

def get_home_alt_amsl(timeout=5.0):
    mav_send(lambda: mav.mav.command_long_send(
        mav.target_system, mav.target_component,
        mavutil.mavlink.MAV_CMD_GET_HOME_POSITION,
        0, 0, 0, 0, 0, 0, 0, 0
    ))
    start = time.time()
    while time.time() - start < timeout:
        msg = mav.recv_match(type="HOME_POSITION", blocking=True, timeout=0.5)
        if msg:
            return msg.altitude / 1000.0
    return None

# ─────────────────────────────────────────────────────────────────────────────
# Command implementations
# ─────────────────────────────────────────────────────────────────────────────

def _cmd_rtl():
    mav_send(lambda: mav.mav.command_long_send(
        mav.target_system, mav.target_component,
        mavutil.mavlink.MAV_CMD_NAV_RETURN_TO_LAUNCH,
        0, 0, 0, 0, 0, 0, 0, 0
    ))
    res, text = wait_ack("RTL", mavutil.mavlink.MAV_CMD_NAV_RETURN_TO_LAUNCH)
    return {"success": res == 0, "detail": f"RTL {text}"}


def _cmd_land():
    mav_send(lambda: mav.mav.command_long_send(
        mav.target_system, mav.target_component,
        mavutil.mavlink.MAV_CMD_NAV_LAND,
        0, 0, 0, 0, float('nan'), 0, 0, 0
    ))
    res, text = wait_ack("LAND", mavutil.mavlink.MAV_CMD_NAV_LAND)
    return {"success": res == 0, "detail": f"LAND {text}"}


def _cmd_arm():
    mav_send(lambda: mav.mav.command_long_send(
        mav.target_system, mav.target_component,
        mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM,
        0, 1, 0, 0, 0, 0, 0, 0
    ))
    res, text = wait_ack("ARM", mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM)
    return {"success": res == 0, "detail": f"ARM {text}"}


def _cmd_disarm():
    mav_send(lambda: mav.mav.command_long_send(
        mav.target_system, mav.target_component,
        mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM,
        0, 0, 0, 0, 0, 0, 0, 0
    ))
    res, text = wait_ack("DISARM", mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM)
    return {"success": res == 0, "detail": f"DISARM {text}"}


def _cmd_loiter():
    """DO_REPOSITION with NaN lat/lon/alt = hold current position. Proven working."""
    mav_send(lambda: mav.mav.command_long_send(
        mav.target_system, mav.target_component,
        mavutil.mavlink.MAV_CMD_DO_REPOSITION,
        0,
        -1,  # speed = keep current
        1,   # MAV_DO_REPOSITION_FLAGS_CHANGE_MODE
        0, 0,
        float("nan"), float("nan"), float("nan")
    ))
    res, text = wait_ack("LOITER", mavutil.mavlink.MAV_CMD_DO_REPOSITION)
    return {"success": res == 0, "detail": f"LOITER {text}"}


def _cmd_takeoff(params):
    """
    Proven sequence: mode → arm → NAV_TAKEOFF (AMSL altitude).
    Accepts alt_agl or altitude param key.
    """
    alt_agl = float(params.get("alt_agl") or params.get("altitude") or 10.0)

    home_alt = get_home_alt_amsl()
    if home_alt is None:
        log.warning("HOME_POSITION unavailable — using alt_agl directly")
        target_amsl = alt_agl
    else:
        target_amsl = home_alt + alt_agl
        log.info("home=%.1fm + agl=%.1fm = %.1fm AMSL", home_alt, alt_agl, target_amsl)

    # step 1: AUTO.TAKEOFF mode before arming
    log.info("Switching to AUTO.TAKEOFF mode ...")
    mav_send(lambda: mav.mav.command_long_send(
        mav.target_system, mav.target_component,
        mavutil.mavlink.MAV_CMD_DO_SET_MODE,
        0,
        mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED,
        4, 2, 0, 0, 0, 0   # main=AUTO(4), sub=TAKEOFF(2)
    ))
    res, text = wait_ack("DO_SET_MODE AUTO.TAKEOFF",
                         mavutil.mavlink.MAV_CMD_DO_SET_MODE, timeout=8)
    if res not in (0, 5):
        log.warning("DO_SET_MODE returned %s — proceeding anyway", text)
    time.sleep(0.5)

    # step 2: arm
    log.info("Sending ARM ...")
    mav_send(lambda: mav.mav.command_long_send(
        mav.target_system, mav.target_component,
        mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM,
        0, 1, 0, 0, 0, 0, 0, 0
    ))
    res, text = wait_ack("ARM", mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM, timeout=10)
    if res != 0:
        return {"success": False,
                "detail": f"ARM failed: {text} — ensure GCS heartbeat active"}
    time.sleep(1.0)

    # step 3: NAV_TAKEOFF
    log.info("Sending NAV_TAKEOFF to %.1fm AMSL ...", target_amsl)
    _amsl = target_amsl
    mav_send(lambda: mav.mav.command_long_send(
        mav.target_system, mav.target_component,
        mavutil.mavlink.MAV_CMD_NAV_TAKEOFF,
        0, 0, 0, 0, 0,
        float("nan"), float("nan"),
        _amsl
    ))
    res, text = wait_ack("NAV_TAKEOFF", mavutil.mavlink.MAV_CMD_NAV_TAKEOFF, timeout=10)
    return {
        "success": res == 0,
        "detail": f"TAKEOFF climbing to {target_amsl:.1f}m AMSL" if res == 0
                  else f"NAV_TAKEOFF {text} (code={res})"
    }


def _cmd_set_mode(params):
    mode = ((params or {}).get("mode") or "").upper()
    mode_map = {
        "MANUAL":       (1, 0),
        "POSCTL":       (3, 0),
        "OFFBOARD":     (6, 0),
        "HOLD":         (4, 3),
        "AUTO.MISSION": (4, 4),
        "AUTO.RTL":     (4, 5),
        "AUTO.LAND":    (4, 6),
    }
    if mode not in mode_map:
        return {"success": False, "detail": f"unknown mode '{mode}'"}
    main, sub = mode_map[mode]
    mav_send(lambda: mav.mav.command_long_send(
        mav.target_system, mav.target_component,
        mavutil.mavlink.MAV_CMD_DO_SET_MODE,
        0,
        mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED,
        main, sub, 0, 0, 0, 0
    ))
    res, text = wait_ack(f"SET_MODE {mode}", mavutil.mavlink.MAV_CMD_DO_SET_MODE)
    return {"success": res == 0, "detail": f"SET_MODE {mode} {text}"}


def send_mavlink_command(cmd_payload):
    cmd    = (cmd_payload.get("cmd") or cmd_payload.get("action") or "").upper()
    params = cmd_payload.get("params") or {}
    log.info("Executing: cmd=%s  params=%s", cmd, params)
    try:
        if cmd == "RTL":               return _cmd_rtl()
        if cmd in ("LOITER", "HOLD"):  return _cmd_loiter()
        if cmd == "TAKEOFF":           return _cmd_takeoff(params)
        if cmd == "LAND":              return _cmd_land()
        if cmd == "ARM":               return _cmd_arm()
        if cmd == "DISARM":            return _cmd_disarm()
        if cmd == "SET_MODE":          return _cmd_set_mode(params)
        log.warning("Unsupported command: %s", cmd)
        return {"success": False, "detail": f"unsupported cmd: {cmd}"}
    except Exception as e:
        log.exception("Exception in send_mavlink_command")
        return {"success": False, "detail": f"exception: {e}"}

# ─────────────────────────────────────────────────────────────────────────────
# MQTT
# ─────────────────────────────────────────────────────────────────────────────
mqtt_client = mqtt.Client(
    client_id=f"px4-agent-{DRONE_ID}",
    clean_session=False
)

if MQTT_USER and MQTT_PASS:
    log.info("MQTT credentials: user=%s", MQTT_USER)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
else:
    log.warning("MQTT credentials NOT set — broker may reject connection")

cloud_link_ok  = True
last_heartbeat = 0.0

def publish_ack(cmd_id, status, exec_result=None, reason=None):
    msg = {
        "cmd_id":   cmd_id,
        "drone_id": DRONE_ID,
        "status":   status,
        "ts_utc":   datetime.utcnow().isoformat() + "Z",
    }
    if exec_result is not None:
        msg["exec_result"] = exec_result
    if reason:
        msg["reason"] = reason
    mqtt_client.publish(f"drone/{DRONE_ID}/cmd_ack", json.dumps(msg), qos=1)
    log.info("Published ACK: status=%s  cmd_id=%s", status, cmd_id)

def on_connect(client, userdata, flags, rc):
    log.info("MQTT on_connect rc=%s", rc)
    if rc != 0:
        log.error("MQTT connection failed rc=%s", rc)
        return
    client.subscribe(f"drone/{DRONE_ID}/cmd", qos=1)
    log.info("Subscribed to drone/%s/cmd", DRONE_ID)
    if ENABLE_CLOUD_LINK_CHECK:
        client.subscribe(f"drone/{DRONE_ID}/link", qos=1)
        log.info("Subscribed to drone/%s/link", DRONE_ID)

def on_message(client, userdata, msg):
    global last_heartbeat, cloud_link_ok
    topic = msg.topic
    try:
        payload = msg.payload.decode("utf-8")
        j = json.loads(payload)
        log.info("Message on %s: %s", topic, payload)
    except Exception as e:
        log.error("Invalid payload on %s: %s", topic, e)
        return

    if topic.endswith("/link"):
        last_heartbeat = time.time()
        if not cloud_link_ok:
            cloud_link_ok = True
            log.info("Cloud link restored")
        return

    if topic.endswith("/cmd"):
        # dual format support
        cmd_id   = j.get("cmd_id")   or j.get("command_id")
        drone_id = j.get("drone_id") or j.get("target_id")

        log.info("cmd_id=%s  drone_id=%s  format=%s",
                 cmd_id, drone_id,
                 "Lambda" if ("action" in j or "command_id" in j) else "Legacy")

        if not cmd_id or not drone_id:
            log.warning("Missing cmd_id or drone_id — dropping")
            return
        if drone_id != DRONE_ID:
            log.info("Command for %s, ignoring", drone_id)
            return
        if command_exists(cmd_id):
            publish_ack(cmd_id, db_get_status(cmd_id) or "DUPLICATE")
            return
        if ENABLE_CLOUD_LINK_CHECK:
            if time.time() - last_heartbeat > HEARTBEAT_TIMEOUT:
                cloud_link_ok = False
            if not cloud_link_ok:
                log.warning("Cloud link down — rejecting %s", cmd_id)
                publish_ack(cmd_id, "NACK", reason="link_down")
                return

        if not db_insert_command(cmd_id, DRONE_ID, j, status="RECEIVED"):
            return

        publish_ack(cmd_id, "ACK")
        threading.Thread(
            target=process_command, args=(j,),
            daemon=True, name=f"cmd-{cmd_id[:8]}"
        ).start()

def process_command(j):
    cmd_id = j.get("cmd_id") or j.get("command_id")
    log.info("Processing: %s", cmd_id)
    try:
        result = send_mavlink_command(j)
        if result.get("success"):
            db_update_status(cmd_id, "EXECUTED")
            publish_ack(cmd_id, "EXECUTED", exec_result=result)
            db_log_event("INFO", "cmd_executed", {"cmd_id": cmd_id, "result": result})
        else:
            db_update_status(cmd_id, "FAILED")
            publish_ack(cmd_id, "NACK", exec_result=result, reason="send_failed")
            db_log_event("ERROR", "send_failed", {"cmd_id": cmd_id, "detail": result})
    except Exception as e:
        log.exception("Exception processing %s", cmd_id)
        db_update_status(cmd_id, "FAILED")
        publish_ack(cmd_id, "NACK", reason=str(e))

def _cloud_hb_watcher():
    global cloud_link_ok
    if not ENABLE_CLOUD_LINK_CHECK:
        log.info("Cloud heartbeat watcher disabled")
        return
    while True:
        if time.time() - last_heartbeat > HEARTBEAT_TIMEOUT:
            if cloud_link_ok:
                cloud_link_ok = False
                log.warning("Cloud heartbeat lost")
                db_log_event("WARN", "heartbeat_lost",
                             {"since": datetime.utcnow().isoformat() + "Z"})
        time.sleep(1)

threading.Thread(target=_cloud_hb_watcher, daemon=True, name="cloud-hb").start()

# ─────────────────────────────────────────────────────────────────────────────
# Start
# ─────────────────────────────────────────────────────────────────────────────
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_HOST, MQTT_PORT)

log.info("px4_agent started — drone=%s  px4=%s  mqtt=%s:%s",
         DRONE_ID, PX4_URL, MQTT_HOST, MQTT_PORT)
mqtt_client.loop_forever()
