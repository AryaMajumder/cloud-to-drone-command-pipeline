#!/usr/bin/env python3
"""
px4_agent.py
- Subscribe: drone/<drone_id>/cmd  (QoS=1)
- Publish ack/result: drone/<drone_id>/cmd_ack (QoS=1)
- Persist commands to SQLite BEFORE ACK
- Translate safe commands -> MAVLink via pymavlink
- Wait for MAVLink COMMAND_ACK to publish EXECUTED (or publish EXECUTED immediately with result on failure)
- Watch heartbeat: drone/<drone_id>/link -> set cloud_link_ok
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
PX4_URL = os.environ.get("PX4_URL", "udp:127.0.0.1:14550")
DB_PATH = os.environ.get("DB_PATH", "/var/lib/px4_agent/agent.db")
HEARTBEAT_TIMEOUT = int(os.environ.get("HEARTBEAT_TIMEOUT", "10"))  # seconds
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# ---------- logging ----------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("px4_agent")

# ---------- sqlite ----------
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
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
conn.commit()

def db_insert_command(cmd_id, drone_id, payload_json, status="RECEIVED"):
    now = datetime.utcnow().isoformat() + "Z"
    cur.execute("INSERT OR IGNORE INTO commands(cmd_id, drone_id, payload, status, created_at, updated_at) VALUES (?,?,?,?,?,?)",
                (cmd_id, drone_id, json.dumps(payload_json), status, now, now))
    conn.commit()

def db_update_status(cmd_id, status):
    now = datetime.utcnow().isoformat() + "Z"
    cur.execute("UPDATE commands SET status=?, updated_at=? WHERE cmd_id=?", (status, now, cmd_id))
    conn.commit()

def db_log_event(level, msg, meta=None):
    cur.execute("INSERT INTO events(level, msg, meta) VALUES (?,?,?)", (level, msg, json.dumps(meta or {})))
    conn.commit()

# ---------- MAVLink ----------
log.info(f"Connecting to PX4 at {PX4_URL} ...")
mav = mavutil.mavlink_connection(PX4_URL)
# allow some time
time.sleep(1)

def send_mavlink_command(cmd_payload):
    """
    Map high-level cmd to MAV_CMD* or set_mode.
    Return dict {success:bool, detail:str, mav_cmd:optional}
    """
    cmd = cmd_payload.get("cmd")
    params = cmd_payload.get("params", {}) or {}
    try:
        if cmd == "RTL":
            mav.mav.command_long_send(mav.target_system, mav.target_component,
                mavutil.mavlink.MAV_CMD_NAV_RETURN_TO_LAUNCH,
                0, 0,0,0,0,0,0,0)
            return {"success": True, "detail": "sent RTL", "mav_cmd": mavutil.mavlink.MAV_CMD_NAV_RETURN_TO_LAUNCH}
        if cmd in ("LOITER","HOLD"):
            mav.mav.command_long_send(mav.target_system, mav.target_component,
                mavutil.mavlink.MAV_CMD_NAV_LOITER_UNLIM,
                0, 0,0,0,0,0,0,0)
            return {"success": True, "detail":"sent LOITER", "mav_cmd": mavutil.mavlink.MAV_CMD_NAV_LOITER_UNLIM}
        if cmd == "SET_MODE":
            mode = (params.get("mode") or "").upper()
            # NOTE: mapping below is example; adjust per PX4 version
            mode_map = {"OFFBOARD": 6, "POSCTL": 4, "AUTO.MISSION": 3, "HOLD":5}
            if mode not in mode_map:
                return {"success": False, "detail": f"unknown mode {mode}"}
            mode_num = mode_map[mode]
            mav.mav.set_mode_send(mav.target_system, mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED, mode_num)
            return {"success": True, "detail": f"set_mode {mode}", "mav_cmd": "SET_MODE"}
        # add more safe commands as needed
        return {"success": False, "detail": f"unsupported cmd {cmd}"}
    except Exception as e:
        return {"success": False, "detail": f"mav send error: {e}"}

# wait for COMMAND_ACK helper
def wait_for_command_ack(timeout=5, target_command=None):
    """
    Wait up to timeout seconds for MAVLink COMMAND_ACK matching target_command (int) or any if None.
    Returns tuple (acked:bool, result_string)
    """
    start = time.time()
    while time.time() - start < timeout:
        msg = mav.recv_match(type='COMMAND_ACK', blocking=True, timeout=timeout - (time.time() - start))
        if not msg:
            break
        try:
            cmd = int(msg.command)
            result = int(msg.result)
            # result 0 = success
            if target_command is None or cmd == target_command:
                res_text = "SUCCESS" if result == 0 else f"ERROR({result})"
                return (result == 0, f"cmd={cmd} result={res_text}")
        except Exception:
            continue
    return (False, "no COMMAND_ACK")

# ---------- MQTT client ----------
client_id = f"px4-agent-{DRONE_ID}"
client = mqtt.Client(client_id=client_id, clean_session=False)

cloud_link_ok = True
last_heartbeat = 0

def publish_ack(cmd_id, status, exec_result=None, reason=None):
    topic = f"drone/{DRONE_ID}/cmd_ack"
    msg = {"cmd_id": cmd_id, "drone_id": DRONE_ID, "status": status, "ts_utc": datetime.utcnow().isoformat() + "Z"}
    if exec_result is not None:
        msg["exec_result"] = exec_result
    if reason:
        msg["reason"] = reason
    client.publish(topic, json.dumps(msg), qos=1)
    log.info(f"Published ack: {msg}")

def on_connect(client, userdata, flags, rc):
    log.info("MQTT connected, subscribing to topics")
    client.subscribe(f"drone/{DRONE_ID}/cmd", qos=1)
    client.subscribe(f"drone/{DRONE_ID}/link", qos=1)

def on_message(client, userdata, msg):
    global last_heartbeat, cloud_link_ok
    topic = msg.topic
    payload = msg.payload.decode("utf-8")
    try:
        j = json.loads(payload)
    except Exception as e:
        log.warning("Invalid JSON payload on %s: %s", topic, e)
        return

    if topic.endswith("/link"):
        last_heartbeat = time.time()
        if not cloud_link_ok:
            cloud_link_ok = True
            log.info("Cloud link restored")
        return

    # command topic
    if topic.endswith("/cmd"):
        cmd_id = j.get("cmd_id")
        drone_id = j.get("drone_id")
        if drone_id != DRONE_ID:
            log.info("Command for another drone, ignoring")
            return

        # If we deem link down, NACK
        if time.time() - last_heartbeat > HEARTBEAT_TIMEOUT:
            cloud_link_ok = False
        if not cloud_link_ok:
            log.warning("Cloud link down: rejecting cmd %s", cmd_id)
            publish_ack(cmd_id, "NACK", reason="link_down")
            db_log_event("WARN", "rejected_cmd_link_down", {"cmd_id": cmd_id})
            return

        # persist before ack
        try:
            db_insert_command(cmd_id, DRONE_ID, j, status="RECEIVED")
        except Exception as e:
            log.error("DB insert failed: %s", e)
            publish_ack(cmd_id, "NACK", reason="db_error")
            return

        # ACK right away
        publish_ack(cmd_id, "ACK")

        # execute in new thread
        threading.Thread(target=process_command, args=(j,)).start()

def process_command(j):
    cmd_id = j.get("cmd_id")
    try:
        res = send_mavlink_command(j)
        if not res.get("success"):
            # immediate NACK with detail
            db_update_status(cmd_id, "FAILED")
            publish_ack(cmd_id, "NACK", exec_result=res, reason="send_failed")
            db_log_event("ERROR", "send_failed", {"cmd_id":cmd_id, "detail":res})
            return

        # wait for COMMAND_ACK matching the mav command if provided
        mav_cmd = res.get("mav_cmd")
        target = mav_cmd if isinstance(mav_cmd, int) else None
        acked, detail = wait_for_command_ack(timeout=6, target_command=target)
        if acked:
            db_update_status(cmd_id, "EXECUTED")
            publish_ack(cmd_id, "EXECUTED", exec_result={"detail": detail})
            db_log_event("INFO", "cmd_executed", {"cmd_id": cmd_id, "detail": detail})
        else:
            # If no COMMAND_ACK, still mark and publish result (depends on policy)
            db_update_status(cmd_id, "EXECUTED_PENDING")
            publish_ack(cmd_id, "EXECUTED", exec_result={"detail": detail})
            db_log_event("WARN", "no_command_ack", {"cmd_id": cmd_id, "detail": detail})
    except Exception as e:
        log.exception("Exception executing command %s", cmd_id)
        db_update_status(cmd_id, "FAILED")
        publish_ack(cmd_id, "NACK", reason=str(e))
        db_log_event("ERROR", "exec_exception", {"cmd_id": cmd_id, "err": str(e)})

def heartbeat_watcher():
    global cloud_link_ok
    while True:
        if time.time() - last_heartbeat > HEARTBEAT_TIMEOUT:
            if cloud_link_ok:
                cloud_link_ok = False
                log.warning("Heartbeat lost, marking cloud_link_ok = False")
                db_log_event("WARN", "heartbeat_lost", {"since": datetime.utcnow().isoformat()+"Z"})
        time.sleep(1)

# ---------- start ----------
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_HOST, MQTT_PORT)
# start heartbeat watcher
threading.Thread(target=heartbeat_watcher, daemon=True).start()
log.info(f"PX4 agent listening as {client_id} on mqtt {MQTT_HOST}:{MQTT_PORT}")
client.loop_forever()
