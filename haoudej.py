#!/usr/bin/env python3
"""
haoudej.py — Raspberry Pi (Pi 4/5) port of HaoudejProgram.

Behaviour mirrors the Windows C++ service:
  - Every day at [schedule] time, runs a MySQL sales query, writes results.csv,
    and POSTs it to the configured API endpoint.
  - Business-date logic: run time >= 12:00 → export today; < 12:00 → export yesterday.
  - MQTT control on storeyes/caisse/<device_id>/request:
      {"cmd":"status"}    → heartbeat JSON
      {"cmd":"sync"}      → export + upload now (today's date)
      {"cmd":"reconcile"} → export + upload the last 30 days

Device ID: read from /proc/device-tree/serial-number (Pi 4/5 native),
           fall back to the CPU serial in /proc/cpuinfo, then hostname.

Run as a daemon:
    python3 haoudej.py [--config /path/to/config.conf]

Install as a systemd service: see haoudej.service next to this file.
"""

import argparse
import csv
import io
import json
import logging
import os
import re
import signal
import socket
import sys
import threading
import time
from configparser import ConfigParser
from datetime import date, timedelta, datetime
from pathlib import Path
from typing import Optional

import MySQLdb
import paho.mqtt.client as mqtt
import requests

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DEFAULT_CONFIG = Path(__file__).parent / "config.conf"
DATA_DIR       = Path("/var/lib/haoudej")
LOG_FILE       = DATA_DIR / "service.log"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _setup_logging():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s  %(levelname)s  %(message)s"
    handlers = [logging.StreamHandler(sys.stdout)]
    try:
        handlers.append(logging.FileHandler(LOG_FILE))
    except OSError:
        pass
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers)

log = logging.getLogger("haoudej")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def load_config(path: Path) -> ConfigParser:
    cfg = ConfigParser()
    cfg.read_dict({
        "mysql":    {"host": "127.0.0.1", "port": "3306", "user": "root",
                     "password": "", "database": ""},
        "query":    {"tva": "10", "output": str(DATA_DIR / "results.csv")},
        "schedule": {"time": "23:30"},
        "api":      {"url": "", "api_key": "", "field": "file",
                     "timeout": "120", "device_id": ""},
        "mqtt":     {"host": "mqtt.storeyes.io", "port": "1883", "keepalive": "60",
                     "user": "storeyes", "password": "12345",
                     "qos": "1", "retain": "false", "timeout": "5", "retries": "5"},
        "status":   {"heartbeat": "300"},
    })
    cfg.read(str(path))
    return cfg

# ---------------------------------------------------------------------------
# Device ID — Raspberry Pi 4 / 5
# ---------------------------------------------------------------------------
def _pi_serial_devicetree() -> Optional[str]:
    """Read the Pi serial from the device-tree node (Pi 4/5 standard path)."""
    try:
        raw = Path("/proc/device-tree/serial-number").read_bytes().rstrip(b"\x00")
        serial = raw.decode("ascii", errors="replace").strip()
        if serial:
            return serial.lower()
    except OSError:
        pass
    return None

def _pi_serial_cpuinfo() -> Optional[str]:
    """Fall back to the Serial field in /proc/cpuinfo."""
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("Serial"):
                serial = line.split(":")[-1].strip().lstrip("0").lower()
                if serial:
                    return serial
    except OSError:
        pass
    return None

def derive_device_id() -> str:
    return (_pi_serial_devicetree()
            or _pi_serial_cpuinfo()
            or socket.gethostname())

def get_device_id(cfg: ConfigParser) -> str:
    pinned = cfg.get("api", "device_id", fallback="").strip()
    return pinned if pinned else derive_device_id()

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------
def date_for_offset(offset_days: int) -> str:
    return (date.today() + timedelta(days=offset_days)).isoformat()

def today_str() -> str:
    return date.today().isoformat()

def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def business_date_for_schedule(sched_hour: int) -> str:
    return date_for_offset(0 if sched_hour >= 12 else -1)

# ---------------------------------------------------------------------------
# MySQL export
# ---------------------------------------------------------------------------
SALES_SQL = """\
SELECT
    a.article_id                                           AS ART_ID,
    m.{date_col}                                           AS VTE_DATE_HEURE,
    TIME(m.{date_col})                                     AS VTE_HEURE,
    a.libelle                                              AS ART_LIBELLE,
    a.quantite                                             AS VTE_QUANTITE,
    ROUND(a.mtt_total / NULLIF(a.quantite, 0), 2)          AS VTE_PRIX_DE_VENTE,
    ROUND(a.mtt_total, 2)                                  AS TOTAL_TTC,
    ROUND(a.mtt_total / (1 + {tva}/100), 2)                AS TOTAL_HT,
    ROUND(a.mtt_total - a.mtt_total / (1 + {tva}/100), 2) AS TOTAL_TVA,
    m.id                                                   AS VTE_ORDRE,
    u.login                                                AS USR_NOM
FROM caisse_mouvement m
JOIN caisse_mouvement_article a ON a.mvm_caisse_id = m.id
LEFT JOIN `user` u ON u.id = m.user_id
WHERE DATE(m.{date_col}) = '{date}'
  AND (m.is_annule IS NULL OR m.is_annule = 0)
  AND (a.is_annule IS NULL OR a.is_annule = 0)
  AND a.mtt_total <> 0
ORDER BY m.{date_col}, m.id, a.idx_element
"""

def run_mysql_export(cfg: ConfigParser, target_date: str) -> int:
    """Run the sales query for target_date and write results.csv.
    Returns the number of data rows, or raises on failure."""
    m = cfg["mysql"]
    tva      = cfg.get("query", "tva",      fallback="10")
    date_col = cfg.get("query", "date_col", fallback="date_creation")
    out_path = Path(cfg.get("query", "output", fallback=str(DATA_DIR / "results.csv")))
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn = MySQLdb.connect(
        host=m["host"],
        port=int(m.get("port", "3306")),
        user=m["user"],
        passwd=m.get("password", ""),
        db=m["database"],
        charset="utf8mb4",
        connect_timeout=10,
    )
    try:
        cur = conn.cursor()
        sql = SALES_SQL.format(tva=tva, date_col=date_col, date=target_date)
        cur.execute(sql)
        columns = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\r\n")
    writer.writerow(columns)
    writer.writerows(rows)
    out_path.write_text(buf.getvalue(), encoding="utf-8")

    log.info("export [%s]: %d rows -> %s", target_date, len(rows), out_path)
    return len(rows)

# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------
def upload_csv(cfg: ConfigParser, device_id: str) -> int:
    """POST results.csv to the API. Returns the HTTP status code."""
    api = cfg["api"]
    url = api.get("url", "").strip()
    if not url or url == "CHANGE_ME":
        raise ValueError("[api] url is not configured")

    out_path = Path(cfg.get("query", "output", fallback=str(DATA_DIR / "results.csv")))
    if not out_path.exists():
        raise FileNotFoundError(f"results.csv not found: {out_path}")

    timeout  = int(api.get("timeout", "120"))
    api_key  = api.get("api_key", "").strip()
    field    = api.get("field", "file")

    headers = {"X-DEVICE-ID": device_id}
    if api_key:
        headers["X-API-KEY"] = api_key

    with out_path.open("rb") as f:
        resp = requests.post(
            url,
            headers=headers,
            files={field: (out_path.name, f, "text/csv")},
            timeout=timeout,
        )

    log.info("upload: HTTP %d", resp.status_code)
    return resp.status_code

# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------
_job_lock = threading.Lock()

def export_and_upload(cfg: ConfigParser, device_id: str, target_date: str) -> str:
    with _job_lock:
        try:
            rows = run_mysql_export(cfg, target_date)
        except Exception as exc:
            log.error("export failed for %s: %s", target_date, exc)
            return f"{target_date}: export FAILED ({exc})"
        try:
            http = upload_csv(cfg, device_id)
            ok = 200 <= http < 300
        except Exception as exc:
            log.error("upload failed: %s", exc)
            return f"{target_date}: rows={rows} upload=FAILED ({exc})"

    return f"{target_date}: rows={rows} upload={'OK' if ok else 'FAILED'} http={http}"

def run_reconcile(cfg: ConfigParser, device_id: str) -> str:
    ok_count = fail_count = 0
    lines = []
    for d in range(1, 31):
        result = export_and_upload(cfg, device_id, date_for_offset(-d))
        lines.append(result)
        if "upload=OK" in result:
            ok_count += 1
        else:
            fail_count += 1
    summary = f"reconcile: ok={ok_count} failed={fail_count}"
    log.info(summary)
    return summary + "\n" + "\n".join(lines)

# ---------------------------------------------------------------------------
# MQTT
# ---------------------------------------------------------------------------
class MqttController:
    def __init__(self, cfg: ConfigParser, device_id: str,
                 on_command, publish_status_fn):
        m = cfg["mqtt"]
        self._device_id     = device_id
        self._on_command    = on_command
        self._publish_status = publish_status_fn
        self._qos           = int(m.get("qos", "1"))
        self._req_topic     = f"storeyes/caisse/{device_id}/request"
        self._res_topic     = f"storeyes/caisse/{device_id}/response"

        self._client = mqtt.Client(client_id=f"{device_id}-caisse", clean_session=True)
        user = m.get("user", "")
        pw   = m.get("password", "")
        if user:
            self._client.username_pw_set(user, pw)

        self._client.on_connect    = self._on_connect
        self._client.on_message    = self._on_message
        self._client.on_disconnect = self._on_disconnect

        self._host      = m.get("host", "mqtt.storeyes.io")
        self._port      = int(m.get("port", "1883"))
        self._keepalive = int(m.get("keepalive", "60"))

    def start(self):
        try:
            self._client.connect(self._host, self._port, self._keepalive)
            self._client.loop_start()
            log.info("MQTT connecting to %s:%s", self._host, self._port)
        except Exception as exc:
            log.warning("MQTT unavailable: %s", exc)

    def stop(self):
        self._client.loop_stop()
        self._client.disconnect()

    def publish(self, topic: str, payload: str):
        self._client.publish(topic, payload, qos=self._qos, retain=False)

    def publish_response(self, payload: str):
        self.publish(self._res_topic, payload)

    @property
    def res_topic(self):
        return self._res_topic

    def _on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            log.warning("MQTT broker refused connection (rc=%d)", rc)
            return
        log.info("MQTT connected — subscribing to %s", self._req_topic)
        client.subscribe(self._req_topic, qos=self._qos)
        self._publish_status("waiting")

    def _on_disconnect(self, client, userdata, rc):
        log.warning("MQTT disconnected (rc=%d), will auto-reconnect", rc)

    def _on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode("utf-8", errors="replace")
            data    = json.loads(payload)
            cmd     = data.get("cmd", "")
        except Exception:
            cmd = ""
        log.info("MQTT request: cmd=%s", cmd)
        threading.Thread(target=self._dispatch, args=(cmd, payload),
                         daemon=True).start()

    def _dispatch(self, cmd: str, payload: str):
        text = self._on_command(cmd, payload)
        self.publish_response(json.dumps({"response": text}))

# ---------------------------------------------------------------------------
# Daemon
# ---------------------------------------------------------------------------
class Daemon:
    def __init__(self, cfg: ConfigParser):
        self._cfg       = cfg
        self._device_id = get_device_id(cfg)
        self._stop      = threading.Event()
        self._last_run  = "never"
        self._mqtt: Optional[MqttController] = None

    def _build_status(self, state: str) -> str:
        sched = self._cfg.get("schedule", "time", fallback="23:30")
        return json.dumps({
            "device_id": self._device_id,
            "state":     state,
            "schedule":  sched,
            "last_run":  self._last_run,
            "timestamp": now_ts(),
        })

    def _publish_status(self, state: str):
        if self._mqtt:
            self._mqtt.publish_response(self._build_status(state))

    def _handle_command(self, cmd: str, _payload: str) -> str:
        if cmd == "status":
            return self._build_status("waiting")
        if cmd == "sync":
            result = export_and_upload(self._cfg, self._device_id, today_str())
            self._last_run = f"sync {result}"
            self._publish_status("waiting")
            return result
        if cmd == "reconcile":
            result = run_reconcile(self._cfg, self._device_id)
            self._last_run = f"reconcile @ {now_ts()}"
            self._publish_status("waiting")
            return result
        return f"unknown command: {cmd}"

    def _parse_schedule(self):
        raw = self._cfg.get("schedule", "time", fallback="23:30")
        m = re.fullmatch(r"(\d{2}):(\d{2})", raw.strip())
        if not m:
            log.warning("Invalid schedule time '%s', defaulting to 23:30", raw)
            return 23, 30
        return int(m.group(1)), int(m.group(2))

    def run(self):
        self._mqtt = MqttController(
            self._cfg, self._device_id,
            self._handle_command, self._publish_status,
        )
        self._mqtt.start()

        shh, smm = self._parse_schedule()
        heartbeat = max(30, int(self._cfg.get("status", "heartbeat", fallback="300")))
        log.info("Daemon started. schedule=%02d:%02d heartbeat=%ds", shh, smm, heartbeat)

        last_fire_date = ""
        last_beat_time = 0.0

        while not self._stop.is_set():
            now       = datetime.now()
            now_mins  = now.hour * 60 + now.minute
            fire_mins = shh * 60 + smm
            today     = today_str()

            if now_mins >= fire_mins and last_fire_date != today:
                biz_date = business_date_for_schedule(shh)
                log.info("Scheduled run for business date %s", biz_date)
                self._publish_status("running")
                result = export_and_upload(self._cfg, self._device_id, biz_date)
                self._last_run = f"scheduled {result}"
                last_fire_date = today
                self._publish_status("waiting")

            if time.monotonic() - last_beat_time >= heartbeat:
                self._publish_status("waiting")
                last_beat_time = time.monotonic()

            self._stop.wait(30)

        log.info("Daemon stopping.")
        self._publish_status("stopped")
        self._mqtt.stop()

    def stop(self):
        self._stop.set()

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="HaoudejProgram — Pi 4/5 edition")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG,
                        help="Path to config.conf")

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--reconcile", nargs="?", const=30, metavar="DAYS", type=int,
                      help="Re-export and upload the last N days (default 30) then exit")
    mode.add_argument("--sync", action="store_true",
                      help="Export and upload today then exit")
    mode.add_argument("--date", metavar="YYYY-MM-DD",
                      help="Export and upload a specific date then exit")

    args = parser.parse_args()

    _setup_logging()

    cfg = load_config(args.config)
    db_name = cfg.get("mysql", "database", fallback="")
    if not db_name or db_name == "CHANGE_ME":
        log.error("[mysql] database is not configured in %s", args.config)
        sys.exit(1)

    device_id = get_device_id(cfg)
    log.info("Device ID: %s", device_id)

    # ---- one-shot modes (no daemon, no MQTT) --------------------------------
    if args.reconcile is not None:
        days = max(1, args.reconcile)
        log.info("Reconcile: exporting last %d days", days)
        ok = fail = 0
        for d in range(1, days + 1):
            result = export_and_upload(cfg, device_id, date_for_offset(-d))
            print(result)
            if "upload=OK" in result:
                ok += 1
            else:
                fail += 1
        print(f"\nreconcile: ok={ok} failed={fail}")
        sys.exit(0 if fail == 0 else 1)

    if args.sync:
        result = export_and_upload(cfg, device_id, today_str())
        print(result)
        sys.exit(0 if "upload=OK" in result else 1)

    if args.date:
        result = export_and_upload(cfg, device_id, args.date)
        print(result)
        sys.exit(0 if "upload=OK" in result else 1)

    # ---- daemon mode --------------------------------------------------------
    daemon = Daemon(cfg)

    def _sig(signum, frame):
        log.info("Signal %d received — shutting down.", signum)
        daemon.stop()

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT,  _sig)

    daemon.run()


if __name__ == "__main__":
    main()
