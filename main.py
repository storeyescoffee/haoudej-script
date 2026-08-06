#!/usr/bin/env python3
"""
main.py — Raspberry Pi (Pi 4/5) port of HaoudejProgram.

Runs the MySQL sales query for a given day, writes results.csv, and POSTs it to
the configured API endpoint. One-shot: it does its job and exits. Scheduling is
cron's problem — `python3 main.py --configure` writes /etc/cron.d/caisse.

  python3 main.py --configure        prompt for DB credentials + schedule, save,
                                     write /etc/cron.d/caisse, test the connection
                                     (that write needs root: sudo is invoked for
                                     it alone, so config.conf stays yours)
  python3 main.py                    export+upload today
  python3 main.py --sync             identical to the above; explicit for cron
  python3 main.py --date 2026-07-01  ... a specific date instead
  python3 main.py --reconcile [N]    ... each of the last N days (default 30)
  python3 main.py --resolve-mac      print the till's IP, found by MAC
  python3 main.py --upload           re-POST the existing results.csv, no export

Default and --sync both export TODAY. The nightly cron job therefore has to run
in the evening, after trading has closed — an overnight run would export a day
that has barely started. Use --date to pick up a day that was missed.

If the upload step fails during the daily export (no flag / --sync / --date),
main.py queues a one-shot `python3 main.py --upload` job 30 minutes out via the
Linux `at` command, so the existing results.csv gets another shot without
re-querying MySQL. If that retry also fails, it re-queues itself the same way —
chaining every 30 minutes until an upload finally succeeds. Requires the `at`
package with atd running: sudo apt install at && sudo systemctl enable --now atd.
--reconcile does not chain retries; a failed day there waits for the next
scheduled run.

Exit status is 0 only if every upload succeeded, so cron mails you on failure.

Device ID: read from /proc/device-tree/serial-number (Pi 4/5 native),
           fall back to the CPU serial in /proc/cpuinfo, then hostname.
"""

import argparse
import csv
import getpass
import io
import logging
import os
import re
import shlex
import socket
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import MySQLdb
import requests

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DEFAULT_CONFIG = Path(__file__).parent / "config.conf"
# Everything the script writes lives beside the script: config.conf, haoudej.log,
# results.csv. Nothing is a daemon, so there is no state to put under /var/lib —
# and keeping it here means the script does not need root just to start up.
DATA_DIR       = Path(__file__).resolve().parent
LOG_FILE       = DATA_DIR / "haoudej.log"
CRON_FILE      = Path("/etc/cron.d/caisse")   # written by --configure, needs root

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _setup_logging():
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
        "mysql": {"host": "127.0.0.1", "port": "3306", "user": "root",
                  "password": "", "database": "", "mac": "",
                  "connect_timeout": "10"},
        "query": {"tva": "10", "date_col": "date_creation",
                  "output": ""},   # blank -> results.csv beside the script
        "api":   {"url": "", "api_key": "", "field": "file",
                  "timeout": "120", "device_id": ""},
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

# ---------------------------------------------------------------------------
# MAC -> IP resolution
#
# The till is usually on DHCP, so [mysql] host can go stale after a lease
# change. When [mysql] mac is set and the configured host does not answer,
# we look the machine up by MAC on the local subnet instead.
# ---------------------------------------------------------------------------
_MAC_RE = re.compile(r"^[0-9a-f]{2}(:[0-9a-f]{2}){5}$")

# IP last resolved from the MAC, reused for the rest of the process lifetime.
_resolved_host: Optional[str] = None

def normalize_mac(raw: str) -> str:
    """Accept 30:0E:D5:34:70:2E, 30-0e-d5-34-70-2e, 300ed534702e -> canonical form."""
    hexes = re.sub(r"[^0-9a-fA-F]", "", raw).lower()
    if len(hexes) != 12:
        return ""
    return ":".join(hexes[i:i + 2] for i in range(0, 12, 2))

def _arp_table() -> dict:
    """Current ARP cache as {mac: ip}. Reads /proc/net/arp, falls back to `arp -n`."""
    table = {}
    try:
        lines = Path("/proc/net/arp").read_text().splitlines()[1:]
        for line in lines:
            fields = line.split()
            if len(fields) >= 4:
                ip, mac = fields[0], normalize_mac(fields[3])
                if mac and mac != "00:00:00:00:00:00":
                    table[mac] = ip
        if table:
            return table
    except OSError:
        pass

    try:
        out = subprocess.run(["arp", "-n"], capture_output=True, text=True,
                             timeout=10).stdout
    except (OSError, subprocess.SubprocessError):
        return table

    for line in out.splitlines():
        ip_m  = re.search(r"\b(\d{1,3}(?:\.\d{1,3}){3})\b", line)
        mac_m = re.search(r"\b([0-9a-fA-F]{2}(?:[:-][0-9a-fA-F]{2}){5})\b", line)
        if ip_m and mac_m:
            mac = normalize_mac(mac_m.group(1))
            if mac and mac != "00:00:00:00:00:00":
                table[mac] = ip_m.group(1)
    return table

def _arp_scan_lookup(mac: str) -> Optional[str]:
    """IP of `mac` via `sudo arp-scan --localnet | awk '/<mac>/{print $1; exit}'`.

    Preferred over the ping sweep: it ARPs every host directly, so it also finds
    machines that drop ICMP. arp-scan needs raw sockets, hence sudo; the cron job
    already runs as root, so sudo is a no-op there. Returns None if arp-scan is
    missing, sudo is not permitted, or the MAC is not on the subnet.

    `mac` must already be normalized; it is interpolated into the awk program."""
    cmd = f"sudo -n arp-scan --localnet | awk '/{mac}/{{print $1; exit}}'"
    try:
        proc = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                              timeout=120)
    except (OSError, subprocess.SubprocessError) as exc:
        log.warning("[mysql] arp-scan failed (%s), falling back to a ping sweep", exc)
        return None

    ip = proc.stdout.strip()
    if re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}", ip):
        return ip

    err = proc.stderr.strip() or f"exit {proc.returncode}, no match"
    log.info("[mysql] arp-scan found nothing for %s (%s)", mac, err)
    return None

def _local_ipv4() -> Optional[str]:
    """Our address on the interface that reaches the LAN gateway."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 53))  # no traffic sent, just picks the route
        return s.getsockname()[0]
    except OSError:
        return None
    finally:
        s.close()

def _prime_arp_cache():
    """Ping every host on our /24 so the kernel ARP cache is populated."""
    local = _local_ipv4()
    if not local:
        return
    prefix = local.rsplit(".", 1)[0]

    def _ping(host: str):
        try:
            subprocess.run(["ping", "-c", "1", "-W", "1", host],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           timeout=3)
        except (OSError, subprocess.SubprocessError):
            pass

    targets = [f"{prefix}.{i}" for i in range(1, 255) if f"{prefix}.{i}" != local]
    log.info("[mysql] sweeping %s.0/24 to populate the ARP cache", prefix)
    with ThreadPoolExecutor(max_workers=64) as pool:
        list(pool.map(_ping, targets))

def resolve_ip_by_mac(mac: str) -> Optional[str]:
    """Find the current IP of a MAC on the local subnet, or None."""
    mac = normalize_mac(mac)
    if not _MAC_RE.match(mac):
        log.warning("[mysql] mac '%s' is not a valid MAC address", mac)
        return None

    # 1. Already in the kernel's ARP cache — free.
    ip = _arp_table().get(mac)
    if ip:
        log.info("[mysql] %s found in ARP cache at %s", mac, ip)
        return ip

    # 2. arp-scan: ARPs the whole subnet, finds hosts that ignore ICMP.
    ip = _arp_scan_lookup(mac)
    if ip:
        log.info("[mysql] %s resolved to %s by arp-scan", mac, ip)
        return ip

    # 3. No arp-scan (or it came up empty): ping sweep, then re-read the cache.
    _prime_arp_cache()
    ip = _arp_table().get(mac)
    if ip:
        log.info("[mysql] %s resolved to %s after ping sweep", mac, ip)
    else:
        log.error("[mysql] %s not found on the local subnet", mac)
    return ip

# ---------------------------------------------------------------------------
# MySQL export
# ---------------------------------------------------------------------------
def connect_mysql(cfg: ConfigParser):
    """Connect to MySQL at [mysql] host; if that fails and [mysql] mac is set,
    re-resolve the host by MAC and retry there."""
    global _resolved_host

    m        = cfg["mysql"]
    port     = int(m.get("port", "3306"))
    mac      = m.get("mac", "").strip()
    settings = dict(
        port=port,
        user=m["user"],
        passwd=m.get("password", ""),
        db=m["database"],
        charset="utf8mb4",
        connect_timeout=int(m.get("connect_timeout", "10")),
    )

    # Prefer an IP already resolved from the MAC earlier in this run.
    host = _resolved_host or m["host"]
    try:
        return MySQLdb.connect(host=host, **settings)
    except MySQLdb.OperationalError as exc:
        if not mac:
            raise
        log.warning("[mysql] %s:%d unreachable (%s) — resolving by MAC %s",
                    host, port, exc, mac)

    ip = resolve_ip_by_mac(mac)
    if not ip:
        raise MySQLdb.OperationalError(
            f"host {host} unreachable and MAC {mac} not found on the local subnet")
    if ip == host:
        raise MySQLdb.OperationalError(
            f"MAC {mac} still resolves to {host}, which is not accepting connections")

    conn = MySQLdb.connect(host=ip, **settings)
    log.info("[mysql] connected via MAC fallback: %s -> %s", mac, ip)
    _resolved_host = ip
    return conn

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

def output_path(cfg: ConfigParser) -> Path:
    """Where results.csv goes. Blank [query] output means the default beside the
    script — an empty value in config.conf overrides the seeded default, so it has
    to be caught here rather than left to a ConfigParser fallback. A relative path
    is resolved against the script too, not cron's working directory."""
    raw = cfg.get("query", "output", fallback="").strip()
    if not raw:
        return DATA_DIR / "results.csv"
    return DATA_DIR / raw   # absolute raw wins; Path("/a") / "/b" -> "/b"

def run_mysql_export(cfg: ConfigParser, target_date: str) -> int:
    """Run the sales query for target_date and write results.csv.
    Returns the number of data rows, or raises on failure."""
    tva      = cfg.get("query", "tva",      fallback="10")
    date_col = cfg.get("query", "date_col", fallback="date_creation")
    out_path = output_path(cfg)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn = connect_mysql(cfg)
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

    out_path = output_path(cfg)
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
            data={"skip_alert_trigger": "true"},
            timeout=timeout,
        )

    log.info("upload: HTTP %d", resp.status_code)
    return resp.status_code

def schedule_at_retry(config_path: Path, delay_minutes: int = 30) -> bool:
    """Queue a one-shot `python3 main.py --upload --config <path>` job via the
    Linux `at` command, `delay_minutes` from now. --upload re-POSTs whatever is
    currently in results.csv and, on failure, calls this again — so a chain of
    failures keeps retrying every `delay_minutes` until one finally succeeds.
    Needs the `at` package with atd running: sudo apt install at &&
    sudo systemctl enable --now atd. Scheduling failure is only logged; the
    run that triggered it has already failed and exits non-zero either way."""
    script = Path(__file__).resolve()
    cmd = (f"{shlex.quote(sys.executable)} {shlex.quote(str(script))} "
           f"--upload --config {shlex.quote(str(config_path.resolve()))}")
    try:
        proc = subprocess.run(["at", "now", "+", str(delay_minutes), "minutes"],
                              input=cmd, text=True, capture_output=True, timeout=10)
    except (OSError, subprocess.SubprocessError) as exc:
        log.error("could not run `at` to schedule an upload retry (%s) — "
                   "is the `at` package installed? sudo apt install at && "
                   "sudo systemctl enable --now atd", exc)
        return False
    if proc.returncode != 0:
        log.error("failed to schedule `at` retry: %s", proc.stderr.strip())
        return False
    log.info("upload failed — retry scheduled via `at` in %d minutes", delay_minutes)
    return True

# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------
def export_and_upload(cfg: ConfigParser, device_id: str, target_date: str,
                       config_path: Optional[Path] = None) -> bool:
    """Export target_date and upload it. True on success.

    config_path is only set for the daily export path (not --reconcile): when
    given, an upload failure (but not an export failure — there'd be nothing
    new to upload) queues a retry via schedule_at_retry."""
    try:
        rows = run_mysql_export(cfg, target_date)
    except Exception as exc:
        log.error("%s: export FAILED (%s)", target_date, exc)
        return False

    try:
        http = upload_csv(cfg, device_id)
    except Exception as exc:
        log.error("%s: rows=%d upload FAILED (%s)", target_date, rows, exc)
        if config_path is not None:
            schedule_at_retry(config_path)
        return False

    ok = 200 <= http < 300
    log.log(logging.INFO if ok else logging.ERROR,
            "%s: rows=%d upload=%s http=%d",
            target_date, rows, "OK" if ok else "FAILED", http)
    if not ok and config_path is not None:
        schedule_at_retry(config_path)
    return ok

def upload_only(cfg: ConfigParser, device_id: str, config_path: Path) -> bool:
    """--upload: re-POST the existing results.csv, no MySQL export. On failure,
    reschedules itself via `at` in 30 minutes (see schedule_at_retry)."""
    out_path = output_path(cfg)
    if not out_path.exists():
        log.error("--upload: %s not found; nothing to upload", out_path)
        return False

    try:
        http = upload_csv(cfg, device_id)
    except Exception as exc:
        log.error("--upload: FAILED (%s)", exc)
        schedule_at_retry(config_path)
        return False

    ok = 200 <= http < 300
    log.log(logging.INFO if ok else logging.ERROR,
            "--upload: %s http=%d", "OK" if ok else "FAILED", http)
    if not ok:
        schedule_at_retry(config_path)
    return ok

# ---------------------------------------------------------------------------
# --configure
# ---------------------------------------------------------------------------
def write_section_values(path: Path, section: str, values: dict):
    """Update `key = value` lines inside [section], leaving the rest of the file
    — comments included — byte-for-byte intact. ConfigParser.write() would drop
    every comment, so the file is edited line by line instead. Keys not already
    present are appended to the section; a missing section is appended whole."""
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []

    out, pending = [], dict(values)
    in_section = False
    section_end = None  # index in `out` just past the last line of our section

    for line in lines:
        header = re.match(r"\s*\[(.+?)\]\s*$", line)
        if header:
            if in_section:          # leaving our section
                section_end = len(out)
            in_section = header.group(1) == section
            out.append(line)
            continue

        if in_section:
            kv = re.match(r"(\s*)([^#;=\s][^=]*?)(\s*=\s*)(.*)$", line)
            if kv and kv.group(2).strip() in pending:
                key = kv.group(2).strip()
                out.append(f"{kv.group(1)}{kv.group(2)}{kv.group(3)}{pending.pop(key)}")
                continue

        out.append(line)

    if in_section:                  # our section ran to end of file
        section_end = len(out)

    if section_end is None:         # section absent entirely
        if out and out[-1].strip():
            out.append("")
        out.append(f"[{section}]")
        section_end = len(out)

    # Append whatever keys the section did not already have.
    for key, value in pending.items():
        out.insert(section_end, f"{key} = {value}")
        section_end += 1

    path.write_text("\n".join(out) + "\n", encoding="utf-8")

def _prompt(label: str, current: str, secret: bool = False) -> str:
    """Ask for one value, offering the current one as the default."""
    if secret:
        shown = "*" * len(current) if current else "empty"
        entered = getpass.getpass(f"  {label} [{shown}]: ")
    else:
        entered = input(f"  {label} [{current or 'empty'}]: ").strip()
    return entered if entered else current

def _existing_cron_times() -> dict:
    """Current HH:MM / weekday out of /etc/cron.d/caisse, to offer as defaults."""
    found = {}
    try:
        text = CRON_FILE.read_text()
    except OSError:
        return found
    for line in text.splitlines():
        if line.startswith("#") or "main.py" not in line:
            continue
        f = line.split()
        if len(f) < 5:
            continue
        minute, hour, dow = f[0], f[1], f[4]
        if "--reconcile" in line:
            found["reconcile"] = f"{dow} {int(hour):02d}:{int(minute):02d}"
        else:
            found["daily"] = f"{int(hour):02d}:{int(minute):02d}"
    return found

def _parse_hhmm(raw: str) -> Optional[tuple]:
    m = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", raw.strip())
    return (int(m.group(1)), int(m.group(2))) if m else None

def render_cron(daily: tuple, reconcile: Optional[tuple], user: str,
                config_path: Path) -> str:
    """Build the /etc/cron.d/caisse content. Note the user column — cron.d entries
    carry one, unlike a per-user crontab."""
    script = Path(__file__).resolve()
    cmd    = f"{sys.executable} {script} --config {config_path}"
    dh, dm = daily

    lines = [
        "# HaoudejProgram — daily MySQL sales export and upload.",
        "# Generated by `main.py --configure`; re-run it to change the schedule.",
        "#",
        "# Runs as root: the MAC fallback in [mysql] shells out to arp-scan, which",
        "# needs raw sockets. No sudoers entry required. Because of that, the log",
        "# and CSV it writes next to the script end up owned by root — chown them",
        f"# back if you want to read them as {user} without sudo.",
        "#",
        f"# Output goes to {LOG_FILE}. Failed runs exit non-zero;",
        "# set MAILTO to have cron mail you about them.",
        "SHELL=/bin/sh",
        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        'MAILTO=""',
        "",
        "# --sync exports TODAY, whatever the hour — so this has to be an evening",
        "# time, once trading has closed. An overnight run would export a day that",
        "# has barely started. To backfill a missed day, run main.py --date YYYY-MM-DD.",
        f"{dm} {dh} * * *\t{user}\t{cmd} --sync",
    ]
    if reconcile:
        rdow, rh, rm = reconcile
        lines += [
            "",
            "# Weekly catch-up: re-export the last 30 days so a failed or skipped nightly",
            "# run gets backfilled. 30 sequential export+upload cycles — not fast.",
            f"{rm} {rh} * * {rdow}\t{user}\t{cmd} --reconcile 30",
        ]
    return "\n".join(lines) + "\n"

def install_cron(cron_text: str) -> bool:
    """Write CRON_FILE, escalating with sudo if we are not root.

    Only this write is escalated, not the whole run: config.conf is deliberately
    written as the invoking user, so it does not end up root-owned."""
    try:
        CRON_FILE.write_text(cron_text)
        os.chmod(CRON_FILE, 0o644)  # cron ignores group/other-writable files
        return True
    except PermissionError:
        pass
    except OSError as exc:
        print(f"\nCannot write {CRON_FILE}: {exc}")
        return False

    print(f"\n{CRON_FILE} needs root — sudo may ask for your password.")
    try:
        tee = subprocess.run(["sudo", "tee", str(CRON_FILE)],
                             input=cron_text, text=True,
                             stdout=subprocess.DEVNULL)
        if tee.returncode == 0:
            chmod = subprocess.run(["sudo", "chmod", "644", str(CRON_FILE)])
            if chmod.returncode == 0:
                return True
    except FileNotFoundError:
        print("sudo is not installed.")

    print(f"\nCould not write {CRON_FILE} with sudo. Install the schedule by hand:\n")
    print(f"  sudo tee {CRON_FILE} >/dev/null <<'EOF'")
    print(cron_text + "EOF")
    print(f"  sudo chmod 644 {CRON_FILE}")
    return False

def configure(path: Path):
    """Prompt for the database credentials and the schedule, write config.conf
    and /etc/cron.d/caisse."""
    cfg = load_config(path)
    m   = cfg["mysql"]

    print(f"Database settings — {path}")
    print("Press Enter to keep the current value shown in brackets.\n")

    values = {
        "host":     _prompt("host",     m.get("host", "")),
        "port":     _prompt("port",     m.get("port", "3306")),
        "user":     _prompt("user",     m.get("user", "")),
        "password": _prompt("password", m.get("password", ""), secret=True),
        "database": _prompt("database", m.get("database", "")),
        "mac":      _prompt("mac (blank = no fallback)", m.get("mac", "")),
    }

    if not values["port"].isdigit():
        print(f"\nport must be a number, got '{values['port']}'. Nothing written.")
        return 1
    if not values["database"] or values["database"] == "CHANGE_ME":
        print("\ndatabase is required. Nothing written.")
        return 1
    if values["mac"]:
        canonical = normalize_mac(values["mac"])
        if not _MAC_RE.match(canonical):
            print(f"\n'{values['mac']}' is not a valid MAC address. Nothing written.")
            return 1
        values["mac"] = canonical

    # ---- schedule -----------------------------------------------------------
    current = _existing_cron_times()
    print(f"\nSchedule — {CRON_FILE}")

    raw_daily = _prompt("daily run time HH:MM", current.get("daily", "23:30"))
    daily = _parse_hhmm(raw_daily)
    if not daily:
        print(f"\n'{raw_daily}' is not a valid HH:MM time. Nothing written.")
        return 1
    if daily[0] < 12:
        # The job exports today, so before noon it would export a day that has
        # only just started.
        print(f"\n{raw_daily} is before midday. The job exports today, so it would")
        print("export a day that has barely begun. Pick an evening time, after")
        print("trading has closed. Nothing written.")
        return 1

    raw_rec = _prompt("weekly reconcile, 'DOW HH:MM' (0=Sun, blank = off)",
                      current.get("reconcile", "0 03:00"))
    reconcile = None
    if raw_rec.strip():
        parts = raw_rec.split()
        hhmm  = _parse_hhmm(parts[-1]) if parts else None
        dow   = parts[0] if len(parts) == 2 else None
        if not hhmm or dow not in {str(d) for d in range(7)}:
            print(f"\n'{raw_rec}' is not a valid 'DOW HH:MM' (0-6). Nothing written.")
            return 1
        reconcile = (int(dow), hhmm[0], hhmm[1])

    # ---- write --------------------------------------------------------------
    write_section_values(path, "mysql", values)
    try:
        os.chmod(path, 0o600)  # the file now holds a password
    except OSError as exc:
        print(f"warning: could not chmod 600 {path}: {exc}")
    print(f"\nWritten {path}.")

    # cron.d entries name the user to run as. root: arp-scan needs raw sockets,
    # and writing /etc/cron.d already requires root anyway.
    cron_user = "root"
    cron_text = render_cron(daily, reconcile, cron_user, path.resolve())
    if not install_cron(cron_text):
        return 1
    print(f"Written {CRON_FILE} (runs as {cron_user}).")

    # Prove the credentials actually work rather than waiting for the nightly run.
    print("\nTesting the connection...")
    try:
        conn = connect_mysql(load_config(path))
    except Exception as exc:
        print(f"FAILED: {exc}")
        return 1
    conn.close()
    print("Connected.")
    return 0

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="HaoudejProgram — Pi 4/5 edition. One-shot; scheduled by cron.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG,
                        help="Path to config.conf")

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--reconcile", nargs="?", const=30, metavar="DAYS", type=int,
                      help="Re-export and upload the last N days (default 30)")
    mode.add_argument("--sync", action="store_true",
                      help="Export and upload today (same as no flag; explicit for cron)")
    mode.add_argument("--date", metavar="YYYY-MM-DD",
                      help="Export and upload a specific date")
    mode.add_argument("--resolve-mac", action="store_true",
                      help="Look up [mysql] mac on the local subnet, print the IP, exit")
    mode.add_argument("--upload", action="store_true",
                      help="Upload the existing results.csv only, no export; "
                           "reschedules itself via `at` every 30 min on failure")
    mode.add_argument("--configure", action="store_true",
                      help="Prompt for the database credentials and save them to config.conf")

    args = parser.parse_args()

    _setup_logging()

    # Runs before the config is validated — it is what makes an invalid config valid.
    if args.configure:
        sys.exit(configure(args.config))

    cfg = load_config(args.config)

    if args.resolve_mac:
        mac = cfg.get("mysql", "mac", fallback="").strip()
        if not mac:
            log.error("[mysql] mac is not set in %s", args.config)
            sys.exit(1)
        ip = resolve_ip_by_mac(mac)
        if not ip:
            sys.exit(1)
        print(ip)
        sys.exit(0)

    device_id = get_device_id(cfg)
    log.info("Device ID: %s", device_id)

    if args.upload:
        sys.exit(0 if upload_only(cfg, device_id, args.config) else 1)

    db_name = cfg.get("mysql", "database", fallback="")
    if not db_name or db_name == "CHANGE_ME":
        log.error("[mysql] database is not configured in %s", args.config)
        sys.exit(1)

    if args.reconcile is not None:
        days = max(1, args.reconcile)
        log.info("Reconcile: exporting last %d days", days)
        failed = sum(not export_and_upload(cfg, device_id, date_for_offset(-d))
                     for d in range(1, days + 1))
        log.info("reconcile: ok=%d failed=%d", days - failed, failed)
        sys.exit(1 if failed else 0)

    # No flag and --sync are the same thing: today.
    target = args.date if args.date else today_str()
    sys.exit(0 if export_and_upload(cfg, device_id, target, config_path=args.config) else 1)


if __name__ == "__main__":
    main()
