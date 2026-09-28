#!/bin/sh
# install.sh — set up HaoudejProgram on a Raspberry Pi (Debian / Raspberry Pi OS).
#
#   sudo ./install.sh
#
# Installs the system packages, including the Python deps (MySQLdb, requests)
# from apt so the system python3 can run main.py.
#
# It no longer installs a cron job: scheduling is done from the admin panel
# (Devices -> haoudej-script -> Schedules, e.g. EVERYDAY 23:00 with --sync), and
# sty-software-manager writes it to /etc/cron.d/sty-schedule. Runs started that
# way report their log and exit code back to the panel. A /etc/cron.d/caisse
# left by an earlier install is removed so the job doesn't run twice.
#
# Safe to re-run.
set -eu

if [ "$(id -u)" -ne 0 ]; then
    echo "install.sh needs root — re-run with: sudo $0" >&2
    exit 1
fi

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
OLD_CRON_FILE=/etc/cron.d/caisse  # written by earlier installs
SUDOERS_FILE=/etc/sudoers.d/caisse
# User that owns logs/ and results.csv, and may run arp-scan via sudo.
RUN_USER=m0hcine24
# Owner of the checkout, so config.conf doesn't end up root-owned.
OWNER="$(stat -c %U "$APP_DIR")"

echo "==> Installing system packages"
apt-get update
# python3-mysqldb/python3-requests are the system builds of requirements.txt;
# arp-scan backs the MAC fallback; at/atd run the 30-minute upload retries.
DEBIAN_FRONTEND=noninteractive apt-get install -y \
    python3 python3-mysqldb python3-requests arp-scan at cron

systemctl enable --now atd
systemctl enable --now cron

if [ ! -f "$APP_DIR/config.conf" ]; then
    sudo -u "$OWNER" cp "$APP_DIR/config.conf.example" "$APP_DIR/config.conf"
    chmod 600 "$APP_DIR/config.conf"
    echo "==> Created config.conf from the example — edit it with your DB credentials"
fi

if ! id "$RUN_USER" >/dev/null 2>&1; then
    echo "user $RUN_USER does not exist — create it or edit RUN_USER in $0" >&2
    exit 1
fi

# Earlier installs ran the job as root, so logs/ and results.csv may be
# root-owned; hand them (and the chmod-600 config.conf) to $RUN_USER.
for f in "$APP_DIR/logs" "$APP_DIR/results.csv" "$APP_DIR/config.conf"; do
    if [ -e "$f" ]; then
        chown -R "$RUN_USER" "$f"
    fi
done

# The MAC fallback runs `sudo -n arp-scan`, which needs raw sockets. Allow
# $RUN_USER to run arp-scan (and only arp-scan) as root without a password.
echo "==> Writing $SUDOERS_FILE"
ARP_SCAN="$(command -v arp-scan)"
TMP_SUDOERS="$(mktemp)"
echo "$RUN_USER ALL=(root) NOPASSWD: $ARP_SCAN" > "$TMP_SUDOERS"
visudo -cf "$TMP_SUDOERS" >/dev/null
install -m 440 "$TMP_SUDOERS" "$SUDOERS_FILE"
rm -f "$TMP_SUDOERS"

if [ -e "$OLD_CRON_FILE" ]; then
    echo "==> Removing $OLD_CRON_FILE (scheduling now lives in the admin panel)"
    rm -f "$OLD_CRON_FILE"
fi

echo "==> Done. Add the daily run in the admin panel: Schedules -> Every day 23:00, --sync"
echo "    Test it now with: cd $APP_DIR && sudo -u $RUN_USER python3 main.py --sync"
