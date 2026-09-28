# haoudej-script

Exports the day's sales from the till's MySQL database to `results.csv` and
uploads it to the configured API. Built for a Raspberry Pi 4/5 running
Raspberry Pi OS / Debian.

## Install

```sh
git clone https://github.com/storeyescoffee/haoudej-script.git
cd haoudej-script
sudo ./install.sh
```

`install.sh`:

- installs the system packages (`python3`, `python3-mysqldb`, `python3-requests`, `arp-scan`, `at`, `cron`)
- copies `config.conf.example` to `config.conf` if it doesn't exist
- lets `m0hcine24` run `arp-scan` via sudo without a password (`/etc/sudoers.d/caisse`), for the MAC fallback
- removes the `/etc/cron.d/caisse` job left by earlier installs

It is safe to re-run.

## Scheduling and reporting

Scheduling lives in the admin panel, not in this repo: on the device's
`haoudej-script` installation, open **Schedules** and add e.g. *Every day 23:00*
with `--sync`. [sty-software-manager](https://github.com/storeyescoffee/sty-software-manager)
writes it to `/etc/cron.d/sty-schedule`, and cron runs `main.py` directly.

Every run started by the manager — scheduled, or on demand from the panel's Run
button — has `STY_COMMAND_ID` and `STY_MANAGER` in its environment. At exit,
`main.py` sends its log and exit code to the panel through
`$STY_MANAGER/main.py --report`; the run shows up there flagged *On demand*,
*Cron* or *Scheduled*. A manual run (neither variable set) doesn't report. The
30-minute `--upload` retries queued via `at` don't report either, so they never
overwrite the original run's result.

## Configure

Edit `config.conf` with the database credentials and API settings; see the
comments in `config.conf.example`. Then run once by hand to check it works:

```sh
python3 main.py --sync
```

Logs go to `logs/YYYY-MM-DD.log`.

## Usage

```sh
python3 main.py --sync              # export + upload today (what the daily schedule runs)
python3 main.py --date 2026-07-01   # a specific day
python3 main.py --reconcile [N]     # each of the last N days (default 30)
python3 main.py --upload            # re-send the existing results.csv
python3 main.py --resolve-mac       # print the till's IP, found by MAC
```

If an upload fails, the script retries it every 30 minutes via `at` until it succeeds.
