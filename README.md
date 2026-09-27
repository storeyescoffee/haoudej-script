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
- writes `/etc/cron.d/caisse`, which runs `main.py --sync` every day at 23:00

It is safe to re-run.

## Configure

Edit `config.conf` with the database credentials and API settings; see the
comments in `config.conf.example`. Then run once by hand to check it works:

```sh
sudo python3 main.py --sync
```

Logs go to `logs/YYYY-MM-DD.log`.

## Usage

```sh
python3 main.py --sync              # export + upload today (what cron runs)
python3 main.py --date 2026-07-01   # a specific day
python3 main.py --reconcile [N]     # each of the last N days (default 30)
python3 main.py --upload            # re-send the existing results.csv
python3 main.py --resolve-mac       # print the till's IP, found by MAC
```

If an upload fails, the script retries it every 30 minutes via `at` until it succeeds.
