#!/usr/bin/env bash
# Install / update the live WebSocket server on the EC2 box behind
# jmd.mrpscan.com. Safe to re-run: it pulls the latest code and restarts.
#
#   sudo bash server_setup.sh
#
# After the first run: put the Mega Bullion login in /opt/bhao/.env (optional),
# and add the nginx blocks from deploy/nginx-jmd.mrpscan.com.conf.
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/bhao}"
REPO="${REPO:-https://github.com/imshus/bhao.git}"
APP_USER="${APP_USER:-${SUDO_USER:-ubuntu}}"
PORT="${PORT:-7005}"

if [ "$(id -u)" -ne 0 ]; then
  echo "run with sudo" >&2
  exit 1
fi

command -v git >/dev/null || apt-get install -y git
command -v python3 >/dev/null || apt-get install -y python3

if [ -d "$APP_DIR/.git" ]; then
  # The checkout is owned by $APP_USER (chown below) and this runs as root;
  # without safe.directory git refuses with "dubious ownership" on re-runs.
  git -c safe.directory="$APP_DIR" -C "$APP_DIR" pull --ff-only
else
  git clone "$REPO" "$APP_DIR"
fi

# The Mega Bullion login stays on this server only - .env is not in git.
[ -f "$APP_DIR/.env" ] || cp "$APP_DIR/.env.example" "$APP_DIR/.env"
chown -R "$APP_USER":"$APP_USER" "$APP_DIR"
chmod 600 "$APP_DIR/.env"

sed -e "s#__APP_DIR__#$APP_DIR#g" \
    -e "s#__APP_USER__#$APP_USER#g" \
    -e "s#__PORT__#$PORT#g" \
    "$APP_DIR/deploy/gold-tracker.service" > /etc/systemd/system/gold-tracker.service

systemctl daemon-reload
systemctl enable gold-tracker >/dev/null
systemctl restart gold-tracker

for _ in $(seq 1 20); do
  if curl -fsS "http://127.0.0.1:$PORT/api" >/dev/null 2>&1; then
    echo "gold-tracker is up on 127.0.0.1:$PORT"
    exit 0
  fi
  sleep 1
done

echo "gold-tracker did not answer on 127.0.0.1:$PORT - check: journalctl -u gold-tracker -n 50" >&2
exit 1
