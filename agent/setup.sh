#!/bin/bash
# ZPulse - Developed by acidvegas in Python (https://github.com/acidvegas/rackwatch)
# zpulse/agent/setup.sh

# Set trace, verbose, and exit on error
set -xev

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTALL_DIR="/opt/zpulse-agent"
SERVICE_NAME="zpulse-agent"

# Check if running as root & an argument is provided
[ "$(id -u)" -ne 0 ] && { echo "Run as root: sudo $0 <ip:port>"; exit 1; }
[ -z "$1"          ] && { echo "Usage: sudo $0 10.0.0.50:8888";  exit 1; }

# Set the dashboard address
DASHBOARD_ADDR="$1"

# Install system packages
apt-get update -qq && apt-get install -y dmidecode smartmontools zfsutils-linux python3-pip python3-venv

# Copy agent files to install directory
mkdir -p "$INSTALL_DIR"
cp "$SCRIPT_DIR/agent.py" "$INSTALL_DIR/"
cp "$SCRIPT_DIR/requirements.txt" "$INSTALL_DIR/"

# Create a Python virtual environment & install dependencies
python3 -m venv "$INSTALL_DIR/venv"
"$INSTALL_DIR/venv/bin/pip" install --quiet -r "$INSTALL_DIR/requirements.txt"

# Install the systemd service
cat > /etc/systemd/system/${SERVICE_NAME}.service <<EOF
[Unit]
Description=ZPulse Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$INSTALL_DIR/venv/bin/python $INSTALL_DIR/agent.py $DASHBOARD_ADDR
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Reload the systemd daemon & enable & start the service
systemctl daemon-reload && systemctl enable ${SERVICE_NAME} && systemctl start ${SERVICE_NAME}

echo "ZPulse Agent installed to $INSTALL_DIR and running!"
