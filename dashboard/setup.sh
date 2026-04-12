#!/bin/sh
# ZPulse - Developed by acidvegas in Python (https://github.com/acidvegas/rackwatch)
# zpulse/dashboard/setup.sh

# Set trace, verbose, and exit on error
set -xev

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTALL_DIR="/opt/zpulse-dashboard"
SERVICE_NAME="zpulse-dashboard"

# Check if running as root
[ "$(id -u)" -ne 0 ] && { echo "Run as root: sudo $0"; exit 1; }

# Install system packages
apt-get update -qq && apt-get install -y python3-pip python3-venv curl

# Copy dashboard files to install directory
mkdir -p "$INSTALL_DIR/templates" "$INSTALL_DIR/static"
cp "$SCRIPT_DIR/dashboard.py" "$INSTALL_DIR/"
cp "$SCRIPT_DIR/requirements.txt" "$INSTALL_DIR/"
cp "$SCRIPT_DIR/templates/index.html" "$INSTALL_DIR/templates/"

# Fetch Chart.js
if [ ! -f "$INSTALL_DIR/static/chart.min.js" ]; then
    curl -sL "https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js" -o "$INSTALL_DIR/static/chart.min.js"
fi

# Create a Python virtual environment & install dependencies
python3 -m venv "$INSTALL_DIR/venv"
"$INSTALL_DIR/venv/bin/pip" install --quiet -r "$INSTALL_DIR/requirements.txt"

# Install the systemd service
cat > /etc/systemd/system/${SERVICE_NAME}.service <<EOF
[Unit]
Description=ZPulse Dashboard
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$INSTALL_DIR
ExecStart=$INSTALL_DIR/venv/bin/python $INSTALL_DIR/dashboard.py
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Reload the systemd daemon & enable & start the service
systemctl daemon-reload && systemctl enable ${SERVICE_NAME} && systemctl start ${SERVICE_NAME}

echo "ZPulse Dashboard installed to $INSTALL_DIR and running!"
echo "  Open:    http://$(hostname -I | awk '{print $1}'):8888"
echo "  Status:  systemctl status ${SERVICE_NAME}"
echo "  Logs:    journalctl -u ${SERVICE_NAME} -f"
echo "  Stop:    systemctl stop ${SERVICE_NAME}"
echo "  Restart: systemctl restart ${SERVICE_NAME}"