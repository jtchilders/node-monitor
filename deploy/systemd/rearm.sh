#!/usr/bin/env bash
# Re-enable linger and (re)start the collector unit after maintenance. Safe to repeat.
set -euo pipefail

UNIT=node-monitor-daemon.service

loginctl enable-linger "$USER"
systemctl --user daemon-reload
systemctl --user enable "$UNIT"
systemctl --user start "$UNIT"

systemctl --user status "$UNIT" --no-pager | grep -E '^\s*(Loaded|Active):' || true
echo "linger: $(loginctl show-user "$USER" --property=Linger --value)"
