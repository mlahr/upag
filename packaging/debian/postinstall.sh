#!/bin/sh
set -e

if ! getent group upag >/dev/null; then
	addgroup --system upag >/dev/null
fi

if ! getent passwd upag >/dev/null; then
	adduser --system --home /var/lib/upag --no-create-home --ingroup upag --disabled-login --shell /usr/sbin/nologin upag >/dev/null
fi

install -d -o upag -g upag -m 0750 /var/lib/upag
install -d -o upag -g upag -m 0755 /run/upag
chown root:upag /etc/upag
chmod 0750 /etc/upag
chown root:upag /etc/upag/config.yaml
chmod 0640 /etc/upag/config.yaml

if command -v systemctl >/dev/null 2>&1; then
	systemctl daemon-reload || true
	systemctl enable upag.service >/dev/null 2>&1 || true
fi

if command -v update-rc.d >/dev/null 2>&1; then
	update-rc.d upag defaults >/dev/null
fi

exit 0
