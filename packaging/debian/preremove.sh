#!/bin/sh
set -e

if [ "$1" = "remove" ] || [ "$1" = "deconfigure" ]; then
	if command -v systemctl >/dev/null 2>&1; then
		systemctl stop upag.service >/dev/null 2>&1 || true
		systemctl disable upag.service >/dev/null 2>&1 || true
	fi
	if [ -x /etc/init.d/upag ]; then
		/etc/init.d/upag stop >/dev/null 2>&1 || true
	fi
fi

exit 0
