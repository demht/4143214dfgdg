#!/bin/sh
set -eu
cd "$(dirname "$0")"
if [ -x .venv/bin/python ]; then
    exec .venv/bin/python bot.py "$@"
fi
exec python3 bot.py "$@"
