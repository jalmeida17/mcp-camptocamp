#!/bin/sh
# Starts the Camptocamp MCP server (stdio), e.g. inside the hermes-agent container.
# Uses the uv bundled in the Hermes image when there is one, else the uv on the PATH.
# Dependencies are pinned by camptocamp_mcp.py.lock: --locked refuses to start rather than
# resolve anything new. Python is never downloaded here (UV_PYTHON_DOWNLOADS=never).
#   C2C_PYTHON   interpreter to use (default /usr/local/bin/python3 when it exists)
#   C2C_OFFLINE  set to 1 to forbid any network access by uv (UV_OFFLINE=1): the uv cache
#                ($UV_CACHE_DIR or ~/.cache/uv) must already hold the packages
#   Manual test: run.sh selftest
set -eu

DIR=$(cd "$(dirname "$0")" && pwd)

# The last uv of the Hermes tools (glob order, no sort -V: busybox has none), else the PATH's.
UV=""
for candidate in /opt/hermes/tools/uv-*/uv; do
    if [ -x "$candidate" ]; then UV=$candidate; fi
done
if [ -z "$UV" ]; then UV=$(command -v uv || true); fi
if [ -z "$UV" ] || [ ! -x "$UV" ]; then echo "run.sh: uv not found" >&2; exit 1; fi

if [ -n "${C2C_PYTHON:-}" ]; then
    [ -x "$C2C_PYTHON" ] || { echo "run.sh: C2C_PYTHON=$C2C_PYTHON is not an executable" >&2; exit 1; }
    PY=$C2C_PYTHON
elif [ -x /usr/local/bin/python3 ]; then
    PY=/usr/local/bin/python3
else
    PY=""
fi

export UV_PYTHON_DOWNLOADS=never UV_NO_PROGRESS=1
if [ "${C2C_OFFLINE:-0}" = 1 ]; then
    export UV_OFFLINE=1
fi

if [ -n "$PY" ]; then
    set -- --python "$PY" --script "$DIR/camptocamp_mcp.py" "$@"
else
    set -- --script "$DIR/camptocamp_mcp.py" "$@"
fi
exec "$UV" run --quiet --locked "$@"
