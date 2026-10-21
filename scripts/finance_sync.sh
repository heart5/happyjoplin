#!/bin/bash
# 财务账本每日增量同步（当月 + 上月），失败也刷新运行状态笔记
set -uo pipefail
cd "$(dirname "$0")/.."

PY=/etc/miniconda3/envs/lsp/bin/python
[ -x "$PY" ] || PY=python3
LOG_DIR=log
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/finance_sync_$(date +%Y%m%d).log"

{
  echo "=== $(date '+%Y-%m-%d %H:%M:%S') finance_sync ==="
  "$PY" -m life.ledger.cli sync "$@"
  RC=$?
  "$PY" -m life.ledger.cli publish status || true
  echo "=== exit $RC ==="
} >> "$LOG" 2>&1

exit $RC
