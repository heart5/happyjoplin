#!/bin/bash
# 财务报告月结：强制重导上月 → 发布月报/净资产/信用卡/状态；
# 1 月额外发布上一年年报。用法：finance_reports.sh [YYYY-MM]
set -uo pipefail
cd "$(dirname "$0")/.."

PY=/etc/miniconda3/envs/lsp/bin/python
[ -x "$PY" ] || PY=python3
LOG_DIR=log
mkdir -p "$LOG_DIR"

MONTH="${1:-$(date -d "$(date +%Y-%m-01) -1 month" +%Y-%m)}"
YEAR="${MONTH%%-*}"
MONTH_NUM="${MONTH##*-}"
LOG="$LOG_DIR/finance_reports_$(date +%Y%m%d).log"

{
  echo "=== $(date '+%Y-%m-%d %H:%M:%S') finance_reports $MONTH ==="
  "$PY" -m life.ledger.cli sync --month "$MONTH" --books personal,work --force
  RC=$?
  if [ $RC -ne 0 ]; then
    echo "月结同步失败，跳过报告发布"
    "$PY" -m life.ledger.cli publish status || true
    echo "=== exit $RC ==="
    exit $RC
  fi

  "$PY" -m life.ledger.cli publish monthly --month "$MONTH" --books personal,work
  "$PY" -m life.ledger.cli publish net-worth --month "$MONTH"
  "$PY" -m life.ledger.cli publish credit-cards

  if [ "$MONTH_NUM" = "01" ]; then
    "$PY" -m life.ledger.cli publish annual --year $((YEAR - 1)) --books personal,work
  fi

  "$PY" -m life.ledger.cli publish status || true
  echo "=== done ==="
} >> "$LOG" 2>&1

exit $RC
