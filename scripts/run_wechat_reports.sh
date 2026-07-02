#!/bin/bash
# 微信数据月报生成 & 上传 Joplin
# 每月 1 日 03:00 由 cron 触发，处理上月数据
set -e
cd "$(dirname "$0")/.."

ACCOUNT="白晔峰"
REPORT_DIR="log"
mkdir -p "$REPORT_DIR"

# 计算上月
YEAR=$(date -d '1 month ago' +%Y)
MONTH=$(date -d '1 month ago' +%m)
MONTH_LABEL="${YEAR}年${MONTH}月"

echo "=== 微信数据月报 ${MONTH_LABEL} ==="

# 1. 财务月报
echo "--- 生成财务月报 ---"
python life/wechat_finance.py --account "$ACCOUNT" --month "${YEAR}-${MONTH}" -o "${REPORT_DIR}/finance_${YEAR}${MONTH}.md"
echo "财务月报完成"

# 2. 人际关系月报
echo "--- 生成人际关系月报 ---"
python life/wechat_relationship.py --account "$ACCOUNT" --month "${YEAR}-${MONTH}" -o "${REPORT_DIR}/relationship_${YEAR}${MONTH}.md"
echo "人际关系月报完成"

# 3. 合并为完整月报
echo "--- 合并月报 ---"
COMBINED="${REPORT_DIR}/wechat_monthly_${YEAR}${MONTH}.md"

cat > "$COMBINED" << EOF
# 微信数据月报 — ${MONTH_LABEL}

> 自动生成时间：$(date '+%Y-%m-%d %H:%M')
> 数据来源：微信聊天记录合并库（wcitemsall_merged.db）

---

EOF

echo "" >> "$COMBINED"
cat "${REPORT_DIR}/finance_${YEAR}${MONTH}.md" >> "$COMBINED"

echo "" >> "$COMBINED"
echo "---" >> "$COMBINED"
echo "" >> "$COMBINED"
cat "${REPORT_DIR}/relationship_${YEAR}${MONTH}.md" >> "$COMBINED"

echo "月报合并完成: $COMBINED"

# 4. 上传到 Joplin
echo "--- 上传 Joplin ---"
python -m func.tools.md2note "$COMBINED" \
    --title "微信数据月报 - ${MONTH_LABEL}" \
    --notebook "ewmobile" \
    --quiet

echo "=== 全部完成 ==="
