#!/bin/bash
# 微信系统健康巡检 — systemd timer 定时执行
# 报告写入 Joplin「微信系统运行巡检 (/tmp)」后 joplin sync

set -e
HOST=$(hostname)
NOW=$(date "+%Y-%m-%d %H:%M:%S")
PROJ="/home/baiyefeng/codebase/happyjoplin"
LOGDIR="$PROJ/log"
PYTHON="/usr/miniconda3/envs/newlsp/bin/python"
JOPLIN="/usr/bin/joplin"

# 1. 进程状态
WEBCHAT_PID=$(ps -eo pid,args | grep "webchat.py" | grep -v grep | awk '{print $1}')
if [ -n "$WEBCHAT_PID" ]; then
    PS_INFO=$(ps -o pid,rssize,vsize,pcpu,lstart= -p $WEBCHAT_PID --no-headers 2>/dev/null)
    PID=$(echo "$PS_INFO" | awk '{print $1}')
    RSS_KB=$(echo "$PS_INFO" | awk '{print $2}')
    RSS_MB=$(awk "BEGIN {printf \"%.1f\", $RSS_KB/1024}")
    VSZ_KB=$(echo "$PS_INFO" | awk '{print $3}')
    VSZ_MB=$(awk "BEGIN {printf \"%.1f\", $VSZ_KB/1024}")
    CPU=$(echo "$PS_INFO" | awk '{print $4}')
    LSTART=$(echo "$PS_INFO" | awk '{$1=$2=$3=$4=""; print substr($0,5)}' | xargs)
    SESSION_START=$(date -d "$LSTART" "+%Y-%m-%d %H:%M:%S" 2>/dev/null || echo "$LSTART")
    START_TS=$(date -d "$SESSION_START" +%s 2>/dev/null)
    NOW_TS=$(date +%s)
    if [ -n "$START_TS" ]; then
        DUR_SEC=$((NOW_TS - START_TS))
        DUR_DAY=$((DUR_SEC/86400))
        DUR_HOUR=$(((DUR_SEC%86400)/3600))
        DUR_MIN=$(((DUR_SEC%3600)/60))
        DURATION="${DUR_DAY}天${DUR_HOUR}小时${DUR_MIN}分钟"
    else
        DURATION="N/A"
    fi
    PROC_OK=true
else
    PROC_OK=false
    PID="N/A"; RSS_MB="N/A"; VSZ_MB="N/A"; CPU="N/A"
    SESSION_START="N/A"; DURATION="N/A"
fi

# 2. 日志扫描 — 自会话起点起
if [ -n "$START_TS" ] && [ "$START_TS" != "N/A" ]; then
    SCAN_TS=$START_TS
else
    SCAN_TS=$(date -d "7 days ago" +%s)
fi

> /tmp/wechat_scan_tmp.txt
for f in "$LOGDIR/happyjoplin.log" "$LOGDIR/happyjoplin.log.1"; do
    [ -f "$f" ] || continue
    awk -v ts="$SCAN_TS" '{
        match($0, /^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}/)
        if (RLENGTH > 0) {
            ts_str = substr($0, RSTART, RLENGTH)
            cmd = "date -d \"" ts_str "\" +%s"
            cmd | getline log_ts
            close(cmd)
            if (log_ts >= ts) print
        }
    }' "$f" >> /tmp/wechat_scan_tmp.txt
done
ALL_LINES=$(wc -l < /tmp/wechat_scan_tmp.txt 2>/dev/null | tr -d ' ')

# 3. 错误扫描
ERR_COUNT=$(grep -ciE "error|exception|traceback|typeerror" /tmp/wechat_scan_tmp.txt 2>/dev/null || echo 0)
ERR_NUM=$(echo "$ERR_COUNT" | head -1 | tr -cd '0-9')
[ -z "$ERR_NUM" ] && ERR_NUM=0
ERR_RECENT=$(grep -iE "error|exception|traceback|typeerror" /tmp/wechat_scan_tmp.txt 2>/dev/null | tail -5 | sed 's/^/    /')
[ -z "$ERR_RECENT" ] && ERR_RECENT="    无"

# 4. 待配置公众号
IGNORED_RAW=$(grep "待配置公众号" /tmp/wechat_scan_tmp.txt 2>/dev/null | sed 's/.*不在ignoredmplist）: //' | sort -u)
IGNORED_NAMES=$(echo "$IGNORED_RAW" | grep -v "^$" || echo "无")

# 5. 分享未匹配公众号
SHARING_RAW=$(grep "公众号信息" /tmp/wechat_scan_tmp.txt 2>/dev/null | grep -v "待配置" | sed 's/.*公众号信息[:：] *//' | sort -u)
SHARING_NAMES=$(echo "$SHARING_RAW" | grep -v "^$" || echo "无")

# 6. 活跃度
DISPATCH_CNT=$(grep -ci "dispatch" /tmp/wechat_scan_tmp.txt 2>/dev/null | head -1 | tr -cd '0-9')
[ -z "$DISPATCH_CNT" ] && DISPATCH_CNT=0
FILEREPLY_CNT=$(grep -ci "fileetc_reply" /tmp/wechat_scan_tmp.txt 2>/dev/null | head -1 | tr -cd '0-9')
[ -z "$FILEREPLY_CNT" ] && FILEREPLY_CNT=0
SHARING_CNT=$(grep -ci "sharing_reply" /tmp/wechat_scan_tmp.txt 2>/dev/null | head -1 | tr -cd '0-9')
[ -z "$SHARING_CNT" ] && SHARING_CNT=0
MSG_CNT=$(grep -ciE "收到消息|收到文件|收到图片|收到视频|收到语音" /tmp/wechat_scan_tmp.txt 2>/dev/null | head -1 | tr -cd '0-9')
[ -z "$MSG_CNT" ] && MSG_CNT=0

# 7. 公众号汇总
ALL_MP=$( (echo "$IGNORED_RAW"; echo "$SHARING_RAW") | grep -v "^无$" | grep -v "^$" | sort -u )
ALL_MP_CNT=$(echo "$ALL_MP" | grep -c . 2>/dev/null || echo 0)

# 8. 综合评估
if $PROC_OK; then
    if [ "$ERR_NUM" -gt 50 ]; then
        ASSESSMENT="进程运行中但错误较多($ERR_NUM条)，建议排查"
    elif [ "$ERR_NUM" -gt 10 ]; then
        ASSESSMENT="整体正常，有少量错误($ERR_NUM条)"
    elif [ "$ERR_NUM" -gt 0 ]; then
        ASSESSMENT="运行健康，错误极少($ERR_NUM条)"
    else
        ASSESSMENT="运行健康，无错误"
    fi
else
    ASSESSMENT="进程不存在！需要立即处理"
fi

# 格式化
if [ "$ALL_MP_CNT" -gt 0 ]; then
    MP_FORMATTED=$(echo "$ALL_MP" | sed 's/^/- /')
    MP_CSV=$(echo "$ALL_MP" | tr '\n' ',' | sed 's/,$//; s/,/, /g')
else
    MP_FORMATTED="- 无"
    MP_CSV="无"
fi

if [ "$IGNORED_NAMES" != "无" ] && [ -n "$IGNORED_NAMES" ]; then
    IGN_FORMATTED=$(echo "$IGNORED_NAMES" | sed 's/^/- /')
else
    IGN_FORMATTED="- 无"
fi

if [ "$SHARING_NAMES" != "无" ] && [ -n "$SHARING_NAMES" ]; then
    SHA_FORMATTED=$(echo "$SHARING_NAMES" | sed 's/^/- /')
else
    SHA_FORMATTED="- 无"
fi

# 9. 生成报告
cat > /tmp/wechat_health_check.md << EOF
# 微信系统运行巡检

**巡检时间**: $NOW
**服务器**: $HOST

## 进程状态

| 指标 | 值 |
|------|-----|
| PID | $PID |
| 内存(RSS) | ${RSS_MB} MB |
| 虚拟内存 | ${VSZ_MB} MB |
| CPU | ${CPU}% |
| 运行时长 | ${DURATION} |
| 会话起点 | ${SESSION_START} |

## 错误扫描

- 扫描范围: 会话起点至今，共 ${ALL_LINES} 行日志
- 错误/异常计数: **${ERR_NUM}** 条

### 最近5条
${ERR_RECENT}

## 待配置公众号

共 **${ALL_MP_CNT}** 个未配置公众号:

${MP_FORMATTED}

**配置用逗号分隔**: ${MP_CSV}

### 来源说明

**ignoredmplist 拦截** (公众号发消息但不在白名单):
${IGN_FORMATTED}

**分享消息未匹配** (分享卡片中出现的未研究公众号):
${SHA_FORMATTED}

## 消息流活跃度

| 维度 | 计数 |
|------|------|
| dispatch 分发 | ${DISPATCH_CNT} |
| fileetc_reply 文件回复 | ${FILEREPLY_CNT} |
| sharing_reply 分享回复 | ${SHARING_CNT} |
| 收消息/文件/图片 | ${MSG_CNT} |

## 综合评估

${ASSESSMENT}

---
*自动巡检 · ${NOW}*
EOF

# 10. 写入Joplin并同步
cd "$PROJ"
$PYTHON etc/md2note.py /tmp/wechat_health_check.md
$JOPLIN sync

# 11. 清理
rm -f /tmp/wechat_scan_tmp.txt

# 12. 异常退出码
if ! $PROC_OK; then
    echo "FATAL: webchat进程不存在"
    exit 1
elif [ "$ERR_NUM" -gt 0 ]; then
    echo "WARN: 发现${ERR_NUM}条错误"
fi
