#!/bin/bash
# 微信系统健康巡检：SSH tc newlsp 环境，报告写入 Joplin
set -e

ssh tc 'source /usr/miniconda3/bin/activate newlsp && bash -s << SCRIPTEOF
cd ~/codebase/happyjoplin

PID=\$(pgrep -f "python life/webchat.py" 2>/dev/null)
if [ -n "\$PID" ]; then
    PROC_EXISTS=1
    CPU=\$(ps -o %cpu= -p \$PID 2>/dev/null | tr -d " ")
    MEM=\$(ps -o %mem= -p \$PID 2>/dev/null | tr -d " ")
    START_TIME=\$(ps -o lstart= -p \$PID 2>/dev/null)
    START_FMT=\$(date -d "\$START_TIME" "+%Y-%m-%d %H:%M" 2>/dev/null)
    UPTIME_SEC=\$(ps -o etimes= -p \$PID 2>/dev/null | tr -d " ")
    UPTIME_MIN=\$((UPTIME_SEC / 60))
else
    PROC_EXISTS=0
    CPU="N/A"
    MEM="N/A"
    START_TIME="N/A"
    START_FMT=""
    UPTIME_MIN="N/A"
fi

COMBINED_LOG=/tmp/combined_wc_check.log
:> \$COMBINED_LOG
for f in log/happyjoplin.log log/happyjoplin.log.1; do
    [ -f "\$f" ] && cat "\$f" >> \$COMBINED_LOG 2>/dev/null
done

if [ -n "\$START_FMT" ]; then
    FILTERED_LOG=/tmp/filtered_wc_check.log
    awk -v st="\$START_FMT" "substr(\\\$0,1,16) >= st" \$COMBINED_LOG > \$FILTERED_LOG 2>/dev/null
else
    FILTERED_LOG=\$COMBINED_LOG
fi
FILTERED_LINES=\$(wc -l < \$FILTERED_LOG 2>/dev/null || echo 0)

ERR_COUNT=\$(grep -ciE "error|exception|traceback|typeerror" \$FILTERED_LOG 2>/dev/null)
ERR_COUNT=\${ERR_COUNT:-0}
ERR_LAST5=\$(grep -iE "error|exception|traceback|typeerror" \$FILTERED_LOG 2>/dev/null | tail -5)
[ -z "\$ERR_LAST5" ] && ERR_LAST5="(无)"

IGNORED_RAW=\$(grep "待配置公众号" \$FILTERED_LOG 2>/dev/null || echo "")
SHARING_RAW=\$(grep "公众号信息" \$FILTERED_LOG 2>/dev/null | grep -v "待配置公众号" || echo "")

if [ -n "\$IGNORED_RAW" ]; then
    IGNORED_MP=\$(echo "\$IGNORED_RAW" | sed "s/.*待配置公众号[：: ]*//" | sed "s/[[:space:]]*\$//" | sort -u | grep -v "^\$")
    IGNORED_COUNT=\$(echo "\$IGNORED_MP" | sed "/^\$/d" | wc -l)
else
    IGNORED_MP="(无)"
    IGNORED_COUNT=0
fi

if [ -n "\$SHARING_RAW" ]; then
    SHARING_UNMATCHED=\$(echo "\$SHARING_RAW" | sed "s/.*公众号信息[：: ]*//" | sed "s/[[:space:]]*\$//" | sort -u | grep -v "^\$")
    SHARING_MP_COUNT=\$(echo "\$SHARING_UNMATCHED" | sed "/^\$/d" | wc -l)
else
    SHARING_UNMATCHED="(无)"
    SHARING_MP_COUNT=0
fi

DISPATCH_COUNT=\$(grep -c "dispatch" \$FILTERED_LOG 2>/dev/null || echo 0)
FILETC_COUNT=\$(grep -c "fileetc_reply" \$FILTERED_LOG 2>/dev/null || echo 0)
SHARING_REPLY_COUNT=\$(grep -c "sharing_reply" \$FILTERED_LOG 2>/dev/null || echo 0)

MEM_FREE=\$(free -h | grep "^Mem:" | tr -s " " | cut -d" " -f4)
DISK_USAGE=\$(df -h / | tail -1 | tr -s " " | cut -d" " -f5)

if [ "\$PROC_EXISTS" -eq 0 ]; then
    ASSESSMENT="**危险：webchat.py 进程不存在！**"
    STATUS="异常"
elif [ "\$ERR_COUNT" -gt 0 ]; then
    ASSESSMENT="**警告：存在 \$ERR_COUNT 条错误日志，需关注。**"
    STATUS="警告"
else
    ASSESSMENT="健康：进程运行正常，无错误日志。"
    STATUS="健康"
fi

ERR_LAST5_INDENT=\$(echo "\$ERR_LAST5" | sed "s/^/    /")
IGNORED_MP_INDENT=\$(echo "\$IGNORED_MP" | sed "s/^/    /")
SHARING_UNMATCHED_INDENT=\$(echo "\$SHARING_UNMATCHED" | sed "s/^/    /")

NOW=\$(date "+%Y-%m-%d %H:%M")
cat > /tmp/wechat_health_check.md << EOF
# 微信系统运行巡检

**巡检时间**: \$NOW
**服务器**: 腾讯云 (tc)
**综合状态**: \$STATUS

## 1. 进程状态

| 项目 | 值 |
|------|-----|
| 进程存在 | \$PROC_EXISTS (1=是) |
| PID | \$PID |
| CPU% | \$CPU |
| MEM% | \$MEM |
| 启动时间 | \$START_TIME |
| 运行时长(分钟) | \$UPTIME_MIN |

## 2. 错误扫描

扫描范围：happyjoplin.log + happyjoplin.log.1，启动后（\$START_FMT 起）共 \$FILTERED_LINES 行
ERROR/Exception/Traceback/TypeError 计数：**\$ERR_COUNT**

最近5条详情：
\$ERR_LAST5_INDENT

## 3. 待配置公众号

### ignoredmplist 来源（\$IGNORED_COUNT 个）
\$IGNORED_MP_INDENT

### 分享中未匹配来源（\$SHARING_MP_COUNT 个）
\$SHARING_UNMATCHED_INDENT

## 4. 消息流活跃度

| 类型 | 次数 |
|------|------|
| dispatch | \$DISPATCH_COUNT |
| fileetc_reply | \$FILETC_COUNT |
| sharing_reply | \$SHARING_REPLY_COUNT |

## 5. 系统资源

| 项目 | 值 |
|------|-----|
| 磁盘使用率 | \$DISK_USAGE |
| 空闲内存 | \$MEM_FREE |

## 6. 综合评估

\$ASSESSMENT
EOF

python etc/md2note.py /tmp/wechat_health_check.md --title "微信系统运行巡检 (/tmp)" 2>&1
joplin sync 2>&1

if [ "\$PROC_EXISTS" -eq 0 ]; then
    echo "!!! ALERT: webchat.py 进程不存在 !!!"
fi
if [ "\$ERR_COUNT" -gt 0 ]; then
    echo "!!! ALERT: ERR_COUNT=\$ERR_COUNT > 0 !!!"
fi
SCRIPTEOF'
