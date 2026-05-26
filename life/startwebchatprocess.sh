#!/data/data/com.termux/files/usr/bin/sh
# Pixel 6 Pro Termux: webchat保活脚本（配合 --renew 续期机制）
# cron: */5 * * * * ~/codebase/happyjoplin/startwebchatprocess.sh

# --renew 续期进行中，不干预
if [ -f /tmp/webchat_renewing ]; then
    exit 0
fi

ps -fe|grep 'python life/webchat.py' |grep -v grep
if [ $? -ne 0 ]
then
    echo "start life/webchat process....."
    cd ~/codebase/happyjoplin
    nohup python life/webchat.py >> ~/downloads/lifewebchat.out 2>&1 &
else
    echo "python life/webchat.py is already running....."
fi
