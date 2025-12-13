#!/bin/bash

# ================== 配置区 ==================
PORT=8521
# ==========================================

# 查找匹配的进程 PID
PIDS=$(pgrep -f "streamlit run app.py.*--server.port=$PORT")

if [ -z "$PIDS" ]; then
    echo "⚠️  未找到在端口 $PORT 上运行的 Streamlit 应用"
    exit 0
fi

echo "正在终止以下进程："
echo "$PIDS"

# 逐个终止
for pid in $PIDS; do
    kill "$pid" 2>/dev/null
done

# 等待 3 秒，若未退出则强制 kill
sleep 3
PIDS_LEFT=$(pgrep -f "streamlit run app.py.*--server.port=$PORT")
if [ -n "$PIDS_LEFT" ]; then
    echo "仍有进程未退出，强制终止..."
    kill -9 $PIDS_LEFT 2>/dev/null
fi

echo "✅ Streamlit 应用（端口 $PORT）已停止"