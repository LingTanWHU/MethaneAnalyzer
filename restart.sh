#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 先停止
"$SCRIPT_DIR/stop.sh"

echo "等待 2 秒..."
sleep 2

# 再启动
"$SCRIPT_DIR/start.sh"