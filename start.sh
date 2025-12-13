#!/bin/bash

PROJECT_DIR="/home/why/MethaneAnalyzer"
ENV_NAME="gdal_env"
PORT=8521
LOG_FILE="streamlit.log"

cd "$PROJECT_DIR" || { echo "错误：项目目录不存在"; exit 1; }

# 直接使用 conda 的完整路径
CONDA_BIN="/home/why/miniconda3/bin/conda"

# 使用 conda run 在指定环境中运行命令（无需 activate）
"$CONDA_BIN" run -n "$ENV_NAME" streamlit run app.py --server.port="$PORT" --server.address=0.0.0.0 --server.headless=true > "$LOG_FILE" 2>&1 &

sleep 2

if pgrep -f "streamlit run app.py.*--server.port=$PORT" > /dev/null; then
    echo "✅ Streamlit 应用已启动（端口: $PORT）"
    echo "日志: $PROJECT_DIR/$LOG_FILE"
else
    echo "❌ 启动失败，请检查日志：$LOG_FILE"
    exit 1
fi