#!/bin/bash
# Paper Trade 本地启动脚本

cd "$(dirname "$0")"

# 激活 conda 环境
source ~/anaconda3/etc/profile.d/conda.sh
conda activate sai

# 创建运行时数据目录
mkdir -p run/db run/logs run/opentimestamps

echo "启动 Paper Trade 服务..."
echo "访问地址: http://0.0.0.0:11182"
echo ""

# Use a single .pycache directory under project root (script already cd'd there)
export PYTHONPYCACHEPREFIX="$(pwd)/.pycache"
export DMS_BASE_URL="http://mongkok:11183"
# If DMS returns 401, set DMS_API_KEY to match DMS server (same value as DMS's env DMS_API_KEY). Or add to .env.
export DMS_API_KEY="your-shared-secret"

# 启动 Flask (通过 socketio)
python app.py
