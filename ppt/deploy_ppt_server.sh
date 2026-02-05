#!/bin/bash
# ============================================================
# Paper Trade Docker 部署脚本
# 用法: ./deploy_ppt_server.sh [命令]
# ============================================================

set -e
cd "$(dirname "$0")"

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# Docker Compose 配置文件
COMPOSE_FILE="docker/docker-compose.yml"
DC="docker compose -f $COMPOSE_FILE"

show_help() {
    echo -e "${CYAN}Paper Trade Docker 部署工具${NC}"
    echo ""
    echo "用法: ./deploy_ppt_server.sh [命令]"
    echo ""
    echo "命令:"
    echo "  init        初始化环境 (复制 .env)"
    echo "  build       构建镜像"
    echo "  up          启动服务"
    echo "  upd         后台启动服务"
    echo "  down        停止服务"
    echo "  restart     重启服务"
    echo "  logs        查看日志"
    echo "  shell       进入容器"
    echo "  ps          查看状态"
    echo "  clean       清理容器和镜像"
    echo "  freeport    释放端口 11182"
    echo "  help        显示帮助"
    echo ""
    echo "示例:"
    echo "  ./deploy_ppt_server.sh init   # 首次使用"
    echo "  ./deploy_ppt_server.sh up     # 启动服务"
    echo "  ./deploy_ppt_server.sh logs   # 查看日志"
}

cmd_init() {
    echo -e "${GREEN}初始化环境...${NC}"
    if [ ! -f .env ]; then
        cp env.example .env
        echo -e "${YELLOW}已创建 .env 文件，请编辑配置:${NC}"
        echo "  vim .env"
    else
        echo ".env 已存在"
    fi
    mkdir -p run/db run/logs run/opentimestamps
    echo -e "${GREEN}完成!${NC}"
}

cmd_build() {
    echo -e "${GREEN}构建镜像...${NC}"
    $DC build
}

cmd_up() {
    echo -e "${GREEN}启动服务...${NC}"
    $DC up --build
}

cmd_upd() {
    echo -e "${GREEN}后台启动服务...${NC}"
    $DC up -d --build
    echo -e "${CYAN}访问: http://localhost:11182${NC}"
}

cmd_down() {
    echo -e "${YELLOW}停止服务...${NC}"
    $DC down
}

cmd_restart() {
    echo -e "${GREEN}重启服务...${NC}"
    $DC restart
}

cmd_logs() {
    $DC logs -f
}

cmd_shell() {
    echo -e "${GREEN}进入容器...${NC}"
    $DC exec web bash
}

cmd_ps() {
    $DC ps
}

cmd_clean() {
    echo -e "${YELLOW}清理容器和镜像...${NC}"
    $DC down --rmi local -v
    echo -e "${GREEN}完成!${NC}"
}

cmd_freeport() {
    PORT=11182
    echo -e "${YELLOW}释放端口 $PORT...${NC}"
    
    # 先清理 Docker 容器
    $DC down 2>/dev/null || true
    
    # 查找占用端口的进程
    PIDS=$(sudo lsof -t -i:$PORT 2>/dev/null || true)
    
    if [ -z "$PIDS" ]; then
        echo -e "${GREEN}端口 $PORT 已释放${NC}"
    else
        echo "发现进程: $PIDS"
        sudo kill -9 $PIDS 2>/dev/null || true
        sleep 1
        echo -e "${GREEN}端口 $PORT 已释放${NC}"

        docker stop $(docker ps -q)
    fi
}

# 主入口
case "${1:-help}" in
    init)    cmd_init ;;
    build)   cmd_build ;;
    up)      cmd_up ;;
    upd)     cmd_upd ;;
    down)    cmd_down ;;
    restart) cmd_restart ;;
    logs)    cmd_logs ;;
    shell)   cmd_shell ;;
    ps)      cmd_ps ;;
    clean)    cmd_clean ;;
    freeport) cmd_freeport ;;
    help|*)   show_help ;;
esac
