#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════
# Rosetta Playground — 本地部署脚本
# 用途: 在 21.6.101.185 上直接启动 Rosetta Playground Server，对外暴露服务
# 说明: 安装依赖后直接运行 Python 服务，无需 Docker / With 平台
#
# 用法:
#   bash deploy_local.sh install              # 首次安装依赖
#   bash deploy_local.sh start-bg [port]      # 后台启动（自动重启，crash 后 3s 恢复）
#   bash deploy_local.sh stop                 # 停止后台服务
#   bash deploy_local.sh restart [port]       # 重启后台服务
#   bash deploy_local.sh status               # 查看服务状态
#   bash deploy_local.sh start [port]         # 前台启动（调试用，Ctrl+C 停止）
#   bash deploy_local.sh proxy [port]         # 配置 nginx 反向代理 (80→port)
#   bash deploy_local.sh unproxy              # 移除 nginx 反向代理
#   bash deploy_local.sh service [port]       # 生成 systemd 服务文件
#
#  Crash 恢复机制:
#   后台启动 (start-bg) 会生成一个包装器脚本 (playground-wrapper.sh)，
#   当 Python 进程意外退出时，等待 3 秒后自动重启。
#   如果持续 crash，查看日志定位原因: tail -f playground.log
#
#  停止机制 (stop):
#   先删除 PID 文件 → 包装器检测到后停止循环 → 杀掉进程树
#   如果进程卡死，stop 会强制 SIGKILL。
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

# ── 配置 ──────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DEFAULT_PORT=${PORT:-19527}
DEFAULT_CONFIG="${DEFAULT_CONFIG:-${PROJECT_DIR}/with_config.json}"
DEFAULT_OUTPUT_DIR="${DEFAULT_OUTPUT_DIR:-${PROJECT_DIR}/results}"
VENV_DIR="${VENV_DIR:-${PROJECT_DIR}/.venv}"

# ── Colors ────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ── 检查 Python ───────────────────────────────────────────────────────────
check_python() {
    if ! command -v python3 &>/dev/null; then
        log_error "python3 is required but not found"
        exit 1
    fi
    log_ok "Python: $(python3 --version)"
}

# ── 创建虚拟环境并安装依赖 ─────────────────────────────────────────────────
install_deps() {
    if [[ ! -d "$VENV_DIR" ]]; then
        log_info "Creating virtualenv at ${VENV_DIR}..."
        python3 -m venv "$VENV_DIR"
    fi

    log_info "Installing dependencies..."
    source "${VENV_DIR}/bin/activate"
    pip install --quiet --upgrade pip
    pip install --quiet -r "${PROJECT_DIR}/requirements.txt"
    # 以开发模式安装 rosetta 自身
    pip install --quiet -e "${PROJECT_DIR}"
    deactivate
    log_ok "Dependencies installed"
}

# ── 检查配置文件 ──────────────────────────────────────────────────────────
check_config() {
    if [[ ! -f "$DEFAULT_CONFIG" ]]; then
        log_error "Config file not found: ${DEFAULT_CONFIG}"
        exit 1
    fi
    log_ok "Config: ${DEFAULT_CONFIG}"
}

# ── 启动服务 ──────────────────────────────────────────────────────────────
start_server() {
    local port="${1:-$DEFAULT_PORT}"
    local config="${2:-$DEFAULT_CONFIG}"
    local output="${3:-$DEFAULT_OUTPUT_DIR}"

    mkdir -p "$output"

    log_info "Activating virtualenv..."
    source "${VENV_DIR}/bin/activate"

    log_info "Starting Rosetta Playground Server..."
    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║        Rosetta Playground Server (LOCAL MODE)           ║"
    echo "╠══════════════════════════════════════════════════════════╣"
    echo "║  Port:    ${port}                                       ║"
    echo "║  Config:  ${config}                                      ║"
    echo "║  Output:  ${output}                                      ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""

    # 使用 exec 替换当前进程，方便 systemd/supervisor 管理
    export ROSETTA_CONFIG="$config"
    exec python3 -m rosetta.playground_server \
        --config "$config" \
        --port "$port" \
        --output-dir "$output"
}

# ── 后台启动服务（自动重启） ──────────────────────────────────────────────
PID_FILE="${PROJECT_DIR}/playground.pid"
LOG_FILE="${PROJECT_DIR}/playground.log"
WRAPPER_SCRIPT="${PROJECT_DIR}/playground-wrapper.sh"

# 生成自动重启包装脚本
_generate_wrapper() {
    local port="$1"
    local config="$2"
    local output="$3"
    cat > "$WRAPPER_SCRIPT" << 'WRAPEOF'
#!/bin/bash
# Rosetta Playground — 自动重启包装器
# 当 Python 服务 crash 时，等待 3 秒后自动重启
set -euo pipefail
PORT=__PORT__
CONFIG=__CONFIG__
OUTPUT=__OUTPUT__
VENV=__VENV__
LOG=__LOG__
PID=__PID__
TIMEOUT=__TIMEOUT__

# 启动一次并记录 PID
run_server() {
    source "${VENV}/bin/activate"
    python3 -m rosetta.playground_server \
        --config "$CONFIG" \
        --port "$PORT" \
        --output-dir "$OUTPUT"
}
export ROSETTA_CONFIG="$CONFIG"
export PYTHONUNBUFFERED=1

echo "$$" > "$PID"

while true; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting playground server on port ${PORT}..."
    if run_server >> "$LOG" 2>&1; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Server exited normally (exit 0)" >> "$LOG"
    else
        local_exit=$?
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Server CRASHED (exit ${local_exit}), restarting in 3s..." >> "$LOG"
    fi
    # 检查 PID 文件是否还在（用户 stop 时会删除）
    if [[ ! -f "$PID" ]]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] PID file removed, exiting wrapper" >> "$LOG"
        exit 0
    fi
    sleep 3
done
WRAPEOF
    # 替换占位符
    sed -i \
        -e "s|__PORT__|${port}|g" \
        -e "s|__CONFIG__|${config}|g" \
        -e "s|__OUTPUT__|${output}|g" \
        -e "s|__VENV__|${VENV_DIR}|g" \
        -e "s|__LOG__|${LOG_FILE}|g" \
        -e "s|__PID__|${PID_FILE}|g" \
        -e "s|__TIMEOUT__||g" \
        "$WRAPPER_SCRIPT"
    chmod +x "$WRAPPER_SCRIPT"
}

start_daemon() {
    local port="${1:-$DEFAULT_PORT}"
    local config="${2:-$DEFAULT_CONFIG}"
    local output="${3:-$DEFAULT_OUTPUT_DIR}"

    # 检查是否已在运行
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        log_warn "Server is already running (PID $(cat "$PID_FILE"))."
        log_warn "  Stop first: bash deploy_local.sh stop"
        exit 1
    fi

    mkdir -p "$output" "$(dirname "$LOG_FILE")"

    # 生成自动重启包装脚本
    _generate_wrapper "$port" "$config" "$output"

    log_info "Starting in background (port ${port}) with auto-restart..."
    bash "$WRAPPER_SCRIPT" &
    local pid=$!
    # 包装器自己会写 PID 文件，但这里需要立即写入让 stop 能找到
    echo "$pid" > "$PID_FILE"

    # 等待几秒检查进程是否存活
    sleep 2
    if kill -0 "$pid" 2>/dev/null; then
        log_ok "Server started (wrapper PID $pid). Logs: $LOG_FILE"
        log_ok "Auto-restart enabled — service will recover automatically if it crashes."
        log_ok "Direct:    http://21.6.101.185:${port}"
        log_ok "Via proxy: http://21.6.101.185/  (run 'proxy' first)"
    else
        log_error "Server failed to start. Check logs: $LOG_FILE"
        tail -5 "$LOG_FILE"
        rm -f "$PID_FILE"
        exit 1
    fi
}

stop_daemon() {
    if [[ ! -f "$PID_FILE" ]]; then
        log_warn "No PID file found at $PID_FILE"
        return
    fi
    local pid
    pid=$(cat "$PID_FILE")
    log_info "Stopping server (PID $pid)..."
    # 先删 PID 文件 → 包装器检测到后自动退出 while 循环
    rm -f "$PID_FILE"
    # 杀掉包装器
    kill "$pid" 2>/dev/null || true
    sleep 0.5
    # 杀掉遗留的 Python 子进程（包装器的孙子进程）
    local child
    child=$(ps --ppid "$pid" -o pid= 2>/dev/null | xargs)
    if [[ -n "$child" ]]; then
        child=$(echo "$child" | head -1)
        kill "$child" 2>/dev/null || true
        # 如果有 Python 进程仍持有端口，强制 SIGKILL
        sleep 0.5
        kill -9 "$child" 2>/dev/null || true
    fi
    # 兜底：杀掉所有仍持有端口的 python3 playground_server 进程
    local stale_pid
    stale_pid=$(ss -tlnp 2>/dev/null | grep ':19527 ' | grep -oP 'pid=\K\d+' | head -1)
    if [[ -n "$stale_pid" ]]; then
        log_warn "Force killing stale process PID $stale_pid on port 19527..."
        kill -9 "$stale_pid" 2>/dev/null || true
    fi
    log_ok "Stopped"
    rm -f "$WRAPPER_SCRIPT" "$PID_FILE"
}

daemon_status() {
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        local pid
        pid=$(cat "$PID_FILE")
        local uptime
        uptime=$(ps -o etime= -p "$pid" 2>/dev/null | xargs)
        log_ok "Server is running (PID $pid, uptime $uptime)"
    else
        log_warn "Server is NOT running"
        [[ -f "$PID_FILE" ]] && rm -f "$PID_FILE"
    fi
}

# ── Nginx 反向代理（将端口 80 转发到内部端口） ────────────────────────────
install_nginx_proxy() {
    local port="${1:-$DEFAULT_PORT}"
    local proxy_host="${2:-21.6.101.185}"
    local upstream="127.0.0.1:${port}"

    if ! command -v nginx &>/dev/null; then
        log_info "nginx not found, installing..."
        if command -v apt-get &>/dev/null; then
            apt-get update -qq && apt-get install -y -qq nginx
        elif command -v yum &>/dev/null; then
            yum install -y -q nginx
        else
            log_error "Cannot install nginx automatically. Please install nginx first."
            exit 1
        fi
        log_ok "nginx installed"
    fi

    local conf="/etc/nginx/sites-enabled/rosetta-playground"
    # Debian/Ubuntu uses sites-enabled; RHEL/CentOS uses conf.d
    if [[ ! -d "/etc/nginx/sites-enabled" ]]; then
        conf="/etc/nginx/conf.d/rosetta-playground.conf"
    fi

    log_info "Creating nginx config: ${conf}"
    cat > /tmp/rosetta-playground-nginx.conf << NGINXEOF
server {
    listen 80;
    server_name ${proxy_host} localhost 127.0.0.1;

    # 禁止直接访问后端端口
    location / {
        proxy_pass http://${upstream};
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        # SSE 支持（不缓冲）
        proxy_buffering off;
        proxy_cache off;

        # 超时配置
        proxy_connect_timeout 60;
        proxy_send_timeout 300;
        proxy_read_timeout 300;
    }
}
NGINXEOF

    cp /tmp/rosetta-playground-nginx.conf "$conf"
    rm -f /etc/nginx/sites-enabled/default 2>/dev/null || true

    # 测试并重载 nginx
    log_info "Testing nginx configuration..."
    nginx -t || { log_error "nginx config test failed"; exit 1; }

    log_info "Reloading nginx..."
    nginx -s reload 2>/dev/null || systemctl reload nginx 2>/dev/null || systemctl restart nginx

    log_ok "nginx proxy configured: http://${proxy_host}/ → http://${upstream}"
}

uninstall_nginx_proxy() {
    local conf
    if [[ -f "/etc/nginx/sites-enabled/rosetta-playground" ]]; then
        conf="/etc/nginx/sites-enabled/rosetta-playground"
    elif [[ -f "/etc/nginx/conf.d/rosetta-playground.conf" ]]; then
        conf="/etc/nginx/conf.d/rosetta-playground.conf"
    else
        log_warn "No nginx config found for rosetta-playground"
        return
    fi

    rm -f "$conf"
    log_info "Removed nginx config: ${conf}"

    nginx -s reload 2>/dev/null || systemctl reload nginx 2>/dev/null || systemctl restart nginx
    log_ok "nginx proxy removed"
}
install_systemd_service() {
    local port="${1:-$DEFAULT_PORT}"
    local config="${2:-$DEFAULT_CONFIG}"
    local output="${3:-$DEFAULT_OUTPUT_DIR}"
    local user="${4:-root}"
    local service_name="rosetta-playground"

    local service_file="/etc/systemd/system/${service_name}.service"

    if [[ -f "$service_file" ]]; then
        log_warn "systemd service already exists: ${service_file}"
        return
    fi

    log_info "Creating systemd service: ${service_file}"
    cat > /tmp/rosetta-playground.service << SERVICEEOF
[Unit]
Description=Rosetta SQL Playground Server
After=network.target

[Service]
Type=simple
User=${user}
WorkingDirectory=${PROJECT_DIR}
Environment=PYTHONUNBUFFERED=1
Environment=ROSETTA_CONFIG=${config}
ExecStart=${VENV_DIR}/bin/python3 -m rosetta.playground_server \\
    --config ${config} \\
    --port ${port} \\
    --output-dir ${output}
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
SERVICEEOF

    log_info "To install:"
    echo "  sudo mv /tmp/rosetta-playground.service /etc/systemd/system/"
    echo "  sudo systemctl daemon-reload"
    echo "  sudo systemctl enable --now rosetta-playground"
    echo ""
    log_warn "Please review the service file before installing: cat /tmp/rosetta-playground.service"
}

# ── 主流程 ────────────────────────────────────────────────────────────────
main() {
    local action="${1:-start}"
    local port="${2:-$DEFAULT_PORT}"

    case "$action" in
        install|setup)
            check_python
            install_deps
            check_config
            log_ok "Setup complete! Run './deploy_local.sh start' to launch"
            ;;
        start|run)
            check_python
            check_config
            start_server "$port"
            ;;
        start-bg|daemon|bg)
            check_python
            check_config
            start_daemon "$port"
            ;;
        stop)
            stop_daemon
            ;;
        restart)
            stop_daemon
            sleep 1
            start_daemon "$port"
            ;;
        status)
            daemon_status
            ;;
        service)
            install_systemd_service "$port"
            ;;
        proxy)
            install_nginx_proxy "$port"
            ;;
        unproxy)
            uninstall_nginx_proxy
            ;;
        *)
            echo ""
            echo "Rosetta Playground 本地部署脚本"
            echo ""
            echo "用法:"
            echo "  bash deploy_local.sh install                # 首次安装依赖"
            echo "  bash deploy_local.sh start [port]           # 启动服务 (前台运行)"
            echo "  bash deploy_local.sh start-bg [port]        # 后台启动服务"
            echo "  bash deploy_local.sh stop                   # 停止后台服务"
            echo "  bash deploy_local.sh restart [port]         # 重启后台服务"
            echo "  bash deploy_local.sh status                 # 查看服务状态"
            echo "  bash deploy_local.sh service [port]         # 生成 systemd 服务文件"
            echo "  bash deploy_local.sh proxy [port]           # 配置 nginx 反向代理 (80→port)"
            echo "  bash deploy_local.sh unproxy                # 移除 nginx 反向代理"
            echo ""
            echo "环境变量:"
            echo "  PORT              服务端口 (默认: 19527)"
            echo "  DEFAULT_CONFIG    配置文件路径 (默认: ./with_config.json)"
            echo "  DEFAULT_OUTPUT_DIR 结果目录 (默认: ./results)"
            echo "  VENV_DIR          虚拟环境目录 (默认: ./.venv)"
            echo ""
            echo "示例:"
            echo "  bash deploy_local.sh install                # 安装"
            echo "  bash deploy_local.sh start-bg               # 后台启动 (默认端口)"
            echo "  bash deploy_local.sh start-bg 8080          # 指定端口后台启动"
            echo "  bash deploy_local.sh proxy                  # nginx 反向代理 80→19527"
            echo "  bash deploy_local.sh stop                   # 停止"
            echo "  bash deploy_local.sh status                 # 查看状态"
            echo "  bash deploy_local.sh service 19527          # 生成 service 文件"
            echo ""
            exit 0
            ;;
    esac
}

main "$@"
