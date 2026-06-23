#!/bin/bash
# ============================================================================
# Rosetta DBMS Restart Script
# 用途: 重启 21.6.101.185 上的各个 DBMS 实例
#       设置 LOCAL_MODE=true 可在本机执行（无需 SSH）
# 用法: bash restart_dbms.sh <dbms_name>
#        bash restart_dbms.sh --all
#        bash restart_dbms.sh --list
# ============================================================================
set -euo pipefail

SSH_HOST="${SSH_HOST:-21.6.101.185}"
SSH_USER="${SSH_USER:-root}"
SSH_TIMEOUT="${SSH_TIMEOUT:-10}"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=${SSH_TIMEOUT} -o BatchMode=no"
LOCAL_MODE="${LOCAL_MODE:-false}"  # 设为 true 时直接在本机执行命令

# ── 判断是否为本地执行 ──────────────────────────────────────────────────────
is_local_mode() {
    [[ "$LOCAL_MODE" == "true" ]] || [[ "$SSH_HOST" == "127.0.0.1" ]] || [[ "$SSH_HOST" == "localhost" ]]
}

# ── DBMS service definitions ─────────────────────────────────────────────
# 格式: name|port|service_command
declare -A DBMS_MAP
DBMS_MAP=(
    ["mysql-9.6"]="3306|pkill -9 mysqld 2>/dev/null; /usr/sbin/mysqld --user=mysql --daemonize"
    ["txsql-8.0"]="3307|/usr/local/mysql/bin/mysqld --user=root --port=3307 --socket=/tmp/mysql_txsql.sock --datadir=/usr/local/mysql/data --pid-file=/usr/local/mysql/data/mysqld.pid --daemonize"
    ["tidb-8.5"]="4000|pkill -9 -f \"tidb-server|tikv-server|pd-server\" 2>/dev/null; nohup tiup playground v8.5.5 --db 1 --pd 1 --kv 1 --tiflash 0 --host 0.0.0.0 --without-monitor > /tmp/tidb.log 2>&1 &"
    ["oceanbase"]="2881|su - admin -c 'obd cluster restart obcluster'"
    ["postgres-15"]="5432|systemctl restart postgresql-15"
    ["oracle"]="1521|echo 'Oracle restart requires manual intervention'"
)

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

# ── Check connectivity ────────────────────────────────────────────────────
check_connectivity() {
    if is_local_mode; then
        # 本地模式只需检查机器本身
        return 0
    fi
    ssh ${SSH_OPTS} "${SSH_USER}@${SSH_HOST}" "echo ok" >/dev/null 2>&1
}

# ── Check port reachability ───────────────────────────────────────────────
check_port() {
    local port="$1"
    if is_local_mode; then
        timeout 3 bash -c "echo >/dev/tcp/127.0.0.1/${port}" 2>/dev/null
    else
        timeout 3 bash -c "echo >/dev/tcp/${SSH_HOST}/${port}" 2>/dev/null
    fi
}

# ── Execute command locally or via SSH ─────────────────────────────────────
execute_cmd() {
    local cmd="$1"
    if is_local_mode; then
        log_info "Executing locally: ${cmd}"
        eval "${cmd}" 2>&1
    else
        log_info "Executing via SSH ${SSH_USER}@${SSH_HOST}: ${cmd}"
        ssh ${SSH_OPTS} "${SSH_USER}@${SSH_HOST}" "${cmd}" 2>&1
    fi
}

# ── Restart single DBMS ───────────────────────────────────────────────────
restart_dbms() {
    local name="$1"
    local entry="${DBMS_MAP[$name]:-}"

    if [[ -z "$entry" ]]; then
        log_error "Unknown DBMS: $name"
        return 2
    fi

    local port="${entry%%|*}"
    local cmd="${entry#*|}"

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log_info "Restarting ${name} (port ${port})..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # 1. Check current status
    log_info "Step 1/4: Checking current status..."
    if check_port "$port"; then
        log_warn "Port ${port} is reachable — DBMS appears to be running"
    else
        log_warn "Port ${port} is NOT reachable — DBMS appears to be DOWN"
    fi

    # 2. Execute restart command
    log_info "Step 2/4: Executing restart command..."
    log_info "  Command: ${cmd}"

    local output
    output=$(execute_cmd "${cmd}") || {
        local exit_code=$?
        log_error "Restart command failed (exit code: ${exit_code})"
        echo "  Output: ${output}"
        return 1
    }
    log_info "  Output: ${output}"

    # 3. Wait for service to start
    log_info "Step 3/4: Waiting for service to start up..."
    for i in $(seq 1 12); do
        sleep 2
        if check_port "$port"; then
            log_ok "Port ${port} is reachable after ${i} attempts (${i}×2s)"
            break
        fi
        if [[ $i -eq 12 ]]; then
            log_error "Port ${port} still NOT reachable after 24 seconds"
        fi
    done

    # 4. Final verification
    log_info "Step 4/4: Final verification..."
    sleep 2
    if check_port "$port"; then
        log_ok "SUCCESS: ${name} is now running on port ${port}"
        return 0
    else
        log_error "FAILED: ${name} is still down after restart attempt"
        return 1
    fi
}

# ── List all defined DBMS ─────────────────────────────────────────────────
list_dbms() {
    echo ""
    echo "Defined DBMS instances:"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    printf "  %-18s %-8s %s\n" "NAME" "PORT" "RESTART COMMAND"
    echo "  ────────────────── ──────── ────────────────────────────────────"
    for name in "${!DBMS_MAP[@]}"; do
        local entry="${DBMS_MAP[$name]}"
        local port="${entry%%|*}"
        local cmd="${entry#*|}"
        printf "  %-18s %-8s %s\n" "$name" "$port" "$cmd"
    done
    echo ""
}

# ── Health check all DBMS ─────────────────────────────────────────────────
health_check() {
    echo ""
    echo "DBMS Health Check:"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    for name in "${!DBMS_MAP[@]}"; do
        local entry="${DBMS_MAP[$name]}"
        local port="${entry%%|*}"
        if check_port "$port"; then
            printf "  ${GREEN}●${NC} %-18s ${GREEN}UP${NC}   (port %s)\n" "$name" "$port"
        else
            printf "  ${RED}●${NC} %-18s ${RED}DOWN${NC} (port %s)\n" "$name" "$port"
        fi
    done
    echo ""
}

# ── Main ──────────────────────────────────────────────────────────────────
main() {
    local target="${1:-}"

    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    if is_local_mode; then
        echo "║        Rosetta DBMS Restart Tool (LOCAL MODE)          ║"
    else
        echo "║        Rosetta DBMS Restart Tool                       ║"
        echo "║        Host: ${SSH_HOST}                               ║"
    fi
    echo "╚══════════════════════════════════════════════════════════╝"

    case "${target}" in
        ""|--help|-h)
            echo ""
            echo "Usage:"
            echo "  bash restart_dbms.sh <dbms_name>    Restart a single DBMS"
            echo "  bash restart_dbms.sh --all           Restart ALL defined DBMS"
            echo "  bash restart_dbms.sh --list          List all defined DBMS"
            echo "  bash restart_dbms.sh --health        Health check all DBMS"
            echo ""
            echo "Environment variables:"
            echo "  SSH_HOST      SSH host (default: 21.6.101.185)"
            echo "  SSH_USER      SSH user (default: root)"
            echo "  SSH_TIMEOUT   SSH timeout in seconds (default: 10)"
            echo "  LOCAL_MODE    Set to 'true' to run locally (no SSH)"
            exit 0
            ;;
        --list|-l)
            list_dbms
            exit 0
            ;;
        --health|-hc)
            health_check
            exit 0
            ;;
        --all|-a)
            echo ""
            if ! is_local_mode; then
                log_info "Checking SSH connectivity to ${SSH_USER}@${SSH_HOST}..."
                if ! check_connectivity; then
                    log_error "Cannot SSH to ${SSH_USER}@${SSH_HOST}"
                    log_error "Please ensure SSH key or password is set up"
                    exit 1
                fi
                log_ok "SSH connection OK"
            fi

            local total=0 success=0 failed=0
            for name in "${!DBMS_MAP[@]}"; do
                ((total++))
                if restart_dbms "$name"; then
                    ((success++))
                else
                    ((failed++))
                fi
            done
            echo ""
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "Restart Summary: ${total} total, ${success} success, ${failed} failed"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            exit $failed
            ;;
        *)
            # Check if DBMS name is defined
            if [[ -z "${DBMS_MAP[$target]:-}" ]]; then
                log_error "Unknown DBMS: ${target}"
                echo ""
                echo "Available DBMS:"
                list_dbms
                exit 2
            fi

            if ! is_local_mode; then
                log_info "Checking SSH connectivity to ${SSH_USER}@${SSH_HOST}..."
                if ! check_connectivity; then
                    log_error "Cannot SSH to ${SSH_USER}@${SSH_HOST}"
                    exit 1
                fi
                log_ok "SSH connection OK"
            fi

            restart_dbms "$target"
            exit $?
            ;;
    esac
}

main "$@"
