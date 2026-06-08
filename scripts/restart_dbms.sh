#!/bin/bash
# ============================================================================
# Rosetta DBMS Restart Script
# 用途: 通过SSH远程重启21.6.101.185上的各个DBMS实例
# 用法: bash restart_dbms.sh <dbms_name>
#        bash restart_dbms.sh --all
#        bash restart_dbms.sh --list
# ============================================================================
set -euo pipefail

SSH_HOST="${SSH_HOST:-21.6.101.185}"
SSH_USER="${SSH_USER:-root}"
SSH_TIMEOUT="${SSH_TIMEOUT:-10}"
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=${SSH_TIMEOUT} -o BatchMode=no"

# ── DBMS service definitions ─────────────────────────────────────────────
# 格式: name|port|service_command
declare -A DBMS_MAP
DBMS_MAP=(
    ["mysql-9.6"]="3306|pkill -9 mysqld; /usr/sbin/mysqld --user=mysql --daemonize"
    ["txsql-8.0"]="3307|/usr/local/mysql/bin/mysqld --user=root --port=3307 --socket=/tmp/mysql_txsql.sock --datadir=/usr/local/mysql/data --pid-file=/usr/local/mysql/data/mysqld.pid --daemonize"
    ["tidb-8.5"]="4000|systemctl restart tidb"
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

# ── Check SSH connectivity ────────────────────────────────────────────────
check_ssh() {
    ssh ${SSH_OPTS} "${SSH_USER}@${SSH_HOST}" "echo ok" >/dev/null 2>&1
}

# ── Check port reachability ───────────────────────────────────────────────
check_port() {
    local port="$1"
    timeout 3 bash -c "echo >/dev/tcp/${SSH_HOST}/${port}" 2>/dev/null
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
    log_info "Restarting ${name} (port ${port}) on ${SSH_HOST}..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # 1. Check current status
    log_info "Step 1/4: Checking current status..."
    if check_port "$port"; then
        log_warn "Port ${port} is reachable — DBMS appears to be running"
    else
        log_warn "Port ${port} is NOT reachable — DBMS appears to be DOWN"
    fi

    # 2. Execute restart command via SSH
    log_info "Step 2/4: Executing restart command via SSH..."
    log_info "  Command: ${cmd}"

    local ssh_output
    ssh_output=$(ssh ${SSH_OPTS} "${SSH_USER}@${SSH_HOST}" "${cmd}" 2>&1) || {
        local exit_code=$?
        log_error "SSH restart command failed (exit code: ${exit_code})"
        echo "  SSH output: ${ssh_output}"
        return 1
    }
    log_info "  SSH output: ${ssh_output}"

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
    echo "║        Rosetta DBMS Restart Tool                        ║"
    echo "║        Host: ${SSH_HOST}                                    ║"
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
            log_info "Checking SSH connectivity to ${SSH_USER}@${SSH_HOST}..."
            if ! check_ssh; then
                log_error "Cannot SSH to ${SSH_USER}@${SSH_HOST}"
                log_error "Please ensure SSH key or password is set up"
                exit 1
            fi
            log_ok "SSH connection OK"

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

            log_info "Checking SSH connectivity to ${SSH_USER}@${SSH_HOST}..."
            if ! check_ssh; then
                log_error "Cannot SSH to ${SSH_USER}@${SSH_HOST}"
                exit 1
            fi
            log_ok "SSH connection OK"

            restart_dbms "$target"
            exit $?
            ;;
    esac
}

main "$@"
