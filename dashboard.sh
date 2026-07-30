#!/data/data/com.termux/files/usr/bin/bash
# XLevelUp Auto-Deploy - Professional Dashboard
set -uo pipefail

BASE_DIR="$HOME/.xlevelup-deploy"
CONF_FILE="$BASE_DIR/config.env"
PID_FILE="$BASE_DIR/watcher.pid"
DEPLOY_LOG="$BASE_DIR/logs/deploy.log"
WATCHER_LOG="$BASE_DIR/logs/watcher.log"

[ -f "$CONF_FILE" ] || { echo "Config not found. Run setup.sh first."; exit 1; }
source "$CONF_FILE"

C_G="\e[32m"; C_R="\e[31m"; C_Y="\e[33m"; C_C="\e[36m"; C_N="\e[0m"; C_B="\e[1m"
W=70

status_watcher() {
    if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE" 2>/dev/null)" 2>/dev/null; then
        echo -e "${C_G}● RUNNING${C_N} (PID $(cat "$PID_FILE"))"
    else
        echo -e "${C_R}● STOPPED${C_N}"
    fi
}

last_deploy() {
    tail -n 1 "$DEPLOY_LOG" 2>/dev/null | cut -d'|' -f2- | sed 's/^ *//' || echo "No deploy yet"
}

last_commit() {
    cd "$CLONE_DIR" 2>/dev/null && git log -1 --pretty=format:"%s" 2>/dev/null || echo "N/A"
}

internet_status() {
    if ping -c1 -W2 8.8.8.8 >/dev/null 2>&1; then
        echo -e "${C_G}Online${C_N}"
    else
        echo -e "${C_R}Offline${C_N}"
    fi
}

draw() {
    clear
    echo -e "${C_C}${C_B}╔$(printf '═%.0s' $(seq 1 $W))╗${C_N}"
    printf "${C_C}${C_B}║${C_N}  %-*s${C_C}${C_B}║${C_N}\n" $((W-2)) "🚀 XLevelUp Auto-Deploy Dashboard"
    echo -e "${C_C}${C_B}╠$(printf '═%.0s' $(seq 1 $W))╣${C_N}"
    printf "${C_C}║${C_N}  Repository   : %-*s${C_C}║${C_N}\n" $((W-17)) "$GH_REPO ($GH_BRANCH)"
    printf "${C_C}║${C_N}  Project Dir  : %-*s${C_C}║${C_N}\n" $((W-17)) "$(basename "$PROJECT_DIR")"
    printf "${C_C}║${C_N}  Watcher      : %-*b${C_C}║${C_N}\n" $((W-14)) "$(status_watcher)"
    printf "${C_C}║${C_N}  Internet     : %-*b${C_C}║${C_N}\n" $((W-14)) "$(internet_status)"
    printf "${C_C}║${C_N}  Last Deploy  : %-*s${C_C}║${C_N}\n" $((W-17)) "$(last_deploy | head -c $((W-17)))"
    printf "${C_C}║${C_N}  Last Commit  : %-*s${C_C}║${C_N}\n" $((W-17)) "$(last_commit | head -c $((W-17)))"
    echo -e "${C_C}${C_B}╚$(printf '═%.0s' $(seq 1 $W))╝${C_N}"
    echo
    echo -e "  ${C_Y}1)${C_N} Start Watcher (background)"
    echo -e "  ${C_Y}2)${C_N} Stop Watcher"
    echo -e "  ${C_Y}3)${C_N} Deploy Now (with custom commit message)"
    echo -e "  ${C_Y}4)${C_N} View Live Logs"
    echo -e "  ${C_Y}5)${C_N} Rollback to Previous Commit"
    echo -e "  ${C_Y}6)${C_N} Edit Configuration"
    echo -e "  ${C_Y}7)${C_N} Switch Branch"
    echo -e "  ${C_Y}8)${C_N} Force Sync (overwrite remote)"
    echo -e "  ${C_Y}0)${C_N} Exit"
    echo
}

is_running() { [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE" 2>/dev/null)" 2>/dev/null; }

while true; do
    draw
    read -rp "Your choice: " CH
    case "$CH" in
        1)
            if is_running; then
                echo -e "${C_Y}Watcher already running.${C_N}"
            else
                nohup bash "$BASE_DIR/supervisor.sh" >>"$BASE_DIR/logs/supervisor.log" 2>&1 &
                disown
                sleep 1
                echo -e "${C_G}Watcher started in background.${C_N}"
            fi
            sleep 1
            ;;
        2)
            pkill -f "supervisor.sh" 2>/dev/null
            pkill -f "watcher.sh" 2>/dev/null
            [ -f "$PID_FILE" ] && kill "$(cat "$PID_FILE")" 2>/dev/null
            rm -f "$PID_FILE"
            echo -e "${C_R}Watcher stopped.${C_N}"
            sleep 1
            ;;
        3)
            read -rp "Commit message (optional, press Enter for auto): " MSG
            if [ -n "$MSG" ]; then
                bash "$BASE_DIR/deploy.sh" "$MSG"
            else
                bash "$BASE_DIR/deploy.sh"
            fi
            read -rp "Press Enter to continue..."
            ;;
        4)
            echo -e "${C_Y}(Press Ctrl+C to exit logs)${C_N}"
            sleep 1
            tail -n 50 -f "$DEPLOY_LOG"
            ;;
        5)
            bash "$BASE_DIR/rollback.sh"
            read -rp "Press Enter to continue..."
            ;;
        6)
            bash "$BASE_DIR/setup.sh"
            source "$CONF_FILE"
            ;;
        7)
            read -rp "Enter new branch name: " NEW_BRANCH
            if [ -n "$NEW_BRANCH" ]; then
                cd "$CLONE_DIR" && git fetch origin "$NEW_BRANCH" && git checkout "$NEW_BRANCH" && sed -i "s/^GH_BRANCH=.*/GH_BRANCH=\"$NEW_BRANCH\"/" "$CONF_FILE"
                echo -e "${C_G}Switched to branch $NEW_BRANCH${C_N}"
            fi
            sleep 1
            ;;
        8)
            echo -e "${C_R}WARNING: This will overwrite remote with local files.${C_N}"
            read -rp "Continue? (y/N): " CONFIRM
            if [[ "$CONFIRM" =~ ^[Yy]$ ]]; then
                bash "$BASE_DIR/deploy.sh" "force sync"
            fi
            sleep 1
            ;;
        0) exit 0 ;;
        *) echo -e "${C_R}Invalid option.${C_N}"; sleep 1 ;;
    esac
done