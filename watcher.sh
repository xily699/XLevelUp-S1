#!/data/data/com.termux/files/usr/bin/bash
###############################################################################
# XLevelUp-Panel Watcher — نسخه حرفه‌ای
# سینک آینه‌ای کامل پوشه /storage/emulated/0/XLevelUp-Panel با ریپوی گیت‌هاب
# - هر فایل جدید/تغییر یافته -> اضافه/آپدیت در ریپو
# - اگر پوشه خالی شد -> همه فایل‌های ریپو حذف می‌شن (mirror واقعی)
# - پوش با GitHub CLI (gh) و retry حرفه‌ای
# - خوداحیایی در Termux:Boot از هر مسیری که اجرا بشه
###############################################################################

set -uo pipefail

CONFIG_FILE="$HOME/.xlevelup.conf"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "فایل تنظیمات پیدا نشد: $CONFIG_FILE"
    echo "اول install.sh رو اجرا کن."
    exit 1
fi
# shellcheck source=/dev/null
source "$CONFIG_FILE"

: "${WATCH_DIR:?WATCH_DIR تنظیم نشده}"
: "${REPO_DIR:?REPO_DIR تنظیم نشده}"
: "${GIT_BRANCH:=main}"
: "${POLL_INTERVAL:=3}"
: "${LOG_FILE:=$HOME/xlevelup/watcher.log}"
: "${STATUS_FILE:=$HOME/xlevelup/status.env}"
: "${GITHUB_USERNAME:=نامشخص}"
: "${BOOT_SCRIPT:=$HOME/.termux/boot/start-xlevelup.sh}"
: "${WATCHER_PATH:=$HOME/xlevelup/watcher.sh}"

LOCK_FILE="${TMPDIR:-/tmp}/xlevelup_watcher.lock"

mkdir -p "$(dirname "$LOG_FILE")" "$(dirname "$STATUS_FILE")"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

write_status() {
    cat > "$STATUS_FILE" <<EOF
PID=$$
START_TIME="$START_TIME"
LAST_CYCLE_TIME="$(date '+%Y-%m-%d %H:%M:%S')"
LAST_SYNC_TIME="${LAST_SYNC_TIME:-هرگز}"
LAST_ACTION="${LAST_ACTION:-در انتظار اولین چرخه}"
ADDED_COUNT=${ADDED_COUNT:-0}
MODIFIED_COUNT=${MODIFIED_COUNT:-0}
DELETED_COUNT=${DELETED_COUNT:-0}
TOTAL_FILES=${TOTAL_FILES:-0}
CYCLE_COUNT=${CYCLE_COUNT:-0}
LAST_ERROR="${LAST_ERROR:-}"
GITHUB_USERNAME="$GITHUB_USERNAME"
REPO_DIR="$REPO_DIR"
WATCH_DIR="$WATCH_DIR"
GIT_BRANCH="$GIT_BRANCH"
POLL_INTERVAL=$POLL_INTERVAL
EOF
}

# --- جلوگیری از اجرای همزمان چند نمونه ---
if [ -f "$LOCK_FILE" ]; then
    OLD_PID=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
        log "یک نمونه دیگه از watcher در حال اجراست (PID $OLD_PID). خروج."
        exit 1
    fi
fi
echo $$ > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"; log "Watcher متوقف شد."' EXIT

# --- خوداحیایی در بوت، فرقی نمی‌کنه از کجا اجرا شده ---
ensure_boot_registered() {
    mkdir -p "$(dirname "$BOOT_SCRIPT")"
    if [ ! -f "$BOOT_SCRIPT" ] || ! grep -q "$WATCHER_PATH" "$BOOT_SCRIPT" 2>/dev/null; then
        cat > "$BOOT_SCRIPT" <<EOF
#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
nohup $WATCHER_PATH >> $LOG_FILE 2>&1 &
EOF
        chmod +x "$BOOT_SCRIPT"
        log "اسکریپت بوت ثبت/به‌روزرسانی شد: $BOOT_SCRIPT"
    fi
}
ensure_boot_registered

if [ ! -d "$WATCH_DIR" ]; then
    log "پوشه مانیتور وجود نداشت، ساخته شد: $WATCH_DIR"
    mkdir -p "$WATCH_DIR"
fi

if [ ! -d "$REPO_DIR/.git" ]; then
    log "خطا: $REPO_DIR یک ریپازیتوری گیت نیست. install.sh رو اجرا کن."
    exit 1
fi

cd "$REPO_DIR" || exit 1

START_TIME="$(date '+%Y-%m-%d %H:%M:%S')"
LAST_SYNC_TIME="هرگز"
LAST_ACTION="راه‌اندازی اولیه"
ADDED_COUNT=0
MODIFIED_COUNT=0
DELETED_COUNT=0
TOTAL_FILES=0
CYCLE_COUNT=0
LAST_ERROR=""
write_status

log "=== Watcher شروع شد | PID=$$ | پوشه: $WATCH_DIR | ریپو: $REPO_DIR@$GIT_BRANCH ==="

sync_cycle() {
    CYCLE_COUNT=$((CYCLE_COUNT+1))

    # سینک آینه‌ای کامل: هرچی تو WATCH_DIR هست دقیقا همون تو REPO_DIR باشه
    if command -v rsync >/dev/null 2>&1; then
        rsync -a --delete --exclude='.git' --exclude='.watcher_state' \
              "$WATCH_DIR/" "$REPO_DIR/" 2>>"$LOG_FILE"
    else
        # fallback بدون rsync
        find "$REPO_DIR" -mindepth 1 -not -path "$REPO_DIR/.git*" -delete 2>>"$LOG_FILE"
        cp -a "$WATCH_DIR/." "$REPO_DIR/" 2>>"$LOG_FILE"
    fi

    TOTAL_FILES=$(find "$WATCH_DIR" -type f 2>/dev/null | wc -l | tr -d ' ')

    # بررسی تغییرات با git status
    local status_out
    status_out=$(git status --porcelain 2>>"$LOG_FILE")

    if [ -z "$status_out" ]; then
        LAST_ACTION="بدون تغییر (${TOTAL_FILES} فایل، همگام)"
        write_status
        return 0
    fi

    ADDED_COUNT=$(echo "$status_out" | grep -c '^??' || true)
    MODIFIED_COUNT=$(echo "$status_out" | grep -c '^ M\|^M ' || true)
    DELETED_COUNT=$(echo "$status_out" | grep -c '^ D\|^D ' || true)

    log "تغییرات شناسایی شد -> جدید:$ADDED_COUNT تغییر:$MODIFIED_COUNT حذف:$DELETED_COUNT"
    log "جزئیات:"
    echo "$status_out" | while read -r line; do log "  $line"; done

    if [ "$TOTAL_FILES" -eq 0 ]; then
        commit_msg="🧹 پاکسازی خودکار: پوشه خالی شد، همه فایل‌های ریپو حذف شدند"
    else
        commit_msg="🔄 سینک خودکار XLevelUp-Panel | جدید:$ADDED_COUNT تغییر:$MODIFIED_COUNT حذف:$DELETED_COUNT | $(date '+%Y-%m-%d %H:%M:%S')"
    fi

    git add -A >> "$LOG_FILE" 2>&1
    if ! git commit -m "$commit_msg" >> "$LOG_FILE" 2>&1; then
        LAST_ERROR="کامیت ناموفق بود"
        LAST_ACTION="خطا در commit"
        write_status
        return 1
    fi

    # تلاش برای rebase قبل از پوش برای جلوگیری از conflict
    git pull --rebase origin "$GIT_BRANCH" >> "$LOG_FILE" 2>&1 || {
        log "هشدار: pull --rebase ناموفق بود، ادامه با تلاش پوش مستقیم"
        git rebase --abort >> "$LOG_FILE" 2>&1 || true
    }

    local push_ok=0
    for i in 1 2 3 4 5; do
        if git push origin "$GIT_BRANCH" >> "$LOG_FILE" 2>&1; then
            push_ok=1
            break
        fi
        log "پوش ناموفق (تلاش $i از ۵)، ${i}0 ثانیه صبر..."
        sleep $((i*10))
    done

    if [ "$push_ok" -eq 1 ]; then
        LAST_SYNC_TIME="$(date '+%Y-%m-%d %H:%M:%S')"
        LAST_ACTION="✔ پوش موفق: $commit_msg"
        LAST_ERROR=""
        log "✔ پوش با موفقیت انجام شد."
    else
        LAST_ERROR="پوش پس از ۵ تلاش شکست خورد — اتصال اینترنت یا توکن رو چک کن"
        LAST_ACTION="✘ خطا در پوش"
        log "✘ پوش پس از ۵ تلاش شکست خورد."
    fi

    write_status
}

while true; do
    if sync_cycle; then
        :
    else
        log "چرخه با خطا مواجه شد اما watcher متوقف نمی‌شه."
    fi
    # هر ۲۰ چرخه یک‌بار مطمئن شو ثبت بوت هنوز پابرجاست
    if [ $(( CYCLE_COUNT % 20 )) -eq 0 ]; then
        ensure_boot_registered
    fi
    sleep "$POLL_INTERVAL"
done
