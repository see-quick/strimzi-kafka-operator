#!/usr/bin/env bash
# =============================================================================
# nightly-perf.sh - Nightly performance test runner with regression detection
# =============================================================================
#
# Runs Strimzi perf tests on a local Kind cluster, exports results to the
# strimzi-perf-results repo, compares against baselines, and pushes.
#
# Usage:
#   ./scripts/nightly-perf.sh [options]
#
# Options:
#   --skip-cluster    Skip Kind cluster creation/deletion (use existing cluster)
#   --skip-push       Skip pushing results to remote
#   --keep-cluster    Don't delete the cluster after tests
#   --dry-run         Export and compare only (skip tests, use existing target/performance)
#
# Prerequisites:
#   - kind, kubectl, podman installed
#   - ~/Documents/Work/kind-script/kind-cluster.sh available
#   - ~/Documents/Work/strimzi-perf-results repo cloned
#
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESULTS_REPO="${RESULTS_REPO:-${HOME}/Documents/Work/strimzi-perf-results}"
KIND_SCRIPT="${KIND_SCRIPT:-${HOME}/Documents/Work/kind-script/kind-cluster.sh}"
LOG_DIR="${PROJECT_DIR}/target/perf-logs"
LOG_FILE="${LOG_DIR}/nightly-perf-$(date +%Y-%m-%d-%H%M%S).log"

SKIP_CLUSTER=false
SKIP_PUSH=false
KEEP_CLUSTER=false
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        --skip-cluster) SKIP_CLUSTER=true ;;
        --skip-push)    SKIP_PUSH=true ;;
        --keep-cluster) KEEP_CLUSTER=true ;;
        --dry-run)      DRY_RUN=true; SKIP_CLUSTER=true ;;
    esac
done

mkdir -p "${LOG_DIR}"

# ---- Log housekeeping ----
# Nightly logs older than 14 days are removed; the launchd stdout/stderr logs
# are append-only and held open by launchd, so truncate them once they exceed
# 10MB (truncation is safe with O_APPEND writers).
find "${LOG_DIR}" -name 'nightly-perf-*.log' -mtime +14 -delete 2>/dev/null || true
for launchd_log in "${LOG_DIR}"/launchd-stdout.log "${LOG_DIR}"/launchd-stderr.log; do
    if [[ -f "${launchd_log}" ]] && [[ $(stat -f%z "${launchd_log}") -gt 10485760 ]]; then
        : > "${launchd_log}"
    fi
done

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"
}

die() {
    log "ERROR: $*"
    exit 1
}

notify() {
    # Best-effort macOS notification so failed/regressed overnight runs are
    # visible in the morning without checking logs.
    osascript -e "display notification \"$2\" with title \"$1\"" 2>/dev/null || true
}

# ---- Phone summaries (optional) ----
# Sends run summaries to the user's phone. Credentials live OUTSIDE the repo
# (this branch is public), in ~/.config/strimzi-perf/notify.env:
#   TELEGRAM_BOT_TOKEN=123456:ABC...   (from @BotFather)
#   TELEGRAM_CHAT_ID=123456789         (your chat with the bot)
#   WHATSAPP_PHONE=+4207...            (number registered with CallMeBot)
#   WHATSAPP_APIKEY=123456
# Any channel with missing config is silently skipped.
for notify_env in "${HOME}/.config/strimzi-perf/notify.env" "${HOME}/.config/strimzi-perf/whatsapp.env"; do
    [[ -f "${notify_env}" ]] && source "${notify_env}"
done

send_whatsapp() {
    [[ -n "${WHATSAPP_PHONE:-}" && -n "${WHATSAPP_APIKEY:-}" ]] || return 0
    curl -sf --max-time 20 -G "https://api.callmebot.com/whatsapp.php" \
        --data-urlencode "phone=${WHATSAPP_PHONE}" \
        --data-urlencode "apikey=${WHATSAPP_APIKEY}" \
        --data-urlencode "text=$1" >>"${LOG_FILE}" 2>&1 \
        || log "WARNING: WhatsApp notification failed"
}

send_telegram() {
    [[ -n "${TELEGRAM_BOT_TOKEN:-}" && -n "${TELEGRAM_CHAT_ID:-}" ]] || return 0
    curl -sf --max-time 20 "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        --data-urlencode "chat_id=${TELEGRAM_CHAT_ID}" \
        --data-urlencode "text=$1" >>"${LOG_FILE}" 2>&1 \
        || log "WARNING: Telegram notification failed"
}

send_summary() {
    send_telegram "$1"
    send_whatsapp "$1"
}

START_TS=$(date +%s)
run_duration() {
    local mins=$(( ($(date +%s) - START_TS) / 60 ))
    echo "$((mins / 60))h$((mins % 60))m"
}

cleanup() {
    local exit_code=$?
    if [[ "${KEEP_CLUSTER}" == "false" && "${SKIP_CLUSTER}" == "false" ]]; then
        log "Cleaning up Kind cluster..."
        "${KIND_SCRIPT}" delete 2>>"${LOG_FILE}" || true
    fi
    if [[ ${exit_code} -ne 0 && "${REGRESSION_EXIT:-false}" != "true" ]]; then
        log "Script failed with exit code ${exit_code}. Logs: ${LOG_FILE}"
        notify "Strimzi nightly perf FAILED" "Exit ${exit_code}, see $(basename "${LOG_FILE}")"
        send_summary "[FAIL] Strimzi nightly perf $(date +%Y-%m-%d): script failed with exit ${exit_code} after $(run_duration). Log: $(basename "${LOG_FILE}")"
    fi
}
trap cleanup EXIT

# ---- Ensure SSH agent is available for git push ----
if [[ -z "${SSH_AUTH_SOCK:-}" ]]; then
    SSH_AUTH_SOCK=$(launchctl getenv SSH_AUTH_SOCK 2>/dev/null || true)
    export SSH_AUTH_SOCK
fi

# ---- Step 0: Update worktree to latest perf-fork ----
# BatchMode + ConnectTimeout prevent a hung ssh (no network, passphrase prompt)
# from stalling the whole run; a failed fetch falls back to the current checkout.
export GIT_SSH_COMMAND="ssh -o BatchMode=yes -o ConnectTimeout=30"
log "Updating to latest perf-fork..."
cd "${PROJECT_DIR}"
if git fetch origin perf-fork 2>>"${LOG_FILE}"; then
    git checkout --detach origin/perf-fork 2>>"${LOG_FILE}" \
        || log "WARNING: checkout of origin/perf-fork failed (local changes?), staying on current HEAD"
else
    log "WARNING: git fetch failed, running with current checkout"
fi

# ---- Step 0b: Auto-sync perf-fork with upstream main ----
# The tests deploy quay.io/strimzi/operator:latest (re-pulled each run), so the
# fork's install files must track upstream or the operator fails startup
# validation on new Kafka versions. Merge upstream in; on conflict, abort and
# run on the unsynced fork. The merged branch is pushed back to origin only
# after the systemtest build succeeds (Step 3), never in a broken state.
SYNC_MERGED=false
SYNC_BUILD_OK=false
if git fetch strimzi-https main 2>>"${LOG_FILE}"; then
    if git merge --no-edit -m "Sync perf-fork with upstream main" strimzi-https/main >>"${LOG_FILE}" 2>&1; then
        if [[ $(git rev-parse HEAD) != $(git rev-parse origin/perf-fork 2>/dev/null) ]]; then
            SYNC_MERGED=true
            log "Merged upstream main into perf-fork (will push after successful build)."
        fi
    else
        git merge --abort 2>/dev/null || true
        log "WARNING: merge with upstream main conflicted, running unsynced perf-fork"
        notify "Strimzi nightly perf" "Upstream merge conflicted, manual perf-fork sync needed"
    fi
else
    log "WARNING: fetch of upstream main failed, skipping sync"
fi
log "Now at: $(git rev-parse --short HEAD)"

# ---- Step 1: Validate prerequisites ----
log "=== Strimzi Nightly Performance Tests ==="
log "Project: ${PROJECT_DIR}"
log "Results repo: ${RESULTS_REPO}"

[[ -d "${RESULTS_REPO}" ]] || die "Results repo not found: ${RESULTS_REPO}"
command -v java >/dev/null || die "java not found"
command -v mvn >/dev/null || die "mvn not found"

if [[ "${SKIP_CLUSTER}" == "false" ]]; then
    [[ -f "${KIND_SCRIPT}" ]] || die "Kind script not found: ${KIND_SCRIPT}"
    command -v kind >/dev/null || die "kind not found"
    command -v kubectl >/dev/null || die "kubectl not found"
    command -v podman >/dev/null || die "podman not found"
fi

COMMIT_SHA=$(cd "${PROJECT_DIR}" && git rev-parse --short HEAD)
log "Commit: ${COMMIT_SHA}"

if [[ "${DRY_RUN}" == "false" ]]; then
    # ---- Step 2: Create Kind cluster ----
    if [[ "${SKIP_CLUSTER}" == "false" ]]; then
        # A cold podman machine makes every podman call crawl and Kind creation
        # flaky, so make sure it is up before touching the cluster.
        if ! podman machine inspect --format '{{.State}}' 2>/dev/null | grep -q running; then
            log "Podman machine not running, starting it..."
            podman machine start >>"${LOG_FILE}" 2>&1 || true
        fi

        # Kind cluster creation occasionally flakes (CNI apply races); retry once.
        create_cluster() {
            "${KIND_SCRIPT}" create --workers 1 --no-cloud-provider --configure-insecure 2>&1 | tee -a "${LOG_FILE}"
        }
        log "Creating Kind cluster..."
        if ! create_cluster; then
            log "Cluster creation failed, deleting leftovers and retrying once..."
            "${KIND_SCRIPT}" delete >>"${LOG_FILE}" 2>&1 || true
            sleep 30
            create_cluster || die "Kind cluster creation failed twice"
        fi
        log "Kind cluster ready."
    fi

    kubectl cluster-info >>"${LOG_FILE}" 2>&1 || die "No Kubernetes cluster accessible"

    # Set Connect build image path to the Kind registry's IP (accessible from inside the cluster)
    export CONNECT_BUILD_IMAGE_PATH=$(podman inspect -f '{{.NetworkSettings.Networks.kind.IPAddress}}' kind-registry):5000/strimzi-connect-build
    log "CONNECT_BUILD_IMAGE_PATH=${CONNECT_BUILD_IMAGE_PATH}"

    # Pre-pull the images the first test class needs, on every Kind node, in the
    # background while the build runs. A fresh :latest pull otherwise eats into
    # the first operator-deploy readiness timeout (CaRenewalPerformance failed
    # exactly this way on 2026-09-13).
    prepull_images() {
        local kafka_version images node image
        kafka_version=$(awk '/^- version:/ {v=$3} /^  default: true/ {print v}' "${PROJECT_DIR}/kafka-versions.yaml" | head -1)
        images=("quay.io/strimzi/operator:latest")
        [[ -n "${kafka_version}" ]] && images+=("quay.io/strimzi/kafka:latest-kafka-${kafka_version}")
        for node in $(podman ps --format '{{.Names}}' | grep '^kind-cluster' || true); do
            for image in "${images[@]}"; do
                podman exec "${node}" crictl pull "${image}" >>"${LOG_FILE}" 2>&1 \
                    || log "WARNING: pre-pull of ${image} on ${node} failed"
            done
        done
        log "Image pre-pull finished."
    }
    prepull_images &
    PREPULL_PID=$!

    # ---- Step 3: Build systemtest module and deploy Strimzi ----
    log "Building systemtest module..."
    cd "${PROJECT_DIR}"
    mvn install -DskipTests -Dcheckstyle.skip=true -pl systemtest -am 2>&1 | tail -5 | tee -a "${LOG_FILE}"
    # The merged tree builds, so the sync is safe to publish in Step 6.
    SYNC_BUILD_OK=true

    # ---- Step 4: Run performance tests ----
    # Make sure the image pre-pull is done before the first deploy needs it.
    if [[ -n "${PREPULL_PID:-}" ]]; then
        wait "${PREPULL_PID}" 2>/dev/null || true
    fi

    # Full mvn output goes only to LOG_FILE; teeing it to stdout was growing
    # launchd-stdout.log without bound (169MB before rotation was added).
    log "Running performance tests (non-capacity), full output in ${LOG_FILE}..."
    cd "${PROJECT_DIR}"
    mvn verify -pl systemtest -Pperformance -DskipTests=false \
        -Dgroups="performance & !capacity" \
        -Dcheckstyle.skip=true \
        -Dmaven.test.failure.ignore=true \
        >>"${LOG_FILE}" 2>&1
    log "Performance tests complete."
fi

# ---- Step 5: Export results and compare ----
log "Exporting results and running baseline comparison..."
cd "${PROJECT_DIR}"

CLASSPATH="systemtest/target/classes:$(mvn -pl systemtest dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout 2>/dev/null)"

java -cp "${CLASSPATH}" \
    io.strimzi.systemtest.performance.regression.ResultExporter \
    --results-repo "${RESULTS_REPO}" \
    --commit "${COMMIT_SHA}" \
    2>&1 | tee -a "${LOG_FILE}" || true

# Write run metadata next to the exported results. The dashboard aggregation
# reads commitSha from it; the rest is environment context for outlier
# forensics (was a slow night the code, or a loaded machine?).
LATEST_RESULTS_DIR=$(ls -d "${RESULTS_REPO}"/results/*/ 2>/dev/null | sort | tail -1 || true)
if [[ -n "${LATEST_RESULTS_DIR}" ]]; then
    COMMIT_SHA="${COMMIT_SHA}" START_TS="${START_TS}" OUT_DIR="${LATEST_RESULTS_DIR}" \
    PROJECT_DIR="${PROJECT_DIR}" SYNC_MERGED="${SYNC_MERGED:-false}" python3 - <<'PYEOF' 2>&1 | tee -a "${LOG_FILE}" || true
import json, os, re, subprocess, datetime

def run(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=20).stdout.strip()
    except Exception:
        return ""

start = datetime.datetime.fromtimestamp(int(os.environ["START_TS"]), datetime.timezone.utc)
now = datetime.datetime.now(datetime.timezone.utc)

podman_vm = {}
try:
    insp = json.loads(run("podman machine inspect") or "[]")
    if insp:
        res = insp[0].get("Resources", {})
        podman_vm = {"cpus": res.get("CPUs"), "memoryMiB": res.get("Memory"), "diskGiB": res.get("DiskSize")}
except Exception:
    pass

kafka_default, ver = "", ""
try:
    for line in open(os.path.join(os.environ["PROJECT_DIR"], "kafka-versions.yaml")):
        s = line.strip()
        if s.startswith("- version:"):
            ver = s.split(":", 1)[1].strip()
        if s == "default: true":
            kafka_default = ver
except Exception:
    pass

kubectl_ver = ""
try:
    kubectl_ver = json.loads(run("kubectl version --client -o json") or "{}").get("clientVersion", {}).get("gitVersion", "")
except Exception:
    pass

meta = {
    "commitSha": os.environ["COMMIT_SHA"],
    "startedAt": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "finishedAt": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "durationSeconds": int((now - start).total_seconds()),
    "upstreamSynced": os.environ["SYNC_MERGED"] == "true",
    "kafkaDefaultVersion": kafka_default,
    "host": {
        "macos": run("sw_vers -productVersion"),
        "model": run("sysctl -n hw.model"),
        "cpus": run("sysctl -n hw.ncpu"),
        "memBytes": run("sysctl -n hw.memsize"),
        "loadAvgEnd": run("sysctl -n vm.loadavg").strip("{} "),
    },
    "podmanMachine": podman_vm,
    "versions": {
        "podman": run("podman --version").replace("podman version ", ""),
        "kind": run("kind --version").replace("kind version ", ""),
        "kubectl": kubectl_ver,
        "java": run("java -version 2>&1 | head -1"),
    },
}
with open(os.path.join(os.environ["OUT_DIR"], "metadata.json"), "w") as f:
    json.dump(meta, f, indent=2)
    f.write("\n")
print("metadata.json written (commit %s, duration %ss)" % (meta["commitSha"], meta["durationSeconds"]))
PYEOF
fi

# ---- Step 6: Push results ----
if [[ "${SKIP_PUSH}" == "false" ]]; then
    # Load SSH key from macOS Keychain for non-interactive launchd sessions
    ssh-add --apple-use-keychain 2>>"${LOG_FILE}" || true

    # Publish the upstream sync now that the merged tree has built successfully.
    if [[ "${SYNC_MERGED}" == "true" && "${SYNC_BUILD_OK}" == "true" ]]; then
        cd "${PROJECT_DIR}"
        if git push origin HEAD:perf-fork 2>>"${LOG_FILE}"; then
            log "Pushed synced perf-fork ($(git rev-parse --short HEAD)) to origin."
        else
            log "WARNING: push of synced perf-fork failed, sync will be redone next run"
        fi
    fi

    log "Pushing results to remote..."
    cd "${RESULTS_REPO}"
    git add -A
    if git diff --cached --quiet; then
        log "No new results to push."
    else
        git commit -s -m "Nightly results $(date +%Y-%m-%d) (${COMMIT_SHA})"
        git pull --rebase origin main >>"${LOG_FILE}" 2>&1 || log "WARNING: rebase on origin/main failed, pushing anyway"
        git push -u origin main
        log "Results pushed."
    fi
else
    log "Skipping push (--skip-push)."
fi

# ---- Summary ----
REGRESSIONS_FILE="${RESULTS_REPO}/regressions/current.json"
if [[ -f "${REGRESSIONS_FILE}" ]] && python3 -c "import json,sys; r=json.load(open(sys.argv[1])); sys.exit(0 if not r.get('regressions') else 1)" "${REGRESSIONS_FILE}" 2>/dev/null; then
    log "All metrics within baseline. Run complete."
    send_summary "[OK] Strimzi nightly perf $(date +%Y-%m-%d): all metrics within baseline. Commit ${COMMIT_SHA}, took $(run_duration). Dashboard: https://see-quick.github.io/perf-dashboard/"
    exit 0
else
    REGRESSION_COUNT=$(python3 -c "import json,sys; print(len(json.load(open(sys.argv[1])).get('regressions', [])))" "${REGRESSIONS_FILE}" 2>/dev/null || echo "?")
    TOP_REGRESSIONS=$(python3 -c "
import json, sys
rs = json.load(open(sys.argv[1])).get('regressions', [])
print('; '.join('%s/%s +%.1f sigma' % (r['testName'], r['metricName'], r['deviations']) for r in rs[:5]))
" "${REGRESSIONS_FILE}" 2>/dev/null || echo "")
    log "REGRESSION DETECTED (${REGRESSION_COUNT} metrics). Check results at: ${RESULTS_REPO}"
    notify "Strimzi nightly perf: REGRESSION" "${REGRESSION_COUNT} metric(s) above baseline (${COMMIT_SHA})"
    send_summary "[REGRESSION] Strimzi nightly perf $(date +%Y-%m-%d): ${REGRESSION_COUNT} metric(s) above baseline. Commit ${COMMIT_SHA}, took $(run_duration). ${TOP_REGRESSIONS}. Dashboard: https://see-quick.github.io/perf-dashboard/"
    REGRESSION_EXIT=true
    exit 1
fi
