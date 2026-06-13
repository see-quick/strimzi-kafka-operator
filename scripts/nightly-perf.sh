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

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"
}

die() {
    log "ERROR: $*"
    exit 1
}

cleanup() {
    local exit_code=$?
    if [[ "${KEEP_CLUSTER}" == "false" && "${SKIP_CLUSTER}" == "false" ]]; then
        log "Cleaning up Kind cluster..."
        "${KIND_SCRIPT}" delete 2>>"${LOG_FILE}" || true
    fi
    if [[ ${exit_code} -ne 0 ]]; then
        log "Script failed with exit code ${exit_code}. Logs: ${LOG_FILE}"
    fi
}
trap cleanup EXIT

# ---- Step 0: Validate prerequisites ----
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
    # ---- Step 1: Create Kind cluster ----
    if [[ "${SKIP_CLUSTER}" == "false" ]]; then
        log "Creating Kind cluster..."
        "${KIND_SCRIPT}" create --workers 1 --no-cloud-provider --configure-insecure 2>&1 | tee -a "${LOG_FILE}"
        log "Kind cluster ready."
    fi

    kubectl cluster-info >>"${LOG_FILE}" 2>&1 || die "No Kubernetes cluster accessible"

    # Set Connect build image path to the Kind registry's IP (accessible from inside the cluster)
    export CONNECT_BUILD_IMAGE_PATH=$(podman inspect -f '{{.NetworkSettings.Networks.kind.IPAddress}}' kind-registry):5000/strimzi-connect-build
    log "CONNECT_BUILD_IMAGE_PATH=${CONNECT_BUILD_IMAGE_PATH}"

    # ---- Step 2: Build systemtest module and deploy Strimzi ----
    log "Building systemtest module..."
    cd "${PROJECT_DIR}"
    mvn install -DskipTests -Dcheckstyle.skip=true -pl systemtest -am 2>&1 | tail -5 | tee -a "${LOG_FILE}"

    # ---- Step 3: Run performance tests ----
    log "Running performance tests (non-capacity)..."
    cd "${PROJECT_DIR}"
    mvn verify -pl systemtest -Pperformance -DskipTests=false \
        -Dgroups="performance & !capacity" \
        -Dcheckstyle.skip=true \
        -Dmaven.test.failure.ignore=true \
        2>&1 | tee -a "${LOG_FILE}"
    log "Performance tests complete."
fi

# ---- Step 4: Export results and compare ----
log "Exporting results and running baseline comparison..."
cd "${PROJECT_DIR}"

CLASSPATH="systemtest/target/classes:$(mvn -pl systemtest dependency:build-classpath -q -DincludeScope=compile -Dmdep.outputFile=/dev/stdout 2>/dev/null)"

java -cp "${CLASSPATH}" \
    io.strimzi.systemtest.performance.regression.ResultExporter \
    --results-repo "${RESULTS_REPO}" \
    --commit "${COMMIT_SHA}" \
    2>&1 | tee -a "${LOG_FILE}"

COMPARATOR_EXIT=$?

# ---- Step 5: Push results ----
if [[ "${SKIP_PUSH}" == "false" ]]; then
    log "Pushing results to remote..."
    cd "${RESULTS_REPO}"
    git add -A
    if git diff --cached --quiet; then
        log "No new results to push."
    else
        git commit -s -m "Nightly results $(date +%Y-%m-%d) (${COMMIT_SHA})"
        git push
        log "Results pushed."
    fi
else
    log "Skipping push (--skip-push)."
fi

# ---- Summary ----
if [[ ${COMPARATOR_EXIT} -ne 0 ]]; then
    log "REGRESSION DETECTED. Check results at: ${RESULTS_REPO}"
    exit 1
else
    log "All metrics within baseline. Run complete."
    exit 0
fi
