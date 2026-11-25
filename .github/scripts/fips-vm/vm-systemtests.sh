#!/usr/bin/env bash
#
# Runs inside the FIPS VM (copied there by run-systemtests-in-vm.sh). Mirrors the steps of run-system-tests.yml
# from the Kind setup onwards: Docker and Kind with the local registry, loading the Strimzi images, building the
# Connect image and running the system tests.
#
# The tests run on the runner's Temurin JDK, which has no FIPS integration, so only the Strimzi pods run in FIPS mode.
#
set -euo pipefail

# shellcheck disable=SC1091
source "${HOME}/env.sh"
export JAVA_HOME=${HOME}/tools/jdk
export PATH=${JAVA_HOME}/bin:${HOME}/tools/maven/bin:${HOME}/tools/bin:/usr/local/bin:${PATH}

# Same version of the setup-kind action as in run-system-tests.yml
SETUP_KIND_REF=bb4758d4afacd9c0ff8f426395bbbece41fd142a

echo "Kernel fips_enabled=$(cat /proc/sys/crypto/fips_enabled)"

############################################################################
# Docker, with a socket the non-root user can use across docker restarts
############################################################################
sudo curl -fsSL --retry 3 -o /etc/yum.repos.d/docker-ce.repo https://download.docker.com/linux/centos/docker-ce.repo
sudo dnf install -y -q docker-ce docker-ce-cli containerd.io make
sudo mkdir -p /etc/systemd/system/docker.socket.d
printf '[Socket]\nSocketMode=0666\n' | sudo tee /etc/systemd/system/docker.socket.d/mode.conf >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable --now docker.socket docker.service

############################################################################
# Kind cluster with the local registry, same script and settings as the setup-kind action
############################################################################
curl -fsSL --retry 3 -o setup-kind.sh \
    "https://raw.githubusercontent.com/strimzi/github-actions/${SETUP_KIND_REF}/.github/actions/dependencies/setup-kind/setup-kind.sh"
CONTROL_NODES=1 WORKER_NODES=3 \
    KIND_VERSION=${KIND_VERSION:-v0.33.0} KIND_CLOUD_PROVIDER_VERSION=v0.12.0 \
    REGISTRY_IMAGE=mirror.gcr.io/registry:3.1.2 \
    bash setup-kind.sh "${ARCH}"

REGISTRY="$(hostname --all-ip-addresses | awk '{ print $1 }'):5001"
export CONNECT_BUILD_REGISTRY=${REGISTRY}

cd strimzi

############################################################################
# Strimzi images: built ones go to the Kind registry, published ones stay on quay.io
############################################################################
if [ "${DOCKER_REGISTRY:-}" != "quay.io" ]; then
    export DOCKER_REGISTRY=${REGISTRY}
    tar -xf "containers-operators-${ARCH}.tar"
    rm "containers-operators-${ARCH}.tar"
    DOCKER_ARCHITECTURE=${ARCH} make docker_load docker_e2e docker_delete_archive
fi

DEFAULT_KAFKA_VERSION=$([[ "${KAFKA_VERSION}" == *"latest"* ]] && yq eval '.[] | select(.default) | .version' kafka-versions.yaml || echo "${KAFKA_VERSION}")
BASE_IMAGE="${DOCKER_REGISTRY}/${DOCKER_ORG}/kafka:${DOCKER_TAG}-kafka-${DEFAULT_KAFKA_VERSION}"

############################################################################
# KafkaConnect image with file-sink plugin, skipped for upgrade
############################################################################
if [[ "${PROFILE}" != *"upgrade"* ]]; then
    CONNECT_IMAGE="${CONNECT_BUILD_REGISTRY}/${DOCKER_ORG}/connect-file-sink:latest"
    eval "$(./systemtest/src/test/resources/connect-build/build-connect-image.sh "${DEFAULT_KAFKA_VERSION}" "${BASE_IMAGE}" "${CONNECT_IMAGE}")"
    docker push "${CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN}"
    export CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN
fi

export KAFKA_TIERED_STORAGE_BASE_IMAGE=${BASE_IMAGE}

############################################################################
# System tests, same invocation as the "Run systemtests" step
############################################################################
# shellcheck disable=SC2086
mvn -f systemtest/pom.xml verify \
    -P"${PROFILE}" \
    -DexcludedGroups="${EXCLUDED_GROUPS:-}" \
    ${GROUPS_OPT:-} \
    ${TESTS_OPT:-} \
    -Dmaven.javadoc.skip=true \
    -B -V \
    -Dfailsafe.rerunFailingTestsCount=1 \
    -Djunit.jupiter.execution.parallel.enabled="$([[ ${PARALLEL:-1} -gt 1 ]] && echo true || echo false)" \
    -Djunit.jupiter.execution.parallel.config.fixed.parallelism="${PARALLEL}" \
    -Dskip.surefire.tests \
    -s ./.azure/systemtests/systemtest-settings.xml
