#!/usr/bin/env bash
#
# Runs the system tests inside the FIPS VM started by start-fips-vm.sh.
#
# Ships into the VM what the runner already prepared: the checkout (including the downloaded container images),
# the local Maven repository (including the Strimzi artifacts built on the runner) and the runner's JDK, Maven
# and yq. Runs vm-systemtests.sh there and copies the test results back into the checkout, where the regular
# archive and report steps pick them up.
#
set -euo pipefail

WORK_DIR=${WORK_DIR:-/tmp/fips-vm}
VM_CHECKOUT=strimzi

# Environment of the job and of the "Run systemtests" step that the tests and the image loading read
FORWARDED_VARS=(
    KAFKA_VERSION ST_KAFKA_VERSION DOCKER_ORG DOCKER_TAG DOCKER_REGISTRY RELEASE_VERSION
    PROFILE ARCH PARALLEL IP_FAMILY KIND_NODE_IMAGE KIND_VERSION KUBE_VERSION
    STRIMZI_RBAC_SCOPE CLUSTER_OPERATOR_INSTALL_TYPE STRIMZI_FEATURE_GATES
    CLUSTER_SECURITY_ENCRYPTION CLUSTER_SECURITY_AUTHENTICATION
    STRIMZI_DEFAULT_LOG_LEVEL STRIMZI_TEST_LOG_LEVEL OPERATOR_IMAGE_PULL_POLICY COMPONENTS_IMAGE_PULL_POLICY
    BRIDGE_IMAGE BUILD_ID RESOURCE_ALLOCATION_STRATEGY TEST_LOG_DIR GROUPS_OPT TESTS_OPT EXCLUDED_GROUPS
    COMMIT TAG PULL_REQUEST
)

function vm_ssh {
    ssh -F "${WORK_DIR}/ssh-config" fips-vm "$@"
}

function copy_results_back {
    vm_ssh "tar -C ${VM_CHECKOUT} -cf - --ignore-failed-read systemtest/target/failsafe-reports systemtest-logs 2>/dev/null" \
        | tar -xf - || true
}
trap copy_results_back EXIT

for var in "${FORWARDED_VARS[@]}"; do
    if [ -n "${!var+x}" ]; then
        printf 'export %s=%q\n' "${var}" "${!var}"
    fi
done > "${WORK_DIR}/env.sh"

MAVEN_HOME=$(dirname "$(dirname "$(readlink -f "$(command -v mvn)")")")

echo "Copying JDK, Maven, yq, the Maven repository and the checkout into the VM"
vm_ssh "mkdir -p tools/jdk tools/maven tools/bin ${VM_CHECKOUT}"
tar -C "${JAVA_HOME}" -cf - . | vm_ssh "tar -C tools/jdk -xf -"
tar -C "${MAVEN_HOME}" -cf - . | vm_ssh "tar -C tools/maven -xf -"
scp -F "${WORK_DIR}/ssh-config" "$(command -v yq)" "${WORK_DIR}/env.sh" fips-vm:
vm_ssh "mv yq tools/bin/"
tar -C "${HOME}" -cf - .m2/repository | vm_ssh "tar -xf -"
tar -cf - . | vm_ssh "tar -C ${VM_CHECKOUT} -xf -"

vm_ssh "bash ${VM_CHECKOUT}/.github/scripts/fips-vm/vm-systemtests.sh"
