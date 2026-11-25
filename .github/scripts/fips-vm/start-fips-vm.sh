#!/usr/bin/env bash
#
# Boots a CentOS Stream 9 VM with the kernel in FIPS mode and leaves it running in the background.
# GitHub runners cannot reboot into a FIPS kernel, so we use a nested VM (requires /dev/kvm).
# Afterwards, the VM is reachable with `ssh -F "${WORK_DIR}/ssh-config" fips-vm`.
#
set -euo pipefail

WORK_DIR=${WORK_DIR:-/tmp/fips-vm}
IMAGE_URL=https://cloud.centos.org/centos/9-stream/x86_64/images/CentOS-Stream-GenericCloud-9-latest.x86_64.qcow2

function vm_ssh {
    ssh -F "${WORK_DIR}/ssh-config" fips-vm "$@"
}

function wait_for_ssh {
    for _ in $(seq 120); do
        vm_ssh true 2>/dev/null && return 0
        sleep 5
    done
    echo "VM did not answer on SSH" >&2
    tail -n 50 "${WORK_DIR}/console.log" >&2
    exit 1
}

mkdir -p "${WORK_DIR}"

# RSA key, because sshd in FIPS mode rejects ed25519 keys
ssh-keygen -q -t rsa -b 4096 -N "" -f "${WORK_DIR}/id_rsa"

cat > "${WORK_DIR}/ssh-config" <<EOF
Host fips-vm
    HostName 127.0.0.1
    Port 2222
    User strimzi
    IdentityFile ${WORK_DIR}/id_rsa
    IdentitiesOnly yes
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    LogLevel ERROR
    ConnectTimeout 10
EOF

cat > "${WORK_DIR}/user-data" <<EOF
#cloud-config
users:
  - name: strimzi
    sudo: ALL=(ALL) NOPASSWD:ALL
    ssh_authorized_keys:
      - $(cat "${WORK_DIR}/id_rsa.pub")
EOF

curl -fsSL --retry 3 -o "${WORK_DIR}/base.qcow2" "${IMAGE_URL}"
qemu-img create -q -f qcow2 -F qcow2 -b "${WORK_DIR}/base.qcow2" "${WORK_DIR}/disk.qcow2" 80G
cloud-localds "${WORK_DIR}/seed.iso" "${WORK_DIR}/user-data"

qemu-system-x86_64 \
    -machine q35,accel=kvm -cpu host \
    -smp "$(( $(nproc) - 1 ))" -m "$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) / 1024 - 3072 ))" \
    -drive file="${WORK_DIR}/disk.qcow2",if=virtio,format=qcow2 \
    -drive file="${WORK_DIR}/seed.iso",if=virtio,format=raw \
    -netdev user,id=net0,hostfwd=tcp:127.0.0.1:2222-:22 -device virtio-net-pci,netdev=net0 \
    -display none -serial file:"${WORK_DIR}/console.log" -daemonize

wait_for_ssh
vm_ssh "cloud-init status --wait >/dev/null"

# Sets the FIPS crypto policy and adds fips=1 to the kernel command line, which needs a reboot
vm_ssh "sudo fips-mode-setup --enable && sudo systemctl reboot" || true
while vm_ssh true 2>/dev/null; do sleep 2; done
wait_for_ssh

[ "$(vm_ssh cat /proc/sys/crypto/fips_enabled)" = "1" ] || { echo "Kernel is not in FIPS mode" >&2; exit 1; }
echo "VM is running with the kernel in FIPS mode"
