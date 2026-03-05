#!/usr/bin/env bash
set -euo pipefail

echo "[fix-docker] stopping stale Docker Desktop processes..."
osascript -e 'tell application "Docker" to quit' >/dev/null 2>&1 || true
for p in \
  "com.docker.backend" \
  "com.docker.virtualization" \
  "com.docker.vpnkit" \
  "vpnkit" \
  "qemu-system-x86_64" \
  "qemu-system-aarch64" \
  "Docker Desktop" \
  "Docker"; do
  pkill -9 -f "$p" >/dev/null 2>&1 || true
done

echo "[fix-docker] cleaning stale sockets..."
mkdir -p "$HOME/.docker/run"
rm -f "$HOME/.docker/run/docker.sock" "$HOME/.docker/run/docker-cli.sock" "$HOME/.docker/run/docker.raw.sock" || true
rm -f "$HOME/Library/Containers/com.docker.docker/Data/"*.sock || true

echo "[fix-docker] starting Docker Desktop..."
open -a Docker

echo "[fix-docker] waiting for engine..."
for i in $(seq 1 90); do
  if docker info >/dev/null 2>&1; then
    echo "[fix-docker] Docker is ready."
    docker version --format 'Client={{.Client.Version}} Server={{.Server.Version}}'
    exit 0
  fi
  sleep 2
done

echo "[fix-docker] timeout: Docker did not become ready in 3 minutes." >&2
exit 1
