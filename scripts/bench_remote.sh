#!/usr/bin/env bash
# One-shot isolated benchmark on two Linux machines: this one runs the stack, the load generator
# runs oha. Nothing else shares their CPUs, unlike Docker Desktop on a workstation.
#
# On two fresh Ubuntu/Debian boxes in the same private network, logged in as root on the stack box:
#   git clone https://github.com/adnaanbheda/metered-python-api.git && cd metered-python-api
#   scripts/bench_remote.sh root@10.0.0.3 10.0.0.2 overhead -q 2000 4000 6000 8000 10000
#
#   $1   load generator's ssh target; needs key auth from this box. Docker gets installed there.
#   $2   this box's address as the load generator reaches it (use the private IP)
#   ...  bench_suite.py arguments; none runs the full grid
# API_REPLICAS (default 8) passes through to compose; one per physical core did best so far.
#
# nginx publishes STAGE_PORT (8080) on all interfaces: firewall it to the private network.
# Results land in bench-results/<timestamp>/ here. Copy them back (scp -r), then destroy both boxes.
set -euo pipefail
[ $# -ge 2 ] || { sed -n 2,15p "$0"; exit 1; }
loadgen=$1 addr=$2
shift 2

install_docker='command -v docker >/dev/null || curl -fsSL https://get.docker.com | sh'
sh -c "$install_docker"
command -v uv >/dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# accept-new: first contact records the host key, so docker -H ssh:// doesn't stop to ask
ssh -o StrictHostKeyChecking=accept-new "$loadgen" "$install_docker && docker pull -q ghcr.io/hatoo/oha:latest"

docker compose up -d --build --wait
OHA_HOST="ssh://$loadgen" STAGE_ADDR="$addr" uv run scripts/bench_suite.py "$@"
