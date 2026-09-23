#!/usr/bin/env bash
# Runs a cargo subcommand inside the hush-dev Linux container, so rdkafka
# links against a real librdkafka instead of requiring MSVC + vcpkg on Windows.
# Usage: scripts/dev-cargo.sh check
#        scripts/dev-cargo.sh test -p aggregator
set -euo pipefail
cd "$(dirname "$0")/.."

docker run --rm \
  -v "$(pwd)":/work \
  -v hush-cargo-registry:/usr/local/cargo/registry \
  -v hush-cargo-target:/work/target \
  -v //var/run/docker.sock:/var/run/docker.sock \
  -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal \
  -w /work \
  hush-dev \
  cargo "$@"
