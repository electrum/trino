#!/usr/bin/env bash

set -eu

ROOT_DIR=$(dirname $0)

docker run -t -v$ROOT_DIR:/work -w /work wasmbuild ./build-wasm.sh "$@"
