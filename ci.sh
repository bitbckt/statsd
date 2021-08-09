#!/bin/bash

set -euo pipefail

DC="${DC:-dmd}"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

dub -q lint -- --styleCheck --skipTests
dub -q test --build=unittest-cov --compiler="$DC"
