#!/usr/bin/env bash

# This script calls each end-to-end scenario sequentially and verifies the
# result

# Exit on error
set -e

DIR="$( cd "$( dirname "$0" )" >/dev/null 2>&1; pwd -P )"

$DIR/../iter8-controller/test/e2e/e2e-scenario-1.sh
$DIR/e2e-scenario-1-verify.sh
$DIR/../iter8-controller/test/e2e/e2e-scenario-2.sh
$DIR/e2e-scenario-2-verify.sh
$DIR/../iter8-controller/test/e2e/e2e-scenario-3.sh
$DIR/e2e-scenario-3-verify.sh
$DIR/../iter8-controller/test/e2e/e2e-scenario-4.sh
$DIR/e2e-scenario-4-verify.sh
$DIR/../iter8-controller/test/e2e/e2e-scenario-5.sh
$DIR/e2e-scenario-5-verify.sh
