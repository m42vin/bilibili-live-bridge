#!/bin/sh
# 使用本地桩与独立测试进程执行验收，不读取 .env 或调用真实平台。
set -eu
validation_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
validation_profile=${1:-load}
case "$validation_profile" in
  smoke)
    export BRIDGE_VALIDATE_CLIENTS=${BRIDGE_VALIDATE_CLIENTS:-4}
    export BRIDGE_VALIDATE_RATE=${BRIDGE_VALIDATE_RATE:-200}
    export BRIDGE_VALIDATE_SECONDS=${BRIDGE_VALIDATE_SECONDS:-1}
    export BRIDGE_VALIDATE_PAYLOAD_BYTES=${BRIDGE_VALIDATE_PAYLOAD_BYTES:-128}
    ;;
  load) ;;
  soak)
    export BRIDGE_VALIDATE_SECONDS=${BRIDGE_VALIDATE_SECONDS:-60}
    export BRIDGE_VALIDATE_CYCLES=${BRIDGE_VALIDATE_CYCLES:-10}
    ;;
  *) echo 'usage: scripts/validate.sh [smoke|load|soak]' >&2; exit 2 ;;
esac
export BRIDGE_VALIDATE_REPORT=${BRIDGE_VALIDATE_REPORT:-$validation_root/target/validation-$validation_profile.json}
case "$BRIDGE_VALIDATE_REPORT" in
  /*) ;;
  *) BRIDGE_VALIDATE_REPORT=$validation_root/$BRIDGE_VALIDATE_REPORT; export BRIDGE_VALIDATE_REPORT ;;
esac
mkdir -p "$(dirname -- "$BRIDGE_VALIDATE_REPORT")"
# 编译、参数或启动阶段失败时不能遗留上次成功的报告。
printf '%s\n' '{"schema_version":1,"passed":false,"phase":"building_or_starting"}' > "$BRIDGE_VALIDATE_REPORT"
cd "$validation_root"
exec cargo test --locked --release -p bridge-native --lib server::validation::acceptance -- --exact --ignored --nocapture --test-threads=1
