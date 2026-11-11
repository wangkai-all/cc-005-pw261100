#!/usr/bin/env bash
# 崩溃恢复演示：事务写到一半 / 写完未提交进度，两种崩溃点后恢复，验证订单与明细一致。
set -u
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD"

WORK=$(mktemp -d /tmp/dbsync-demo-XXXXXX)
LOG="$WORK/changes.jsonl"
DB="$WORK/target.db"
EXP="$WORK/expected.json"

echo "== workdir: $WORK =="
echo

echo "== 1. 生成日志（40 笔事务，含跨表事务/更新/删除）=="
python3 -m dbsync genlog --out "$LOG" --txns 40 --seed 7 --expect-out "$EXP"
echo

run() { python3 -m dbsync run --db "$DB" --log "$LOG" --once "$@"; }

echo "== 2. 故障注入：事务写到一半进程崩溃 =="
run --crash-mid-txn
echo "   exit code: $? (42 = 注入的崩溃)"
echo "-- 崩溃后的进度（注意：不允许出现半笔事务）--"
python3 -m dbsync progress --db "$DB" --log "$LOG"
python3 -m dbsync verify --db "$DB"
echo

echo "== 3. 恢复运行，收敛到期望状态 =="
run
python3 -m dbsync verify --db "$DB" --expect "$EXP"
echo

echo "== 4. 换一个库，故障注入：写完数据+进度、COMMIT 前崩溃 =="
DB2="$WORK/target2.db"
python3 -m dbsync run --db "$DB2" --log "$LOG" --once --crash-before-commit
echo "   exit code: $? (42 = 注入的崩溃)"
python3 -m dbsync progress --db "$DB2" --log "$LOG"
echo "-- 恢复 --"
python3 -m dbsync run --db "$DB2" --log "$LOG" --once
python3 -m dbsync verify --db "$DB2" --expect "$EXP"
echo

echo "== 5. 冲突演示：同主键同版本不同内容，同步器停住 =="
LOG2="$WORK/conflict.jsonl"
DB3="$WORK/target3.db"
python3 -m dbsync genlog --out "$LOG2" --txns 20 --seed 9 --conflict
python3 -m dbsync run --db "$DB3" --log "$LOG2" --once
echo "   exit code: $? (2 = 冲突停机)"
python3 -m dbsync conflicts --db "$DB3"
python3 -m dbsync progress --db "$DB3" --log "$LOG2"
echo

echo "== demo done. files kept in $WORK =="
