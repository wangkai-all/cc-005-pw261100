# dbsync — JSONL 变更日志 → SQLite 事务级同步器

把持续追加的 JSONL 变更日志同步到目标 SQLite，支持新增 / 修改 / 删除。
纯 Python 标准库实现，不依赖任何外部消息服务。

## 保证的语义

| 要求 | 实现 |
| --- | --- |
| 整笔事务原子生效（订单和明细不脱节） | 按 `txid` 缓冲事件，见到 `commit` 标记才在**一个 SQLite 事务**里应用整笔变更 |
| 日志尾部只有半笔事务 | 留在缓冲区等待后续内容，不生效、不推进进度 |
| 数据与消费进度一起提交 | `applied_offset` 存在目标库 `sync_meta` 表，与业务变更**同一个 SQLite 事务**提交 |
| 任意写入位置退出后不丢不重 | 崩溃 → SQLite 回滚未提交事务 → 重启从已提交 offset 重读；版本守卫保证重放幂等 |
| 重复投递 | 同版本同内容 → 幂等跳过 |
| 旧版本变更 | 版本低于当前 → 丢弃，绝不覆盖新版本 |
| 删除后防复活 | 删除写 `tombstones` 墓碑（含版本），旧插入事件被墓碑挡住 |
| 同主键同版本不同内容 | 报冲突：整笔事务回滚、记入 `conflicts` 表、进度停在该事务之前、进程退出码 2，绝不静默二选一 |

## 日志格式

每行一个 JSON 对象：

```json
{"txid": 1001, "seq": 0, "table": "orders",      "pk": "o-1", "op": "insert", "version": 1, "data": {"id": "o-1", "customer": "alice", "amount": 99.5, "status": "new"}, "commit": false}
{"txid": 1001, "seq": 1, "table": "order_items", "pk": "i-1", "op": "insert", "version": 1, "data": {"id": "i-1", "order_id": "o-1", "sku": "SKU-100", "qty": 2, "price": 49.75}, "commit": true}
{"txid": 1002, "seq": 0, "table": "orders",      "pk": "o-1", "op": "delete", "version": 2, "commit": true}
```

- `txid` 事务号，`seq` 事务内序号，`commit: true` 标记事务最后一个事件（也支持纯提交标记 `{"txid": ..., "commit": true}`）
- `op`: `insert` / `update` / `delete`；`insert`/`update` 的 `data` 是**整行后镜像**
- `version` 行版本，源端对同一主键单调递增
- 业务表：`orders(id, customer, amount, status)`、`order_items(id, order_id, sku, qty, price)`，目标库额外带 `version` 列

## 命令

```bash
# 所有命令在仓库根目录执行（或设 PYTHONPATH 指向仓库根目录）

# 初始化目标库（run 会自动建表，此命令可选）
python3 -m dbsync init --db target.db

# 启动同步：默认持续 tail；--once 处理到当前 EOF 退出
python3 -m dbsync run --db target.db --log changes.jsonl [--interval 0.5]
python3 -m dbsync run --db target.db --log changes.jsonl --once

# 查看进度（可与 run 并发执行，WAL 模式）
python3 -m dbsync progress --db target.db --log changes.jsonl

# 查看冲突
python3 -m dbsync conflicts --db target.db

# 校验订单/明细一致性（无孤儿明细；--expect 可与生成器导出的期望状态逐行比对）
python3 -m dbsync verify --db target.db [--expect expected.json]
```

`run` 退出码：`0` 正常；`2` 冲突停机；`3` 日志解析失败（进度不动）；`4` 日志文件问题；`42` 故障注入的崩溃。

## 本地日志生成器

```bash
python3 -m dbsync genlog --out changes.jsonl --txns 40 --seed 7 \
    [--duplicates]     # 部分日志段紧跟着重复投递
    [--stale]          # 注入旧版本变更 + 已删行的旧插入（复活探针）
    [--redeliver]      # 把早前一整段日志原样追加到末尾
    [--conflict]       # 注入同主键同版本不同内容的冲突事件
    [--partial-tail]   # 日志以半笔事务结尾（剩余部分写入 changes.jsonl.tail-rest）
    [--expect-out expected.json]   # 导出最终期望状态，供 verify --expect 比对
```

半笔事务可用 `cat changes.jsonl.tail-rest >> changes.jsonl` 补全，同步器随即放行该事务。

## 故障注入与恢复验证

```bash
# 事务写到一半（已执行部分 DML、未 COMMIT）进程崩溃
python3 -m dbsync run --db target.db --log changes.jsonl --once --crash-mid-txn   # 退出码 42

# 写完数据+进度、COMMIT 前一刻崩溃
python3 -m dbsync run --db target.db --log changes.jsonl --once --crash-before-commit

# 读到第 N 个事件即崩溃（事务可能只读到一半）
python3 -m dbsync run --db target.db --log changes.jsonl --once --crash-after-read 5

# 恢复：去掉故障标志重跑，随后验证一致性
python3 -m dbsync run --db target.db --log changes.jsonl --once
python3 -m dbsync verify --db target.db --expect expected.json
```

一键演示（崩溃 → 恢复 → 校验，外加冲突演示）：

```bash
./scripts/demo_crash_recovery.sh
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

覆盖：跨表事务原子性、半事务等待与补全、交错事务、重复/旧版本幂等、墓碑防复活、
冲突停机且不推进进度（重启后仍停住）、三种崩溃点恢复后收敛到期望状态。

## 设计要点与边界

- **水位线（offset）**：`sync_meta.applied_offset` 是"此前所有事件均已应用"的最大文件偏移。
  读事件时维护一个窗口，只有窗口前缀中的事务全部应用后才推进水位线，因此即使日志中
  事务交错也不会跳过未应用的事件。事务按日志出现顺序应用（队首阻塞），保证不越过
  任何未完成/冲突的事务。
- **崩溃安全**：`PRAGMA journal_mode=WAL` + `synchronous=FULL`，进程在 COMMIT 前任意
  时刻死亡都由 SQLite 回滚；故障注入用 `os._exit()` 模拟，不走任何清理路径。
- **单写者**：假定只有一个同步进程写目标库。
- **日志只追加**：不支持截断/轮转；若文件小于已提交进度，同步器拒绝继续。
- `data` 必须是整行后镜像；`txid` 由生产方保证唯一（重复投递是字节级重复，天然幂等）。
