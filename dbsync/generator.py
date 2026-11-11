"""本地变更日志生成器：产出带事务边界的 JSONL，可注入重复/旧版本/冲突/半事务。

生成器自己维护一份"源库真像"，--expect-out 可导出最终期望状态，
供 `dbsync verify --expect` 在同步后逐行比对。
"""

import json
import random

CUSTOMERS = ["alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi"]
SKUS = ["SKU-100", "SKU-200", "SKU-300", "SKU-400"]
STATUSES = ["new", "paid", "shipped", "done"]

ORDER_COLS = ("id", "customer", "amount", "status")
ITEM_COLS = ("id", "order_id", "sku", "qty", "price")


def generate(out_path, txns=30, seed=1, partial_tail=False, duplicates=False,
             stale=False, conflict=False, redeliver=False, expect_out=None):
    rng = random.Random(seed)
    live_orders = {}        # id -> row dict（含 version）
    live_items = {}
    deleted_orders = []     # (id, 删除前的行) —— 用于"旧插入复活"探针
    tx_lines = []           # 每笔事务的行列表（供 redeliver 切片）
    all_lines = []
    txid = 1000

    def emit(events):
        nonlocal txid
        txid += 1
        lines = []
        for i, ev in enumerate(events):
            ev = dict(ev)
            ev["txid"] = txid
            ev["seq"] = i
            ev["commit"] = (i == len(events) - 1)
            lines.append(json.dumps(ev, ensure_ascii=False))
        tx_lines.append(lines)
        all_lines.extend(lines)
        # 同一段日志紧跟着重复投递一次
        if duplicates and rng.random() < 0.25:
            all_lines.extend(lines)

    for i in range(txns):
        r = rng.random()
        live = list(live_orders)
        if not live or r < 0.45:
            # 新订单 + 1~3 条明细：一笔跨表事务
            oid = f"o-{seed}-{i}"
            row = {"id": oid, "customer": rng.choice(CUSTOMERS),
                   "amount": round(rng.uniform(10, 500), 2), "status": "new"}
            live_orders[oid] = {**row, "version": 1}
            events = [dict(table="orders", pk=oid, op="insert", version=1, data=row)]
            for j in range(rng.randint(1, 3)):
                iid = f"{oid}-i{j}"
                irow = {"id": iid, "order_id": oid, "sku": rng.choice(SKUS),
                        "qty": rng.randint(1, 5), "price": round(rng.uniform(1, 100), 2)}
                live_items[iid] = {**irow, "version": 1}
                events.append(dict(table="order_items", pk=iid, op="insert", version=1, data=irow))
            emit(events)
        elif r < 0.65:
            # 更新订单（版本 +1）
            oid = rng.choice(live)
            cur = live_orders[oid]
            cur["version"] += 1
            cur["amount"] = round(cur["amount"] + rng.uniform(-20, 50), 2)
            cur["status"] = rng.choice(STATUSES)
            emit([dict(table="orders", pk=oid, op="update", version=cur["version"],
                       data={k: cur[k] for k in ORDER_COLS})])
        elif r < 0.78 and live_items:
            # 更新明细
            iid = rng.choice(list(live_items))
            cur = live_items[iid]
            cur["version"] += 1
            cur["qty"] += rng.randint(1, 3)
            emit([dict(table="order_items", pk=iid, op="update", version=cur["version"],
                       data={k: cur[k] for k in ITEM_COLS})])
        elif r < 0.88 and live_items:
            # 删除一条明细
            iid = rng.choice(list(live_items))
            cur = live_items.pop(iid)
            emit([dict(table="order_items", pk=iid, op="delete", version=cur["version"] + 1)])
        elif live:
            # 删除订单及其全部明细：一笔跨表事务
            oid = rng.choice(live)
            ocur = live_orders.pop(oid)
            events = [dict(table="orders", pk=oid, op="delete", version=ocur["version"] + 1)]
            for iid in [k for k, v in live_items.items() if v["order_id"] == oid]:
                icur = live_items.pop(iid)
                events.append(dict(table="order_items", pk=iid, op="delete",
                                   version=icur["version"] + 1))
            deleted_orders.append((oid, ocur))
            emit(events)

    if stale:
        # 旧版本变更：版本低于当前，必须被丢弃（不能覆盖新版本）
        for oid, cur in list(live_orders.items())[:2]:
            if cur["version"] > 1:
                row = {k: cur[k] for k in ORDER_COLS}
                row["amount"] = round(row["amount"] + 999.0, 2)
                emit([dict(table="orders", pk=oid, op="update",
                           version=cur["version"] - 1, data=row)])
        # 复活探针：已删除订单的旧插入事件重新投递，墓碑必须挡住
        for oid, ocur in deleted_orders[:2]:
            emit([dict(table="orders", pk=oid, op="insert", version=1,
                       data={k: ocur[k] for k in ORDER_COLS})])

    if conflict and live_orders:
        # 同主键、同版本、不同内容：同步器必须报冲突并停住
        oid, cur = next(iter(live_orders.items()))
        row = {k: cur[k] for k in ORDER_COLS}
        row["amount"] = round(row["amount"] + 12345.0, 2)
        emit([dict(table="orders", pk=oid, op="update", version=cur["version"], data=row)])

    if redeliver and tx_lines:
        # 把早前一整段（按事务边界对齐）原样追加到末尾，模拟重复投递
        start = rng.randrange(len(tx_lines))
        span = rng.randint(1, min(5, len(tx_lines) - start))
        for lines in tx_lines[start:start + span]:
            all_lines.extend(lines)

    tail_rest_path = None
    if partial_tail:
        # 半笔事务：订单已写、明细和 commit 标记还没来。剩余部分写到 .tail-rest 文件，
        # 之后 `cat xxx.tail-rest >> 日志` 即可补全这笔事务。
        tail_txid = txid + 100
        oid = f"o-{seed}-tail"
        iid = f"{oid}-i0"
        ev1 = {"txid": tail_txid, "seq": 0, "table": "orders", "pk": oid, "op": "insert",
               "version": 1, "data": {"id": oid, "customer": "tail", "amount": 1.0,
                                      "status": "new"}, "commit": False}
        ev2 = {"txid": tail_txid, "seq": 1, "table": "order_items", "pk": iid, "op": "insert",
               "version": 1, "data": {"id": iid, "order_id": oid, "sku": "SKU-100",
                                      "qty": 1, "price": 1.0}, "commit": True}
        all_lines.append(json.dumps(ev1, ensure_ascii=False))
        tail_rest_path = out_path + ".tail-rest"
        with open(tail_rest_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(ev2, ensure_ascii=False) + "\n")

    with open(out_path, "w", encoding="utf-8") as f:
        for line in all_lines:
            f.write(line + "\n")

    if expect_out:
        exp = {
            "orders": sorted(live_orders.values(), key=lambda r: r["id"]),
            "order_items": sorted(live_items.values(), key=lambda r: r["id"]),
        }
        with open(expect_out, "w", encoding="utf-8") as f:
            json.dump(exp, f, ensure_ascii=False, indent=2, sort_keys=True)

    return {
        "path": out_path,
        "lines": len(all_lines),
        "live_orders": len(live_orders),
        "live_items": len(live_items),
        "tail_rest": tail_rest_path,
    }
