"""端到端测试：通过子进程跑 CLI，覆盖崩溃恢复、幂等、版本守卫、墓碑与冲突。"""

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

EXIT_OK = 0
EXIT_CONFLICT = 2
EXIT_CRASH_INJECTED = 42


def cli(*args, cwd=ROOT):
    return subprocess.run(
        [sys.executable, "-m", "dbsync", *args],
        capture_output=True, text=True, cwd=cwd, timeout=60)


def write_log(path, events):
    """events: list of dict；写为 JSONL（每行带换行）。"""
    with open(path, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")


def ev(txid, seq, table, pk, op, version, data=None, commit=True):
    e = {"txid": txid, "seq": seq, "table": table, "pk": pk, "op": op,
         "version": version, "commit": commit}
    if data is not None:
        e["data"] = data
    return e


def order(oid, amount=100.0, customer="alice", status="new"):
    return {"id": oid, "customer": customer, "amount": amount, "status": status}


def item(iid, oid, qty=1, price=10.0, sku="SKU-100"):
    return {"id": iid, "order_id": oid, "sku": sku, "qty": qty, "price": price}


class Case(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory(prefix="dbsync-test-")
        self.db = os.path.join(self.dir.name, "target.db")
        self.log = os.path.join(self.dir.name, "changes.jsonl")

    def tearDown(self):
        self.dir.cleanup()

    # ------------------------------------------------------------ helpers

    def run_sync(self, *extra, expect_code=EXIT_OK):
        r = cli("run", "--db", self.db, "--log", self.log, "--once", *extra)
        self.assertEqual(
            r.returncode, expect_code,
            f"run exited {r.returncode}, want {expect_code}\nstdout:{r.stdout}\nstderr:{r.stderr}")
        return r

    def query(self, sql, args=()):
        con = sqlite3.connect(self.db)
        con.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in con.execute(sql, args)]
        finally:
            con.close()

    def rows(self, table):
        return self.query(f"SELECT * FROM {table} ORDER BY id")

    def offset(self):
        rows = self.query("SELECT value FROM sync_meta WHERE key='applied_offset'")
        return int(rows[0]["value"])

    def orphans(self):
        return self.query(
            "SELECT i.id FROM order_items i LEFT JOIN orders o ON i.order_id = o.id "
            "WHERE o.id IS NULL")

    # ------------------------------------------------------------ tests

    def test_multi_table_transaction_is_atomic(self):
        write_log(self.log, [
            ev(1, 0, "orders", "o1", "insert", 1, order("o1"), commit=False),
            ev(1, 1, "order_items", "i1", "insert", 1, item("i1", "o1"), commit=False),
            ev(1, 2, "order_items", "i2", "insert", 1, item("i2", "o1"), commit=True),
        ])
        self.run_sync()
        self.assertEqual(len(self.rows("orders")), 1)
        self.assertEqual(len(self.rows("order_items")), 2)
        self.assertEqual(self.orphans(), [])
        self.assertEqual(self.offset(), os.path.getsize(self.log))

    def test_partial_tail_waits_then_completes(self):
        # 半笔事务：订单已写、明细和 commit 还没来 —— 不允许生效
        write_log(self.log, [
            ev(1, 0, "orders", "o1", "insert", 1, order("o1"), commit=False),
            ev(1, 1, "order_items", "i1", "insert", 1, item("i1", "o1"), commit=False),
        ])
        r = self.run_sync()
        self.assertIn("waiting for commit marker", r.stdout)
        self.assertEqual(self.rows("orders"), [])
        self.assertEqual(self.rows("order_items"), [])
        self.assertEqual(self.offset(), 0)

        # 补全事务（commit 标记到达）后整笔生效
        with open(self.log, "a", encoding="utf-8") as f:
            f.write(json.dumps(ev(1, 2, "order_items", "i2", "insert", 1,
                                  item("i2", "o1"), commit=True)) + "\n")
        self.run_sync()
        self.assertEqual(len(self.rows("orders")), 1)
        self.assertEqual(len(self.rows("order_items")), 2)
        self.assertEqual(self.orphans(), [])

    def test_interleaved_transactions_apply_in_order(self):
        write_log(self.log, [
            ev("A", 0, "orders", "o1", "insert", 1, order("o1"), commit=False),
            ev("B", 0, "order_items", "i1", "insert", 1, item("i1", "o1"), commit=False),
            ev("B", 1, "orders", "o2", "insert", 1, order("o2"), commit=True),
            ev("A", 1, "order_items", "i2", "insert", 1, item("i2", "o2"), commit=True),
        ])
        self.run_sync()
        self.assertEqual(len(self.rows("orders")), 2)
        self.assertEqual(len(self.rows("order_items")), 2)
        self.assertEqual(self.offset(), os.path.getsize(self.log))

    def test_duplicate_and_stale_events_are_idempotent(self):
        write_log(self.log, [
            ev(1, 0, "orders", "o1", "insert", 1, order("o1", amount=100.0)),
            ev(2, 0, "orders", "o1", "update", 2, order("o1", amount=200.0)),
            # 重复投递（同版本同内容）
            ev(3, 0, "orders", "o1", "update", 2, order("o1", amount=200.0)),
            # 旧版本（不得覆盖）
            ev(4, 0, "orders", "o1", "update", 1, order("o1", amount=100.0)),
        ])
        self.run_sync()
        rows = self.rows("orders")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["amount"], 200.0)
        self.assertEqual(rows[0]["version"], 2)

    def test_tombstone_blocks_resurrection(self):
        write_log(self.log, [
            ev(1, 0, "orders", "o1", "insert", 1, order("o1")),
            ev(2, 0, "orders", "o1", "delete", 2),
            # 旧插入事件重复投递：墓碑必须挡住，不能复活
            ev(3, 0, "orders", "o1", "insert", 1, order("o1")),
        ])
        self.run_sync()
        self.assertEqual(self.rows("orders"), [])
        tombs = self.query("SELECT * FROM tombstones WHERE pk='o1'")
        self.assertEqual(len(tombs), 1)
        self.assertEqual(tombs[0]["version"], 2)

    def test_conflict_halts_and_does_not_advance(self):
        write_log(self.log, [
            ev(1, 0, "orders", "o1", "insert", 1, order("o1", amount=100.0)),
            # 同主键、同版本、不同内容 -> 冲突
            ev(2, 0, "orders", "o1", "update", 1, order("o1", amount=999.0)),
            # 冲突之后的正常事务：绝不允许越过冲突生效
            ev(3, 0, "orders", "o2", "insert", 1, order("o2")),
        ])
        offset_after_tx1 = None
        self.run_sync(expect_code=EXIT_CONFLICT)
        # 冲突事务整笔回滚：o1 仍是 v1/100.0，o2 未出现
        rows = self.rows("orders")
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["id"], rows[0]["amount"], rows[0]["version"]),
                         ("o1", 100.0, 1))
        offset_after_tx1 = self.offset()
        self.assertLess(offset_after_tx1, os.path.getsize(self.log))
        # 冲突已记录
        conflicts = self.query("SELECT * FROM conflicts")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["txid"], "2")
        # 重启再跑：仍停在同一位置，不重复记录、不推进
        self.run_sync(expect_code=EXIT_CONFLICT)
        self.assertEqual(self.offset(), offset_after_tx1)
        self.assertEqual(len(self.query("SELECT * FROM conflicts")), 1)
        self.assertEqual(len(self.rows("orders")), 1)

    def test_conflict_inside_transaction_rolls_back_whole_txn(self):
        # 一笔事务里：先合法插入 o1，随后同事务内出现冲突事件 —— 整笔回滚
        write_log(self.log, [
            ev(1, 0, "orders", "o9", "insert", 1, order("o9")),
            ev(2, 0, "orders", "o1", "insert", 1, order("o1", amount=100.0), commit=False),
            ev(2, 1, "orders", "o9", "update", 1, order("o9", amount=555.0), commit=True),
        ])
        self.run_sync(expect_code=EXIT_CONFLICT)
        rows = self.rows("orders")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], "o9")
        self.assertEqual(rows[0]["amount"], 100.0)  # o9 未被事务 2 污染
        self.assertEqual(rows[0]["version"], 1)

    def test_crash_before_commit_then_resume(self):
        r = cli("genlog", "--out", self.log, "--txns", "40", "--seed", "7",
                "--expect-out", self.dir.name + "/expected.json")
        self.assertEqual(r.returncode, 0, r.stderr)
        # 写完数据+进度、COMMIT 前崩溃
        self.run_sync("--crash-before-commit", expect_code=EXIT_CRASH_INJECTED)
        # 崩溃后库里不允许出现半笔事务：明细必须都有订单
        self.assertEqual(self.orphans(), [])
        # 恢复：不加故障标志重跑，必须完整收敛到期望状态
        self.run_sync()
        v = cli("verify", "--db", self.db, "--expect", self.dir.name + "/expected.json")
        self.assertEqual(v.returncode, 0, f"{v.stdout}\n{v.stderr}")
        self.assertIn("PASS", v.stdout)

    def test_crash_mid_txn_then_resume(self):
        r = cli("genlog", "--out", self.log, "--txns", "40", "--seed", "11",
                "--expect-out", self.dir.name + "/expected.json")
        self.assertEqual(r.returncode, 0, r.stderr)
        # 事务写到一半崩溃
        self.run_sync("--crash-mid-txn", expect_code=EXIT_CRASH_INJECTED)
        self.assertEqual(self.orphans(), [])
        self.run_sync()
        v = cli("verify", "--db", self.db, "--expect", self.dir.name + "/expected.json")
        self.assertEqual(v.returncode, 0, f"{v.stdout}\n{v.stderr}")

    def test_crash_after_read_then_resume(self):
        r = cli("genlog", "--out", self.log, "--txns", "30", "--seed", "3",
                "--expect-out", self.dir.name + "/expected.json")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.run_sync("--crash-after-read", "5", expect_code=EXIT_CRASH_INJECTED)
        self.assertEqual(self.orphans(), [])
        self.run_sync()
        v = cli("verify", "--db", self.db, "--expect", self.dir.name + "/expected.json")
        self.assertEqual(v.returncode, 0, f"{v.stdout}\n{v.stderr}")

    def test_full_chaos_log_converges(self):
        # 重复投递 + 旧版本 + 复活探针 + 旧段重投 + 半事务尾巴，全部混在一起
        r = cli("genlog", "--out", self.log, "--txns", "60", "--seed", "21",
                "--duplicates", "--stale", "--redeliver", "--partial-tail",
                "--expect-out", self.dir.name + "/expected.json")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.run_sync()
        # 半事务尾巴未生效：进度停在完整事务边界
        self.assertLess(self.offset(), os.path.getsize(self.log))
        v = cli("verify", "--db", self.db, "--expect", self.dir.name + "/expected.json")
        self.assertEqual(v.returncode, 0, f"{v.stdout}\n{v.stderr}")
        # 补全尾巴后收敛
        with open(self.log, "a", encoding="utf-8") as dst, \
                open(self.log + ".tail-rest", encoding="utf-8") as src:
            dst.write(src.read())
        self.run_sync()
        self.assertEqual(self.offset(), os.path.getsize(self.log))
        tail = self.query("SELECT * FROM orders WHERE id='o-21-tail'")
        self.assertEqual(len(tail), 1)
        self.assertEqual(self.orphans(), [])

    def test_progress_and_conflicts_commands(self):
        write_log(self.log, [
            ev(1, 0, "orders", "o1", "insert", 1, order("o1", amount=100.0)),
            ev(2, 0, "orders", "o1", "update", 1, order("o1", amount=999.0)),
        ])
        self.run_sync(expect_code=EXIT_CONFLICT)
        p = cli("progress", "--db", self.db, "--log", self.log)
        self.assertEqual(p.returncode, 0)
        self.assertIn("conflicts:      1", p.stdout)
        self.assertIn("unconsumed", p.stdout)
        c = cli("conflicts", "--db", self.db)
        self.assertEqual(c.returncode, 0)
        self.assertIn("o1", c.stdout)
        self.assertIn("999", c.stdout)


if __name__ == "__main__":
    unittest.main()
