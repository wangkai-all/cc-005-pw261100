"""命令行入口：

  python -m dbsync init      --db target.db
  python -m dbsync genlog    --out changes.jsonl [--txns 30] [--seed 1]
                             [--duplicates] [--stale] [--conflict] [--redeliver]
                             [--partial-tail] [--expect-out expected.json]
  python -m dbsync run       --db target.db --log changes.jsonl [--once] [--interval 0.5]
                             [--crash-after-read N] [--crash-mid-txn] [--crash-before-commit]
  python -m dbsync progress  --db target.db [--log changes.jsonl]
  python -m dbsync conflicts --db target.db
  python -m dbsync verify    --db target.db [--expect expected.json]
"""

import argparse
import json
import os
import sqlite3
import sys

from .events import ParseError
from .generator import generate
from .schema import BUSINESS_TABLES, ensure_schema
from .syncer import (EXIT_CONFLICT, EXIT_IO_ERROR, EXIT_OK, EXIT_PARSE_ERROR,
                     FaultInjector, Syncer)


def _connect(db_path):
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def cmd_init(args):
    con = _connect(args.db)
    ensure_schema(con)
    con.close()
    print(f"initialized {args.db}")
    return EXIT_OK


def cmd_genlog(args):
    info = generate(
        out_path=args.out, txns=args.txns, seed=args.seed,
        partial_tail=args.partial_tail, duplicates=args.duplicates,
        stale=args.stale, conflict=args.conflict, redeliver=args.redeliver,
        expect_out=args.expect_out)
    print(f"wrote {info['lines']} event(s) to {info['path']}")
    print(f"final live state: {info['live_orders']} order(s), {info['live_items']} item(s)")
    if info["tail_rest"]:
        print(f"partial tail written; complete it later with: "
              f"cat {info['tail_rest']} >> {info['path']}")
    if args.expect_out:
        print(f"expected final state written to {args.expect_out}")
    return EXIT_OK


def cmd_run(args):
    fault = FaultInjector(
        crash_after_read=args.crash_after_read,
        crash_mid_txn=args.crash_mid_txn,
        crash_before_commit=args.crash_before_commit)
    syncer = Syncer(args.db, args.log, fault=fault)
    try:
        syncer.run(once=args.once, interval=args.interval)
    except ParseError as e:
        print(f"[run] FATAL: unparseable log, stopped without advancing: {e}",
              file=sys.stderr)
        return EXIT_PARSE_ERROR
    except (FileNotFoundError, RuntimeError) as e:
        print(f"[run] FATAL: {e}", file=sys.stderr)
        return EXIT_IO_ERROR
    except KeyboardInterrupt:
        print("\n[run] interrupted", file=sys.stderr)
    finally:
        for line in syncer.summary_lines():
            print(f"[run] {line}")
        conflict = syncer.conflict
        syncer.close()
    if conflict:
        print("[run] stopped on conflict; progress NOT advanced past the conflicting "
              "transaction. Inspect with: python -m dbsync conflicts --db " + args.db,
              file=sys.stderr)
        return EXIT_CONFLICT
    return EXIT_OK


def cmd_progress(args):
    con = _connect(args.db)
    ensure_schema(con)
    meta = {r["key"]: r["value"] for r in con.execute("SELECT key, value FROM sync_meta")}
    offset = int(meta.get("applied_offset") or 0)
    print(f"database:        {args.db}")
    print(f"applied offset:  {offset} byte(s)")
    print(f"last applied tx: {meta.get('last_txid') or '-'}")
    print(f"applied at:      {meta.get('applied_at') or '-'}")
    for table in BUSINESS_TABLES:
        n = con.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        print(f"{table + ':':16}{n} row(s)")
    tombs = con.execute("SELECT COUNT(*) AS c FROM tombstones").fetchone()["c"]
    confs = con.execute("SELECT COUNT(*) AS c FROM conflicts").fetchone()["c"]
    print(f"tombstones:     {tombs}")
    print(f"conflicts:      {confs}")
    if args.log:
        if os.path.exists(args.log):
            size = os.path.getsize(args.log)
            lag = size - offset
            state = "in sync" if lag == 0 else f"{lag} byte(s) unconsumed (waiting for more data or stopped)"
            print(f"log file:       {args.log} ({size} byte(s)) -> {state}")
        else:
            print(f"log file:       {args.log} (missing)")
    con.close()
    return EXIT_OK


def cmd_conflicts(args):
    con = _connect(args.db)
    ensure_schema(con)
    rows = con.execute(
        "SELECT * FROM conflicts ORDER BY id").fetchall()
    if not rows:
        print("no conflicts recorded")
        con.close()
        return EXIT_OK
    print(f"{len(rows)} conflict(s):")
    for r in rows:
        print(f"  #{r['id']} txid={r['txid']} {r['table_name']}/{r['pk']} "
              f"v{r['version']} detected_at={r['detected_at']}")
        print(f"      existing: {r['existing']}")
        print(f"      incoming: {r['incoming']}")
    con.close()
    return EXIT_OK


def cmd_verify(args):
    con = _connect(args.db)
    ensure_schema(con)
    orders = [dict(r) for r in con.execute("SELECT * FROM orders ORDER BY id")]
    items = [dict(r) for r in con.execute("SELECT * FROM order_items ORDER BY id")]
    ok = True

    order_ids = {o["id"] for o in orders}
    orphans = [i for i in items if i["order_id"] not in order_ids]
    print(f"orders: {len(orders)}  order_items: {len(items)}  orphan items: {len(orphans)}")
    if orphans:
        ok = False
        for i in orphans[:10]:
            print(f"  ORPHAN: item {i['id']} references missing order {i['order_id']}")

    if args.expect:
        with open(args.expect, encoding="utf-8") as f:
            exp = json.load(f)
        for table, actual in (("orders", orders), ("order_items", items)):
            want = sorted(exp[table], key=lambda r: r["id"])
            norm = lambda rows: [json.dumps(r, sort_keys=True) for r in rows]
            if norm(want) != norm(actual):
                ok = False
                want_ids = {r["id"] for r in want}
                got_ids = {r["id"] for r in actual}
                print(f"  MISMATCH in {table}: "
                      f"missing={sorted(want_ids - got_ids)[:5]} "
                      f"unexpected={sorted(got_ids - want_ids)[:5]}")
                for w, g in zip(norm(want), norm(actual)):
                    if w != g:
                        print(f"    expected: {w}\n    actual:   {g}")
                        break
            else:
                print(f"  {table}: matches expected state ({len(actual)} row(s))")
    con.close()
    print("VERIFY: " + ("PASS" if ok else "FAIL"))
    return EXIT_OK if ok else 1


def build_parser():
    p = argparse.ArgumentParser(
        prog="dbsync",
        description="Sync a JSONL change log into SQLite with transactional guarantees.")
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("init", help="create target schema")
    sp.add_argument("--db", required=True)
    sp.set_defaults(func=cmd_init)

    sp = sub.add_parser("genlog", help="generate a local change log")
    sp.add_argument("--out", required=True)
    sp.add_argument("--txns", type=int, default=30)
    sp.add_argument("--seed", type=int, default=1)
    sp.add_argument("--duplicates", action="store_true",
                    help="re-deliver some segments (idempotency test)")
    sp.add_argument("--stale", action="store_true",
                    help="inject old-version events and resurrection probes")
    sp.add_argument("--conflict", action="store_true",
                    help="inject a same-pk same-version different-content event")
    sp.add_argument("--redeliver", action="store_true",
                    help="append a copy of an old segment at the end")
    sp.add_argument("--partial-tail", action="store_true",
                    help="end the log with half a transaction (no commit marker)")
    sp.add_argument("--expect-out", help="write expected final state JSON here")
    sp.set_defaults(func=cmd_genlog)

    sp = sub.add_parser("run", help="start syncing")
    sp.add_argument("--db", required=True)
    sp.add_argument("--log", required=True)
    sp.add_argument("--once", action="store_true",
                    help="process to current EOF and exit (default: keep tailing)")
    sp.add_argument("--interval", type=float, default=0.5,
                    help="poll interval in seconds when tailing")
    sp.add_argument("--crash-after-read", type=int, default=None, metavar="N",
                    help="fault injection: kill process after reading N events")
    sp.add_argument("--crash-mid-txn", action="store_true",
                    help="fault injection: kill process with a transaction half-written")
    sp.add_argument("--crash-before-commit", action="store_true",
                    help="fault injection: kill process after writes, before COMMIT")
    sp.set_defaults(func=cmd_run)

    sp = sub.add_parser("progress", help="show consumption progress")
    sp.add_argument("--db", required=True)
    sp.add_argument("--log", help="also compare against this log file's size")
    sp.set_defaults(func=cmd_progress)

    sp = sub.add_parser("conflicts", help="list recorded conflicts")
    sp.add_argument("--db", required=True)
    sp.set_defaults(func=cmd_conflicts)

    sp = sub.add_parser("verify", help="check orders/items consistency")
    sp.add_argument("--db", required=True)
    sp.add_argument("--expect", help="expected final state JSON from genlog --expect-out")
    sp.set_defaults(func=cmd_verify)

    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    return args.func(args)
