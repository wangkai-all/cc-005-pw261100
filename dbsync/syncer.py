"""同步引擎：把 JSONL 变更日志事务性地应用到 SQLite。

核心不变量：
  1. 一笔源事务的所有变更 + 消费进度（applied_offset）在同一个 SQLite 事务里提交。
     进程在任意写入位置崩溃，SQLite 都会回滚未提交事务 —— 重启后从已提交进度
     重新消费，不漏数据，也不会重复产生业务记录（版本守卫保证幂等）。
  2. 只有看到 commit 标记的完整事务才允许应用；日志尾部的半笔事务留在缓冲区
     等待后续内容。
  3. 行版本只升不降：旧版本事件被丢弃；删除写墓碑，阻止旧插入事件复活数据。
  4. 同主键、同版本、内容不同 => 冲突：整笔事务回滚，记录到 conflicts 表，
     消费进度停在该事务之前，绝不静默二选一。
"""

import json
import os
import sqlite3
import sys
import time
from collections import OrderedDict, deque
from datetime import datetime, timezone

from .events import ParseError, parse_event
from .schema import BUSINESS_TABLES, ensure_schema

EXIT_OK = 0
EXIT_CONFLICT = 2
EXIT_PARSE_ERROR = 3
EXIT_IO_ERROR = 4
EXIT_CRASH_INJECTED = 42


def _utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ConflictError(Exception):
    """同主键同版本但内容不同。携带冲突事件与目标库现有行。"""

    def __init__(self, event, existing):
        self.event = event
        self.existing = existing
        super().__init__(
            f"conflict on {event.table}/{event.pk} v{event.version} (txid={event.txid}): "
            f"existing={json.dumps(existing, ensure_ascii=False, sort_keys=True)} "
            f"incoming={json.dumps(event.data, ensure_ascii=False, sort_keys=True)}"
        )


class FaultInjector:
    """故障注入：在指定写入位置用 os._exit 模拟进程崩溃（不走任何清理逻辑）。

    crash_after_read=N   读到第 N 个事件后立即崩溃（事务可能只读到一半）
    crash_mid_txn        第一笔多事件事务应用了第一条 DML 后、COMMIT 前崩溃
    crash_before_commit  第一笔事务全部写完（含进度）后、COMMIT 前崩溃
    """

    def __init__(self, crash_after_read=None, crash_mid_txn=False, crash_before_commit=False):
        self.crash_after_read = crash_after_read
        self.crash_mid_txn = crash_mid_txn
        self.crash_before_commit = crash_before_commit
        self._mid_done = False
        self._pre_commit_done = False

    def on_event_read(self, n):
        if self.crash_after_read is not None and n >= self.crash_after_read:
            self.die(f"crash-after-read: read event #{n}, process killed before applying")

    def on_mid_txn(self):
        if self.crash_mid_txn and not self._mid_done:
            self._mid_done = True
            self.die("mid-txn: transaction half-written, killed before COMMIT")

    def on_before_commit(self):
        if self.crash_before_commit and not self._pre_commit_done:
            self._pre_commit_done = True
            self.die("before-commit: all writes done (data+progress), killed before COMMIT")

    @staticmethod
    def die(why):
        print(f"[fault-injection] {why}", file=sys.stderr, flush=True)
        os._exit(EXIT_CRASH_INJECTED)


class TxBuffer:
    """一笔事务的事件缓冲，直到 commit 标记到达。"""

    __slots__ = ("buf_id", "txid", "events", "complete")

    def __init__(self, buf_id, txid):
        self.buf_id = buf_id
        self.txid = txid
        self.events = []
        self.complete = False


class Syncer:
    def __init__(self, db_path, log_path, fault=None):
        self.db_path = db_path
        self.log_path = log_path
        self.fault = fault or FaultInjector()
        self.db = None
        self.watermark = 0               # 已提交的消费进度（文件字节偏移）
        self.pending = OrderedDict()     # txid -> TxBuffer（队首未完成则阻塞，按日志顺序应用）
        self.window = deque()            # (offset_end, buf_id)：水位线之后已读事件的窗口
        self.applied_buf_ids = set()     # 窗口内已应用的 buf_id
        self.next_buf_id = 1
        self.events_read = 0
        self.events_applied = 0
        self.txns_applied = 0
        self.conflict = None
        self._fh = None
        self._line_no = 0

    # ------------------------------------------------------------------ setup

    def open(self):
        self.db = sqlite3.connect(self.db_path, isolation_level=None)  # 手动控制事务
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=5000")
        ensure_schema(self.db)
        self.watermark = int(self._get_meta("applied_offset", "0"))

    def close(self):
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        if self.db is not None:
            self.db.close()
            self.db = None

    def _get_meta(self, key, default=None):
        row = self.db.execute("SELECT value FROM sync_meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def _set_meta(self, key, value):
        self.db.execute(
            "INSERT INTO sync_meta(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)))

    # ------------------------------------------------------------------ main loop

    def run(self, once=False, interval=0.5):
        """消费日志。once=True 处理到当前 EOF 即返回；否则持续 tail。"""
        self.open()
        try:
            while self._fh is None:
                if os.path.exists(self.log_path):
                    if os.path.getsize(self.log_path) < self.watermark:
                        raise RuntimeError(
                            f"log file {self.log_path} is smaller than applied offset "
                            f"{self.watermark} — log was truncated, refusing to continue")
                    self._fh = open(self.log_path, "rb")
                    self._fh.seek(self.watermark)
                    self._line_no = None  # 行号仅用于报错，恢复后无法得知精确行号
                elif once:
                    raise FileNotFoundError(f"log file not found: {self.log_path}")
                else:
                    time.sleep(interval)

            while True:
                self._drain_log()
                if once:
                    break
                time.sleep(interval)
        except ConflictError as e:
            # 冲突：整笔事务已回滚，进度未越过它。记录后停住。
            self.conflict = e
            self._record_conflict(e)
        return self

    def _drain_log(self):
        """读取并处理所有当前可用的完整行；尾部半行留待下次。"""
        while True:
            pos = self._fh.tell()
            raw = self._fh.readline()
            if not raw:
                break  # EOF，等待追加
            if not raw.endswith(b"\n"):
                self._fh.seek(pos)  # 半行：还没写完，下次再读
                break
            line = raw.decode("utf-8").strip()
            if self._line_no is not None:
                self._line_no += 1
            if not line:
                continue
            ev = parse_event(line, self._line_no or 0)
            self.events_read += 1
            self.fault.on_event_read(self.events_read)
            self._on_event(ev, self._fh.tell())

    # ------------------------------------------------------- transaction handling

    def _on_event(self, ev, offset_end):
        buf = self.pending.get(ev.txid)
        if buf is None:
            buf = TxBuffer(self.next_buf_id, ev.txid)
            self.next_buf_id += 1
            self.pending[ev.txid] = buf
        if ev.is_op:
            buf.events.append(ev)
        if ev.commit:
            buf.complete = True
        self.window.append((offset_end, buf.buf_id))
        self._drain_completed()

    def _drain_completed(self):
        """按日志出现顺序应用已完成事务（队首阻塞，保证不越过任何问题事务）。"""
        while self.pending:
            buf = next(iter(self.pending.values()))
            if not buf.complete:
                break
            self._apply_txn(buf)
            del self.pending[buf.txid]

    def _apply_txn(self, buf):
        """单笔源事务：所有 DML + 消费进度在同一个 SQLite 事务里提交。"""
        events = sorted(buf.events, key=lambda e: e.seq)
        con = self.db
        con.execute("BEGIN IMMEDIATE")
        try:
            for i, ev in enumerate(events):
                self._apply_event(con, ev)
                self.events_applied += 1
                if i == 0 and len(events) > 1:
                    self.fault.on_mid_txn()  # 故障点：事务写到一半

            # 推进水位线：窗口前缀中所有已应用事件的末尾
            self.applied_buf_ids.add(buf.buf_id)
            while self.window and self.window[0][1] in self.applied_buf_ids:
                off, _ = self.window.popleft()
                self.watermark = off
            live_ids = {bid for _, bid in self.window}
            self.applied_buf_ids.intersection_update(live_ids)

            self._set_meta("applied_offset", self.watermark)
            self._set_meta("last_txid", buf.txid)
            self._set_meta("applied_at", _utcnow())

            self.fault.on_before_commit()  # 故障点：写完未提交
            con.commit()
            self.txns_applied += 1
        except BaseException:
            con.rollback()
            raise

    # ------------------------------------------------------------- row semantics

    def _apply_event(self, con, ev):
        cols = BUSINESS_TABLES[ev.table]
        row = con.execute(f"SELECT * FROM {ev.table} WHERE id = ?", (ev.pk,)).fetchone()
        tomb = con.execute(
            "SELECT version FROM tombstones WHERE table_name=? AND pk=?",
            (ev.table, ev.pk)).fetchone()

        if ev.op in ("insert", "update"):
            if row is not None:
                cur_v = row["version"]
                if ev.version > cur_v:
                    self._write_row(con, ev, cols)
                    con.execute("DELETE FROM tombstones WHERE table_name=? AND pk=?",
                                (ev.table, ev.pk))
                elif ev.version == cur_v:
                    if not _same_content(row, ev.data, cols):
                        raise ConflictError(ev, _row_to_dict(row))
                    # 内容相同：重复投递，幂等跳过
                else:
                    pass  # 旧版本：不得覆盖新版本
            else:
                if tomb is not None and tomb["version"] >= ev.version:
                    pass  # 已在此版本或更高版本删除：禁止复活
                else:
                    if tomb is not None:
                        con.execute("DELETE FROM tombstones WHERE table_name=? AND pk=?",
                                    (ev.table, ev.pk))
                    self._write_row(con, ev, cols)
        elif ev.op == "delete":
            if row is not None:
                if ev.version >= row["version"]:
                    con.execute(f"DELETE FROM {ev.table} WHERE id = ?", (ev.pk,))
                    self._put_tombstone(con, ev)
                # else：旧版本的删除，忽略
            else:
                if tomb is None or ev.version > tomb["version"]:
                    # 行不存在也记墓碑：挡住随后可能到达的旧插入事件
                    self._put_tombstone(con, ev)

    def _write_row(self, con, ev, cols):
        values = [ev.data.get(c) for c in cols]
        values[0] = ev.pk  # 主键以 pk 字段为准
        collist = ", ".join(cols)
        placeholders = ", ".join("?" for _ in cols)
        updates = ", ".join(f"{c}=excluded.{c}" for c in cols[1:])
        con.execute(
            f"INSERT INTO {ev.table}({collist}, version) VALUES ({placeholders}, ?) "
            f"ON CONFLICT(id) DO UPDATE SET {updates}, version=excluded.version",
            (*values, ev.version))

    def _put_tombstone(self, con, ev):
        con.execute(
            "INSERT INTO tombstones(table_name, pk, version, txid, deleted_at) "
            "VALUES (?,?,?,?,?) "
            "ON CONFLICT(table_name, pk) DO UPDATE SET "
            "version=excluded.version, txid=excluded.txid, deleted_at=excluded.deleted_at",
            (ev.table, ev.pk, ev.version, ev.txid, _utcnow()))

    # ------------------------------------------------------------------ conflicts

    def _record_conflict(self, err):
        ev = err.event
        self.db.execute(
            "INSERT OR REPLACE INTO conflicts"
            "(txid, table_name, pk, version, existing, incoming, detected_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (ev.txid, ev.table, ev.pk, ev.version,
             json.dumps(err.existing, ensure_ascii=False, sort_keys=True),
             json.dumps(ev.data, ensure_ascii=False, sort_keys=True),
             _utcnow()))
        self.db.commit()

    # ------------------------------------------------------------------ reporting

    def summary_lines(self):
        lines = [
            f"applied {self.txns_applied} transaction(s), {self.events_applied} event(s) this run",
            f"committed offset: {self.watermark}",
        ]
        if self.conflict:
            lines.append(f"STOPPED on conflict: {self.conflict}")
            if self.pending:
                txids = ", ".join(self.pending.keys())
                lines.append(f"transaction(s) blocked behind the conflict: {txids}")
        elif self.pending:
            txids = ", ".join(self.pending.keys())
            lines.append(f"incomplete transaction(s) waiting for commit marker: {txids}")
        else:
            lines.append("no incomplete transactions")
        return lines


def _row_to_dict(row):
    return {k: row[k] for k in row.keys()}


def _same_content(row, data, cols):
    """目标行与事件 data（完整后镜像）内容是否一致（version 列除外）。"""
    for c in cols:
        if c == "id":
            continue
        incoming = data.get(c)
        current = row[c]
        if current == incoming:
            continue
        # 容忍 JSON 整数与 SQLite REAL 的表示差异（如 5 vs 5.0）
        try:
            if current is not None and incoming is not None and float(current) == float(incoming):
                continue
        except (TypeError, ValueError):
            pass
        return False
    return True
