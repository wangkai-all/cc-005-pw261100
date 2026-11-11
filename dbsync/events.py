"""JSONL 变更事件的解析与校验。

日志每行一个 JSON 对象，两种形态：

1) 数据事件：
   {"txid": 1001, "seq": 0, "table": "orders", "pk": "o-1", "op": "insert",
    "version": 1, "data": {...整行后镜像...}, "commit": false}
   - op: insert / update / delete
   - insert/update 的 data 是该行的完整后镜像（full after-image）
   - delete 无 data

2) 纯提交标记（可选形态，事务最后一事件也可以直接带 "commit": true）：
   {"txid": 1001, "commit": true}

一笔事务只有看到 commit 标记才允许在目标库生效。
"""

import json
from dataclasses import dataclass
from typing import Optional

from .schema import BUSINESS_TABLES

OPS = ("insert", "update", "delete")


class ParseError(Exception):
    """日志内容无法解析/不合法。同步器必须停住，不能跳过坏数据。"""


@dataclass
class Event:
    txid: str
    seq: int
    commit: bool
    table: Optional[str] = None    # None 表示纯提交标记
    pk: Optional[str] = None
    op: Optional[str] = None
    version: Optional[int] = None
    data: Optional[dict] = None
    line_no: int = 0

    @property
    def is_op(self):
        return self.op is not None


def parse_event(line: str, line_no: int = 0) -> Event:
    where = f"line {line_no}" if line_no else "log line"
    try:
        obj = json.loads(line)
    except json.JSONDecodeError as e:
        raise ParseError(f"{where}: invalid JSON: {e}") from e
    if not isinstance(obj, dict):
        raise ParseError(f"{where}: event must be a JSON object")

    if "txid" not in obj:
        raise ParseError(f"{where}: missing 'txid'")
    txid = str(obj["txid"])
    commit = bool(obj.get("commit", False))
    try:
        seq = int(obj.get("seq", 0))
    except (TypeError, ValueError):
        raise ParseError(f"{where}: 'seq' must be an integer")

    # 纯提交标记：没有 table/op
    if obj.get("table") is None and obj.get("op") is None:
        if not commit:
            raise ParseError(f"{where}: event has neither table/op nor commit marker")
        return Event(txid=txid, seq=seq, commit=True, line_no=line_no)

    table = obj.get("table")
    op = obj.get("op")
    pk = obj.get("pk")
    version = obj.get("version")

    if table not in BUSINESS_TABLES:
        raise ParseError(f"{where}: unknown table {table!r}")
    if op not in OPS:
        raise ParseError(f"{where}: unknown op {op!r}")
    if pk is None:
        raise ParseError(f"{where}: missing 'pk'")
    pk = str(pk)
    if not isinstance(version, int) or version < 0:
        raise ParseError(f"{where}: 'version' must be a non-negative integer")

    data = obj.get("data")
    if op in ("insert", "update"):
        if not isinstance(data, dict):
            raise ParseError(f"{where}: {op} requires a 'data' object (full row image)")
        unknown = set(data) - set(BUSINESS_TABLES[table])
        if unknown:
            raise ParseError(f"{where}: unknown column(s) for {table}: {sorted(unknown)}")
        if "id" in data and str(data["id"]) != pk:
            raise ParseError(f"{where}: data.id {data['id']!r} does not match pk {pk!r}")

    return Event(txid=txid, seq=seq, commit=commit, table=table, pk=pk,
                 op=op, version=version, data=data, line_no=line_no)
