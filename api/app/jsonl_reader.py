"""JSONL 日志文件的倒序读取工具。

日志类数据（活动流、执行明细）都是「追加写入、按时间只增」的形态：新记录总在
文件尾部。查询只要最新的一页时，从尾部按块倒着读、取满一页就停，读取量与页
大小相关而不是文件大小。

``bytes_read`` 记录实际读取的字节数，供调用方核对一次查询扫描了多少数据
（也便于测试断言「取一页不会把整个文件读一遍」）。
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

# 倒序读文件时每次拉取的字节块（64 KiB 约等于数百条记录）
READ_CHUNK = 64 * 1024


class ReverseLineReader:
    """从文件尾部倒着逐行读取（行按追加顺序即时间顺序落盘）。"""

    def __init__(self, path: Path, chunk_size: int = READ_CHUNK) -> None:
        self._path = path
        self._chunk = max(1, chunk_size)
        self.bytes_read = 0

    def lines(self) -> Iterator[bytes]:
        with self._path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            pos = fh.tell()
            tail = b""
            while pos > 0:
                size = min(self._chunk, pos)
                pos -= size
                fh.seek(pos)
                chunk = fh.read(size)
                self.bytes_read += len(chunk)
                parts = (chunk + tail).split(b"\n")
                # 第一段可能是不完整的行，留给下一轮拼接
                tail = parts[0]
                for line in reversed(parts[1:]):
                    if line.strip():
                        yield line
            if tail.strip():
                yield tail


__all__ = ["READ_CHUNK", "ReverseLineReader"]
