from __future__ import annotations

import io
import math
import struct
import zlib
from typing import Iterable, List


def vbyte_encode(numbers: Iterable[int]) -> bytes:
    out = bytearray()
    for n in numbers:
        if n < 0:
            raise ValueError("vbyte only supports non-negative integers")
        while True:
            to_write = n & 0x7F
            n >>= 7
            if n:
                out.append(to_write | 0x80)
            else:
                out.append(to_write)
                break
    return bytes(out)


def vbyte_decode(data: bytes) -> List[int]:
    numbers: List[int] = []
    n = 0
    shift = 0
    for b in data:
        if b & 0x80:
            n |= (b & 0x7F) << shift
            shift += 7
        else:
            n |= (b & 0x7F) << shift
            numbers.append(n)
            n = 0
            shift = 0
    if shift != 0:
        raise ValueError("truncated vbyte stream")
    return numbers


def zlib_compress(raw: bytes, level: int = 6) -> bytes:
    return zlib.compress(raw, level=level)


def zlib_decompress(data: bytes) -> bytes:
    return zlib.decompress(data)


def add_skip_pointers(postings: List[int]) -> List[tuple[int, int]]:
    # Returns list of (doc_id, skip_index) where skip_index is target index or -1
    L = len(postings)
    if L == 0:
        return []
    step = int(math.sqrt(L)) or 1
    result: List[tuple[int, int]] = []
    for i, d in enumerate(postings):
        target = i + step
        result.append((d, target if target < L else -1))
    return result


