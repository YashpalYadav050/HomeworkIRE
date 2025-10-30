from __future__ import annotations

import json
import time
from statistics import mean
from typing import Callable, Dict, Iterable, List, Sequence, Tuple

import numpy as np


def percentile_latencies(latencies_ms: Sequence[float]) -> Dict[str, float]:
    if not latencies_ms:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0}
    arr = np.array(latencies_ms)
    return {
        "p50": float(np.percentile(arr, 50)),
        "p95": float(np.percentile(arr, 95)),
        "p99": float(np.percentile(arr, 99)),
        "avg": float(arr.mean()),
    }


def measure_latency(fn: Callable[[str], object], queries: Sequence[str]) -> Tuple[List[float], Dict[str, float]]:
    latencies: List[float] = []
    for q in queries:
        t0 = time.perf_counter()
        _ = fn(q)
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000.0)
    return latencies, percentile_latencies(latencies)


def measure_throughput(fn: Callable[[str], object], queries: Sequence[str]) -> float:
    if not queries:
        return 0.0
    t0 = time.perf_counter()
    for q in queries:
        _ = fn(q)
    t1 = time.perf_counter()
    elapsed = t1 - t0
    return float(len(queries) / elapsed) if elapsed > 0 else float("inf")


def precision_recall_at_k(predicted: Sequence[str], relevant: Sequence[str], k: int) -> Tuple[float, float]:
    k = max(1, min(k, len(predicted)))
    topk = set(predicted[:k])
    rel = set(relevant)
    tp = len(topk & rel)
    prec = tp / float(k)
    rec = tp / float(len(rel)) if rel else 0.0
    return prec, rec


def mean_average_precision(preds: Sequence[Sequence[str]], rels: Sequence[Sequence[str]]) -> float:
    ap_values: List[float] = []
    for predicted, relevant in zip(preds, rels):
        if not relevant:
            ap_values.append(0.0)
            continue
        rel_set = set(relevant)
        hit_count = 0
        precisions: List[float] = []
        for i, doc_id in enumerate(predicted, start=1):
            if doc_id in rel_set:
                hit_count += 1
                precisions.append(hit_count / i)
        ap_values.append(mean(precisions) if precisions else 0.0)
    return float(mean(ap_values)) if ap_values else 0.0


