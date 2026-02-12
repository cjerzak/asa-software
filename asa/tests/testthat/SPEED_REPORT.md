# Speed Test Report

**Last Run:** 2026-02-12 14:01:40 CST
**Overall Status:** PASS
**Tolerance Factor:** 4.00x baseline

## Results

| Benchmark | Current | Baseline | Threshold | Ratio | Status |
|-----------|---------|----------|-----------|-------|--------|
| build_prompt | 0.1276s | 0.09s | 0.36s | 1.42x | PASS |
| helper_funcs | 0.0717s | 0.07s | 0.28s | 1.02x | PASS |
| combined | 0.1115s | 0.09s | 0.36s | 1.23x | PASS |
| agent_search | 31.4s | 18s | 70s | 1.78x | PASS |

## Baseline Reference

Baselines were established on Dec 19, 2024. Tests fail if current time exceeds
`baseline * 4.00`.

| Benchmark | Baseline | Operations per Run |
|-----------|----------|-------------------|
| build_prompt | 0.09s | 10 iterations x 50 calls x 3 templates |
| helper_funcs | 0.07s | 10 iterations x 100 calls x 5 functions |
| combined | 0.09s | 10 iterations of mixed workload |
| agent_search | 18s | 1 search task (API + network latency) |

