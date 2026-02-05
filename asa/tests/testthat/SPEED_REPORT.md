# Speed Test Report

**Last Run:** 2026-02-05 13:35:59 CST
**Overall Status:** PASS
**Tolerance Factor:** 4.00x baseline

## Results

| Benchmark | Current | Baseline | Threshold | Ratio | Status |
|-----------|---------|----------|-----------|-------|--------|
| build_prompt | 0.1257s | 0.09s | 0.36s | 1.40x | PASS |
| helper_funcs | 0.0701s | 0.07s | 0.28s | 1.00x | PASS |
| combined | 0.1089s | 0.09s | 0.36s | 1.20x | PASS |
| agent_search | 23.0s | 18s | 70s | 1.31x | PASS |

## Baseline Reference

Baselines were established on Dec 19, 2024. Tests fail if current time exceeds
`baseline * 4.00`.

| Benchmark | Baseline | Operations per Run |
|-----------|----------|-------------------|
| build_prompt | 0.09s | 10 iterations x 50 calls x 3 templates |
| helper_funcs | 0.07s | 10 iterations x 100 calls x 5 functions |
| combined | 0.09s | 10 iterations of mixed workload |
| agent_search | 18s | 1 search task (API + network latency) |

