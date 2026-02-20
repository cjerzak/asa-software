# Speed Test Report

**Last Run:** 2026-02-19 18:26:12 CST
**Overall Status:** PASS
**Tolerance Factor:** 4.00x baseline

## Results

| Benchmark | Current | Baseline | Threshold | Ratio | Status |
|-----------|---------|----------|-----------|-------|--------|
| build_prompt | 0.1413s | 0.09s | 0.36s | 1.57x | PASS |
| helper_funcs | 0.0764s | 0.07s | 0.28s | 1.09x | PASS |
| combined | 0.1160s | 0.09s | 0.36s | 1.27x | PASS |
| agent_search | 30.9s | 18s | 70s | 1.75x | PASS |

## Baseline Reference

Baselines were established on Dec 19, 2024. Tests fail if current time exceeds
`baseline * 4.00`.

| Benchmark | Baseline | Operations per Run |
|-----------|----------|-------------------|
| build_prompt | 0.09s | 10 iterations x 50 calls x 3 templates |
| helper_funcs | 0.07s | 10 iterations x 100 calls x 5 functions |
| combined | 0.09s | 10 iterations of mixed workload |
| agent_search | 18s | 1 search task (API + network latency) |

