# Speed Test Report

**Last Run:** 2025-12-19 18:57:18 EST
**Overall Status:** PASS
**Tolerance Factor:** 1.25x baseline

## Results

| Benchmark | Current | Baseline | Threshold | Ratio | Status |
|-----------|---------|----------|-----------|-------|--------|
| build_prompt | 0.0865s | 0.15s | 0.19s | 0.58x | PASS |
| helper_funcs | 0.0705s | 0.15s | 0.19s | 0.47x | PASS |
| combined | 0.0978s | 0.20s | 0.25s | 0.49x | PASS |

## Baseline Reference

Baselines were established on Dec 19, 2024. Tests fail if current time exceeds
`baseline * 1.25`.

| Benchmark | Baseline | Operations per Run |
|-----------|----------|-------------------|
| build_prompt | 0.15s | 10 iterations x 50 calls x 3 templates |
| helper_funcs | 0.15s | 10 iterations x 100 calls x 5 functions |
| combined | 0.20s | 10 iterations of mixed workload |
| agent_search | 60s | 1 search task (API + network latency) |

