# Speed Test Report

**Last Run:** 2025-12-19 18:51:18 EST
**Overall Status:** PASS
**Tolerance Factor:** 1.25x baseline

## Results

| Benchmark | Current | Baseline | Threshold | Ratio | Status |
|-----------|---------|----------|-----------|-------|--------|
| build_prompt | 0.0948s | 0.15s | 0.19s | 0.63x | PASS |
| helper_funcs | 0.0762s | 0.15s | 0.19s | 0.51x | PASS |
| combined | 0.1067s | 0.20s | 0.25s | 0.53x | PASS |

## Baseline Reference

Baselines were established on Dec 19, 2024. Tests fail if current time exceeds
`baseline * 1.25`.

| Benchmark | Baseline | Operations per Run |
|-----------|----------|-------------------|
| build_prompt | 0.15s | 10 iterations x 50 calls x 3 templates |
| helper_funcs | 0.15s | 10 iterations x 100 calls x 5 functions |
| combined | 0.20s | 10 iterations of mixed workload |

