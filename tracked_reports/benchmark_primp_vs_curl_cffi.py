#!/usr/bin/env python3
"""
Benchmark: PRIMP vs curl_cffi for DuckDuckGo HTML search
=========================================================

Compares latency and result quality for both HTTP clients through:
  1. Tor proxy  (socks5h://127.0.0.1:9050)
  2. Direct connection (baseline)

Each condition runs 5 iterations with 3-second inter-request delays.
Results saved to tracked_reports/search_tier_benchmark.txt.
"""

import os
import sys
import time
import statistics
from urllib.parse import urlparse, parse_qs, unquote

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
QUERY = "Ramona Moye Camaconi Bolivia"
URL = "https://html.duckduckgo.com/html"
PARAMS = {"q": QUERY}
ITERATIONS = 5
INTER_DELAY = 3.0  # seconds between requests
TOR_PROXY = "socks5h://127.0.0.1:9050"
TIMEOUT = 30

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "search_tier_benchmark.txt")


def _parse_ddg_links(html: str) -> int:
    """Count result links from DuckDuckGo HTML response."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    return len(soup.select("a.result__a"))


def bench_primp(proxy: str | None, label: str) -> list[dict]:
    """Benchmark PRIMP client."""
    import primp

    results = []
    for i in range(ITERATIONS):
        if i > 0:
            time.sleep(INTER_DELAY)

        t0 = time.perf_counter()
        try:
            client = primp.Client(
                impersonate="chrome_131",
                impersonate_os="windows",
                proxy=proxy,
                timeout=TIMEOUT,
                cookie_store=False,
                follow_redirects=True,
            )
            t_connect = time.perf_counter() - t0

            resp = client.get(URL, params=PARAMS, timeout=TIMEOUT)
            t_total = time.perf_counter() - t0
            t_response = t_total - t_connect

            body = resp.text or ""
            n_links = _parse_ddg_links(body)

            results.append({
                "iter": i + 1,
                "label": label,
                "status": resp.status_code,
                "connect_s": round(t_connect, 3),
                "response_s": round(t_response, 3),
                "total_s": round(t_total, 3),
                "body_len": len(body),
                "n_links": n_links,
                "error": None,
            })
            client.close()
        except Exception as e:
            t_total = time.perf_counter() - t0
            results.append({
                "iter": i + 1,
                "label": label,
                "status": -1,
                "connect_s": -1,
                "response_s": -1,
                "total_s": round(t_total, 3),
                "body_len": 0,
                "n_links": 0,
                "error": f"{type(e).__name__}: {e}",
            })
        print(f"  [{label}] iter {i+1}/{ITERATIONS}: "
              f"total={results[-1]['total_s']:.3f}s, "
              f"links={results[-1]['n_links']}, "
              f"err={results[-1]['error']}")
    return results


def bench_curl_cffi(proxy: str | None, label: str) -> list[dict]:
    """Benchmark curl_cffi client."""
    from curl_cffi import requests as curl_requests

    proxies = {"https": proxy, "http": proxy} if proxy else None
    results = []
    for i in range(ITERATIONS):
        if i > 0:
            time.sleep(INTER_DELAY)

        t0 = time.perf_counter()
        try:
            t_connect = time.perf_counter() - t0  # curl_cffi has no separate connect phase
            resp = curl_requests.get(
                URL,
                params=PARAMS,
                impersonate="chrome131",
                proxies=proxies,
                timeout=TIMEOUT,
            )
            t_total = time.perf_counter() - t0
            t_response = t_total - t_connect

            body = resp.text or ""
            n_links = _parse_ddg_links(body)

            results.append({
                "iter": i + 1,
                "label": label,
                "status": resp.status_code,
                "connect_s": round(t_connect, 3),
                "response_s": round(t_response, 3),
                "total_s": round(t_total, 3),
                "body_len": len(body),
                "n_links": n_links,
                "error": None,
            })
        except Exception as e:
            t_total = time.perf_counter() - t0
            results.append({
                "iter": i + 1,
                "label": label,
                "status": -1,
                "connect_s": -1,
                "response_s": -1,
                "total_s": round(t_total, 3),
                "body_len": 0,
                "n_links": 0,
                "error": f"{type(e).__name__}: {e}",
            })
        print(f"  [{label}] iter {i+1}/{ITERATIONS}: "
              f"total={results[-1]['total_s']:.3f}s, "
              f"links={results[-1]['n_links']}, "
              f"err={results[-1]['error']}")
    return results


def summarise(all_results: list[dict]) -> str:
    """Build summary report."""
    lines = []
    lines.append("=" * 72)
    lines.append("BENCHMARK: PRIMP vs curl_cffi — DuckDuckGo HTML search")
    lines.append(f"Query: {QUERY!r}")
    lines.append(f"Iterations: {ITERATIONS}, Inter-delay: {INTER_DELAY}s")
    lines.append("=" * 72)

    # Group by label
    from collections import defaultdict
    groups = defaultdict(list)
    for r in all_results:
        groups[r["label"]].append(r)

    for label in groups:
        rows = groups[label]
        lines.append(f"\n--- {label} ---")
        ok = [r for r in rows if r["error"] is None]
        fail = [r for r in rows if r["error"] is not None]
        lines.append(f"  Success: {len(ok)}/{len(rows)}")
        if fail:
            for f in fail:
                lines.append(f"  FAIL iter {f['iter']}: {f['error']}")

        if ok:
            totals = [r["total_s"] for r in ok]
            links = [r["n_links"] for r in ok]
            bodies = [r["body_len"] for r in ok]
            lines.append(f"  Total time (s): mean={statistics.mean(totals):.3f}  "
                         f"median={statistics.median(totals):.3f}  "
                         f"min={min(totals):.3f}  max={max(totals):.3f}")
            if len(totals) > 1:
                lines.append(f"                  stdev={statistics.stdev(totals):.3f}")
            lines.append(f"  Links:          mean={statistics.mean(links):.1f}  "
                         f"min={min(links)}  max={max(links)}")
            lines.append(f"  Body length:    mean={statistics.mean(bodies):.0f}  "
                         f"min={min(bodies)}  max={max(bodies)}")

        # Per-iteration detail
        lines.append(f"  {'iter':>4}  {'status':>6}  {'total_s':>8}  {'links':>5}  {'body_len':>8}  error")
        for r in rows:
            lines.append(f"  {r['iter']:>4}  {r['status']:>6}  {r['total_s']:>8.3f}  "
                         f"{r['n_links']:>5}  {r['body_len']:>8}  {r['error'] or ''}")

    # Comparative summary
    lines.append("\n" + "=" * 72)
    lines.append("COMPARATIVE SUMMARY")
    lines.append("=" * 72)
    for mode in ["tor", "direct"]:
        p_label = f"primp+{mode}"
        c_label = f"curl_cffi+{mode}"
        p_ok = [r for r in groups.get(p_label, []) if r["error"] is None]
        c_ok = [r for r in groups.get(c_label, []) if r["error"] is None]
        if p_ok and c_ok:
            p_mean = statistics.mean([r["total_s"] for r in p_ok])
            c_mean = statistics.mean([r["total_s"] for r in c_ok])
            diff_pct = ((c_mean - p_mean) / p_mean) * 100
            winner = "curl_cffi" if c_mean < p_mean else "primp"
            lines.append(f"\n  [{mode.upper()}]  primp={p_mean:.3f}s  curl_cffi={c_mean:.3f}s  "
                         f"diff={diff_pct:+.1f}%  winner={winner}")
        else:
            lines.append(f"\n  [{mode.upper()}]  insufficient data for comparison")

    return "\n".join(lines)


def main():
    print("Starting PRIMP vs curl_cffi benchmark...")
    print(f"Query: {QUERY!r}")
    print(f"Tor proxy: {TOR_PROXY}")
    print()

    all_results = []

    # 1) curl_cffi + Tor
    print("=== curl_cffi + Tor ===")
    all_results.extend(bench_curl_cffi(TOR_PROXY, "curl_cffi+tor"))
    time.sleep(INTER_DELAY)

    # 2) PRIMP + Tor
    print("\n=== PRIMP + Tor ===")
    all_results.extend(bench_primp(TOR_PROXY, "primp+tor"))
    time.sleep(INTER_DELAY)

    # 3) curl_cffi + Direct
    print("\n=== curl_cffi + Direct ===")
    all_results.extend(bench_curl_cffi(None, "curl_cffi+direct"))
    time.sleep(INTER_DELAY)

    # 4) PRIMP + Direct
    print("\n=== PRIMP + Direct ===")
    all_results.extend(bench_primp(None, "primp+direct"))

    # Summary
    report = summarise(all_results)
    print("\n" + report)

    with open(OUTPUT_PATH, "w") as f:
        f.write(report + "\n")
    print(f"\nResults saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
