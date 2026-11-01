# Benchmarks

What metering costs per request, and what the stack sustains end to end. Numbers below were measured on 2026-10-31 and 2026-11-01.

Target from [DESIGN.md](../DESIGN.md): ~2,000 quota operations/second sustained, peaking at 10,000/second, with <10 ms metering overhead per request.

## Environment

Every run up to [Isolated machines](#isolated-machines-2026-11-01) shares the same machine, load generator included, so throughput numbers are a floor for this PC, not a property of the design.

- CPU: AMD Ryzen 5 5600X, 6 cores / 12 threads
- OS: Windows 11 Pro, Docker Desktop 29.7.2 (WSL2 VM, 12 CPUs, 16 GB)
- Stack ([compose.yaml](../compose.yaml)): 8 API replicas (one uvicorn process each, or one granian worker each via the override in section 3) behind nginx 1.27 round-robin, one Redis 8.4 with AOF on
- Test org: a throwaway `bench` org with a limit of 10^12, so no request is ever rejected for quota. Its keys are deleted afterwards.

## Benchmarking methodology and notes

A stress testing tool like ab is a powerful tool to check how the API sustains performance. Picked oha because it's a more modern tool.

Uvicorn remains the baseline with granian comparisons.

httpx / `bench http` was a bottleneck, native binary of oha much better for stress testing.

nginx keepalive wasn't included earlier, added now to reduce new connection per request overhead.

We also ran into redis max connections error that resulted in 502s but they dished out 500 internal server error rather than a 503.

Results vary if stress testing the deployment stack from Windows host or inside docker's network. We'll pick inside docker's network as the default for now.

## Rate sweeps (2026-11-01)

`API_REPLICAS=6 uv run scripts/bench_suite.py overhead -q 2000 5000 8000 10000 15000` on `/container_tracking`: open loop (`oha -q --latency-correction`), 20s per rate, 512 connections, 6 replicas, oha pinned to 2 CPUs. Cells: successful req/s (p50 / p99 ms).

### 1. Access logs off (uvicorn + nginx)

`bench-results/20261101-143723`

| Rate | uvicorn off | uvicorn on | granian off | granian on |
| --- | --- | --- | --- | --- |
| 2k | 1,998 (0.8 / 1.6) | 1,997 (1.2 / 7.1) | 1,999 (0.9 / 1.9) | 1,999 (1.6 / 6.3) |
| 5k | 4,996 (0.9 / 3.3) | 4,992 (99 / 453) | 4,996 (1.1 / 26.9) | 4,973 (82 / 832) |
| 8k | 7,995 (1.5 / 26.1) | 5,057 (3,657 / 7,252) | 7,988 (2.2 / 33.0) | 5,125 (3,830 / 7,114) |
| 10k | 9,815 (23 / 370) | 5,077 (4,822 / 9,742) | 9,985 (4.1 / 55.9) | 5,125 (5,239 / 9,663) |
| 15k | 9,925 (2,952 / 6,666) | 5,111 (6,532 / 13,025) | 11,935 (2,058 / 4,033) | 5,127 (6,454 / 13,042) |

Meter on caps at ~5.1k; meter off ~10k (uvicorn) / ~12k (granian). nginx log off didn't move the metered cap.

### 2. + Swagger headers via `openapi_extra`, cached `current_period()`

X-Org-Id / X-Request-Id were `Depends` (resolved every request) only for Swagger; now OpenAPI-only, schema unchanged. `current_period()` (~2% replica CPU) cached until month end. `bench-results/20261101-150006`

| Rate | uvicorn off | uvicorn on | granian off | granian on |
| --- | --- | --- | --- | --- |
| 2k | 1,997 (0.7 / 1.4) | 1,999 (1.1 / 12.4)* | 1,998 (0.8 / 2.1) | 1,999 (1.4 / 10.2) |
| 5k | 4,996 (0.8 / 2.8) | 4,997 (7.4 / 889) | 4,996 (0.9 / 8.4) | 4,957 (7.1 / 635) |
| 8k | 7,994 (1.1 / 10.1) | 5,852 (2,745 / 5,290) | 7,993 (1.2 / 26.7) | 6,026 (2,707 / 4,851) |
| 10k | 9,986 (1.4 / 23.1) | 5,833 (4,081 / 8,238) | 9,993 (1.8 / 31.2) | 5,915 (4,298 / 8,089) |
| 15k | 13,386 (962 / 2,113) | 5,851 (6,157 / 12,060) | 14,989 (8.8 / 87.6) | 6,029 (6,096 / 11,846) |

\* Cold pools after restart; re-run: 1,999 (1.1 / 6.0), +0.4 / +4.5ms over meter off (`-151141`).

- Meter on cap ~5.1k → ~5.9–6.0k (+15%). At 5k p50 99 → 7ms; p99 still past the knee.
- Meter off: uvicorn holds 10k (p99 370 → 23ms), granian holds 15k.

### 3. Reverted: per-call deadline, direct EVALSHA

One `asyncio.timeout` per store call instead of redis-py `socket_timeout`, EVALSHA without `Script`, no JSON parse for empty bodies. Profile predicted 5–8%. 15k, 3 runs (`-154448`, `-154751`, `-155051`):

| | run 1 | run 2 | run 3 | median | iter 2 |
| --- | --- | --- | --- | --- | --- |
| uvicorn off | 12,796 | 13,466 | 12,695 | 12,796 | 13,386 |
| uvicorn on | 4,844 | 5,951 | 5,750 | 5,750 | 5,851 |
| granian off | 14,974 | 14,977 | 14,989 | 14,977 | 14,989 |
| granian on | 5,805 | 5,599 | 5,655 | 5,655 | 6,029 |

Run-to-run noise (~20% meter on) exceeds expected gain; not worth changing the core Redis path.

## Profile: meter CPU (after 2)

py-spy, one uvicorn replica, 30s, 6 replicas at 5k req/s (~833/replica), meter on vs `--no-meter`. Meter on: 1,108µs CPU/req (~92% busy); off: 459µs (~38%). Meter adds ~650µs:

| Part | µs/req | Notes |
| --- | --- | --- |
| redis-py client | ~200 | connection, `Script`, parser; ~22 telemetry hooks (run even when off) |
| Redis socket writes | ~140 | ~50µs/send via Docker VM networking; reserve + commit = 2 sends |
| asyncio timeouts, locks | ~75 | `socket_timeout` = 4 timeouts/req |
| Our code | ~80 | store 19, meter 18, fingerprint 17, urllib 13, uuid 7, json 7 |
| Rest | ~150 | uvicorn/FastAPI slower on saturated core |

Redis spends ~32µs per `evalsha`; cost is the Python client. 2 sends (~140µs) stay while reserve/commit are separate scripts.

## Isolated machines (2026-11-01)

Two DigitalOcean droplets on one private network, nothing else on either:

- Stack: 16 vCPU AMD EPYC 9555P, 32 GB, Ubuntu 24.04, Docker 29.9.2; 8 API replicas, nginx, Redis as above
- Load generator: 8 vCPU, 16 GB, same CPU; oha over `docker -H ssh://`

`API_REPLICAS=8 scripts/bench_remote.sh root@10.132.0.3 10.132.0.2 overhead -q 2000 4000 6000 8000 10000 15000`, same oha flags as the rate sweeps. Run 3 times each for `main` (d2c3785) and `perf/commit-loop` (66e141c), interleaved: `bench-results/20261101-131611`, `-134310`, `-141004` (main), `-132944`, `-135636`, `-142330` (commit loop).

Every rate was held (successful req/s within 0.1% of target), with 0 non-200s and 0 errors across all 144 runs. The ~5–6k meter-on cap above was the workstation, not the stack.

### Meter overhead (`main`, median of 3)

Cells: p50 / p99 ms.

| Rate | uvicorn off | uvicorn on | granian off | granian on |
| --- | --- | --- | --- | --- |
| 2k | 0.3 / 0.6 | 0.5 / 1.0 | 0.3 / 0.6 | 0.6 / 3.6 |
| 4k | 0.3 / 0.6 | 0.5 / 1.2 | 0.4 / 0.6 | 0.7 / 1.3 |
| 6k | 0.4 / 0.7 | 0.6 / 1.6 | 0.4 / 0.7 | 0.8 / 10.7 |
| 8k | 0.4 / 0.8 | 0.7 / 2.1 | 0.4 / 0.8 | 0.8 / 1.8 |
| 10k | 0.4 / 0.9 | 0.8 / 2.1 | 0.5 / 1.0 | 0.9 / 2.2 |
| 15k | 0.5 / 1.2 | 1.0 / 6.2 | 0.6 / 1.2 | 1.2 / 85.5 |

- uvicorn: meter adds 0.2–0.4ms p50 and 0.4–1.2ms p99 up to 10k, +5ms p99 at 15k. Inside the <10ms budget at every rate.
- granian on, 15k: p99 swings 8.9–175ms between runs; it's at its knee there.
- granian on, 6k: p99 ~10ms in all 6 runs, both branches, but not at 4k or 8k, and not with the meter off. Reproducible, not investigated.

### Commit loop A/B (meter on, p99 ms, median of 3)

| Rate | uvicorn main | uvicorn loop | granian main | granian loop |
| --- | --- | --- | --- | --- |
| 2k | 1.0 | 1.0 | 3.6 | 3.1 |
| 4k | 1.2 | 1.2 | 1.3 | 1.3 |
| 6k | 1.6 | 1.6 | 10.7 | 9.5 |
| 8k | 2.1 | 1.9 | 1.8 | 1.7 |
| 10k | 2.1 | 2.0 | 2.2 | 2.1 |
| 15k | 6.2 (3.8–8.7) | 7.4 (4.6–63.2) | 85.5 (8.9–175) | 14.2 (11.4–112) |

p50 within 0.03ms at every rate. Up to 10k, uvicorn p99 is within 0.2ms; granian's 2k and 6k are 0.5 and 1.2ms lower with the loop, inside their run-to-run spread (6k: 10.6–10.8 vs 9.3–10.6). At 15k the spread is larger than any difference. No measurable gain here: the stack isn't CPU-bound at these rates, and the loop saves client CPU, which only shows under saturation (+2% throughput on the workstation).
