# Benchmarks

What metering costs per request, and what the stack sustains end to end. Numbers below were measured on 2026-10-31.

Target from [DESIGN.md](../DESIGN.md): ~2,000 quota operations/second sustained, peaking at 10,000/second, with <10 ms metering overhead per request.

## Environment

Every run shares the same machine, load generator included, so throughput numbers are a floor for this PC, not a property of the design.

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