# Hub load & pprof

Go is installed at `~/.local/go` (`go1.25.0`).

```bash
export PATH="$HOME/.local/go/bin:$PATH"

# Burst load (finds max enqueue vs drop)
go run ./cmd/loadtest -rooms=100 -users=20 -producers=32 -duration=8s \
  -cpuprofile=profiles/cpu_load.prof -memprofile=profiles/mem_load.prof

# Paced load (0% drop, better CPU profile of real work)
go run ./cmd/loadtest -rooms=100 -users=20 -producers=1 -rate=200000 -duration=5s \
  -cpuprofile=profiles/cpu_rate.prof -memprofile=profiles/mem_rate.prof

# Benchmarks
go test ./internal/ws/ -bench 'BenchmarkHub|BenchmarkEnsure' -benchtime=1s -count=1

# Soak
go test ./internal/ws/ -run TestLoadHubSustained -v -count=1

# Inspect
go tool pprof -top -focus='server/internal/ws' profiles/cpu_rate.prof
go tool pprof -http=:8081 profiles/cpu_rate.prof

# Live server pprof
ENABLE_PPROF=1 ./app
# go tool pprof http://localhost:PORT/debug/pprof/profile?seconds=20
```

Race detector needs `gcc` (`CGO_ENABLED=1`).
