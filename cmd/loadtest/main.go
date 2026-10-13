// Proper Hub capacity test: stepped RPS, latency percentiles, pass/fail SLOs.
//
//	go run ./cmd/loadtest -mode=capacity
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"server/internal/ws"
)

type stepResult struct {
	TargetRPS   int
	Duration    time.Duration
	Enqueued    int64
	Dropped     int64
	Delivered   int64
	DropRate    float64
	AchievedRPS float64
	P50         time.Duration
	P95         time.Duration
	P99         time.Duration
	P999        time.Duration
	Pass        bool
	Reason      string
}

// metricsConn records delivery latency from Message.CreatedAt set at enqueue time.
type metricsConn struct {
	m *metrics
}

type metrics struct {
	delivered atomic.Int64
	lat       []int64
	idx       atomic.Uint64
}

func newMetrics(cap int) *metrics {
	return &metrics{lat: make([]int64, cap)}
}

func (m *metrics) reset() {
	m.delivered.Store(0)
	m.idx.Store(0)
}

func (m *metrics) conn() ws.ConnWriter { return &metricsConn{m: m} }

func (c *metricsConn) record(createdAt time.Time) {
	if createdAt.IsZero() {
		return
	}
	lat := time.Since(createdAt).Nanoseconds()
	if lat < 0 {
		lat = 0
	}
	i := c.m.idx.Add(1) - 1
	if i < uint64(len(c.m.lat)) {
		c.m.lat[i] = lat
	}
}

func (c *metricsConn) WriteJSON(v interface{}) error {
	c.m.delivered.Add(1)
	switch msg := v.(type) {
	case map[string]interface{}:
		if content, ok := msg["content"].(*ws.Message); ok && content != nil {
			c.record(content.CreatedAt)
		}
	default:
		// roomMessageOut lives in package ws — match via JSON round-trip only on raw path.
	}
	return nil
}

func (c *metricsConn) WriteRawJSON(data []byte) error {
	c.m.delivered.Add(1)
	c.record(createdAtFromRoomJSON(data))
	return nil
}

// createdAtFromRoomJSON pulls Message.CreatedAt without full JSON unmarshal.
func createdAtFromRoomJSON(data []byte) time.Time {
	const key = `"CreatedAt":"`
	i := bytes.Index(data, []byte(key))
	if i < 0 {
		return time.Time{}
	}
	rest := data[i+len(key):]
	j := bytes.IndexByte(rest, '"')
	if j <= 0 {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, string(rest[:j]))
	if err != nil {
		t, err = time.Parse(time.RFC3339, string(rest[:j]))
		if err != nil {
			return time.Time{}
		}
	}
	return t
}

func (c *metricsConn) Close() error                      { return nil }
func (c *metricsConn) ReadMessage() (int, []byte, error) { return 0, nil, nil }

func (m *metrics) percentiles() (p50, p95, p99, p999 time.Duration) {
	n := int(m.idx.Load())
	if n == 0 {
		return 0, 0, 0, 0
	}
	if n > len(m.lat) {
		n = len(m.lat)
	}
	samples := append([]int64(nil), m.lat[:n]...)
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	pct := func(p float64) time.Duration {
		i := int(math.Ceil(p*float64(len(samples)))) - 1
		if i < 0 {
			i = 0
		}
		if i >= len(samples) {
			i = len(samples) - 1
		}
		return time.Duration(samples[i])
	}
	return pct(0.50), pct(0.95), pct(0.99), pct(0.999)
}

func main() {
	mode := flag.String("mode", "capacity", "capacity | burst")
	rooms := flag.Int("rooms", 100, "number of rooms")
	users := flag.Int("users", 20, "users per room")
	stepDur := flag.Duration("step", 5*time.Second, "duration per RPS step")
	maxDrop := flag.Float64("max-drop", 0.1, "max enqueue drop percent to pass")
	maxP99 := flag.Duration("max-p99", 100*time.Millisecond, "max p99 delivery latency to pass")
	cpuProfile := flag.String("cpuprofile", "", "write CPU profile")
	memProfile := flag.String("memprofile", "", "write heap profile")
	flag.Parse()

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	met := newMetrics(2_000_000)
	h := ws.NewLoadHub()
	totalClients := ws.SeedLoadHubWithConn(h, *rooms, *users, func(string, string) ws.ConnWriter {
		return met.conn()
	})

	fmt.Printf("=== Hub capacity test (realistic) ===\n")
	fmt.Printf("rooms=%d users/room=%d clients=%d GOMAXPROCS=%d\n",
		*rooms, *users, totalClients, runtime.GOMAXPROCS(0))
	fmt.Printf("SLO: drop <= %.2f%%  AND  p99 <= %s\n", *maxDrop, *maxP99)
	fmt.Printf("Note: in-process Hub only (no Mongo/Redis/real WS). Measures fan-out capacity.\n\n")

	var results []stepResult
	if *mode == "burst" {
		results = append(results, runStep(h, met, *rooms, 0, 16, *stepDur, *maxDrop, *maxP99))
		printStep(results[0])
	} else {
		steps := []int{1_000, 5_000, 10_000, 25_000, 50_000, 75_000, 100_000, 150_000, 200_000, 250_000, 300_000}
		maxPass := 0
		for _, rps := range steps {
			fmt.Printf("--- target %d msg/s for %s ---\n", rps, *stepDur)
			res := runStep(h, met, *rooms, rps, 8, *stepDur, *maxDrop, *maxP99)
			printStep(res)
			results = append(results, res)
			if res.Pass {
				maxPass = rps
			} else {
				fmt.Printf("ramp stopped at %d msg/s: %s\n", rps, res.Reason)
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
		fmt.Printf("\nMax sustainable RPS under SLO: %d\n", maxPass)
	}

	printVerdict(results)

	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		runtime.GC()
		_ = pprof.WriteHeapProfile(f)
		log.Printf("wrote %s", *memProfile)
	}
}

func runStep(
	h *ws.Hub,
	met *metrics,
	rooms, targetRPS, producers int,
	duration time.Duration,
	maxDrop float64,
	maxP99 time.Duration,
) stepResult {
	if producers < 1 {
		producers = 1
	}
	met.reset()
	h.ResetDroppedBroadcasts()

	var enqueued atomic.Int64
	var producerDrop atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	type job struct {
		room int
		seq  int64
	}
	jobs := make(chan job, 16384)

	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				now := time.Now()
				ok := h.EnqueueBroadcast(&ws.Message{
					UniqId:    fmt.Sprintf("cap-%d-%d", j.room, j.seq),
					RoomID:    fmt.Sprintf("room-%d", j.room),
					ClientID:  fmt.Sprintf("u-%d-0", j.room),
					Content:   "capacity-payload",
					Type:      "text",
					CreatedAt: now,
				})
				if ok {
					enqueued.Add(1)
				} else {
					producerDrop.Add(1)
				}
			}
		}()
	}

	start := time.Now()
	var seq int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(jobs)
		room := 0
		if targetRPS <= 0 {
			for {
				select {
				case <-stop:
					return
				default:
					seq++
					select {
					case jobs <- job{room: room % rooms, seq: seq}:
						room++
					case <-stop:
						return
					}
				}
			}
		}
		// Batch pacer: every 10ms emit targetRPS/100 jobs (accurate under load).
		const tick = 10 * time.Millisecond
		batch := targetRPS / int(time.Second/tick)
		if batch < 1 {
			batch = 1
		}
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				for i := 0; i < batch; i++ {
					seq++
					select {
					case jobs <- job{room: room % rooms, seq: seq}:
						room++
					case <-stop:
						return
					default:
						producerDrop.Add(1)
					}
				}
			}
		}
	}()

	time.Sleep(duration)
	close(stop)
	wg.Wait()
	ws.WaitBroadcastDrain(h, 3*time.Second)
	time.Sleep(150 * time.Millisecond)
	elapsed := time.Since(start)

	eq := enqueued.Load()
	drop := producerDrop.Load() + int64(h.DroppedBroadcasts())
	del := met.delivered.Load()
	dropRate := 100 * float64(drop) / float64(eq+drop+1)
	achieved := float64(eq) / elapsed.Seconds()
	p50, p95, p99, p999 := met.percentiles()

	// Must actually reach ~90% of target RPS, otherwise the step did not test that load.
	reach := 100.0
	if targetRPS > 0 {
		reach = 100 * achieved / float64(targetRPS)
	}
	pass := eq > 0 && dropRate <= maxDrop && p99 <= maxP99 && (targetRPS <= 0 || reach >= 90)
	reason := "ok"
	switch {
	case eq == 0:
		pass = false
		reason = "no messages enqueued"
	case targetRPS > 0 && reach < 90:
		pass = false
		reason = fmt.Sprintf("achieved only %.0f%% of target (pacer/hub bottleneck)", reach)
	case dropRate > maxDrop:
		pass = false
		reason = fmt.Sprintf("drop %.3f%% > %.3f%%", dropRate, maxDrop)
	case p99 > maxP99:
		pass = false
		reason = fmt.Sprintf("p99 %s > %s", p99, maxP99)
	}

	return stepResult{
		TargetRPS:   targetRPS,
		Duration:    elapsed,
		Enqueued:    eq,
		Dropped:     drop,
		Delivered:   del,
		DropRate:    dropRate,
		AchievedRPS: achieved,
		P50:         p50,
		P95:         p95,
		P99:         p99,
		P999:        p999,
		Pass:        pass,
		Reason:      reason,
	}
}

func printStep(r stepResult) {
	target := "burst"
	if r.TargetRPS > 0 {
		target = fmt.Sprintf("%d/s", r.TargetRPS)
	}
	status := "PASS"
	if !r.Pass {
		status = "FAIL"
	}
	fmt.Printf("[%s] target=%-8s achieved=%7.0f/s  drop=%6.3f%%  delivered_writes=%d\n",
		status, target, r.AchievedRPS, r.DropRate, r.Delivered)
	fmt.Printf("       latency  p50=%-10s p95=%-10s p99=%-10s p999=%-10s  (%s)\n",
		r.P50, r.P95, r.P99, r.P999, r.Reason)
}

func printVerdict(results []stepResult) {
	fmt.Printf("\n======== FINAL REPORT ========\n")
	allPass := true
	maxPass := 0
	for _, r := range results {
		t := "burst"
		if r.TargetRPS > 0 {
			t = fmt.Sprintf("%d/s", r.TargetRPS)
		}
		st := "PASS"
		if !r.Pass {
			st = "FAIL"
			allPass = false
		} else if r.TargetRPS > maxPass {
			maxPass = r.TargetRPS
		}
		fmt.Printf("%-8s %s  achieved=%7.0f/s  drop=%6.3f%%  p50=%s  p99=%s\n",
			t, st, r.AchievedRPS, r.DropRate, r.P50, r.P99)
	}
	fmt.Printf("------------------------------\n")
	if len(results) == 1 && results[0].TargetRPS == 0 {
		if results[0].Pass {
			fmt.Println("VERDICT: burst PASS (unexpected — check SLO)")
		} else {
			fmt.Println("VERDICT: burst FAIL — unlimited producers are not a valid service SLO test.")
		}
		return
	}
	fmt.Printf("Max RPS under SLO: %d msg/s\n", maxPass)
	if allPass {
		fmt.Println("VERDICT: all steps PASS")
	} else if maxPass == 0 {
		fmt.Println("VERDICT: FAIL — cannot sustain even the lowest step under SLO")
	} else {
		fmt.Printf("VERDICT: PARTIAL — healthy up to %d msg/s, fails above that\n", maxPass)
	}
}
