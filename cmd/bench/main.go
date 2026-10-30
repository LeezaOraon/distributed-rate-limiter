// Command bench drives load against one or more server instances and reports
// throughput, latency percentiles and a status-code breakdown.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	status int
	d      time.Duration
}

func main() {
	urls := flag.String("urls", "http://localhost:8080/", "comma-separated target URLs, used round-robin")
	n := flag.Int("n", 10000, "total requests (ignored if -duration is set)")
	dur := flag.Duration("duration", 0, "run for this long instead of a fixed request count")
	c := flag.Int("c", 100, "concurrent workers")
	flag.Parse()

	targets := strings.Split(*urls, ",")
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *c * 2,
			MaxIdleConnsPerHost: *c * 2,
		},
	}

	var next atomic.Int64
	deadline := time.Now().Add(*dur)
	results := make([][]result, *c)

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *c; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				i := next.Add(1) - 1
				if *dur > 0 {
					if time.Now().After(deadline) {
						return
					}
				} else if i >= int64(*n) {
					return
				}
				t0 := time.Now()
				resp, err := client.Get(targets[i%int64(len(targets))])
				status := 0
				if err == nil {
					// drain so the connection is reused rather than redialed
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					status = resp.StatusCode
				}
				results[w] = append(results[w], result{status, time.Since(t0)})
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	counts := map[int]int{}
	var lat []time.Duration
	for _, rs := range results {
		for _, r := range rs {
			counts[r.status]++
			lat = append(lat, r.d)
		}
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration { return lat[int(float64(len(lat)-1)*p)] }

	fmt.Printf("requests=%d workers=%d instances=%d elapsed=%s\n", len(lat), *c, len(targets), elapsed.Round(time.Millisecond))
	fmt.Printf("throughput=%.0f req/s\n", float64(len(lat))/elapsed.Seconds())
	fmt.Printf("latency p50=%s p90=%s p99=%s p99.9=%s max=%s\n", pct(.5), pct(.9), pct(.99), pct(.999), lat[len(lat)-1])
	fmt.Printf("allowed(200)=%d denied(429)=%d unavailable(503)=%d errors(0)=%d other=%d\n",
		counts[200], counts[429], counts[503], counts[0],
		len(lat)-counts[200]-counts[429]-counts[503]-counts[0])
}
