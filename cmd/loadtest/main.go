package main

import (
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"
)

func main() {
	url := flag.String("url", "http://localhost:8080/", "target URL")
	n := flag.Int("n", 30, "total number of requests to send")
	c := flag.Int("c", 15, "number of concurrent workers")
	flag.Parse()

	jobs := make(chan int, *n)
	results := make(chan int, *n) // HTTP status codes (0 = request error)

	var wg sync.WaitGroup
	for w := 0; w < *c; w++ {
		wg.Add(1)
		go worker(*url, jobs, results, &wg)
	}

	for i := 0; i < *n; i++ {
		jobs <- i
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	counts := map[int]int{}
	for status := range results {
		counts[status]++
	}

	fmt.Printf("sent=%d allowed(200)=%d denied(429)=%d other=%d\n",
		*n, counts[http.StatusOK], counts[http.StatusTooManyRequests],
		*n-counts[http.StatusOK]-counts[http.StatusTooManyRequests])
}

func worker(url string, jobs <-chan int, results chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	client := &http.Client{Timeout: 2 * time.Second}

	for range jobs {
		resp, err := client.Get(url)
		if err != nil {
			results <- 0
			continue
		}
		resp.Body.Close()
		results <- resp.StatusCode
	}
}
