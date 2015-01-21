// Benchmarks for the key value store.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	keyLen     = 16
	minBodyLen = 16
	maxBodyLen = 512
)

var client *http.Client

type target struct {
	method string
	url    string
	body   []byte
}

func newTarget(method, url string, body []byte) target {
	return target{
		method: method,
		url:    url,
		body:   body,
	}
}

func (t *target) do() (start, end, in, out int64, err error) {
	req, err := http.NewRequest(t.method, t.url, bytes.NewBuffer(t.body))
	if err != nil {
		return 0, 0, 0, 0, err
	}
	in = req.ContentLength
	start = time.Now().UnixNano()
	res, err := client.Do(req)
	end = time.Now().UnixNano()
	if err != nil {
		return
	}
	defer func() {
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()
	}()
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	out = int64(len(b))
	return
}

type result struct {
	Method string        `json:"method"`
	URL    string        `json:"url"`
	In     int64         `json:"in"`
	Out    int64         `json:"out"`
	Start  int64         `json:"start"`
	End    int64         `json:"end"`
	Dur    time.Duration `json:"dur"`
	Err    string        `json:"err"`
}

func (r result) csv() []string {
	return []string{
		r.Method,
		r.URL,
		strconv.FormatInt(r.In, 10),
		strconv.FormatInt(r.Out, 10),
		strconv.FormatInt(r.Start, 10),
		strconv.FormatInt(r.End, 10),
		strconv.FormatInt(r.Dur.Nanoseconds(), 10),
		r.Err,
	}
}

var (
	csvHeader = []string{
		"method", "url", "in", "out", "start", "end", "dur", "err",
	}
)

func writeCSVLine(w *bufio.Writer, fields []string) {
	for i, f := range fields {
		w.WriteString(f)
		if i != len(fields)-1 {
			w.WriteString(",")
		}
	}
	w.WriteString("\n")
}

func randRange(from, to int) int {
	return rand.Intn(to-from) + from
}

func randChar() byte {
	a := byte('a')
	z := byte('z')
	return byte(randRange(int(a), int(z)))
}

func randString(length int) string {
	b := make([]byte, length)
	for i := 0; i < length; i++ {
		b[i] = randChar()
	}
	return string(b)
}

func generateTargets(addr string, writes, localReads,
	randReads int) []target {

	rand.Seed(time.Now().UTC().UnixNano())
	var targets []target
	var keys []string
	for w := 0; w < writes; w++ {
		k := randString(keyLen)
		keys = append(keys, k)
		bl := randRange(minBodyLen, maxBodyLen)
		t := newTarget("PUT", "http://"+addr+"/apps/kvstore/"+k,
			[]byte(randString(bl)))
		targets = append(targets, t)
		for r := 0; r < localReads/writes+1; r++ {
			t := newTarget("GET", "http://"+addr+"/apps/kvstore/"+k, []byte{})
			targets = append(targets, t)
		}
	}

	for r := 0; r < randReads; r++ {
		t := newTarget("GET",
			"http://"+addr+"/apps/kvstore/"+randString(keyLen), []byte{})
		targets = append(targets, t)
	}

	return targets
}

func run(id int, targets []target, rounds int, ch chan<- []result) {
	for i := 0; i < rounds; i++ {
		results := make([]result, 0, len(targets))
		for _, t := range targets {
			var err error
			res := result{
				Method: t.method,
				URL:    t.url,
			}
			res.Start, res.End, res.In, res.Out, err = t.do()
			res.Dur = time.Duration(res.End - res.Start)
			if err != nil {
				res.Err = err.Error()
			}
			results = append(results, res)
		}

		if ch != nil {
			ch <- results
		}
	}
}

func mustSave(results []result, w io.Writer) {
	b, err := json.Marshal(results)
	if err != nil {
		panic(err)
	}
	w.Write(b)
}

var (
	addr    = flag.String("addr", "localhost:7767", "server address")
	writes  = flag.Int("writes", 10, "number of random keys to writes per round")
	localr  = flag.Int("localreads", 100, "number of reads from written keys")
	randr   = flag.Int("randomreads", 0, "number of random keys to read")
	rounds  = flag.Int("rounds", 1, "number of rounds")
	workers = flag.Int("workers", 1, "number of parallel clients")
	timeout = flag.Duration("timeout", 60*time.Second, "request timeout")
	output  = flag.String("out", "bench.out", "benchmark output file")
)

func main() {
	flag.Parse()

	f, err := os.Create(*output)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	client = &http.Client{Timeout: *timeout}

	targets := generateTargets(*addr, *writes, *localr, *randr)
	fmt.Print("\nwarming up workers\n")
	writeCSVLine(w, csvHeader)
	run(0, targets, 1, nil)

	fmt.Printf("starting...\n")
	var wg sync.WaitGroup
	wg.Add(*workers)
	ch := make(chan []result, *workers)
	for i := 0; i < *workers; i++ {
		go func(i int) {
			run(i, targets, *rounds, ch)
			wg.Done()
		}(i)
	}

	writeDone := make(chan struct{})
	go func() {
		for results := range ch {
			for _, r := range results {
				writeCSVLine(w, r.csv())
			}
		}
		writeDone <- struct{}{}
	}()

	wg.Wait()
	close(ch)
	<-writeDone

	w.Flush()
	f.Close()
}
