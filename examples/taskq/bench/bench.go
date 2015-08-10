package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/golang/glog"
	"github.com/kandoo/beehive/examples/taskq/client"
	"github.com/kandoo/beehive/examples/taskq/server"
)

var (
	servers = flag.String("s", "127.0.0.1:7979",
		"comma-seperated list of servers")
	workers = flag.Int("w", 1,
		"number of workers (equally spread among servers)")
	latency = flag.Bool("l", false,
		"whether to do latency test instead of througput")
	queues = flag.Int("q", 1, "number of named queues to use")
	shared = flag.Bool("shared", false,
		"whether to share the queues for all workers")
	enqOnly    = flag.Bool("e", false, "whether to only enque tasks")
	size       = flag.Int("b", 1024, "task size")
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
)

func connect(servers ...string) (clients []*client.Client, err error) {
	for _, s := range servers {
		c, err := client.New(s)
		if err != nil {
			return nil, err
		}
		clients = append(clients, c)
	}
	return
}

type workload struct {
	enqs []server.Task
	deqs []server.Queue
}

type result struct {
	start   time.Time
	end     time.Time
	size    uint64
	reqs    uint64
	enqDurs []time.Duration
	deqDurs []time.Duration
	ackDurs []time.Duration
	errors  []error
}

func genWorkload(workers, queues, tasks, size int, shared, enq bool) (
	workloads []workload) {

	b := make([]byte, size)
	for i := 0; i < size; i++ {
		b[i] = 'b'
	}

	q := 0
	for w := 0; w < workers; w++ {
		var wl workload
		if shared {
			q = 0
		}
		for t := 0; t < tasks; t++ {
			if shared {
				q = (q + t) % queues
			}
			task := server.Task{
				Queue: server.Queue(fmt.Sprintf("benchq-%v", q)),
				Body:  b,
			}
			wl.enqs = append(wl.enqs, task)
			if !enq {
				wl.deqs = append(wl.deqs, task.Queue)
			}
		}
		if !shared {
			q = (q + 1) % queues
		}
		workloads = append(workloads, wl)
	}

	return
}

func closeClients(clients []*client.Client) {
	for _, c := range clients {
		c.Close()
	}
}

func launch(c *client.Client, w workload, start chan struct{},
	resch chan result) {

	<-start

	call := make(chan client.Response, len(w.enqs))
	reqStart := make(map[server.Request]time.Time)
	reqEnd := make(map[server.Request]time.Time)

	var minStart time.Time
	go func() {
		for i, e := range w.enqs {
			now := time.Now()
			if i == 0 {
				minStart = now
			}
			req, _ := c.DoEnQ(string(e.Queue), e.Body, call)
			reqStart[req] = now
		}
	}()

	res := result{
		reqs: uint64(len(w.enqs)),
	}
	for _ = range w.enqs {
		response := <-call
		if response.Error != nil {
			res.reqs--
			continue
		}
		reqEnd[response.Request] = time.Now()
	}

	var maxEnd time.Time
	for req, end := range reqEnd {
		if maxEnd.Before(end) {
			maxEnd = end
		}
		d := end.Sub(reqStart[req])
		res.enqDurs = append(res.enqDurs, d)
	}

	res.start = minStart
	res.end = maxEnd

	resch <- res
}

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			glog.Error("cannot register cpu profile: %v", err)
			os.Exit(-1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var clients []*client.Client
	servers := strings.Split(*servers, ",")
	for w := 0; w < *workers; w++ {
		c, err := client.New(servers[w%len(servers)])
		if err != nil {
			glog.Fatalf("cannot create client: %v", err)
		}
		clients = append(clients, c)
	}
	defer closeClients(clients)

	n := 1
	var lastTput uint64
	for {
		workloads := genWorkload(*workers, *queues, n, *size, *shared, *enqOnly)
		startch := make(chan struct{})
		resch := make(chan result)
		for w := 0; w < *workers; w++ {
			go launch(clients[w], workloads[w], startch, resch)
		}
		close(startch)

		var reqs uint64
		var start time.Time
		var end time.Time
		res := make([]result, *workers)
		for w := 0; w < *workers; w++ {
			res[w] = <-resch
			if end.IsZero() || res[w].end.After(end) {
				end = res[w].end
			}
			if start.IsZero() || res[w].start.Before(start) {
				start = res[w].start
			}
			reqs += res[w].reqs
		}

		dur := uint64(end.Sub(start))
		tput := reqs * 1e9 / dur
		ratio := math.Abs(float64(tput-lastTput) / float64(lastTput))

		if lastTput != 0 && ratio <= 0.1 {
			break
		}
		break
		n *= 2
		fmt.Println(n, tput-lastTput, dur, reqs, tput)
		lastTput = tput
	}

	fmt.Println(lastTput)
}
