package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"sort"
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

func launchWorker(c *client.Client, w workload, timeout time.Duration,
	start chan struct{}, resch chan result) {

	<-start

	call := make(chan client.Response, len(w.enqs))
	reqStart := make(map[server.Request]time.Time)
	reqEnd := make(map[server.Request]time.Time)

	var minStart time.Time
	done := make(chan struct{})
	go func() {
		for i, e := range w.enqs {
			now := time.Now()
			if i == 0 {
				minStart = now
			}
			req, _ := c.DoEnQ(string(e.Queue), e.Body, call)
			reqStart[req] = now
		}
		close(done)
	}()

	res := result{
		// TODO(soheil): implement deques as well.
		size: uint64(len(w.enqs)), //+ len(w.deqs),
	}
	var to <-chan time.Time
	if timeout != 0 {
		to = time.After(timeout)
	}

loop:
	for i := 0; i < len(w.enqs); i++ {
		select {
		case response := <-call:
			if response.Error != nil {
				continue
			}
			reqEnd[response.Request] = time.Now()
			res.reqs++
		case <-to:
			break loop
		}
	}

	<-done

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

func run(clients []*client.Client, workers, queues, tasks, size int,
	timeout time.Duration, shared, enq bool) []result {

	workloads := genWorkload(workers, queues, tasks, size, shared, enq)
	startc := make(chan struct{})
	resc := make(chan result)
	for w := 0; w < workers; w++ {
		go launchWorker(clients[w], workloads[w], timeout, startc, resc)
	}
	close(startc)

	res := make([]result, workers)
	for w := 0; w < workers; w++ {
		res[w] = <-resc
	}
	return res
}

func warmup(clients []*client.Client, workers, queues, size int,
	shared, enq bool) []result {

	return run(clients, workers, queues, 1, size, 0, shared, enq)
}

func metrics(res []result) (dur, reqs, tput uint64, lats []uint64) {
	var start time.Time
	var end time.Time
	for w := 0; w < *workers; w++ {
		if end.IsZero() || res[w].end.After(end) {
			end = res[w].end
		}
		if start.IsZero() || res[w].start.Before(start) {
			start = res[w].start
		}
		lats = append(lats, uint64(res[w].end.Sub(res[w].start)))
		reqs += res[w].reqs
	}
	dur = uint64(end.Sub(start))
	tput = reqs * 1e9 / dur
	return dur, reqs, tput, lats
}

type byval []uint64

func (s byval) Len() int           { return len(s) }
func (s byval) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byval) Less(i, j int) bool { return s[i] < s[j] }

func avg(s []uint64) (a uint64) {
	if len(s) == 0 {
		return 0
	}

	for _, i := range s {
		a += i
	}
	return a / uint64(len(s))
}

func prc(s []uint64, p int) (n uint64) {
	if p > 100 {
		p = 100
	}
	return s[len(s)*p/100-1]
}

func converged(prevt, currt uint64) bool {
	if prevt == 0 {
		return false
	}

	return prevt > currt || float64(prevt-currt)/float64(prevt) < 0.1
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

	fmt.Printf("warming up... ")
	warmup(clients, *workers, *queues, *size, *shared, *enqOnly)
	fmt.Printf("[ok]\n")

	n := 2
	var lastTput uint64
	for {
		fmt.Printf("trying %d ", n)
		res := run(clients, *workers, *queues, n, *size, 30*time.Second, *shared,
			*enqOnly)
		_, _, tput, _ := metrics(res)
		if converged(lastTput, tput) {
			fmt.Println("[no]")
			break
		}

		fmt.Println("[ok]")
		n *= 2
		lastTput = tput
	}

	n /= 2
	var tputs []uint64
	var lats []uint64
	for i := 0; i < 16; i++ {
		res := run(clients, *workers, *queues, n, *size, 30*time.Second, *shared,
			*enqOnly)
		_, _, tput, lat := metrics(res)
		tputs = append(tputs, tput)
		lats = append(lats, lat...)
	}

	sort.Sort(byval(tputs))
	sort.Sort(byval(lats))

	fmt.Printf("throughput avg=%v median=%v max=%v min=%v\n", avg(tputs),
		prc(tputs, 50), tputs[len(tputs)-1], tputs[0])
	fmt.Printf("latency avg=%v median=%v max=%v min=%v\n",
		time.Duration(avg(lats)), time.Duration(prc(lats, 50)),
		time.Duration(lats[len(lats)-1]), time.Duration(lats[0]))
}
