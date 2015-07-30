# taskq: A distributed task queue in Go
This is a simple distributed task queue using which you can:
* enqueue a task into a named queue.
* dequeue a task from a named queue.
* acknowledge a task in a named queue.

Tasks are either in "active" or "dequeued" state. `taskq` dequeue tasks
from the list of active tasks, return them to the user, and put them in
the dequeued list. If a dequeued task is not acknowledged after a
timetout (2-3 min), the task will be put back  in the active queue.

## Desgin docs
TODO: add the medium link when its public.

## How to install
`taskq` (well, technically Beehive) requires Go 1.4+. Once you have
installed Go and have setup your GOPATH, run the following command:

```bash
$ go get github.com/kandoo/beehive/examples/taskq
```

## Running the first node 
You can run the first node using:
```bash
taskq -logtostderr -addr ADDR1 -statepath STATE1
```

`ADDR1` is the address of the first node in the form of `IP:PORT`.
`STATE1` is the directory in which the first node stores its state.

## Running a cluster
You can run new nodes to join the cluster using:
```bash
taskq -logtostderr -addr ADDRN -paddrs ADDR1 -statepath STATEN
```

`ADDRN` is the address of the new node in the form of `IP:PORT`.
`ADDR1` is the address of the first node.
`STATEN` is the directory in which the N'th node stores its state.

