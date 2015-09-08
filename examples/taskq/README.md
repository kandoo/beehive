# TaskQ: A distributed task queue in Go
This is a simple distributed task queue using which you can:
* enqueue a task into a named queue.
* dequeue a task from a named queue.
* acknowledge a task in a named queue.

Tasks are either in "active" or "dequeued" state. TaskQ dequeue tasks
from the list of active tasks, return them to the user, and put them in
the dequeued list. If a dequeued task is not acknowledged after a
timetout (2-3 min), the task will be put back  in the active queue.

## Installation
**Option 1.** Install TaskQ using [goget](https://github.com/soheilhy/goget):

```bash
curl -sL https://git.io/goget | bash -s -- github.com/kandoo/beehive/examples/taskq
```

**Option 2.** Install Go 1.4+ on your machine, setup your Go workspace
(i.e., the `$GOPATH`), and go get TaskQ:

```bash
go get github.com/kandoo/beehive/examples/taskq
```

## Standalone Mode
To run TaskQ in standalone mode with no replication, run the following command:

```bash
taskq -logtostderr -taskq.repl=1
```

This command runs a TaskQ server with a *replication factor* of 1, which
basically means saving data on the local machine with no replication on any
other nodes. You can send requests to TaskQ either using HTTP or its own
text-based protocol. By default, HTTP is served on localhost:7677, TaskQ
protocol is served on port 7979, and TaskQ's data is saved in `/tmp/beehive`.
You can override these addresses using `-addr`, `-taskq.addr`, and `-statepath`:
```bash
taskq -logtostderr -addr=ADDR -taskq.addr=TADDR -statepath=DIR -taskq.repl=1
```

## Clustered Mode

### Run the first node
Each TaskQ server has two listening ports. One used for its HTTP API and also
for Beehive's RPC. The other one is used for TaskQ's text-based protocol. The
former is set using `-addr` and the later is set using `-taskq.addr`.

You can run the first node using:
```bash
taskq -logtostderr -addr ADDR1 -taskq.addr TADDR1 -statepath DIR1
```

`ADDR1` and `TADDR1` are the listening addresses of the first node in the form
of `IP:PORT`. `STATE1` is the directory where the first node stores its state.

### Add a new nodes to the cluster
You can add new nodes to the cluster using:

```bash
taskq -logtostderr -addr ADDRN -paddrs ADDR1 -taskq.addr TADDRN -statepath DIRN
```

`ADDRN` and `TADDRN` are the listening addresses of the new node in the form
of `IP:PORT`. `ADDR1` is the address of the first node. You can also use the
address of any other live nodes instead of `ADDR1`.
`DIRN` is the directory where the N'th node stores its state.

## API
TaskQ has two endpoints: (i) HTTP and (ii) its own text-based protocol. They
support the same set of functionalities, but HTTP is easier to use while
TaskQ protocol is more efficient.

## HTTP API
TaskQ handles HTTP requests in the following format:

|URL                      |Method|Description|
|-------------------------|------|-----------|
|`/apps/taskq/QUEUE/tasks`|`POST`|Enqueue a task in QUEUE. Task body is submitted as POST data.|
|`/apps/taskq/QUEUE/tasks/deque`|`POST`|Dequeue a task from QUEUE. It returns the JSON representation of the dequeued task.|
|`/apps/taskq/QUEUE/tasks/ID`|`DELETE`|Acknowledge task ID in QUEUE|

For example, you can enqueue a task in TaskQ using the following command:
```
curl -X POST -d "TASKBODY" http://localhost:7677/apps/taskq/queue1/tasks
```

You can dequeue a task using the following command:
```
curl -X POST http://localhost:7677/apps/taskq/queue1/tasks/deque
```

You can acknowlege a dequeued task using the following command:
```
curl -X DELETE http://localhost:7677/apps/taskq/queue1/tasks/1
```

**NOTE:** You can send HTTP requests to any TaskQ server in your cluster.
TaskQ would remain consistent no matter where you initiate your request.

## TaskQ Protocol
To avoid the overheads of HTTP, TaskQ also has a text-based protocol with the
following requests:

|Request|Description|
|-------|-----------|
|`RID enq QUEUE LEN BODY`|Enqueue a task in QUEUE. LEN is a length of BODY.<sup>+</sup>|
|`RID deq QUEUE`|Dequeue a task from QUEUE|
|`RID ack QUEUE TASKID`|Acknowledge TASKID in QUEUE|
<sup>+</sup> If LEN is 0, the length of the task's body will be calculated
automatically.

RID is the request ID assigned by the client. This ID is returned along with
the response to identify its respective request.

TaskQ protocol has the following response format:

|Request|Description|
|-------|-----------|
|`RID enqed QUEUE TASKID`|The task is successfully enqueued.|
|`RID deqed QUEUE TASKID LEN BODY`|A task is successfully dequeued.|
|`RID acked QUEUE TASKID`|The task is successfully acknowledged.|
|`RID error MSG`|There is a request error.|

## Go Client
TaskQ has a Go client which is built based on the TaskQ text-based protocol.
For more information, refer to
[its GoDoc](http://godoc.org/github.com/kandoo/beehive/examples/taskq/client).

## Fault-tolerance
TaskQ's replication factor (set via `-taskq.repl`) is the replication factor
of one queue.  If you have `N` nodes in your cluster and a replication factor
of `R` for TaskQ, the nodes that store the tasks of one single queue can be on
any `R` nodes. Assuming that `R <= N`, as long as the majority of nodes serving
a queue (i.e., `0.5 x R + 1`) are up, TaskQ will serve requests to that queue.
Note that, when `R < N`, there will be nodes which does not participate as a
replica for a given queue.
