# Beehive ![Travis Build Status](https://api.travis-ci.org/kandoo/beehive.svg?branch=master "Travis Build Status") [![GoDoc](https://godoc.org/github.com/kandoo/beehive?status.svg)](http://godoc.org/github.com/kandoo/beehive)

Beehive is a distributed programming framework that comes with built-in
transactions, replication, fault-tolerance, runtime instrumentation, and
optimized placement.

* [Installation](#installation)
* [Hello World](#hello-world)
	* [Message](#message)
	* [Message Handler](#message-handler)
	* [Emit Hello](#emit-hello)
	* [Start](#start)
	* [Run a Cluster](#run-a-cluster)
* [Deep Dive](#deep-dive)
	* [Hives](#hives)
	* [Applications, Dictionaries, and Message Handlers](#applications-dictionaries-and-message-handlers)
	* [Map and Consistent Concurrency](#map-and-consistent-concurrency)
	* [Bees](#bees)
	* [Life of a Message](#life-of-a-message)
	* [Reply Messages](#reply-messages)
	* [HTTP Handlers](#http-handlers)
	* [Detached Handlers](#detached-handlers)
	* [Transactions](#transactions)
	* [Runtime Instrumentation and Optimization](#runtime-instrumentation-and-optimization)
* [Projects using Beehive](#projects-using-beehive)
* [Mailing List](#mailing-list)
* [Publications](#publications)

## Installation

**Option 1.** Install Beehive using [goget](https://github.com/soheilhy/goget):

```
curl -sL https://git.io/goget | bash -s -- github.com/kandoo/beehive
```

**Option 2.** Install go 1.4+, set up your `GOPATH`, and install Beehive using
`go get`:

```
go get github.com/kandoo/beehive
```

**Test Your Setup.** Enter Beehive's root directory
(`$GOPATH/src/github.com/kandoo/beehive`) and run:
```
go test -v
```

## Hello World
Let's write a simple example that counts the number of times we have
said hello to each person. You can find the complete example in the
[GoDoc](https://godoc.org/github.com/kandoo/beehive/#example-package)

### Message
Beehive is based on asynchronous message passing. Naturally, the first step is
to define a `Hello` message:
```go
// Hello represents a message in our hello world example.
type Hello struct {
	Name string // Name is the name of the person saying hello.
}
```
### Message Handler
To handle `Hello` messages, we need to write an application that
has a message handler for `Hello`. Pretty analogous to HTTP handlers,
except we are processing messages not HTTP requests.

A message handler in Beehive consists of two functions: (i) `Rcv` and
(ii) `Map`. `Rcv` is the function that actually processes a message.
Since Beehive provides a generic runtime `Map` function that works for
all applications, let's skip the `Map` function for now, and we will
explain it in next section.

This is a simple `Rcv` function that handles `Hello` messages (don't panic
it's all comments ;) ):
```go
// Rcvf receives the message and the context.
func Rcvf(msg beehive.Msg, ctx beehive.RcvContext) error {
    // msg is an envelope around the Hello message.
    // You can retrieve the Hello, using msg.Data() and then
    // you need to assert that its a Hello.
    hello := msg.Data().(Hello)
    // Using ctx.Dict you can get (or create) a dictionary.
    dict := ctx.Dict("hello_dict")
    // Using Get(), you can get the value associated with
    // a key in the dictionary. Keys are always string
    // and values are generic interface{}'s.
    v, err := dict.Get(hello.Name)
    // If there is an error, the entry is not in the
    // dictionary. Otherwise, we set cnt based on
    // the value we already have in the dictionary
    // for that name.
    cnt := 0
    if err == nil {
        cnt = v.(int)
    }
    // Now we increment the count.
    cnt++
    // And then we print the hello message.
    ctx.Printf("hello %s (%d)!\n", hello.Name, cnt)
    // Finally we update the count stored in the dictionary.
    return dict.Put(hello.Name, cnt)
}
```

To register a message handler, we first create an application and then we
register the `Hello` handler for our application:
```go
// Create the hello world application and make sure .
app := beehive.NewApp("hello-world", beehive.Persistent(1))
// Register the handler for Hello messages.
app.HandleFunc(Hello{}, beehive.RuntimeMap(Rcvf), Rcvf)
```
Note that our application is persistent and will save its state on 1 node
(i.e., persistent but not replicated).

### Emit Hello
Now, to send a `Hello` message, you can emit it:
```go
// Emit simply emits a message, here a
// string of your name.
go beehive.Emit(Hello{Name: "your name"})
// Emit another message with the same name
// to test the counting feature.
go beehive.Emit(Hello{Name: "your name"})
```
Whenever you emit a `Hello` message, it will be processed by all applications
that have a handler for `Hello`. Here, we have only one application, but
you could create different applications with different handlers for `Hello`.
All of them would receive the `Hello` message.

### Start
Finally, we need to start Beehive:
```go
beehive.Start()
```

When you run the application (say `go run helloworld.go`),
you will have the following output:
```
bee 1/HelloWorld/0000000000000402> hello your name (1)!
bee 1/HelloWorld/0000000000000402> hello your name (2)!
```

When you run the application one more time, you will see the
following output:
```
bee 1/HelloWorld/0000000000000402> hello your name (3)!
bee 1/HelloWorld/0000000000000402> hello your name (4)!
```
Note that the counter is saved on disk, so you can safely
restart your application.

### Run a Cluster
This simple hello world application is actually a distributed application.
The message handler is automatically sharded by `Hello.Name`. Later,
we will explain how that happens.
For now, let's just try to run our hello world application in a cluster.

Run the first node as you have done previously (`go run helloworld.go`).
Wait until you see the hello messages:
```
bee 1/HelloWorld/0000000000000402> hello your name (5)!
bee 1/HelloWorld/0000000000000402> hello your name (6)!
```

Then, run a new node using the following command:
```
go run helloworld.go -addr localhost:7678 -paddrs localhost:7677 -statepath /tmp/beehive2
```

After you connect the second node, the first node should generate the following
output:
```
bee 1/HelloWorld/0000000000000402> hello your name (7)!
bee 1/HelloWorld/0000000000000402> hello your name (8)!
```

Note that in the last command,
`-addr` sets the listening address of the beehive server,
`-paddrs` sets the address of the peers (the first node is listening on the
default port, 7677), and
`-statepath` sets where beehive should store its state and the dictionaries.

**Note:** You can reinitializing the cluster by removing both
`/tmp/beehive` and `/tmp/beehive2`, and re-running the commands.

## Deep Dive

### Hives
A __Hive__ is basically a Beehive server, representing one logical unit of
computing (say, a physical or a virtual machine). Hives can form, join to,
and leave a cluster. Beehive clusters are homogeneous, meaning that all
hives in the same cluster are running the same set of applications.

### Applications, Dictionaries, and Message Handlers
A Beehive application is a set of asynchronous message handlers.
Message handlers simply process async messages and store their state in
dictionaries. A dictionary is basically a hash map.
Behind the scenes, these dictionaries are saved to disk and are replicated.
A message handler is composed of a `Rcv` function that actually processes
the message and a `Map` function declaring how messages should be
sharded/partitioned. Beehive provides a generic `Map` functions (as
you saw in the Hello World example) and also has a
[compiler](https://github.com/kandoo/beehive/tree/master/compiler) that
can generate `Map` functions based on your `Rcv` functions.
Having said that, `Map` functions are almost always one-liners and
are pretty easy to implement.

### `Map` and Consistent Concurrency
To make the distributed and concurrent version of of message handlers,
we want to balance the load of message processing among
multiple go-routines across multiple hives. We need to do
this in a way that the application's behavior remains identical
to when we use only a single, centralized go-routine. To do so,
we need to preserve the consistency of application dictionaries.

In other words,  we want to make sure that each
entry (or as we call them, __cell__) in an application dictionary is
always accessed on the same logical go-routine. Otherwise,
we can't guarantee that the application behaves consistently
when distributed over multiple hives. For example, what would happen
to our hello world application if two different go-routines could
read and modify the same entry concurrently?

To that end, for each message, we need to know what are the keys used
to process the message in the `Rcv` function of a message handler.
We call this the __mapped cells__ of that message. Each message
handler, in addition to its `Rcv` function, needs to provide a
`Map` function that maps an incoming message to __cells__ or simply
keys in application dictionaries. `Map` functions are usually
very simple to implement, but you can also use Beehive's generic
`RuntimeMap` function or the Beehive compiler to generate the
`Map` function.

### Bees
Applications and their message handlers are passive in Beehive.
Internally, each hive has a set of go-routines called __bees__
that run the message handlers for each application.
Each bee exclusively owns a set of cells. These cells are
the cells that must be accessed by the same go-routine to
preserve consistency.
Cells are locked by bees using an internal distributed consensus
mechanism implemented using Raft. Bees persist their cells
if needed and, when a hive restarts, we reload all the bees.

Moreover, for replicated applications, bees will form
a colony of bees (itself and some other bees on other hives)
and will consistently replicate its cells using raft.
When a bee fails, we hand its cells and workload to other
bees in the colony. The size of a colony is equal to the
application's [replication factor](https://godoc.org/github.com/kandoo/beehive#Persistent).

### Life of a Message
When a message is emitted on a hive, we first pass that message
to the `Map` function of the registered message handlers for that
type of message.
The `Map` function returns the mapped cells of that message.
Then, we relay the message to the bee that has any of the keys
in the mapped cell. That bee in response calls the `Rcv`
function of that message handler. This bee may be on the same
hive or can be on another hive.

If there is no such bee (this is the first message mapped to those
cells), we elect a hive, create one bee on it, and relay the message.
By default, we create the bee on the local hive, but applications
can register custom
[placement methods](https://godoc.org/github.com/kandoo/beehive#WithPlacement)
to change this behavior. For example, using this option, one can
implement a random placement.

### Reply Messages
So far, we have seen emitted messages but we cannot use that
for communication between applications, say to implement
a request response system. For example, a better way to implement
our hello world application would be emitting a `Hello` message
and waiting for a response that contains the count.

In Beehive, you can reply to a message using
[ReplyTo](https://godoc.org/github.com/kandoo/beehive#MockRcvContext.ReplyTo)
method. For example, we can rewrite our hello world
application using reply messages:

```go
func Rcvf(msg beehive.Msg, ctx beehive.RcvContext) error {
	hello := msg.Data().(Hello)
	dict := ctx.Dict("hello_dict")
	v, err := dict.Get(hello.Name)
	cnt := 0
	if err == nil {
			cnt = v.(int)
	}
	cnt++
	// Reply to the message with the count of hellos.
	ctx.ReplyTo(msg, cnt)
	return dict.Put(name, cnt)
}
```

To use this version, we also need to emit `Hello` messages
and wait for the application's response. We can implement
this using [Sync](https://godoc.org/github.com/kandoo/beehive#Hive.Sync):

```go
go beehive.Start()
defer beehive.Stop()

name = "your name"
// Sync sends the Hello message and waits until it receives the reply.
res, err := beehive.Sync(context.TODO(), Hello{Name: name})
if err != nil {
	...
	return
}
cnt := res.(int)
fmt.Printf("%s (%d)!\n", name, cnt)
```

### HTTP Handlers
Beehive applications can register custom HTTP handlers, handling requests to
URLs with the `/apps/APP_NAME/` prefix. To register a HTTP handler,
applications can use
[`HandleHTTP`](https://godoc.org/github.com/kandoo/beehive#App.HandleHTTP):

```go
app.HandleHTTP("/", httpHandler)
```

Internally we use [Gorilla mux](http://www.gorillatoolkit.org/pkg/mux), and we
expose the sub-router of each application. As a result, applications can match
against regular expression, use parameters in the URL, and specify HTTP methods:

```go
app.HandleHTTP("/{name}", httpHandler).Methods("POST")
```

HTTP handlers usually communicate with the application using synchronous
messaging. For example, we can implement a HTTP handler for our hello world
application as follows:

```go
type HelloHTTPHandler struct {}

func (h *HelloHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name, ok := vars["name"]
	if !ok {
		http.Error(w, "no name", http.StatusBadRequest)
		return
	}

	res, err := beehive.Sync(context.TODO(), Hello{Name: name})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "hello %s (%d)\n", name, res.(int))
}
```

Now you can send HTTP requests to this handler using curl:

```
curl -X POST localhost:7677/apps/hello-world/yourname
```

And you should see the following output:
```
hello yourname (1)
```

### Detached Handlers
Sometimes you need to create go-routines that read data from network connections
or file system and generate messages. For example, to implement a network
listener for a custom protocol, we need to run a network listener and then
run go-routines for each established connection. To implement such
functionalities in Beehive, we can use
[detached handlers](https://godoc.org/github.com/kandoo/beehive#DetachedHandler).
A detached handler is different than a message handler in a sense that it is
started in its `Start` method and only receives replies.

The
[Detached Example](https://godoc.org/github.com/kandoo/beehive/#example-package--Detached)
demonstrates how we used detached handlers to implement a text-based protocol in
Beehive. When you run this example, you can telnet to port 6789 for sending
names to the hello world application:

```
telnet localhost 6789
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
your name
hello your name (1)!
your name
hello your name (2)!
your name
hello your name (3)!
```

### Transactions
By default, all applications are transactional in Beehive,
meaning that the bee opens up a transaction for each
call to the `Rcv` function. If the `Rcv` panics or returns an
error the transaction is automatically aborted, and otherwise
is committed.

In Beehive, transactions include all the modification to the
dictionaries and all the messages emitted in a `Rcv` function.
That is, when a transaction is aborted, all the messages and all
dictionary modifications are dropped.

Transactions are by default implicit, but message handlers can
explicitly control transactions. To open a transaction in a `Rcv`
function you can use `BeginTx()`, `CommitTx()` and `AbortTx()`:
```go
func Rcvf(msg bh.Msg, ctx bh.RcvContext) error {
	ctx.BeginTx()
	d1 := ctx.Dict("d1")
	d1.Put("k", []byte{1})
	ctx.Emit(MyMsg("test1"))
	// Update d1/k and emit test1.
	ctx.CommitTx()

	ctx.BeginTx()
	d2 := ctx.Dict("d2")
	d2.Put("k", []byte{2})
	ctx.Emit(MyMsg("test2"))
	// DO NOT update d2/k and DO NOT emit test2.
	ctx.AbortTx()
}
```

To disable automatic transactions for an application (say,
for performance reasons), use the
[`NonTransactional`](https://godoc.org/github.com/kandoo/beehive#NonTransactional)
option.

### Runtime Instrumentation and Optimization
Beehive is capable of automatic runtime instrumentation. It measures the
messages exchanged between different bees and can use it to live migrate bees
to minimize latency. You can enable instrumentation and optimization by passing
`-instrument` command line arguments to your Beehive program. You can also
access the instrumentation data on Beehive's web interface (by default,
http://localhost:7677/).

## Projects using Beehive
- [Beehive Distributed SDN Controller](https://github.com/kandoo/beehive-netctrl)
- [TaskQ](https://github.com/kandoo/beehive/tree/master/examples/taskq)

## Mailing List
Google group: [https://groups.google.com/forum/#!forum/beehive-dev](https://groups.google.com/forum/#!forum/beehive-dev)

Please report bugs in github, not in the group.

## Publications
Soheil Hassas Yeganeh, Yashar Ganjali,
[Beehive: Towards a Simple Abstraction for Scalable Software-Defined Networking](http://conferences.sigcomm.org/hotnets/2014/papers/hotnets-XIII-final17.pdf),
HotNets XIII, 2014.
