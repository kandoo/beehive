# Hello World
Let's write an application that says hello to a given name
and counts the number of hellos it said to that name.
To implement this, we need an application that prints 
`hello NAME` in response to name messages and stores the stats of
each name in a dictionary.

This is the simplest way to create an application in beehive:
```go

import bh "github.com/kandoo/beehive"

func mapf(...) {...}
func rcvf(...) {...}
func main() {
	// Create a new application.
	app := bh.NewApp("HelloWorld", bh.AppPersistent(1))
	// This application handles string messages.
	// Its map function is mapf and its receive function
	// is rcvf.
	app.HandleFunc(string(""), mapf, rcvf)
	// Emit simply emits a message, here a
	// string of your name.
    go bh.Emit("your name")
    // We emit another message with the same name
    // to test the counting feature.
    go bh.Emit("your name")
    // Start starts the default hive.
	bh.Start()
}
```

Let's first implement the `rcvf`:
```go
// rev functions receive a message and the context.
func rcvf(msg bh.Msg, ctx bh.RcvContext) error {
	// The name is stored in the message data.
	name := msg.Data().(string)
	// Using cx.Dict you can get (or create) a dictionary.
	dict := ctx.Dict("hello_dict")
	// Using Get(), you can get the value for a key in the 
	// dictionary. Keys are always string and values
	// are always []byte.
	v, err := dict.Get(name)
	// If there is an error, the entry is not in the 
	// dictionary. So, we initialize v to 0.
	if err != nil {
		v = []byte{0}
	}
	// Now we increment the count. Well, this would
	// overflow at 255 but that's fine for the purpose
	// of this tutorial.
	cnt := v[0] + 1
	// And then we print the hello message. ctx.ID() returns
	// the ID of the bee that is running this handler.
	fmt.Printf("%v> hello %s (%d)!\n", ctx.ID(), name, cnt)
	// Finally we update the count stored in the dictionary.
	dict.Put(name, []byte{cnt})
	return nil
}
```

In this receive function (as explained in the comments), we simply
lookup the name in `hello_dict` and find out how many times
we have said hello for this name. Then we say hello accordingly!

To ensure that `rcvf` behaves well when distributed, we need to
make sure each name in `hello_dict` is updated in the same hive.
This is very simple to do in Beehive. We just need to implement a 
map function that maps the message to `("hello_dict", name)`:
```go
func mapf(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	// rcvf accesses hello_dict using the name
	// in this message.
	return bh.MappedCells{
		{
			Dict: "hello_dict",
			Key:  msg.Data().(string),
		},
	}
}
```

This ensures that all messages containing the same name will be
processed by the same bee (or in the same go-routine).

When you run this application, you will see the following output:
```
# go run helloworld.go

1> hello your name (1)!
1> hello your name (2)!
```

If you kill and restart the program, you will see the following output:
```
# go run helloworld.go

1> hello your name (3)!
1> hello your name (4)!
```

Yes, the count are preserved! why? Because the application was a persistent
application, remember?

```go
app := bh.NewApp("HelloWorld", bh.AppPersistent(1))
```

`bh.AppPersistent(1)` is an application option that makes this
application persistent with a replication factor of `1`. We will talk
about replication in [fault-tolrance](fault-tolerance.md).

Let's emit two different names in our `main` function:

```go
func main() {
	app := bh.NewApp("HelloWorld", bh.AppPersistent(1))
	app.HandleFunc(string(""), mapf, rcvf)
	name1 := "1st name"
	name2 := "2nd name"
	for i := 0; i < 3; i++ {
		go bh.Emit(name1)
		go bh.Emit(name2)
	}
	bh.Start()
}
```

The output of this application should be as follows (with a slightly different
order and with an occasional raft logs ;) ):

```
# go run helloworld.go

2> hello 2nd name (1)!
2> hello 2nd name (2)!
1> hello 1st name (1)!
2> hello 2nd name (3)!
1> hello 1st name (2)!
1> hello 1st name (3)!
```

Note that "1st name" and "2nd name" are handled by different bees.

In our example, we made the application persisent and transactional:
```go
bh.NewApp(..., bh.AppPersistent(1))
```

If you re-run the program (you can exit with `ctrl+c`),
you see that the counts are preserved for each name:
```
# go run helloworld.go

2> hello 2nd name (4)!
2> hello 2nd name (5)!
2> hello 2nd name (6)!
1> hello 1st name (4)!
1> hello 1st name (5)!
1> hello 1st name (6)!
```

You can find the complete code on
[the Hello World example](https://github.com/kandoo/beehive/tree/master/examples/helloworld/helloworld.go)

[Next: Handlers, Map + Rcv](handlers.md)
