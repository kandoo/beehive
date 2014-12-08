# Overview

## Hives
In Beehive, each logical computing node (say, a physical or a virtual
machine) is denoted as a __hive__. Hives can form, join to, and leave a
cluster. Beehive clusters are homogenous, meaning that all hives in the
same cluster are running the same set of applications.

## Applications, Dictionaries, and Message Handlers
In Beehive, an __application__ is defined as a set of asynchronous
__message handlers__. Message handlers simply process async
messages and store their state in __dictionaries__. Application
dictionaries are in-memory key-value stores (with support for
[persistence](persistence.md) and [replication](fault-tolerant.md)).
Each message handler has a `rcv` function that actually processes
the message.

```
                 +-----------------+---+---+---+-------------------+
                 |                 |   |   |   |                   |
                 |                 |   |   |   |                   |
     +-----------v-----------+     v   v   v   v       +-----------v-----------+
     | hive 1                |                         |  hive N               |
     |                       |                         |                       |
     |                       |                         |                       |
     | +-------------------+ |                         | +-------------------+ |
     | |app1               | |                         | |app1               | |
     | |-------------------| |                         | |-------------------| |
     | |handler1 (map, rcv)| |                         | |handler1 (map, rcv)| |
     | |handler2 (map, rcv)| |     ..............      | |handler2 (map, rcv)| |
     | +-------------------+ |                         | +-------------------+ |
     |                       |                         |                       |
     | +-------------------+ |                         | +-------------------+ |
     | |app2               | |                         | |app2               | |
     | |-------------------| |                         | |-------------------| |
     | |handler1 (map, rcv)| |                         | |handler1 (map, rcv)| |
     | +-------------------+ |                         | +-------------------+ |
     +-----------------------+                         +-----------------------+
```

# Consistency and `map`
To scale, we want to balance the load of message processing among 
multiple hives. But, we need to do this in a way that the application's
behavior remains identical to when we use only a single, centralized
machine.

The important part for this is preserving consistency of application
dictionaries. In other words,  we want to make sure that each 
entry (or as we call them, __cell__) in an application dictionary is 
always accessed on the same machine. Otherwise, we can't guarantee
that the application state remains consistent when distributed over
multiple hives.

To that end, for each message, we need to know what are the keys used
to process the message in the `rcv` function of a message handler.
We call this the __mapped cells__ of that message. Each message
handler, in addition to its `rcv` function, needs to provide a 
`map` function that maps an incoming message to __cells__ or simply
keys in application dictionaries. 

## Bees
Applications and their message handlers are completely passive
in Beehive.
Internally, each hive has a set of go-routines called __bees__ for 
each application.
Each bee exclusively owns a set of cells. These cells are
the cells that must be collocated to preserve state
consistency. Cells are locked by bees using an internal
distributed consensus mechanism. Bees can be persistent and
fault-tolerant. When a hive crashes, we reload all the bees.

Moreover, for replicated applications bees will form
a colony of bees (itself and some other bees on other hives)
and will consistently replicate its cells using raft.
When a bee fails, we hand its cells and workload to other
bees in the cluster if it is replicated.

## Life of a Message
When a message arrives at a hive, we first pass that message
to the `map` function of the registered message handlers.
The `map` function returns the mapped cells of that message.
Then, we relay the message to the bee that has any of the keys
in the mapped cell and that bee in return calls the `rcv`
function of that message handler. This bee may be on the same 
hive or can be on another hive.
If there is no such bee (this is the first message mapped to those
cells) , we elect a hive, create one bee on it, and relay the message.

[Next: Write Your First App](helloworld.md)
