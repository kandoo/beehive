# Beehive ![Travis Build Status](https://api.travis-ci.org/kandoo/beehive.svg?branch=master "Travis Build Status")

A distributed messaging platform focused on simplicity. Our goal
is to create a programming model that is almost identical to a
centralized application yet can automatically be distributed and
optimized. Beehive comes with built-in support for transactions,
replication, fault-tolerance, runtime instrumentation, and optimized
placement.

Beehive is:
- _Transactional_: Message handlers are either ran successfully, or
  otherwise won't have any side effects.
- _Replicated_: State of each application is replicated using a
  distributed consensus mechanism.
- _Fault Tolerant_: When a machine fails, its workload will be
  handed off to other machines in the cluster.
- _Instrumented_: You can see how your applications interact and
  how they exchange messages.
- _Optimized_: Beehive is able to adjust the placement of
  applications in the cluster to optimize their performance.

## Installation

_Prerequisite_ you need to install go (preferably version 1.2+) and
set up your GOPATH.

To install Beehive, run:

```
# go get github.com/kandoo/beehive
```

To test your setup, enter Beehive's root directory and run:
```
# go test -v
```

## Documents
- [User Guide](https://github.com/kandoo/beehive/tree/master/Docs/guide.md)
- [API Docs](https://godoc.org/github.com/kandoo/beehive)
- [Examples](https://github.com/kandoo/beehive/tree/master/examples)


## Projects using Beehive:
- [Beehive Distributed SDN Controller](https://github.com/kandoo/beehive-netctrl)

## Discussions
Google group: [https://groups.google.com/forum/#!forum/beehive-dev](https://groups.google.com/forum/#!forum/beehive-dev)

Please report bugs in github, not in the group.

## Publications
Soheil Hassas Yeganeh, Yashar Ganjali,
[Beehive: Towards a Simple Abstraction for Scalable Software-Defined Networking](http://conferences.sigcomm.org/hotnets/2014/papers/hotnets-XIII-final17.pdf),
HotNets XIII, 2014.
