# Transactions
By default, all applications are transactional in Beehive,
meaning that the bee will open up a transaction for each
call to the `rcv` function. If the `rcv` panics or returns an
error the transaction is automatically aborted, and otherwise 
is committed.

## Transaction == State + Message
Beehive transactions include all the modification to the state
dictionaries and all the messages emitted in a `rcv` function.
That is, when a transaction is aborted, all the messages and all
state modifications are dropped.

## Explicit Transactions
To open a transaction in a `rcv` function you can use the `BeginTx()`, `CommitTx()` and `AbortTx()`.
```go
func rcvf(msg bh.Msg, ctx bh.RcvContext) error {
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

## Disable Transactions
To disable automatic transactions for an application (say,
for performance reasons), use the `AppNonTransactional` option:
```go
NewApp("myapp", bh.AppNonTransactional)
```
