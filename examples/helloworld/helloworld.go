package main

import (
	"fmt"

	bh "github.com/kandoo/beehive"
)

const (
	helloDict = "HelloCountDict"
)

func rcvf(msg bh.Msg, ctx bh.RcvContext) error {
	name := msg.Data().(string)
	v, err := ctx.Dict(helloDict).Get(name)
	if err != nil {
		v = []byte{0}
	}

	cnt := v[0] + 1
	fmt.Printf("%v> hello %s (%d)!\n", ctx.ID(), name, cnt)
	ctx.Dict(helloDict).Put(name, []byte{cnt})
	return nil
}

func mapf(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return bh.MappedCells{{helloDict, msg.Data().(string)}}
}

func main() {
	app := bh.NewApp("HelloWorld")
	app.SetFlags(bh.AppFlagPersistent | bh.AppFlagTransactional)
	app.HandleFunc(string(""), mapf, rcvf)

	name1 := "1st name"
	name2 := "2nd name"
	for i := 0; i < 3; i++ {
		go bh.Emit(name1)
		go bh.Emit(name2)
	}

	bh.Start()
}
