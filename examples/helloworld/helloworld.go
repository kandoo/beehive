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
		fmt.Printf("%v> Hello %s!\n", ctx.ID(), name)
		ctx.Dict(helloDict).Put(name, []byte{1})
		return nil
	}

	cnt := v[0] + 1
	fmt.Printf("%v> hello %s for the %d'th time!\n", ctx.ID(), name, cnt)
	ctx.Dict(helloDict).Put(name, []byte{cnt})
	return nil
}

func mapf(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return bh.MappedCells{{helloDict, msg.Data().(string)}}
}

func main() {
	app := bh.NewApp("HelloWorld")
	app.HandleFunc(string(""), mapf, rcvf)
	name1 := "First"
	name2 := "Second"
	go bh.Emit(name1)
	go bh.Emit(name2)
	go bh.Emit(name1)
	go bh.Emit(name1)
	go bh.Emit(name2)
	go bh.Emit(name1)
	bh.Start()
}
