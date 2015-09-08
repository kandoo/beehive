package main

import bh "github.com/kandoo/beehive"

const (
	helloDict = "HelloCountDict"
)

func rcvf(msg bh.Msg, ctx bh.RcvContext) error {
	name := msg.Data().(string)

	cnt := 0
	if v, err := ctx.Dict(helloDict).Get(name); err == nil {
		cnt = v.(int)
	}

	cnt++
	ctx.Printf("hello %s (%d)!\n", name, cnt)
	ctx.Dict(helloDict).Put(name, cnt)
	return nil
}

func mapf(msg bh.Msg, ctx bh.MapContext) bh.MappedCells {
	return bh.MappedCells{{helloDict, msg.Data().(string)}}
}

func main() {
	app := bh.NewApp("HelloWorld", bh.Persistent(1))
	app.HandleFunc(string(""), mapf, rcvf)

	name1 := "1st name"
	name2 := "2nd name"
	for i := 0; i < 3; i++ {
		go bh.Emit(name1)
		go bh.Emit(name2)
	}

	bh.Start()
}
