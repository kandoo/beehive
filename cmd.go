package beehive

import (
	"errors"
	"fmt"
)

// If App is not "" and To is 0, the command should be handed to the qee.
// Otherwise it is for a bee of that app.
type cmd struct {
	To   uint64
	App  string
	Data interface{}
}

func (c cmd) String() string {
	return fmt.Sprintf("CMD -> %v\t%#v", c.To, c.Data)
}

var ErrInvalidCmd = errors.New("Invalid command")

type cmdAndChannel struct {
	cmd cmd
	ch  chan cmdResult
}

type cmdResult struct {
	Data interface{}
	Err  error
}

func (r cmdResult) get() (interface{}, error) {
	return r.Data, r.Err
}

func newCmdAndChannel(d interface{}, a string, to uint64,
	ch chan cmdResult) cmdAndChannel {

	return cmdAndChannel{
		cmd: cmd{
			Data: d,
			App:  a,
			To:   to,
		},
		ch: ch,
	}
}
