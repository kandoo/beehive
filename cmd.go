package beehive

import (
	"encoding/gob"
	"errors"
	"fmt"
)

// If App is "" and To is 0, the command is handed to the hive.
// If App is not "" and To is 0, the command should be handed to the qee.
// Otherwise it is for a bee of that app.
type cmd struct {
	Hive uint64
	App  string
	Bee  uint64
	Data interface{}
}

func (c cmd) String() string {
	return fmt.Sprintf("CMD -> %v/%s/%v\t%#v", c.Hive, c.App, c.Bee, c.Data)
}

// ErrInvalidCmd is returned when the requested command is invalid or is not
// supported by the receiver of the command.
var ErrInvalidCmd = errors.New("invalid command")

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

func newCmdAndChannel(d interface{}, h uint64, a string, b uint64,
	ch chan cmdResult) cmdAndChannel {

	return cmdAndChannel{
		cmd: cmd{
			Hive: h,
			App:  a,
			Bee:  b,
			Data: d,
		},
		ch: ch,
	}
}

func init() {
	gob.Register(cmd{})
}
