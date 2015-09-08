package beehive

import (
	"sync"

	"golang.org/x/net/context"
)

//DefaultHive is the hive used by Start() and NewApp().
var DefaultHive Hive

// Start starts the DefaultHive. This method blocks.
func Start() {
	maybeInitDefaultHive()
	DefaultHive.Start()
}

// NewApp creates a new application on DefaultHive.
func NewApp(name string, options ...AppOption) App {
	maybeInitDefaultHive()
	return DefaultHive.NewApp(name, options...)
}

// Emit emits a message on DefaultHive.
func Emit(msgData interface{}) {
	maybeInitDefaultHive()
	DefaultHive.Emit(msgData)
}

// Sync processes a synchrounous message (req) and blocks until the response
// is recieved on DefaultHive.
func Sync(ctx context.Context, req interface{}) (res interface{}, err error) {
	maybeInitDefaultHive()
	return DefaultHive.Sync(ctx, req)
}

var defaultHiveInit sync.Once

func maybeInitDefaultHive() {
	defaultHiveInit.Do(func() {
		DefaultHive = NewHive()
	})
}
