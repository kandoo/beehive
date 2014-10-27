package beehive

//DefaultHive is the hive used by Start() and NewApp().
var DefaultHive Hive

// Start starts the DefaultHive. This method blocks.
func Start() {
	maybeInitDefaultHive()
	DefaultHive.Start()
}

// NewApp creates a new application on the DefaultHive.
func NewApp(name string) App {
	maybeInitDefaultHive()
	return DefaultHive.NewApp(name)
}

// Emit emits a message on the DefaultHive.
func Emit(data interface{}) {
	DefaultHive.Emit(data)
}

func maybeInitDefaultHive() {
	if DefaultHive == nil {
		DefaultHive = NewHive()
	}
}
