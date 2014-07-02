package actor

import (
	"os"
	"os/signal"
	"syscall"
)

func (s *stage) registerSignals() {
	s.sigCh = make(chan os.Signal, 1)
	signal.Notify(s.sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-s.sigCh
		s.Stop()
	}()
}
