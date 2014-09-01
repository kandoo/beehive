package bh

import "testing"

func maybeSkipRegisteryTest(h *hive, t *testing.T) {
	if len(h.config.RegAddrs) != 0 {
		return
	}

	t.Skip("Registery tests run only when the hive is connected to registery.")
}

func hiveWithAddressForRegisteryTests(addr string, t *testing.T) *hive {
	cfg := DefaultCfg
	cfg.HiveAddr = addr
	return NewHiveWithConfig(cfg).(*hive)
}

func TestRegistery(t *testing.T) {
	h1 := hiveWithAddressForRegisteryTests("127.0.0.1:32771", t)
	maybeSkipRegisteryTest(h1, t)

	h2 := hiveWithAddressForRegisteryTests("127.0.0.1:32772", t)
	maybeSkipRegisteryTest(h2, t)

	joinCh1 := make(chan interface{})
	go h1.Start(joinCh1)
	h1.waitUntilStarted()

	joinCh2 := make(chan interface{})
	go h2.Start(joinCh2)
	h2.waitUntilStarted()

	h1.Stop()
	<-joinCh1

	h2.Stop()
	<-joinCh2
}

func TestRegisteryUnregister(t *testing.T) {
	h := hiveWithAddressForRegisteryTests("127.0.0.1:32771", t)
	joinCh := make(chan interface{})
	maybeSkipRegisteryTest(h, t)

	go h.Start(joinCh)
	h.waitUntilStarted()
	h.Stop()
	<-joinCh

	k, _ := h.registery.hiveRegKeyVal()
	_, err := h.registery.Get(k, false, false)
	if err == nil {
		t.Errorf("Registery entry is not removed.")
	}
}
