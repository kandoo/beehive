package bucket

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	cases := []struct {
		rate       Rate
		quantum    uint64
		resolution time.Duration
	}{
		{
			rate:       1 * TPS,
			quantum:    1,
			resolution: time.Second,
		},
		{
			rate:       3 * MTPS,
			quantum:    3000,
			resolution: DefaultResolution,
		},
	}

	for _, c := range cases {
		b := New(c.rate, 0, DefaultResolution)
		if b.quantum != c.quantum {
			t.Errorf("invalid quantum: want=%v get=%v", c.quantum, b.quantum)
		}
		if b.resolution != c.resolution {
			t.Errorf("invalid resolution: want=%v get=%v", c.resolution, b.resolution)
		}
	}
}

func TestUnlimited(t *testing.T) {
	b := New(Unlimited, 0, DefaultResolution)
	if !b.Has(1000) {
		t.Errorf("bucket is not unlimited")
	}
	if !b.Get(1000) {
		t.Errorf("bucket is not unlimited")
	}
	b.Put(1000)
	b.Ticker()
}

func TestTicker(t *testing.T) {
	b := New(1*MTPS, 1000, DefaultResolution)
	k := b.Ticker()
	select {
	case <-k.C:
		b.Tick()
	case <-time.After(2 * DefaultResolution):
		t.Fatalf("ticker does not tick")
	}

	if b.tokens != 1000 {
		t.Errorf("invalid number of tokens: want=%v get=%v", 1000, b.tokens)
	}
}

func TestTick(t *testing.T) {
	b := New(1*MTPS, 1e10, DefaultResolution)
	b.Tick()

	if b.tokens != 1000 {
		t.Errorf("invalid tick quantum: want=%d get=%d", 1000, b.tokens)
	}
}

func TestMax(t *testing.T) {
	b := New(1*MTPS, 1e10, DefaultResolution)
	b.Put(1e11)
	b.Get(0)

	if b.tokens != 1e10 {
		t.Errorf("tokens are not maxed: want=%d get=%d", int(1e10), b.tokens)
	}
}
