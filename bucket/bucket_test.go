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
			quantum:    3,
			resolution: time.Microsecond,
		},
	}

	for _, c := range cases {
		b := New(c.rate, 0)
		if b.quantum != c.quantum {
			t.Errorf("invalid quantum: want=%v get=%v", c.quantum, b.quantum)
		}
		if b.resolution != c.resolution {
			t.Errorf("invalid resolution: want=%v get=%v", c.resolution, b.resolution)
		}
	}
}

func TestUnlimited(t *testing.T) {
	b := New(Unlimited, 0)
	if !b.Has(1000) {
		t.Errorf("bucket is not unlimited")
	}
	if !b.Get(1000) {
		t.Errorf("bucket is not unlimited")
	}
	if b.When(1000) != 0 {
		t.Errorf("bucket is not unlimited")
	}
}

func TestMax(t *testing.T) {
	const tks = 2e6
	b := New(1*MTPS, tks)
	<-time.After(b.When(tks))
	b.Get(0)

	if b.tokens != tks {
		t.Errorf("tokens are not maxed: want=%d get=%d", int(tks), b.tokens)
	}
}

func TestWhenGetHas(t *testing.T) {
	cases := []struct {
		rate    Rate
		max     uint64
		request uint64
		wait    time.Duration
		ticks   time.Duration
	}{
		{
			rate:    1 * TPS,
			max:     1,
			request: 1,
			wait:    1 * time.Second,
			ticks:   100 * time.Millisecond,
		},
		{
			rate:    3333 * TPS,
			max:     3333,
			request: 1,
			wait:    1 * time.Second,
			ticks:   100 * time.Millisecond,
		},
		{
			rate:    1000 * TPS,
			max:     100,
			request: 10,
			wait:    10 * time.Millisecond,
			ticks:   1 * time.Millisecond,
		},
		{
			rate:    Unlimited,
			max:     0,
			request: 1e11,
			wait:    0,
			ticks:   1 * time.Millisecond,
		},
	}

	for has := 0; has < 2; has++ {
		for _, c := range cases {
			s := time.Now()
			b := New(c.rate, c.max)
			if w := b.When(c.request) + time.Since(s); w < c.wait {
				t.Errorf("invalid when: want=%v+ got=%v", c.wait, w)
			}
			n := int(c.wait / c.ticks)
			for i := 0; i < n; i++ {
				if b.Has(c.request) {
					if time.Since(s) < c.wait {
						t.Errorf("invalid wait: bucket is filled after %v ticks", i)
					}
					break
				}
				<-time.After(c.ticks)
			}

			f := b.Get
			if has != 0 {
				f = b.Has
			}
			if !f(c.request) {
				t.Errorf("invalid token amount: want=%v got=%v", c.request, b.tokens)
			}
		}
	}
}

func TestReset(t *testing.T) {
	b := New(Unlimited, 0)
	b.Reset()
}
