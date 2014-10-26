package beehive

import "math/rand"

type replicationStrategy interface {
	// SelectHives selects n hives that are not blacklisted. If not possible, it
	// returns an empty slice.
	selectHives(blackList []uint64, n int) []uint64
}

type rndRepliction struct {
	hive *hive
}

func (r *rndRepliction) selectHives(blacklist []uint64, n int) []uint64 {
	if n <= 0 {
		return nil
	}

	blmap := make(map[uint64]uint64)
	for _, h := range blacklist {
		blmap[h] = h
	}

	lives := r.hive.registry.hives()
	whitelist := make([]uint64, 0, len(lives))
	for _, h := range lives {
		if h.ID == r.hive.ID() || blmap[h.ID] != 0 {
			continue
		}
		whitelist = append(whitelist, h.ID)
	}

	if len(whitelist) < n {
		n = len(whitelist)
	}

	if n == 0 {
		return nil
	}

	rndHives := make([]uint64, 0, n)
	for _, i := range rand.Perm(n) {
		rndHives = append(rndHives, whitelist[i])
	}
	return rndHives
}

func newRndReplication(h *hive) *rndRepliction {
	r := &rndRepliction{
		hive: h,
	}
	return r
}
