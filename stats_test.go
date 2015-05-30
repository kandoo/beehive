package beehive

import "testing"

func testStatUpdate(t *testing.T, ctx *MockRcvContext, infos []BeeInfo,
	up beeMatrixUpdate, minScore int) (*MockRcvContext, optimizerStat, error) {

	if ctx == nil {
		reg := newRegistry("")
		for _, i := range infos {
			if reg.BeeID < i.ID {
				reg.BeeID = i.ID
			}
		}
		for _, i := range infos {
			reg.addBee(i)
		}
		h := &hive{
			registry: reg,
		}
		ctx = &MockRcvContext{
			CtxHive: h,
		}
	}
	msg := MockMsg{
		MsgData: up,
	}
	var o Handler = optimizerCollector{}
	o.Rcv(&msg, ctx)
	d := ctx.Dict(dictOptimizer)

	os := optimizerStat{}
	v, err := d.Get(formatBeeID(up.Bee))
	if err != nil {
		t.Errorf("error in loading the optimizer stat: %v", err)
		return ctx, os, err
	}
	os = v.(optimizerStat)

	o = optimizer{minScore: minScore}
	o.Rcv(&MockMsg{}, ctx)
	v, err = d.Get(formatBeeID(up.Bee))
	if err != nil {
		t.Errorf("error in loading the optimizer stat: %v", err)
		return ctx, os, err
	}
	os = v.(optimizerStat)

	for id, cnt := range os.Matrix {
		if up.Matrix[id] != cnt {
			t.Errorf("invalid bee matrix for %v: actual=%v want=%v", id, cnt,
				up.Matrix[id])
		}
	}
	return ctx, os, nil
}

func TestStatUpdateMigration(t *testing.T) {
	infos := []BeeInfo{
		{ID: 1, Hive: 1},
		{ID: 2, Hive: 2},
		{ID: 3, Hive: 3},
	}
	up := beeMatrixUpdate{
		Bee: 1,
		Matrix: map[uint64]uint64{
			2: 1,
			3: 2,
		},
	}
	ctx, _, err := testStatUpdate(t, nil, infos, up, 0)
	if err != nil {
		return
	}
	stats := getOptimizerStats(ctx.Dict(dictOptimizer))
	if stats[1].Migrated {
		t.Error("1 should not be migrated")
	}
	if !stats[2].Migrated {
		t.Error("2 should be migrated")
	}
	if !stats[3].Migrated {
		t.Error("3 should be migrated")
	}

	if len(ctx.CtxMsgs) != 2 {
		t.Fatalf("optimizer didnot migrate")
	}
	migrated := make(map[uint64]struct{})
	for _, msg := range ctx.CtxMsgs {
		cmd := msg.Data().(cmdMigrate)
		if cmd.To != 1 {
			t.Errorf("invalid destination hive: actual=%v want=1", cmd.To)
		}
		migrated[cmd.Bee] = struct{}{}
	}
	if _, ok := migrated[2]; !ok {
		t.Error("2 is not migrated")
	}
	if _, ok := migrated[3]; !ok {
		t.Error("3 is not migrated")
	}
}

func TestStatUpdateNoMigration(t *testing.T) {
	infos := []BeeInfo{
		{ID: 1, Hive: 1},
		{ID: 2, Hive: 2},
		{ID: 3, Hive: 1},
		{ID: 4, Hive: 2},
	}
	ups := []beeMatrixUpdate{
		{
			Bee: 1,
			Matrix: map[uint64]uint64{
				2: 1,
				3: 2,
			},
		},
		{
			Bee: 2,
			Matrix: map[uint64]uint64{
				4: 10,
				1: 1,
			},
		},
		{
			Bee: 1,
			Matrix: map[uint64]uint64{
				2: 4,
				3: 2,
			},
		},
	}
	for _, up := range ups {
		ctx, os, err := testStatUpdate(t, nil, infos, up, 1)
		if err != nil {
			return
		}
		if os.Migrated {
			t.Errorf("invalid migrated flag in the optimizer state")
		}
		if len(ctx.CtxMsgs) != 0 {
			t.Errorf("optimizer migrated the bee")
		}
	}
}

func TestStatRequest(t *testing.T) {
	infos := []BeeInfo{
		{ID: 1, Hive: 1},
		{ID: 2, Hive: 2},
		{ID: 3, Hive: 1},
		{ID: 4, Hive: 2},
	}
	ups := []beeMatrixUpdate{
		{
			Bee: 1,
			Matrix: map[uint64]uint64{
				2: 1,
				3: 2,
			},
		},
		{
			Bee: 2,
			Matrix: map[uint64]uint64{
				1: 4,
				3: 12,
				4: 20,
			},
		},
	}
	var ctx *MockRcvContext
	var err error
	for _, up := range ups {
		ctx, _, err = testStatUpdate(t, ctx, infos, up, 0)
		if err != nil {
			t.Fatalf("error in test stat update: %v", err)
			return
		}
	}
	ctx.CtxMsgs = nil

	req := statRequest{}
	msg := MockMsg{
		MsgData: req,
		MsgFrom: 1,
	}
	h := statRequestHandler{}
	if err := h.Rcv(&msg, ctx); err != nil {
		t.Fatalf("error in request handler: %v", err)
	}
	if len(ctx.CtxMsgs) == 0 {
		t.Fatal("no repsonse from request handler")
	}
	res := ctx.CtxMsgs[0].Data().(statResponse)
	for b, m := range res.Matrix {
		up := ups[b-1]
		for id, cnt := range up.Matrix {
			if m[id] != cnt {
				t.Errorf("invalid data in matrix of bee %v: actual=%v want=%v", id,
					m[id], cnt)
			}
		}
	}
}

func TestProvenanceUpdate(t *testing.T) {
	ctx := MockRcvContext{}
	r := beeRecord{
		Bee: 1,
		In:  &msg{MsgData: string("test")},
		Out: []*msg{&msg{MsgData: string("test")}},
	}
	c := localCollector{}
	k := formatBeeID(r.Bee)

	for i := 1; i < 10; i++ {
		c.updateProvenance(r, &ctx)
		var m provMatrix
		if v, err := ctx.Dict(dictLocalProv).Get(k); err == nil {
			m = v.(provMatrix)
		}
		if cnt := m["string"]["string"]; cnt != uint64(i) {
			t.Errorf("incorrect provenence data: actual=%v want=%v", cnt, i)
		}
	}
}
