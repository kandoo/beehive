package beehive

import "testing"

func testStatUpdate(t *testing.T, ctx *MockRcvContext, infos []BeeInfo,
	up beeMatrixUpdate) (*MockRcvContext, optimizerStat, error) {

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
	o := optimizer{}
	o.Rcv(&msg, ctx)
	d := ctx.Dict(optimizerDict)
	var os optimizerStat
	if err := d.GetGob(formatBeeID(up.Bee), &os); err != nil {
		t.Errorf("error in loading the optimizer stat: %v", err)
		return ctx, os, err
	}
	for id, cnt := range os.BeeMatrix.Matrix {
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
	ctx, os, err := testStatUpdate(t, nil, infos, up)
	if err != nil {
		return
	}

	if !os.Migrated {
		t.Errorf("invalid migrated flag in the optimizer state")
	}
	if len(ctx.CtxMsgs) != 1 {
		t.Errorf("optimizer didnot migrate")
	}
	cmd := ctx.CtxMsgs[0].Data().(cmdMigrate)
	if cmd.Bee != 1 {
		t.Errorf("invalid bee migrated: actual=%v want=%v", cmd.Bee, 1)
	}
	if cmd.To != 3 {
		t.Errorf("invalid destination hive: actual=%v want=%v", cmd.To, 3)
	}
}

func TestStatUpdateNoMigration(t *testing.T) {
	infos := []BeeInfo{
		{ID: 1, Hive: 1},
		{ID: 2, Hive: 2},
		{ID: 3, Hive: 1},
	}
	up := beeMatrixUpdate{
		Bee: 1,
		Matrix: map[uint64]uint64{
			2: 1,
			3: 2,
		},
	}
	ctx, os, err := testStatUpdate(t, nil, infos, up)
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
		ctx, _, err = testStatUpdate(t, ctx, infos, up)
		if err != nil {
			t.Fatalf("error in test stat update: %v", err)
			return
		}
	}

	req := statRequest{ID: 1}
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
	if res.ID != req.ID {
		t.Errorf("invalid response id: actual=%v want=%v", res.ID, req.ID)
	}
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
