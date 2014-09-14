package bh

import "math/rand"

type ReplicationStrategy interface {
	SelectSlaveHives(mc MappedCells, repFactor int) []HiveID
}

type BaseReplHandler struct {
	LiveHives map[HiveID]bool
}

func (h *BaseReplHandler) Rcv(msg Msg, ctx RcvContext) error {
	switch d := msg.Data().(type) {
	case HiveJoined:
		h.LiveHives[d.HiveID] = true
	case HiveLeft:
		delete(h.LiveHives, d.HiveID)
	}
	return nil
}

func (h *BaseReplHandler) Map(msg Msg, ctx MapContext) MappedCells {
	return MappedCells{{"D", Key(ctx.Hive().ID())}}
}

func (h *BaseReplHandler) Hives(ctx RcvContext) []HiveID {
	hives := make([]HiveID, 0, len(h.LiveHives))
	for id := range h.LiveHives {
		if id == ctx.Hive().ID() {
			continue
		}
		hives = append(hives, id)
	}
	return hives
}

type ReplicationQuery struct {
	RepFactor int
	Cells     MappedCells
	Res       chan []HiveID
}

type rndRepl struct {
	BaseReplHandler
	hive Hive
}

func (h *rndRepl) Rcv(msg Msg, ctx RcvContext) error {
	switch d := msg.Data().(type) {
	case ReplicationQuery:
		nSlaves := d.RepFactor - 1
		hives := h.Hives(ctx)
		rndHives := make([]HiveID, 0, nSlaves)
		for _, i := range rand.Perm(nSlaves) {
			rndHives = append(rndHives, hives[i])
		}
		d.Res <- rndHives
		return nil
	default:
		return h.BaseReplHandler.Rcv(msg, ctx)
	}
}

func (h *rndRepl) SelectSlaveHives(mc MappedCells, repFactor int) []HiveID {
	if repFactor < 2 {
		return nil
	}

	resCh := make(chan []HiveID)
	h.hive.Emit(ReplicationQuery{
		RepFactor: repFactor,
		Cells:     mc,
		Res:       resCh,
	})
	return <-resCh
}

func newRndRepl(h Hive) *rndRepl {
	r := &rndRepl{
		BaseReplHandler: BaseReplHandler{
			LiveHives: make(map[HiveID]bool),
		},
		hive: h,
	}
	app := h.NewApp("RndRepl")
	app.Handle(ReplicationQuery{}, r)
	app.Handle(HiveJoined{}, r)
	app.Handle(HiveLeft{}, r)
	return r
}
