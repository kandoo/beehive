package beehive

import (
	"encoding/gob"

	"github.com/kandoo/beehive/raft"
)

type cmdAddFollower struct {
	Hive uint64
	Bee  uint64
}
type cmdAddHive struct{ Info raft.NodeInfo }
type cmdCampaign struct{}
type cmdCreateBee struct{}
type cmdFindBee struct{ ID uint64 }
type cmdHandoff struct{ To uint64 }
type cmdJoinColony struct{ Colony Colony }
type cmdRefreshRole struct{}
type cmdLiveHives struct{}
type cmdMigrate struct {
	Bee uint64
	To  uint64
}
type cmdNewHiveID struct{ Addr string }
type cmdPing struct{}
type cmdReloadBee struct {
	ID     uint64
	Colony Colony
}
type cmdStart struct{}
type cmdStartDetached struct{ Handler DetachedHandler }
type cmdStop struct{}
type cmdSync struct{}

func init() {
	gob.Register(cmdAddFollower{})
	gob.Register(cmdAddHive{})
	gob.Register(cmdCampaign{})
	gob.Register(cmdCreateBee{})
	gob.Register(cmdFindBee{})
	gob.Register(cmdHandoff{})
	gob.Register(cmdJoinColony{})
	gob.Register(cmdRefreshRole{})
	gob.Register(cmdLiveHives{})
	gob.Register(cmdMigrate{})
	gob.Register(cmdAddHive{})
	gob.Register(cmdFindBee{})
	gob.Register(cmdCreateBee{})
	gob.Register(cmdNewHiveID{})
	gob.Register(cmdPing{})
	gob.Register(cmdReloadBee{})
	gob.Register(cmdStart{})
	gob.Register(cmdStartDetached{})
	gob.Register(cmdStop{})
	gob.Register(cmdSync{})
}
