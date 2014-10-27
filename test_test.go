package beehive

import (
	"fmt"
	"os"
)

func removeState(cfg HiveConfig) {
	os.RemoveAll(cfg.StatePath)
}

func waitTilStareted(h Hive) {
	pingHive(h)
}

func pingHive(h Hive) {
	h.(*hive).sendCmd(cmdPingHive{})
}

var (
	port = 6677
)

func newHiveAddrForTest() string {
	port++
	return fmt.Sprintf("127.0.0.1:%v", port)
}
