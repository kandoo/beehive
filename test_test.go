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
	h.(*hive).processCmd(cmdPing{})
}

var (
	testPort = 6000
)

func newHiveConfigForTest() (cfg HiveConfig) {
	cfg = DefaultCfg
	testPort++
	cfg.RPCAddr = fmt.Sprintf("127.0.0.1:%v", testPort)
	testPort++
	cfg.PublicAddr = fmt.Sprintf("127.0.0.1:%v", testPort)
	cfg.StatePath = fmt.Sprintf("/tmp/bhtest-%v", testPort)
	removeState(cfg)
	return cfg
}
