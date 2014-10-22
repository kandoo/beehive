package beehive

import "os"

func removeState(cfg HiveConfig) {
	os.RemoveAll(cfg.StatePath)
}

func waitTilStareted(h Hive) {
	pingHive(h)
}

func pingHive(h Hive) {
	h.(*hive).sendCmd(cmdPingHive{})
}
