package beehive

import "os"

func removeState(cfg HiveConfig) {
	os.RemoveAll(cfg.StatePath)
}

func waitTilStareted(h Hive) {
	h.(*hive).sendCmd(cmdPingHive{})
}
