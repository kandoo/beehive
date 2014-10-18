package beehive

import (
	"os"
	"testing"
)

func TestHiveIDFromPeers(t *testing.T) {
	if id := hiveIDFromPeers(nil); id != 1 {
		t.Errorf("%v is not a valid default hive ID", id)
	}
}

func TestDefaultMeta(t *testing.T) {
	cfg := HiveConfig{
		StatePath: "/tmp/metatest/",
	}
	os.Mkdir(cfg.StatePath, 0700)
	defer os.RemoveAll(cfg.StatePath)
	m := meta(cfg)
	if m.Hive.ID != 1 {
		t.Errorf("%v is not a valid default hive ID", m.Hive.ID)
	}

	m = meta(cfg)
	if m.Hive.ID != 1 {
		t.Errorf("%v is not a valid default hive ID", m.Hive.ID)
	}
}
