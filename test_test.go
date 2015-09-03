package beehive

import (
	"fmt"
	"os"
)

func removeState(path string) {
	os.RemoveAll(path)
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

func newHiveForTest(opts ...HiveOption) Hive {
	testPort++
	addr := fmt.Sprintf("127.0.0.1:%v", testPort)
	path := fmt.Sprintf("/tmp/bhtest-%v", testPort)
	removeState(path)
	opts = append(opts, Addr(addr))
	opts = append(opts, StatePath(path))
	return NewHive(opts...)
}
