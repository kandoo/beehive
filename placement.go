package beehive

import "math/rand"

// PlacementMethod represents a placement algorithm that chooses a hive among
// live hives for the given mapped cells. This interface is used only for the
// first message that is mapped to those cells.
//
// The elected hive might go down, while the system is assigning the mapped
// cells to it. In such a case, the message will be placed locally after
// receiving an error.
type PlacementMethod interface {
	// Place returns the metadata of the hive chosen for cells. cells is the
	// mapped cells of a message according to the map function of the
	// application's message handler. thisHive is the local hive and liveHives
	// contains the meta data about live hives. Note that liveHives contains
	// thisHive.
	Place(cells MappedCells, thisHive Hive, liveHives []HiveInfo) HiveInfo
}

// RandomPlacement is a placement method that place mapped cells on a random
// hive.
type RandomPlacement struct {
	*rand.Rand
}

func (r RandomPlacement) Place(cells MappedCells, thisHive Hive,
	liveHives []HiveInfo) HiveInfo {

	return liveHives[r.Intn(len(liveHives))]
}
