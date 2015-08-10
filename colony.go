package beehive

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
)

const (
	Nil uint64 = 0
)

// Colony is the colony of bees that maintain a consistent state.
type Colony struct {
	ID        uint64   `json:"id"`
	Leader    uint64   `json:"leader"`
	Followers []uint64 `json:"followers"`
}

func (c Colony) String() string {
	return fmt.Sprintf("colony(id=%v, leader=%v, followers=%+v)", c.ID, c.Leader,
		c.Followers)
}

// IsNil returns whether the colony does not represent a valid colony.
func (c Colony) IsNil() bool {
	return c.ID == Nil && c.Leader == Nil && len(c.Followers) == 0
}

// IsLeader returns whether id is the leader of this colony.
func (c Colony) IsLeader(id uint64) bool {
	return c.Leader == id
}

// IsFollower retursn whether id is the follower in this colony.
func (c Colony) IsFollower(id uint64) bool {
	for _, s := range c.Followers {
		if s == id {
			return true
		}
	}
	return false
}

// Contains returns whether id is the leader or a follower in this colony.
func (c Colony) Contains(id uint64) bool {
	return c.IsLeader(id) || c.IsFollower(id)
}

// AddFollower adds a follower to the colony. Returns false if id is already a
// follower.
func (c *Colony) AddFollower(id uint64) bool {
	if id == Nil {
		return false
	}

	if c.IsLeader(id) {
		return false
	}

	if c.IsFollower(id) {
		return false
	}

	c.Followers = append(c.Followers, id)
	return true
}

// DelFollower deletes id from the followers of this colony. Returns false if
// id is not already a follower.
func (c *Colony) DelFollower(id uint64) bool {
	for i, s := range c.Followers {
		if s == id {
			c.Followers = append(c.Followers[:i], c.Followers[i+1:]...)
			return true
		}
	}

	return false
}

// DeepCopy creates a cloned copy of the colony.
func (c Colony) DeepCopy() Colony {
	f := make([]uint64, len(c.Followers))
	copy(f, c.Followers)
	c.Followers = f
	return c
}

// Equals return whether c is equal to thatC.
func (c Colony) Equals(thatC Colony) bool {
	if c.ID != thatC.ID {
		return false
	}

	if c.Leader != thatC.Leader {
		return false
	}

	if len(c.Followers) != len(thatC.Followers) {
		return false
	}

	if len(c.Followers) == 0 && len(thatC.Followers) == 0 {
		return true
	}

	f := make(map[uint64]bool)
	for _, b := range c.Followers {
		f[b] = true
	}

	for _, b := range thatC.Followers {
		if _, ok := f[b]; !ok {
			return false
		}
	}

	return true
}

// Bytes returns the []byte representation of this colony.
func (c *Colony) Bytes() ([]byte, error) {
	j, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return j, nil
}

// ColonyFromBytes creates a colony from its []byte representation.
func ColonyFromBytes(b []byte) (Colony, error) {
	c := Colony{}
	err := json.Unmarshal(b, &c)
	return c, err
}

func init() {
	gob.Register(Colony{})
}
