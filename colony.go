package beehive

import "encoding/json"

// Colony is the colony of bees that maintain a consistent state.
type Colony struct {
	Leader    uint64   `json:"leader"`
	Followers []uint64 `json:"followers"`
}

func (c Colony) IsLeader(id uint64) bool {
	return c.Leader == id
}

func (c Colony) IsFollower(id uint64) bool {
	for _, s := range c.Followers {
		if s == id {
			return true
		}
	}
	return false
}

func (c Colony) Contains(id uint64) bool {
	return c.IsLeader(id) || c.IsFollower(id)
}

func (c *Colony) AddFollower(id uint64) bool {
	if c.IsLeader(id) {
		return false
	}

	if c.IsFollower(id) {
		return false
	}

	c.Followers = append(c.Followers, id)
	return true
}

func (c *Colony) DelFollower(id uint64) bool {
	for i, s := range c.Followers {
		if s == id {
			c.Followers = append(c.Followers[:i], c.Followers[i+1:]...)
			return true
		}
	}

	return false
}

func (c Colony) DeepCopy() Colony {
	f := make([]uint64, len(c.Followers))
	copy(f, c.Followers)
	c.Followers = f
	return c
}

func (c Colony) Equal(thatC Colony) bool {
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

//func (c *Colony) FollowerHives() []HiveID {
//f:= make([]HiveID, 0, len(c.Followers))
//for _, s := range c.Followers {
//f= append(f, s.HiveID)
//}
//return f
//}

func (c *Colony) Bytes() ([]byte, error) {
	j, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func ColonyFromBytes(b []byte) (Colony, error) {
	c := Colony{}
	err := json.Unmarshal(b, &c)
	return c, err
}
