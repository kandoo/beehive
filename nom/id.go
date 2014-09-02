package nom

import "strings"

// UID is a unique ID of a NOM object. Unlike UUID/GUID, this ID contains
// redundant information about the object. For example a port's UID contains its
// network and node ID along with an ID for the port.
type UID string

// UIDSeparator is the token added in between the parts of a UID.
const UIDSeparator = "$$"

// UIDJoin joins an array of IDs into a UID.
func UIDJoin(ids ...string) UID {
	return UID(strings.Join(ids, UIDSeparator))
}

// UIDSplit splits a UID into its smaller IDs.
func UIDSplit(id UID) []string {
	return strings.Split(string(id), UIDSeparator)
}
