package strings

import (
	"bytes"
	"fmt"
)

// Join is a more generic version of the standard strings.Join and accepts
// an array of interfac{}'s. Each element is formatted using "%v".
func Join(a []interface{}, sep string) string {
	var buf bytes.Buffer
	l := len(a)
	for i := range a {
		buf.WriteString(fmt.Sprintf("%v", a[i]))
		if i == l-1 {
			break
		}
		buf.WriteString(",")
	}
	return buf.String()
}
