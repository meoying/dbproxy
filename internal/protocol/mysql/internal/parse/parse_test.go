package parse

import "testing"

func TestParse(t *testing.T) {
	Parse("select * from xx where id=1")
}
