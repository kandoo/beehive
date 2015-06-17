package compiler

import (
	"go/token"
	"os"
	"testing"
)

func TestCompiler(t *testing.T) {
	fset := token.NewFileSet()
	handlers, err := HandlersInDir(fset, "../examples/te")
	if err != nil {
		t.Errorf("error in finding receivers: %v", err)
	}

	if len(handlers) == 0 {
		t.Fatalf("no hanlders found for TE")
	}

	for _, h := range handlers {
		h.Map = nil
	}

	err = GenerateMap(os.Stdout, handlers)
	if err != nil {
		t.Errorf("error in generating receivers: %v", err)
	}
}

// TODO(soheil): Add real tests.
