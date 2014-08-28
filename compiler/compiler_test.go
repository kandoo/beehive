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
		t.Errorf("Error in finding receivers: %v", err)
	}

	for _, h := range handlers {
		h.Map = nil
	}

	err = GenerateMap(os.Stdout, handlers)
	if err != nil {
		t.Errorf("Error in generating receivers: %v", err)
	}
}

// TODO(soheil): Add real tests.
