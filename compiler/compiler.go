package compiler

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"os"
	"path"
)

var (
	ErrNoHandler = errors.New("compiler: no handler provided")
)

// Represents a message handler.
type Handler struct {
	Type    ast.Expr          // Type of the handler.
	Rcv     *ast.FuncDecl     // Rcv function of the handler. Cannot be nil.
	Map     *ast.FuncDecl     // Map function of the handler. Can be nil.
	Imports map[string]string // Packages that must be imported for mapping.
	Package string            // Package name of this handler.
}

func HandlersInDir(fset *token.FileSet, path string) ([]*Handler, error) {
	pkgs, err := parser.ParseDir(fset, path, isGo, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	res := make([]*Handler, 0)
	for _, pkg := range pkgs {
		handlers, err := HandlersInPackage(pkg)
		if err != nil {
			return nil, err
		}

		for _, h := range handlers {
			res = append(res, h)
		}
	}
	return res, nil
}

// Finds all the event handlers declared in package.
func HandlersInPackage(pkg *ast.Package) ([]*Handler, error) {
	fmt.Printf("Parsing package %s\n", pkg.Name)
	handlers := make([]*Handler, 0)
	handlerMap := make(map[string]*Handler)

	for name, file := range pkg.Files {
		fmt.Printf("Processing file %s\n", name)
		imports := importNames(file.Imports)
		for _, decl := range file.Decls {
			switch fn := decl.(type) {
			case *ast.FuncDecl:
				if isGenerated(fn) {
					continue
				}

				isR := isRcv(fn, imports)
				isM := isMap(fn, imports)

				if !isR && !isM {
					continue
				}

				rcvr, err := qualifiedTypeStr(fn.Recv.List[0].Type, imports)
				if err != nil {
					continue
				}
				h := handlerMap[rcvr]
				if h == nil {
					h = &Handler{
						Type:    fn.Recv.List[0].Type,
						Package: pkg.Name,
						Imports: imports,
					}
					handlerMap[rcvr] = h
					handlers = append(handlers, h)
				}

				switch {
				case isR:
					h.Rcv = fn
				case isM:
					h.Map = fn
				}
			}
		}
	}

	return handlers, nil
}

// Generates map function for the given handlers into the writer. Handlers must
// be all of the same package.
func GenerateMap(w io.Writer, handlers []*Handler) error {
	if len(handlers) == 0 {
		return ErrNoHandler
	}

	var fileBuf bytes.Buffer
	var pkg string
	for i, h := range handlers {
		if i == 0 {
			pkg = h.Package
			continue
		}

		if pkg != h.Package {
			return errors.New(
				fmt.Sprintf("Handlers of from two different packages: %s and %s",
					pkg, h.Package))
		}
	}

	fmt.Fprintf(&fileBuf, "package %s\n\n", pkg)

	fset := token.NewFileSet()
	var mapBuf bytes.Buffer
	for _, h := range handlers {
		if h.Map != nil {
			continue
		}

		h = generateMapFromRcv(h)
		fmt.Fprintf(&mapBuf, "%s\n", generatedComment)
		printer.Fprint(&mapBuf, fset, h.Map)
		fmt.Fprintln(&mapBuf)
	}

	imported := make(map[string]bool)
	for _, h := range handlers {
		for _, p := range h.Imports {
			imported[p] = true
		}
	}

	if len(imported) > 0 {
		fmt.Fprint(&fileBuf, "import (\n")
		for p, _ := range imported {
			fmt.Fprintf(&fileBuf, "\t\"%s\"\n", p)
		}
		fmt.Fprint(&fileBuf, ")\n\n")
	}

	fmt.Fprint(&fileBuf, mapBuf.String())
	fmtBuf, err := format.Source(fileBuf.Bytes())
	if err != nil {
		return err
	}

	fmt.Fprint(w, string(fmtBuf))
	return nil
}

func isGo(f os.FileInfo) bool {
	return path.Ext(f.Name()) == ".go"
}
