package compiler

import (
	"errors"
	"fmt"
	"go/ast"
)

func typeStr(exp ast.Expr) (string, error) {
	switch t := exp.(type) {
	case *ast.Ident:
		return t.Name, nil

	case *ast.StarExpr:
		name, err := typeStr(t.X)
		if err != nil {
			return "", err
		}
		return name, nil

	case *ast.SelectorExpr:
		pkg, err := typeStr(t.X)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s.%s", pkg, t.Sel.Name), nil
	}

	return "", errors.New("Expression is not a type.")
}

func str(expr ast.Expr) (string, error) {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name, nil
	case *ast.SelectorExpr:
		xID, err := str(e.X)
		if err != nil {
			return "", err
		}

		sID, err := str(e.Sel)
		if err != nil {
			return "", err
		}

		return xID + "." + sID, nil
	case *ast.BasicLit:
		return e.Value, nil
	}

	return "", errors.New(fmt.Sprintf("Is not an ID: %+v", expr))
}

type idVisitor struct {
	rIDs map[string]ast.Node
	wIDs map[string]ast.Node
}

func idMapValues(ids map[string]ast.Node) []string {
	v := make([]string, 0, len(ids))
	for k, _ := range ids {
		v = append(v, k)
	}
	return v
}

func (v *idVisitor) Visit(n ast.Node) (w ast.Visitor) {
	switch node := n.(type) {
	case *ast.Ident, *ast.SelectorExpr:
		id, err := str(node.(ast.Expr))
		if err == nil {
			v.rIDs[id] = n
		}
	case *ast.StarExpr:
		ast.Walk(v, node.X)
		return nil
	case *ast.CallExpr:
		stmt, ok := node.Fun.(*ast.SelectorExpr)
		if ok {
			ast.Walk(v, stmt.X)
		}

		if node.Args == nil {
			return nil
		}

		for _, arg := range node.Args {
			ast.Walk(v, arg)
		}
		return nil
	case *ast.AssignStmt:
		for _, e := range node.Lhs {
			id, err := str(e)
			if err != nil {
				return
			}
			v.wIDs[id] = n
		}

		for _, e := range node.Rhs {
			ast.Walk(v, e)
		}
		return nil
	case *ast.GenDecl:
		for _, s := range node.Specs {
			val, ok := s.(*ast.ValueSpec)
			if !ok {
				continue
			}

			for _, n := range val.Names {
				id, _ := str(n)
				v.wIDs[id] = node
			}
		}
	}
	w = v
	return
}

func ids(s ast.Stmt) ([]string, []string) {
	v := idVisitor{
		rIDs: make(map[string]ast.Node),
		wIDs: make(map[string]ast.Node),
	}

	ast.Walk(&v, s)
	return idMapValues(v.rIDs), idMapValues(v.wIDs)
}
