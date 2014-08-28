package compiler

import (
	"go/ast"
	"strings"
)

const (
	recvFunc = "Recv"
	mapFunc  = "Map"

	msgType    = "actor.Msg"
	mapSetType = "actor.MapSet"

	rcvCtxType = "actor.RecvContext"
	mapCtxType = "actor.Context"

	dictFunc    = "Dict"
	dictGetFunc = "Get"
	dictSetFunc = "Set"

	generatedComment = "// __generated_by_beehive__"
)

func generateMapFromRecv(h *Handler) *Handler {
	m := h.Rcv
	m.Name = &ast.Ident{
		Name: mapFunc,
	}

	body, dks := filterBlock(m.Body, make(map[string]bool))

	retE := &ast.CompositeLit{
		Type: &ast.Ident{
			Name: mapSetType,
		},
	}

	for _, dk := range dks {
		dkExpr := &ast.CompositeLit{
			Elts: []ast.Expr{
				&ast.Ident{
					Name: dk.d,
				},
				&ast.Ident{
					Name: dk.k,
				},
			},
		}
		retE.Elts = append(retE.Elts, dkExpr)
	}

	body.List = append(body.List, &ast.ReturnStmt{Results: []ast.Expr{retE}})
	m.Body = body

	m.Type.Results = &ast.FieldList{
		List: []*ast.Field{
			&ast.Field{
				Type: &ast.Ident{
					Name: mapSetType,
				},
			},
		},
	}

	m.Type.Params.List[1].Type = &ast.Ident{
		Name: mapCtxType,
	}

	h.Map = m
	h.Imports = []string{"github.com/soheilhy/actor/actor"}
	return h
}

type dictKey struct {
	d string
	k string
}

// Keeps the statements that are need to calculate MapSet.
func filterBlock(blk *ast.BlockStmt, filterIDs map[string]bool) (*ast.BlockStmt,
	[]dictKey) {

	dicts := dictionaries(blk)

	usedDks := make(map[dictKey]bool)
	filtered := make([]ast.Stmt, 0, len(blk.List))
	for i := len(blk.List) - 1; i >= 0; i-- {
		switch s := blk.List[i].(type) {
		// TODO(soheil): Support switches.
		case *ast.SwitchStmt:
		// TODO(soheil): Add support for ifs.
		case *ast.IfStmt:
		default:
			// TODO(soheil): It's actually more complicated that. What about
			// functional calls, what about multiple return values, ...?
			dks, yes := accessesDictionary(s, dicts)
			if yes {
				for _, dk := range dks {
					filterIDs[dk.k] = true
					usedDks[dk] = true
				}
				continue
			}

			dirty := false
			rIDs, wIDs := ids(s)
			for _, id := range wIDs {
				if filterIDs[id] {
					dirty = true
					break
				}
			}

			if !dirty {
				continue
			}

			for _, id := range rIDs {
				filterIDs[id] = true
			}

			filtered = append([]ast.Stmt{s}, filtered...)
		}
	}

	blk.List = filtered

	keys := make([]dictKey, 0, len(usedDks))
	for dk, _ := range usedDks {
		keys = append(keys, dk)
	}
	return blk, keys
}

type dictVisitor struct {
	dicts map[string]string
	keys  []dictKey
}

func (v *dictVisitor) Visit(n ast.Node) (w ast.Visitor) {
	w = v
	switch node := n.(type) {
	case *ast.CallExpr:
		expr, ok := node.Fun.(*ast.SelectorExpr)
		if !ok {
			return
		}

		if expr.Sel.Name != dictGetFunc && expr.Sel.Name != dictSetFunc {
			return
		}

		isDict := false

		var dict string
		s, err := str(expr.X)
		if err == nil {
			dict, isDict = v.dicts[s]
		} else {
			c, ok := expr.X.(*ast.CallExpr)
			isDict = ok && isDictionary(c)

			dict, err = str(c.Args[0])
			if err != nil {
				return
			}
		}

		if !isDict {
			return
		}

		key, err := str(node.Args[0])
		if err == nil {
			v.keys = append(v.keys, dictKey{d: dict, k: key})
		}
	}

	return
}

func accessesDictionary(s ast.Stmt, dicts map[string]string) ([]dictKey, bool) {
	v := dictVisitor{
		dicts: dicts,
	}
	ast.Walk(&v, s)
	return v.keys, len(v.keys) > 0
}

// Returns the IDs of all dictionaries created in the block.
func dictionaries(block *ast.BlockStmt) map[string]string {
	dicts := make(map[string]string)
	for _, e := range block.List {
		switch s := e.(type) {
		case *ast.AssignStmt:
			if len(s.Rhs) == 0 {
				continue
			}

			for i, rhs := range s.Rhs {
				c, ok := rhs.(*ast.CallExpr)
				if !ok {
					continue
				}

				if !isDictionary(c) {
					continue
				}

				lhsID, err := str(s.Lhs[i])
				if err != nil {
					continue
				}
				arg, err := str(c.Args[0])
				if err != nil {
					continue
				}

				dicts[lhsID] = arg
			}
		}
	}
	return dicts
}

// Whether the function call returns a dictionary.
func isDictionary(call *ast.CallExpr) bool {
	if call == nil {
		return false
	}

	fn, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	// TODO(soheil): We should make this more generic. What if X is not an ident.
	x, ok := fn.X.(*ast.Ident)
	if !ok {
		return false
	}

	if x.Obj == nil {
		return false
	}

	switch d := x.Obj.Decl.(type) {
	case *ast.Field:
		t, err := typeStr(d.Type)
		if err != nil {
			return false
		}

		if t != rcvCtxType {
			return false
		}
	}

	return fn.Sel.Name == dictFunc
}

func isRcv(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || fn.Name.Name != recvFunc {
		return false
	}

	t := fn.Type
	if t.Results != nil && len(t.Results.List) != 0 {
		return false
	}

	if len(t.Params.List) != 2 {
		return false
	}

	if n, err := typeStr(t.Params.List[0].Type); err != nil || n != msgType {
		return false
	}

	if n, err := typeStr(t.Params.List[1].Type); err != nil || n != rcvCtxType {
		return false
	}

	return true
}

func isMap(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || fn.Name.Name != mapFunc {
		return false
	}

	t := fn.Type
	if t.Results == nil || len(t.Results.List) != 1 {
		return false
	}

	if n, err := typeStr(t.Results.List[0].Type); err != nil || n != mapSetType {
		return false
	}

	if len(t.Params.List) != 2 {
		return false
	}

	if n, err := typeStr(t.Params.List[0].Type); err != nil || n != msgType {
		return false
	}

	if n, err := typeStr(t.Params.List[1].Type); err != nil || n != mapCtxType {
		return false
	}

	return true
}

func isGenerated(fn *ast.FuncDecl) bool {
	if fn.Doc == nil {
		return false
	}

	for _, c := range fn.Doc.List {
		if strings.Contains(c.Text, generatedComment) {
			return true
		}
	}
	return false
}
