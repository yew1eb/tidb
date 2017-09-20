package expression

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
	"testing"
)

var (
	col0 = &Column{RetType: &types.FieldType{Tp: mysql.TypeVarchar, Flen: 7, Decimal: -1, Charset: charset.CharsetBin, Collate: charset.CollationBin}, Index: 0}
	col1 = &Column{RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flen: 1, Decimal: 0, Charset: charset.CharsetBin, Collate: charset.CollationBin}, Index: 1}
	col2 = &Column{RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flen: 1, Decimal: 0, Charset: charset.CharsetBin, Collate: charset.CollationBin}, Index: 2}
	col3 = &Column{RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flen: 1, Decimal: 0, Charset: charset.CharsetBin, Collate: charset.CollationBin}, Index: 3}
	col4 = &Column{RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flen: 7, Decimal: -1, Charset: charset.CharsetBin, Collate: charset.CollationBin}, Index: 4}

	dat0 = types.NewStringDatum("abcdefg")
	dat1 = types.NewIntDatum(4)
	dat2 = types.NewIntDatum(0)
	dat3 = types.NewIntDatum(1)
	dat4 = types.NewStringDatum("hijklmn")

	row3 = []types.Datum{dat0, dat1, dat2, dat3, dat4}
)

// expr:  hex(left(string, int))
// expr1: insert(expr, int, int, string)
// expr2: lpad(expr1, int, string)
// expr3: instr(expr2, string)
func buildStringFunctionNew() Expression {
	unspecifiedType := types.NewFieldType(0)
	left, err := NewFunction(ctx, ast.Left, unspecifiedType, col0, col1)
	if err != nil {
		panic(err.Error())
	}
	expr, err := NewFunction(ctx, ast.Hex, unspecifiedType, left)
	if err != nil {
		panic(err.Error())
	}
	expr1, err := NewFunction(ctx, ast.InsertFunc, unspecifiedType, expr, col2, col3, col4)
	if err != nil {
		panic(err.Error())
	}
	expr2, err := NewFunction(ctx, ast.Lpad, unspecifiedType, expr1, col2, col0)
	if err != nil {
		panic(err.Error())
	}
	expr3, err := NewFunction(ctx, ast.Instr, unspecifiedType, expr2, col0)
	if err != nil {
		panic(err.Error())
	}
	return expr3
}

func buildStringFunctionOld() Expression {
	unspecifiedType := types.NewFieldType(0)
	left, err := NewFunction(ctx, ast.Left1, unspecifiedType, col0, col1)
	if err != nil {
		panic(err.Error())
	}
	expr, err := NewFunction(ctx, ast.Hex1, unspecifiedType, left)
	if err != nil {
		panic(err.Error())
	}
	expr1, err := NewFunction(ctx, ast.InsertFunc1, unspecifiedType, expr, col2, col3, col4)
	if err != nil {
		panic(err.Error())
	}
	expr2, err := NewFunction(ctx, ast.Lpad1, unspecifiedType, expr1, col2, col4)
	if err != nil {
		panic(err.Error())
	}
	expr3, err := NewFunction(ctx, ast.Instr1, unspecifiedType, expr2, col0)
	if err != nil {
		panic(err.Error())
	}
	return expr3
}

func BenchmarkStringFunctionNew(b *testing.B) {
	expr := buildStringFunctionNew()
	for i := 0; i < b.N; i++ {
		_, err := expr.Eval(row3)
		if err != nil {
			log.Warning(errors.ErrorStack(err))
			return
		}
	}
}

func BenchmarkStringFunctionOld(b *testing.B) {
	expr := buildStringFunctionOld()
	for i := 0; i < b.N; i++ {
		_, err := expr.Eval(row3)
		if err != nil {
			log.Warning(errors.ErrorStack(err))
			return
		}
	}
}
