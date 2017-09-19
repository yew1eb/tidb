package expression

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
	"github.com/ngaut/log"

	"testing"
)

var (
	c0 = &Column{
		RetType: &types.FieldType{Tp: mysql.TypeString, Flen: 20, Decimal: -1},
		Index:   0,
	}
	c1 = &Column{
		RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flen: mysql.MaxIntWidth, Decimal: 0},
		Index:   1,
	}
	c2 = &Column{
		RetType: &types.FieldType{Tp: mysql.TypeString, Flen: 20, Decimal: -1},
		Index:   2,
	}
	c3 = &Column{
		RetType: &types.FieldType{Tp: mysql.TypeNewDecimal, Flen: 20, Decimal: -1},
		Index:   3,
	}
	c4 = &Column{
		RetType: &types.FieldType{Tp: mysql.TypeString, Flen: 20, Decimal: -1},
		Index:   3,
	}

	datum0 = types.NewStringDatum("123.321")
	datum1 = types.NewIntDatum(321)
	datum2 = types.NewStringDatum("456.789")
	datum3 = types.NewDecimalDatum(types.NewDecFromStringForTest("789.6666"))
	datum4 = types.NewStringDatum("890.12")

	row = []types.Datum{datum0, datum1, datum2, datum3, datum4}

	ctx = mock.NewContext()
)

// expr: (string + int) * string / (decimal - string)
// cast1: cast(expr as char(20))
// expr1: cast1 INTDIV expr
// cast2: cast(expr1 as char(20))
// cast3: cast(expr1 as decimal)
// expr2: cast2 = cast3
func buildNewFunction() Expression {
	sc := ctx.GetSessionVars().StmtCtx
	sc.IgnoreTruncate = true
	unspecifiedType := types.NewFieldType(0)
	plus, err := NewFunction(ctx, ast.Plus, unspecifiedType, c0, c1)
	if err != nil {
		panic("line57")
	}
	mul, err := NewFunction(ctx, ast.Mul, unspecifiedType, plus, c2)
	if err != nil {
		panic("line61")
	}
	minus, err := NewFunction(ctx, ast.Minus, unspecifiedType, c3, c4)
	if err != nil {
		panic("line63")
	}
	expr, err := NewFunction(ctx, ast.Div, unspecifiedType, mul, minus)
	if err != nil {
		panic("line69")
	}
	cast1 := NewCastFunc(&types.FieldType{Tp: mysql.TypeString, Flen: 20, Decimal: -1, Charset: charset.CharsetBin, Collate: charset.CollationBin}, expr, ctx)
	expr1, err := NewFunction(ctx, ast.IntDiv, unspecifiedType, cast1, expr)
	if err != nil {
		panic("line74")
	}
	cast2 := NewCastFunc(&types.FieldType{Tp: mysql.TypeString, Flen: 20, Decimal: -1, Charset: charset.CharsetBin, Collate: charset.CollationBin}, expr1, ctx)
	cast3 := NewCastFunc(&types.FieldType{Tp: mysql.TypeNewDecimal, Charset: charset.CharsetBin, Collate: charset.CollationBin}, expr1, ctx)
	expr2, err := NewFunction(ctx, ast.EQ, unspecifiedType, cast2, cast3)
	if err != nil {
		panic("line79")
	}
	return expr2
}

func buildOldFunction() Expression {
	sc := ctx.GetSessionVars().StmtCtx
	sc.IgnoreTruncate = true
	unspecifiedType := types.NewFieldType(0)
	plus, err := NewFunction(ctx, ast.Plus1, unspecifiedType, c0, c1)
	if err != nil {
		panic("line89")
	}
	mul, err := NewFunction(ctx, ast.Mul1, unspecifiedType, plus, c2)
	if err != nil {
		panic("line93")
	}
	minus, err := NewFunction(ctx, ast.Minus1, unspecifiedType, c3, c4)
	if err != nil {
		panic("line97")
	}
	expr, err := NewFunction(ctx, ast.Div1, unspecifiedType, mul, minus)
	if err != nil {
		panic("line101")
	}
	tp1 := &types.FieldType{Tp: mysql.TypeString, Flen: 20, Decimal: -1, Charset: charset.CharsetBin, Collate: charset.CollationBin}
	bt1 := &builtinCastSig{newBaseBuiltinFunc([]Expression{expr}, ctx), tp1}
	cast1 := &ScalarFunction{
		FuncName: model.NewCIStr(ast.Cast),
		RetType:  tp1,
		Function: bt1,
	}
	expr1, err := NewFunction(ctx, ast.IntDiv1, unspecifiedType, cast1, expr)
	if err != nil {
		panic("line112")
	}
	tp2 := &types.FieldType{Tp: mysql.TypeString, Flen: 20, Decimal: -1, Charset: charset.CharsetBin, Collate: charset.CollationBin}
	bt2 := &builtinCastSig{newBaseBuiltinFunc([]Expression{expr1}, ctx), tp2}
	cast2 := &ScalarFunction{
		FuncName: model.NewCIStr(ast.Cast),
		RetType:  tp2,
		Function: bt2,
	}
	tp3 := &types.FieldType{Tp: mysql.TypeNewDecimal, Charset: charset.CharsetBin, Collate: charset.CollationBin}
	bt3 := &builtinCastSig{newBaseBuiltinFunc([]Expression{expr1}, ctx), tp3}
	cast3 := &ScalarFunction{
		FuncName: model.NewCIStr(ast.Cast),
		RetType:  tp3,
		Function: bt3,
	}
	expr2, err := NewFunction(ctx, ast.EQ1, unspecifiedType, cast2, cast3)
	if err != nil {
		panic("line130")
	}
	return expr2
}

func BenchmarkFunctionNew(b *testing.B) {
	expr := buildNewFunction()
	b.ResetTimer()
	for i:=0; i<b.N; i++{
		_, err := expr.Eval(row)
		if err != nil {
			log.Warning(err)
			return
		}
	}
}

func BenchmarkFunctionOld(b *testing.B) {
	expr := buildOldFunction()
	b.ResetTimer()
	for i:=0; i<b.N; i++{
		_, err := expr.Eval(row)
		if err != nil {
			log.Warning(err)
			return
		}
	}
}

