package expression

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
	"testing"
)

var (
	co0 = &Column{RetType: &types.FieldType{Tp: mysql.TypeVarchar, Flen: 10, Decimal: 0, Charset: charset.CharsetBin, Collate: charset.CollationBin}, Index: 0}
	co1 = &Column{RetType: &types.FieldType{Tp: mysql.TypeNewDecimal, Flen: 2, Decimal: 0, Charset: charset.CharsetBin, Collate: charset.CollationBin}, Index: 1}
	co2 = &Column{RetType: &types.FieldType{Tp: mysql.TypeVarchar, Flen: 8, Decimal: 0, Charset: charset.CharsetBin, Collate: charset.CollationBin}, Index: 2}
	co3 = &Column{RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flen: 6, Decimal: 0, Charset: charset.CharsetBin, Collate: charset.CollationBin}, Index: 3}

	daycon = &Constant{RetType: &types.FieldType{Tp: mysql.TypeVarchar, Flen: 10, Decimal: 0, Charset: charset.CharsetBin, Collate: charset.CollationBin}, Value: types.NewStringDatum("DAY")}
	d0     = types.NewStringDatum("2017-01-01 12:12:12")
	d1     = types.NewDecimalDatum(types.NewDecFromStringForTest("31"))
	d2     = types.NewStringDatum("2017-01-02 12:30:50")
	d3     = types.NewIntDatum(20170202100101)

	row2 = []types.Datum{d0, d1, d2, d3}
)

// expr:  SUBDATE( ADDDATE(co0, co1), co0)
// expr1: ADDTIME(expr, TIMEDIFF(co2, co3))
// expr2: SUBTIME(expr1, co2)
// cast1: cast(expr1 as char(20))
// cast2: cast(expr2 as signed)
// expr3: TIMEDIFF(cast1, cast2)
func buildTimeFuncNew() Expression {
	sc := ctx.GetSessionVars().StmtCtx
	sc.IgnoreTruncate = true
	ctx.GetSessionVars().StrictSQLMode = false
	unspecifiedType := types.NewFieldType(0)
	adddate, err := NewFunction(ctx, ast.AddDate, unspecifiedType, co0, co1, daycon)
	if err != nil {
		panic(err.Error())
	}
	expr, err := NewFunction(ctx, ast.SubDate, unspecifiedType, adddate, co0, daycon)
	if err != nil {
		panic(err.Error())
	}
	timediff, err := NewFunction(ctx, ast.TimeDiff, unspecifiedType, co2, co3)
	if err != nil {
		panic(err.Error())
	}
	expr1, err := NewFunction(ctx, ast.AddTime, unspecifiedType, expr, timediff)
	if err != nil {
		panic(err.Error())
	}
	expr2, err := NewFunction(ctx, ast.SubTime, unspecifiedType, expr1, co2)
	if err != nil {
		panic(err.Error())
	}
	cast1 := NewCastFunc(&types.FieldType{Tp: mysql.TypeString, Flen: 20, Decimal: -1, Charset: charset.CharsetBin, Collate: charset.CollationBin}, expr1, ctx)
	cast2 := NewCastFunc(&types.FieldType{Tp: mysql.TypeLonglong, Flen: 20, Decimal: -1, Charset: charset.CharsetBin, Collate: charset.CollationBin}, expr2, ctx)
	expr3, err := NewFunction(ctx, ast.TimeDiff, unspecifiedType, cast1, cast2)
	if err != nil {
		panic(err.Error())
	}
	return expr3
}

func buildTimeFuncOld() Expression {
	sc := ctx.GetSessionVars().StmtCtx
	sc.IgnoreTruncate = true
	ctx.GetSessionVars().StrictSQLMode = false
	unspecifiedType := types.NewFieldType(0)
	adddate, err := NewFunction(ctx, ast.AddDate1, unspecifiedType, co0, co1, daycon)
	if err != nil {
		panic(err.Error())
	}
	expr, err := NewFunction(ctx, ast.SubDate1, unspecifiedType, adddate, co0, daycon)
	if err != nil {
		panic(err.Error())
	}
	timediff, err := NewFunction(ctx, ast.TimeDiff1, unspecifiedType, co2, co3)
	if err != nil {
		panic(err.Error())
	}
	expr1, err := NewFunction(ctx, ast.AddTime1, unspecifiedType, expr, timediff)
	if err != nil {
		panic(err.Error())
	}
	expr2, err := NewFunction(ctx, ast.SubTime1, unspecifiedType, expr1, co2)
	if err != nil {
		panic(err.Error())
	}

	tp1 := &types.FieldType{Tp: mysql.TypeString, Flen: 20, Decimal: -1, Charset: charset.CharsetBin, Collate: charset.CollationBin}
	bt1 := &builtinCastSig{newBaseBuiltinFunc([]Expression{expr1}, ctx), tp1}
	cast1 := &ScalarFunction{
		FuncName: model.NewCIStr(ast.Cast),
		RetType:  tp1,
		Function: bt1,
	}
	tp2 := &types.FieldType{Tp: mysql.TypeLonglong, Flen: 20, Decimal: -1, Charset: charset.CharsetBin, Collate: charset.CollationBin}
	bt2 := &builtinCastSig{newBaseBuiltinFunc([]Expression{expr2}, ctx), tp2}
	cast2 := &ScalarFunction{
		FuncName: model.NewCIStr(ast.Cast),
		RetType:  tp2,
		Function: bt2,
	}
	expr3, err := NewFunction(ctx, ast.TimeDiff1, unspecifiedType, cast1, cast2)
	if err != nil {
		panic(err.Error())
	}
	return expr3
}

func BenchmarkTimeFunctionNew(b *testing.B) {
	ctx.GetSessionVars().StrictSQLMode = false
	ctx.GetSessionVars().StmtCtx.IgnoreTruncate = true
	expr := buildTimeFuncNew()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := expr.Eval(row2)
		if err != nil {
			return
		}
	}
}

func BenchmarkTimeFunctionOld(b *testing.B) {
	ctx.GetSessionVars().StrictSQLMode = false
	ctx.GetSessionVars().StmtCtx.IgnoreTruncate = true
	expr := buildTimeFuncOld()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := expr.Eval(row2)
		if err != nil {
			return
		}
	}
}
