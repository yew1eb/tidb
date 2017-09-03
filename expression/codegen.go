package expression

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
)

func (c *Constant) Codegen(cg *Codegen, inputTuple string) (string, error) {
	switch c.RetType.Tp {
	case mysql.TypeLonglong:
		c.codegenResult = cg.GenConstantBigint(c.Value.GetInt64())
	case mysql.TypeDouble:
		c.codegenResult = cg.GenConstantDouble(c.Value.GetFloat64())
	default:
		return "", errors.Errorf("unsupported constant type [%s] for codegen", c.RetType)
	}
	return c.codegenResult, nil
}

func (col *Column) Codegen(cg *Codegen, inputTuple string) (string, error) {
	col.codegenResult = fmt.Sprintf("column_%s", cg.AllocateID())
	switch col.GetType().Tp {
	case mysql.TypeLonglong:
		cg.Init.WriteString(fmt.Sprintf("FieldBigint* %s = GetBigintFromTuple(%s, %v);\n", col.codegenResult, inputTuple, col.Index))
	case mysql.TypeDouble:
		cg.Init.WriteString(fmt.Sprintf("FieldDouble* %s = GetDoubleFromTuple(%s, %v);\n", col.codegenResult, inputTuple, col.Index))
	default:
		return "", errors.Errorf("unsupported column type [%s] for codegen", col.GetType())
	}
	return col.codegenResult, nil
}

func (sf *ScalarFunction) Codegen(cg *Codegen, inputTuple string) (string, error) {
	resultField, err := sf.Function.codegen(cg, inputTuple)
	if err != nil {
		return "", errors.Trace(err)
	}
	return resultField, err
}

func (s *baseBuiltinFunc) codegen(cg *Codegen, inputTuple string) (string, error) {
	return "", errors.Errorf("baseBuiltinFunc.codegen is not supported.")
}

func (s *builtinArithmeticPlusIntSig) codegen(cg *Codegen, inputTuple string) (string, error) {
	lhs, err := s.args[0].Codegen(cg, inputTuple)
	if err != nil {
		return "", errors.Trace(err)
	}

	rhs, err := s.args[1].Codegen(cg, inputTuple)
	if err != nil {
		return "", errors.Trace(err)
	}

	s.cgResult = fmt.Sprintf("field_result_func_plus_%s", cg.AllocateID())
	cg.Init.WriteString(fmt.Sprintf("FieldBigint* %s = (FieldBigint*)malloc(sizeof(FieldBigint));\n", s.cgResult))
	cg.Clear.WriteString(fmt.Sprintf("free(%s);\n", s.cgResult))
	cg.Main.WriteString(fmt.Sprintf("PlusBigint(%s, %s, %s);\n", lhs, rhs, s.cgResult))
	return s.cgResult, nil
}

func (s *builtinArithmeticMinusIntSig) codegen(cg *Codegen, inputTuple string) (string, error) {
	lhs, err := s.args[0].Codegen(cg, inputTuple)
	if err != nil {
		return "", errors.Trace(err)
	}

	rhs, err := s.args[1].Codegen(cg, inputTuple)
	if err != nil {
		return "", errors.Trace(err)
	}

	s.cgResult = fmt.Sprintf("field_result_func_minus_%s", cg.AllocateID())
	cg.Init.WriteString(fmt.Sprintf("FieldBigint* %s = (FieldBigint*)malloc(sizeof(FieldBigint));\n", s.cgResult))
	cg.Clear.WriteString(fmt.Sprintf("free(%s);\n", s.cgResult))
	cg.Main.WriteString(fmt.Sprintf("MinusBigint(%s, %s, %s);\n", lhs, rhs, s.cgResult))
	return s.cgResult, nil
}
