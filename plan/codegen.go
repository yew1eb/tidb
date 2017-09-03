package plan

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
)

func (bp *basePhysicalPlan) Codegen(cg *expression.Codegen) (string, error) {
	return "", errors.Errorf("codegen %T not supported", bp.basePlan.self)
}

func (p *Projection) Codegen(cg *expression.Codegen) (string, error) {
	outputTuple, err := cg.GenTuple(p.Schema(), p.ID())
	if err != nil {
		return "", errors.Trace(err)
	}

	inputTuple, err := p.Children()[0].(PhysicalPlan).Codegen(cg)
	if err != nil {
		return "", errors.Trace(err)
	}

	for i, expr := range p.Exprs {
		resultField, err := expr.Codegen(cg, inputTuple)
		if err != nil {
			return "", errors.Trace(err)
		}
		cg.Main.WriteString(fmt.Sprintf("memcpy(%s->fields + %s->offset[%v], %s, %s->size[%v]);\n", outputTuple, outputTuple, i, resultField, outputTuple, i))
	}

	for i, col := range p.Schema().Columns {
		switch col.GetType().Tp {
		case mysql.TypeLonglong:
			cg.Main.WriteString(fmt.Sprintf("printf(\"%s\", GetBigintFromTuple(%s, %v)->value);\n", "%lld", outputTuple, i))
		case mysql.TypeDouble:
			cg.Main.WriteString(fmt.Sprintf("printf(\"%s\", GetDoubleFromTuple(%s, %v)->value);\n", "%llf", outputTuple, i))
		default:
			return "", errors.Errorf("unsupported type in %T.Codegen()", p)
		}
		if i+1 < len(p.Schema().Columns) {
			cg.Main.WriteString("printf(\" \");\n")
		} else {
			cg.Main.WriteString("printf(\"\\n\");\n")
		}
	}

	return outputTuple, nil
}

func (p *PhysicalTableReader) Codegen(cg *expression.Codegen) (string, error) {
	outputTuple, err := cg.GenTuple(p.Schema(), p.ID())
	if err != nil {
		return "", errors.Trace(err)
	}
	for i, col := range p.Schema().Columns {
		switch col.GetType().Tp {
		case mysql.TypeLonglong:
			cg.Main.WriteString(fmt.Sprintf("SetBigint2Tuple(%s, %v, MockGetBigintFromDataSource());\n", outputTuple, i))
		case mysql.TypeDouble:
			cg.Main.WriteString(fmt.Sprintf("SetDouble2Tuple(%s, %v, MockGetDoubleFromDataSource());\n", outputTuple, i))
		default:
			return "", errors.Errorf("unsupported type in %T.Codegen()", p)
		}
	}
	return outputTuple, nil
}
