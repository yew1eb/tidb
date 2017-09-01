package plan

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
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
		cg.Main.WriteString(fmt.Sprintf("memcpy(%s->offset[%v], %s, %s->size[%v]);\n", outputTuple, i, resultField, outputTuple, i))
	}

	return outputTuple, nil
}

func (p *PhysicalTableReader) Codegen(cg *expression.Codegen) (string, error) {
	outputTuple, err := cg.GenTuple(p.Schema(), p.ID())
	if err != nil {
		return "", errors.Trace(err)
	}

	return outputTuple, nil
}
