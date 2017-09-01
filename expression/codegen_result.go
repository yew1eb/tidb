package expression

func (c *Constant) CodegenGetResult() string {
	return c.codegenResult
}

func (col *Column) CodegenGetResult() string {
	return col.codegenResult
}

func (sf *ScalarFunction) CodegenGetResult() string {
	return sf.Function.codegenGetResult()
}

func (s *baseBuiltinFunc) codegenGetResult() string {
	return s.cgResult
}
