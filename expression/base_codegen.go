package expression

import (
	"bytes"
	"fmt"
	"os"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
)

type Codegen struct {
	id       int // allocated for variables, functions...
	Header   *bytes.Buffer
	Variable *bytes.Buffer
	Function *bytes.Buffer
	Init     *bytes.Buffer
	Clear    *bytes.Buffer
	Main     *bytes.Buffer
}

func (cg *Codegen) Finalize() error {
	file, err := os.Create("main.c")
	if err != nil {
		return errors.Trace(err)
	}

	file.WriteString("// headers\n")
	file.WriteString("#include \"expression/mathematical/operators.h\"\n")
	file.WriteString("#include \"type/tuple.h\"\n")
	file.WriteString("#include \"mock/data_source.h\"\n")
	file.WriteString("#include \"type/type.h\"\n")
	file.WriteString("#include <stdlib.h>\n")
	file.WriteString("#include <string.h>\n")
	file.WriteString("#include <stdio.h>\n")
	file.Write(cg.Header.Bytes())

	file.WriteString("int main() {\n")
	file.WriteString("// variable\n")
	file.Write(cg.Variable.Bytes())
	file.WriteString("\n")

	//file.WriteString("// variables\n")
	//file.Write(cg.Function.Bytes())
	//file.WriteString("\n")

	file.WriteString("// initialize\n")
	file.Write(cg.Init.Bytes())
	file.WriteString("\n")

	file.WriteString("// compute logical\n")
	file.WriteString("while (MockHasMoreTuple()) {\n")
	file.Write(cg.Main.Bytes())
	file.WriteString("}\n")
	file.WriteString("\n")

	file.WriteString("// clean up\n")
	file.Write(cg.Clear.Bytes())
	file.WriteString("return 0;\n}\n")

	file.Close()
	return nil
}

func (cg *Codegen) AllocateID() string {
	allocated := fmt.Sprintf("%d", cg.id)
	cg.id++
	return allocated
}

func (cg *Codegen) GenConstantBigint(val int64) string {
	variableName := fmt.Sprintf("constant_%s", cg.AllocateID())
	cg.Init.WriteString(fmt.Sprintf("FieldBigint* %s = (FieldBigint*)malloc(sizeof(FieldBigint));\n", variableName))
	cg.Clear.WriteString(fmt.Sprintf("free(%s);\n", variableName))
	cg.Init.WriteString(fmt.Sprintf("%s->value = %v;\n", variableName, val))
	cg.Init.WriteString(fmt.Sprintf("%s->isNull = false;\n", variableName))
	return variableName
}

func (cg *Codegen) GenConstantDouble(val float64) string {
	variableName := fmt.Sprintf("constant_%s", cg.AllocateID())
	cg.Init.WriteString(fmt.Sprintf("FieldDouble* %s = (FieldDouble*)malloc(sizeof(FieldDouble));\n", variableName))
	cg.Clear.WriteString(fmt.Sprintf("free(%s);\n", variableName))
	cg.Init.WriteString(fmt.Sprintf("%s->value = %v;\n", variableName, val))
	cg.Init.WriteString(fmt.Sprintf("%s->isNull = false;\n", variableName))
	return variableName
}

func (cg *Codegen) GenTuple(schema *Schema, id string) (string, error) {
	tupleName := fmt.Sprintf("tuple_%s_%s", id, cg.AllocateID())
	cg.Variable.WriteString(fmt.Sprintf("Tuple* %s = (Tuple*)malloc(sizeof(Tuple));\n", tupleName))

	cg.Init.WriteString(fmt.Sprintf("%s->offset = (size_t*)malloc(sizeof(size_t)*%v);\n", tupleName, len(schema.Columns)))
	cg.Init.WriteString(fmt.Sprintf("%s->size   = (size_t*)malloc(sizeof(size_t)*%v);\n", tupleName, len(schema.Columns)))
	sizeofFields := bytes.NewBufferString("")
	for i, col := range schema.Columns {
		if sizeofFields.Len() == 0 {
			cg.Init.WriteString(fmt.Sprintf("%s->offset[%v] = 0;\n", tupleName, i))
		} else {
			cg.Init.WriteString(fmt.Sprintf("%s->offset[%v] = %s;\n", tupleName, i, sizeofFields))
		}

		if i > 0 {
			sizeofFields.WriteString(" + ")
		}

		switch col.RetType.Tp {
		case mysql.TypeLonglong:
			sizeofFields.WriteString(fmt.Sprintf("sizeof(FieldBigint)"))
			cg.Init.WriteString(fmt.Sprintf("%s->size[%v] = sizeof(FieldBigint);\n", tupleName, i))
		case mysql.TypeDouble:
			sizeofFields.WriteString(fmt.Sprintf("sizeof(FieldDouble)"))
			cg.Init.WriteString(fmt.Sprintf("%s->size[%v] = sizeof(FieldDouble);\n", tupleName, i))
		default:
			return "", errors.Errorf("unsupported type [%s] in GenTuple.", col.RetType)
		}
	}
	cg.Init.WriteString(fmt.Sprintf("%s->fields = malloc(%s);\n", tupleName, sizeofFields))

	cg.Clear.WriteString(fmt.Sprintf("free(%s->offset);\n", tupleName))
	cg.Clear.WriteString(fmt.Sprintf("free(%s->size);\n", tupleName))
	cg.Clear.WriteString(fmt.Sprintf("free(%s->fields);\n", tupleName))
	cg.Clear.WriteString(fmt.Sprintf("free(%s);\n", tupleName))
	return tupleName, nil
}

func NewCodegen() *Codegen {
	return &Codegen{
		id:       0,
		Header:   bytes.NewBufferString(""),
		Variable: bytes.NewBufferString(""),
		Function: bytes.NewBufferString(""),
		Init:     bytes.NewBufferString(""),
		Clear:    bytes.NewBufferString(""),
		Main:     bytes.NewBufferString(""),
	}
}
