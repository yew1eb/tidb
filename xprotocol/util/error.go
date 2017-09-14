package util

import (
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tipb/go-mysqlx"
)

var (
	ErrXBadMessage   = ErrorMessage(mysql.ErXBadMessage, "Invalid message", mysql.DefaultMySQLState)
	ErrXNoSuchUser   = ErrorMessage(mysql.ErrNoSuchUser, "Invalid user or password", mysql.DefaultMySQLState)
	ErrXAccessDenied = ErrorMessage(mysql.ErrAccessDenied, "Invalid user or password", mysql.DefaultMySQLState)
)

// ErrorMessage returns Mysqlx Error.
func ErrorMessage(errcode uint16, msg string, state string) *Mysqlx.Error {
	code := uint32(errcode)
	sqlState := state
	errMsg := Mysqlx.Error{
		Severity: Mysqlx.Error_ERROR.Enum(),
		Code:     &code,
		SqlState: &sqlState,
		Msg:      &msg,
	}
	return &errMsg
}

var (
	ErXBadSchema         = terror.ClassXProtocol.New(codeErXBadSchema, mysql.MySQLErrName[mysql.ErXBadSchema])
	ErXBadTable          = terror.ClassXProtocol.New(codeErXBadTable, mysql.MySQLErrName[mysql.ErXBadTable])
	ErrTableExists       = terror.ClassXProtocol.New(codeErrTableExists, mysql.MySQLErrName[mysql.ErrTableExists])
	ErXInvalidCollection = terror.ClassXProtocol.New(codeErXInvalidCollection, mysql.MySQLErrName[mysql.ErXInvalidCollection])
	ErrJSONUsedAsKey     = terror.ClassXProtocol.New(codeErrJSONUsedAsKey, mysql.MySQLErrName[mysql.ErrJSONUsedAsKey])
	ErXBadNotice         = terror.ClassXProtocol.New(codeErXBadNotice, mysql.MySQLErrName[mysql.ErXBadNotice])
)

const (
	codeErXBadSchema         terror.ErrCode = terror.ErrCode(mysql.ErXBadSchema)
	codeErXBadTable                         = terror.ErrCode(mysql.ErXBadTable)
	codeErrTableExists                      = terror.ErrCode(mysql.ErrTableExists)
	codeErXInvalidCollection                = terror.ErrCode(mysql.ErXInvalidCollection)
	codeErrJSONUsedAsKey                    = terror.ErrCode(mysql.ErrJSONUsedAsKey)
	codeErXBadNotice                        = terror.ErrCode(mysql.ErXBadNotice)
)

func init() {
	xProtocolMySQLErrCodes := map[terror.ErrCode]uint16{
		codeErXBadSchema:         mysql.ErXBadSchema,
		codeErXBadTable:          mysql.ErXBadTable,
		codeErrTableExists:       mysql.ErrTableExists,
		codeErXInvalidCollection: mysql.ErXInvalidCollection,
		codeErrJSONUsedAsKey:     mysql.ErrJSONUsedAsKey,
		codeErXBadNotice:         mysql.ErXBadNotice,
	}
	terror.ErrClassToMySQLCodes[terror.ClassXProtocol] = xProtocolMySQLErrCodes
}
