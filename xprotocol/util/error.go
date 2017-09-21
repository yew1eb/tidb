// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tipb/go-mysqlx"
)

var (
	ErrXBadMessage   = XErrorMessage(mysql.ErXBadMessage, "Invalid message", mysql.DefaultMySQLState)
	ErrXNoSuchUser   = XErrorMessage(mysql.ErrNoSuchUser, "Invalid user or password", mysql.DefaultMySQLState)
	ErrXAccessDenied = XErrorMessage(mysql.ErrAccessDenied, "Invalid user or password", mysql.DefaultMySQLState)
)

var (
	ErXBadMessage                = ErrorMessage(mysql.ErXBadMessage, mysql.MySQLErrName[mysql.ErXBadMessage])
	ErXCapabilitiesPrepareFailed = ErrorMessage(mysql.ErXCapabilitiesPrepareFailed, mysql.MySQLErrName[mysql.ErXCapabilitiesPrepareFailed])
	ErrAccessDenied              = ErrorMessage(mysql.ErrAccessDenied, mysql.MySQLErrName[mysql.ErrAccessDenied])
	ErXBadSchema                 = ErrorMessage(mysql.ErXBadSchema, mysql.MySQLErrName[mysql.ErXBadSchema])
	ErXBadTable                  = ErrorMessage(mysql.ErXBadTable, mysql.MySQLErrName[mysql.ErXBadTable])
	ErrTableExists               = ErrorMessage(mysql.ErrTableExists, mysql.MySQLErrName[mysql.ErrTableExists])
	ErXInvalidCollection         = ErrorMessage(mysql.ErXInvalidCollection, mysql.MySQLErrName[mysql.ErXInvalidCollection])
	ErrJSONUsedAsKey             = ErrorMessage(mysql.ErrJSONUsedAsKey, mysql.MySQLErrName[mysql.ErrJSONUsedAsKey])
	ErXBadNotice                 = ErrorMessage(mysql.ErXBadNotice, mysql.MySQLErrName[mysql.ErXBadNotice])
)

const (
	codeErXBadMessage                terror.ErrCode = terror.ErrCode(mysql.ErXBadMessage)
	codeErXAccessDenied                             = terror.ErrCode(mysql.ErrAccessDenied)
	codeErXBadSchema                                = terror.ErrCode(mysql.ErXBadSchema)
	codeErXBadTable                                 = terror.ErrCode(mysql.ErXBadTable)
	codeErrTableExists                              = terror.ErrCode(mysql.ErrTableExists)
	codeErXInvalidCollection                        = terror.ErrCode(mysql.ErXInvalidCollection)
	codeErrJSONUsedAsKey                            = terror.ErrCode(mysql.ErrJSONUsedAsKey)
	codeErXBadNotice                                = terror.ErrCode(mysql.ErXBadNotice)
	codeErXCapabilitiesPrepareFailed                = terror.ErrCode(mysql.ErXCapabilitiesPrepareFailed)

	// crud
	CodeErXBadProjection = terror.ErrCode(mysql.ErXBadProjection)
	CodeErXBadInsertData = terror.ErrCode(mysql.ErXBadInsertData)

	// expr
	CodeErXExprMissingArg = terror.ErrCode(mysql.ErXExprMissingArg)
)

func init() {
	xProtocolMySQLErrCodes := map[terror.ErrCode]uint16{
		codeErXBadMessage:                mysql.ErXBadMessage,
		codeErXCapabilitiesPrepareFailed: mysql.ErXCapabilitiesPrepareFailed,
		codeErXAccessDenied:              mysql.ErrAccessDenied,
		codeErXBadSchema:                 mysql.ErXBadSchema,
		codeErXBadTable:                  mysql.ErXBadTable,
		codeErrTableExists:               mysql.ErrTableExists,
		codeErXInvalidCollection:         mysql.ErXInvalidCollection,
		codeErrJSONUsedAsKey:             mysql.ErrJSONUsedAsKey,
		codeErXBadNotice:                 mysql.ErXBadNotice,
		CodeErXBadProjection:             mysql.ErXBadProjection,
		CodeErXBadInsertData:             mysql.ErXBadInsertData,
		CodeErXExprMissingArg:            mysql.ErXExprMissingArg,
	}
	terror.ErrClassToMySQLCodes[terror.ClassXProtocol] = xProtocolMySQLErrCodes
}

// ErrorMessage returns terror Error.
func ErrorMessage(code terror.ErrCode, msg string) *terror.Error {
	return terror.ClassXProtocol.New(code, msg)
}

// XErrorMessage returns Mysqlx Error.
func XErrorMessage(errcode uint16, msg string, state string) *Mysqlx.Error {
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
