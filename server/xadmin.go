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

package server

import (
	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tipb/go-mysqlx/Sql"
	"strings"
)

const (
	countDoc string = "COUNT(CASE WHEN (column_name = 'doc' " +
		"AND data_type = 'json') THEN 1 ELSE NULL END)"

	// countID is not exactly same as MySQL X Protocol,
	//     TiDB: JSON_UNQUOTE(JSON_EXTRACT(doc,''$._id''))
	//     MySQL: json_unquote(json_extract(`doc`,''$._id''))'
	countID = "COUNT(CASE WHEN (column_name = '_id' " +
		"AND generation_expression = " +
		"'JSON_UNQUOTE(JSON_EXTRACT(doc,''$._id''))') THEN 1 ELSE NULL END)"
	countGen = "COUNT(CASE WHEN (column_name != '_id' " +
		"AND generation_expression RLIKE " +
		"'^(json_unquote[[.(.]])?json_extract[[.(.]]`doc`," +
		"''[[.$.]]([[...]][^[:space:][...]]+)+''[[.).]]{1,2}$') THEN 1 ELSE NULL " +
		"END)"
)

func (xsql *xSQL) dispatchAdminCmd(msg Mysqlx_Sql.StmtExecute) error {
	stmt := string(msg.GetStmt())
	args := msg.GetArgs()
	log.Infof("MySQL X SQL statement: %s", stmt)
	switch stmt {
	case "ping":
		return xsql.ping(args)
	case "list_clients":
		return util.ErXInvalidAdminCommand.GenByArgs(msg.GetNamespace(), stmt)
	case "kill_client":
		return xsql.killClient(args)
	case "create_collection":
		return xsql.createCollection(args)
	case "drop_collection":
		return xsql.dropCollection(args)
	case "ensure_collection":
		return xsql.ensureCollection(args)
	case "create_collection_index":
		return xsql.createCollectionIndex(args)
	case "drop_collection_index":
		return xsql.dropCollectionIndex(args)
	case "list_objects":
		return xsql.listObjects(args)
	case "enable_notices":
		return xsql.enableNotices(args)
	case "disable_notices":
		return util.ErXInvalidAdminCommand.GenByArgs(msg.GetNamespace(), stmt)
	case "list_notices":
		return util.ErXInvalidAdminCommand.GenByArgs(msg.GetNamespace(), stmt)
	default:
		return util.ErXInvalidAdminCommand.GenByArgs(msg.GetNamespace(), stmt)
	}
}

func (xsql *xSQL) ping(args []*Mysqlx_Datatypes.Any) error {
	if len(args) != 0 {
		return util.ErXCmdNumArguments.GenByArgs(0, len(args))
	}
	return nil
}

func (xsql *xSQL) listClients() error {
	return nil
}

func (xsql *xSQL) killClient(args []*Mysqlx_Datatypes.Any) error {
	if err := checkArgs(args, []Mysqlx_Datatypes.Scalar_Type{Mysqlx_Datatypes.Scalar_V_UINT}); err != nil {
		return errors.Trace(err)
	}
	id := args[0].GetScalar().GetVUnsignedInt()
	xsql.xcc.server.Kill(id, false)
	return nil
}

func (xsql *xSQL) createCollectionImpl(args []*Mysqlx_Datatypes.Any) error {
	if err := checkArgs(args, []Mysqlx_Datatypes.Scalar_Type{Mysqlx_Datatypes.Scalar_V_STRING, Mysqlx_Datatypes.Scalar_V_STRING}); err != nil {
		return errors.Trace(err)
	}

	schema := string(args[0].GetScalar().GetVString().GetValue())
	collection := string(args[1].GetScalar().GetVString().GetValue())

	sql := "CREATE TABLE "
	if len(schema) != 0 {
		sql += util.QuoteIdentifier(schema) + "."
	}
	sql += util.QuoteIdentifier(collection) + " (doc JSON," +
		"_id VARCHAR(32) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(doc, '$._id'))) STORED PRIMARY KEY" +
		") CHARSET utf8mb4 ENGINE=InnoDB;"
	log.Infof("CreateCollection: %s", collection)

	return xsql.executeStmtNoResult(sql)
}

func (xsql *xSQL) createCollection(args []*Mysqlx_Datatypes.Any) error {
	return xsql.createCollectionImpl(args)
}

func (xsql *xSQL) ensureCollection(args []*Mysqlx_Datatypes.Any) error {
	err := xsql.createCollectionImpl(args)
	if err != nil {
		if !terror.ErrorEqual(err, infoschema.ErrTableExists) {
			return errors.Trace(err)
		}
	}
	schema := string(args[0].GetScalar().GetVString().GetValue())
	collection := string(args[1].GetScalar().GetVString().GetValue())
	isColl, err := xsql.isCollection(schema, collection)
	if err != nil {
		return errors.Trace(err)
	}
	if !isColl {
		return util.ErXInvalidCollection
	}
	return nil
}

func (xsql *xSQL) dropCollection(args []*Mysqlx_Datatypes.Any) error {
	if err := checkArgs(args, []Mysqlx_Datatypes.Scalar_Type{Mysqlx_Datatypes.Scalar_V_STRING, Mysqlx_Datatypes.Scalar_V_STRING}); err != nil {
		return errors.Trace(err)
	}
	schema := string(args[0].GetScalar().GetVString().GetValue())
	collection := string(args[1].GetScalar().GetVString().GetValue())
	if len(schema) == 0 {
		return util.ErXBadSchema
	}
	if len(collection) == 0 {
		return util.ErXBadTable
	}
	sql := "DROP TABLE " + util.QuoteIdentifier(schema) + "." + util.QuoteIdentifier(collection)
	log.Infof("DropCollection: %s", collection)
	return xsql.executeStmtNoResult(sql)
}

func (xsql *xSQL) createCollectionIndex(args []*Mysqlx_Datatypes.Any) error {
	return util.ErrJSONUsedAsKey
}

func (xsql *xSQL) dropCollectionIndex(args []*Mysqlx_Datatypes.Any) error {
	return util.ErrJSONUsedAsKey
}

func (xsql *xSQL) listObjects(args []*Mysqlx_Datatypes.Any) error {
	if err := checkArgs(args, []Mysqlx_Datatypes.Scalar_Type{Mysqlx_Datatypes.Scalar_V_STRING, Mysqlx_Datatypes.Scalar_V_STRING}); err != nil {
		return errors.Trace(err)
	}
	schema := string(args[0].GetScalar().GetVString().GetValue())
	pattern := string(args[1].GetScalar().GetVString().GetValue())
	if err := xsql.isSchemaSelectedAndExists(schema); err != nil {
		return errors.Trace(err)
	}
	sql :=
		"SELECT BINARY T.table_name AS name, " +
			"IF(ANY_VALUE(T.table_type) LIKE '%VIEW', " +
			"IF(COUNT(*)=1 AND " +
			countDoc +
			"=1, 'COLLECTION_VIEW', 'VIEW'), IF(COUNT(*)-2 = " +
			countGen +
			" AND " +
			countDoc +
			"=1 AND " +
			countID +
			"=1, 'COLLECTION', 'TABLE')) AS type " +
			"FROM information_schema.tables AS T " +
			"LEFT JOIN information_schema.columns AS C ON (" +
			"BINARY T.table_schema = C.table_schema AND " +
			"BINARY T.table_name = C.table_name) " +
			"WHERE T.table_schema = "

	if len(schema) == 0 {
		sql += "schema()"
	} else {
		sql += util.QuoteString(schema)
	}

	if len(pattern) != 0 {
		sql += " AND T.table_name LIKE " + util.QuoteString(pattern)
	}

	sql += " GROUP BY name ORDER BY name"
	return xsql.executeStmt(sql)
}

func (xsql *xSQL) enableNotices(args []*Mysqlx_Datatypes.Any) error {
	enableWarning := false
	for i, v := range args {
		if err := isString(v, i); err != nil {
			return errors.Trace(err)
		}
		notice := string(v.GetScalar().GetVString().GetValue())
		if strings.EqualFold(notice, "warning") {
			enableWarning = true
		} else if err := isFixedNoticeName(notice); err != nil {
			return errors.Trace(err)
		}
		if enableWarning {
			// TODO: enable warning here, need a context.
		}
	}
	return nil
}

func (xsql *xSQL) disableNotices(args []*Mysqlx_Datatypes.Any) error {
	return nil
}

func (xsql *xSQL) listNotices(args []*Mysqlx_Datatypes.Any) error {
	return nil
}

func (xsql *xSQL) isSchemaSelectedAndExists(schema string) error {
	sql := "SHOW TABLES"
	if len(schema) != 0 {
		sql = sql + " FROM " + util.QuoteIdentifier(schema)
	}
	return xsql.executeStmtNoResult(sql)
}

func (xsql *xSQL) isCollection(schema string, collection string) (bool, error) {
	sql := "SELECT COUNT(*) AS cnt," + countDoc + " As doc," + countID + " AS id," + countGen +
		" AS gen " + "FROM information_schema.columns WHERE table_name = " +
		util.QuoteString(collection) + " AND table_schema = "
	if len(schema) == 0 {
		sql += "schema()"
	} else {
		sql += util.QuoteString(schema)
	}
	rs, err := (*xsql.ctx).Execute(sql)
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(rs) != 1 {
		var name string
		if len(schema) != 0 {
			name = schema + "." + collection
		} else {
			name = collection
		}

		log.Infof("Unable to recognize '%s' as a collection; query result size: %lu",
			name, len(rs))
		return false, nil
	}

	return true, nil
	// TODO: need to fetch sql result to determine that the table is collection or not.
	//defer rs[0].Close()
	//row, err := rs[0].Next()
	//cols, err := rs[0].Columns()
	//rowData, err := rowToRow(xsql.xcc.alloc, cols, row)
}

func isString(any *Mysqlx_Datatypes.Any, pos int) error {
	return checkScalarArg(any, pos, Mysqlx_Datatypes.Scalar_V_STRING)
}

var fixedNoticeNames = [4]string{"account_expired", "generated_insert_id", "rows_affected", "produced_message"}

func isFixedNoticeName(name string) error {
	for _, v := range fixedNoticeNames {
		if strings.EqualFold(name, v) {
			return nil
		}
	}
	return util.ErXBadNotice
}

func checkArgs(args []*Mysqlx_Datatypes.Any, expects []Mysqlx_Datatypes.Scalar_Type) error {
	if len(args) != len(expects) {
		return util.ErXCmdNumArguments.GenByArgs(len(expects), len(args))
	}
	for i, v := range args {
		return checkScalarArg(v, i, expects[i])
	}
	return nil
}

func checkScalarArg(arg *Mysqlx_Datatypes.Any, pos int, expectType Mysqlx_Datatypes.Scalar_Type) error {
	if arg.GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return util.ErXCmdArgumentType.GenByArgs(arg.String(), pos, expectType.String())
	}
	if arg.GetScalar().GetType() != expectType {
		return util.ErXCmdArgumentType.GenByArgs(arg.GetScalar().String(), pos, expectType)
	}
	return nil
}
