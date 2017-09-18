package server

import (
	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tipb/go-mysqlx/Sql"
	"strings"
)

const (
	CountDoc string = "COUNT(CASE WHEN (column_name = 'doc' " +
		"AND data_type = 'json') THEN 1 ELSE NULL END)"
	CountID = "COUNT(CASE WHEN (column_name = '_id' " +
		"AND generation_expression = " +
		"'json_unquote(json_extract(`doc`,''$._id''))') THEN 1 ELSE NULL END)"
	CountGen = "COUNT(CASE WHEN (column_name != '_id' " +
		"AND generation_expression RLIKE " +
		"'^(json_unquote[[.(.]])?json_extract[[.(.]]`doc`," +
		"''[[.$.]]([[...]][^[:space:][...]]+)+''[[.).]]{1,2}$') THEN 1 ELSE NULL " +
		"END)"
)

func (xsql *XSql) dispatchAdminCmd(msg Mysqlx_Sql.StmtExecute) error {
	stmt := string(msg.GetStmt())
	log.Infof("[YUSP] %s", stmt)
	var err error
	args := msg.GetArgs()
	switch stmt {
	case "ping":
		err = xsql.ping(args)
	case "list_clients":
	case "kill_client":
		err = xsql.killClient(args)
	case "create_collection":
		err = xsql.createCollection(args)
	case "drop_collection":
		err = xsql.dropCollection(args)
	case "ensure_collection":
		err = xsql.ensureCollection(args)
	case "create_collection_index":
		err = xsql.createCollectionIndex(args)
	case "drop_collection_index":
		err = xsql.dropCollectionIndex(args)
	case "list_objects":
		err = xsql.listObjects(args)
	case "enable_notices":
		err = xsql.enableNotices(args)
	case "disable_notices":
	case "list_notices":
	default:
		return errors.New("unknown statement")
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (xsql *XSql) ping(args []*Mysqlx_Datatypes.Any) error {
	if len(args) != 0 {
		return errors.New("not enough arguments")
	}
	return xsql.sendExecOk()
}

func (xsql *XSql) listClients() error {
	return nil
}

func (xsql *XSql) killClient(args []*Mysqlx_Datatypes.Any) error {
	if len(args) != 1 {
		return errors.New("not enough arguments")
	}
	if args[0].GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Any_Type_name[int32(Mysqlx_Datatypes.Any_SCALAR)],
			Mysqlx_Datatypes.Any_Type_name[int32(args[0].GetType())])
	}
	if args[0].GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_UINT {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Scalar_Type_name[int32(Mysqlx_Datatypes.Scalar_V_UINT)],
			Mysqlx_Datatypes.Scalar_Type_name[int32(args[0].GetScalar().GetType())])
	}
	id := args[0].GetScalar().GetVUnsignedInt()
	xsql.xcc.server.Kill(id, false)
	return xsql.sendExecOk()
}

func (xsql *XSql) createCollectionImpl(args []*Mysqlx_Datatypes.Any) error {
	if len(args) != 2 {
		return errors.New("not enough arguments")
	}
	if args[0].GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Any_Type_name[int32(Mysqlx_Datatypes.Any_SCALAR)],
			Mysqlx_Datatypes.Any_Type_name[int32(args[0].GetType())])
	}
	if args[0].GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_STRING {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Scalar_Type_name[int32(Mysqlx_Datatypes.Scalar_V_STRING)],
			Mysqlx_Datatypes.Scalar_Type_name[int32(args[0].GetScalar().GetType())])
	}
	if args[1].GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Any_Type_name[int32(Mysqlx_Datatypes.Any_SCALAR)],
			Mysqlx_Datatypes.Any_Type_name[int32(args[1].GetType())])
	}
	if args[1].GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_STRING {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Scalar_Type_name[int32(Mysqlx_Datatypes.Scalar_V_STRING)],
			Mysqlx_Datatypes.Scalar_Type_name[int32(args[1].GetScalar().GetType())])
	}
	schema := string(args[0].GetScalar().GetVString().GetValue())
	collection := string(args[1].GetScalar().GetVString().GetValue())

	sql := "CREATE TABLE "
	if len(schema) != 0 {
		sql += schema + "."
	}
	sql += collection + " (doc JSON," +
		"_id VARCHAR(32) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(doc, '$._id'))) STORED PRIMARY KEY" +
		") CHARSET utf8mb4 ENGINE=InnoDB;"
	log.Infof("CreateCollection: %s", collection)

	return xsql.executeStmtNoResult(sql)
}

func (xsql *XSql) createCollection(args []*Mysqlx_Datatypes.Any) error {
	err := xsql.createCollectionImpl(args)
	if err != nil {
		return errors.Trace(err)
	}
	return xsql.sendExecOk()
}

func (xsql *XSql) ensureCollection(args []*Mysqlx_Datatypes.Any) error {
	err := xsql.createCollectionImpl(args)
	if err != nil {
		if !terror.ErrorEqual(err, util.ErrTableExists) {
			return errors.Trace(err)
		}
		return errors.Trace(err)
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
	return xsql.sendExecOk()
}

func (xsql *XSql) dropCollection(args []*Mysqlx_Datatypes.Any) error {
	if len(args) != 2 {
		return errors.New("not enough arguments")
	}
	if args[0].GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Any_Type_name[int32(Mysqlx_Datatypes.Any_SCALAR)],
			Mysqlx_Datatypes.Any_Type_name[int32(args[0].GetType())])
	}
	if args[0].GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_STRING {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Scalar_Type_name[int32(Mysqlx_Datatypes.Scalar_V_STRING)],
			Mysqlx_Datatypes.Scalar_Type_name[int32(args[0].GetScalar().GetType())])
	}
	if args[1].GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Any_Type_name[int32(Mysqlx_Datatypes.Any_SCALAR)],
			Mysqlx_Datatypes.Any_Type_name[int32(args[1].GetType())])
	}
	if args[1].GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_STRING {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Scalar_Type_name[int32(Mysqlx_Datatypes.Scalar_V_STRING)],
			Mysqlx_Datatypes.Scalar_Type_name[int32(args[1].GetScalar().GetType())])
	}
	schema := string(args[0].GetScalar().GetVString().GetValue())
	collection := string(args[1].GetScalar().GetVString().GetValue())
	if len(schema) == 0 {
		return util.ErXBadSchema
	}
	if len(collection) == 0 {
		return util.ErXBadTable
	}
	sql := "DROP TABLE " + schema + "." + collection
	log.Infof("DropCollection: %s", collection)
	if err := xsql.executeStmtNoResult(sql); err != nil {
		return errors.Trace(err)
	}
	return xsql.sendExecOk()
}

func (xsql *XSql) createCollectionIndex(args []*Mysqlx_Datatypes.Any) error {
	return util.ErrJSONUsedAsKey
}

func (xsql *XSql) dropCollectionIndex(args []*Mysqlx_Datatypes.Any) error {
	return util.ErrJSONUsedAsKey
}

func (xsql *XSql) listObjects(args []*Mysqlx_Datatypes.Any) error {
	if len(args) != 2 {
		return errors.New("not enough arguments")
	}
	if args[0].GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Any_Type_name[int32(Mysqlx_Datatypes.Any_SCALAR)],
			Mysqlx_Datatypes.Any_Type_name[int32(args[0].GetType())])
	}
	if args[0].GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_STRING {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Scalar_Type_name[int32(Mysqlx_Datatypes.Scalar_V_STRING)],
			Mysqlx_Datatypes.Scalar_Type_name[int32(args[0].GetScalar().GetType())])
	}
	if args[1].GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Any_Type_name[int32(Mysqlx_Datatypes.Any_SCALAR)],
			Mysqlx_Datatypes.Any_Type_name[int32(args[1].GetType())])
	}
	if args[1].GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_STRING {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Scalar_Type_name[int32(Mysqlx_Datatypes.Scalar_V_STRING)],
			Mysqlx_Datatypes.Scalar_Type_name[int32(args[1].GetScalar().GetType())])
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
			CountDoc +
			"=1, 'COLLECTION_VIEW', 'VIEW'), IF(COUNT(*)-2 = " +
			CountGen +
			" AND " +
			CountDoc +
			"=1 AND " +
			CountID +
			"=1, 'COLLECTION', 'TABLE')) AS type " +
			"FROM information_schema.tables AS T " +
			"LEFT JOIN information_schema.columns AS C ON (" +
			"BINARY T.table_schema = C.table_schema AND " +
			"BINARY T.table_name = C.table_name) " +
			"WHERE T.table_schema = "

	if len(schema) == 0 {
		sql += "schema()"
	} else {
		sql += quoteString(schema)
	}

	if len(pattern) != 0 {
		sql += " AND T.table_name LIKE " + quoteString(pattern)
	}

	sql += " GROUP BY name ORDER BY name"
	log.Infof("[YUSP] %s", sql)
	if err := xsql.executeStmt(sql); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (xsql *XSql) enableNotices(args []*Mysqlx_Datatypes.Any) error {
	enableWarning := false
	for _, v := range args {
		if err := isString(v); err != nil {
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
	return xsql.sendExecOk()
}

func (xsql *XSql) disableNotices(args []*Mysqlx_Datatypes.Any) error {
	return xsql.sendExecOk()
}

func (xsql *XSql) listNotices(args []*Mysqlx_Datatypes.Any) error {
	return xsql.sendExecOk()
}

func (xsql *XSql) isSchemaSelectedAndExists(schema string) error {
	sql := "SHOW TABLES"
	if len(schema) != 0 {
		sql = sql + " FROM " + quoteIdentifier(schema)
	}
	if err := xsql.executeStmtNoResult(sql); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func quoteIdentifier(str string) string {
	return "`" + str + "`"
}

func quoteString(str string) string {
	return "'" + str + "'"
}

func (xsql *XSql) isCollection(schema string, collection string) (bool, error) {
	sql := "SELECT COUNT(*) AS cnt," + CountDoc + " As doc," + CountID + " AS id," + CountGen +
		" AS gen " + "FROM information_schema.columns WHERE table_name = " +
		quoteString(collection) + " AND table_schema = "
	if len(schema) == 0 {
		sql += "schema()"
	} else {
		sql += quoteString(schema)
	}
	log.Infof("[YUSP] sql: %s", sql)
	rs, err := xsql.ctx.Execute(sql)
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
	// TODO: need to fetch sql result to determine is collection or not.
	//defer rs[0].Close()
	//row, err := rs[0].Next()
	//cols, err := rs[0].Columns()
	//rowData, err := rowToRow(xsql.xcc.alloc, cols, row)
}

func isString(any *Mysqlx_Datatypes.Any) error {
	if any.GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Any_Type_name[int32(Mysqlx_Datatypes.Any_SCALAR)],
			Mysqlx_Datatypes.Any_Type_name[int32(any.GetType())])
	}
	if any.GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_STRING {
		return errors.Errorf("wrong type, need %s, but get %s",
			Mysqlx_Datatypes.Scalar_Type_name[int32(Mysqlx_Datatypes.Scalar_V_STRING)],
			Mysqlx_Datatypes.Scalar_Type_name[int32(any.GetScalar().GetType())])
	}
	return nil
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
