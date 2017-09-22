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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/driver"
	"github.com/pingcap/tidb/xprotocol/notice"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx/Sql"
	"github.com/pingcap/tidb/xprotocol/util"
)

type xSQL struct {
	xcc *mysqlXClientConn
	ctx *driver.QueryCtx
	pkt *xpacketio.XPacketIO
}

func createContext(xcc *mysqlXClientConn, pkt *xpacketio.XPacketIO) *xSQL {
	return &xSQL{
		xcc: xcc,
		ctx: &xcc.ctx,
		pkt: pkt,
	}
}

func (xsql *xSQL) dealSQLStmtExecute(payload []byte) error {
	var msg Mysqlx_Sql.StmtExecute
	if err := msg.Unmarshal(payload); err != nil {
		return err
	}

	switch msg.GetNamespace() {
	case "xplugin", "mysqlx":
		// TODO: 'xplugin' is deprecated, need to send a notice message.
		if err := xsql.dispatchAdminCmd(msg); err != nil {
			return errors.Trace(err)
		}
	case "sql", "":
		sql := string(msg.GetStmt())
		if err := xsql.executeStmt(sql); err != nil {
			return errors.Trace(err)
		}
	default:
		return util.ErXInvalidNamespace.GenByArgs(msg.GetNamespace())
	}
	return notice.SendExecOk(xsql.pkt, (*xsql.ctx).LastInsertID())
}

func (xsql *xSQL) executeStmtNoResult(sql string) error {
	if _, err := (*xsql.ctx).Execute(sql); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (xsql *xSQL) executeStmt(sql string) error {
	rs, err := (*xsql.ctx).Execute(sql)
	if err != nil {
		return err
	}
	for _, r := range rs {
		if err := notice.WriteResultSet(r, xsql.pkt, xsql.xcc.alloc); err != nil {
			return err
		}
	}
	return nil
}
