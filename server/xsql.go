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
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Sql"
)

type xSQL struct {
	xcc *mysqlXClientConn
	ctx driver.QueryCtx
	pkt *xpacketio.XPacketIO
}

// CreateContext is the init function for sql context.
func CreateContext(xcc *mysqlXClientConn, ctx driver.QueryCtx, pkt *xpacketio.XPacketIO) *xSQL {
	return &xSQL{
		xcc: xcc,
		ctx: ctx,
		pkt: pkt,
	}
}

// DealSQLStmtExecute deals Mysqlx.ClientMessages_SQL_STMT_EXECUTE message.
func (xsql *xSQL) DealSQLStmtExecute(payload []byte) error {
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
		return errors.New("unknown namespace")
	}
	return xsql.sendExecOk()
}

func (xsql *xSQL) executeStmtNoResult(sql string) error {
	if _, err := xsql.ctx.Execute(sql); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (xsql *xSQL) executeStmt(sql string) error {
	rs, err := xsql.ctx.Execute(sql)
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

func (xsql *xSQL) sendExecOk() error {
	// TODO: return more notice here, for example: rows affected.
	if xsql.ctx.LastInsertID() > 0 {
		if err := notice.SendLastInsertID(xsql.pkt, xsql.ctx.LastInsertID()); err != nil {
			return errors.Trace(err)
		}
	}
	if err := xsql.pkt.WritePacket(Mysqlx.ServerMessages_SQL_STMT_EXECUTE_OK, nil); err != nil {
		return errors.Trace(err)
	}
	return nil
}
