package server

import (
	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/driver"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Sql"
	"github.com/pingcap/tidb/xprotocol/notice"
)

type XSql struct {
	xcc *mysqlXClientConn
	ctx driver.QueryCtx
	pkt *xpacketio.XPacketIO
}

func CreateContext(xcc *mysqlXClientConn, ctx driver.QueryCtx, pkt *xpacketio.XPacketIO) *XSql {
	return &XSql{
		xcc: xcc,
		ctx: ctx,
		pkt: pkt,
	}
}

func (xsql *XSql) DealSQLStmtExecute(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
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
		log.Infof("[YUSP] %s", sql)
		if err := xsql.executeStmt(sql); err != nil {
			return errors.Trace(err)
		}
	default:
		return errors.New("unknown namespace")
	}
	return xsql.sendExecOk()
}

func (xsql *XSql) executeStmtNoResult(sql string) error {
	if _, err := xsql.ctx.Execute(sql); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (xsql *XSql) executeStmt(sql string) error {
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

func (xsql *XSql) sendExecOk() error {
	if xsql.ctx.LastInsertID() > 0 {
		if err := notice.SendLastInsertID(xsql.pkt, xsql.ctx.LastInsertID()); err != nil {
			return errors.Trace(err)
		}
	}
	if err := xsql.pkt.WritePacket(int32(Mysqlx.ServerMessages_SQL_STMT_EXECUTE_OK), nil); err != nil {
		return errors.Trace(err)
	}
	return nil
}
