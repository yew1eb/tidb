package crud

import (
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tidb/driver"
	"github.com/pingcap/tidb/xprotocol/notice"
	"github.com/pingcap/tidb/util/arena"
)

type builder interface {
	buildQuery([]byte) string
}

func (crud *XCrud) createCrudBuilder(msgType Mysqlx.ClientMessages_Type) builder {
	switch msgType {
	case Mysqlx.ClientMessages_CRUD_FIND:
	case Mysqlx.ClientMessages_CRUD_INSERT:
	case Mysqlx.ClientMessages_CRUD_UPDATE:
	case Mysqlx.ClientMessages_CRUD_DELETE:
	case Mysqlx.ClientMessages_CRUD_CREATE_VIEW:
	case Mysqlx.ClientMessages_CRUD_MODIFY_VIEW:
	case Mysqlx.ClientMessages_CRUD_DROP_VIEW:
	default:
	}
	return nil
}

type XCrud struct {
	ctx driver.QueryCtx
	pkt *xpacketio.XPacketIO
	alloc arena.Allocator
}

func (crud *XCrud) DealCrudStmtExecute(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	build := crud.createCrudBuilder(msgType)
	sqlQuery := build.buildQuery(payload)
	rset, err := crud.ctx.Execute(sqlQuery)
	if err != nil {
		return err
	}
	for _, r := range rset {
		if err := notice.WriteResultSet(r, crud.pkt, crud.alloc); err != nil {
			return err
		}
	}
	return nil
}

func CreateCrud(ctx driver.QueryCtx, pkt *xpacketio.XPacketIO, alloc arena.Allocator) *XCrud {
	return &XCrud{
		ctx:   ctx,
		pkt:   pkt,
		alloc: alloc,
	}
}
