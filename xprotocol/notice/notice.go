package notice

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/driver"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tipb/go-mysqlx/Notice"
	"github.com/pingcap/tipb/go-mysqlx/Resultset"
)

// notice message sent to client.
type Notice struct {
	noticeType noticeType
	value      []byte
	pkt        *xpacketio.XPacketIO
}

type noticeType uint32

const (
	KNoticeWarning                noticeType = 1
	KNoticeSessionVariableChanged            = 2
	KNoticeSessionStateChanged               = 3
)

func (n *Notice) sendLocalNotice(forceFlush bool) error {
	return n.sendNotice(Mysqlx_Notice.Frame_LOCAL, forceFlush)
}

func (n *Notice) sendNotice(scope Mysqlx_Notice.Frame_Scope, forceFlush bool) error {
	frameType := uint32(n.noticeType)
	msg := Mysqlx_Notice.Frame{
		Type:    &frameType,
		Scope:   &scope,
		Payload: n.value,
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	n.pkt.WritePacket(Mysqlx.ServerMessages_NOTICE, data)
	return nil
}

// SendNoticeOK send notice message 'ok' to client, this is different from server message ok and
// sql statement exec message ok.
func SendNoticeOK(pkt *xpacketio.XPacketIO, content string) error {
	msg := Mysqlx.Ok{
		Msg: &content,
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	notice := Notice{
		noticeType: KNoticeSessionStateChanged,
		value:      data,
		pkt:        pkt,
	}

	return notice.sendLocalNotice(false)
}

func sendLastInsertID(pkt *xpacketio.XPacketIO, lastID uint64) error {
	param := Mysqlx_Notice.SessionStateChanged_Parameter(Mysqlx_Notice.SessionStateChanged_GENERATED_INSERT_ID)
	scalarType := Mysqlx_Datatypes.Scalar_V_UINT
	id := lastID
	msg := Mysqlx_Notice.SessionStateChanged{
		Param: &param,
		Value: &Mysqlx_Datatypes.Scalar{
			Type:         &scalarType,
			VUnsignedInt: &id,
		},
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	notice := Notice{
		noticeType: KNoticeSessionStateChanged,
		value:      data,
		pkt:        pkt,
	}

	return notice.sendLocalNotice(false)
}

// SendClientId send client id to client
func SendClientId(pkt *xpacketio.XPacketIO, sessionId uint32) error {
	param := Mysqlx_Notice.SessionStateChanged_Parameter(Mysqlx_Notice.SessionStateChanged_CLIENT_ID_ASSIGNED)
	scalarType := Mysqlx_Datatypes.Scalar_V_UINT
	id := uint64(sessionId)
	msg := Mysqlx_Notice.SessionStateChanged{
		Param: &param,
		Value: &Mysqlx_Datatypes.Scalar{
			Type:         &scalarType,
			VUnsignedInt: &id,
		},
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	notice := Notice{
		noticeType: KNoticeSessionStateChanged,
		value:      data,
		pkt:        pkt,
	}

	return notice.sendLocalNotice(false)
}

// WriteResultSet write result set message to client
// @TODO this is important to performance, need to consider carefully and tuning in next pr
func WriteResultSet(r driver.ResultSet, pkt *xpacketio.XPacketIO, alloc arena.Allocator) error {
	defer r.Close()
	row, err := r.Next()
	if err != nil {
		return errors.Trace(err)
	}
	cols, err := r.Columns()
	if err != nil {
		return errors.Trace(err)
	}

	// Write column information.
	for _, c := range cols {
		tp, err := util.MysqlType2XType(c.Type, mysql.HasUnsignedFlag(uint(c.Flag)))
		if err != nil {
			return errors.Trace(err)
		}
		flags := uint32(c.Flag)
		columnMeta := Mysqlx_Resultset.ColumnMetaData{
			Type:          &tp,
			Name:          []byte(c.Name),
			Table:         []byte(c.OrgName),
			OriginalTable: []byte(c.OrgTable),
			Schema:        []byte(c.Schema),
			Length:        &c.ColumnLength,
			Flags:         &flags,
		}
		data, err := columnMeta.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		if err := pkt.WritePacket(Mysqlx.ServerMessages_RESULTSET_COLUMN_META_DATA, data); err != nil {
			return errors.Trace(err)
		}
	}

	// Write rows.
	for {
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		if err != nil {
			return errors.Trace(err)
		}

		rowData, err := rowToRow(alloc, cols, row)
		if err != nil {
			return errors.Trace(err)
		}
		data, err := rowData.Marshal()
		if err != nil {
			return errors.Trace(err)
		}

		if err := pkt.WritePacket(Mysqlx.ServerMessages_RESULTSET_ROW, data); err != nil {
			return errors.Trace(err)
		}
		row, err = r.Next()
	}

	if err := pkt.WritePacket(Mysqlx.ServerMessages_RESULTSET_FETCH_DONE, []byte{}); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// SendExecOk send exec ok message to client, used when statement is finished.
func SendExecOk(pkt *xpacketio.XPacketIO, lastID uint64) error {
	if lastID > 0 {
		if err := sendLastInsertID(pkt, lastID); err != nil {
			return errors.Trace(err)
		}
	}
	if err := pkt.WritePacket(Mysqlx.ServerMessages_SQL_STMT_EXECUTE_OK, nil); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func rowToRow(alloc arena.Allocator, columns []*driver.ColumnInfo, row []types.Datum) (*Mysqlx_Resultset.Row, error) {
	if len(columns) != len(row) {
		return nil, mysql.ErrMalformPacket
	}
	var fields [][]byte
	for i, val := range row {
		datum, err := driver.DumpDatumToBinary(alloc, columns[i], val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fields = append(fields, datum)
	}
	return &Mysqlx_Resultset.Row{
		Field: fields,
	}, nil
}
