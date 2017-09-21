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

type Notice struct {
	noticeType NoticeType
	value      []byte
	pkt        *xpacketio.XPacketIO
}

type NoticeType uint32

const (
	KNoticeWarning                NoticeType = 1
	KNoticeSessionVariableChanged            = 2
	KNoticeSessionStateChanged               = 3
)

func (n *Notice) SendLocalNotice(forceFlush bool) error {
	return n.SendNotice(Mysqlx_Notice.Frame_LOCAL, forceFlush)
}

func (n *Notice) SendNotice(scope Mysqlx_Notice.Frame_Scope, forceFlush bool) error {
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

func SendOK(pkt *xpacketio.XPacketIO, content *string) error {
	msg := Mysqlx.Ok{
		Msg: content,
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

	return notice.SendLocalNotice(false)
}

func SendLastInsertID(pkt *xpacketio.XPacketIO, lastID uint64) error {
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

	return notice.SendLocalNotice(false)
}

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

	return notice.SendLocalNotice(false)
}

func SendInitError(pkt *xpacketio.XPacketIO, code *uint16, msg *string) error {
	errCode := uint32(*code)
	sqlState := mysql.DefaultMySQLState
	severity := Mysqlx.Error_Severity(Mysqlx.Error_FATAL)
	mysqlxErr := Mysqlx.Error{
		Code:     &errCode,
		SqlState: &sqlState,
		Msg:      msg,
		Severity: &severity,
	}

	data, err := mysqlxErr.Marshal()
	if err != nil {
		return err
	}

	return pkt.WritePacket(Mysqlx.ServerMessages_ERROR, data)
}

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
