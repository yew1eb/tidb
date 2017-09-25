package notice

import (
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tipb/go-mysqlx/Notice"
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
