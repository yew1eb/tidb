package server

import (
	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/xprotocol/notice"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Session"
	"github.com/pingcap/tidb/xprotocol/util"
)

type sessionState int32

const (
	// start as Authenticating
	authenticating sessionState = iota
	// once authenticated, we can handle work
	ready
	// connection is closing, but wait for data to flush out first
	closing
)

type XAuth struct {
	xcc         *mysqlXClientConn
	authHandler authHandler

	mState            sessionState
	mStateBeforeClose sessionState
}

func (xa *XAuth) handleMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	if xa.mState == authenticating {
		return xa.handleAuthMessage(msgType, payload)
	} else if xa.mState == ready {
		return xa.handleReadyMessage(msgType, payload)
	}

	// this is not the same as it is in mysql-x-plugin, which returns nothing, and you should never get here.
	return util.ErrorMessage(mysql.ErrUnknown, "unknown ssession state.")
}

func (xa *XAuth) handleReadyMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	switch msgType {
	case Mysqlx.ClientMessages_SESS_CLOSE:
		notice.SendNoticeOK(xa.xcc.pkt, "bye!")
		xa.onClose(false)
		return nil
	case Mysqlx.ClientMessages_CON_CLOSE:
		notice.SendNoticeOK(xa.xcc.pkt, "bye!")
		xa.onClose(false)
		return nil
	case Mysqlx.ClientMessages_SESS_RESET:
		xa.mState = closing
		xa.onSessionReset()
		return nil
	}
	return util.ErXBadMessage
}

func (xa *XAuth) handleAuthMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	var r *Response
	switch msgType {
	case Mysqlx.ClientMessages_SESS_AUTHENTICATE_START:
		var data Mysqlx_Session.AuthenticateStart
		if err := data.Unmarshal(payload); err != nil {
			log.Errorf("Can't Unmarshal message %s, err %s", msgType.String(), err.Error())
			return util.ErXBadMessage
		}

		xa.authHandler = xa.createAuthHandler(*data.MechName)
		if xa.authHandler == nil {
			log.Errorf("Can't create XAuth handler with mech name %s", *data.MechName)
			xa.stopAuth()
			return util.ErrorMessage(mysql.ErrNotSupportedAuthMode,"Invalid authentication method "+*data.MechName)
		}

		r = xa.authHandler.handleStart(data.MechName, data.AuthData, data.InitialResponse)
	case Mysqlx.ClientMessages_SESS_AUTHENTICATE_CONTINUE:
		var data Mysqlx_Session.AuthenticateContinue
		if err := data.Unmarshal(payload); err != nil {
			return util.ErXBadMessage
		}

		r = xa.authHandler.handleContinue(data.GetAuthData())
	default:
		xa.stopAuth()
		return util.ErXBadMessage
	}

	switch r.status {
	case Succeeded:
		xa.onAuthSuccess(r)
	case Failed:
		xa.onAuthFailure(r)
		return util.ErrorMessage(mysql.ErrAccessDenied, r.data)
	default:
		xa.sendAuthContinue(&r.data)
	}

	return nil
}

func (xa *XAuth) onAuthSuccess(r *Response) {
	notice.SendClientId(xa.xcc.pkt, xa.xcc.connectionID)
	xa.stopAuth()
	xa.mState = ready
	xa.sendAuthOk(&r.data)
}

func (xa *XAuth) onAuthFailure(r *Response) {
	xa.stopAuth()
}

//@TODO need to implement
func (xa *XAuth) onSessionReset() {
}

func (xa *XAuth) onClose(updateOldState bool) {
	if xa.mState != closing {
		if updateOldState {
			xa.mStateBeforeClose = xa.mState
		}
		xa.mState = closing
	}
}

func (xa *XAuth) stopAuth() {
	xa.authHandler = nil
}

func (xa *XAuth) ready() bool {
	return xa.mState == ready
}

func (xa *XAuth) sendAuthOk(value *string) error {
	msg := Mysqlx_Session.AuthenticateOk{
		AuthData: []byte(*value),
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	return xa.xcc.pkt.WritePacket(Mysqlx.ServerMessages_SESS_AUTHENTICATE_OK, data)
}

func (xa *XAuth) sendAuthContinue(value *string) error {
	msg := Mysqlx_Session.AuthenticateContinue{
		AuthData: []byte(*value),
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	return xa.xcc.pkt.WritePacket(Mysqlx.ServerMessages_SESS_AUTHENTICATE_CONTINUE, data)
}
