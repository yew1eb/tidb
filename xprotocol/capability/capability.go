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

package capability

import (
	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Connection"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
)

// CheckCapabilitiesPrepareSetMsg deals the initial capabilities set message of client.
func CheckCapabilitiesPrepareSetMsg(tp Mysqlx.ClientMessages_Type, msg []byte) ([]Handler, error) {
	if tp != Mysqlx.ClientMessages_CON_CAPABILITIES_SET {
		log.Infof("Invalid message %d received during client initialization", tp.String())
		return nil, util.ErXBadMessage
	}
	var set Mysqlx_Connection.CapabilitiesSet
	if err := set.Unmarshal(msg); err != nil {
		return nil, errors.Trace(err)
	}
	if set.GetCapabilities() == nil {
		return nil, errors.New("bad capabilities set")
	}
	caps := set.GetCapabilities().GetCapabilities()
	if caps == nil {
		return nil, errors.New("bad capabilities set")
	}
	if caps[0].GetName() != "client.pwd_expire_ok" {
		return nil, errors.New("bad capabilities set")
	}
	if caps[0].GetValue().GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return nil, errors.New("bad capabilities set")
	}
	if caps[0].GetValue().GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_BOOL {
		return nil, errors.New("bad capabilities set")
	}
	if !caps[0].GetValue().GetScalar().GetVBool() {
		return nil, errors.New("bad capabilities set")
	}
	return []Handler{
		&HandlerExpiredPasswords{
			Name: "client.pwd_expire_ok",
			Expired: true},
			}, nil
}

// CheckCapabilitiesGetMsg deals capabilities get message, get message content will always be empty.
func CheckCapabilitiesGetMsg(tp Mysqlx.ClientMessages_Type, _ []byte) error {
	if tp != Mysqlx.ClientMessages_CON_CAPABILITIES_GET {
		return errors.New("bad capabilities get")
	}
	return nil
}

// CheckCapabilitiesSetMsg deals the second capabilities set message.
func CheckCapabilitiesSetMsg(tp Mysqlx.ClientMessages_Type, msg []byte) error {
	if tp != Mysqlx.ClientMessages_CON_CAPABILITIES_SET {
		return errors.New("bad capabilities set")
	}
	var set Mysqlx_Connection.CapabilitiesSet
	if err := set.Unmarshal(msg); err != nil {
		return errors.Trace(err)
	}
	if set.GetCapabilities() == nil {
		return errors.New("bad capabilities set")
	}
	caps := set.GetCapabilities().GetCapabilities()
	if caps == nil {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetName() != "tls" {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetValue().GetType() != Mysqlx_Datatypes.Any_SCALAR {
		return errors.New("bad capabilities set")
	}
	if caps[0].GetValue().GetScalar().GetType() != Mysqlx_Datatypes.Scalar_V_BOOL {
		return errors.New("bad capabilities set")
	}
	if !caps[0].GetValue().GetScalar().GetVBool() {
		return errors.New("bad capabilities set")
	}
	return nil
}
