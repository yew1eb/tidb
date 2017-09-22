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
	"bytes"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/auth"
	xutil "github.com/pingcap/tidb/xprotocol/util"
	"net"
)

type authMysql41State int32

const (
	S_starting authMysql41State = iota
	S_waiting_response
	S_done
	S_error
)

type saslMysql41Auth struct {
	m_state authMysql41State
	m_salt  []byte

	xauth *XAuth
}

func (spa *saslMysql41Auth) handleStart(mechanism *string, data []byte, initial_response []byte) *response {
	r := response{}

	if spa.m_state == S_starting {
		spa.m_salt = util.RandomBuf(mysql.ScrambleLength)
		r.data = string(spa.m_salt)
		r.status = Ongoing
		r.errCode = 0
		spa.m_state = S_waiting_response
	} else {
		r.status = Error
		r.errCode = mysql.ErrNetPacketsOutOfOrder

		spa.m_state = S_error
	}

	return &r
}

func (spa *saslMysql41Auth) handleContinue(data []byte) *response {
	if spa.m_state == S_waiting_response {
		dbname, user, passwd := spa.extractNullTerminatedElement(data)
		xcc := spa.xauth.xcc
		xcc.dbname = string(dbname)
		xcc.user = string(user)

		spa.m_state = S_done
		if !spa.xauth.xcc.server.skipAuth() {
			// Do Auth
			addr := spa.xauth.xcc.conn.RemoteAddr().String()
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				return &response{
					status: Failed,
					data: xutil.ErrAccessDenied.ToSQLError().Message,
					errCode: xutil.ErrAccessDenied.ToSQLError().Code,
					}
			}
			if !spa.xauth.xcc.ctx.Auth(&auth.UserIdentity{Username: string(user), Hostname: host},
				passwd, spa.m_salt) {
				return &response{
					status: Failed,
					data: xutil.ErrAccessDenied.ToSQLError().Message,
					errCode: xutil.ErrAccessDenied.ToSQLError().Code,
				}
			}
		}

		return &response{
			status: Succeeded,
			errCode: 0,
		}
	} else {
		spa.m_state = S_error

		return &response{
			status: Error,
			errCode: mysql.ErrNetPacketsOutOfOrder,
			}
	}
}

func (spa *saslMysql41Auth) extractNullTerminatedElement(data []byte) ([]byte, []byte, []byte) {
	slices := bytes.Split(data, []byte{0})
	return slices[0], slices[1], slices[2]
}
