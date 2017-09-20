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

package protocol

import (
	"errors"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/tidb/util/arena"
)

func DumpIntBinary(value int64) ([]byte, error) {
	p := proto.NewBuffer([]byte{})
	if err := p.EncodeZigzag64(uint64(value)); err != nil {
		return nil, err
	}
	return p.Bytes(), nil
}

func DumpUIntBinary(value uint64) ([]byte, error) {
	p := proto.NewBuffer([]byte{})
	if err := p.EncodeVarint(uint64(value)); err != nil {
		return nil, err
	}
	return p.Bytes(), nil
}

func DumpStringBinary(b []byte, alloc arena.Allocator) []byte {
	data := alloc.Alloc(len(b) + 1)
	data = append(data, b...)
	data = append(data, byte(0))
	return data
}

func StrToXDecimal(str string) ([]byte, error) {
	if len(str) == 0 {
		return nil, nil
	}
	scale := 0
	dotPos := strings.Index(str, ".")
	slices := strings.Split(str, ".")
	if len(slices) > 2 {
		return nil, errors.New("invalid decimal")
	}
	if dotPos != -1 {
		scale = len(str) - dotPos - 1
	}
	dec := []byte{byte(scale)}
	sign := 0xc
	if strings.HasPrefix(slices[0], "-") || strings.HasPrefix(slices[0], "+") {
		if strings.HasPrefix(slices[0], "-") {
			sign = 0xd
		}
		slices[0] = slices[0][1:]
	}

	joined := ""
	for _, v := range slices {
		if _, err := strconv.Atoi(v); err != nil {
			return nil, errors.New("invalid decimal")
		}
		joined += v
	}

	log.Infof("[YUSP] joined: %s", joined)
	// Append two char into one byte.
	// If joined[i+1] is the last char, stop the loop.
	// If joined[i+2] is the last char, stop the loop in the next loop after append sign.
	for i := 0; i < len(joined); i += 2 {
		if i == len(joined)-1 {
			// If it is the last char of joined, append like the following.
			dec = append(dec, byte((int(joined[i])-int('0'))<<4|sign))
			sign = 0
			break
		}
		dec = append(dec, byte((int(joined[i])-int('0'))<<4|(int(joined[i+1])-int('0'))))
	}

	if sign != 0 {
		dec = append(dec, byte(sign<<4))
	}
	log.Infof("[YUSP] dec: %#o", dec)
	return dec, nil
}
