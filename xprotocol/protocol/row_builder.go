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
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/tidb/util/arena"
)

func DumpNullBinary() []byte {
	return nil
}

func DumpIntBinary(value int64) ([]byte, error) {
	b := []byte{}
	p := proto.NewBuffer(b)
	if err := p.EncodeZigzag64(uint64(value)); err != nil {
		return nil, err
	}
	return p.Bytes(), nil
}

func DumpUIntBinary(value uint64) ([]byte, error) {
	b := []byte{}
	p := proto.NewBuffer(b)
	if err := p.EncodeVarint(uint64(value)); err != nil {
		return nil, err
	}
	return p.Bytes(), nil
}

func DumpDecimalBinary() []byte {
	return nil
}

func DumpDoubleBinary() []byte {
	return nil
}

func DumpFloatBinary() []byte {
	return nil
}

func DumpDateBinary() []byte {
	return nil
}

func DumpTimeBinary() []byte {
	return nil
}

func DumpDatetimeBinary() []byte {
	return nil
}

func DumpSetBinary() []byte {
	return nil
}

func DumpBitBinary() []byte {
	return nil
}

func DumpStringBinary(b []byte, alloc arena.Allocator) []byte {
	data := alloc.Alloc(len(b) + 1)
	data = append(data, b...)
	data = append(data, byte(0))
	return data
}
