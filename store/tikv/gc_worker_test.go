// Copyright 2016 PingCAP, Inc.
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

package tikv_test

import (
	"math"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
)

type tikvStore interface {
	Close() error
}

type testGCWorkerSuite struct {
	store    tikvStore
	oracle   *oracles.MockOracle
	gcWorker *tikv.GCWorker
}

var _ = Suite(&testGCWorkerSuite{})

func (s *testGCWorkerSuite) SetUpTest(c *C) {
	s.oracle = &oracles.MockOracle{}
	store, _ := tikv.NewMockTikvStore(tikv.WithOracle(s.oracle))
	s.store = store.(tikvStore)
	_, err := tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	creator := func() (context.Context, error) { return tidb.CreateSession(store) }
	gcWorker, err := tikv.NewGCWorker(store, creator, true)
	c.Assert(err, IsNil)
	s.gcWorker = gcWorker
}

func (s *testGCWorkerSuite) TearDownTest(c *C) {
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testGCWorkerSuite) timeEqual(c *C, t1, t2 time.Time, epsilon time.Duration) {
	c.Assert(math.Abs(float64(t1.Sub(t2))), Less, float64(epsilon))
}

func (s *testGCWorkerSuite) TestGetOracleTime(c *C) {
	t1, err := s.gcWorker.getOracleTime()
	c.Assert(err, IsNil)
	s.timeEqual(c, time.Now(), t1, time.Millisecond*10)

	s.oracle.addOffset(time.Second * 10)
	t2, err := s.gcWorker.getOracleTime()
	c.Assert(err, IsNil)
	s.timeEqual(c, t2, t1.Add(time.Second*10), time.Millisecond*10)
}

func (s *testGCWorkerSuite) TestPrepareGC(c *C) {
	now, err := s.gcWorker.getOracleTime()
	c.Assert(err, IsNil)
	close(s.gcWorker.done)
	ok, _, err := s.gcWorker.prepare()
	c.Assert(err, IsNil)
	lastRun, err := s.gcWorker.loadTime(gcLastRunTimeKey, s.gcWorker.session)
	c.Assert(err, IsNil)
	c.Assert(lastRun, NotNil)
	safePoint, err := s.gcWorker.loadTime(gcSafePointKey, s.gcWorker.session)
	c.Assert(err, IsNil)
	s.timeEqual(c, safePoint.Add(gcDefaultLifeTime), now, 2*time.Second)

	// Change GC run interval.
	err = s.gcWorker.saveDuration(gcRunIntervalKey, time.Minute*5)
	c.Assert(err, IsNil)
	s.oracle.addOffset(time.Minute * 4)
	ok, _, err = s.gcWorker.prepare()
	c.Assert(err, IsNil)
	c.Assert(ok, IsFalse)
	s.oracle.addOffset(time.Minute * 2)
	ok, _, err = s.gcWorker.prepare()
	c.Assert(err, IsNil)
	c.Assert(ok, IsTrue)

	// Change GC life time.
	err = s.gcWorker.saveDuration(gcLifeTimeKey, time.Minute*30)
	c.Assert(err, IsNil)
	s.oracle.addOffset(time.Minute * 5)
	ok, _, err = s.gcWorker.prepare()
	c.Assert(err, IsNil)
	c.Assert(ok, IsFalse)
	s.oracle.addOffset(time.Minute * 40)
	now, err = s.gcWorker.getOracleTime()
	c.Assert(err, IsNil)
	ok, _, err = s.gcWorker.prepare()
	c.Assert(err, IsNil)
	c.Assert(ok, IsTrue)
	safePoint, err = s.gcWorker.loadTime(gcSafePointKey, s.gcWorker.session)
	c.Assert(err, IsNil)
	s.timeEqual(c, safePoint.Add(time.Minute*30), now, 2*time.Second)
}
