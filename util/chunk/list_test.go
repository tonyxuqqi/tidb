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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

func (s *testChunkSuite) TestList(c *check.C) {
	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
	}
	l := NewList(fields, 2, 2)
	srcChunk := NewChunkWithCapacity(fields, 32)
	srcChunk.AppendInt64(0, 1)
	srcRow := srcChunk.GetRow(0)

	// Test basic append.
	for i := 0; i < 5; i++ {
		l.AppendRow(srcRow)
	}
	c.Assert(l.NumChunks(), check.Equals, 3)
	c.Assert(l.Len(), check.Equals, 5)
	c.Assert(len(l.freelist), check.Equals, 0)

	// Test chunk reuse.
	l.Reset()
	c.Assert(len(l.freelist), check.Equals, 3)

	for i := 0; i < 5; i++ {
		l.AppendRow(srcRow)
	}
	c.Assert(len(l.freelist), check.Equals, 0)

	// Test add chunk then append row.
	l.Reset()
	nChunk := NewChunkWithCapacity(fields, 32)
	nChunk.AppendNull(0)
	l.Add(nChunk)
	ptr := l.AppendRow(srcRow)
	c.Assert(l.NumChunks(), check.Equals, 2)
	c.Assert(ptr.ChkIdx, check.Equals, uint32(1))
	c.Assert(ptr.RowIdx, check.Equals, uint32(0))
	row := l.GetRow(ptr)
	c.Assert(row.GetInt64(0), check.Equals, int64(1))

	// Test iteration.
	l.Reset()
	for i := 0; i < 5; i++ {
		tmp := NewChunkWithCapacity(fields, 32)
		tmp.AppendInt64(0, int64(i))
		l.AppendRow(tmp.GetRow(0))
	}
	expected := []int64{0, 1, 2, 3, 4}
	var results []int64
	err := l.Walk(func(r Row) error {
		results = append(results, r.GetInt64(0))
		return nil
	})
	c.Assert(err, check.IsNil)
	c.Assert(results, check.DeepEquals, expected)
}

func (s *testChunkSuite) TestListMemoryUsage(c *check.C) {
	fieldTypes := make([]*types.FieldType, 0, 5)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeJSON})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDatetime})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDuration})

	jsonObj, err := json.ParseBinaryFromString("1")
	c.Assert(err, check.IsNil)
	timeObj := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeDatetime, 0)
	durationObj := types.Duration{Duration: math.MaxInt64, Fsp: 0}

	maxChunkSize := 2
	srcChk := NewChunkWithCapacity(fieldTypes, maxChunkSize)
	srcChk.AppendFloat32(0, 12.4)
	srcChk.AppendString(1, "123")
	srcChk.AppendJSON(2, jsonObj)
	srcChk.AppendTime(3, timeObj)
	srcChk.AppendDuration(4, durationObj)

	list := NewList(fieldTypes, maxChunkSize, maxChunkSize*2)
	c.Assert(list.GetMemTracker().BytesConsumed(), check.Equals, int64(0))

	list.AppendRow(srcChk.GetRow(0))
	c.Assert(list.GetMemTracker().BytesConsumed(), check.Equals, int64(0))

	memUsage := list.chunks[0].MemoryUsage()
	list.Reset()
	c.Assert(list.GetMemTracker().BytesConsumed(), check.Equals, memUsage)

	list.Add(srcChk)
	c.Assert(list.GetMemTracker().BytesConsumed(), check.Equals, memUsage+srcChk.MemoryUsage())
}

func BenchmarkListMemoryUsage(b *testing.B) {
	fieldTypes := make([]*types.FieldType, 0, 4)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDatetime})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: mysql.TypeDuration})

	chk := NewChunkWithCapacity(fieldTypes, 2)
	timeObj := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeDatetime, 0)
	durationObj := types.Duration{Duration: math.MaxInt64, Fsp: 0}
	chk.AppendFloat64(0, 123.123)
	chk.AppendString(1, "123")
	chk.AppendTime(2, timeObj)
	chk.AppendDuration(3, durationObj)
	row := chk.GetRow(0)

	initCap := 50
	list := NewList(fieldTypes, 2, 8)
	for i := 0; i < initCap; i++ {
		list.AppendRow(row)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list.GetMemTracker().BytesConsumed()
	}
}

func BenchmarkListAdd(b *testing.B) {
	numChk, numRow := 1, 2
	chks, fields := initChunks(numChk, numRow)
	chk := chks[0]
	l := NewList(fields, numRow, numRow)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Add(chk)
	}
}

func BenchmarkListGetRow(b *testing.B) {
	numChk, numRow := 10000, 2
	chks, fields := initChunks(numChk, numRow)
	l := NewList(fields, numRow, numRow)
	for _, chk := range chks {
		l.Add(chk)
	}
	rand.Seed(0)
	ptrs := make([]RowPtr, 0, b.N)
	for i := 0; i < mathutil.Min(b.N, 10000); i++ {
		ptrs = append(ptrs, RowPtr{
			ChkIdx: rand.Uint32() % uint32(numChk),
			RowIdx: rand.Uint32() % uint32(numRow),
		})
	}
	for i := 10000; i < cap(ptrs); i++ {
		ptrs = append(ptrs, ptrs[i%10000])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.GetRow(ptrs[i])
	}
}
