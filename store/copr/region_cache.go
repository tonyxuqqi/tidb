// Copyright 2021 PingCAP, Inc.
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

package copr

import (
	"bytes"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"go.uber.org/zap"
)

// RegionCache wraps tikv.RegionCache.
type RegionCache struct {
	*tikv.RegionCache
}

// NewRegionCache returns a new RegionCache.
func NewRegionCache(rc *tikv.RegionCache) *RegionCache {
	return &RegionCache{rc}
}

// SplitRegionRanges gets the split ranges from pd region.
func (c *RegionCache) SplitRegionRanges(bo *Backoffer, keyRanges []kv.KeyRange) ([]kv.KeyRange, error) {
	ranges := NewKeyRanges(keyRanges)

	locations, err := c.SplitKeyRangesByLocations(bo, ranges, true) // TODO: add config
	if err != nil {
		return nil, errors.Trace(err)
	}
	var ret []kv.KeyRange
	for _, loc := range locations {
		for i := 0; i < loc.Ranges.Len(); i++ {
			ret = append(ret, loc.Ranges.At(i))
		}
	}
	return ret, nil
}

// LocationKeyRanges wrapps a real Location in PD and its logical ranges info.
type LocationKeyRanges struct {
	// Location is the real location in PD.
	Location *tikv.KeyLocation
	// Ranges is the logic ranges the current Location contains.
	Ranges *KeyRanges
}

// SplitKeyRangesByLocations splits the KeyRanges by logical info in the cache.
func (c *RegionCache) SplitKeyRangesByLocations(bo *Backoffer, ranges *KeyRanges, enableSplitOnBucket bool) ([]*LocationKeyRanges, error) {
	res := make([]*LocationKeyRanges, 0)

	isInBucket := func(bucket *metapb.RegionBucket, key []byte) bool {
		return bytes.Compare(bucket.StartKey, key) <= 0 &&
			(bytes.Compare(key, bucket.EndKey) < 0 || len(bucket.EndKey) == 0)
	}

	isInRegionOrBucket := func(loc *tikv.KeyLocation, bucketIdx int, useBucket bool, key []byte) bool {
		if !useBucket {
			return loc.Contains(key)
		}
		return isInBucket(loc.Buckets[bucketIdx], key)
	}

	equalRegionOrBucketEndKey := func(loc *tikv.KeyLocation, bucketIdx int, useBucket bool, key []byte) bool {
		if !useBucket {
			return bytes.Equal(loc.EndKey, key)
		}
		return bytes.Equal(loc.Buckets[bucketIdx].EndKey, key)
	}
	var loc *tikv.KeyLocation
	var err error
	bucketIdx := 0
	regionCount := 0
	rangeCount := ranges.Len()
	for ranges.Len() > 0 {
		if loc == nil || bucketIdx >= len(loc.Buckets) || !enableSplitOnBucket {
			loc, err = c.LocateKey(bo.TiKVBackoffer(), ranges.At(0).StartKey)
			if err != nil {
				return res, errors.Trace(err)
			}
			bucketIdx = 0
			regionCount++
		}
		useBucket := enableSplitOnBucket && len(loc.Buckets) != 0

		// Iterate to the first range that is not complete in the region bucket.
		var i int
		for ; i < ranges.Len(); i++ {
			r := ranges.At(i)
			if !(isInRegionOrBucket(loc, bucketIdx, useBucket, r.EndKey) || equalRegionOrBucketEndKey(loc, bucketIdx, useBucket, r.EndKey)) {
				break
			}
		}
		// All rest ranges belong to the same region bucket.
		if i == ranges.Len() {
			res = append(res, &LocationKeyRanges{Location: loc, Ranges: ranges})
			bucketIdx++
			break
		}

		r := ranges.At(i)
		if isInRegionOrBucket(loc, bucketIdx, useBucket, r.StartKey) {
			// Part of r is not in the region bucket. We may need to split it if the range cross more than two region buckets or cross one region.
			endKey := loc.EndKey
			lastIncludedIdx := i - 1
			if useBucket {
				if len(loc.Buckets) > bucketIdx+1 && (isInRegionOrBucket(loc, bucketIdx+1, useBucket, r.EndKey) || equalRegionOrBucketEndKey(loc, bucketIdx+1, useBucket, r.EndKey)) {
					endKey = r.EndKey // EndKey belong to next region bucket
					lastIncludedIdx = i
				} else {
					endKey = loc.Buckets[bucketIdx].EndKey
				}
			}

			taskRanges := ranges.Slice(0, lastIncludedIdx+1)
			if lastIncludedIdx == i-1 {
				taskRanges.last = &kv.KeyRange{
					StartKey: r.StartKey,
					EndKey:   endKey,
				}
			}
			res = append(res, &LocationKeyRanges{Location: loc, Ranges: taskRanges})
			ranges = ranges.Slice(i+1, ranges.Len())
			if lastIncludedIdx == i-1 {
				ranges.first = &kv.KeyRange{
					StartKey: endKey,
					EndKey:   r.EndKey,
				}
			}
		} else if i != 0 {
			// rs[i] is not in the region bucket.
			taskRanges := ranges.Slice(0, i)
			res = append(res, &LocationKeyRanges{Location: loc, Ranges: taskRanges})
			ranges = ranges.Slice(i, ranges.Len())
		}
		bucketIdx++
	}
	if enableSplitOnBucket && len(res) != regionCount {
		logutil.Logger(bo.GetCtx()).Info("SplitKeyRangesByLocations",
			zap.Int("Res.size", len(res)), zap.Int("RegionCount", regionCount), zap.Int("KeyRangeCount", rangeCount))
	}
	return res, nil
}

// OnSendFailForBatchRegions handles send request fail logic.
func (c *RegionCache) OnSendFailForBatchRegions(bo *Backoffer, store *tikv.Store, regionInfos []RegionInfo, scheduleReload bool, err error) {
	metrics.RegionCacheCounterWithSendFail.Add(float64(len(regionInfos)))
	if !store.IsTiFlash() {
		logutil.Logger(bo.GetCtx()).Info("Should not reach here, OnSendFailForBatchRegions only support TiFlash")
		return
	}
	for _, ri := range regionInfos {
		if ri.Meta == nil {
			continue
		}
		c.OnSendFailForTiFlash(bo.TiKVBackoffer(), store, ri.Region, ri.Meta, scheduleReload, err)
	}
}
