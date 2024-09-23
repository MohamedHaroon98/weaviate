//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	bolt "go.etcd.io/bbolt"
)

type keyExistsOnUpperSegmentsFn func(key []byte) (bool, error)

func (sg *SegmentGroup) makeKeyExistsOnUpperSegments(segmentIdx int) keyExistsOnUpperSegmentsFn {
	return func(key []byte) (bool, error) {
		if !IsExpectedStrategy(sg.strategy, StrategyReplace) {
			return false, fmt.Errorf("has only possible for strategy %q", StrategyReplace)
		}

		for i := segmentIdx + 1; i < sg.Len(); i++ {
			has, err := sg.segments[i].has(key)
			if err != nil {
				return false, err
			}
			if has {
				return true, nil
			}
		}
		return false, nil
	}
}

func (sg *SegmentGroup) cleanupOnce(shouldAbort cyclemanager.ShouldAbortCallback) (bool, error) {
	// validate supported strategy
	switch sg.strategy {
	case StrategyReplace:
		// supported, continue
	case StrategySetCollection, StrategyMapCollection, StrategyRoaringSet:
		// TODO AL: add support for other strategies in the future
		return false, nil
	default:
		return false, fmt.Errorf("unrecognized strategy %q", sg.strategy)
	}

	// fmt.Printf("  ==> cleanupOnce %q strategy %q\n", sg.dir, sg.strategy)

	// TODO AL: take shouldAbort into account

	segmentIdx, err := sg.findCleanupCandidate()
	if err != nil {
		return false, err
	}
	if segmentIdx == -1 {
		return false, nil
	}

	if sg.allocChecker != nil {
		// allocChecker is optional
		if err := sg.allocChecker.CheckAlloc(100 * 1024 * 1024); err != nil {
			// if we don't have at least 100MB to spare, don't start a cleanup. A
			// cleanup does not actually need a 100MB, but it will create garbage
			// that needs to be cleaned up. If we're so close to the memory limit, we
			// can increase stability by preventing anything that's not strictly
			// necessary. Cleanup can simply resume when the cluster has been
			// scaled.
			sg.logger.WithFields(logrus.Fields{
				"action": "lsm_compaction",
				"event":  "compaction_skipped_oom",
				"path":   sg.dir,
			}).WithError(err).
				Warnf("skipping compaction due to memory pressure")

			return false, nil
		}
	}

	segment := sg.segmentAtPos(segmentIdx)

	tmpSegmentPath := filepath.Join(sg.dir, "segment-"+segmentID(segment.path)+".db.tmp")
	scratchSpacePath := segment.path + "cleanup.scratch.d"

	file, err := os.Create(tmpSegmentPath)
	if err != nil {
		return false, err
	}

	switch sg.strategy {
	case StrategyReplace:
		c := newSegmentCleanerReplace(file, segment.newCursor(),
			sg.makeKeyExistsOnUpperSegments(segmentIdx), segment.level,
			segment.secondaryIndexCount, scratchSpacePath)
		if err := c.do(); err != nil {
			// fmt.Printf("  ==> cleanup error %s\n\n", err)

			return false, err
		}
	}

	if err := file.Sync(); err != nil {
		return false, fmt.Errorf("fsync cleaned segment file: %w", err)
	}
	if err := file.Close(); err != nil {
		return false, fmt.Errorf("close cleaned segment file: %w", err)
	}

	if err := sg.replaceCleanedSegment(segmentIdx, tmpSegmentPath); err != nil {
		return false, fmt.Errorf("replace compacted segments: %w", err)
	}

	return true, nil
}

var cleanupBucket = []byte("cleanup")
var cleanupInterval = time.Minute // TODO AL: env configured

func (sg *SegmentGroup) findCleanupCandidate() (int, error) {
	noCandidate := -1

	if sg.isReadyOnly() {
		fmt.Printf("  ==> no candidate / read only\n")
		return noCandidate, nil
	}

	var l int
	var ids []uint64
	var sizes []int64

	err := func() error {
		sg.maintenanceLock.RLock()
		defer sg.maintenanceLock.RUnlock()

		if l = len(sg.segments); l > 1 {
			ids = make([]uint64, l)
			sizes = make([]int64, l)

			for i, seg := range sg.segments {
				id, err := strconv.ParseUint(segmentID(seg.path), 10, 64)
				if err != nil {
					return fmt.Errorf("parse segment id %q: %w", segmentID(seg.path), err)
				}
				ids[i] = id
				sizes[i] = seg.size
			}
		}
		return nil
	}()
	if err != nil {
		fmt.Printf("  ==> no candidate / segments read, err [%s]\n", err)
		return noCandidate, err
	}
	if l <= 1 {
		fmt.Printf("  ==> no candidate / len [%d]\n", l)
		return noCandidate, nil
	}

	path := filepath.Join(sg.dir, "cleanup.db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		fmt.Printf("  ==> no candidate / open bolt, err [%s]\n", err)
		return noCandidate, fmt.Errorf("open cleanup bolt %q: %w", path, err)
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(cleanupBucket)
		return err
	})
	if err != nil {
		fmt.Printf("  ==> no candidate / create bucket, err [%s]\n", err)
		return noCandidate, fmt.Errorf("create cleanup bolt %q: %w", path, err)
	}

	now := time.Now()
	tsOldest := now.UnixNano()
	tsThreshold := now.Add(-cleanupInterval).UnixNano()

	kToDelete := [][]byte{}
	candidateIdx := noCandidate

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(cleanupBucket)
		c := b.Cursor()

		idx := 0
		ck, cv := c.First()

		// no point cleaning last segment, therefore "l-1"
		for ck != nil && idx < l-1 {
			cid := binary.BigEndian.Uint64(ck)
			id := ids[idx]

			fmt.Printf("  ==> loop cid [%d] id [%d]\n", cid, id)

			if id > cid {
				// id no longer exists, to be removed from bolt
				kToDelete = append(kToDelete, ck)
				ck, cv = c.Next()
			} else if id < cid {
				// id not yet present in bolt
				if tsOldest > 0 {
					tsOldest = 0
					candidateIdx = idx
				}
				idx++
			} else {
				// id present in bolt
				cts := int64(binary.BigEndian.Uint64(cv))
				if tsOldest > cts {
					tsOldest = cts
					candidateIdx = idx
				}
				ck, cv = c.Next()
				idx++
			}
		}
		// in case 1st loop finished due to idx reached len
		for ; ck != nil; ck, _ = c.Next() {
			cid := binary.BigEndian.Uint64(ck)
			if cid != ids[l-1] {
				kToDelete = append(kToDelete, ck)
			}
		}
		// in case 1st loop finished due to cursor finished
		for ; idx < l-1 && tsOldest > 0; idx++ {
			tsOldest = 0
			candidateIdx = idx
		}
		return nil
	})
	if err != nil {
		fmt.Printf("  ==> no candidate / searching, err [%s]\n", err)
		return noCandidate, fmt.Errorf("searching cleanup bolt %q: %w", path, err)
	}

	hasDeletes := len(kToDelete) > 0
	hasCandidate := candidateIdx != noCandidate && tsOldest < tsThreshold

	if hasDeletes || hasCandidate {
		err = db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(cleanupBucket)

			for _, k := range kToDelete {
				if err := b.Delete(k); err != nil {
					return err
				}
			}
			if hasCandidate {
				bufK := make([]byte, 8)
				bufV := make([]byte, 8)

				fmt.Printf("  ==> storing candidate idx [%d] id [%d] ts [%d]\n",
					candidateIdx, ids[candidateIdx], tsOldest)

				binary.BigEndian.PutUint64(bufK, ids[candidateIdx])
				binary.BigEndian.PutUint64(bufV, uint64(now.UnixNano()))
				return b.Put(bufK, bufV)
			}
			return nil
		})
		if err != nil {
			fmt.Printf("  ==> no candidate / updating, err [%s]\n", err)
			return noCandidate, fmt.Errorf("updating cleanup bolt %q: %w", path, err)
		}
		if hasCandidate {
			fmt.Printf("  ==> candidate! [%d]\n", candidateIdx)
			return candidateIdx, nil
		}
	}
	fmt.Printf("  ==> no candidate / end\n")
	return noCandidate, nil
}

func (sg *SegmentGroup) replaceCleanedSegment(segmentIdx int, tmpSegmentPath string,
) error {
	oldSegment := sg.segmentAtPos(segmentIdx)
	countNetAdditions := oldSegment.countNetAdditions

	precomputedFiles, err := preComputeSegmentMeta(tmpSegmentPath, countNetAdditions,
		sg.logger, sg.useBloomFilter, sg.calcCountNetAdditions)
	if err != nil {
		return fmt.Errorf("precompute segment meta: %w", err)
	}

	sg.maintenanceLock.Lock()
	defer sg.maintenanceLock.Unlock()

	if err := oldSegment.close(); err != nil {
		return fmt.Errorf("close disk segment %q: %w", oldSegment.path, err)
	}
	if err := oldSegment.drop(); err != nil {
		return fmt.Errorf("drop disk segment %q: %w", oldSegment.path, err)
	}
	if err := fsync(sg.dir); err != nil {
		return fmt.Errorf("fsync segment directory %q: %w", sg.dir, err)
	}

	segmentId := segmentID(oldSegment.path)
	var segmentPath string

	// the old segment have been deleted, we can now safely remove the .tmp
	// extension from the new segment itself and the pre-computed files
	for i, tmpPath := range precomputedFiles {
		path, err := sg.stripTmpExtension(tmpPath, segmentId, segmentId)
		if err != nil {
			return fmt.Errorf("strip .tmp extension of new segment %q: %w", tmpPath, err)
		}
		if i == 0 {
			// the first element in the list is the segment itself
			segmentPath = path
		}
	}

	newSegment, err := newSegment(segmentPath, sg.logger, sg.metrics, nil,
		sg.mmapContents, sg.useBloomFilter, sg.calcCountNetAdditions, false)
	if err != nil {
		return fmt.Errorf("create new segment %q: %w", newSegment.path, err)
	}

	sg.segments[segmentIdx] = newSegment
	return nil
}
