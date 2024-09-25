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

// ================================================================

func newSegmentCleaner(sg *SegmentGroup) (segmentCleaner, error) {
	if sg.cleanupInterval <= 0 {
		return &segmentCleanerNoop{}, nil
	}

	switch sg.strategy {
	case StrategyReplace:
		cleaner := &segmentCleanerImpl{sg: sg}
		if err := cleaner.init(); err != nil {
			return nil, err
		}
		return cleaner, nil
	case StrategyMapCollection,
		StrategySetCollection,
		StrategyRoaringSet:
		// TODO AL: add roaring set range
		return &segmentCleanerNoop{}, nil
	default:
		return nil, fmt.Errorf("unrecognized strategy %q", sg.strategy)
	}
}

type segmentCleaner interface {
	close() error
	cleanupOnce(shouldAbort cyclemanager.ShouldAbortCallback) (cleaned bool, err error)
}

type segmentCleanerNoop struct{}

func (c *segmentCleanerNoop) close() error {
	return nil
}

func (c *segmentCleanerNoop) cleanupOnce(shouldAbort cyclemanager.ShouldAbortCallback) (bool, error) {
	return false, nil
}

// TODO AL: rename
type segmentCleanerImpl struct {
	sg *SegmentGroup
	db *bolt.DB
}

func (c *segmentCleanerImpl) init() error {
	path := filepath.Join(c.sg.dir, cleanupDbFileName)
	var db *bolt.DB
	var err error

	if db, err = bolt.Open(path, 0o600, nil); err != nil {
		return fmt.Errorf("open cleanup bolt db %q: %w", path, err)
	}

	if err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(cleanupSegmentsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(cleanupMetaBucket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return fmt.Errorf("create bucket cleanup bolt db %q: %w", path, err)
	}

	c.db = db
	return nil
}

func (c *segmentCleanerImpl) close() error {
	if err := c.db.Close(); err != nil {
		return fmt.Errorf("close cleanup bolt db %q: %w", c.db.Path(), err)
	}
	return nil
}

func (c *segmentCleanerImpl) findCandidate() (int, onCompletedFunc, error) {
	noCandidate := -1

	if c.sg.isReadyOnly() {
		fmt.Printf("  ==> no candidate / read only\n")
		return noCandidate, nil, nil
	}

	nowTs := time.Now().UnixNano()
	nextAllowedTs := nowTs - int64(c.sg.cleanupInterval)
	nextAllowedStoredTs := c.readNextAllowed()

	t := func(ts int64) time.Time {
		return time.Unix(0, 0).Add(time.Duration(ts))
	}

	if nextAllowedStoredTs > nextAllowedTs {
		// Too soon for next cleanup
		fmt.Printf("  ==> no candidate / earliestAllowedStoredTs [%s] > earliestAllowedTs [%s]\n",
			t(nextAllowedStoredTs), t(nextAllowedTs))
		return noCandidate, nil, nil
	}
	fmt.Printf("  ==> CONTINUING  / earliestAllowedStoredTs [%s] <= earliestAllowedTs [%s]\n",
		t(nextAllowedStoredTs), t(nextAllowedTs))

	ids, sizes, err := c.getSegmentIdsAndSizes()
	if err != nil {
		fmt.Printf("  ==> no candidate / segments read, err [%s]\n", err)
		return noCandidate, nil, err
	}
	if count := len(ids); count <= 1 {
		// Too few segments for cleanup
		if err := c.storeNextAllowed(nowTs); err != nil {
			fmt.Printf("  ==> no candidate / len [%d], err [%s]\n", count, err)
			return noCandidate, nil, err
		}
		fmt.Printf("  ==> no candidate / len [%d]\n", count)
		return noCandidate, nil, nil
	}

	candidateIdx, earliestCleanedTs, nonExistentSegmentKeys := c.readEarliestCleaned(ids, sizes, noCandidate, nowTs)

	if err := c.deleteSegmentMetas(nonExistentSegmentKeys); err != nil {
		fmt.Printf("  ==> no candidate / deleting, err [%s]\n", err)
		return noCandidate, nil, err
	}

	if candidateIdx != noCandidate && earliestCleanedTs <= nextAllowedTs {
		// candidate found
		id := ids[candidateIdx]
		onCompleted := func(newSize int64) error {
			fmt.Printf("  ==> storing candidate idx [%d] id [%d] ts [%s]\n",
				candidateIdx, id, t(earliestCleanedTs))
			return c.storeSegmentMeta(id, newSize, nowTs)
		}
		fmt.Printf("  ==> candidate! [%d][%d]\n", candidateIdx, id)
		return candidateIdx, onCompleted, nil
	}

	if err := c.storeNextAllowed(earliestCleanedTs); err != nil {
		fmt.Printf("  ==> no candidate / updated earliestCleanedTs [%s] ; nextAllowedTs [%s], err [%s]\n",
			t(earliestCleanedTs), t(nextAllowedTs), err)
		return noCandidate, nil, err
	}

	fmt.Printf("  ==> no candidate / updated earliestCleanedTs [%s] ; nextAllowedTs [%s]\n",
		t(earliestCleanedTs), t(nextAllowedTs))
	return noCandidate, nil, nil
}

func (c *segmentCleanerImpl) getSegmentIdsAndSizes() ([]int64, []int64, error) {
	c.sg.maintenanceLock.RLock()
	defer c.sg.maintenanceLock.RUnlock()

	var ids []int64
	var sizes []int64

	if count := len(c.sg.segments); count > 1 {
		ids = make([]int64, count)
		sizes = make([]int64, count)

		for i, seg := range c.sg.segments {
			segId := segmentID(seg.path)
			id, err := strconv.ParseInt(segId, 10, 64)
			if err != nil {
				return nil, nil, fmt.Errorf("parse segment id %q: %w", segId, err)
			}
			ids[i] = id
			sizes[i] = seg.size
		}
	}

	return ids, sizes, nil
}

func (c *segmentCleanerImpl) readNextAllowed() int64 {
	ts := int64(0)
	c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(cleanupMetaBucket)
		v := b.Get(cleanupMetaKeyNextAllowedTs)
		if v != nil {
			ts = int64(binary.BigEndian.Uint64(v))
		}
		return nil
	})
	return ts
}

func (c *segmentCleanerImpl) storeNextAllowed(ts int64) error {
	if err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(cleanupMetaBucket)
		bufV := make([]byte, 8)

		binary.BigEndian.PutUint64(bufV, uint64(ts))
		return b.Put(cleanupMetaKeyNextAllowedTs, bufV)
	}); err != nil {
		return fmt.Errorf("updating cleanup bolt db %q: %w", c.db.Path(), err)
	}
	return nil
}

func (c *segmentCleanerImpl) deleteSegmentMetas(segIds [][]byte) error {
	if len(segIds) > 0 {
		if err := c.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(cleanupSegmentsBucket)
			for _, k := range segIds {
				if err := b.Delete(k); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("deleting from cleanup bolt db %q: %w", c.db.Path(), err)
		}
	}
	return nil
}

func (c *segmentCleanerImpl) readEarliestCleaned(ids, sizes []int64,
	candidateIdx int, earliestCleanedTs int64,
) (int, int64, [][]byte) {
	count := len(ids)
	nonExistentSegmentKeys := [][]byte{}

	t := func(ts int64) time.Time {
		return time.Unix(0, 0).Add(time.Duration(ts))
	}

	c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(cleanupSegmentsBucket)
		c := b.Cursor()

		idx := 0
		ck, cv := c.First()

		// last segment does not need to be cleaned, therefore "count-1"
		for ck != nil && idx < count-1 {
			cid := int64(binary.BigEndian.Uint64(ck))
			id := ids[idx]

			fmt.Printf("  ==> loop cid [%d] id [%d]\n", cid, id)

			if id > cid {
				fmt.Printf("    ==> id > cid ; deleting cid\n")
				// id no longer exists, to be removed from bolt
				nonExistentSegmentKeys = append(nonExistentSegmentKeys, ck)
				ck, cv = c.Next()
			} else if id < cid {
				fmt.Printf("    ==> id < cid ; earliestCleanedTs [%s]\n", t(earliestCleanedTs))
				// id not yet present in bolt
				if earliestCleanedTs > 0 {
					fmt.Printf("    ==> earliestCleanedTs > 0\n")
					earliestCleanedTs = 0
					candidateIdx = idx
				}
				idx++
			} else {
				// id present in bolt
				csize := int64(binary.BigEndian.Uint64(cv[8:16]))
				size := sizes[idx]
				fmt.Printf("    ==> id = cid ; size [%d] csize [%d]\n", size, csize)
				if size != csize {
					cts := int64(binary.BigEndian.Uint64(cv[0:8]))
					fmt.Printf("    ==> size != csize ; earliestCleanedTs [%s] cts [%s]\n", t(earliestCleanedTs), t(cts))
					if earliestCleanedTs > cts {
						fmt.Printf("    ==> earliestCleanedTs > cts\n")
						earliestCleanedTs = cts
						candidateIdx = idx
					}
				}
				ck, cv = c.Next()
				idx++
			}
		}
		// in case main loop finished due to idx reached count first
		for ; ck != nil; ck, _ = c.Next() {
			cid := int64(binary.BigEndian.Uint64(ck))
			fmt.Printf("  ==> cursor loop ; cid [%d]\n", cid)
			if cid != ids[count-1] {
				nonExistentSegmentKeys = append(nonExistentSegmentKeys, ck)
			}
		}
		// in case main loop finished due to cursor reached end first
		for ; idx < count-1 && earliestCleanedTs > 0; idx++ {
			fmt.Printf("  ==> idx loop ; earliestCleanedTs [%s]\n", t(earliestCleanedTs))
			earliestCleanedTs = 0
			candidateIdx = idx
		}
		return nil
	})
	return candidateIdx, earliestCleanedTs, nonExistentSegmentKeys
}

func (c *segmentCleanerImpl) storeSegmentMeta(id, size, cleanedTs int64) error {
	if err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(cleanupSegmentsBucket)
		bufK := make([]byte, 8)
		bufV := make([]byte, 16)

		binary.BigEndian.PutUint64(bufK, uint64(id))
		binary.BigEndian.PutUint64(bufV[0:8], uint64(cleanedTs))
		binary.BigEndian.PutUint64(bufV[8:16], uint64(size))
		return b.Put(bufK, bufV)
	}); err != nil {
		return fmt.Errorf("updating cleanup bolt db %q: %w", c.db.Path(), err)
	}
	return nil
}

func (c *segmentCleanerImpl) cleanupOnce(shouldAbort cyclemanager.ShouldAbortCallback,
) (bool, error) {
	// TODO AL: take shouldAbort into account

	segmentIdx, onCompleted, err := c.findCandidate()
	if err != nil {
		return false, err
	}
	if segmentIdx == -1 {
		return false, nil
	}

	if c.sg.allocChecker != nil {
		// allocChecker is optional
		if err := c.sg.allocChecker.CheckAlloc(100 * 1024 * 1024); err != nil {
			// if we don't have at least 100MB to spare, don't start a cleanup. A
			// cleanup does not actually need a 100MB, but it will create garbage
			// that needs to be cleaned up. If we're so close to the memory limit, we
			// can increase stability by preventing anything that's not strictly
			// necessary. Cleanup can simply resume when the cluster has been
			// scaled.
			c.sg.logger.WithFields(logrus.Fields{
				"action": "lsm_compaction",
				"event":  "compaction_skipped_oom",
				"path":   c.sg.dir,
			}).WithError(err).
				Warnf("skipping compaction due to memory pressure")

			return false, nil
		}
	}

	segment := c.sg.segmentAtPos(segmentIdx)

	tmpSegmentPath := filepath.Join(c.sg.dir, "segment-"+segmentID(segment.path)+".db.tmp")
	scratchSpacePath := segment.path + "cleanup.scratch.d"

	file, err := os.Create(tmpSegmentPath)
	if err != nil {
		return false, err
	}

	switch c.sg.strategy {
	case StrategyReplace:
		c := newSegmentCleanerReplace(file, segment.newCursor(),
			c.sg.makeKeyExistsOnUpperSegments(segmentIdx), segment.level,
			segment.secondaryIndexCount, scratchSpacePath)
		if err := c.do(); err != nil {
			return false, err
		}
	}

	if err := file.Sync(); err != nil {
		return false, fmt.Errorf("fsync cleaned segment file: %w", err)
	}
	if err := file.Close(); err != nil {
		return false, fmt.Errorf("close cleaned segment file: %w", err)
	}

	newSize, err := c.replaceCleanedSegment(segmentIdx, tmpSegmentPath)
	if err != nil {
		return false, fmt.Errorf("replace compacted segments: %w", err)
	}
	if err := onCompleted(newSize); err != nil {
		return false, fmt.Errorf("callback cleaned segment file: %w", err)
	}

	return true, nil
}

func (c *segmentCleanerImpl) replaceCleanedSegment(segmentIdx int, tmpSegmentPath string,
) (int64, error) {
	oldSegment := c.sg.segmentAtPos(segmentIdx)
	countNetAdditions := oldSegment.countNetAdditions

	precomputedFiles, err := preComputeSegmentMeta(tmpSegmentPath, countNetAdditions,
		c.sg.logger, c.sg.useBloomFilter, c.sg.calcCountNetAdditions)
	if err != nil {
		return 0, fmt.Errorf("precompute segment meta: %w", err)
	}

	c.sg.maintenanceLock.Lock()
	defer c.sg.maintenanceLock.Unlock()

	if err := oldSegment.close(); err != nil {
		return 0, fmt.Errorf("close disk segment %q: %w", oldSegment.path, err)
	}
	if err := oldSegment.drop(); err != nil {
		return 0, fmt.Errorf("drop disk segment %q: %w", oldSegment.path, err)
	}
	if err := fsync(c.sg.dir); err != nil {
		return 0, fmt.Errorf("fsync segment directory %q: %w", c.sg.dir, err)
	}

	segmentId := segmentID(oldSegment.path)
	var segmentPath string

	// the old segment have been deleted, we can now safely remove the .tmp
	// extension from the new segment itself and the pre-computed files
	for i, tmpPath := range precomputedFiles {
		path, err := c.sg.stripTmpExtension(tmpPath, segmentId, segmentId)
		if err != nil {
			return 0, fmt.Errorf("strip .tmp extension of new segment %q: %w", tmpPath, err)
		}
		if i == 0 {
			// the first element in the list is the segment itself
			segmentPath = path
		}
	}

	newSegment, err := newSegment(segmentPath, c.sg.logger, c.sg.metrics, nil,
		c.sg.mmapContents, c.sg.useBloomFilter, c.sg.calcCountNetAdditions, false)
	if err != nil {
		return 0, fmt.Errorf("create new segment %q: %w", newSegment.path, err)
	}

	c.sg.segments[segmentIdx] = newSegment
	return newSegment.size, nil
}

// ================================================================

const cleanupDbFileName = "cleanup.db.bolt"

var (
	cleanupSegmentsBucket       = []byte("segments")
	cleanupMetaBucket           = []byte("meta")
	cleanupMetaKeyNextAllowedTs = []byte("nextAllowedTs")
)

// func (sg *SegmentGroup) isCleanupSupported() bool {
// 	fmt.Printf("  ==> isCleanupSupported [%s] / cleanup interval [%s]\n", sg.dir, sg.cleanupInterval)

// 	if sg.cleanupInterval <= 0 {
// 		return false
// 	}

// 	switch sg.strategy {
// 	case StrategyReplace:
// 		return true
// 	case StrategyMapCollection,
// 		StrategySetCollection,
// 		StrategyRoaringSet:
// 		// TODO AL: add roaring set range
// 		// TODO AL: add support for other strategies in the future?
// 		return false
// 	default:
// 		err := fmt.Errorf("unrecognized strategy %q", sg.strategy)
// 		sg.logger.
// 			WithField("action", "check_segments_cleanup_supported").
// 			WithField("dir", sg.dir).
// 			WithError(err).
// 			Errorf("unrecognized strategy")
// 		return false
// 	}
// }

// func (sg *SegmentGroup) initCleanupDBIfSupported() error {
// 	if sg.isCleanupSupported() {
// 		path := filepath.Join(sg.dir, cleanupDbFileName)

// 		db, err := bolt.Open(path, 0o600, nil)
// 		if err != nil {
// 			return fmt.Errorf("open cleanup bolt db %q: %w", path, err)
// 		}

// 		err = db.Update(func(tx *bolt.Tx) error {
// 			_, err := tx.CreateBucketIfNotExists(cleanupSegmentsBucket)
// 			return err
// 		})
// 		if err != nil {
// 			return fmt.Errorf("segments bucket cleanup bolt db %q: %w", path, err)
// 		}

// 		err = db.Update(func(tx *bolt.Tx) error {
// 			_, err := tx.CreateBucketIfNotExists(cleanupMetaBucket)
// 			return err
// 		})
// 		if err != nil {
// 			return fmt.Errorf("meta bucket cleanup bolt db %q: %w", path, err)
// 		}

// 		sg.cleanupDB = db
// 	}
// 	return nil
// }

// func (sg *SegmentGroup) closeCleanupDBIfSupported() error {
// 	if sg.isCleanupSupported() {
// 		if err := sg.cleanupDB.Close(); err != nil {
// 			path := filepath.Join(sg.dir, cleanupDbFileName)
// 			return fmt.Errorf("close cleanup bolt db %q: %w", path, err)
// 		}
// 	}
// 	return nil
// }

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

// func (sg *SegmentGroup) cleanupOnce(shouldAbort cyclemanager.ShouldAbortCallback) (bool, error) {
// 	// TODO AL: take shouldAbort into account

// 	segmentIdx, onCompleted, err := sg.findCleanupCandidate()
// 	if err != nil {
// 		return false, err
// 	}
// 	if segmentIdx == -1 {
// 		return false, nil
// 	}

// 	if sg.allocChecker != nil {
// 		// allocChecker is optional
// 		if err := sg.allocChecker.CheckAlloc(100 * 1024 * 1024); err != nil {
// 			// if we don't have at least 100MB to spare, don't start a cleanup. A
// 			// cleanup does not actually need a 100MB, but it will create garbage
// 			// that needs to be cleaned up. If we're so close to the memory limit, we
// 			// can increase stability by preventing anything that's not strictly
// 			// necessary. Cleanup can simply resume when the cluster has been
// 			// scaled.
// 			sg.logger.WithFields(logrus.Fields{
// 				"action": "lsm_compaction",
// 				"event":  "compaction_skipped_oom",
// 				"path":   sg.dir,
// 			}).WithError(err).
// 				Warnf("skipping compaction due to memory pressure")

// 			return false, nil
// 		}
// 	}

// 	segment := sg.segmentAtPos(segmentIdx)

// 	tmpSegmentPath := filepath.Join(sg.dir, "segment-"+segmentID(segment.path)+".db.tmp")
// 	scratchSpacePath := segment.path + "cleanup.scratch.d"

// 	file, err := os.Create(tmpSegmentPath)
// 	if err != nil {
// 		return false, err
// 	}

// 	switch sg.strategy {
// 	case StrategyReplace:
// 		c := newSegmentCleanerReplace(file, segment.newCursor(),
// 			sg.makeKeyExistsOnUpperSegments(segmentIdx), segment.level,
// 			segment.secondaryIndexCount, scratchSpacePath)
// 		if err := c.do(); err != nil {
// 			// fmt.Printf("  ==> cleanup error %s\n\n", err)

// 			return false, err
// 		}
// 	}

// 	if err := file.Sync(); err != nil {
// 		return false, fmt.Errorf("fsync cleaned segment file: %w", err)
// 	}
// 	if err := file.Close(); err != nil {
// 		return false, fmt.Errorf("close cleaned segment file: %w", err)
// 	}

// 	if err := sg.replaceCleanedSegment(segmentIdx, tmpSegmentPath); err != nil {
// 		return false, fmt.Errorf("replace compacted segments: %w", err)
// 	}
// 	if err := onCompleted(sg.segmentAtPos(segmentIdx).size); err != nil {
// 		return false, fmt.Errorf("callback cleaned segment file: %w", err)
// 	}

// 	return true, nil
// }

type onCompletedFunc func(size int64) error

// func (sg *SegmentGroup) findCleanupCandidate() (int, onCleanupCompletedFunc, error) {
// 	t := func(ts int64) time.Time {
// 		return time.Unix(0, 0).Add(time.Duration(ts))
// 	}

// 	noCandidate := -1

// 	if sg.isReadyOnly() {
// 		fmt.Printf("  ==> no candidate / read only\n")
// 		return noCandidate, nil, nil
// 	}

// 	var count int
// 	var ids []uint64
// 	var sizes []int64

// 	err := func() error {
// 		sg.maintenanceLock.RLock()
// 		defer sg.maintenanceLock.RUnlock()

// 		if count = len(sg.segments); count > 1 {
// 			ids = make([]uint64, count)
// 			sizes = make([]int64, count)

// 			for i, seg := range sg.segments {
// 				id, err := strconv.ParseUint(segmentID(seg.path), 10, 64)
// 				if err != nil {
// 					return fmt.Errorf("parse segment id %q: %w", segmentID(seg.path), err)
// 				}
// 				ids[i] = id
// 				sizes[i] = seg.size
// 			}
// 		}
// 		return nil
// 	}()
// 	if err != nil {
// 		fmt.Printf("  ==> no candidate / segments read, err [%s]\n", err)
// 		return noCandidate, nil, err
// 	}
// 	if count <= 1 {
// 		fmt.Printf("  ==> no candidate / len [%d]\n", count)
// 		return noCandidate, nil, nil
// 	}

// 	now := time.Now()
// 	tsOldest := now.UnixNano()
// 	tsThreshold := now.Add(-sg.cleanupInterval).UnixNano()
// 	tsOldestStored := int64(0)

// 	sg.cleanupDB.View(func(tx *bolt.Tx) error {
// 		b := tx.Bucket(cleanupMetaBucket)

// 		v := b.Get(cleanupMetaKeyTsOldest)
// 		if v != nil {
// 			tsOldestStored = int64(binary.BigEndian.Uint64(v))
// 		}
// 		return nil
// 	})

// 	if tsOldestStored > tsThreshold {
// 		fmt.Printf("  ==> no candidate / tsOldestStored [%s] > tsThreshold [%s]\n", t(tsOldestStored), t(tsThreshold))
// 		return noCandidate, nil, nil
// 	}
// 	fmt.Printf("  ==> CONTINUING tsOldestStored [%s] <= tsThreshold [%s]\n", t(tsOldestStored), t(tsThreshold))

// 	kToDelete := [][]byte{}
// 	candidateIdx := noCandidate

// 	err = sg.cleanupDB.View(func(tx *bolt.Tx) error {
// 		b := tx.Bucket(cleanupSegmentsBucket)
// 		c := b.Cursor()

// 		idx := 0
// 		ck, cv := c.First()

// 		// no point cleaning last segment, therefore "l-1"
// 		for ck != nil && idx < count-1 {
// 			cid := binary.BigEndian.Uint64(ck)
// 			id := ids[idx]

// 			fmt.Printf("  ==> loop cid [%d] id [%d]\n", cid, id)

// 			if id > cid {
// 				fmt.Printf("    ==> id > cid ; deleting cid\n")
// 				// id no longer exists, to be removed from bolt
// 				kToDelete = append(kToDelete, ck)
// 				ck, cv = c.Next()
// 			} else if id < cid {
// 				fmt.Printf("    ==> id < cid ; tsOldest [%s]\n", t(tsOldest))
// 				// id not yet present in bolt
// 				if tsOldest > 0 {
// 					fmt.Printf("    ==> tsOldest > 0\n")
// 					tsOldest = 0
// 					candidateIdx = idx
// 				}
// 				idx++
// 			} else {
// 				// id present in bolt
// 				csize := int64(binary.BigEndian.Uint64(cv[8:16]))
// 				size := sizes[idx]
// 				fmt.Printf("    ==> id = cid ; size [%d] csize [%d]\n", size, csize)
// 				if size != csize {
// 					cts := int64(binary.BigEndian.Uint64(cv[0:8]))
// 					fmt.Printf("    ==> size != csize ; tsOldest [%s] cts [%s]\n", t(tsOldest), t(cts))
// 					if tsOldest > cts {
// 						fmt.Printf("    ==> tsOldest > cts\n")
// 						tsOldest = cts
// 						candidateIdx = idx
// 					}
// 				}
// 				ck, cv = c.Next()
// 				idx++
// 			}
// 		}
// 		// in case 1st loop finished due to idx reached len
// 		for ; ck != nil; ck, _ = c.Next() {
// 			cid := binary.BigEndian.Uint64(ck)
// 			fmt.Printf("  ==> cursor loop ; cid [%d]\n", cid)
// 			if cid != ids[count-1] {
// 				kToDelete = append(kToDelete, ck)
// 			}
// 		}
// 		// in case 1st loop finished due to cursor finished
// 		for ; idx < count-1 && tsOldest > 0; idx++ {
// 			fmt.Printf("  ==> idx loop ; tsOldest [%s]\n", t(tsOldest))
// 			tsOldest = 0
// 			candidateIdx = idx
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		fmt.Printf("  ==> no candidate / searching, err [%s]\n", err)
// 		return noCandidate, nil, fmt.Errorf("searching cleanup bolt %q: %w", sg.cleanupDB.Path(), err)
// 	}

// 	if len(kToDelete) > 0 {
// 		err = sg.cleanupDB.Update(func(tx *bolt.Tx) error {
// 			b := tx.Bucket(cleanupSegmentsBucket)

// 			for _, k := range kToDelete {
// 				if err := b.Delete(k); err != nil {
// 					return err
// 				}
// 			}
// 			return nil
// 		})
// 		if err != nil {
// 			fmt.Printf("  ==> no candidate / deleting, err [%s]\n", err)
// 			return noCandidate, nil, fmt.Errorf("deleting from cleanup bolt %q: %w", sg.cleanupDB.Path(), err)
// 		}
// 	}

// 	if candidateIdx != noCandidate && tsOldest <= tsThreshold {
// 		id := ids[candidateIdx]
// 		onCompleted := func(newSize int64) error {
// 			err := sg.cleanupDB.Update(func(tx *bolt.Tx) error {
// 				b := tx.Bucket(cleanupSegmentsBucket)
// 				bufK := make([]byte, 8)
// 				bufV := make([]byte, 16)

// 				fmt.Printf("  ==> storing candidate idx [%d] id [%d] ts [%s]\n",
// 					candidateIdx, id, t(tsOldest))

// 				binary.BigEndian.PutUint64(bufK, id)
// 				binary.BigEndian.PutUint64(bufV[0:8], uint64(now.UnixNano()))
// 				binary.BigEndian.PutUint64(bufV[8:16], uint64(newSize))
// 				return b.Put(bufK, bufV)
// 			})
// 			if err != nil {
// 				return fmt.Errorf("updating cleanup bolt %q: %w", sg.cleanupDB.Path(), err)
// 			}
// 			return nil
// 		}
// 		fmt.Printf("  ==> candidate! [%d][%d]\n", candidateIdx, id)
// 		return candidateIdx, onCompleted, nil
// 	}

// 	err = sg.cleanupDB.Update(func(tx *bolt.Tx) error {
// 		b := tx.Bucket(cleanupMetaBucket)
// 		buf := make([]byte, 8)
// 		binary.BigEndian.PutUint64(buf, uint64(tsOldest))
// 		return b.Put(cleanupMetaKeyTsOldest, buf)
// 	})
// 	if err != nil {
// 		fmt.Printf("  ==> no candidate / updating tsOldest, err [%s]\n", err)
// 		return noCandidate, nil, fmt.Errorf("updating cleanup bolt %q: %w", sg.cleanupDB.Path(), err)
// 	}

// 	fmt.Printf("  ==> no candidate / updated tsOldest [%s] ; tsThreshold [%s]\n", t(tsOldest), t(tsThreshold))
// 	return noCandidate, nil, nil
// }

// func (sg *SegmentGroup) replaceCleanedSegment(segmentIdx int, tmpSegmentPath string,
// ) error {
// 	oldSegment := sg.segmentAtPos(segmentIdx)
// 	countNetAdditions := oldSegment.countNetAdditions

// 	precomputedFiles, err := preComputeSegmentMeta(tmpSegmentPath, countNetAdditions,
// 		sg.logger, sg.useBloomFilter, sg.calcCountNetAdditions)
// 	if err != nil {
// 		return fmt.Errorf("precompute segment meta: %w", err)
// 	}

// 	sg.maintenanceLock.Lock()
// 	defer sg.maintenanceLock.Unlock()

// 	if err := oldSegment.close(); err != nil {
// 		return fmt.Errorf("close disk segment %q: %w", oldSegment.path, err)
// 	}
// 	if err := oldSegment.drop(); err != nil {
// 		return fmt.Errorf("drop disk segment %q: %w", oldSegment.path, err)
// 	}
// 	if err := fsync(sg.dir); err != nil {
// 		return fmt.Errorf("fsync segment directory %q: %w", sg.dir, err)
// 	}

// 	segmentId := segmentID(oldSegment.path)
// 	var segmentPath string

// 	// the old segment have been deleted, we can now safely remove the .tmp
// 	// extension from the new segment itself and the pre-computed files
// 	for i, tmpPath := range precomputedFiles {
// 		path, err := sg.stripTmpExtension(tmpPath, segmentId, segmentId)
// 		if err != nil {
// 			return fmt.Errorf("strip .tmp extension of new segment %q: %w", tmpPath, err)
// 		}
// 		if i == 0 {
// 			// the first element in the list is the segment itself
// 			segmentPath = path
// 		}
// 	}

// 	newSegment, err := newSegment(segmentPath, sg.logger, sg.metrics, nil,
// 		sg.mmapContents, sg.useBloomFilter, sg.calcCountNetAdditions, false)
// 	if err != nil {
// 		return fmt.Errorf("create new segment %q: %w", newSegment.path, err)
// 	}

// 	sg.segments[segmentIdx] = newSegment
// 	return nil
// }
