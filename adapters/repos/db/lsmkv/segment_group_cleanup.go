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

const cleanupDbFileName = "cleanup.db.bolt"

var cleanupBucket = []byte("cleanup")

var cleanupInterval = time.Minute * 1 // TODO AL: env configured

func (sg *SegmentGroup) isCleanupSupported() bool {
	switch sg.strategy {
	case StrategyReplace:
		return true
	case StrategyMapCollection,
		StrategySetCollection,
		StrategyRoaringSet:
		// TODO AL: add roaring set range
		// TODO AL: add support for other strategies in the future?
		return false
	default:
		err := fmt.Errorf("unrecognized strategy %q", sg.strategy)
		sg.logger.
			WithField("action", "check_segments_cleanup_supported").
			WithField("dir", sg.dir).
			WithError(err).
			Errorf("unrecognized strategy")
		return false
	}
}

func (sg *SegmentGroup) initCleanupDBIfSupported() error {
	if sg.isCleanupSupported() {
		path := filepath.Join(sg.dir, cleanupDbFileName)

		db, err := bolt.Open(path, 0o600, nil)
		if err != nil {
			return fmt.Errorf("open cleanup bolt db %q: %w", path, err)
		}

		err = db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(cleanupBucket)
			return err
		})
		if err != nil {
			return fmt.Errorf("bucket cleanup bolt db %q: %w", path, err)
		}

		sg.cleanupDB = db
	}
	return nil
}

func (sg *SegmentGroup) closeCleanupDBIfSupported() error {
	if sg.isCleanupSupported() {
		if err := sg.cleanupDB.Close(); err != nil {
			path := filepath.Join(sg.dir, cleanupDbFileName)
			return fmt.Errorf("close cleanup bolt db %q: %w", path, err)
		}
	}
	return nil
}

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
	// TODO AL: take shouldAbort into account

	segmentIdx, onSuccess, err := sg.findCleanupCandidate()
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
	if err := onSuccess(sg.segmentAtPos(segmentIdx).size); err != nil {
		return false, fmt.Errorf("callback cleaned segment file: %w", err)
	}

	return true, nil
}

func (sg *SegmentGroup) findCleanupCandidate() (int, func(size int64) error, error) {
	noCandidate := -1
	var noopUpdate func(size int64) error = nil

	if sg.isReadyOnly() {
		fmt.Printf("  ==> no candidate / read only\n")
		return noCandidate, noopUpdate, nil
	}

	var count int
	var ids []uint64
	var sizes []int64

	err := func() error {
		sg.maintenanceLock.RLock()
		defer sg.maintenanceLock.RUnlock()

		if count = len(sg.segments); count > 1 {
			ids = make([]uint64, count)
			sizes = make([]int64, count)

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
		return noCandidate, noopUpdate, err
	}
	if count <= 1 {
		fmt.Printf("  ==> no candidate / len [%d]\n", count)
		return noCandidate, noopUpdate, nil
	}

	now := time.Now()
	tsOldest := now.UnixNano()
	tsThreshold := now.Add(-cleanupInterval).UnixNano()

	kToDelete := [][]byte{}
	candidateIdx := noCandidate

	err = sg.cleanupDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(cleanupBucket)
		c := b.Cursor()

		idx := 0
		ck, cv := c.First()

		// no point cleaning last segment, therefore "l-1"
		for ck != nil && idx < count-1 {
			cid := binary.BigEndian.Uint64(ck)
			id := ids[idx]

			fmt.Printf("  ==> loop cid [%d] id [%d]\n", cid, id)

			if id > cid {
				fmt.Printf("    ==> id > cid ; deleting cid\n")
				// id no longer exists, to be removed from bolt
				kToDelete = append(kToDelete, ck)
				ck, cv = c.Next()
			} else if id < cid {
				fmt.Printf("    ==> id < cid ; tsOldest [%d]\n", tsOldest)
				// id not yet present in bolt
				if tsOldest > 0 {
					fmt.Printf("    ==> tsOldest > 0\n")
					tsOldest = 0
					candidateIdx = idx
				}
				idx++
			} else {
				// id present in bolt
				cts := int64(binary.BigEndian.Uint64(cv[0:8]))
				fmt.Printf("    ==> id = cid ; tsOldest [%d] cts [%d]\n", tsOldest, cts)
				if tsOldest > cts {
					csize := int64(binary.BigEndian.Uint64(cv[8:16]))
					size := sizes[idx]
					fmt.Printf("    ==> tsOldest > cts ; size [%d] csize [%d]\n", size, csize)
					if size != csize {
						fmt.Printf("    ==> size != csize\n")
						tsOldest = cts
						candidateIdx = idx
					}
				}
				ck, cv = c.Next()
				idx++
			}
		}
		// in case 1st loop finished due to idx reached len
		for ; ck != nil; ck, _ = c.Next() {
			cid := binary.BigEndian.Uint64(ck)
			fmt.Printf("  ==> cursor loop ; cid [%d]\n", cid)
			if cid != ids[count-1] {
				kToDelete = append(kToDelete, ck)
			}
		}
		// in case 1st loop finished due to cursor finished
		for ; idx < count-1 && tsOldest > 0; idx++ {
			fmt.Printf("  ==> idx loop ; tsOldest [%d]\n", tsOldest)
			tsOldest = 0
			candidateIdx = idx
		}
		return nil
	})
	if err != nil {
		fmt.Printf("  ==> no candidate / searching, err [%s]\n", err)
		return noCandidate, noopUpdate, fmt.Errorf("searching cleanup bolt %q: %w", sg.cleanupDB.Path(), err)
	}

	if len(kToDelete) > 0 {
		err = sg.cleanupDB.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(cleanupBucket)

			for _, k := range kToDelete {
				if err := b.Delete(k); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			fmt.Printf("  ==> no candidate / deleting, err [%s]\n", err)
			return noCandidate, noopUpdate, fmt.Errorf("deleting from cleanup bolt %q: %w", sg.cleanupDB.Path(), err)
		}
	}

	if candidateIdx != noCandidate && tsOldest < tsThreshold {
		id := ids[candidateIdx]
		update := func(newSize int64) error {
			err := sg.cleanupDB.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(cleanupBucket)
				bufK := make([]byte, 8)
				bufV := make([]byte, 16)

				fmt.Printf("  ==> storing candidate idx [%d] id [%d] ts [%d]\n",
					candidateIdx, id, tsOldest)

				binary.BigEndian.PutUint64(bufK, id)
				binary.BigEndian.PutUint64(bufV[0:8], uint64(now.UnixNano()))
				binary.BigEndian.PutUint64(bufV[8:16], uint64(newSize))
				return b.Put(bufK, bufV)
			})
			if err != nil {
				return fmt.Errorf("updating cleanup bolt %q: %w", sg.cleanupDB.Path(), err)
			}
			return nil
		}
		fmt.Printf("  ==> candidate! [%d][%d]\n", candidateIdx, id)
		return candidateIdx, update, nil
	}

	fmt.Printf("  ==> no candidate / end\n")
	return noCandidate, noopUpdate, nil
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
