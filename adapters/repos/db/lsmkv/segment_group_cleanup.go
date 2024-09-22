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
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/cyclemanager"
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
	// TODO AL: take shouldAbort into account

	segmentIdx := sg.findCleanupCandidate()
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

	switch segment.strategy {
	case segmentindex.StrategyReplace:
		c := newSegmentCleanerReplace(file, segment.newCursor(),
			sg.makeKeyExistsOnUpperSegments(segmentIdx), segment.level,
			segment.secondaryIndexCount, scratchSpacePath)
		if err := c.do(); err != nil {
			return false, err
		}

	case segmentindex.StrategySetCollection,
		segmentindex.StrategyMapCollection,
		segmentindex.StrategyRoaringSet:
		// TODO AL: add support in the future
	default:
		return false, fmt.Errorf("unrecognized strategy %v", segment.strategy)
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

func (sg *SegmentGroup) findCleanupCandidate() int {
	if sg.isReadyOnly() {
		return -1
	}

	// TODO AL: implement
	return 0
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
