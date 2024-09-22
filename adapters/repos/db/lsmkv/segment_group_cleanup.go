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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
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

func (sg *SegmentGroup) cleanupOnce() (bool, error) {
	candidatePos := sg.findCleanupCandidate()
	if candidatePos == -1 {
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

	segment := sg.segmentAtPos(candidatePos)

	tmpPath := filepath.Join(sg.dir, "segment-"+segmentID(segment.path)+".db.tmp")
	scratchSpacePath := segment.path + "cleanup.scratch.d"

	file, err := os.Create(tmpPath)
	if err != nil {
		return false, err
	}

	switch segment.strategy {
	case segmentindex.StrategyReplace:
	// TODO

	case segmentindex.StrategySetCollection,
		segmentindex.StrategyMapCollection,
		segmentindex.StrategyRoaringSet:
		// TODO. not supported yet
	default:
		return false, fmt.Errorf("unrecognized strategy %v", segment.strategy)
	}

	if err := file.Sync(); err != nil {
		return false, fmt.Errorf("fsync cleaned segment file: %w", err)
	}

	if err := file.Close(); err != nil {
		return false, fmt.Errorf("close cleaned segment file: %w", err)
	}

	if err := sg.replaceCleanedSegment(candidatePos, tmpPath); err != nil {
		return false, fmt.Errorf("replace compacted segments: %w", err)
	}

	return true, nil
}

func (sg *SegmentGroup) findCleanupCandidate() int {
	if sg.isReadyOnly() {
		return -1
	}

	// TODO implement
	return 0
}

func (sg *SegmentGroup) replaceCleanedSegment(pos int,
	tmpPath string,
) error {
	sg.maintenanceLock.RLock()
	updatedCountNetAdditions := sg.segments[old1].countNetAdditions +
		sg.segments[old2].countNetAdditions
	sg.maintenanceLock.RUnlock()

	// WIP: we could add a random suffix to the tmp file to avoid conflicts
	precomputedFiles, err := preComputeSegmentMeta(tmpPath,
		updatedCountNetAdditions, sg.logger,
		sg.useBloomFilter, sg.calcCountNetAdditions)
	if err != nil {
		return fmt.Errorf("precompute segment meta: %w", err)
	}

	sg.maintenanceLock.Lock()
	defer sg.maintenanceLock.Unlock()

	leftSegment := sg.segments[old1]
	rightSegment := sg.segments[old2]

	if err := leftSegment.close(); err != nil {
		return errors.Wrap(err, "close disk segment")
	}

	if err := rightSegment.close(); err != nil {
		return errors.Wrap(err, "close disk segment")
	}

	if err := leftSegment.drop(); err != nil {
		return errors.Wrap(err, "drop disk segment")
	}

	if err := rightSegment.drop(); err != nil {
		return errors.Wrap(err, "drop disk segment")
	}

	err = fsync(sg.dir)
	if err != nil {
		return fmt.Errorf("fsync segment directory %s: %w", sg.dir, err)
	}

	sg.segments[old1] = nil
	sg.segments[old2] = nil

	var newPath string
	// the old segments have been deleted, we can now safely remove the .tmp
	// extension from the new segment itself and the pre-computed files which
	// carried the name of the second old segment
	for i, path := range precomputedFiles {
		updated, err := sg.stripTmpExtension(path, segmentID(leftSegment.path), segmentID(rightSegment.path))
		if err != nil {
			return errors.Wrap(err, "strip .tmp extension of new segment")
		}

		if i == 0 {
			// the first element in the list is the segment itself
			newPath = updated
		}
	}

	seg, err := newSegment(newPath, sg.logger, sg.metrics, nil,
		sg.mmapContents, sg.useBloomFilter, sg.calcCountNetAdditions, false)
	if err != nil {
		return errors.Wrap(err, "create new segment")
	}

	sg.segments[old2] = seg

	sg.segments = append(sg.segments[:old1], sg.segments[old1+1:]...)

	return nil
}
