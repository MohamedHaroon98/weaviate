//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"time"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

type Memtable interface {
	get(key []byte) ([]byte, error)
	getBySecondary(pos int, key []byte) ([]byte, error)
	put(key, value []byte, opts ...SecondaryKeyOption) error
	setTombstone(key []byte, opts ...SecondaryKeyOption) error
	getCollection(key []byte) ([]value, error)
	getMap(key []byte) ([]MapPair, error)
	append(key []byte, values []value) error
	appendMapSorted(key []byte, pair MapPair) error
	// sum of all memtable sizes (same as SizeHighest if there is only one commitlog file)
	Size() uint64
	// size of the largest memtable
	SizeHighest() uint64
	ActiveDuration() time.Duration
	IdleDuration() time.Duration
	countStats() *countStats
	writeWAL() error
	// sum of all commitlog filesizes (same as commitlogSizeHighest if there is only one commitlog file)
	CommitlogSize() int64
	// size of the largest commitlog file
	CommitlogSizeHighest() int64
	CommitlogPath() string
	Path() string
	SecondaryIndices() uint16
	Strategy() string
	UpdatePath(bucketDir, newBucketDir string)
	CommitlogPause()
	CommitlogUnpause()
	CommitlogClose() error
	CommitlogDelete() error
	CommitlogFileSize() (int64, error)

	roaringSetAddOne(key []byte, value uint64) error
	roaringSetAddList(key []byte, values []uint64) error
	roaringSetAddListBatch(batch []KeyValue) []error
	roaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error
	roaringSetRemoveOne(key []byte, value uint64) error
	roaringSetRemoveList(key []byte, values []uint64) error
	roaringSetRemoveBitmap(key []byte, bm *sroar.Bitmap) error
	roaringSetAddRemoveBitmaps(key []byte, additions *sroar.Bitmap, deletions *sroar.Bitmap) error
	roaringSetGet(key []byte) (roaringset.BitmapLayer, error)

	closeRequestChannels()
	flush() error

	newCollectionCursor() innerCursorCollection
	newRoaringSetCursor() roaringset.InnerCursor
	newMapCursor() innerCursorMap
	newCursor() innerCursorReplace
	flattenNodesRoaringSet() []*roaringset.BinarySearchNode
	flattenInOrderKey() []*binarySearchNode
	flattenInOrderKeyMulti() []*binarySearchNodeMulti
	flattenInOrderKeyMap() []*binarySearchNodeMap
}
