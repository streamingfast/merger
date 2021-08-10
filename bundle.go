// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merger

import (
	"sort"
	"time"

	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Bundle struct {
	fileList           map[string]*OneBlockFile // key: "0000000100-20170701T122141.0-24a07267-e5914b39" ->
	timeSortedFileList []*OneBlockFile

	lowerBlock uint64 // base NewTestBlock number for bundle, like 38918100 (always % chunkSize)
	chunkSize  uint64

	upperBlockID   string // this would correspond to the block_id of the LAST NewTestBlock of the bundle, 38918199
	upperBlockTime time.Time
}

func (b *Bundle) upperBlock() uint64 {
	return b.lowerBlock + b.chunkSize
}

func NewBundle(lowerBlockNum, chunkSize uint64) *Bundle {
	zlog.Info("creating new bundle", zap.Uint64("lower_block_num", lowerBlockNum), zap.Uint64("chunk_size", chunkSize))
	b := &Bundle{
		chunkSize:  chunkSize,
		lowerBlock: lowerBlockNum,
		fileList:   make(map[string]*OneBlockFile, chunkSize),
	}
	return b
}

func (b *Bundle) timeSortedFiles() (files []*OneBlockFile) {
	files = make([]*OneBlockFile, len(b.fileList))

	i := 0
	for _, b := range b.fileList {
		files[i] = b
		i++
	}

	sort.SliceStable(files, func(i, j int) bool {
		if files[i].blockTime.Equal(files[j].blockTime) {
			return files[i].num < files[j].num
		}

		return files[i].blockTime.Before(files[j].blockTime)
	})
	return
}

func (b *Bundle) isComplete() (complete bool) {
	prevID := b.upperBlockID
	var lowestContiguous *OneBlockFile

	files := b.timeSortedFiles()
	for i := len(files) - 1; i >= 0; i-- {
		if files[i].id == prevID {
			prevID = files[i].previousID
			lowestContiguous = files[i]
			zlog.Debug("setting lowest contiguous to",
				zap.Uint64("block_num", lowestContiguous.num),
				zap.String("block_id", lowestContiguous.id),
				zap.String("previous_id", lowestContiguous.previousID),
			)
		}
		continue
	}

	if lowestContiguous == nil {
		zlog.Debug("did not find upper block", zap.String("upper_block_id", b.upperBlockID))
		return false
	}

	// Accept blocks that are lower...
	if lowestContiguous.num <= b.lowerBlock {
		return true
	}
	if b.lowerBlock == 0 && lowestContiguous.num <= 2 {
		return true
	}

	zlog.Warn("found a hole in a oneblock files", zap.Uint64("bundle_lower_block", b.lowerBlock), zap.Uint64("missing_block_num", lowestContiguous.num-1), zap.String("missing_block_id", prevID))
	return false
}

func (b *Bundle) triage(filename string, sourceStore dstore.Store, seenCache *SeenBlockCache) (processed bool, err error) {
	if b.containsFilename(filename) {
		return true, nil
	}

	blockNum, blockTime, blockIDSuffix, previousIDSuffix, canonicalName, err := parseFilename(filename)
	if err != nil {
		return false, err
	}

	if blockNum < b.upperBlock() {
		zlog.Debug("adding and downloading file", zap.String("filename", filename), zap.Time("blocktime", blockTime), zap.Uint64("blockNum", blockNum))
		b.add(filename, blockNum, blockTime, blockIDSuffix, previousIDSuffix, canonicalName)
		return true, nil
	}

	if blockNum+b.chunkSize < b.lowerBlock {
		zlog.Warn("including an unseen block that is far before lower block number, should reprocess to make it cleaner", zap.Uint64("delta", b.lowerBlock-blockNum))
		// TODO add that block to the previous bundle automatically to help with future replays-from-blockfile
	}

	if blockNum == b.upperBlock() {
		if b.upperBlockTime.IsZero() || blockTime.Before(b.upperBlockTime) {
			zlog.Debug("upper block time stretched", zap.Time("block_time", blockTime))
			b.upperBlockID = previousIDSuffix
			b.upperBlockTime = blockTime
		}
	}

	return false, nil
}

func (b *Bundle) containsFilename(filename string) bool {
	_, found := b.fileList[filename]
	return found
}

func (b *Bundle) add(filename string, blockNum uint64, blockTime time.Time, blockIDSuffix string, previousIDSuffix string, canonicalName string) {
	if obf, ok := b.fileList[canonicalName]; ok {
		obf.filenames[filename] = Empty
		return
	}
	obf := &OneBlockFile{
		canonicalName: canonicalName,
		filenames: map[string]struct{}{
			filename: Empty,
		},
		blockTime:  blockTime,
		id:         blockIDSuffix,
		num:        blockNum,
		previousID: previousIDSuffix,
	}
	b.fileList[canonicalName] = obf
}

func (b *Bundle) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddUint64("lower_block_num", b.lowerBlock)
	encoder.AddUint64("upper_block_num", b.upperBlock())
	encoder.AddInt("file_count", len(b.fileList))
	return nil
}
