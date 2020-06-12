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
	"context"
	"io/ioutil"
	"sort"
	"time"

	"github.com/dfuse-io/dstore"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Bundle struct {
	fileList map[string]*OneBlockFile // key: "0000000100-20170701T122141.0-24a07267-e5914b39" ->

	lowerBlock uint64 // base NewTestBlock number for bundle, like 38918100 (always % chunkSize)
	chunkSize  uint64

	upperBlockID   string // this would correspond to the block_id of the LAST NewTestBlock of the bundle, 38918199
	upperBlockTime time.Time

	downloadWaitGroup *errgroup.Group
}

func (b *Bundle) upperBlock() uint64 {
	return b.lowerBlock + b.chunkSize
}

func NewBundle(lowerBlockNum, chunkSize uint64) *Bundle {
	zlog.Info("Creating new bundle", zap.Uint64("lower_block_num", lowerBlockNum), zap.Uint64("chunk_size", chunkSize))
	b := &Bundle{
		chunkSize:  chunkSize,
		lowerBlock: lowerBlockNum,
		fileList:   make(map[string]*OneBlockFile),
	}
	group := &errgroup.Group{}
	b.downloadWaitGroup = group
	return b
}

func (b *Bundle) timeSortedFiles() (files []*OneBlockFile) {
	for _, b := range b.fileList {
		files = append(files, b)
	}
	sort.SliceStable(files, func(i, j int) bool {
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
			zlog.Debug("setting lowestContiguous to", zap.Uint64("block_num", lowestContiguous.num), zap.String("block_id", lowestContiguous.id), zap.String("previous_id", lowestContiguous.previousID))
		}
		continue
	}

	if lowestContiguous == nil {
		zlog.Debug("did not find upperBlockID", zap.String("upper_block_id", b.upperBlockID))
		return false //did not find upper previousID
	}
	if lowestContiguous.num <= b.lowerBlock { // accept blocks that are lower...
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

	blockNum, blockTime, blockIDSuffix, previousIDSuffix, err := parseFilename(filename)
	if err != nil {
		return false, err
	}

	if blockNum < b.upperBlock() {
		zlog.Debug("adding and downloading file", zap.String("filename", filename), zap.Time("blocktime", blockTime), zap.Uint64("blockNum", blockNum))
		b.addAndDownload(&OneBlockFile{
			name:       filename,
			blockTime:  blockTime,
			id:         blockIDSuffix,
			num:        blockNum,
			previousID: previousIDSuffix,
		}, sourceStore)
		return true, nil
	}

	if blockNum+b.chunkSize < b.lowerBlock {
		zlog.Warn("including an unseen NewTestBlock that is far before lower NewTestBlock number, should reprocess to make it cleaner", zap.Uint64("delta", b.lowerBlock-blockNum))
		// TODO add that NewTestBlock to the previous bundle automatically to help with future replays-from-blockfile
	}

	if blockNum == b.upperBlock() {
		if b.upperBlockTime.IsZero() || blockTime.Before(b.upperBlockTime) {
			zlog.Debug("upper NewTestBlock time stretched", zap.Time("block_time", blockTime))
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

func (b *Bundle) addAndDownload(oneBlock *OneBlockFile, sourceStore dstore.Store) {
	b.fileList[oneBlock.name] = oneBlock

	b.downloadWaitGroup.Go(func() error {
		// FIXME: we need to manage the error, make it bubble up, retry or something...
		err := Retry(5, 500*time.Millisecond, func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()

			return downloadFile(ctx, oneBlock, sourceStore)
		})

		if err != nil {
			return err
		}

		return nil
	})
}

func downloadFile(ctx context.Context, bf *OneBlockFile, s dstore.Store) error {
	out, err := s.OpenObject(ctx, bf.name)
	if err != nil {
		return err
	}
	defer out.Close()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// TODO: In the future, implement `ctx` cancellation *during* download here.. would require
	//       splitting `ReadAll` in chunks and check if the context is `Done` between each chunk.
	bf.blk, err = ioutil.ReadAll(out)
	return err
}
