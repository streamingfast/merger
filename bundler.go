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
	"fmt"
	"sync"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
)

type Bundler struct {
	sync.Mutex

	io IOInterface

	baseBlockNum uint64
	bundleSize   uint64

	irreversibleBlocks []*bstream.OneBlockFile
	forkable           *forkable.Forkable
}

func NewBundler(startBlock, bundleSize uint64, io IOInterface) *Bundler {
	b := &Bundler{
		bundleSize: bundleSize,
		io:         io,
	}
	b.Reset(toBaseNum(startBlock, bundleSize), nil)
	return b
}

// BaseBlockNum can be called from a different thread
func (b *Bundler) BaseBlockNum() uint64 {
	b.Lock()
	defer b.Unlock()
	return b.baseBlockNum
}

// PreMergedBlocks can be called from a different thread
func (b *Bundler) PreMergedBlocks() []*bstream.OneBlockFile {
	b.Lock()
	defer b.Unlock()
	return b.irreversibleBlocks
}

func (b *Bundler) HandleBlockFile(obf *bstream.OneBlockFile) error {
	return b.forkable.ProcessBlock(obf.ToBstreamBlock(), obf) // forkable will call our own b.ProcessBlock() on irreversible blocks only
}

func (b *Bundler) Reset(nextBase uint64, lib bstream.BlockRef) {
	options := []forkable.Option{
		forkable.WithFilters(bstream.StepIrreversible),
	}
	if lib != nil {
		options = append(options, forkable.WithInclusiveLIB(lib))
	}
	b.forkable = forkable.New(b, options...)

	b.Lock()
	b.baseBlockNum = nextBase
	b.Unlock()
}

func (b *Bundler) ProcessBlock(_ *bstream.Block, obj interface{}) error {
	obf := obj.(bstream.ObjectWrapper).WrappedObject().(*bstream.OneBlockFile)
	if obf.Num < b.baseBlockNum {
		// we may be receiving an inclusive LIB just before our bundle, ignore it
		return nil
	}

	if obf.Num < b.baseBlockNum+b.bundleSize {
		b.Lock()
		b.irreversibleBlocks = append(b.irreversibleBlocks, obf)
		b.Unlock()
		return nil
	}

	if err := b.io.MergeAndStore(context.Background(), b.baseBlockNum, b.irreversibleBlocks); err != nil {
		return err
	}

	b.io.DeleteAsync(b.irreversibleBlocks)
	b.Lock()
	b.irreversibleBlocks = []*bstream.OneBlockFile{obf}
	b.baseBlockNum += b.bundleSize
	b.Unlock()

	return nil
}

// String can be called from a different thread
func (b *Bundler) String() string {
	b.Lock()
	defer b.Unlock()

	var firstBlock, lastBlock string
	length := len(b.irreversibleBlocks)
	if length != 0 {
		firstBlock = b.irreversibleBlocks[0].String()
		lastBlock = b.irreversibleBlocks[length-1].String()
	}

	return fmt.Sprintf(
		"bundle_size: %d, base_block_num: %d, first_block: %s, last_block: %s, length: %d",
		b.bundleSize,
		b.baseBlockNum,
		firstBlock,
		lastBlock,
		length,
	)
}
