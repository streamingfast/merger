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

	irreversibleBlocks []*OneBlockFile
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
func (b *Bundler) PreMergedBlocks() []*OneBlockFile {
	b.Lock()
	defer b.Unlock()
	return b.irreversibleBlocks
}

func (b *Bundler) HandleBlockFile(obf *OneBlockFile) error {
	return b.forkable.ProcessBlock(obf.ToBstreamBlock(), obf) // forkable will call our own b.ProcessBlock() on irreversible blocks only
}

func (b *Bundler) Reset(nextBase uint64, lib bstream.BlockRef) {
	options := []forkable.Option{
		forkable.WithFilters(bstream.StepIrreversible),
	}
	if lib != nil {
		options = append(options, forkable.WithExclusiveLIB(lib))
	}
	b.forkable = forkable.New(b, options...)

	b.Lock()
	b.baseBlockNum = nextBase
	b.Unlock()
}

func (b *Bundler) ProcessBlock(_ *bstream.Block, obj interface{}) error {
	obf := obj.(*OneBlockFile)
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
	b.irreversibleBlocks = nil
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
