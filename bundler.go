package merger

import (
	"fmt"
	"math"
	"sort"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"go.uber.org/zap"
)

type Bundler struct {
	db         *forkable.ForkDB
	bundleSize uint64

	lastMergeOneBlockFile      *OneBlockFile
	exclusiveHighestBlockLimit uint64
}

func NewBundler(bundleSize uint64, firstExclusiveHighestBlockLimit uint64) *Bundler {
	return &Bundler{
		bundleSize:                 bundleSize,
		db:                         forkable.NewForkDB(),
		exclusiveHighestBlockLimit: firstExclusiveHighestBlockLimit,
	}
}

func (b *Bundler) String() string {
	var lastMergeBlockNum uint64
	if b.lastMergeOneBlockFile != nil {
		lastMergeBlockNum = b.lastMergeOneBlockFile.num
	}

	return fmt.Sprintf("bundle_size: %d, last_merge_block_num: %d, inclusive_lower_block_num: %d, exclusive_highest_block_limit: %d", b.bundleSize, lastMergeBlockNum, b.BundleInclusiveLowerBlock(), b.exclusiveHighestBlockLimit)
}

func (b *Bundler) BundleInclusiveLowerBlock() uint64 {
	return b.exclusiveHighestBlockLimit - b.bundleSize
}

func (b *Bundler) Boostrap(fetchOneBlockFilesFromMergedFile func(lowBlockNum uint64) ([]*OneBlockFile, error)) error {
	initialLowBlockNum := b.BundleInclusiveLowerBlock()
	err := b.loadOneBlocksToLib(initialLowBlockNum, fetchOneBlockFilesFromMergedFile)
	if err != nil {
		return fmt.Errorf("loading one block files: %w", err)
	}
	return nil
}

func (b *Bundler) loadOneBlocksToLib(initialLowBlockNum uint64, fetchOneBlockFilesFromMergedFile func(lowBlockNum uint64) ([]*OneBlockFile, error)) error {
	libNum := uint64(math.MaxUint64)
	lowBlockNum := initialLowBlockNum

	for {
		zlog.Info("fetching one block files", zap.Uint64("at_low_block_num", lowBlockNum))

		oneBlockFiles, err := fetchOneBlockFilesFromMergedFile(lowBlockNum)
		if err != nil {
			return fmt.Errorf("failed to fetch merged file for low block num: %d: %w", lowBlockNum, err)
		}
		sort.Slice(oneBlockFiles, func(i, j int) bool { return oneBlockFiles[i].num > oneBlockFiles[j].num })
		for _, f := range oneBlockFiles {
			if libNum == math.MaxUint64 {
				zlog.Info("found lib to reach", zap.Uint64("lib_block_num", libNum))
				libNum = f.libNum
			}
			f.merged = true
			b.AddOneBlockFile(f)
			if f.num == libNum {
				return nil
			}
		}

		zlog.Info("processed one block files", zap.Uint64("at_low_block_num", lowBlockNum), zap.Uint64("lib_num_to_reach", libNum))

		lowBlockNum = lowBlockNum - b.bundleSize
	}
}

func (b *Bundler) AddOneBlockFile(oneBlockFile *OneBlockFile) (exist bool) {
	if block := b.db.BlockForID(oneBlockFile.id); block != nil {
		obf := block.Object.(*OneBlockFile)
		for filename := range oneBlockFile.filenames { //this is an ugly patch. ash stepd ;-)
			obf.filenames[filename] = Empty
		}
		return true
	}

	blockRef := bstream.NewBlockRef(oneBlockFile.id, oneBlockFile.num)
	exist = b.db.AddLink(blockRef, oneBlockFile.previousID, oneBlockFile)
	return
}

func (b *Bundler) AddPreMergedOneBlockFiles(oneBlockFiles []*OneBlockFile) {
	if len(oneBlockFiles) == 0 {
		return
	}
	for _, oneBlockFile := range oneBlockFiles {
		b.AddOneBlockFile(oneBlockFile)
	}
	b.lastMergeOneBlockFile = oneBlockFiles[len(oneBlockFiles)-1]
	b.exclusiveHighestBlockLimit += b.bundleSize
}

func (b *Bundler) LongestChain() []string {
	roots, err := b.db.Roots()
	if err != nil {
		return nil //this is happening when there is no links in db
	}

	count := 0
	var longestChain []string
	for _, root := range roots {
		tree := b.db.BuildTreeWithID(root)
		lc := tree.Chains().LongestChain()

		if len(longestChain) == len(lc) {
			count++
		}

		if len(longestChain) < len(lc) {
			count = 1
			longestChain = lc
		}
	}
	if count > 1 { // found multiple chain with same length
		return nil
	}

	return longestChain
}

func (b *Bundler) IsBlockTooOld(blockNum uint64) bool {
	roots, err := b.db.Roots()
	if err != nil { //if there is no root it can't be too old
		return false
	}
	//
	lowestRootBlockNum := uint64(math.MaxUint64)
	for _, root := range roots {
		block := b.db.BlockForID(root)
		if block.BlockNum < lowestRootBlockNum {
			lowestRootBlockNum = block.BlockNum
		}
	}
	return blockNum < lowestRootBlockNum
}

func (b *Bundler) LongestChainFirstBlockNum() (uint64, error) {
	longestChain := b.LongestChain()
	if len(longestChain) == 0 {
		return 0, fmt.Errorf("no longuest chain available")
	}
	block := b.db.BlockForID(longestChain[0])
	return block.BlockNum, nil
}

func (b *Bundler) IsComplete() (complete bool, highestBlockLimit uint64) {
	longest := b.LongestChain()
	for _, blockID := range longest {
		blk := b.db.BlockForID(blockID)

		if blk.BlockNum >= b.exclusiveHighestBlockLimit {
			return true, highestBlockLimit
		}
		highestBlockLimit = blk.BlockNum
	}
	return false, 0
}

func (b *Bundler) ToBundle(inclusiveHighestBlockLimit uint64) []*OneBlockFile {
	var out []*OneBlockFile
	b.db.IterateLinks(func(blockID, previousBlockID string, object interface{}) (getNext bool) {
		oneBlockFile := object.(*OneBlockFile)
		blkNum := oneBlockFile.num
		if !oneBlockFile.merged && blkNum <= inclusiveHighestBlockLimit { //get all none merged files
			out = append(out, oneBlockFile)
		}
		return true
	})

	sort.Slice(out, func(i, j int) bool {
		if out[i].blockTime.Equal(out[j].blockTime) {
			return out[i].num < out[j].num
		}

		return out[i].blockTime.Before(out[j].blockTime)
	})

	return out
}

func (b *Bundler) Commit(inclusiveHighestBlockLimit uint64) {
	oneBlockFiles := b.ToBundle(inclusiveHighestBlockLimit)
	var highestOneBlockFile *OneBlockFile

	for _, file := range oneBlockFiles {
		if highestOneBlockFile == nil || file.num >= highestOneBlockFile.num {
			highestOneBlockFile = b.db.BlockForID(file.id).Object.(*OneBlockFile)
		}
		file.merged = true
	}

	b.exclusiveHighestBlockLimit += b.bundleSize
	b.lastMergeOneBlockFile = highestOneBlockFile
	return
}

func (b *Bundler) Purge(callback func(purgedOneBlockFiles []*OneBlockFile)) {
	if b.lastMergeOneBlockFile == nil {
		return
	}
	libRef := b.db.BlockInCurrentChain(bstream.NewBlockRef(b.lastMergeOneBlockFile.id, b.lastMergeOneBlockFile.num), b.lastMergeOneBlockFile.libNum)
	var purgedOneBlockFiles []*OneBlockFile
	if libRef != bstream.BlockRefEmpty {
		purgedBlocks := b.db.MoveLIB(libRef)
		for _, block := range purgedBlocks {
			purgedOneBlockFiles = append(purgedOneBlockFiles, block.Object.(*OneBlockFile))
		}
	}
	callback(purgedOneBlockFiles)
	return
}
