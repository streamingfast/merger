package bundle

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"go.uber.org/zap"
)

type Bundler struct {
	db         *forkable.ForkDB
	bundleSize uint64

	lastMergeOneBlockFile      *OneBlockFile
	exclusiveHighestBlockLimit uint64

	mutex sync.Mutex
}

func NewBundler(bundleSize uint64, firstExclusiveHighestBlockLimit uint64) *Bundler {
	zlog.Info("new bundler", zap.Uint64("bundle_size", bundleSize), zap.Uint64("first_exclusive_highest_block_limit", firstExclusiveHighestBlockLimit))
	return &Bundler{
		bundleSize:                 bundleSize,
		db:                         forkable.NewForkDB(),
		exclusiveHighestBlockLimit: firstExclusiveHighestBlockLimit,
	}
}

func (b *Bundler) String() string {
	var lastMergeBlockNum uint64
	if b.lastMergeOneBlockFile != nil {
		lastMergeBlockNum = b.lastMergeOneBlockFile.Num
	}

	return fmt.Sprintf("bundle_size: %d, last_merge_block_num: %d, inclusive_lower_block_num: %d, exclusive_highest_block_limit: %d", b.bundleSize, lastMergeBlockNum, b.BundleInclusiveLowerBlock(), b.exclusiveHighestBlockLimit)
}

func (b *Bundler) BundleInclusiveLowerBlock() uint64 {
	return b.exclusiveHighestBlockLimit - b.bundleSize
}

func (b *Bundler) Boostrap(fetchOneBlockFilesFromMergedFile func(lowBlockNum uint64) ([]*OneBlockFile, error)) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

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
		sort.Slice(oneBlockFiles, func(i, j int) bool { return oneBlockFiles[i].Num > oneBlockFiles[j].Num })
		for _, f := range oneBlockFiles {
			if libNum == math.MaxUint64 {
				zlog.Info("found lib to reach", zap.Uint64("lib_block_num", libNum))
				libNum = f.LibNum()
			}
			f.Merged = true
			b.addOneBlockFile(f)
			if f.Num == libNum {
				return nil
			}
		}

		zlog.Info("processed one block files", zap.Uint64("at_low_block_num", lowBlockNum), zap.Uint64("lib_num_to_reach", libNum))

		lowBlockNum = lowBlockNum - b.bundleSize
	}
}

func (b *Bundler) LastMergeOneBlockFile() *OneBlockFile {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.lastMergeOneBlockFile
}

func (b *Bundler) ExclusiveHighestBlockLimit() uint64 {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.exclusiveHighestBlockLimit
}

func (b *Bundler) AddOneBlockFile(oneBlockFile *OneBlockFile) (exist bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	exists := b.addOneBlockFile(oneBlockFile)
	return exists
}

func (b *Bundler) addOneBlockFile(oneBlockFile *OneBlockFile) (exists bool) {
	if block := b.db.BlockForID(oneBlockFile.ID); block != nil {
		obf := block.Object.(*OneBlockFile)
		for filename := range oneBlockFile.Filenames { //this is an ugly patch. ash stepd ;-)
			obf.Filenames[filename] = Empty
		}
		return true
	}

	blockRef := bstream.NewBlockRef(oneBlockFile.ID, oneBlockFile.Num)
	exists = b.db.AddLink(blockRef, oneBlockFile.PreviousID, oneBlockFile)
	return exists
}

func (b *Bundler) AddPreMergedOneBlockFiles(oneBlockFiles []*OneBlockFile) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(oneBlockFiles) == 0 {
		return
	}
	for _, oneBlockFile := range oneBlockFiles {
		b.addOneBlockFile(oneBlockFile)
	}
	b.lastMergeOneBlockFile = oneBlockFiles[len(oneBlockFiles)-1]
	b.exclusiveHighestBlockLimit += b.bundleSize
}

func (b *Bundler) LongestOneBlockFileChain() (oneBlockFiles []*OneBlockFile) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	lc := b.longestChain()
	for _, id := range lc {
		oneBlockFiles = append(oneBlockFiles, b.db.BlockForID(id).Object.(*OneBlockFile))
	}
	sort.Slice(oneBlockFiles, func(i, j int) bool {
		if oneBlockFiles[i].BlockTime.Equal(oneBlockFiles[j].BlockTime) {
			return oneBlockFiles[i].Num < oneBlockFiles[j].Num
		}

		return oneBlockFiles[i].BlockTime.Before(oneBlockFiles[j].BlockTime)
	})

	return
}
func (b *Bundler) LongestChain() []string {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.longestChain()
}

func (b *Bundler) longestChain() []string {

	roots, err := b.db.Roots()
	if err != nil {
		return nil //this is happening when there is no links in db
	}

	var longestChain []string

	for _, root := range roots {
		tree := b.db.BuildTreeWithID(root)
		lc := tree.Chains().LongestChain()

		if lc == nil || len(longestChain) == len(lc) {
			// found multiple chains with same length
			return nil
		}

		if len(longestChain) < len(lc) {
			longestChain = lc
		}
	}

	return longestChain
}

func (b *Bundler) IsBlockTooOld(blockNum uint64) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

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
	b.mutex.Lock()
	defer b.mutex.Unlock()

	longestChain := b.longestChain()
	if longestChain == nil || len(longestChain) == 0 {
		return 0, fmt.Errorf("no longest chain available")
	}
	block := b.db.BlockForID(longestChain[0])
	return block.BlockNum, nil
}

func (b *Bundler) IsComplete() (complete bool, highestBlockLimit uint64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	longest := b.longestChain()
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
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.toBundle(inclusiveHighestBlockLimit)
}
func (b *Bundler) toBundle(inclusiveHighestBlockLimit uint64) []*OneBlockFile {

	var out []*OneBlockFile
	//1100
	//995a - 996a - 997a - 998a - 999a - 1000a - 1001a - 1002a - 1003a - 1004a - 1005a - 1006a -- 1101
	//									 - 1001b - 1002b - 1003b - 1004b

	b.db.IterateLinks(func(blockID, previousBlockID string, object interface{}) (getNext bool) {
		oneBlockFile := object.(*OneBlockFile)
		blkNum := oneBlockFile.Num
		if !oneBlockFile.Merged && blkNum <= inclusiveHighestBlockLimit { //get all none merged files
			out = append(out, oneBlockFile)
		}
		return true
	})

	sort.Slice(out, func(i, j int) bool {
		if out[i].BlockTime.Equal(out[j].BlockTime) {
			return out[i].Num < out[j].Num
		}

		return out[i].BlockTime.Before(out[j].BlockTime)
	})

	return out
}

//1100
//995a - 996a - 997a - 998a - 999a - 1000a - 1001a - 1002a - 1003a - 1004a - 1005a - 1006a -- 1101
//									 - 1001b - 1002b - 1003b - 1004b

func (b *Bundler) Commit(inclusiveHighestBlockLimit uint64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	oneBlockFiles := b.toBundle(inclusiveHighestBlockLimit)
	var highestOneBlockFile *OneBlockFile

	for _, file := range oneBlockFiles {
		if highestOneBlockFile == nil || file.Num >= highestOneBlockFile.Num {
			highestOneBlockFile = b.db.BlockForID(file.ID).Object.(*OneBlockFile)
		}
		file.Merged = true
	}

	b.exclusiveHighestBlockLimit += b.bundleSize
	b.lastMergeOneBlockFile = highestOneBlockFile
	return
}

func (b *Bundler) Purge(callback func(oneBlockFilesToDelete []*OneBlockFile)) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.lastMergeOneBlockFile == nil {
		return
	}
	lastMergeOneBlockFileLibNum := b.lastMergeOneBlockFile.LibNum()
	libRef := b.db.BlockInCurrentChain(bstream.NewBlockRef(b.lastMergeOneBlockFile.ID, b.lastMergeOneBlockFile.Num), lastMergeOneBlockFileLibNum)
	collected := map[string]*OneBlockFile{}
	if libRef != bstream.BlockRefEmpty {
		purgedBlocks := b.db.MoveLIB(libRef)
		for _, block := range purgedBlocks {
			oneBlockFile := block.Object.(*OneBlockFile)
			if oneBlockFile.Merged && !oneBlockFile.Deleted {
				collected[block.BlockID] = block.Object.(*OneBlockFile)
			}
		}
	}

	b.db.IterateLinks(func(blockID, previousBlockID string, object interface{}) (getNext bool) {
		oneBlockFile := object.(*OneBlockFile)
		if oneBlockFile.Merged && !oneBlockFile.Deleted {
			collected[oneBlockFile.ID] = oneBlockFile
		}
		return true
	})

	var toDelete []*OneBlockFile
	for _, oneBlockFile := range collected {
		oneBlockFile.Deleted = true
		toDelete = append(toDelete, oneBlockFile)
	}
	callback(toDelete)

	return
}
