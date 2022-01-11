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
	forkDB     *forkable.ForkDB
	bundleSize uint64

	lastMergeOneBlockFile      *OneBlockFile
	exclusiveHighestBlockLimit uint64

	mutex sync.Mutex
}

func NewBundler(bundleSize uint64, firstExclusiveHighestBlockLimit uint64) *Bundler {
	zlog.Info("new bundler", zap.Uint64("bundle_size", bundleSize), zap.Uint64("first_exclusive_highest_block_limit", firstExclusiveHighestBlockLimit))
	return &Bundler{
		bundleSize:                 bundleSize,
		forkDB:                     forkable.NewForkDB(),
		exclusiveHighestBlockLimit: firstExclusiveHighestBlockLimit,
	}
}

func (b *Bundler) String() string {
	var lastMergeBlockNum uint64
	if b.lastMergeOneBlockFile != nil {
		lastMergeBlockNum = b.lastMergeOneBlockFile.Num
	}

	lc := b.longestChain()
	return fmt.Sprintf(
		"\nbundle_size: %d, \nlast_merge_block_num: %d, \ninclusive_lower_block_num: %d, \nexclusive_highest_block_limit: %d \nlib_num: %d \nlib id:%s  \nlongest chain lenght: %d",
		b.bundleSize,
		lastMergeBlockNum,
		b.bundleInclusiveLowerBlock(),
		b.exclusiveHighestBlockLimit,
		b.forkDB.LIBNum(),
		b.forkDB.LIBID(),

		len(lc),
	)
}

func (b *Bundler) BundleInclusiveLowerBlock() uint64 {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.bundleInclusiveLowerBlock()
}

func (b *Bundler) bundleInclusiveLowerBlock() uint64 {
	return b.exclusiveHighestBlockLimit - b.bundleSize
}

func (b *Bundler) Bootstrap(fetchOneBlockFilesFromMergedFile func(lowBlockNum uint64) ([]*OneBlockFile, error)) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	initialLowBlockNum := b.bundleInclusiveLowerBlock() - b.bundleSize //we want the last one merged
	zlog.Info("Bootstrapping", zap.Uint64("initial_low_block_num", initialLowBlockNum))

	err := b.loadOneBlocksToLib(initialLowBlockNum, fetchOneBlockFilesFromMergedFile)
	if err != nil {
		return fmt.Errorf("loading one block files: %w", err)
	}

	zlog.Info("Bootstrapped", zap.Uint64("lib_num", b.forkDB.LIBNum()), zap.String("lib_id", b.forkDB.LIBID()))
	return nil
}

//100 - - - - - - 200 - - - - - - 300 - - - - - Live (400) 401  410  411
//                192             282                 375        376  376

func (b *Bundler) loadOneBlocksToLib(initialLowBlockNum uint64, fetchOneBlockFilesFromMergedFile func(lowBlockNum uint64) ([]*OneBlockFile, error)) error {

	lowBlockNum := initialLowBlockNum

	for {
		zlog.Info("fetching one block files", zap.Uint64("at_low_block_num", lowBlockNum))

		oneBlockFiles, err := fetchOneBlockFilesFromMergedFile(lowBlockNum)
		if err != nil {
			return fmt.Errorf("failed to fetch merged file for low block num: %d: %w", lowBlockNum, err)
		}

		sort.Slice(oneBlockFiles, func(i, j int) bool { return oneBlockFiles[i].Num < oneBlockFiles[j].Num })
		for _, f := range oneBlockFiles {
			f.Merged = true
			f.Deleted = true //one block files from merged file do not need to be deleted by merger
			b.addOneBlockFile(f)
		}
		if b.forkDB.HasLIB() {
			return nil
		}
		zlog.Info("processed one block files", zap.Uint64("at_low_block_num", lowBlockNum))

		lowBlockNum = lowBlockNum - b.bundleSize
	}
}

func findLibNum(oneBlockFiles []*OneBlockFile) uint64 {
	libNum := uint64(0)
	for _, oneBlockFile := range oneBlockFiles {
		if oneBlockFile.LibNum() > libNum {
			libNum = oneBlockFile.LibNum()
		}
	}

	return libNum
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
	if block := b.forkDB.BlockForID(oneBlockFile.ID); block != nil {
		obf := block.Object.(*OneBlockFile)
		for filename := range oneBlockFile.Filenames { //this is an ugly patch. ash stepd ;-)
			obf.Filenames[filename] = Empty
		}
		return true
	}

	blockRef := bstream.NewBlockRef(oneBlockFile.ID, oneBlockFile.Num)
	exists = b.forkDB.AddLink(blockRef, oneBlockFile.PreviousID, oneBlockFile)

	if !b.forkDB.HasLIB() { // always skip processing until LIB is set
		b.forkDB.SetLIB(bstream.NewBlockRef(oneBlockFile.ID, oneBlockFile.Num), oneBlockFile.PreviousID, oneBlockFile.LibNum())
	}

	return exists
}

func (b *Bundler) BundlePreMergedOneBlockFiles(oneBlockFiles []*OneBlockFile) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(oneBlockFiles) == 0 {
		return
	}
	for _, oneBlockFile := range oneBlockFiles {
		oneBlockFile.Merged = true
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
		oneBlockFiles = append(oneBlockFiles, b.forkDB.BlockForID(id).Object.(*OneBlockFile))
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

	tree, err := b.forkDB.BuildTree()
	if err != nil {
		return nil //this is happening when there is no links in forkDB
	}

	return tree.Chains().LongestChain()
}

func (b *Bundler) LongestChainLastBlockFile() *OneBlockFile {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	ch := b.longestChain()
	if len(ch) == 0 {
		return nil
	}
	blk := b.forkDB.BlockForID(ch[len(ch)-1])
	return blk.Object.(*OneBlockFile)
}

func (b *Bundler) IsBlockTooOld(blockNum uint64) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	roots, err := b.forkDB.Roots()
	if err != nil { //if there is no root it can't be too old
		return false
	}

	//Find the smallest root of all chains in forkdb
	lowestRootBlockNum := uint64(math.MaxUint64)
	for _, root := range roots {
		block := b.forkDB.BlockForID(root)
		if block.BlockNum < lowestRootBlockNum {
			lowestRootBlockNum = block.BlockNum
		}
	}

	// return true is blockNum is small that all the root block of all chains
	return blockNum < lowestRootBlockNum
}

func (b *Bundler) LongestChainFirstBlockNum() (uint64, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	longestChain := b.longestChain()
	if longestChain == nil || len(longestChain) == 0 {
		return 0, fmt.Errorf("no longest chain available")
	}
	block := b.forkDB.BlockForID(longestChain[0])
	return block.BlockNum, nil
}

func (b *Bundler) BundleCompleted() (complete bool, highestBlockLimit uint64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	//{
	//name: "wrong longest chain",
	//	files: []string{
	//	"0000000115-20210728T105016.0-00000115a-00000114a-90-suffix",
	//	"0000000116-20210728T105016.0-00000116a-00000115a-90-suffix",
	//	"0000000117-20210728T105016.0-00000117a-00000116a-90-suffix",
	//	"0000000118-20210728T105016.0-00000118a-00000117a-90-suffix",
	//	"0000000120-20210728T105016.0-00000120a-00000118a-90-suffix",
	//
	//	"0000000300-20210728T105016.0-00000300a-00000299a-150-suffix",
	//	"0000000301-20210728T105016.0-00000301a-00000300a-150-suffix",
	//	"0000000302-20210728T105016.0-00000302a-00000301a-150-suffix",
	//	"0000000303-20210728T105016.0-00000303a-00000302a-150-suffix",
	//	"0000000304-20210728T105016.0-00000304a-00000303a-150-suffix",
	//	"0000000305-20210728T105016.0-00000305a-00000304a-150-suffix",
	//},
	//	lastMergeBlockID:           "00000114a",
	//	exclusiveHighestBlockLimit: 120,
	//	expectedCompleted:          false,
	//	expectedHighestBlockLimit:  118,
	//},

	longest := b.longestChain()

	for _, blockID := range longest {
		blk := b.forkDB.BlockForID(blockID)

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

	out := b.toBundle(inclusiveHighestBlockLimit)
	return out
}
func (b *Bundler) toBundle(inclusiveHighestBlockLimit uint64) []*OneBlockFile {

	var out []*OneBlockFile
	//1100
	//995a - 996a - 997a - 998a - 999a - 1000a - 1001a - 1002a - 1003a - 1004a - 1005a - 1006a -- 1101
	//									 - 1001b - 1002b - 1003b - 1004b

	b.forkDB.IterateLinks(func(blockID, previousBlockID string, object interface{}) (getNext bool) {
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

	//if uint64(len(out)) != b.bundleSize {
	//	panic(fmt.Sprintf("toBundle() called with missing block files; out: %d, bundleSize: %d", len(out), b.bundleSize))
	//}

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
			highestOneBlockFile = b.forkDB.BlockForID(file.ID).Object.(*OneBlockFile)
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
	libRef := b.forkDB.BlockInCurrentChain(bstream.NewBlockRef(b.lastMergeOneBlockFile.ID, b.lastMergeOneBlockFile.Num), lastMergeOneBlockFileLibNum)
	collected := map[string]*OneBlockFile{}
	if libRef != bstream.BlockRefEmpty {
		purgedBlocks := b.forkDB.MoveLIB(libRef)
		for _, block := range purgedBlocks {
			oneBlockFile := block.Object.(*OneBlockFile)
			if oneBlockFile.Merged && !oneBlockFile.Deleted {
				collected[block.BlockID] = block.Object.(*OneBlockFile)
			}
		}
	}

	b.forkDB.IterateLinks(func(blockID, previousBlockID string, object interface{}) (getNext bool) {
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
