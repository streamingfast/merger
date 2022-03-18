package bundle

import (
	"fmt"
	"sort"
	"sync"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
		forkDB:                     forkable.NewForkDB(forkable.ForkDBWithLogger(zlog)),
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
		"bundle_size: %d, last_merge_block_num: %d, inclusive_lower_block_num: %d, exclusive_highest_block_limit: %d lib_num: %d lib id:%s longest chain lenght: %d",
		b.bundleSize,
		lastMergeBlockNum,
		b.bundleInclusiveLowerBlock(),
		b.exclusiveHighestBlockLimit,
		b.forkDB.LIBNum(),
		b.forkDB.LIBID(),

		len(lc),
	)
}

func (b *Bundler) InitLIB(blk bstream.BlockRef) {
	b.forkDB.InitLIB(blk)
}

func (b *Bundler) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddUint64("bundle_size", b.bundleSize)
	encoder.AddUint64("inclusive_lower_block_num", b.bundleInclusiveLowerBlock())
	encoder.AddUint64("exclusive_highest_block_limit", b.exclusiveHighestBlockLimit)

	if b.lastMergeOneBlockFile != nil {
		encoder.AddUint64("last_merge_one_block_num", b.lastMergeOneBlockFile.Num)
		encoder.AddTime("last_merge_one_block_time", b.lastMergeOneBlockFile.BlockTime)
	}

	encoder.AddUint64("lib_num", b.forkDB.LIBNum())
	encoder.AddString("lib_id", b.forkDB.LIBID())
	encoder.AddInt("longest_chain_length", len(b.longestChain()))

	return nil
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

	lastMergedLowBlockNum := b.bundleInclusiveLowerBlock() - b.bundleSize //we want the last one merged

	oneBlockFiles, err := fetchOneBlockFilesFromMergedFile(lastMergedLowBlockNum)
	if err != nil {
		return fmt.Errorf("searching for lib: failed to fetch merged file for low block num: %d: %w", lastMergedLowBlockNum, err)
	}

	libNumToStartFrom := findLibNum(oneBlockFiles)
	zlog.Info("Bootstrapping ", zap.Uint64("lib_num_to_start_from", libNumToStartFrom), zap.Uint64("last_merged_low_block_num", lastMergedLowBlockNum))

	err = b.loadOneBlocksFromLib(libNumToStartFrom, lastMergedLowBlockNum, fetchOneBlockFilesFromMergedFile)
	if err != nil {
		return fmt.Errorf("loading one block files: %w", err)
	}

	if !b.forkDB.HasLIB() {
		return fmt.Errorf("bootstrap completed and lib not set")
	}

	zlog.Info("Bootstrapped", zap.Uint64("lib_num", b.forkDB.LIBNum()), zap.String("lib_id", b.forkDB.LIBID()))
	return nil
}

func (b *Bundler) loadOneBlocksFromLib(libNumToStartFrom uint64, lastMergedLowBlockNum uint64, fetchOneBlockFilesFromMergedFile func(lowBlockNum uint64) ([]*OneBlockFile, error)) error {
	lowBlockNum := (libNumToStartFrom / b.bundleSize) * b.bundleSize
	for {
		zlog.Info("fetching merged files", zap.Uint64("at_low_block_num", lowBlockNum))

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
		zlog.Info("processed merge file", zap.Uint64("at_low_block_num", lowBlockNum))

		lowBlockNum = lowBlockNum + b.bundleSize

		if lowBlockNum > lastMergedLowBlockNum {
			break
		}
	}
	return nil
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
	zlog.Debug("adding one block file", zap.String("file_name", oneBlockFile.CanonicalName))

	blockRef := bstream.NewBlockRef(oneBlockFile.ID, oneBlockFile.Num)
	exists = b.forkDB.AddLink(blockRef, oneBlockFile.PreviousID, oneBlockFile)

	level := zap.DebugLevel
	if !b.forkDB.HasLIB() {
		level = zap.InfoLevel
	}

	if b.forkDB.HasLIB() {

		if oneBlockFile.LibNum() <= b.forkDB.LIBNum() {
			return exists
		}

		isPartOfCurrentChain := b.forkDB.BlockInCurrentChain(blockRef, b.forkDB.LIBNum()) != bstream.BlockRefEmpty
		if !isPartOfCurrentChain {
			return exists
		}
	}

	zlog.Check(level, "setting lib value").Write(zap.Uint64("current_block_num", oneBlockFile.Num), zap.Uint64("lib_num_candidate", oneBlockFile.LibNum()))
	b.forkDB.SetLIB(bstream.NewBlockRef(oneBlockFile.ID, oneBlockFile.Num), oneBlockFile.PreviousID, oneBlockFile.LibNum())

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

	rootID, err := b.forkDB.Root()
	if err != nil {
		return false
	}

	rootOneBlockFile := b.forkDB.BlockForID(rootID).Object.(*OneBlockFile)
	//root is always <= to the forkdb lib.
	return blockNum < rootOneBlockFile.Num
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

	b.forkDB.IterateLinks(func(_, _ string, object interface{}) (getNext bool) {
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
