package merger

import (
	"encoding/gob"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"go.uber.org/zap"
)

type Bundler struct {
	db         *forkable.ForkDB
	bundleSize uint64

	//cache
	lastMergeOneBlockFile      *OneBlockFile
	exclusiveHighestBlockLimit uint64
	filename                   string
}

func NewBundler(bundleSize uint64, firstExclusiveHighestBlockLimit uint64, filename string) *Bundler {
	return &Bundler{
		bundleSize:                 bundleSize,
		db:                         forkable.NewForkDB(),
		exclusiveHighestBlockLimit: firstExclusiveHighestBlockLimit,
		filename:                   filename,
	}
}

func NewBundlerFromFile(filename string) (bundler *Bundler, err error) {
	bundler, err = load(filename)
	if err != nil {
		return nil, fmt.Errorf("loading bundler from file: %s : %w", filename, err)
	}
	zlog.Info("loaded bundler", zap.String("filename", filename), zap.Stringer("bundler", bundler))
	bundler.filename = filename
	return
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
	//todo: bootstrap using libNum ....
	//initialLowBlockNum := b.BundleInclusiveLowerBlock()
	//blockNumToReach := b.exclusiveHighestBlockLimit - b.maxFixableFork
	//firstStreamableBlockNum := bstream.GetProtocolFirstStreamableBlock
	//
	//if blockNumToReach < firstStreamableBlockNum {
	//	blockNumToReach = firstStreamableBlockNum
	//}
	//
	//err := b.loadOneBlocks(initialLowBlockNum, blockNumToReach, fetchOneBlockFilesFromMergedFile)
	//if err != nil {
	//	return fmt.Errorf("loading one block files")
	//}
	return nil
}

func (b *Bundler) loadOneBlocks(initialLowBlockNum, blockNumToReach uint64, fetchOneBlockFilesFromMergedFile func(lowBlockNum uint64) ([]*OneBlockFile, error)) error {
	lowBlockNum := initialLowBlockNum
	optimizeStopOnSingleRoot := false
	lowestBlockNumAdded := uint64(math.MaxUint64)

	for {
		zlog.Info("fetching one block files", zap.Uint64("at_low_block_num", lowBlockNum))

		oneBlockFiles, err := fetchOneBlockFilesFromMergedFile(lowBlockNum)
		if err != nil {
			return fmt.Errorf("failed to fetch merged file for low block num: %d: %w", lowBlockNum, err)
		}
		sort.Slice(oneBlockFiles, func(i, j int) bool { return oneBlockFiles[i].num > oneBlockFiles[j].num })

		for _, f := range oneBlockFiles {
			f.merged = true
			b.AddOneBlockFile(f)
			if f.num < lowestBlockNumAdded {
				lowestBlockNumAdded = f.num
			}

			if optimizeStopOnSingleRoot {
				if b.rootCount() == 1 {
					return nil
				}
			}
		}

		zlog.Info("processed one block files", zap.Uint64("at_low_block_num", lowBlockNum))

		lowBlockNum = lowBlockNum - b.bundleSize
		if b.rootCount() != 1 && lowestBlockNumAdded <= blockNumToReach {
			// we got multiple roots we need more block from earlier merge file
			optimizeStopOnSingleRoot = true
			continue
		}

		if lowestBlockNumAdded <= blockNumToReach {
			break
		}
	}
	return nil
}

func (b *Bundler) rootCount() uint64 {
	roots, err := b.db.Roots()
	if err != nil {
		return 0
	}

	return uint64(len(roots))
}

func (b *Bundler) AddFile(filename string, blockNum uint64, blockTime time.Time, blockID string, previousID string, libNum uint64, canonicalName string) {
	if block := b.db.BlockForID(blockID); block != nil {
		obf := block.Object.(*OneBlockFile)
		obf.filenames[filename] = Empty
	}

	obf := &OneBlockFile{
		canonicalName: canonicalName,
		filenames: map[string]struct{}{
			filename: Empty,
		},
		blockTime:  blockTime,
		id:         blockID,
		num:        blockNum,
		previousID: previousID,
		libNum:     libNum,
	}

	b.AddOneBlockFile(obf)
}

func (b *Bundler) AddOneBlockFile(oneBlockFile *OneBlockFile) (exist bool) {
	//todo: should we accept before being bootstrapped

	blockRef := bstream.NewBlockRef(oneBlockFile.id, oneBlockFile.num)
	exist = b.db.AddLink(blockRef, oneBlockFile.previousID, oneBlockFile)
	return
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
	panic("implement base on libNum")
	return true
	//roots, err := b.db.Roots()
	//if err != nil { //if there is no root it can't be too old
	//	return false
	//}
	//
	//highestBlockNum := uint64(0)
	//for _, root := range roots {
	//	tree := b.db.BuildTreeWithID(root)
	//	longestChain := tree.Chains().LongestChain()
	//	leafBlockID := longestChain[len(longestChain)-1]
	//	leafBlock := b.db.BlockForID(leafBlockID)
	//	if leafBlock.BlockNum > highestBlockNum {
	//		highestBlockNum = leafBlock.BlockNum
	//	}
	//}
	//
	//acceptableBlockNum := int64(highestBlockNum - b.maxFixableFork)
	//return int64(blockNum) < acceptableBlockNum
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

func load(filename string) (bundler *Bundler, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return
	}
	defer f.Close()

	dataDecoder := gob.NewDecoder(f)
	err = dataDecoder.Decode(&bundler)
	return
}

func (b *Bundler) Save() error {
	if b.filename == "" { // in memory mode
		return nil
	}
	f, err := os.Create(b.filename)
	if err != nil {
		return err
	}
	defer f.Close()
	dataEncoder := gob.NewEncoder(f)
	return dataEncoder.Encode(b)
}
