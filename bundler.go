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
	tree                       *forkable.Node
	chains                     *forkable.ChainList
	lastMergeBlock             *forkable.Block
	exclusiveHighestBlockLimit uint64
	filename                   string
	maxFixableFork             uint64
}

func NewBundler(bundleSize uint64, maxFixableFork uint64, firstExclusiveHighestBlockLimit uint64, filename string) *Bundler {
	return &Bundler{
		bundleSize:                 bundleSize,
		maxFixableFork:             maxFixableFork,
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
	if b.lastMergeBlock != nil {
		lastMergeBlockNum = b.lastMergeBlock.BlockNum
	}

	return fmt.Sprintf("bundle_size: %d, last_merge_block_num: %d, inclusive_lower_block_num: %d, exclusive_highest_block_limit: %d", b.bundleSize, lastMergeBlockNum, b.BundleInclusiveLowerBlock(), b.exclusiveHighestBlockLimit)
}

func (b *Bundler) BundleInclusiveLowerBlock() uint64 {
	return b.exclusiveHighestBlockLimit - b.bundleSize
}

func (b *Bundler) Boostrap(fetchOneBlockFilesFromMergedFile func(lowBlockNum uint64) ([]*OneBlockFile, error)) {
	b.tryLoadOneBlock(b.BundleInclusiveLowerBlock(), b.BundleInclusiveLowerBlock(), fetchOneBlockFilesFromMergedFile)
	return
}

func (b *Bundler) tryLoadOneBlock(initialLowBlockNum, lowBlockNumToReach uint64, fetchOneBlockFilesFromMergedFile func(lowBlockNum uint64) ([]*OneBlockFile, error)) {
	lowBlockNum := initialLowBlockNum
	optimizeStopOnSingleRoot := false
	lowestBlockNumAdded := uint64(math.MaxUint64)

	for {
		zlog.Info("fetching one block files", zap.Uint64("at_low_block_num", lowBlockNum))

		oneBlockFiles, err := fetchOneBlockFilesFromMergedFile(lowBlockNum)
		if err != nil {
			zlog.Warn("**************************************************************************************")
			zlog.Warn("failed to fetch merged file", zap.Uint64("low_block_num", lowBlockNum))
			if b.rootCount() != 1 {
				zlog.Warn("bundler forkdb has unstable root", zap.Uint64("root_count", b.rootCount()))
			}
			zlog.Warn("**************************************************************************************")
			return
		}
		sort.Slice(oneBlockFiles, func(i, j int) bool { return oneBlockFiles[i].num > oneBlockFiles[j].num })

		for _, f := range oneBlockFiles {
			f.merged = true
			b.AddOneBlockFile(f)
			if f.num < lowestBlockNumAdded {
				lowestBlockNumAdded = f.num
			}

			if optimizeStopOnSingleRoot {
				if b.rootCount() == 1 && lowestBlockNumAdded <= lowBlockNumToReach {
					return
				}
			}
		}

		zlog.Info("processed one block files", zap.Uint64("at_low_block_num", lowBlockNum))

		if b.rootCount() == 1 && lowestBlockNumAdded <= lowBlockNumToReach {
			break
		}

		// we got multiple roots we need more block from earlier merge file
		lowBlockNum = lowBlockNum - b.bundleSize
		optimizeStopOnSingleRoot = true
	}
	b.resetMemoize()
}

func (b *Bundler) rootCount() uint64 {
	roots, err := b.db.Roots()
	if err != nil {
		return 0
	}

	return uint64(len(roots))
}

func (b *Bundler) AddFile(filename string, blockNum uint64, blockTime time.Time, blockID string, previousID string, canonicalName string) {
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
	}

	b.AddOneBlockFile(obf)
}

func (b *Bundler) AddOneBlockFile(oneBlockFile *OneBlockFile) (exist bool) {
	blockRef := bstream.NewBlockRef(oneBlockFile.id, oneBlockFile.num)
	exist = b.db.AddLink(blockRef, oneBlockFile.previousID, oneBlockFile)
	b.resetMemoize()
	return
}

func (b *Bundler) resetMemoize() {
	b.tree = nil
	b.chains = nil
}

func (b *Bundler) getTree() (*forkable.Node, error) {
	if b.tree == nil {
		t, err := b.db.BuildTree()
		if err != nil {
			return nil, fmt.Errorf("building tree: %w", err)
		}
		b.tree = t
	}

	return b.tree, nil
}

func (b *Bundler) getChains() (*forkable.ChainList, error) {
	tree, err := b.getTree()
	if err != nil {
		return nil, fmt.Errorf("getting tree: %w", err)
	}
	if b.chains == nil {
		b.chains = tree.Chains()
	}
	return b.chains, nil
}

func (b *Bundler) IsBlockTooOld(blockNum uint64) bool {
	roots, err := b.db.Roots()
	if err != nil { //if there is no root it can't be too old
		return false
	}

	highestBlockNum := uint64(0)
	for _, root := range roots {
		tree := b.db.BuildTreeWithID(root)
		longestChain := tree.Chains().LongestChain()
		leafBlockID := longestChain[len(longestChain)-1]
		leafBlock := b.db.BlockForID(leafBlockID)
		if leafBlock.BlockNum > highestBlockNum {
			highestBlockNum = leafBlock.BlockNum
		}
	}

	acceptableBlockNum := int64(highestBlockNum - b.maxFixableFork)
	return int64(blockNum) < acceptableBlockNum
}

func (b *Bundler) LongestChainFirstBlockNum() (uint64, error) {
	chains, err := b.getChains()
	if err != nil {
		return 0, fmt.Errorf("getting chains: %w", err)
	}

	longestChain := chains.LongestChain()
	if len(longestChain) == 0 {
		return 0, fmt.Errorf("no longuest chain available")
	}
	block := b.db.BlockForID(longestChain[0])
	return block.BlockNum, err
}

func (b *Bundler) isComplete() (complete bool, highestBlockLimit uint64) {
	chains, err := b.getChains()
	if err != nil {
		return false, 0
	}

	longest := chains.LongestChain()
	for _, blockID := range longest {
		blk := b.db.BlockForID(blockID)

		if blk.BlockNum >= b.exclusiveHighestBlockLimit {
			return true, highestBlockLimit
		}
		highestBlockLimit = blk.BlockNum
	}

	return false, 0
}

func (b *Bundler) ToBundle(inclusiveHighestBlockLimit uint64) ([]*OneBlockFile, error) {
	chains, err := b.getChains()
	if err != nil {
		return nil, err
	}

	seen := map[string]bool{}
	var out []*OneBlockFile
	for _, chain := range chains.Chains {
		for _, blockID := range chain {
			if found := seen[blockID]; found {
				continue
			}
			blk := b.db.BlockForID(blockID)
			oneBlockFile := blk.Object.(*OneBlockFile)
			blkNum := blk.BlockNum
			if !oneBlockFile.merged && blkNum <= inclusiveHighestBlockLimit { //get all none merged files
				seen[blockID] = true
				out = append(out, oneBlockFile)
			}
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].blockTime.Equal(out[j].blockTime) {
			return out[i].num < out[j].num
		}

		return out[i].blockTime.Before(out[j].blockTime)
	})

	return out, nil
}

func (b *Bundler) Commit(inclusiveHighestBlockLimit uint64) error {
	oneBlockFiles, err := b.ToBundle(inclusiveHighestBlockLimit)
	if err != nil {
		return err
	}
	var highestBlock *forkable.Block

	for _, file := range oneBlockFiles {
		if highestBlock == nil || file.num >= highestBlock.BlockNum {
			highestBlock = b.db.BlockForID(file.id)
		}
		file.merged = true
	}

	b.exclusiveHighestBlockLimit += b.bundleSize
	b.lastMergeBlock = highestBlock
	return nil
}

func (b *Bundler) Purge(callback func(purgedOneBlockFiles []*OneBlockFile)) error {
	chainLists, err := b.getChains()
	if err != nil {
		//todo: maybe log info ...
		return nil
	}

	longestChain := chainLists.LongestChain()
	longestChainLength := uint64(len(longestChain))
	if longestChainLength == 0 {
		return nil
	}

	if longestChainLength < b.maxFixableFork {
		return nil
	}

	delta := longestChainLength - b.maxFixableFork
	upToBlockID := longestChain[delta-1]
	upToBlock := b.db.BlockForID(upToBlockID)

	return b.purge(upToBlock.BlockNum, callback)
}

func (b *Bundler) purge(upToBlock uint64, callback func(purgedOneBlockFiles []*OneBlockFile)) error {
	node, err := b.getTree()
	if err != nil {
		return err
	}

	chains := node.Chains()
	longest := chains.LongestChain()
	purgedOneBlockFiles := make([]*OneBlockFile, 0)
	purgedOneBlockFiles, _ = recursivePurge(upToBlock, node, longest, b.db, purgedOneBlockFiles)
	b.resetMemoize()

	callback(purgedOneBlockFiles)

	return nil
}

//                                  |                           |                                  |                           |
// 100a - 101a - 102a - 103a - 104a - 106a - 107a - 108a - 109a - 110a - 111a - 112a - 113a - 114a - 115a - 116a - 117a - 118a - 120a
//            \- 102b - 103b                     \- 108b - 109b - 110b
//                                                             \- 110c - 111c

func recursivePurge(upToBlock uint64, node *forkable.Node, longest []string, db *forkable.ForkDB, alreadyPurgedOneBlockFiles []*OneBlockFile) (purgedOneBlockFiles []*OneBlockFile, stop bool) {
	purgedOneBlockFiles = alreadyPurgedOneBlockFiles

	block := db.BlockForID(node.ID)
	oneBlock := block.Object.(*OneBlockFile)
	if oneBlock.merged {
		return nil, false //we just skip this block, but we continue to walk the tree
	}

	if block.BlockNum > upToBlock {
		return nil, true
	}

	forkDetected := len(node.Children) > 1
	if forkDetected {
		purgeableFork := isPurgeableFork(node.Children, upToBlock, longest, db)
		if !purgeableFork {
			return nil, true // we stop
		}
	}

	for _, child := range node.Children {
		purgedOneBlockFiles, stop = recursivePurge(upToBlock, child, longest, db, purgedOneBlockFiles)
		if stop {
			break
		}
	}

	purgedOneBlockFiles = append(purgedOneBlockFiles, block.Object.(*OneBlockFile))
	db.DeleteLink(block.BlockID)
	return purgedOneBlockFiles, false
}

func isPurgeableFork(nodes []*forkable.Node, upToBlock uint64, longest []string, db *forkable.ForkDB) bool {
	for _, node := range nodes {
		if inChain(node.ID, longest) { //block on longest chain should always be purgeable
			continue
		}

		block := db.BlockForID(node.ID)
		if block.BlockNum > upToBlock {
			return false
		}

		purgeable := isPurgeableFork(node.Children, upToBlock, longest, db)
		if !purgeable {
			return false
		}
	}

	return true
}

func inChain(lookupID string, chain []string) bool {
	for _, id := range chain {
		if id == lookupID {
			return true
		}
	}
	return false
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
