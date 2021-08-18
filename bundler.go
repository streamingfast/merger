package merger

import (
	"fmt"
	"sort"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
)

type Bundler struct {
	db         *forkable.ForkDB
	bundleSize uint64

	//cache
	tree                       *forkable.Node
	chains                     *forkable.ChainList
	lastMergeBlock             *forkable.Block
	exclusiveHighestBlockLimit uint64
}

func NewBundler(bundleSize uint64) *Bundler {
	return &Bundler{
		bundleSize: bundleSize,
		db:         forkable.NewForkDB(),
	}
}

func (b *Bundler) AddFile(filename string, blockNum uint64, blockTime time.Time, blockID string, previousID string, canonicalName string) error {
	if block := b.db.BlockForID(blockID); block != nil {
		obf := block.Object.(*OneBlockFile)
		obf.filenames[filename] = Empty
		return nil
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

	return b.AddOneBlockFile(obf)
}

func (b *Bundler) AddOneBlockFile(oneBlockFile *OneBlockFile) error {
	blockRef := bstream.NewBlockRef(oneBlockFile.id, oneBlockFile.num)
	b.db.AddLink(blockRef, oneBlockFile.previousID, oneBlockFile)
	b.reset()
	return nil
}

func (b *Bundler) reset() {
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

func (b *Bundler) MergeableFiles(inclusiveHighestBlockLimit uint64) ([]*OneBlockFile, error) {
	chains, err := b.getChains()
	if err != nil {
		return nil, err
	}

	seen := map[string]bool{}
	var out []*OneBlockFile
	var highestBlockNum uint64
	var highestBlock *forkable.Block
	for _, chain := range chains.Chains {
		for _, blockID := range chain {
			if found := seen[blockID]; found {
				continue
			}
			blk := b.db.BlockForID(blockID)
			oneBlockFile := blk.Object.(*OneBlockFile)
			blkNum := blk.BlockNum
			if !oneBlockFile.merged && blkNum <= inclusiveHighestBlockLimit { //get all none merged files
				if blkNum > highestBlockNum {
					highestBlockNum = blkNum
					highestBlock = blk
				}
				seen[blockID] = true

				oneBlockFile.merged = true //todo: this should be done only when file is really merged
				out = append(out, oneBlockFile)
			}
		}
	}
	b.exclusiveHighestBlockLimit += b.bundleSize //todo: this should be done only when file is really merged
	b.lastMergeBlock = highestBlock              //todo: this should be done only when file is really merged

	sort.Slice(out, func(i, j int) bool {
		if out[i].blockTime.Equal(out[j].blockTime) {
			return out[i].num < out[j].num
		}

		return out[i].blockTime.Before(out[j].blockTime)
	})

	return out, nil
}

func (b *Bundler) Purge(upToBlock uint64) error {
	node, err := b.getTree()
	if err != nil {
		return err
	}

	chains := node.Chains()
	longest := chains.LongestChain()
	purge(upToBlock, node, longest, b.db)
	b.reset()
	return nil
}

func purge(upToBlock uint64, node *forkable.Node, longest []string, db *forkable.ForkDB) (stop bool) {
	//todo: we should not purge none merge block ...

	block := db.BlockForID(node.ID)
	if block.BlockNum > upToBlock {
		return true
	}

	forkDetected := len(node.Children) > 1
	if forkDetected {
		purgeableFork := isPurgeableFork(node.Children, upToBlock, longest, db)
		if !purgeableFork {
			return true // we stop
		}
	}

	for _, child := range node.Children {
		stop = purge(upToBlock, child, longest, db)
		if stop {
			break
		}
	}

	db.DeleteLink(block.BlockID)
	return false
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
