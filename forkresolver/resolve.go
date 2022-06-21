package forkresolver

import (
	"fmt"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/merger"
)

func parseFilenames(in []string, upTo uint64) map[string]*merger.OneBlockFile {
	out := make(map[string]*merger.OneBlockFile)
	for _, f := range in {
		obf, err := merger.NewOneBlockFile(f)
		if err != nil {
			continue
		}
		if obf.Num > upTo {
			continue
		}
		out[obf.ID] = obf
	}
	return out
}

type resolver struct {
	// FIXME add context ...
	mergedBlockFilesGetter func(base uint64) (map[string]*merger.OneBlockFile, error)
	oneBlockFilesGetter    func(upTo uint64) map[string]*merger.OneBlockFile

	// func (s *DStoreIO) FetchMergedOneBlockFiles(lowBlockNum uint64) ([]*merger.OneBlockFile, error) {
	// function to get the list... already have that somewhere in merger
}

func (r *resolver) download(file *merger.OneBlockFile) bstream.BlockRef {
	return bstream.NewBlockRef(file.ID, file.Num) //FIXME need to add the block file downloader
}

func (r *resolver) loadPreviousMergedBlocks(base uint64, blocks map[string]*merger.OneBlockFile) error {
	loadedBlocks, err := r.mergedBlockFilesGetter(base)
	if err != nil {
		return err
	}
	for k, v := range loadedBlocks {
		blocks[k] = v
	}
	return nil
}

func (r *resolver) resolve(block bstream.BlockRef, lib bstream.BlockRef) (undoBlocks []bstream.BlockRef, continueAfter uint64, err error) {
	base := block.Num() / 100 * 100
	mergedBlocks, err := r.mergedBlockFilesGetter(base)
	if err != nil {
		return nil, 0, err
	}
	nextID := merger.TruncateBlockID(block.ID())
	oneBlocks := r.oneBlockFilesGetter(block.Num())

	for {
		if blk := mergedBlocks[nextID]; blk != nil {
			continueAfter = blk.Num
			break
		}

		forkedBlock := oneBlocks[nextID]

		if forkedBlock == nil && base <= lib.Num() {
			return nil, 0, fmt.Errorf("cannot resolve block %s: no oneBlockFile or merged block found with ID %s", block, nextID)
		}

		// also true when forkedBlock.Num < base && base <= lib.Num()
		if forkedBlock.Num < lib.Num() {
			return nil, 0, fmt.Errorf("cannot resolve block %s: forked chain goes beyond LIB, looking for ID %s (this should not happens)", block, nextID)
		}

		if forkedBlock == nil || forkedBlock.Num < base {
			base -= 100
			err := r.loadPreviousMergedBlocks(base, mergedBlocks)
			if err != nil {
				return nil, 0, fmt.Errorf("cannot resolve block %s (cannot load previous bundle (%d): %w)", block, base, err)
			}
			continue // retry with more mergedBlocks loaded
		}

		undoBlocks = append(undoBlocks, r.download(forkedBlock))
		nextID = forkedBlock.PreviousID

	}

	return
}
