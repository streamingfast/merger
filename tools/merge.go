package tools

import (
	"context"
	"fmt"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/merger"
	"github.com/streamingfast/merger/bundle"
	"go.uber.org/zap"
)

// Merge is a function to produce bundles from one-block-files with less constraints than the full merger stack.
// It is meant to be used for producing chunks of blocks in parallel or getting out of bad situations...
// It will not perform continuity checks accross bundles, it allows user-driven confirmation and skipping file deletion, when testing
func Merge(zlog *zap.Logger, tracer logging.Tracer, oneBlocksStore, mergedBlocksStore dstore.Store, start, stop, bundleSize uint64, confirmer func(string) bool, withDelete bool) error {
	if start/100*100 != start {
		return fmt.Errorf("start is NOT set on a boundary")
	}

	filesDeleter := merger.NewOneBlockFilesDeleter(zlog, oneBlocksStore)
	if withDelete {
		filesDeleter.Start(2, 200)
	}

	dstoreIO := merger.NewDStoreIO(
		zlog,
		tracer,
		oneBlocksStore,
		mergedBlocksStore,
		2,
		time.Millisecond*500,
		start,
		bundleSize,
	)

	next := start
	for {
		bundler := bundle.NewBundler(zlog, next, next, bundleSize)
		ctx := context.Background()
		var highestSeenBlockFile *bundle.OneBlockFile
		var addedFileCount int
		callback := func(o *bundle.OneBlockFile) error {
			if bundler.IsBlockTooOld(o.Num) {
				return nil
			}
			if o.Num >= bundler.ExclusiveHighestBlockLimit() {
				return dstore.StopIteration
			}
			if highestSeenBlockFile == nil {
				bundler.InitLIB(bstream.NewBlockRef(o.ID, o.Num))
			}
			highestSeenBlockFile = o
			bundler.AddOneBlockFile(o)
			addedFileCount++
			return nil
		}

		if err := dstoreIO.WalkOneBlockFiles(ctx, callback); err != nil {
			zlog.Error("walking oneblockfiles", zap.Error(err))
			return err
		}
		if highestSeenBlockFile == nil {
			return fmt.Errorf("no block file seen in requested range")
		}

		longestChain := bundler.LongestOneBlockFileChain()
		if len(longestChain) == 0 {
			return fmt.Errorf("no linkable block file seen in requested range")
		}

		var warnings []string
		if len(longestChain) != 100 {
			warnings = append(warnings, fmt.Sprintf("longest chain does not contain 100 blocks, only %d", len(longestChain)))
		}
		if highestSeenBlockFile.Num != longestChain[len(longestChain)-1].Num {
			warnings = append(warnings, fmt.Sprintf("found one block files *above* longest chain: %s > %s", highestSeenBlockFile, longestChain[len(longestChain)-1]))
		}

		bundle := bundler.ToBundle(next + bundleSize - 1)
		if len(bundle) == 0 {
			return fmt.Errorf("cannot merge 'empty' bundle")
		}
		zlog.Info("current bundle",
			zap.Stringer("longest_chain_lowest_block", longestChain[0]),
			zap.Stringer("longest_chain_highest_block", longestChain[len(longestChain)-1]),
			zap.Int("length_bundle", len(bundle)),
			zap.Object("bundler", bundler),
			zap.Strings("warnings", warnings),
		)

		if confirmer != nil && !confirmer(fmt.Sprintf("Create merged bundle from [%s] to [%s] (%d blocks) into [%d] with %d warnings ?", longestChain[0], longestChain[len(longestChain)-1], len(longestChain), bundler.BundleInclusiveLowerBlock(), len(warnings))) {
			return fmt.Errorf("cancelled")
		}

		if err := dstoreIO.MergeAndStore(bundler.BundleInclusiveLowerBlock(), bundle); err != nil {
			zlog.Error("uploading", zap.Error(err))
			return err
		}
		zlog.Info("merge bundle uploaded: %d", zap.Uint64("lower_block", bundler.BundleInclusiveLowerBlock()))
		if withDelete {
			if confirmer != nil && !confirmer(fmt.Sprintf("Delete one-block-files [%s] to [%s] (%d blocks) ?", longestChain[0], longestChain[len(longestChain)-1], len(longestChain))) {
				zlog.Info("skipping file deletion...")
			} else {
				filesDeleter.Delete(bundle)
			}
		}

		next += bundleSize
		if next >= stop {
			zlog.Info("reached stop block")
			time.Sleep(time.Second) // for filesDeleter to finish...
			return nil
		}
	}

}
