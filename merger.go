// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merger

import (
	"context"
	"fmt"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/merger/bundle"
	"github.com/streamingfast/merger/metrics"

	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type Merger struct {
	*shutter.Shutter
	chunkSize                      uint64
	grpcListenAddr                 string
	timeBetweenStoreLookups        time.Duration // should be very low on local filesystem
	maxOneBlockOperationsBatchSize int

	bundler           *bundle.Bundler
	io                IOInterface
	deleteFilesFunc   func(oneBlockFiles []*bundle.OneBlockFile)
	deleteFilesMinAge time.Duration

	logger *zap.Logger
}

func NewMerger(
	logger *zap.Logger,
	bundler *bundle.Bundler,
	timeBetweenStoreLookups time.Duration,
	maxOneBlockOperationsBatchSize int,
	grpcListenAddr string,
	io IOInterface,
	deleteFilesMinAge time.Duration,
	deleteFilesFunc func(oneBlockFiles []*bundle.OneBlockFile),
) *Merger {
	return &Merger{
		Shutter:                        shutter.New(),
		bundler:                        bundler,
		grpcListenAddr:                 grpcListenAddr,
		timeBetweenStoreLookups:        timeBetweenStoreLookups,
		maxOneBlockOperationsBatchSize: maxOneBlockOperationsBatchSize,
		io:                             io,
		deleteFilesFunc:                deleteFilesFunc,
		deleteFilesMinAge:              deleteFilesMinAge,
		logger:                         logger,
	}
}

func (m *Merger) Launch() {
	m.logger.Info("starting merger", zap.Object("bundle", m.bundler))

	m.startServer()

	err := m.launch()
	m.logger.Info("merger exited", zap.Error(err))
	m.Shutdown(err)
}

func (m *Merger) launch() (err error) {
	for {
		if m.IsTerminating() {
			return nil
		}

		m.logger.Info("verifying if bundle file already exist in store", zap.Uint64("bundle_inclusive_lower_block", m.bundler.BundleInclusiveLowerBlock()))
		if oneBlockFiles, err := m.io.FetchMergedOneBlockFiles(m.bundler.BundleInclusiveLowerBlock()); err == nil {
			if len(oneBlockFiles) > 0 {
				m.logger.Info("adding files from already merge bundle", zap.Uint64("bundle_inclusive_lower_block", m.bundler.BundleInclusiveLowerBlock()), zap.Int("file_count", len(oneBlockFiles)))
				m.bundler.BundlePreMergedOneBlockFiles(oneBlockFiles)         // this will change state and skip to next bundle ...
				m.bundler.Purge(time.Time{}, func([]*bundle.OneBlockFile) {}) // no file deletion, just move LIB on forkdb

				//let check if next bundle has also been merged by another process
				//no bundle can be completed at this time
				continue
			}
		}

		isBundleComplete, highestBundleBlockNum, err := m.bundler.BundleCompleted() // multiple bundle can be completed. No need to fetch more on block file
		if err != nil {
			return err
		}
		if !isBundleComplete {
			ctx, cancel := context.WithTimeout(context.Background(), ListFilesTimeout)
			deletableOldFiles, highestSeenOneBlockFile, err := m.retrieveOneBlockFile(ctx)
			cancel()
			if err != nil {
				return fmt.Errorf("retrieving one block files: %w", err)
			}
			if len(deletableOldFiles) > 0 {
				m.deleteFilesFunc(deletableOldFiles)
			}

			isBundleComplete, highestBundleBlockNum, err = m.bundler.BundleCompleted()
			if err != nil {
				return err
			}
			if !isBundleComplete {
				m.logger.Info("bundle not completed after retrieving one block file", zap.Object("bundle", m.bundler))
				if err := m.bundler.CheckContinuity(highestSeenOneBlockFile); err != nil {
					return err
				}
				select {
				case <-time.After(m.timeBetweenStoreLookups):
					continue
				case <-m.Terminating():
					return m.Err()
				}
			}
		}

		bundleFiles := m.bundler.ToBundle(highestBundleBlockNum)
		m.logger.Info("merging bundle",
			zap.Uint64("highest_bundle_block_num", highestBundleBlockNum),
			zap.Object("bundle", m.bundler),
			zap.Int("count", len(bundleFiles)),
		)

		if err = m.io.MergeAndStore(m.bundler.BundleInclusiveLowerBlock(), bundleFiles); err != nil {
			return err
		}

		m.logger.Info("bundle files uploaded")

		m.bundler.Commit(highestBundleBlockNum)

		lastMergedOneBlockFile := m.bundler.LastMergeOneBlockFile()
		if lastMergedOneBlockFile == nil {
			// sanity check
			return fmt.Errorf("unable to process, expected a least merger one block file")
		}

		m.logger.Info("bundle merged and committed", zap.Object("bundle", m.bundler))

		metrics.HeadBlockTimeDrift.SetBlockTime(lastMergedOneBlockFile.BlockTime)
		metrics.HeadBlockNumber.SetUint64(lastMergedOneBlockFile.Num)

		m.bundler.Purge(time.Now().Add(-m.deleteFilesMinAge), func(oneBlockFilesToDelete []*bundle.OneBlockFile) {
			if len(oneBlockFilesToDelete) > 0 {
				m.deleteFilesFunc(oneBlockFilesToDelete)
			}
		})
	}
}

func (m *Merger) retrieveOneBlockFile(ctx context.Context) (deletable []*bundle.OneBlockFile, highestSeenBlockFile *bundle.OneBlockFile, err error) {
	addedFileCount := 0
	seenFileCount := 0
	callback := func(o *bundle.OneBlockFile) error {
		highestSeenBlockFile = o
		if m.bundler.IsBlockTooOld(o.Num) {
			deletable = append(deletable, o)
			return nil
		}
		exists := m.bundler.AddOneBlockFile(o)
		if exists {
			seenFileCount += 1
			return nil
		}
		addedFileCount++
		if addedFileCount >= m.maxOneBlockOperationsBatchSize {
			return dstore.StopIteration
		}
		return nil
	}

	err = m.io.WalkOneBlockFiles(ctx, callback)
	if err == dstore.StopIteration {
		err = nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("fetching one block files: %w", err)
	}

	highestNum := m.bundler.BundleInclusiveLowerBlock()
	highest := m.bundler.LongestChainLastBlockFile()
	if highest != nil && highest.Num >= m.bundler.BundleInclusiveLowerBlock() && highest.Num < m.bundler.ExclusiveHighestBlockLimit() {
		highestNum = highest.Num
		metrics.HeadBlockTimeDrift.SetBlockTime(highest.BlockTime)
		metrics.HeadBlockNumber.SetUint64(highestNum)
	}

	zapFields := []zap.Field{
		zap.Int("seen_files_count", seenFileCount),
		zap.Int("too_old_files_count", len(deletable)),
		zap.Int("added_files_count", addedFileCount),
		zap.Uint64("highest_linkable_block_file", highestNum),
	}

	if highestSeenBlockFile != nil {
		zapFields = append(zapFields, zap.Uint64("highest_seen_block_file", highestSeenBlockFile.Num))
	}

	m.logger.Info("retrieved list of files", zapFields...)

	return
}
