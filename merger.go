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
	"encoding/gob"
	"fmt"
	"os"
	"time"

	"github.com/streamingfast/merger/metrics"

	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type Merger struct {
	*shutter.Shutter
	chunkSize               uint64
	grpcListenAddr          string
	timeBetweenStoreLookups time.Duration // should be very low on local filesystem
	oneBlockDeletionThreads int

	bundler             *Bundler
	fetchMergedFileFunc func(lowBlockNum uint64) ([]*OneBlockFile, error)
	fetchOneBlockFiles  func(ctx context.Context) (oneBlockFiles []*OneBlockFile, err error)
	deleteFilesFunc     func(oneBlockFiles []*OneBlockFile)
	mergeUploadFunc     func(inclusiveLowerBlock uint64, oneBlockFiles []*OneBlockFile) (err error)
	stateFile           string
}

func NewMerger(
	bundler *Bundler,
	timeBetweenStoreLookups time.Duration,
	grpcListenAddr string,
	fetchMergedFileFunc func(lowBlockNum uint64) ([]*OneBlockFile, error),
	fetchOneBlockFiles func(ctx context.Context) (oneBlockFiles []*OneBlockFile, err error),
	deleteFilesFunc func(oneBlockFiles []*OneBlockFile),
	mergeUploadFunc func(inclusiveLowerBlock uint64, oneBlockFiles []*OneBlockFile) (err error),
	stateFile string,
) *Merger {
	return &Merger{
		Shutter:                 shutter.New(),
		bundler:                 bundler,
		grpcListenAddr:          grpcListenAddr,
		timeBetweenStoreLookups: timeBetweenStoreLookups,
		fetchMergedFileFunc:     fetchMergedFileFunc,
		fetchOneBlockFiles:      fetchOneBlockFiles,
		deleteFilesFunc:         deleteFilesFunc,
		mergeUploadFunc:         mergeUploadFunc,
		stateFile:               stateFile,
	}
}

func (m *Merger) Launch() {
	zlog.Info("starting merger", zap.Stringer("bundler", m.bundler))

	m.startServer()

	err := m.launch()
	zlog.Info("merger exited", zap.Error(err))
	m.Shutdown(err)
}

func (m *Merger) launch() (err error) {
	for {
		if m.IsTerminating() {
			return nil
		}

		zlog.Debug("verifying if bundle file already exist in store")
		if oneBlockFiles, err := m.fetchMergedFileFunc(m.bundler.BundleInclusiveLowerBlock()); err == nil {
			m.bundler.AddPreMergedOneBlockFiles(oneBlockFiles)
		}

		isBundleComplete, highestBundleBlockNum := m.bundler.IsComplete()
		if !isBundleComplete {
			ctx, cancel := context.WithTimeout(context.Background(), ListFilesTimeout)
			tooOldFiles, lastOneBlockFileAdded, err := m.retrieveOneBlockFile(ctx)
			cancel()
			if err != nil {
				return fmt.Errorf("retreiving one block files: %w", err)
			}

			if len(tooOldFiles) > 0 {
				m.deleteFilesFunc(tooOldFiles)
			}

			if lastOneBlockFileAdded == nil {
				select {
				case <-time.After(m.timeBetweenStoreLookups):
					continue
				case <-m.Terminating():
					return m.Err()
				}
			}

			if m.bundler.lastMergeOneBlockFile != nil && lastOneBlockFileAdded.num < m.bundler.lastMergeOneBlockFile.num { // will still drift if there is a hole and lastFile is advancing
				metrics.HeadBlockTimeDrift.SetBlockTime(lastOneBlockFileAdded.blockTime)
			}

			continue //until not completed
		}

		zlog.Info("merging bundle",
			zap.Uint64("highest_bundle_block_num", highestBundleBlockNum),
			zap.Stringer("bundler", m.bundler),
		)

		bundleFiles := m.bundler.ToBundle(highestBundleBlockNum)

		if err := m.mergeUploadFunc(m.bundler.BundleInclusiveLowerBlock(), bundleFiles); err != nil {
			return err
		}

		m.bundler.Commit(highestBundleBlockNum)

		state := &State{ExclusiveHighestBlockLimit: m.bundler.exclusiveHighestBlockLimit}
		zlog.Info("saving state", zap.Stringer("state", state))
		err = SaveState(state, m.stateFile)
		if err != nil {
			zlog.Warn("failed to save state", zap.Error(err))
		}

		m.bundler.Purge(func(purgedOneBlockFiles []*OneBlockFile) {
			if len(purgedOneBlockFiles) > 0 {
				m.deleteFilesFunc(purgedOneBlockFiles)
			}
		})

		//todo: update metrics and progressFilename
		//metrics.HeadBlockTimeDrift.SetBlockTime(b.upperBlockTime)
		//metrics.HeadBlockNumber.SetUint64(b.lowersBlock + m.chunkSize)
		//
		//if m.progressFilename != "" {
		//	err := ioutil.WriteFile(m.progressFilename, []byte(fmt.Sprintf("%d", b.lowerBlock+m.chunkSize)), 0644)
		//	if err != nil {
		//		zlog.Warn("cannot write progress to file", zap.String("filename", m.progressFilename), zap.Error(err))
		//	}
		//}
	}

}

func (m *Merger) retrieveOneBlockFile(ctx context.Context) (tooOld []*OneBlockFile, lastOneBlockFileAdded *OneBlockFile, err error) {
	addedFileCount := 0
	oneBlockFiles, err := m.fetchOneBlockFiles(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("fetching one block files: %w", err)
	}
	for _, oneBlockFile := range oneBlockFiles {

		switch {
		case m.bundler.IsBlockTooOld(oneBlockFile.num):
			tooOld = append(tooOld, oneBlockFile)
		default:
			m.bundler.AddOneBlockFile(oneBlockFile)
			lastOneBlockFileAdded = oneBlockFile
			addedFileCount++
		}
	}

	zlog.Info("retrieved list of files",
		zap.Int("too_old_files_count", len(tooOld)),
		zap.Int("added_files_count", addedFileCount),
	)

	return
}

type State struct {
	ExclusiveHighestBlockLimit uint64
}

func (s *State) String() string {
	return fmt.Sprintf("exclusive_highest_block_limit: %d", s.ExclusiveHighestBlockLimit)
}

func LoadState(filename string) (state *State, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	dataDecoder := gob.NewDecoder(f)
	err = dataDecoder.Decode(&state)
	return
}

func SaveState(state *State, filename string) error {
	if filename == "" { // in memory mode
		return nil
	}
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	dataEncoder := gob.NewEncoder(f)
	return dataEncoder.Encode(state)
}
