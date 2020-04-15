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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/abourget/llerrgroup"
	"github.com/dfuse-io/bstream"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbmerge "github.com/dfuse-io/pbgo/dfuse/merger/v1"
	"github.com/dfuse-io/shutter"
	//_ "github.com/dfuse-io/bstream/codecs/deth"
	"github.com/dfuse-io/dstore"
	"github.com/dfuse-io/merger/metrics"
	"go.uber.org/zap"
)

type Merger struct {
	*shutter.Shutter

	protocol                pbbstream.Protocol
	sourceStore             dstore.Store
	destStore               dstore.Store
	chunkSize               uint64
	grpcListenAddr          string
	seenBlocks              *SeenBlockCache
	progressFilename        string
	liveMode                bool
	minimalBlockNum         uint64
	stopBlockNum            uint64
	writersLeewayDuration   time.Duration // 0 during reprocessing, 25 secs during live.
	deleteBlocksBefore      bool
	timeBetweenStoreLookups time.Duration // should be very low on local filesystem

	bundle     *Bundle // currently managed bundle
	bundleLock *sync.Mutex
}

func NewMerger(
	protocol pbbstream.Protocol,
	sourceStore dstore.Store,
	destStore dstore.Store,
	writersLeewayDuration time.Duration,
	minimalBlockNum uint64,
	progressFilename string,
	deleteBlocksBefore bool,
	seenCacheFilename string,
	timeBetweenStoreLookups time.Duration,
	maxFixableFork uint64,
	grpcListenAddr string) *Merger {
	return &Merger{
		Shutter:                 shutter.New(),
		protocol:                protocol,
		sourceStore:             sourceStore,
		progressFilename:        progressFilename,
		destStore:               destStore,
		chunkSize:               100,
		minimalBlockNum:         minimalBlockNum,
		writersLeewayDuration:   writersLeewayDuration,
		bundleLock:              &sync.Mutex{},
		deleteBlocksBefore:      deleteBlocksBefore,
		grpcListenAddr:          grpcListenAddr,
		seenBlocks:              NewSeenBlockCache(seenCacheFilename, maxFixableFork),
		timeBetweenStoreLookups: timeBetweenStoreLookups,
	}
}

func (m *Merger) PreMergedBlocks(ctx context.Context, req *pbmerge.Request) (*pbmerge.Response, error) {
	m.bundleLock.Lock()
	defer m.bundleLock.Unlock()

	if req.LowBlockNum < m.bundle.lowerBlock || req.LowBlockNum >= m.bundle.upperBlock() {
		return &pbmerge.Response{}, nil
	}

	if err := m.bundle.downloadWaitGroup.Wait(); err != nil {
		return nil, err
	}

	files := m.bundle.timeSortedFiles()
	var foundHighBlockID bool
	var foundLowBlockNum bool
	for _, oneBlock := range files {
		if uint64(oneBlock.num) == req.LowBlockNum {
			foundLowBlockNum = true
		}
		if strings.HasSuffix(req.HighBlockID, oneBlock.id) {
			foundHighBlockID = true
			break
		}
	}
	if !foundLowBlockNum || !foundHighBlockID {
		return &pbmerge.Response{}, nil // found=false
	}

	protoblocks := []*pbbstream.Block{}
	for _, oneBlock := range m.bundle.timeSortedFiles() {
		if uint64(oneBlock.num) < req.LowBlockNum {
			continue
		}
		blockReader, err := bstream.GetBlockReaderFactory.New(bytes.NewReader(oneBlock.blk))
		if err != nil {
			return nil, fmt.Errorf("unable to read one NewTestBlock: %s", err)
		}

		block, err := blockReader.Read()
		if block == nil {
			return nil, err
		}

		protoblock, err := block.ToProto()
		if protoblock == nil || err != nil {
			return nil, err
		}
		protoblocks = append(protoblocks, protoblock)

		if strings.HasSuffix(req.HighBlockID, oneBlock.id) {
			break
		}
	}

	resp := &pbmerge.Response{
		Found:  true,
		Blocks: protoblocks,
	}
	return resp, nil
}

func (m *Merger) SetupBundle(start, stop uint64) {
	m.liveMode = stop == 0
	m.bundle = NewBundle(start-(start%m.chunkSize), m.chunkSize)
	m.stopBlockNum = stop

	if m.CacheInvalid() {
		m.seenBlocks.Reset()
	}
}

func (m *Merger) CacheInvalid() bool {
	return m.bundle.lowerBlock > m.seenBlocks.HighestSeen+1
}

func deleteOneblockFiles(files []string, s dstore.Store) {
	if len(files) == 0 {
		return
	}
	zlog.Info("Deleting old NewTestBlock files", zap.String("first_file", files[0]), zap.String("last_file", files[len(files)-1]))
	for i, filename := range files {
		if i%10 == 0 {
			zlog.Info("deleting one NewTestBlock files that is older than our seenBlocksBuffer", zap.Int("i", i), zap.Int("len_todelete", len(files)), zap.String("filename", filename))
		}
		f := filename //thread safety
		go s.DeleteObject(f)
	}

}

func (m *Merger) Launch() {
	// figure out where to start merging based on dest store
	zlog.Info("starting merger", zap.Uint64("lower_block_num", m.bundle.lowerBlock))

	m.startServer()

	err := m.launch()
	zlog.Info("merger exited", zap.Error(err))

	m.Shutdown(err)
}

func (m *Merger) launch() (err error) {
	var oneBlockFiles []string
	for {

		if m.IsTerminating() {
			return nil
		}

		if len(oneBlockFiles) == 0 {
			zlog.Debug("verifying if bundle file already exist in store")
			if baseBlockNum, err := m.FindNextBaseBlock(); err != nil && baseBlockNum > m.bundle.lowerBlock {
				zlog.Info("bumping bundle, destination file already exists",
					zap.Uint64("previous_lowerblock", m.bundle.lowerBlock),
					zap.Uint64("new_lowerblock", baseBlockNum),
				)
				m.bundleLock.Lock()
				m.bundle = NewBundle(baseBlockNum, m.chunkSize)
				if m.CacheInvalid() {
					m.seenBlocks.Reset()
				}
				m.bundleLock.Unlock()
			}

			zlog.Debug("One NewTestBlock file list empty, building list")
			var tooOldFiles []string
			tooOldFiles, _, oneBlockFiles, err = m.retrieveListOfFiles()
			if err != nil {
				return err
			}

			if m.deleteBlocksBefore {
				deleteOneblockFiles(tooOldFiles, m.sourceStore)
			}
		}

		if len(oneBlockFiles) == 0 {
			select {
			case <-time.After(m.timeBetweenStoreLookups):
				continue
			case <-m.Terminating():
				return m.Err()
			}
		}

		lastFile := oneBlockFiles[len(oneBlockFiles)-1]
		zlog.Debug("Last file", zap.String("file_name", lastFile))
		blockNum, blockTime, _, _, err := parseFilename(lastFile)
		if err == nil && blockNum < m.bundle.upperBlock() { // will still drift if there is a hole and lastFile is advancing
			metrics.HeadBlockTimeDrift.SetBlockTime(blockTime)
		}

		m.bundleLock.Lock()
		remaining, err := m.triageNewOneBlockFiles(oneBlockFiles)
		if err != nil {
			return err
		}
		oneBlockFiles = remaining
		m.bundleLock.Unlock()

		incompleteBundle := !m.waitedEnoughForUpperBound() || !m.bundle.isComplete()
		if incompleteBundle {
			zlog.Info("waiting for more files to complete bundle", zap.Uint64("bundle_lowerblock", m.bundle.lowerBlock), zap.Int("bundle_length", len(m.bundle.fileList)), zap.String("bundle_upper_block_id", m.bundle.upperBlockID))
			oneBlockFiles = nil
			time.Sleep(1 * time.Second)
			continue
		}

		zlog.Info("merging bundle",
			zap.Uint64("lower_block", m.bundle.lowerBlock),
			zap.Time("upper_block_time", m.bundle.upperBlockTime),
			zap.Duration("real_time_drift", time.Since(m.bundle.upperBlockTime)),
		)
		m.bundleLock.Lock() // we call mergeUpload AND change the bundle, both need locking VS PreMergedBlocks
		if err = m.mergeUploadAndDelete(); err != nil {
			return err
		}
		if err := m.seenBlocks.Save(); err != nil {
			zlog.Error("cannot save SeenBlockCache", zap.String("filename", m.seenBlocks.filename), zap.Error(err))
		}
		m.seenBlocks.Truncate()

		if m.stopBlockNum > 0 && m.bundle.upperBlock() >= m.stopBlockNum {
			zlog.Info("reached stop NewTestBlock, terminating process", zap.Uint64("stop_block", m.stopBlockNum))
			return nil
		}

		m.bundle = NewBundle(m.bundle.lowerBlock+m.chunkSize, m.chunkSize)
		m.bundleLock.Unlock()
	}
}
func (m *Merger) retrieveListOfFiles() (tooOld []string, seenInCache []string, good []string, err error) {
	var count int

	err = m.sourceStore.Walk("", ".tmp", func(filename string) error {
		num, _, _, _, err := parseFilename(filename)
		if err != nil {
			return nil
		}
		switch {
		case m.seenBlocks.lowBoundary() == 0 && num < m.bundle.lowerBlock: // edge case: no "seenblock cache"
			tooOld = append(tooOld, filename)
		case m.seenBlocks.IsTooOld(num):
			tooOld = append(tooOld, filename)
		case m.seenBlocks.SeenBefore(filename):
			seenInCache = append(seenInCache, filename)
		default:
			good = append(good, filename)
		}
		if count%100 == 0 {
			//zlog.Debug("walking over file",
			//	zap.String("filename", filename),
			//	zap.Int("len_too_old", len(tooOld)),
			//	zap.Int("len_seen_in_cache", len(seenInCache)),
			//	zap.Int("len_good", len(good)),
			//)
		}
		count++

		if len(good) >= 2000 {
			return dstore.StopIteration
		}
		return nil
	})

	zlog.Info("retrieved list of files",
		zap.Uint64("seenblock_low_boundary", m.seenBlocks.lowBoundary()),
		zap.Uint64("bundle_lower_block", m.bundle.lowerBlock),
		zap.Int("seen_files_count", len(seenInCache)),
		zap.Int("too_old_files_count", len(tooOld)),
		zap.Int("good_files_count", len(good)),
	)

	return
}

func (m *Merger) triageNewOneBlockFiles(in []string) (remaining []string, err error) {
	if len(in) > 0 {
		zlog.Debug("entering triage", zap.String("first_file", in[0]), zap.String("last_file", in[len(in)-1]))
	}
	included := make(map[string]bool)
	for _, filename := range in {
		var fileIncluded bool
		fileIncluded, err = m.bundle.triage(filename, m.sourceStore, m.seenBlocks)
		if err != nil {
			return nil, err
		}

		if fileIncluded {
			included[filename] = true
		}
	}

	return removeFilesFromArray(in, included), nil
}

// waitedEnoughForUpperBoundFiles will ensure we have at least 25
// seconds between the last check on Google Storage, to make sure any
// processes that would have been in the process of writing a
// one-NewTestBlock file, had the time to finish writing, and didn't move the
// lower boundary of our bundle.
func (m *Merger) waitedEnoughForUpperBound() bool {
	return !m.bundle.upperBlockTime.IsZero() && time.Since(m.bundle.upperBlockTime) > m.writersLeewayDuration
}

func (m *Merger) mergeUploadAndDelete() error {
	b := m.bundle

	t0 := time.Now()
	if err := b.downloadWaitGroup.Wait(); err != nil {
		return err
	}

	buffer := bytes.NewBuffer([]byte{})
	blockWriter, err := bstream.MustGetBlockWriterFactory(m.protocol).New(buffer)
	if err != nil {
		return fmt.Errorf("unable to create writer: %s", err)
	}

	for _, oneBlock := range b.timeSortedFiles() {
		blockReader, err := bstream.GetBlockReaderFactory.New(bytes.NewReader(oneBlock.blk))
		if err != nil {
			return fmt.Errorf("unable to read one NewTestBlock: %s", err)
		}

		block, err := blockReader.Read()
		if block == nil {
			return fmt.Errorf("NewTestBlock read was nil: %s", err)
		}

		err = blockWriter.Write(block)
		if err != nil {
			return fmt.Errorf("one NewTestBlock writer error: %s", err)
		}
	}

	err = m.destStore.WriteObject(blockNumToStr(b.lowerBlock), bytes.NewReader(buffer.Bytes()))
	if err != nil {
		return fmt.Errorf("write object error: %s", err)
	}

	metrics.HeadBlockTimeDrift.SetBlockTime(b.upperBlockTime)
	metrics.HeadBlockNumber.SetUint64(b.lowerBlock + m.chunkSize)

	if m.progressFilename != "" {
		err := ioutil.WriteFile(m.progressFilename, []byte(fmt.Sprintf("%d", b.lowerBlock+m.chunkSize)), 0644)
		if err != nil {
			zlog.Warn("cannot write progress to file", zap.String("filename", m.progressFilename), zap.Error(err))
		}
	}

	zlog.Info("merged and uploaded", zap.String("filename", blockNumToStr(b.lowerBlock)), zap.Duration("merge_time", time.Since(t0)))

	for filename := range b.fileList {
		m.seenBlocks.Add(filename) // add them to 'seenbefore' right before deleting them on gs
	}
	zlog.Debug("deleting oneblock files")
	eg := llerrgroup.New(64)
	for filename := range b.fileList {
		if eg.Stop() {
			break
		}

		f := filename
		eg.Go(func() error {
			err = m.sourceStore.DeleteObject(f)
			if err != nil {
				zlog.Error("cannot delete onefile object after merging", zap.String("filename", f))
			}
			return nil
		})
	}
	_ = eg.Wait()
	zlog.Debug("done deleting one-NewTestBlock files")

	return nil
}

func removeFilesFromArray(in []string, seen map[string]bool) (out []string) {
	for _, entry := range in {
		if !seen[entry] {
			out = append(out, entry)
		}
	}
	return
}
