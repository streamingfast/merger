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
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dstore"
	"github.com/streamingfast/merger/metrics"
	pbmerge "github.com/dfuse-io/pbgo/dfuse/merger/v1"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

type Merger struct {
	*shutter.Shutter
	oneBlocksStore                 dstore.Store
	destStore                      dstore.Store
	chunkSize                      uint64
	grpcListenAddr                 string
	seenBlocks                     *SeenBlockCache
	progressFilename               string
	stopBlockNum                   uint64
	writersLeewayDuration          time.Duration // 0 during reprocessing, 25 secs during live.
	timeBetweenStoreLookups        time.Duration // should be very low on local filesystem
	oneBlockDeletionThreads        int
	maxOneBlockOperationsBatchSize int
	isBatchMode                    bool

	bundle     *Bundle // currently managed bundle
	bundleLock *sync.Mutex
}

func NewMerger(
	sourceStore dstore.Store,
	destStore dstore.Store,
	writersLeewayDuration time.Duration,
	chunkSize uint64,
	seenBlocks *SeenBlockCache,
	startBlockNum uint64,
	stopBlockNum uint64,
	timeBetweenStoreLookups time.Duration,
	grpcListenAddr string,
	oneBlockDeletionThreads int,
	maxOneBlockOperationsBatchSize int,
	isBatchMode bool,
) *Merger {
	return &Merger{
		Shutter:                        shutter.New(),
		oneBlocksStore:                 sourceStore,
		destStore:                      destStore,
		chunkSize:                      chunkSize,
		bundle:                         NewBundle(startBlockNum-(startBlockNum%chunkSize), chunkSize),
		stopBlockNum:                   stopBlockNum,
		seenBlocks:                     seenBlocks,
		writersLeewayDuration:          writersLeewayDuration,
		bundleLock:                     &sync.Mutex{},
		grpcListenAddr:                 grpcListenAddr,
		timeBetweenStoreLookups:        timeBetweenStoreLookups,
		oneBlockDeletionThreads:        oneBlockDeletionThreads,
		maxOneBlockOperationsBatchSize: maxOneBlockOperationsBatchSize,
		isBatchMode:                    isBatchMode,
	}
}
func (m *Merger) PreMergedBlocks(req *pbmerge.Request, server pbmerge.Merger_PreMergedBlocksServer) error {
	m.bundleLock.Lock()
	defer m.bundleLock.Unlock()

	if req.LowBlockNum < m.bundle.lowerBlock || req.LowBlockNum >= m.bundle.upperBlock() {
		err := fmt.Errorf("cannot find requested blocks")
		server.SetHeader(metadata.New(map[string]string{"error": err.Error()}))
		return err
	}

	files := m.bundle.timeSortedFiles()
	var foundHighBlockID bool
	var foundLowBlockNum bool
	for _, oneBlock := range files {
		if oneBlock.num == req.LowBlockNum {
			foundLowBlockNum = true
		}
		if strings.HasSuffix(req.HighBlockID, oneBlock.id) {
			foundHighBlockID = true
			break
		}
	}
	if !foundLowBlockNum {
		err := fmt.Errorf("cannot find requested lowBlockNum")
		server.SetHeader(metadata.New(map[string]string{"error": err.Error()}))
		return err
	}
	if !foundHighBlockID {
		err := fmt.Errorf("cannot find requested highBlockID")
		server.SetHeader(metadata.New(map[string]string{"error": err.Error()}))
		return err
	}

	for _, oneBlock := range m.bundle.timeSortedFiles() {
		if oneBlock.num < req.LowBlockNum {
			continue
		}
		data, err := oneBlock.Data(server.Context(), m.oneBlocksStore)
		if err != nil {
			return fmt.Errorf("unable to get one block data: %w", err)
		}

		blockReader, err := bstream.GetBlockReaderFactory.New(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("unable to read one block: %w", err)
		}

		block, err := blockReader.Read()
		if block == nil {
			return err
		}

		protoblock, err := block.ToProto()
		if protoblock == nil || err != nil {
			return err
		}

		err = server.Send(
			&pbmerge.Response{
				Found: true, //todo: this is not require any more
				Block: protoblock,
			})

		if err != nil {
			return fmt.Errorf("unable send response to client: %w", err)
		}

		if strings.HasSuffix(req.HighBlockID, oneBlock.id) {
			break
		}
	}

	return nil
}

func newOneBlockFilesDeleter(store dstore.Store) *oneBlockFilesDeleter {
	return &oneBlockFilesDeleter{
		store: store,
	}
}

type oneBlockFilesDeleter struct {
	sync.Mutex
	toProcess chan string
	store     dstore.Store
}

func (od *oneBlockFilesDeleter) Start(threads int, maxDeletions int) {
	od.toProcess = make(chan string, maxDeletions)
	for i := 0; i < threads; i++ {
		go od.processDeletions()
	}
}

func (od *oneBlockFilesDeleter) Delete(files []string) {
	od.Lock()
	defer od.Unlock()
	if len(files) == 0 {
		return
	}
	zlog.Info("deleting files that are too old or already seen", zap.Int("number_of_files", len(files)), zap.String("first_file", files[0]), zap.String("last_file", files[len(files)-1]))

	deletable := make(map[string]struct{})

	// dedupe processing queue
	for empty := false; !empty; {
		select {
		case f := <-od.toProcess:
			deletable[f] = Empty
		default:
			empty = true
		}
	}

	for _, file := range files {
		if len(od.toProcess) == cap(od.toProcess) {
			break
		}
		if _, exists := deletable[file]; !exists {
			od.toProcess <- file
		}
		deletable[file] = Empty
	}
}

func (od *oneBlockFilesDeleter) processDeletions() {
	for {
		file := <-od.toProcess

		var err error
		for i := 0; i < 3; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), DeleteObjectTimeout)
			err = od.store.DeleteObject(ctx, file)
			cancel()
			if err == nil {
				break
			}
			time.Sleep(time.Duration(100*i) * time.Millisecond)
		}
		if err != nil {
			zlog.Warn("cannot delete oneblock file after a few retries", zap.String("file", file), zap.Error(err))
		}
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

func fetchMergedFile(store dstore.Store, lowBlockNum uint64) (io.ReadCloser, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), GetObjectTimeout)

	out, err := store.OpenObject(ctx, fileNameForBlocksBundle(lowBlockNum))
	if err != nil {
		cancel()
		return out, nil, err
	}
	return out, cancel, nil
}

func (m *Merger) processRemoteMergedFile(file io.ReadCloser) (err error) {
	defer file.Close()

	prevLower := m.bundle.lowerBlock
	newLower := prevLower + m.chunkSize
	zlog.Info("bumping bundle, destination file already exists",
		zap.Uint64("previous_lowerblock", prevLower),
		zap.Uint64("new_lowerblock", newLower),
	)

	blkReader, err := bstream.GetBlockReaderFactory.New(file)
	if err != nil {
		return err
	}

	seenBlocks := []string{}
	targetEndBlock := prevLower + m.chunkSize - 1
	var seenTargetEndBlock bool
	for {
		block, err := blkReader.Read()
		if block == nil {
			if err == io.EOF {
				break
			}
			return err
		}
		seenBlocks = append(seenBlocks, blockFileName(block))
		if block.Num() == targetEndBlock {
			seenTargetEndBlock = true
		}
	}
	if !seenTargetEndBlock {
		return fmt.Errorf("remote merged block file for blocks %d (length:%d) does not contain block %d", prevLower, m.chunkSize, targetEndBlock)
	}

	m.bundleLock.Lock()
	defer m.bundleLock.Unlock()
	m.bundle = NewBundle(newLower, m.chunkSize)
	for _, seenblk := range seenBlocks {
		m.seenBlocks.Add(seenblk)
	}
	if err := m.seenBlocks.Save(); err != nil {
		zlog.Error("cannot save SeenBlockCache", zap.Error(err))
	}
	m.seenBlocks.Truncate()

	return nil
}

func (m *Merger) launch() (err error) {
	var oneBlockFiles []string
	od := newOneBlockFilesDeleter(m.oneBlocksStore)
	od.Start(m.oneBlockDeletionThreads, 100000)

	for {
		if m.IsTerminating() {
			return nil
		}

		zlog.Debug("verifying if bundle file already exist in store")
		if remoteMergedFile, cancel, err := fetchMergedFile(m.destStore, m.bundle.lowerBlock); err == nil {
			err := m.processRemoteMergedFile(remoteMergedFile)
			cancel()
			if err != nil {
				zlog.Error("error processing remote file to bump bundle", zap.Error(err), zap.Uint64("bundle_lowerblock", m.bundle.lowerBlock))
			} else {
				continue // keep bumping
			}
		}

		if len(oneBlockFiles) == 0 {
			zlog.Debug("one block file list empty, building list")
			var tooOldFiles []string
			var seenFiles []string

			ctx, cancel := context.WithTimeout(context.Background(), ListFilesTimeout)
			tooOldFiles, seenFiles, oneBlockFiles, err = m.retrieveListOfFiles(ctx)
			cancel()
			if err != nil {
				return err
			}

			od.Delete(append(tooOldFiles, seenFiles...))
			if len(oneBlockFiles) == 0 {
				select {
				case <-time.After(m.timeBetweenStoreLookups):
					continue
				case <-m.Terminating():
					return m.Err()
				}
			}
		}

		lastFile := oneBlockFiles[len(oneBlockFiles)-1]
		zlog.Debug("last file", zap.String("file_name", lastFile))
		blockNum, blockTime, _, _, _, err := parseFilename(lastFile)
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

		completeBundle := m.bundle.isComplete() && m.waitedEnoughForUpperBound()

		// special case, if we set a stop block because we are running a batch job 1000->2000, we can end last bundle after reading blocks 1900->1999 even if another batch job deleted the block 2000
		if m.stopBlockNum == m.bundle.lowerBlock+100 && len(m.bundle.fileList) >= 100 && !completeBundle {
			var foundUpperIDs []string
			for _, f := range m.bundle.timeSortedFiles() {
				if f.num == m.bundle.upperBlock()-1 {
					foundUpperIDs = append(foundUpperIDs, f.id)
				}
			}

			// recalculate after we've set arbitrary upper block ID
			for _, upperID := range foundUpperIDs {
				m.bundle.upperBlockID = upperID
				completeBundle = m.bundle.isComplete()
				if completeBundle {
					break
				}
				m.bundle.upperBlockID = ""
			}
		}

		if !completeBundle {
			zlog.Info("waiting for more files to complete bundle", zap.Uint64("bundle_lowerblock", m.bundle.lowerBlock), zap.Int("bundle_length", len(m.bundle.fileList)), zap.String("bundle_upper_block_id", m.bundle.upperBlockID))
			oneBlockFiles = nil
			select {
			case <-time.After(m.timeBetweenStoreLookups):
				continue
			case <-m.Terminating():
				return m.Err()
			}
		}

		zlog.Info("merging bundle",
			zap.Uint64("lower_block", m.bundle.lowerBlock),
			zap.Time("upper_block_time", m.bundle.upperBlockTime),
			zap.Duration("real_time_drift", time.Since(m.bundle.upperBlockTime)),
		)
		m.bundleLock.Lock() // we call mergeUpload AND change the bundle, both need locking VS PreMergedBlocks

		uploaded, err := m.mergeUpload()
		if err != nil {
			return err
		}
		if uploaded != nil {
			od.Delete(uploaded)
		}
		if err := m.seenBlocks.Save(); err != nil {
			zlog.Error("cannot save SeenBlockCache", zap.Error(err))
		}
		m.seenBlocks.Truncate()

		if m.stopBlockNum > 0 && m.bundle.upperBlock() >= m.stopBlockNum {
			zlog.Info("reached stop block, terminating process", zap.Uint64("stop_block", m.stopBlockNum))
			return nil
		}

		m.bundle = NewBundle(m.bundle.lowerBlock+m.chunkSize, m.chunkSize)
		m.bundleLock.Unlock()
	}
}

func (m *Merger) retrieveListOfFiles(ctx context.Context) (tooOld []string, seenInCache []string, good []string, err error) {

	canonicalGoodFiles := make(map[string]struct{})

	err = m.oneBlocksStore.Walk(ctx, "", ".tmp", func(filename string) error {
		num, _, _, _, canonical, err := parseFilename(filename)
		if err != nil {
			return nil
		}
		switch {
		case m.isBatchMode && num < m.bundle.lowerBlock:
			return nil
		case m.seenBlocks.IsTooOld(num):
			tooOld = append(tooOld, filename)
		case m.seenBlocks.SeenBefore(canonical):
			seenInCache = append(seenInCache, filename)
		default:
			good = append(good, filename)
			canonicalGoodFiles[canonical] = Empty // good files, deduped by their canonical name
		}

		if len(canonicalGoodFiles) >= m.maxOneBlockOperationsBatchSize {
			return dstore.StopIteration
		}
		return nil
	})

	zlog.Info("retrieved list of files",
		zap.Uint64("seenblock_low_boundary", m.seenBlocks.LowestBlockNum),
		zap.Uint64("bundle_lower_block", m.bundle.lowerBlock),
		zap.Int("seen_files_count", len(seenInCache)),
		zap.Int("too_old_files_count", len(tooOld)),
		zap.Int("good_files_count", len(good)),
		zap.Int("canonical_good_files_count", len(canonicalGoodFiles)),
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
		fileIncluded, err = m.bundle.triage(filename, m.oneBlocksStore, m.seenBlocks)
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
// one-block file, had the time to finish writing, and didn't move the
// lower boundary of our bundle.
func (m *Merger) waitedEnoughForUpperBound() bool {
	return !m.bundle.upperBlockTime.IsZero() && time.Since(m.bundle.upperBlockTime) > m.writersLeewayDuration
}

func (m *Merger) mergeUpload() (uploaded []string, err error) {
	b := m.bundle

	t0 := time.Now()

	bundleFilename := fileNameForBlocksBundle(b.lowerBlock)
	zlog.Debug("about to write merged blocks to storage location", zap.String("filename", bundleFilename), zap.Duration("write_timeout", WriteObjectTimeout), zap.Object("bundle", b))

	err = Retry(5, 500*time.Millisecond, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), WriteObjectTimeout)
		defer cancel()
		return m.destStore.WriteObject(ctx, bundleFilename, NewBundleReader(ctx, b, m.oneBlocksStore))
	})
	if err != nil {
		return nil, fmt.Errorf("write object error: %s", err)
	}

	metrics.HeadBlockTimeDrift.SetBlockTime(b.upperBlockTime)
	metrics.HeadBlockNumber.SetUint64(b.lowerBlock + m.chunkSize)

	if m.progressFilename != "" {
		err := ioutil.WriteFile(m.progressFilename, []byte(fmt.Sprintf("%d", b.lowerBlock+m.chunkSize)), 0644)
		if err != nil {
			zlog.Warn("cannot write progress to file", zap.String("filename", m.progressFilename), zap.Error(err))
		}
	}

	zlog.Info("merged and uploaded", zap.String("filename", fileNameForBlocksBundle(b.lowerBlock)), zap.Duration("merge_time", time.Since(t0)))

	for _, obf := range b.fileList {
		m.seenBlocks.Add(obf.canonicalName) // add them to 'seenbefore' right before deleting them on gs
		for filename := range obf.filenames {
			uploaded = append(uploaded, filename)
		}
	}

	return
}

func removeFilesFromArray(in []string, seen map[string]bool) (out []string) {
	out = make([]string, 0, len(in)-len(seen))
	for _, entry := range in {
		if !seen[entry] {
			out = append(out, entry)
		}
	}
	return
}

func blockFileName(block *bstream.Block) string {
	blockTime := block.Time()
	blockTimeString := fmt.Sprintf("%s.%01d", blockTime.Format("20060102T150405"), blockTime.Nanosecond()/100000000)

	blockID := block.ID()
	if len(blockID) > 8 {
		blockID = blockID[len(blockID)-8:]
	}

	previousID := block.PreviousID()
	if len(previousID) > 8 {
		previousID = previousID[len(previousID)-8:]
	}

	return fmt.Sprintf("%010d-%s-%s-%s", block.Num(), blockTimeString, blockID, previousID)
}
