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
	"io"
	"math"
	"sync"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/merger/metrics"
	pbmerge "github.com/streamingfast/pbgo/dfuse/merger/v1"
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
	deleteFilesFunc     func(fileNames []*OneBlockFile)
	mergeUploadFunc     func(inclusiveLowerBlock uint64, oneBlockFiles []*OneBlockFile) (err error)
}

func NewMerger(
	bundler *Bundler,
	timeBetweenStoreLookups time.Duration,
	grpcListenAddr string,
	fetchMergedFileFunc func(lowBlockNum uint64) ([]*OneBlockFile, error),
	fetchOneBlockFiles func(ctx context.Context) (oneBlockFiles []*OneBlockFile, err error),
	deleteFilesFunc func(fileNames []*OneBlockFile),
	mergeUploadFunc func(inclusiveLowerBlock uint64, oneBlockFiles []*OneBlockFile) (err error),
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
	}
}

func (m *Merger) Launch() {
	// figure out where to start merging based on dest store
	zlog.Info("starting merger", zap.Stringer("bundler", m.bundler))

	m.startServer()

	err := m.launch()
	zlog.Info("merger exited", zap.Error(err))
	m.Shutdown(err)
}

func (m *Merger) launch() (err error) {

	err = m.bootstrap()
	if err != nil {
		return fmt.Errorf("bootstraping: %w", err)
	}

	return m.mainLoop()
}

func NewOneBlockFilesDeleter(store dstore.Store) *oneBlockFilesDeleter {
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

func (od *oneBlockFilesDeleter) Delete(oneBlockFiles []*OneBlockFile) {
	od.Lock()
	defer od.Unlock()

	if len(oneBlockFiles) == 0 {
		return
	}

	var fileNames []string
	for _, oneBlockFile := range oneBlockFiles {
		for filename, _ := range oneBlockFile.filenames {
			fileNames = append(fileNames, filename)
		}
	}
	zlog.Info("deleting files that are too old or already seen", zap.Int("number_of_files", len(fileNames)), zap.String("first_file", fileNames[0]), zap.String("last_file", fileNames[len(fileNames)-1]))

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

	for _, file := range fileNames {
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

func toOneBlockFile(mergeFileReader io.ReadCloser) (oneBlockFiles []*OneBlockFile, err error) {
	defer mergeFileReader.Close()

	blkReader, err := bstream.GetBlockReaderFactory.New(mergeFileReader)
	if err != nil {
		return nil, err
	}

	lowerBlock := uint64(math.MaxUint64)
	highestBlock := uint64(0)
	for {
		block, err := blkReader.Read()

		if block.Num() < lowerBlock {
			lowerBlock = block.Num()
		}

		if block.Num() > highestBlock {
			highestBlock = block.Num()
		}

		if block == nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		fileName := blockFileName(block)
		oneBlockFile := MustNewOneBlockFile(fileName)
		oneBlockFile.merged = true
		oneBlockFiles = append(oneBlockFiles, oneBlockFile)
	}
	zlog.Info("Processed, already existing merged file",
		zap.Uint64("lower_block", lowerBlock),
		zap.Uint64("highest_block", highestBlock),
	)

	return
}

func (m *Merger) bootstrap() (err error) {
	err = m.bundler.Boostrap(func(lowBlockNum uint64) (oneBlockFiles []*OneBlockFile, err error) {
		oneBlockFiles, fetchErr := m.fetchMergedFileFunc(lowBlockNum)
		if fetchErr != nil {
			return nil, fmt.Errorf("fetching one block file from merged file with low block num:%d %w", lowBlockNum, fetchErr)
		}
		return oneBlockFiles, err
	})

	if err != nil {
		return fmt.Errorf("bundler bootstrap")
	}
	return nil
}

func (m *Merger) mainLoop() (err error) {
	for {
		if m.IsTerminating() {
			return nil
		}

		zlog.Debug("verifying if bundle file already exist in store")
		if oneBlockFiles, err := m.fetchMergedFileFunc(m.bundler.InclusiveLowerBlock()); err == nil {
			for _, oneBlockFile := range oneBlockFiles {
				m.bundler.AddOneBlockFile(oneBlockFile)
			}
			if complete, highestBlockLimit := m.bundler.isComplete(); complete {
				if err := m.bundler.Commit(highestBlockLimit); err != nil {
					return fmt.Errorf("commiting already merged block: %w", err)
				}
				if err := m.bundler.Purge(func(purgedOneBlockFiles []*OneBlockFile) {}); err != nil {
					return fmt.Errorf("purging already merged block: %w", err)
				}
				continue //let try next bundle
			} else {
				return fmt.Errorf("bundler should have a completed a bundle at the point: bundler: %s", m.bundler)
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), ListFilesTimeout)
		tooOldFiles, lastOneBlockFileAdded, err := m.retrieveOneBlockFile(ctx)
		cancel()
		if err != nil {
			return fmt.Errorf("retreiving one block files: %w", err)
		}

		m.deleteFilesFunc(tooOldFiles)

		if lastOneBlockFileAdded == nil {
			select {
			case <-time.After(m.timeBetweenStoreLookups):
				continue
			case <-m.Terminating():
				return m.Err()
			}
		}

		if lastOneBlockFileAdded.num < m.bundler.lastMergeBlock.BlockNum { // will still drift if there is a hole and lastFile is advancing
			metrics.HeadBlockTimeDrift.SetBlockTime(lastOneBlockFileAdded.blockTime)
		}

		isBundleComplete, highestBundleBlockNum := m.bundler.isComplete()
		if !isBundleComplete {
			zlog.Info("waiting for more files to complete bundle", zap.Stringer("bundler", m.bundler))

			select {
			case <-time.After(m.timeBetweenStoreLookups):
				continue
			case <-m.Terminating():
				return m.Err()
			}
		}

		zlog.Info("merging bundle",
			zap.Uint64("highest_bundle_block_num", highestBundleBlockNum),
			zap.Stringer("bundler", m.bundler),
		)

		bundleFiles, err := m.bundler.ToBundle(highestBundleBlockNum)
		if err != nil {
			return err
		}

		if err := m.mergeUploadFunc(m.bundler.InclusiveLowerBlock(), bundleFiles); err != nil {
			return err
		}

		if err := m.bundler.Commit(highestBundleBlockNum); err != nil {
			return fmt.Errorf("commiting: %w", err)
		}

		err = m.bundler.Purge(func(purgedOneBlockFiles []*OneBlockFile) {
			m.deleteFilesFunc(purgedOneBlockFiles)
		})

		if err != nil {
			return fmt.Errorf("purging: %w", err)
		}

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
		firstSeenBlock, err := m.bundler.FirstBlockNum()
		if err != nil {
			return nil, nil, fmt.Errorf("getting bundler firstBlock")
		}

		isTooOld := oneBlockFile.num < firstSeenBlock
		switch {
		case isTooOld:
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

func (m *Merger) addOneBlockFiles(in []string) (err error) {
	if len(in) > 0 {
		zlog.Debug("entering triage", zap.String("first_file", in[0]), zap.String("last_file", in[len(in)-1]))
	}

	for _, filename := range in {
		blockNum, blockTime, blockIDSuffix, previousIDSuffix, canonicalName, err := parseFilename(filename)
		if err != nil {
			return err
		}
		m.bundler.AddFile(filename, blockNum, blockTime, blockIDSuffix, previousIDSuffix, canonicalName)
	}

	return nil
}

//todo : fix me
func (m *Merger) PreMergedBlocks(req *pbmerge.Request, server pbmerge.Merger_PreMergedBlocksServer) error {
	//m.bundleLock.Lock()
	//defer m.bundleLock.Unlock()
	//
	//if req.LowBlockNum < m.bundle.lowerBlock || req.LowBlockNum >= m.bundle.upperBlock() {
	//	err := fmt.Errorf("cannot find requested blocks")
	//	server.SetHeader(metadata.New(map[string]string{"error": err.Error()}))
	//	return err
	//}
	//
	//files := m.bundle.timeSortedFiles()
	//var foundHighBlockID bool
	//var foundLowBlockNum bool
	//for _, oneBlock := range files {
	//	if oneBlock.num == req.LowBlockNum {
	//		foundLowBlockNum = true
	//	}
	//	if strings.HasSuffix(req.HighBlockID, oneBlock.id) {
	//		foundHighBlockID = true
	//		break
	//	}
	//}
	//if !foundLowBlockNum {
	//	err := fmt.Errorf("cannot find requested lowBlockNum")
	//	server.SetHeader(metadata.New(map[string]string{"error": err.Error()}))
	//	return err
	//}
	//if !foundHighBlockID {
	//	err := fmt.Errorf("cannot find requested highBlockID")
	//	server.SetHeader(metadata.New(map[string]string{"error": err.Error()}))
	//	return err
	//}
	//
	//for _, oneBlock := range m.bundle.timeSortedFiles() {
	//	if oneBlock.num < req.LowBlockNum {
	//		continue
	//	}
	//	data, err := oneBlock.Data(server.Context(), m.oneBlocksStore)
	//	if err != nil {
	//		return fmt.Errorf("unable to get one block data: %w", err)
	//	}
	//
	//	blockReader, err := bstream.GetBlockReaderFactory.New(bytes.NewReader(data))
	//	if err != nil {
	//		return fmt.Errorf("unable to read one block: %w", err)
	//	}
	//
	//	block, err := blockReader.Read()
	//	if block == nil {
	//		return err
	//	}
	//
	//	protoblock, err := block.ToProto()
	//	if protoblock == nil || err != nil {
	//		return err
	//	}
	//
	//	err = server.Send(
	//		&pbmerge.Response{
	//			Found: true, //todo: this is not require any more
	//			Block: protoblock,
	//		})
	//
	//	if err != nil {
	//		return fmt.Errorf("unable send response to client: %w", err)
	//	}
	//
	//	if strings.HasSuffix(req.HighBlockID, oneBlock.id) {
	//		break
	//	}
	//}
	//
	return nil
}
