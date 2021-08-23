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
	oneBlocksStore                 dstore.Store
	destStore                      dstore.Store
	chunkSize                      uint64
	grpcListenAddr                 string
	progressFilename               string
	timeBetweenStoreLookups        time.Duration // should be very low on local filesystem
	oneBlockDeletionThreads        int
	maxOneBlockOperationsBatchSize int

	bundler    *Bundler // currently managed bundle
	bundleLock *sync.Mutex
}

func NewMerger(
	sourceStore dstore.Store,
	destStore dstore.Store,
	bundler *Bundler,
	timeBetweenStoreLookups time.Duration,
	grpcListenAddr string,
	oneBlockDeletionThreads int,
	maxOneBlockOperationsBatchSize int,
) *Merger {
	return &Merger{
		Shutter:                        shutter.New(),
		oneBlocksStore:                 sourceStore,
		destStore:                      destStore,
		bundler:                        bundler,
		bundleLock:                     &sync.Mutex{},
		grpcListenAddr:                 grpcListenAddr,
		timeBetweenStoreLookups:        timeBetweenStoreLookups,
		oneBlockDeletionThreads:        oneBlockDeletionThreads,
		maxOneBlockOperationsBatchSize: maxOneBlockOperationsBatchSize,
	}
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
	zlog.Info("starting merger", zap.Stringer("bundler", m.bundler))

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

func (m *Merger) addExistingMergedFileBlocksToBundler(file io.ReadCloser) (err error) {
	defer file.Close()

	blkReader, err := bstream.GetBlockReaderFactory.New(file)
	if err != nil {
		return err
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
			return err
		}
		fileName := blockFileName(block)
		oneBlockFile := MustNewOneBlockFile(fileName)
		oneBlockFile.merged = true
		m.bundler.AddOneBlockFile(oneBlockFile)
	}
	zlog.Info("Processed, already existing merged file",
		zap.Uint64("lower_block", lowerBlock),
		zap.Uint64("highest_block", highestBlock),
	)

	m.bundleLock.Lock()
	defer m.bundleLock.Unlock()

	if err := m.bundler.Save(); err != nil {
		zlog.Error("cannot save SeenBlockCache", zap.Error(err))
	}
	//todo: purge here?

	return nil
}

func (m *Merger) launch() (err error) {
	filesDeleter := newOneBlockFilesDeleter(m.oneBlocksStore)
	filesDeleter.Start(m.oneBlockDeletionThreads, 100000)

	for {
		if m.IsTerminating() {
			return nil
		}

		zlog.Debug("verifying if bundle file already exist in store")
		if remoteMergedFile, cancel, err := fetchMergedFile(m.destStore, m.bundler.InclusiveLowerBlock()); err == nil {
			err := m.addExistingMergedFileBlocksToBundler(remoteMergedFile)
			//todo: commit here
			//todo: we should call bundler purge func here.
			cancel()
			if err != nil {
				zlog.Error("error processing remote file to bump bundle", zap.Error(err), zap.Uint64("start_block_num", m.bundler.InclusiveLowerBlock()))
			} else {
				continue // keep bumping
			}
		}

		//todo: at this point we should only have one root in the forkdb and we should validate here

		zlog.Debug("one block file list empty, building list")

		m.bundleLock.Lock()
		ctx, cancel := context.WithTimeout(context.Background(), ListFilesTimeout)
		tooOldFiles, lastOneBlockFileAdded, err := m.retrieveOneBlockFile(ctx)
		cancel()
		if err != nil {
			return err
		}
		m.bundleLock.Unlock()

		//todo: replace this block by bundler.purge with call back
		filesDeleter.Delete(tooOldFiles)
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

		//bilc: isBundleComplete := m.bundle.isComplete() && m.waitedEnoughForUpperBound()
		isBundleComplete, highestBundleBlockNum := m.bundler.isComplete()
		if !isBundleComplete {
			zlog.Info("waiting for more files to complete bundle") //zap.Uint64("bundle_lowerblock", m.bundle.lowerBlock), //todo
			//zap.Int("bundle_length", len(m.bundle.fileList)), //todo
			//zap.String("bundle_upper_block_id", m.bundle.upperBlockID), //todo

			select {
			case <-time.After(m.timeBetweenStoreLookups):
				continue
			case <-m.Terminating():
				return m.Err()
			}
		}

		zlog.Info("merging bundle",
			zap.Uint64("highest_bundle_block_num", highestBundleBlockNum),
			//zap.Uint64("lower_block", m.bundle.lowerBlock), //todo
			//zap.Time("upper_block_time", m.bundle.upperBlockTime), //todo
			//zap.Duration("real_time_drift", time.Since(m.bundle.upperBlockTime)), //todo
		)
		m.bundleLock.Lock() // we call mergeUpload AND change the bundle, both need locking VS PreMergedBlocks

		bundleFiles, err := m.bundler.ToBundle(highestBundleBlockNum)
		if err != nil {
			return err
		}

		err = m.mergeUpload(m.bundler.InclusiveLowerBlock(), bundleFiles)
		if err != nil {
			return err
		}

		//todo: purge here using "filesDeleter.Delete(uploaded)" make sure to delete all filenames for oneblock file.
		//todo bundler commit

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

		m.bundleLock.Unlock()

	}
}

func (m *Merger) retrieveOneBlockFile(ctx context.Context) (tooOld []string, lastOneBlockFileAdded *OneBlockFile, err error) {
	addedFileCount := 0
	err = m.oneBlocksStore.Walk(ctx, "", ".tmp", func(filename string) error {
		//todo: call bundle add then check reponse to act on onblockfile ...
		oneBlockFile := MustNewOneBlockFile(filename)

		firstSeenBlock, err := m.bundler.FirstBlockNum()
		if err != nil {
			return fmt.Errorf("getting bundler firstBlock")
		}

		isTooOld := oneBlockFile.num < firstSeenBlock
		switch {
		case isTooOld:
			tooOld = append(tooOld, filename)
		default:
			m.bundler.AddOneBlockFile(oneBlockFile)
			lastOneBlockFileAdded = oneBlockFile
			addedFileCount++
		}

		if addedFileCount >= m.maxOneBlockOperationsBatchSize {
			return dstore.StopIteration
		}
		return nil
	})

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

func (m *Merger) mergeUpload(inclusiveLowerBlock uint64, oneBlockFiles []*OneBlockFile) (err error) {
	if len(oneBlockFiles) == 0 {
		return
	}
	t0 := time.Now()

	bundleFilename := fileNameForBlocksBundle(inclusiveLowerBlock)
	zlog.Debug("about to write merged blocks to storage location", zap.String("filename", bundleFilename), zap.Duration("write_timeout", WriteObjectTimeout), zap.Uint64("lower_block_num", oneBlockFiles[0].num), zap.Uint64("highest_block_num", oneBlockFiles[len(oneBlockFiles)-1].num))

	err = Retry(5, 500*time.Millisecond, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), WriteObjectTimeout)
		defer cancel()
		return m.destStore.WriteObject(ctx, bundleFilename, NewBundleReader(ctx, oneBlockFiles, m.oneBlocksStore))
	})
	if err != nil {
		return fmt.Errorf("write object error: %s", err)
	}

	zlog.Info("merged and uploaded", zap.String("filename", fileNameForBlocksBundle(inclusiveLowerBlock)), zap.Duration("merge_time", time.Since(t0)))

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
