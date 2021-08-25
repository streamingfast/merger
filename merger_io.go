package merger

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/streamingfast/dstore"

	"github.com/streamingfast/bstream"
	"go.uber.org/zap"
)

type MergerIO struct {
	oneBlocksStore                 dstore.Store
	destStore                      dstore.Store
	maxOneBlockOperationsBatchSize int
}

func NewMergerIO(oneBlocksStore dstore.Store, destStore dstore.Store, maxOneBlockOperationsBatchSize int) *MergerIO {
	return &MergerIO{
		oneBlocksStore:                 oneBlocksStore,
		destStore:                      destStore,
		maxOneBlockOperationsBatchSize: maxOneBlockOperationsBatchSize,
	}
}

func (io *MergerIO) MergeUpload(inclusiveLowerBlock uint64, oneBlockFiles []*OneBlockFile) (err error) {
	if len(oneBlockFiles) == 0 {
		return
	}
	t0 := time.Now()

	bundleFilename := fileNameForBlocksBundle(inclusiveLowerBlock)
	zlog.Debug("about to write merged blocks to storage location", zap.String("filename", bundleFilename), zap.Duration("write_timeout", WriteObjectTimeout), zap.Uint64("lower_block_num", oneBlockFiles[0].num), zap.Uint64("highest_block_num", oneBlockFiles[len(oneBlockFiles)-1].num))

	err = Retry(5, 500*time.Millisecond, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), WriteObjectTimeout)
		defer cancel()
		return io.destStore.WriteObject(ctx, bundleFilename, NewBundleReader(ctx, oneBlockFiles, io.oneBlocksStore))
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

	return fmt.Sprintf("%010d-%s-%s-%s-%d", block.Num(), blockTimeString, blockID, previousID, block.LibNum)
}

func (io *MergerIO) FetchMergeFile(lowBlockNum uint64) ([]*OneBlockFile, error) {
	ctx, cancel := context.WithTimeout(context.Background(), GetObjectTimeout)
	defer cancel()
	reader, err := io.destStore.OpenObject(ctx, fileNameForBlocksBundle(lowBlockNum))
	if err != nil {
		return nil, err
	}

	out, err := toOneBlockFile(reader)
	return out, err
}

func (io *MergerIO) FetchOneBlockFiles(ctx context.Context) (oneBlockFiles []*OneBlockFile, err error) {
	fileCount := 0
	err = io.oneBlocksStore.Walk(ctx, "", ".tmp", func(filename string) error {
		oneBlockFile := MustNewOneBlockFile(filename)
		oneBlockFiles = append(oneBlockFiles, oneBlockFile)

		if fileCount >= io.maxOneBlockOperationsBatchSize {
			return dstore.StopIteration
		}
		return nil
	})

	zlog.Info("retrieved list of files",
		zap.Int("files_count", fileCount),
	)

	return
}

type oneBlockFilesDeleter struct {
	sync.Mutex
	toProcess chan string
	store     dstore.Store
}

func NewOneBlockFilesDeleter(store dstore.Store) *oneBlockFilesDeleter {
	return &oneBlockFilesDeleter{
		store: store,
	}
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
