package merger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"sync"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/merger/bundle"
	"go.uber.org/zap"
)

type IOInterface interface {
	MergeAndStore(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error)
	FetchMergedOneBlockFiles(lowBlockNum uint64) ([]*bundle.OneBlockFile, error)
	WalkOneBlockFiles(ctx context.Context, callback func(*bundle.OneBlockFile) error) error
	DownloadOneBlockFile(ctx context.Context, oneBlockFile *bundle.OneBlockFile) (data []byte, err error)
}

type OneBlockFilesDeleter interface {
	Delete(oneBlockFiles []*bundle.OneBlockFile)
}

type DStoreIO struct {
	oneBlocksStore    dstore.Store
	mergedBlocksStore dstore.Store
	retryAttempts     int
	retryCooldown     time.Duration
}

func NewDStoreIO(
	oneBlocksStore dstore.Store,
	mergedBlocksStore dstore.Store,
	retryAttempts int,
	retryCooldown time.Duration,
) *DStoreIO {
	return &DStoreIO{
		oneBlocksStore:    oneBlocksStore,
		mergedBlocksStore: mergedBlocksStore,
		retryAttempts:     retryAttempts,
		retryCooldown:     retryCooldown,
	}
}

func (s *DStoreIO) MergeAndStore(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
	if len(oneBlockFiles) == 0 {
		return
	}
	t0 := time.Now()

	bundleFilename := fileNameForBlocksBundle(inclusiveLowerBlock)
	zlog.Info("about to write merged blocks to storage location", zap.String("filename", bundleFilename), zap.Duration("write_timeout", WriteObjectTimeout), zap.Uint64("lower_block_num", oneBlockFiles[0].Num), zap.Uint64("highest_block_num", oneBlockFiles[len(oneBlockFiles)-1].Num))

	err = Retry(5, 500*time.Millisecond, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), WriteObjectTimeout)
		defer cancel()
		return s.mergedBlocksStore.WriteObject(ctx, bundleFilename, bundle.NewBundleReader(ctx, oneBlockFiles, s.DownloadOneBlockFile))
	})
	if err != nil {
		return fmt.Errorf("write object error: %s", err)
	}

	zlog.Info("merged and uploaded", zap.String("filename", fileNameForBlocksBundle(inclusiveLowerBlock)), zap.Duration("merge_time", time.Since(t0)))

	return
}

func (s *DStoreIO) FetchMergedOneBlockFiles(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
	ctx, cancel := context.WithTimeout(context.Background(), GetObjectTimeout)
	defer cancel()
	reader, err := s.mergedBlocksStore.OpenObject(ctx, fileNameForBlocksBundle(lowBlockNum))
	if err != nil {
		return nil, err
	}

	out, err := toOneBlockFile(reader)
	return out, err
}

func (s *DStoreIO) WalkOneBlockFiles(ctx context.Context, callback func(*bundle.OneBlockFile) error) error {
	return s.oneBlocksStore.Walk(ctx, "", ".tmp", func(filename string) error {
		oneBlockFile := bundle.MustNewOneBlockFile(filename)

		if oneBlockFile.InnerLibNum == nil {
			data, err := oneBlockFile.Data(ctx, s.DownloadOneBlockFile)
			if err != nil {
				return fmt.Errorf("getting one block file data %q: %w", filename, err)
			}

			blockReader, err := bstream.GetBlockReaderFactory.New(bytes.NewReader(data))
			if err != nil {
				return fmt.Errorf("unable to read one block %q: %w", filename, err)
			}

			block, err := blockReader.Read()
			if block == nil {
				return err
			}

			oneBlockFile.InnerLibNum = &block.LibNum
			if err := callback(oneBlockFile); err != nil {
				return err
			}

		}
		return nil
	})

}

func (s *DStoreIO) DownloadOneBlockFile(ctx context.Context, oneBlockFile *bundle.OneBlockFile) (data []byte, err error) {
	for filename := range oneBlockFile.Filenames { // will try to get MemoizeData from any of those files
		var out io.ReadCloser
		out, err = s.oneBlocksStore.OpenObject(ctx, filename)
		zlog.Debug("downloading one block", zap.String("file_name", filename))
		if err != nil {
			continue
		}
		defer out.Close()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		data, err = ioutil.ReadAll(out)
		if err == nil {
			return data, nil
		}
	}

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

func (od *oneBlockFilesDeleter) Delete(oneBlockFiles []*bundle.OneBlockFile) {
	od.Lock()
	defer od.Unlock()

	if len(oneBlockFiles) == 0 {
		return
	}

	var fileNames []string
	for _, oneBlockFile := range oneBlockFiles {
		for filename := range oneBlockFile.Filenames {
			fileNames = append(fileNames, filename)
		}
	}
	zlog.Info("deleting files that are too old or already seen", zap.Int("number_of_files", len(fileNames)), zap.String("first_file", fileNames[0]), zap.String("last_file", fileNames[len(fileNames)-1]))

	deletable := make(map[string]struct{})

	// dedupe processing queue
	for empty := false; !empty; {
		select {
		case f := <-od.toProcess:
			deletable[f] = bundle.Empty
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
		deletable[file] = bundle.Empty
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

// TODO(froch, 20220107): remove this code, dead code with new mindreader and correct filenames
func toOneBlockFile(mergeFileReader io.ReadCloser) (oneBlockFiles []*bundle.OneBlockFile, err error) {
	defer mergeFileReader.Close()

	blkReader, err := bstream.GetBlockReaderFactory.New(mergeFileReader)
	if err != nil {
		return nil, err
	}

	lowerBlock := uint64(math.MaxUint64)
	highestBlock := uint64(0)
	for {
		block, err := blkReader.Read()
		if block != nil {
			if block.Num() < lowerBlock {
				lowerBlock = block.Num()
			}

			if block.Num() > highestBlock {
				highestBlock = block.Num()
			}

			// we do this little dance to ensure that the 'canonical filename' will match any other oneblockfiles
			// the oneblock encoding/decoding stay together inside 'bundle' package
			fileName := bundle.BlockFileName(block)
			oneBlockFile := bundle.MustNewOneBlockFile(fileName)
			oneBlockFile.Merged = true
			oneBlockFiles = append(oneBlockFiles, oneBlockFile)
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	return
}
