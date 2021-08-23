package merger

import (
	"context"
	"fmt"
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

	return fmt.Sprintf("%010d-%s-%s-%s", block.Num(), blockTimeString, blockID, previousID)
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
