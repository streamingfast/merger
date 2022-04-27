package bundle

import (
	"context"
	"fmt"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dhammer"
	"go.uber.org/zap"
	"io"
	"sync"
)

type BundleReader struct {
	ctx              context.Context
	readBuffer       []byte
	readBufferOffset int
	oneBlockFiles    []*OneBlockFile
	headerPassed     bool

	downloader      *dhammer.Nailer
	startDownloader sync.Once
}

const ParallelOneBlockDownload = 2

func NewBundleReader(ctx context.Context, oneBlockFiles []*OneBlockFile, oneBlockDownloader oneBlockDownloaderFunc) *BundleReader {
	return &BundleReader{
		ctx:           ctx,
		oneBlockFiles: oneBlockFiles,
		downloader:    dhammer.NewNailer(ParallelOneBlockDownload, downloadOneBlockJob(oneBlockDownloader)),
	}
}

func (r *BundleReader) Read(p []byte) (bytesRead int, err error) {
	r.startDownloader.Do(func() {
		r.downloader.Start(r.ctx)
		go func() {
			for _, oneBlockFile := range r.oneBlockFiles {
				r.downloader.Push(r.ctx, oneBlockFile)
			}
			r.oneBlockFiles = []*OneBlockFile{}
			zlog.Debug("finished queuing one block files to be read")
			r.downloader.Close()
		}()
	})

	if r.readBuffer == nil {
		// nothing in the buffer read the next one block that should be ready
		if err := r.downloader.Err(); err != nil {
			return 0, fmt.Errorf("parallel one block downloader failed: %w", err)
		}
		
		out, hasMore := <-r.downloader.Out
		if !hasMore {
			return 0, io.EOF
		}

		if err := r.downloader.Err(); err != nil {
			return 0, fmt.Errorf("parallel one block downloader failed: %w", err)
		}

		data := out.([]byte)
		if len(data) == 0 {
			r.readBuffer = nil
			return 0, fmt.Errorf("one-block-file corrupt: empty data")
		}

		if r.headerPassed {
			if len(data) < bstream.GetBlockWriterHeaderLen {
				return 0, fmt.Errorf("one-block-file corrupt: expected header size of %d, but file size is only %d bytes", bstream.GetBlockWriterHeaderLen, len(data))
			}
			data = data[bstream.GetBlockWriterHeaderLen:]
		} else {
			r.headerPassed = true
		}
		r.readBuffer = data
		r.readBufferOffset = 0
	}
	// there are still bytes to be read
	bytesRead = copy(p, r.readBuffer[r.readBufferOffset:])
	r.readBufferOffset += bytesRead
	if r.readBufferOffset >= len(r.readBuffer) {
		r.readBuffer = nil
	}

	return bytesRead, nil
}

func downloadOneBlockJob(oneBlockDownloader oneBlockDownloaderFunc) dhammer.NailerFunc {
	return func(ctx context.Context, in interface{}) (interface{}, error) {
		oneBlockFile := in.(*OneBlockFile)
		if tracer.Enabled() {
			zlog.Debug("downloading one block file", zap.String("failename", oneBlockFile.CanonicalName))
		}
		data, err := oneBlockFile.Data(ctx, oneBlockDownloader)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve oneblock %s file data: %w", oneBlockFile.CanonicalName, err)
		}
		return data, nil
	}
}
