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
	"sync"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dhammer"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

type BundleReader struct {
	ctx              context.Context
	readBuffer       []byte
	readBufferOffset int
	oneBlockFiles    []*bstream.OneBlockFile
	headerPassed     bool

	downloader      *dhammer.Nailer
	startDownloader sync.Once

	logger *zap.Logger
}

func NewBundleReader(ctx context.Context, logger *zap.Logger, tracer logging.Tracer, oneBlockFiles []*bstream.OneBlockFile, oneBlockDownloader bstream.OneBlockDownloaderFunc) *BundleReader {
	return &BundleReader{
		ctx:           ctx,
		oneBlockFiles: oneBlockFiles,
		downloader:    dhammer.NewNailer(ParallelOneBlockDownload, downloadOneBlockJob(logger, tracer, oneBlockDownloader), dhammer.NailerLogger(logger), dhammer.NailerTracer(tracer)),
		logger:        logger,
	}
}

func (r *BundleReader) Read(p []byte) (bytesRead int, err error) {
	r.startDownloader.Do(func() {
		r.downloader.Start(r.ctx)
		go func() {
			for _, oneBlockFile := range r.oneBlockFiles {
				r.downloader.Push(r.ctx, oneBlockFile)
			}
			r.oneBlockFiles = []*bstream.OneBlockFile{}
			r.logger.Debug("finished queuing one block files to be read")
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

func downloadOneBlockJob(logger *zap.Logger, tracer logging.Tracer, oneBlockDownloader bstream.OneBlockDownloaderFunc) dhammer.NailerFunc {
	return func(ctx context.Context, in interface{}) (interface{}, error) {
		oneBlockFile := in.(*bstream.OneBlockFile)
		if tracer.Enabled() {
			logger.Debug("downloading one block file", zap.String("failename", oneBlockFile.CanonicalName))
		}
		data, err := oneBlockFile.Data(ctx, oneBlockDownloader)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve oneblock %s file data: %w", oneBlockFile.CanonicalName, err)
		}
		return data, nil
	}
}
