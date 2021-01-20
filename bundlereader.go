package merger

import (
	"context"
	"fmt"
	"io"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dstore"
	"go.uber.org/zap"
)

type BundleReader struct {
	ctx               context.Context
	readBuffer        []byte
	readBufferOffset  int
	oneBlockFiles     []*OneBlockFile
	oneBlockFileStore dstore.Store
	headerPassed      bool
}

func NewBundleReader(ctx context.Context, b *Bundle, oneBlockFileStore dstore.Store) *BundleReader {
	return &BundleReader{
		ctx:               ctx,
		oneBlockFileStore: oneBlockFileStore,
		oneBlockFiles:     b.timeSortedFiles(),
	}
}

func (r *BundleReader) Read(p []byte) (bytesRead int, err error) {
	for {
		if r.readBuffer != nil {
			break
		}

		if len(r.oneBlockFiles) <= 0 {
			return 0, io.EOF
		}

		obf := r.oneBlockFiles[0]
		r.oneBlockFiles = r.oneBlockFiles[1:]
		zlog.Debug("downloading one block file", zap.String("canonical_name", obf.canonicalName))
		data, err := obf.Data(r.ctx, r.oneBlockFileStore)
		if err != nil {
			return 0, err
		}

		if r.headerPassed {
			if len(data) < bstream.GetBlockWriterHeaderLen {
				return 0, fmt.Errorf("one-block-file corrupt: expected header size of %d, but file size is only %d bytes", bstream.GetBlockWriterHeaderLen, len(data))
			}
			data = data[bstream.GetBlockWriterHeaderLen:]
		} else {
			r.headerPassed = true
		}

		if len(data) == 0 {
			r.readBuffer = nil
			continue
		}

		r.readBuffer = data
		r.readBufferOffset = 0
	}

	bytesRead = copy(p, r.readBuffer[r.readBufferOffset:])
	r.readBufferOffset += bytesRead
	if r.readBufferOffset >= len(r.readBuffer) {
		r.readBuffer = nil
	}

	return bytesRead, nil
}
