package merger

import (
	"context"
	"io"

	"github.com/dfuse-io/bstream"

	"github.com/dfuse-io/dstore"
)

type BundleReader struct {
	ctx               context.Context
	readBuffer        []byte
	oneBlockFiles     []*OneBlockFile
	oneBlockFileStore dstore.Store
	headerRead        bool
}

func NewBundleReader(ctx context.Context, b *Bundle, oneBlockFileStore dstore.Store) *BundleReader {
	return &BundleReader{
		ctx:               ctx,
		oneBlockFileStore: oneBlockFileStore,
		oneBlockFiles:     b.timeSortedFiles(),
	}
}
func (r *BundleReader) Read(p []byte) (bytesRead int, err error) {
	bytesToRead := len(p)

	//load more data if needed
	for len(r.readBuffer) < bytesToRead {
		var obf *OneBlockFile
		if len(r.oneBlockFiles) > 0 {

			//pop first one block file from bundle
			obf, r.oneBlockFiles = r.oneBlockFiles[0], r.oneBlockFiles[1:]
			data, err := obf.Data(r.ctx, r.oneBlockFileStore)
			if err != nil {
				return 0, err
			}

			//merge file only contain header
			removeHeaderLength := bstream.GetBlockWriterHeaderLen
			if !r.headerRead {
				removeHeaderLength = 0
				r.headerRead = true
			}
			r.readBuffer = append(r.readBuffer, data[removeHeaderLength:]...)
		} else {
			break //no more file
		}
	}

	bytesRead = copy(p, r.readBuffer)
	if bytesRead == 0 {
		return 0, io.EOF
	}
	r.readBuffer = r.readBuffer[bytesRead:] //remove read data from buffer
	return bytesRead, nil
}
