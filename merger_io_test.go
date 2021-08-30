package merger

import (
	"context"
	"fmt"
	"io"
	"path"
	"runtime"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dbin"
	"github.com/streamingfast/dstore"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
	"github.com/stretchr/testify/require"
)

func TestMergerIO_FetchOneBlockFiles(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	baseDir := path.Dir(filename)
	baseDir = path.Join(baseDir, "bundle/test_data")
	oneBlockStoreStore, err := dstore.NewStore("file://"+baseDir, "dbin", "", true)
	require.NoError(t, err)

	bstream.GetBlockReaderFactory = bstream.BlockReaderFactoryFunc(blockReaderFactory)

	mergerIO := &MergerIO{
		oneBlocksStore:                 oneBlockStoreStore,
		maxOneBlockOperationsBatchSize: 3,
	}

	oneBlockFiles, err := mergerIO.FetchOneBlockFiles(context.Background())
	require.NoError(t, err)

	require.Equal(t, uint64(1), oneBlockFiles[2].LibNum())
	//if err != nil {
	//	return fmt.Errorf("failed to init source archive store: %w", err)
	//}

}

func blockReaderFactory(reader io.Reader) (bstream.BlockReader, error) {
	return NewBlockReader(reader)
}

// BlockReader reads the dbin format where each element is assumed to be a `bstream.Block`.
type BlockReader struct {
	src *dbin.Reader
}

func NewBlockReader(reader io.Reader) (out *BlockReader, err error) {
	dbinReader := dbin.NewReader(reader)
	contentType, version, err := dbinReader.ReadHeader()
	if err != nil {
		return nil, fmt.Errorf("unable to read file header: %s", err)
	}

	Protocol := pbbstream.Protocol(pbbstream.Protocol_value[contentType])

	if Protocol != pbbstream.Protocol_ETH && version != 1 {
		return nil, fmt.Errorf("reader only knows about %s block kind at version 1, got %s at version %d", Protocol, contentType, version)
	}

	return &BlockReader{
		src: dbinReader,
	}, nil
}

func (l *BlockReader) Read() (*bstream.Block, error) {
	message, err := l.src.ReadMessage()
	if len(message) > 0 {
		pbBlock := new(pbbstream.Block)
		err = proto.Unmarshal(message, pbBlock)
		if err != nil {
			return nil, fmt.Errorf("unable to read block proto: %s", err)
		}

		blk, err := bstream.BlockFromProto(pbBlock)
		if err != nil {
			return nil, err
		}

		return blk, nil
	}

	if err == io.EOF {
		return nil, err
	}

	// In all other cases, we are in an error path
	return nil, fmt.Errorf("failed reading next dbin message: %s", err)
}
