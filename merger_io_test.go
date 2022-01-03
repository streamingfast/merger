package merger

import (
	"context"
	"fmt"
	"io"
	"path"
	"runtime"
	"testing"

	"github.com/streamingfast/merger/bundle"

	"github.com/golang/protobuf/ptypes"

	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dbin"
	"github.com/streamingfast/dstore"
	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
	"github.com/stretchr/testify/require"
)

func TestNewMergerIO(t *testing.T) {
	oneBlockStoreStore, err := dstore.NewDBinStore("/tmp/oneblockstore")
	require.NoError(t, err)

	mergedBlocksStore, err := dstore.NewDBinStore("/tmp/mergedblockstore")
	require.NoError(t, err)

	mio := NewMergerIO(oneBlockStoreStore, mergedBlocksStore, 10, nil, nil)
	require.NotNil(t, mio)
	require.IsType(t, &MergerIO{}, mio)
}

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
}

func TestMergerIO_FetchOneBlockFiles_GetOneBlockFileDataError(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	baseDir := path.Dir(filename)
	baseDir = path.Join(baseDir, "bundle/test_data")
	oneBlockStoreStore, err := dstore.NewStore("file://"+baseDir, "dbin", "", true)
	require.NoError(t, err)

	downloadFileFunc := func(ctx context.Context, oneBlockFile *bundle.OneBlockFile) (data []byte, err error) {
		return nil, fmt.Errorf("yo")
	}

	mergerIO := &MergerIO{
		oneBlocksStore:                 oneBlockStoreStore,
		maxOneBlockOperationsBatchSize: 3,
		downloadFileFunc:               downloadFileFunc,
	}

	oneBlockFiles, err := mergerIO.FetchOneBlockFiles(context.Background())
	require.Error(t, err)
	require.Errorf(t, err, "getting one block file data: yo")
	require.Nil(t, oneBlockFiles)
}

func TestMergerIO_FetchOneBlockFiles_GetBlockReaderFactoryError(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	baseDir := path.Dir(filename)
	baseDir = path.Join(baseDir, "bundle/test_data")
	oneBlockStoreStore, err := dstore.NewStore("file://"+baseDir, "dbin", "", true)
	require.NoError(t, err)

	bstream.GetBlockReaderFactory = bstream.BlockReaderFactoryFunc(blockReaderFactoryNil)

	downloadFileFunc := func(ctx context.Context, oneBlockFile *bundle.OneBlockFile) (data []byte, err error) {
		return []byte{}, nil
	}

	mergerIO := &MergerIO{
		oneBlocksStore:                 oneBlockStoreStore,
		maxOneBlockOperationsBatchSize: 3,
		downloadFileFunc:               downloadFileFunc,
	}

	oneBlockFiles, err := mergerIO.FetchOneBlockFiles(context.Background())
	require.Error(t, err)
	require.Nil(t, oneBlockFiles)
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

		blk, err := blockFromProto(pbBlock)
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

func blockFromProto(b *pbbstream.Block) (*bstream.Block, error) {
	blockTime, err := ptypes.Timestamp(b.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("unable to turn google proto Timestamp %q into time.Time: %w", b.Timestamp.String(), err)
	}

	return bstream.MemoryBlockPayloadSetter(&bstream.Block{
		Id:             b.Id,
		Number:         b.Number,
		PreviousId:     b.PreviousId,
		Timestamp:      blockTime,
		LibNum:         b.LibNum,
		PayloadKind:    b.PayloadKind,
		PayloadVersion: b.PayloadVersion,
	}, b.PayloadBuffer)
}

func TestMergerIO_MergeUpload_ZeroLengthOneBlockFiles(t *testing.T) {
	oneBlockStoreStore, err := dstore.NewDBinStore("/tmp/oneblockstore")
	require.NoError(t, err)

	mergedBlocksStore, err := dstore.NewDBinStore("/tmp/mergedblockstore")
	require.NoError(t, err)

	mio := NewMergerIO(oneBlockStoreStore, mergedBlocksStore, 10, nil, nil)
	err = mio.MergeUpload(0, []*bundle.OneBlockFile{})
	require.Nil(t, err)
}

func TestMergerIO_MergeUpload(t *testing.T) {
	files := []*bundle.OneBlockFile{
		bundle.MustNewOneBlockFile("0000000114-20210728T105016.0-00000114a-00000113a-90-suffix"),
		bundle.MustNewOneBlockFile("0000000115-20210728T105116.0-00000115a-00000114a-90-suffix"),
		bundle.MustNewOneBlockFile("0000000116-20210728T105216.0-00000116a-00000115a-90-suffix"),
		bundle.MustNewOneBlockFile("0000000117-20210728T105316.0-00000117a-00000116a-90-suffix"),
		bundle.MustNewOneBlockFile("0000000118-20210728T105316.0-00000118a-00000117a-90-suffix"),
	}

	oneBlockStoreStore, err := dstore.NewDBinStore("/tmp/oneblockstore")
	require.NoError(t, err)
	mergedBlocksStore, err := dstore.NewDBinStore("/tmp/mergedblockstore")
	require.NoError(t, err)
	mio := NewMergerIO(oneBlockStoreStore, mergedBlocksStore, 10, nil, nil)

	err = mio.MergeUpload(114, files)
	require.NoError(t, err)
}

func TestMergerIO_MergeUpload_WriteObjectError(t *testing.T) {
	files := []*bundle.OneBlockFile{
		bundle.MustNewOneBlockFile("0000000114-20210728T105016.0-00000114a-00000113a-90-suffix"),
		bundle.MustNewOneBlockFile("0000000115-20210728T105116.0-00000115a-00000114a-90-suffix"),
		bundle.MustNewOneBlockFile("0000000116-20210728T105216.0-00000116a-00000115a-90-suffix"),
		bundle.MustNewOneBlockFile("0000000117-20210728T105316.0-00000117a-00000116a-90-suffix"),
		bundle.MustNewOneBlockFile("0000000118-20210728T105316.0-00000118a-00000117a-90-suffix"),
	}

	oneBlockStoreStore, err := dstore.NewDBinStore("/tmp/oneblockstore")
	require.NoError(t, err)
	mergedBlocksStore, err := dstore.NewDBinStore("/tmp/mergedblockstore")
	require.NoError(t, err)
	mio := NewMergerIO(oneBlockStoreStore, mergedBlocksStore, 10, nil, nil)

	mio.writeObjectFunc = func() error {
		return fmt.Errorf("yo")
	}

	err = mio.MergeUpload(114, files)
	require.Error(t, err)
}

func TestMergerIO_FetchMergeFile(t *testing.T) {
	files := []*bundle.OneBlockFile{
		bundle.MustNewOneBlockFile("0000000114-20210728T105016.0-00000114a-00000113a-90-suffix"),
		bundle.MustNewOneBlockFile("0000000115-20210728T105116.0-00000115a-00000114a-90-suffix"),
		bundle.MustNewOneBlockFile("0000000116-20210728T105216.0-00000116a-00000115a-90-suffix"),
		bundle.MustNewOneBlockFile("0000000117-20210728T105316.0-00000117a-00000116a-90-suffix"),
		bundle.MustNewOneBlockFile("0000000118-20210728T105316.0-00000118a-00000117a-90-suffix"),
	}

	oneBlockStoreStore, err := dstore.NewDBinStore("/tmp/oneblockstore")
	require.NoError(t, err)
	mergedBlocksStore, err := dstore.NewDBinStore("/tmp/mergedblockstore")
	require.NoError(t, err)
	mio := NewMergerIO(oneBlockStoreStore, mergedBlocksStore, 10, nil, nil)

	err = mio.MergeUpload(114, files)
	require.Nil(t, err)

	obf, err := mio.FetchMergeFile(114)
	// test file is empty
	require.Error(t, err)
	require.Errorf(t, err, "EOF")
	require.Nil(t, obf)
}

func TestMergerIO_FetchMergeFile_OpenObjectError(t *testing.T) {
	oneBlockStoreStore, err := dstore.NewDBinStore("/tmp/oneblockstore")
	require.NoError(t, err)
	mergedBlocksStore, err := dstore.NewDBinStore("/tmp/mergedblockstore")
	require.NoError(t, err)
	mio := NewMergerIO(oneBlockStoreStore, mergedBlocksStore, 10, nil, nil)

	obf, err := mio.FetchMergeFile(69)
	// file not found
	require.Error(t, err)
	require.Errorf(t, err, "not found")
	require.Nil(t, obf)
}
