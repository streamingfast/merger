package merger

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"runtime"
	"testing"
	"time"

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

	mio := NewIOStore(oneBlockStoreStore, mergedBlocksStore, 10, nil, nil, 1, 10*time.Millisecond)
	require.NotNil(t, mio)
	require.IsType(t, &IOStore{}, mio)
}

func TestMergerIO_FetchOneBlockFiles(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	baseDir := path.Dir(filename)
	baseDir = path.Join(baseDir, "bundle/test_data")
	oneBlockStoreStore, err := dstore.NewStore("file://"+baseDir, "dbin", "", true)
	require.NoError(t, err)

	bstream.GetBlockReaderFactory = bstream.BlockReaderFactoryFunc(blockReaderFactory)

	mergerIO := &IOStore{
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

	mergerIO := &IOStore{
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

	mergerIO := &IOStore{
		oneBlocksStore:                 oneBlockStoreStore,
		maxOneBlockOperationsBatchSize: 3,
		downloadFileFunc:               downloadFileFunc,
	}

	oneBlockFiles, err := mergerIO.FetchOneBlockFiles(context.Background())
	require.Error(t, err)
	require.Nil(t, oneBlockFiles)
}

func TestMergerIO_FetchOneBlockFiles_ReadBlockError(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	baseDir := path.Dir(filename)
	baseDir = path.Join(baseDir, "bundle/test_data")
	oneBlockStoreStore, err := dstore.NewStore("file://"+baseDir, "dbin", "", true)
	require.NoError(t, err)

	bstream.GetBlockReaderFactory = bstream.BlockReaderFactoryFunc(blockReaderFactoryError)

	mergerIO := &IOStore{
		oneBlocksStore:                 oneBlockStoreStore,
		maxOneBlockOperationsBatchSize: 3,
	}

	oneBlockFiles, err := mergerIO.FetchOneBlockFiles(context.Background())
	require.Error(t, err)
	require.Errorf(t, err, "yo")
	require.Nil(t, oneBlockFiles)
}

func blockReaderFactory(reader io.Reader) (bstream.BlockReader, error) {
	return NewBlockReader(reader)
}

func blockReaderFactoryNil(reader io.Reader) (bstream.BlockReader, error) {
	return nil, fmt.Errorf("yo")
}

func blockReaderFactoryError(reader io.Reader) (bstream.BlockReader, error) {
	return NewBlockReaderError(reader)
}

// BlockReader reads the dbin format where each element is assumed to be a `bstream.Block`.
type BlockReader struct {
	src *dbin.Reader
}

// BlockReaderError forces an error on Read
type BlockReaderError struct {
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

func NewBlockReaderError(reader io.Reader) (out *BlockReaderError, err error) {
	dbinReader := dbin.NewReader(reader)
	return &BlockReaderError{
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

func (l *BlockReaderError) Read() (*bstream.Block, error) {
	return nil, fmt.Errorf("yo")
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

	mio := NewIOStore(oneBlockStoreStore, mergedBlocksStore, 10, nil, nil, 1, 10*time.Millisecond)
	err = mio.MergeAndUpload(0, []*bundle.OneBlockFile{})
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
	mio := NewIOStore(oneBlockStoreStore, mergedBlocksStore, 10, nil, nil, 1, 10*time.Millisecond)

	err = mio.MergeAndUpload(114, files)
	require.NoError(t, err)
}

func TestMergerIO_MergeUpload_WriteObjectError(t *testing.T) {
	t.Skip()
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
	mio := NewIOStore(oneBlockStoreStore, mergedBlocksStore, 10, nil, nil, 1, 10*time.Millisecond)

	mio.(*IOStore).writeObjectFunc = func() error {
		return fmt.Errorf("yo")
	}

	err = mio.MergeAndUpload(114, files)
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
	mio := NewIOStore(oneBlockStoreStore, mergedBlocksStore, 10, nil, nil, 1, 10*time.Millisecond)

	err = mio.MergeAndUpload(114, files)
	require.Nil(t, err)

	obf, err := mio.FetchMergedOneBlockFiles(114)
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
	mio := NewIOStore(oneBlockStoreStore, mergedBlocksStore, 10, nil, nil, 1, 10*time.Millisecond)

	obf, err := mio.FetchMergedOneBlockFiles(69)
	// file not found
	require.Error(t, err)
	require.Errorf(t, err, "not found")
	require.Nil(t, obf)
}

func TestNewOneBlockFilesDeleter(t *testing.T) {
	oneBlockStoreStore, err := dstore.NewDBinStore("/tmp/oneblockstore")
	require.NoError(t, err)
	filesDeleter := NewOneBlockFilesDeleter(oneBlockStoreStore)
	require.NotNil(t, filesDeleter)
	require.IsType(t, &oneBlockFilesDeleter{}, filesDeleter)
}

func TestOneBlockFilesDeleter_Start_ZeroLengthOneBlockFiles(t *testing.T) {
	targetPath := "/tmp/oneblockstore"
	var oneBlockFiles []*bundle.OneBlockFile

	oneBlockStoreStore, err := dstore.NewDBinStore(targetPath)
	require.NoError(t, err)
	filesDeleter := NewOneBlockFilesDeleter(oneBlockStoreStore)

	filesDeleter.Delete(oneBlockFiles)
	filesDeleter.Start(1, 100)
}

func TestOneBlockFilesDeleter_Start(t *testing.T) {

	targetPath := "/tmp/oneblockstore"
	filenames := []string{
		"0000000114-20210728T105016.0-00000114a-00000113a-90-suffix",
		"0000000115-20210728T105116.0-00000115a-00000114a-90-suffix",
		"0000000116-20210728T105216.0-00000116a-00000115a-90-suffix",
		"0000000117-20210728T105316.0-00000117a-00000116a-90-suffix",
		"0000000118-20210728T105316.0-00000118a-00000117a-90-suffix",
	}
	var oneBlockFiles []*bundle.OneBlockFile

	oneBlockStoreStore, err := dstore.NewStore(targetPath, "", "", false)
	require.NoError(t, err)
	filesDeleter := NewOneBlockFilesDeleter(oneBlockStoreStore)

	for _, filename := range filenames {
		f := fmt.Sprintf("%s/%s", targetPath, filename)
		if err = ioutil.WriteFile(f, []byte{}, 0644); err != nil {
			panic(err)
		}
		oneBlockFiles = append(oneBlockFiles, bundle.MustNewOneBlockFile(filename))
	}

	filesDeleter.Start(1, 100)
	filesDeleter.Delete(oneBlockFiles)
	time.Sleep(1 * time.Second)

	files, err := ioutil.ReadDir(targetPath)
	require.NoError(t, err)

	require.Equal(t, len(files), 0)
}
