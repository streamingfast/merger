package merger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dbin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBundleReader_ReadSimpleFiles(t *testing.T) {
	bundle := NewTestBundle()

	r := NewBundleReader(context.Background(), testLogger, testTracer, bundle, nil)
	r1 := make([]byte, 4)

	read, err := r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 2)
	assert.Equal(t, r1, []byte{0x1, 0x2, 0x0, 0x0})

	read, err = r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 2)
	assert.Equal(t, r1, []byte{0x3, 0x4, 0x0, 0x0})

	read, err = r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 2)
	assert.Equal(t, r1, []byte{0x5, 0x6, 0x0, 0x0})

	read, err = r.Read(r1)
	assert.Equal(t, 0, read)
	assert.Equal(t, err, io.EOF)
}

func TestBundleReader_ReadByChunk(t *testing.T) {
	bundle := NewTestBundle()

	r := NewBundleReader(context.Background(), testLogger, testTracer, bundle, nil)
	r1 := make([]byte, 1)

	read, err := r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 1)
	assert.Equal(t, r1, []byte{0x1})

	read, err = r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 1)
	assert.Equal(t, r1, []byte{0x2})

	read, err = r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 1)
	assert.Equal(t, r1, []byte{0x3})

	read, err = r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 1)
	assert.Equal(t, r1, []byte{0x4})

	read, err = r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 1)
	assert.Equal(t, r1, []byte{0x5})

	read, err = r.Read(r1)
	require.NoError(t, err)
	assert.Equal(t, read, 1)
	assert.Equal(t, r1, []byte{0x6})

	read, err = r.Read(r1)
	require.Error(t, io.EOF)
}

func NewTestOneBlockFileFromFile(t *testing.T, fileName string) *OneBlockFile {
	t.Helper()
	data, err := ioutil.ReadFile(path.Join("test_data", fileName))
	require.NoError(t, err)
	time.Sleep(1 * time.Millisecond)
	return &OneBlockFile{
		CanonicalName: fileName,
		Filenames:     map[string]struct{}{fileName: Empty},
		BlockTime:     time.Now(),
		ID:            "",
		Num:           0,
		PreviousID:    "",
		MemoizeData:   data,
	}
}

func NewTestBundle() []*OneBlockFile {
	bstream.GetBlockWriterHeaderLen = 0

	bt := time.Time{}
	o1 := &OneBlockFile{
		CanonicalName: "o1",
		BlockTime:     bt,
		MemoizeData:   []byte{0x1, 0x2},
	}
	o2 := &OneBlockFile{
		CanonicalName: "o2",
		BlockTime:     bt.Local().Add(1 * time.Second),
		MemoizeData:   []byte{0x3, 0x4},
	}
	o3 := &OneBlockFile{
		CanonicalName: "o3",
		BlockTime:     bt.Local().Add(2 * time.Second),
		MemoizeData:   []byte{0x5, 0x6},
	}
	return []*OneBlockFile{o1, o2, o3}
}

func NewDownloadBundle() []*OneBlockFile {
	bt := time.Time{}
	o1 := &OneBlockFile{
		CanonicalName: "o1",
		BlockTime:     bt,
		MemoizeData:   []byte{},
	}
	o2 := &OneBlockFile{
		CanonicalName: "o2",
		BlockTime:     bt.Local().Add(1 * time.Second),
		MemoizeData:   []byte{},
	}
	o3 := &OneBlockFile{
		CanonicalName: "o3",
		BlockTime:     bt.Local().Add(2 * time.Second),
		MemoizeData:   []byte{},
	}
	return []*OneBlockFile{o1, o2, o3}
}

func TestBundleReader_Read_Then_Read_Block(t *testing.T) {
	//important
	bstream.GetBlockWriterHeaderLen = 10

	bundle := []*OneBlockFile{
		NewTestOneBlockFileFromFile(t, "0000000001-20150730T152628.0-13406cb6-b1cb8fa3.dbin"),
		NewTestOneBlockFileFromFile(t, "0000000002-20150730T152657.0-044698c9-13406cb6.dbin"),
		NewTestOneBlockFileFromFile(t, "0000000003-20150730T152728.0-a88cf741-044698c9.dbin"),
	}

	r := NewBundleReader(context.Background(), testLogger, testTracer, bundle, nil)
	allBlockData, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	dbinReader := dbin.NewReader(bytes.NewReader(allBlockData))

	//Reader header once
	_, _, err = dbinReader.ReadHeader()

	//Block 1
	require.NoError(t, err)
	b1, err := dbinReader.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, b1, bundle[0].MemoizeData[14:])

	//Block 2
	require.NoError(t, err)
	b2, err := dbinReader.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, b2, bundle[1].MemoizeData[14:])

	//Block 3
	require.NoError(t, err)
	b3, err := dbinReader.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, b3, bundle[2].MemoizeData[14:])
}

func TestBundleReader_Read_DownloadOneBlockFileError(t *testing.T) {
	bundle := NewDownloadBundle()

	downloadOneBlockFile := func(ctx context.Context, oneBlockFile *OneBlockFile) (data []byte, err error) {
		return nil, fmt.Errorf("some error")
	}

	r := NewBundleReader(context.Background(), testLogger, testTracer, bundle, downloadOneBlockFile)
	r1 := make([]byte, 4)

	read, err := r.Read(r1)
	require.Equal(t, read, 0)
	require.Errorf(t, err, "some error")
}

func TestBundleReader_Read_DownloadOneBlockFileCorrupt(t *testing.T) {
	//important
	bstream.GetBlockWriterHeaderLen = 10

	bundle := NewDownloadBundle()

	downloadOneBlockFile := func(ctx context.Context, oneBlockFile *OneBlockFile) (data []byte, err error) {
		return []byte{0xAB, 0xCD, 0xEF}, nil
	}

	r := NewBundleReader(context.Background(), testLogger, testTracer, bundle, downloadOneBlockFile)
	r1 := make([]byte, 4)
	r.headerPassed = true

	read, err := r.Read(r1)
	require.Equal(t, read, 0)
	require.Error(t, err)
}

func TestBundleReader_Read_DownloadOneBlockFileZeroLength(t *testing.T) {
	bundle := NewDownloadBundle()

	downloadOneBlockFile := func(ctx context.Context, oneBlockFile *OneBlockFile) (data []byte, err error) {
		return []byte{}, nil
	}

	r := NewBundleReader(context.Background(), testLogger, testTracer, bundle, downloadOneBlockFile)
	r1 := make([]byte, 4)
	r.headerPassed = true

	read, err := r.Read(r1)
	require.Equal(t, read, 0)
	require.Errorf(t, err, "EOF")
}

func TestBundleReader_Read_ReadBufferNotNil(t *testing.T) {
	bundle := NewDownloadBundle()

	downloadOneBlockFile := func(ctx context.Context, oneBlockFile *OneBlockFile) (data []byte, err error) {
		return nil, fmt.Errorf("some error")
	}

	r := NewBundleReader(context.Background(), testLogger, testTracer, bundle, downloadOneBlockFile)
	r.readBuffer = []byte{0xAB, 0xCD}
	r1 := make([]byte, 4)

	read, err := r.Read(r1)
	require.Equal(t, read, 2)
	require.Nil(t, err)
}

func TestBundleReader_Read_EmptyListOfOneBlockFiles(t *testing.T) {
	bundle := NewDownloadBundle()

	downloadOneBlockFile := func(ctx context.Context, oneBlockFile *OneBlockFile) (data []byte, err error) {
		return nil, fmt.Errorf("some error")
	}

	r := NewBundleReader(context.Background(), testLogger, testTracer, bundle, downloadOneBlockFile)
	r.oneBlockFiles = []*OneBlockFile{}
	r1 := make([]byte, 4)

	read, err := r.Read(r1)
	require.Equal(t, read, 0)
	require.Errorf(t, err, "EOF")
}
